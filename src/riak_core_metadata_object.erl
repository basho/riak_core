%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(riak_core_metadata_object).

-export([value/1,
         values/1,
         value_count/1,
         context/1,
         empty_context/0,
         hash/1,
         modify/4,
         reconcile/2,
         resolve/2,
         is_stale/2,
         equal_context/2]).

-include("riak_core_metadata.hrl").

%% @doc returns a single value. if the object holds more than one value an error is generated
%% @see values/2
-spec value(metadata_object()) -> metadata_value().
value(Metadata) ->
    [Value] = values(Metadata),
    Value.

%% @doc returns a list of values held in the object
-spec values(metadata_object()) -> [metadata_value()].
values({metadata, Object}) ->
    [Value || {Value, _Ts} <- dvvset:values(Object)].

%% @doc returns the number of siblings in the given object
-spec value_count(metadata_object()) -> non_neg_integer().
value_count({metadata, Object}) ->
    dvvset:size(Object).

%% @doc returns the context (opaque causal history) for the given object
-spec context(metadata_object()) -> metadata_context().
context({metadata, Object}) ->
    dvvset:join(Object).

%% @doc returns the representation for an empty context (opaque causal history)
-spec empty_context() -> metadata_context().
empty_context() -> [].

%% @doc returns a hash representing the metadata objects contents
-spec hash(metadata_object()) -> binary().
hash({metadata, Object}) ->
    riak_core_util:sha(term_to_binary(Object)).

%% @doc modifies a potentially existing object, setting its value and updating
%% the causual history. If a function is provided as the third argument
%% then this function also is used for conflict resolution. The difference
%% between this function and resolve/2 is that the logical clock is advanced in the
%% case of this function. Additionally, the resolution functions are slightly different.
-spec modify(metadata_object() | undefined,
             metadata_context(),
             metadata_value() | metadata_modifier(),
             term()) -> metadata_object().
modify(undefined, Context, Fun, ServerId) when is_function(Fun) ->
    modify(undefined, Context, Fun(undefined), ServerId);
modify(Obj, Context, Fun, ServerId) when is_function(Fun) ->
    modify(Obj, Context, Fun(values(Obj)), ServerId);
modify(undefined, _Context, Value, ServerId) ->
    %% Ignore the context since we dont have a value, its invalid if not
    %% empty anyways, so give it a valid one
    NewRecord = dvvset:new(timestamped_value(Value)),
    {metadata, dvvset:update(NewRecord, ServerId)};
modify({metadata, Existing}, Context, Value, ServerId) ->
    InsertRec = dvvset:new(Context, timestamped_value(Value)),
    {metadata, dvvset:update(InsertRec, Existing, ServerId)}.

%% @doc Reconciles a remote object received during replication or anti-entropy
%% with a local object. If the remote object is an anscestor of or is equal to the local one
%% `false' is returned, otherwise the reconciled object is returned as the second
%% element of the two-tuple
-spec reconcile(metadata_object(), metadata_object() | undefined) ->
                       false | {true, metadata_object()}.
reconcile(undefined, _LocalObj) ->
    false;
reconcile(RemoteObj, undefined) ->
    {true, RemoteObj};
reconcile({metadata, RemoteObj}, {metadata, LocalObj}) ->
    Less  = dvvset:less(RemoteObj, LocalObj),
    Equal = dvvset:equal(RemoteObj, LocalObj),
    case not (Equal or Less) of
        false -> false;
        true ->
            {true, {metadata, dvvset:sync([LocalObj, RemoteObj])}}
    end.

%% @doc Resolves siblings using either last-write-wins or the provided function and returns
%% an object containing a single value. The causal history is not updated
-spec resolve(metadata_object(), lww | fun(([metadata_value()]) -> metadata_value())) ->
                     metadata_object().
resolve({metadata, Object}, lww) ->
    LWW = fun ({_,TS1}, {_,TS2}) -> TS1 =< TS2 end,
    {metadata, dvvset:lww(LWW, Object)};
resolve({metadata, Existing}, Reconcile) when is_function(Reconcile) ->
    ResolveFun = fun({A, _}, {B, _}) -> timestamped_value(Reconcile(A, B)) end,
    F = fun([Value | Rest]) -> lists:foldl(ResolveFun, Value, Rest) end,
    {metadata, dvvset:reconcile(F, Existing)}.

%% @doc Determines if the given context (version vector) is causually newer than
%% an existing object. If the object missing or if the context does not represent
%% an anscestor of the current key, false is returned. Otherwise, when the context
%% does represent an ancestor of the existing object or the existing object itself,
%% true is returned
-spec is_stale(metadata_context(), metadata_object()) -> boolean().
is_stale(_, undefined) ->
    false;
is_stale(RemoteContext, {metadata, Obj}) ->
    LocalContext = dvvset:join(Obj),
    %% returns true (stale) when local context is causally newer or equal to remote context
    descends(LocalContext, RemoteContext).

descends(_, []) ->
    true;
descends(Ca, Cb) ->
    [{NodeB, CtrB} | RestB] = Cb,
    case lists:keyfind(NodeB, 1, Ca) of
        false -> false;
        {_, CtrA} ->
            (CtrA >= CtrB) andalso descends(Ca, RestB)
    end.

%% @doc Returns true if the given context and the context of the existing object are equal
-spec equal_context(metadata_context(), metadata_object()) -> boolean().
equal_context(Context, {metadata, Obj}) ->
    Context =:= dvvset:join(Obj).

timestamped_value(Value) ->
    {Value, os:timestamp()}.
