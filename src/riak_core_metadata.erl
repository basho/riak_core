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
-module(riak_core_metadata).

-export([get/2,
         get/3,
         fold/3,
         iterator/1,
         itr_next/1,
         itr_done/1,
         itr_key_values/1,
         itr_key/1,
         itr_values/1,
         put/3,
         put/4]).

-include("riak_core_metadata.hrl").

%% Get Option Types
-type get_opt_default_val() :: {default, metadata_value()}.
-type get_opt_resolver()    :: {resolver, metadata_resolver()}.
-type get_opt()             :: get_opt_default_val() | get_opt_resolver().
-type get_opts()            :: [get_opt()].

%% Put Option Types
-type put_opts()            :: [].


%% @doc same as get(FullPrefix, Key, [])
-spec get(metadata_prefix(), metadata_key()) -> metadata_value() | undefined.
get(FullPrefix, Key) ->
    get(FullPrefix, Key, []).

%% @doc Retrieves the local value stored at the given prefix and key.
%%
%% get/3 can take the following options:
%%  * default: value to return if no value is found, `undefined' if not given.
%%  * resolver:  A function that resolves conflicts if they are encountered. If not given
%%               last-write-wins is used to resolve the conflicts
%%
%% NOTE: an update will be broadcast if conflicts are resolved. any further conflicts generated
%% by concurrenct writes during resolution are not resolved
-spec get(metadata_prefix(), metadata_key(), get_opts()) -> metadata_value().
get({Prefix, SubPrefix}=FullPrefix, Key, Opts) when is_binary(Prefix) andalso
                                                    is_binary(SubPrefix) ->
    PKey = prefixed_key(FullPrefix, Key),
    Default = get_option(default, Opts, undefined),
    ResolveMethod = get_option(resolver, Opts, lww),
    case riak_core_metadata_manager:get(PKey) of
        undefined -> Default;
        Existing -> maybe_resolve(PKey, Existing, ResolveMethod)
    end.

%% @spec Fold over all keys and values stored under a given prefix/subprefix
-spec fold(fun(({metadata_key(), [metadata_value()]}) -> any()), any(), metadata_prefix()) ->
                  any().
fold(Fun, Acc0, FullPrefix) ->
    It = iterator(FullPrefix),
    fold_it(Fun, Acc0, It).

fold_it(Fun, Acc, It) ->
    case itr_done(It) of
        true -> Acc;
        false ->
            Next = Fun(itr_key_values(It), Acc),
            fold_it(Fun, Next, itr_next(It))
    end.

%% @doc Return an iterator pointing to the first key stored under a prefix
-spec iterator(metadata_prefix()) -> riak_core_metadata_manager:iterator().
iterator({Prefix, SubPrefix}=FullPrefix) when is_binary(Prefix) andalso
                                              is_binary(SubPrefix) ->
    riak_core_metadata_manager:iterator(FullPrefix).

%% @doc Advances the iterator
-spec itr_next(riak_core_metadata_manager:iterator()) -> riak_core_metadata_manager:iterator().
itr_next(It) ->
    riak_core_metadata_manager:iterate(It).

%% @doc Returns true if there is nothing more to iterate over
-spec itr_done(riak_core_metadata_manager:iterator()) -> boolean().
itr_done(It) ->
    riak_core_metadata_manager:iterator_done(It).

%% @doc Return the key and all sibling values pointed at by the iterator. Before
%% calling this function, check the iterator is not complete w/ itr_done/1.
-spec itr_key_values(riak_core_metadata_manager:iterator()) -> {metadata_key(), [metadata_value()]}.
itr_key_values(It) ->
    {Key, Obj} = riak_core_metadata_manager:iterator_value(It),
    {Key, riak_core_metadata_object:values(Obj)}.

%% @doc Return the key pointed at by the iterator. Before
%% calling this function, check the iterator is not complete w/ itr_done/1.
-spec itr_key(riak_core_metadata_manager:iterator()) -> metadata_key().
itr_key(It) ->
    {Key, _} = itr_key_values(It),
    Key.

%% @doc Return all sibling values pointed at by the iterator. Before
%% calling this function, check the iterator is not complete w/ itr_done/1.
-spec itr_values(riak_core_metadata_manager:iterator()) -> [metadata_value()].
itr_values(It) ->
    {_, Values} = itr_key_values(It),
    Values.

%% @doc same as put(FullPrefix, Key, Value, [])
-spec put(metadata_prefix(), metadata_key(), metadata_value()) -> ok.
put(FullPrefix, Key, Value) ->
    put(FullPrefix, Key, Value, []).

%% @doc Stores the value at the given prefix and key locally and then triggers a
%% broadcast to notify other nodes in the cluster. Currently, there are no put
%% options
-spec put(metadata_prefix(), metadata_key(), metadata_value(), put_opts()) -> ok.
put({Prefix, SubPrefix}=FullPrefix, Key, Value, _Opts) when is_binary(Prefix) andalso
                                                            is_binary(SubPrefix) ->
    PKey = prefixed_key(FullPrefix, Key),
    CurrentContext = current_context(PKey),
    Updated = riak_core_metadata_manager:put(PKey, CurrentContext, Value),
    broadcast(PKey, Updated).

current_context(PKey) ->
    case riak_core_metadata_manager:get(PKey) of
        undefined -> riak_core_metadata_object:empty_context();
        CurrentMeta -> riak_core_metadata_object:context(CurrentMeta)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
maybe_resolve(PKey, Existing, Method) ->
    SibCount = riak_core_metadata_object:value_count(Existing),
    maybe_resolve(PKey, Existing, SibCount, Method).

%% @private
maybe_resolve(_PKey, Existing, 1, _Method) ->
    riak_core_metadata_object:value(Existing);
maybe_resolve(PKey, Existing, _, Method) ->
    Reconciled = riak_core_metadata_object:resolve(Existing, Method),
    RContext = riak_core_metadata_object:context(Reconciled),
    RValue = riak_core_metadata_object:value(Reconciled),
    Stored = riak_core_metadata_manager:put(PKey, RContext, RValue),
    broadcast(PKey, Stored),
    RValue.

%% @private
broadcast(PKey, Obj) ->
    Broadcast = #metadata_broadcast{pkey = PKey,
                                    obj  = Obj},
    riak_core_broadcast:broadcast(Broadcast, riak_core_metadata_manager).

%% @private
-spec prefixed_key(metadata_prefix(), metadata_key()) -> metadata_pkey().
prefixed_key(FullPrefix, Key) ->
    {FullPrefix, Key}.

get_option(Key, Opts, Default) ->
    proplists:get_value(Key, Opts, Default).
