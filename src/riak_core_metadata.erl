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


%% @doc same as get(Prefix, Key, [])
-spec get(metadata_prefix(), metadata_key()) -> metadata_value() | undefined.
get(Prefix, Key) ->
    get(Prefix, Key, []).

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
get(Prefix, Key, Opts) ->
    PKey = prefixed_key(Prefix, Key),
    Default = get_option(default, Opts, undefined),
    ResolveMethod = get_option(resolver, Opts, lww),
    case riak_core_metadata_manager:get(PKey) of
        undefined -> Default;
        Existing -> maybe_resolve(PKey, Existing, ResolveMethod)
    end.

%% @doc same as put(Prefix, Key, Value, [])
-spec put(metadata_prefix(), metadata_key(), metadata_value()) -> ok.
put(Prefix, Key, Value) ->
    put(Prefix, Key, Value, []).

%% @doc Stores the value at the given prefix and key locally and then triggers a
%% broadcast to notify other nodes in the cluster. Currently, there are no put
%% options
-spec put(metadata_prefix(), metadata_key(), metadata_value(), put_opts()) -> ok.
put(Prefix, Key, Value, _Opts) ->
    PKey = prefixed_key(Prefix, Key),
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
prefixed_key(Prefix, Key) ->
    {Prefix, Key}.

get_option(Key, Opts, Default) ->
    proplists:get_value(Key, Opts, Default).
