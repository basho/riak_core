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

%% @doc Bucket Types allow groups of buckets to share configuration
%% details.  Each bucket belongs to a type and inherits its
%% properties. Buckets can override the properties they inherit using
%% {@link riak_core_bucket}.  The "Default Bucket Type" always
%% exists. The Default Type's properties come from the riak_core
%% `default_bucket_props' application config, so the Default Type and
%% its buckets continue to act as they had prior to the existence of
%% Bucket Types.
%%
%% Unlike Buckets, Bucket Types must be explicitly created. In
%% addition, types support setting some properties only on creation
%% (via {@link riak_core_bucket_props:validate/4}). Since, types are
%% stored using {@link riak_core_metadata}, in order to provide safe
%% creation semantics the following invariant must be satisfied: all
%% nodes in the cluster either see no type or a single version of the
%% type for the lifetime of the cluster (nodes need not see the single
%% version at the same time). As part of ensuring this invariant, creation
%% is a two-step process:
%%
%%   1. The type is created and is inactive. To the node an inactive type
%%      does not exist
%%   2. When the creation has propogated to all nodes, the type may be activated.
%%      As the activation propogates, nodes will be able to use the type
%%
%% The first step is performed using {@link create/2}. The second by
%% {@link activate/1}.  After the type has been activated, some
%% properties may be updated using {@link update/2}. All operations are
%% serialized through {@link riak_core_claimant} except reading bucket
%% type properties with {@link get/1}.
%%
%% Bucket types can be in one of four states. The
%% state of a type can be queried using {@link status/1}.
%%
%%   1. undefined - the type has not been created
%%   2. created - the type has been created but has not propogated to all nodes
%%   3. ready - the type has been created and has propogated to all nodes but
%%              has not been activated
%%   4. active - the Bucket Type has been activated, but the activation may
%%               not have propogated to all nodes yet
%%
%% In order for the invariant to hold, additional restrictions are
%% placed on the operations, generally and based on the state of the
%% Bucket Type. These restrictions are in-place to ensure safety
%% during cases where membership changes or node failures change the
%% {@link riak_core_claimant} to a new node -- ensuring concurrent
%% updates do not break the invariant.
%%
%%   * calling {@link create/1} multiple times before a Bucket Type
%%     is active is allowed. The newer creation will supersede any
%%     previous ones. In addition, the type will be "claimed" by the
%%     {@link riak_core_claimant} node writing the property. Future
%%     calls to {@link create/1} must be serialized through the same
%%     claimant node or the call will not succeed. In the case where
%%     the claimed type fails to propogate to a new claimant during a
%%     a failure the potential concurrent update is resolved with
%%     last-write-wins. Since nodes can not use inactive types, this is
%%     safe.
%%   * A type may only be activated if it is in the `ready' state. This means
%%     all nodes must be reachable from the claimant
%%   * {@link create/1} will fail if the type is active. Activation concurrent
%%     with creation is not possible due to the previous restriction
%%   * {@link update/1} will fail unless the type is updated. {@link update/1} does
%%     not allow modifications to properties for which the invariant must hold
%%     (NOTE: this is up to the implementor of the riak_core bucket_validator).
%%
%% There is one known case where this invariant does not hold:
%%    * in the case where a singleton cluster activates a type before being joined
%%      to a cluster that has activated the same type. This is a case poorly handled
%%      by most riak_core applications and is considered acceptable (so dont do it!).
-module(riak_core_bucket_type).

-include("riak_core_bucket_type.hrl").

-export([defaults/0,
    defaults/1,
    create/2,
    status/1,
    activate/1,
    update/2,
    get/1,
    reset/1,
    fold/2,
    iterator/0,
    itr_next/1,
    itr_done/1,
    itr_value/1,
    itr_close/1,
    property_hash/2,
    property_hash/3, all_n/0]).

-export_type([bucket_type/0]).
-type bucket_type()       :: binary().
-type bucket_type_props() :: riak_core_bucket:properties().

-define(IF_CAPABLE(X, E), case riak_core_capability:get({riak_core, bucket_types}) of
                              true -> X;
                              false -> E
                          end).

%% @doc The hardcoded defaults for all bucket types.
-spec defaults() -> bucket_type_props().
defaults() ->
    custom_type_defaults().

%% @doc The hardcoded defaults for the legacy, default bucket
%% type. These find their way into the `default_bucket_props'
%% environment variable
-spec defaults(default_type) -> bucket_type_props().
defaults(default_type) ->
    default_type_defaults().

default_type_defaults() ->
    common_defaults() ++
        [{dvv_enabled, false},
         {allow_mult, false}].

custom_type_defaults() ->
    common_defaults() ++
        %% @HACK dvv is a riak_kv only thing, yet there is nowhere else
        %% to put it (except maybe fixups?)
        [{dvv_enabled, true},
         {allow_mult, true}].

common_defaults() ->
    [{linkfun, {modfun, riak_kv_wm_link_walker, mapreduce_linkfun}},
     {old_vclock, 86400},
     {young_vclock, 20},
     {big_vclock, 50},
     {small_vclock, 50},
     {pr, 0},
     {r, quorum},
     {w, quorum},
     {pw, 0},
     {dw, quorum},
     {rw, quorum},
     {basic_quorum, false},
     {notfound_ok, true},
     {n_val,3},
     {last_write_wins,false},
     {precommit, []},
     {postcommit, []},
     {chash_keyfun, {riak_core_util, chash_std_keyfun}}].

%% @doc When creating a Time Series table, set the replication property
%% defaults to other values than for normal KV bucket types.  Naturally
%% let the user override any default values (per RTS-1469)
%% The `ddl` property contains the TS table DDL, so denotes a
%% time series bucket type.
-spec ts_defaults(bucket_type_props()) -> bucket_type_props().
ts_defaults(Props) ->
    case proplists:is_defined(ddl, Props) of
        true ->
            ts_specific_defaults();
        _ -> []
    end.

ts_specific_defaults() ->
    [{allow_mult, false},
     {dvv_enabled, false},
     {dw, one},
     {last_write_wins, true},
     {r, one},
     {rw, one}].


%% @doc Create the type. The type is not activated (available to nodes) at this time. This
%% function may be called arbitrarily many times if the claimant does not change between
%% calls and the type is not active. An error will also be returned if the properties
%% are not valid. Properties not provided will be taken from those returned by
%% @see defaults/0.
-spec create(bucket_type(), bucket_type_props()) -> ok | {error, term()}.
create(?DEFAULT_TYPE, _Props) ->
    {error, default_type};
create(BucketType, Props) when is_binary(BucketType) ->
    Props1 = riak_core_bucket_props:merge(Props,
        riak_core_bucket_props:merge(ts_defaults(Props), defaults())),
    ?IF_CAPABLE(riak_core_claimant:create_bucket_type(BucketType, Props1),
                {error, not_capable}).

%% @doc Returns the state the type is in.
-spec status(bucket_type()) -> undefined | created | ready | active.
status(?DEFAULT_TYPE) ->
    active;
status(BucketType) when is_binary(BucketType) ->
    ?IF_CAPABLE(riak_core_claimant:bucket_type_status(BucketType), undefined).

%% @doc Activate the type. This will succeed only if the type is in the `ready' state. Otherwise,
%% an error is returned.
-spec activate(bucket_type()) -> ok | {error, undefined | not_ready}.
activate(?DEFAULT_TYPE) ->
    ok;
activate(BucketType) when is_binary(BucketType) ->
    ?IF_CAPABLE(riak_core_claimant:activate_bucket_type(BucketType), {error, undefined}).

%% @doc Update an existing bucket type. Updates may only be performed
%% on active types. Properties not provided will keep their existing
%% values.
-spec update(bucket_type(), bucket_type_props()) -> ok | {error, term()}.
update(?DEFAULT_TYPE, _Props) ->
    {error, no_default_update}; %% default props are in the app.config
update(BucketType, Props) when is_binary(BucketType)->
    ?IF_CAPABLE(riak_core_claimant:update_bucket_type(BucketType, Props), {error, not_capable}).

%% @doc Return the properties associated with the given bucket type.
-spec get(bucket_type()) -> undefined | bucket_type_props().
get(<<"default">>) ->
    riak_core_bucket_props:defaults();
get(BucketType) when is_binary(BucketType) ->
    riak_core_claimant:get_bucket_type(BucketType, undefined).

%% @doc Reset the properties of the bucket. This only affects properties that
%% can be set using {@link update/2} and can only be performed on an active
%% type.
%%
%% This is not currently hooked into `riak-admin' but can be invoked
%% from the console.
-spec reset(bucket_type()) -> ok | {error, term()}.
reset(BucketType) ->
    update(BucketType, defaults()).

%% @doc iterate over bucket types and find any active buckets.
-spec all_n() -> riak_core_bucket:nval_set().
all_n() ->
    riak_core_bucket_type:fold(fun bucket_type_prop_nval_fold/2, ordsets:new()).

%% @private
-spec bucket_type_prop_nval_fold({bucket_type(), riak_core_bucket:properties()},
        riak_core_bucket:nval_set()) -> riak_core_bucket:nval_set().
bucket_type_prop_nval_fold({_BType, BProps}, Accum) ->
    case riak_core_bucket:get_value(active, BProps) of
        true ->
            bucket_prop_nval_fold(BProps, Accum);
        _ ->
            Accum
    end.

-spec bucket_prop_nval_fold(riak_core_bucket:properties(), riak_core_bucket:nval_set()) ->
    riak_core_bucket:nval_set().
bucket_prop_nval_fold(BProps, Accum) ->
    case riak_core_bucket:get_value(n_val, BProps) of
        undefined ->
            Accum;
        NVal ->
            ordsets:add_element(NVal, Accum)
    end.

%% @doc Fold over all bucket types, storing result in accumulator
-spec fold(fun(({bucket_type(), bucket_type_props()}, any()) -> any()),
           Accumulator::any()) ->
    any().
fold(Fun, Accum) ->
    fold(iterator(), Fun, Accum).

-spec fold(
    riak_core_metadata:iterator(),
    fun(({bucket_type(), bucket_type_props()}, any()) -> any()),
    any()
) ->
    any().
fold(It, Fun, Accum) ->
    case riak_core_bucket_type:itr_done(It) of
        true ->
            riak_core_bucket_type:itr_close(It),
            Accum;
        _ ->
            NewAccum = Fun(itr_value(It), Accum),
            fold(riak_core_bucket_type:itr_next(It), Fun, NewAccum)
    end.

%% @doc Return an iterator that can be used to walk through all existing bucket types
%% and their properties
-spec iterator() -> riak_core_metadata:iterator().
iterator() ->
    riak_core_claimant:bucket_type_iterator().

%% @doc Advance the iterator to the next bucket type. itr_done/1 should always be called
%% before this function
-spec itr_next(riak_core_metadata:iterator()) ->
                      riak_core_metadata:iterator().
itr_next(It) ->
    riak_core_metadata:itr_next(It).

%% @doc Returns true if there are no more bucket types to iterate over
-spec itr_done(riak_core_metadata:iterator()) -> boolean().
itr_done(It) ->
    riak_core_metadata:itr_done(It).

%% @doc Returns the type and properties that the iterator points too. Any siblings,
%% are resolved at this time. itr_done/1 should be checked before calling this function.
-spec itr_value(riak_core_metadata:iterator()) ->
                       {bucket_type(), bucket_type_props()}.
itr_value(It) ->
    {BucketType, Props} = riak_core_metadata:itr_key_values(It),
    {BucketType, Props}.


-spec itr_close(riak_core_metadata:iterator()) -> ok.
itr_close(It) ->
    riak_core_metadata:itr_close(It).

%% @doc Returns a hash of a specified set of bucket type properties
%% whose values may have implications on the treatment or handling of
%% buckets created using the bucket type.
-spec property_hash(bucket_type(), [term()]) -> undefined | integer().
property_hash(Type, PropKeys) ->
    property_hash(Type, PropKeys, ?MODULE:get(Type)).

-spec property_hash(bucket_type(), [term()], undefined | bucket_type_props()) ->
                           undefined | integer().
property_hash(_Type, _PropKeys, undefined) ->
    undefined;
property_hash(_Type, PropKeys, Props) ->
    erlang:phash2([lists:keyfind(PropKey, 1, Props) || PropKey <- PropKeys]).
