%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2015 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_core_claimant).
-behaviour(gen_server).

-include("riak_core_bucket_type.hrl").

%% API
-export([start_link/0]).
-export([leave_member/1,
         remove_member/1,
         force_replace/2,
         replace/2,
         resize_ring/1,
         abort_resize/0,
         plan/0,
         commit/0,
         clear/0,
         ring_changed/2,
         create_bucket_type/2,
         update_bucket_type/2,
         bucket_type_status/1,
         activate_bucket_type/1,
         get_bucket_type/2,
         get_bucket_type/3,
         bucket_type_iterator/0]).
-export([reassign_indices/1]). % helpers for claim sim

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type action() :: leave
                | remove
                | {replace, node()}
                | {force_replace, node()}.

-type riak_core_ring() :: riak_core_ring:riak_core_ring().

%% A tuple representing a given cluster transition:
%%   {Ring, NewRing} where NewRing = f(Ring)
-type ring_transition() :: {riak_core_ring(), riak_core_ring()}.

-record(state, {
          last_ring_id,
          %% The set of staged cluster changes
          changes :: [{node(), action()}],

          %% Ring computed during the last planning stage based on
          %% applying a set of staged cluster changes. When commiting
          %% changes, the computed ring must match the previous planned
          %% ring to be allowed.
          next_ring :: riak_core_ring(),

          %% Random number seed passed to remove_node to ensure the
          %% current randomized remove algorithm is deterministic
          %% between plan and commit phases
          seed}).

-define(ROUT(S,A),ok).
%%-define(ROUT(S,A),?debugFmt(S,A)).
%%-define(ROUT(S,A),io:format(S,A)).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Spawn and register the riak_core_claimant server
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Determine how the cluster will be affected by the staged changes,
%%      returning the set of pending changes as well as a list of ring
%%      modifications that correspond to each resulting cluster transition
%%      (eg. the initial transition that applies the staged changes, and
%%      any additional transitions triggered by later rebalancing).
-spec plan() -> {error, term()} | {ok, [action()], [ring_transition()]}.
plan() ->
    gen_server:call(claimant(), plan, infinity).

%% @doc Commit the set of staged cluster changes, returning true on success.
%%      A commit is only allowed to succeed if the ring is ready and if the
%%      current set of changes matches those computed by the most recent
%%      call to plan/0.
-spec commit() -> ok | {error, term()}.
commit() ->
    gen_server:call(claimant(), commit, infinity).

%% @doc Stage a request for `Node' to leave the cluster. If committed, `Node'
%%      will handoff all of its data to other nodes in the cluster and then
%%      shutdown.
leave_member(Node) ->
    stage(Node, leave).

%% @doc Stage a request for `Node' to be forcefully removed from the cluster.
%%      If committed, all partitions owned by `Node' will immediately be
%%      re-assigned to other nodes. No data on `Node' will be transfered to
%%      other nodes, and all replicas on `Node' will be lost.
remove_member(Node) ->
    stage(Node, remove).

%% @doc Stage a request for `Node' to be replaced by `NewNode'. If committed,
%%      `Node' will handoff all of its data to `NewNode' and then shutdown.
%%      The current implementation requires `NewNode' to be a fresh node that
%%      is joining the cluster and does not yet own any partitions of its own.
replace(Node, NewNode) ->
    stage(Node, {replace, NewNode}).

%% @doc Stage a request for `Node' to be forcefully replaced by `NewNode'.
%%      If committed, all partitions owned by `Node' will immediately be
%%      re-assigned to `NewNode'. No data on `Node' will be transfered,
%%      and all replicas on `Node' will be lost. The current implementation
%%      requires `NewNode' to be a fresh node that is joining the cluster
%%      and does not yet own any partitions of its own.
force_replace(Node, NewNode) ->
    stage(Node, {force_replace, NewNode}).

%% @doc Stage a request to resize the ring. If committed, all nodes
%%      will participate in resizing operation. Unlike other operations,
%%      the new ring is not installed until all transfers have completed.
%%      During that time requests continue to be routed to the old ring.
%%      After completion, the new ring is installed and data is safely
%%      removed from partitons no longer owner by a node or present
%%      in the ring.
-spec resize_ring(integer()) -> ok | {error, atom()}.
resize_ring(NewRingSize) ->
    %% use the node making the request. it will be ignored
    stage(node(), {resize, NewRingSize}).

-spec abort_resize() -> ok | {error, atom()}.
abort_resize() ->
    stage(node(), abort_resize).

%% @doc Clear the current set of staged transfers
clear() ->
    gen_server:call(claimant(), clear, infinity).

%% @doc This function is called as part of the ring reconciliation logic
%%      triggered by the gossip subsystem. This is only called on the one
%%      node that is currently the claimant. This function is the top-level
%%      entry point to the claimant logic that orchestrates cluster state
%%      transitions. The current code path:
%%          riak_core_gossip:reconcile/2
%%          --> riak_core_ring:ring_changed/2
%%          -----> riak_core_ring:internal_ring_changed/2
%%          --------> riak_core_claimant:ring_changed/2
ring_changed(Node, Ring) ->
    internal_ring_changed(Node, Ring).

%% @see riak_core_bucket_type:create/2
-spec create_bucket_type(riak_core_bucket_type:bucket_type(), [{atom(), any()}]) ->
                                ok | {error, term()}.
create_bucket_type(BucketType, Props) ->
    gen_server:call(claimant(), {create_bucket_type, BucketType, Props}, infinity).

%% @see riak_core_bucket_type:status/1
-spec bucket_type_status(riak_core_bucket_type:bucket_type()) ->
                                undefined | created | ready | active.
bucket_type_status(BucketType) ->
    gen_server:call(claimant(), {bucket_type_status, BucketType}, infinity).

%% @see riak_core_bucket_type:activate/1
-spec activate_bucket_type(riak_core_bucket_type:bucket_type()) ->
                                  ok | {error, undefined | not_ready}.
activate_bucket_type(BucketType) ->
    gen_server:call(claimant(), {activate_bucket_type, BucketType}, infinity).

%% @doc Lookup the properties for `BucketType'. If there are no properties or
%% the type is inactive, the given `Default' value is returned.
-spec get_bucket_type(riak_core_bucket_type:bucket_type(), X) -> [{atom(), any()}] | X.
get_bucket_type(BucketType, Default) ->
    get_bucket_type(BucketType, Default, true).

%% @doc Lookup the properties for `BucketType'. If there are no properties or
%% the type is inactive and `RequireActive' is `true', the given `Default' value is
%% returned.
-spec get_bucket_type(riak_core_bucket_type:bucket_type(), X, boolean()) ->
                             [{atom(), any()}] | X.
get_bucket_type(BucketType, Default, RequireActive) ->
    %% we resolve w/ last-write-wins because conflicts only occur
    %% during creation when the claimant is changed and create on a
    %% new claimant happens before the original propogates. In this
    %% case we want the newest create. Updates can also result in
    %% conflicts so we choose the most recent as well.
    case riak_core_metadata:get(?BUCKET_TYPE_PREFIX, BucketType,
                                [{default, Default}]) of
        Default -> Default;
        Props -> maybe_filter_inactive_type(RequireActive, Default, Props)
    end.

%% @see riak_core_bucket_type:update/2
-spec update_bucket_type(riak_core_bucket_type:bucket_type(), [{atom(), any()}]) ->
                                ok | {error, term()}.
update_bucket_type(BucketType, Props) ->
    gen_server:call(claimant(), {update_bucket_type, BucketType, Props}).

%% @see riak_core_bucket_type:iterator/0
-spec bucket_type_iterator() -> riak_core_metadata:iterator().
bucket_type_iterator() ->
    riak_core_metadata:iterator(?BUCKET_TYPE_PREFIX, [{default, undefined},
                                              {resolver, fun riak_core_bucket_props:resolve/2}]).

%%%===================================================================
%%% Claim sim helpers until refactor
%%%===================================================================

reassign_indices(CState) ->
    reassign_indices(CState, [], erlang:now(), fun no_log/2).

%%%===================================================================
%%% Internal API helpers
%%%===================================================================

stage(Node, Action) ->
    gen_server:call(claimant(), {stage, Node, Action}, infinity).

claimant() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {?MODULE, riak_core_ring:claimant(Ring)}.

maybe_filter_inactive_type(false, _Default, Props) ->
    Props;
maybe_filter_inactive_type(true, Default, Props) ->
    case type_active(Props) of
        true -> Props;
        false -> Default
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    schedule_tick(),
    {ok, #state{changes=[], seed=erlang:now()}}.

handle_call(clear, _From, State) ->
    State2 = clear_staged(State),
    {reply, ok, State2};

handle_call({stage, Node, Action}, _From, State) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    {Reply, State2} = maybe_stage(Node, Action, Ring, State),
    {reply, Reply, State2};

handle_call(plan, _From, State) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    case riak_core_ring:ring_ready(Ring) of
        false ->
            Reply = {error, ring_not_ready},
            {reply, Reply, State};
        true ->
            {Reply, State2} = generate_plan(Ring, State),
            {reply, Reply, State2}
    end;

handle_call(commit, _From, State) ->
    {Reply, State2} = commit_staged(State),
    {reply, Reply, State2};

handle_call({create_bucket_type, BucketType, Props0}, _From, State) ->
    Existing = get_bucket_type(BucketType, undefined, false),
    case can_create_type(BucketType, Existing, Props0) of
        {ok, Props} ->
            InactiveProps = lists:keystore(active, 1, Props, {active, false}),
            ClaimedProps = lists:keystore(claimant, 1, InactiveProps, {claimant, node()}),
            riak_core_metadata:put(?BUCKET_TYPE_PREFIX, BucketType, ClaimedProps),
            {reply, ok, State};
        Error ->
            {reply, Error, State}
    end;

handle_call({update_bucket_type, BucketType, Props0}, _From, State) ->
    Existing = get_bucket_type(BucketType, [], false),
    case can_update_type(BucketType, Existing, Props0) of
        {ok, Props} ->
            MergedProps = riak_core_bucket_props:merge(Props, Existing),
            riak_core_metadata:put(?BUCKET_TYPE_PREFIX, BucketType, MergedProps),
            {reply, ok, State};
        Error ->
            {reply, Error, State}
    end;

handle_call({bucket_type_status, BucketType}, _From, State) ->
    Existing = get_bucket_type(BucketType, undefined, false),
    Reply = get_type_status(BucketType, Existing),
    {reply, Reply, State};

handle_call({activate_bucket_type, BucketType}, _From, State) ->
    Existing = get_bucket_type(BucketType, undefined, false),
    Status = get_type_status(BucketType, Existing),
    Reply = maybe_activate_type(BucketType, Status, Existing),
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    State2 = tick(State),
    {noreply, State2};

handle_info(reset_ring_id, State) ->
    State2 = State#state{last_ring_id=undefined},
    {noreply, State2};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
%% @doc Verify that a cluster change request is valid and add it to
%%      the list of staged changes.
maybe_stage(Node, Action, Ring, State=#state{changes=Changes}) ->
    case valid_request(Node, Action, Changes, Ring) of
        true ->
            Changes2 = orddict:store(Node, Action, Changes),
            Changes3 = filter_changes(Changes2, Ring),
            State2 = State#state{changes=Changes3},
            {ok, State2};
        Error ->
            {Error, State}
    end.

%% @private
%% @doc Determine how the staged set of cluster changes will affect
%%      the cluster. See {@link plan/0} for additional details.
generate_plan(Ring, State=#state{changes=Changes}) ->
    Changes2 = filter_changes(Changes, Ring),
    Joining = [{Node, join} || Node <- riak_core_ring:members(Ring, [joining])],
    AllChanges = lists:ukeysort(1, Changes2 ++ Joining),
    State2 = State#state{changes=Changes2},
    generate_plan(AllChanges, Ring, State2).

generate_plan([], _, State) ->
    %% There are no changes to apply
    {{ok, [], []}, State};
generate_plan(Changes, Ring, State=#state{seed=Seed}) ->
    case compute_all_next_rings(Changes, Seed, Ring) of
        {error, invalid_resize_claim} ->
            {{error, invalid_resize_claim}, State};
        {ok, NextRings} ->
            {_, NextRing} = hd(NextRings),
            State2 = State#state{next_ring=NextRing},
            Reply = {ok, Changes, NextRings},
            {Reply, State2}
    end.

%% @private
%% @doc Commit the set of staged cluster changes. See {@link commit/0}
%%      for additional details.
commit_staged(State=#state{next_ring=undefined}) ->
    {{error, nothing_planned}, State};
commit_staged(State) ->
    case maybe_commit_staged(State) of
        {ok, _} ->
            State2 = State#state{next_ring=undefined,
                                 changes=[],
                                 seed=erlang:now()},
            {ok, State2};
        not_changed ->
            {error, State};
        {not_changed, Reason} ->
            {{error, Reason}, State}
    end.

%% @private
maybe_commit_staged(State) ->
    riak_core_ring_manager:ring_trans(fun maybe_commit_staged/2, State).

%% @private
maybe_commit_staged(Ring, State=#state{changes=Changes, seed=Seed}) ->
    Changes2 = filter_changes(Changes, Ring),
    case compute_next_ring(Changes2, Seed, Ring) of
        {error, invalid_resize_claim} ->
            {ignore, invalid_resize_claim};
        {ok, NextRing} ->
            maybe_commit_staged(Ring, NextRing, State)
    end.

%% @private
maybe_commit_staged(Ring, NextRing, #state{next_ring=PlannedRing}) ->
    Claimant = riak_core_ring:claimant(Ring),
    IsReady = riak_core_ring:ring_ready(Ring),
    IsClaimant = (Claimant == node()),
    IsSamePlan = same_plan(PlannedRing, NextRing),
    case {IsReady, IsClaimant, IsSamePlan} of
        {false, _, _} ->
            {ignore, ring_not_ready};
        {_, false, _} ->
            ignore;
        {_, _, false} ->
            {ignore, plan_changed};
        _ ->
            NewRing = riak_core_ring:increment_vclock(Claimant, NextRing),
            {new_ring, NewRing}
    end.

%% @private
%% @doc Clear the current set of staged transfers. Since `joining' nodes
%%      are determined based on the node's actual state, rather than a
%%      staged action, the only way to clear pending joins is to remove
%%      the `joining' nodes from the cluster. Used by the public API
%%      call {@link clear/0}.
clear_staged(State) ->
    remove_joining_nodes(),
    State#state{changes=[], seed=erlang:now()}.

%% @private
remove_joining_nodes() ->
    riak_core_ring_manager:ring_trans(fun remove_joining_nodes/2, ok).

%% @private
remove_joining_nodes(Ring, _) ->
    Claimant = riak_core_ring:claimant(Ring),
    IsClaimant = (Claimant == node()),
    Joining = riak_core_ring:members(Ring, [joining]),
    AreJoining = (Joining /= []),
    case IsClaimant and AreJoining of
        false ->
            ignore;
        true ->
            NewRing = remove_joining_nodes_from_ring(Claimant, Joining, Ring),
            {new_ring, NewRing}
    end.

%% @private
remove_joining_nodes_from_ring(Claimant, Joining, Ring) ->
    NewRing =
        lists:foldl(fun(Node, RingAcc) ->
                            riak_core_ring:set_member(Claimant, RingAcc, Node,
                                                      invalid, same_vclock)
                    end, Ring, Joining),
    NewRing2 = riak_core_ring:increment_vclock(Claimant, NewRing),
    NewRing2.

%% @private
valid_request(Node, Action, Changes, Ring) ->
    case Action of
        leave ->
            valid_leave_request(Node, Ring);
        remove ->
            valid_remove_request(Node, Ring);
        {replace, NewNode} ->
            valid_replace_request(Node, NewNode, Changes, Ring);
        {force_replace, NewNode} ->
            valid_force_replace_request(Node, NewNode, Changes, Ring);
        {resize, NewRingSize} ->
            valid_resize_request(NewRingSize, Changes, Ring);
        abort_resize ->
            valid_resize_abort_request(Ring)
    end.

%% @private
valid_leave_request(Node, Ring) ->
    case {riak_core_ring:all_members(Ring),
          riak_core_ring:member_status(Ring, Node)} of
        {_, invalid} ->
            {error, not_member};
        {[Node], _} ->
            {error, only_member};
        {_, valid} ->
            true;
        {_, joining} ->
            true;
        {_, _} ->
            {error, already_leaving}
    end.

%% @private
valid_remove_request(Node, Ring) ->
    IsClaimant = (Node == riak_core_ring:claimant(Ring)),
    case {IsClaimant,
          riak_core_ring:all_members(Ring),
          riak_core_ring:member_status(Ring, Node)} of
        {true, _, _} ->
            {error, is_claimant};
        {_, _, invalid} ->
            {error, not_member};
        {_, [Node], _} ->
            {error, only_member};
        _ ->
            true
    end.

%% @private
valid_replace_request(Node, NewNode, Changes, Ring) ->
    AlreadyReplacement = lists:member(NewNode, existing_replacements(Changes)),
    NewJoining =
        (riak_core_ring:member_status(Ring, NewNode) == joining)
        and (not orddict:is_key(NewNode, Changes)),
    case {riak_core_ring:member_status(Ring, Node),
          AlreadyReplacement,
          NewJoining} of
        {invalid, _, _} ->
            {error, not_member};
        {leaving, _, _} ->
            {error, already_leaving};
        {_, true, _} ->
            {error, already_replacement};
        {_, _, false} ->
            {error, invalid_replacement};
        _ ->
            true
    end.

%% @private
valid_force_replace_request(Node, NewNode, Changes, Ring) ->
    IsClaimant = (Node == riak_core_ring:claimant(Ring)),
    AlreadyReplacement = lists:member(NewNode, existing_replacements(Changes)),
    NewJoining =
        (riak_core_ring:member_status(Ring, NewNode) == joining)
        and (not orddict:is_key(NewNode, Changes)),
    case {IsClaimant,
          riak_core_ring:member_status(Ring, Node),
          AlreadyReplacement,
          NewJoining} of
        {true, _, _, _} ->
            {error, is_claimant};
        {_, invalid, _, _} ->
            {error, not_member};
        {_, _, true, _} ->
            {error, already_replacement};
        {_, _, _, false} ->
            {error, invalid_replacement};
        _ ->
            true
    end.

%% @private
%% restrictions preventing resize along with other operations are temporary
valid_resize_request(NewRingSize, [], Ring) ->
    Capable = riak_core_capability:get({riak_core, resizable_ring}, false),
    IsResizing = riak_core_ring:num_partitions(Ring) =/= NewRingSize,

    %% NOTE/TODO: the checks below are a stop-gap measure to limit the changes
    %%            made by the introduction of ring resizing. future implementation
    %%            should allow applications to register with some flag indicating support
    %%            for dynamic ring, if all registered applications support it
    %%            the cluster is capable. core knowing about search/kv is :(
    ControlRunning = app_helper:get_env(riak_control, enabled, false),
    SearchRunning = app_helper:get_env(riak_search, enabled, false),
    NodeCount = length(riak_core_ring:all_members(Ring)),
    Changes = length(riak_core_ring:pending_changes(Ring)) > 0,
    case {ControlRunning, SearchRunning, Capable, IsResizing, NodeCount, Changes} of
        {false, false, true, true, N, false} when N > 1 -> true;
        {true, _, _, _, _, _} -> {error, control_running};
        {_,  true, _, _, _, _} -> {error, search_running};
        {_, _, false, _, _, _} -> {error, not_capable};
        {_, _, _, false, _, _} -> {error, same_size};
        {_, _, _, _, 1, _} -> {error, single_node};
        {_, _, _, _, _, true} -> {error, pending_changes}
    end.


valid_resize_abort_request(Ring) ->
    IsResizing = riak_core_ring:is_resizing(Ring),
    IsPostResize = riak_core_ring:is_post_resize(Ring),
    case IsResizing andalso not IsPostResize of
        true -> true;
        false -> {error, not_resizing}
    end.

%% @private
%% @doc Filter out any staged changes that are no longer valid. Changes
%%      can become invalid based on other staged changes, or by cluster
%%      changes that bypass the staging system.
filter_changes(Changes, Ring) ->
    orddict:filter(fun(Node, Change) ->
                           filter_changes_pred(Node, Change, Changes, Ring)
                   end, Changes).

%% @private
filter_changes_pred(Node, {Change, NewNode}, Changes, Ring)
  when (Change == replace) or (Change == force_replace) ->
    IsMember = (riak_core_ring:member_status(Ring, Node) /= invalid),
    IsJoining = (riak_core_ring:member_status(Ring, NewNode) == joining),
    NotChanging = (not orddict:is_key(NewNode, Changes)),
    IsMember and IsJoining and NotChanging;
filter_changes_pred(Node, _, _, Ring) ->
    IsMember = (riak_core_ring:member_status(Ring, Node) /= invalid),
    IsMember.

%% @private
existing_replacements(Changes) ->
    [Node || {_, {Change, Node}} <- Changes,
             (Change == replace) or (Change == force_replace)].

%% @private
%% Determine if two rings have logically equal cluster state
same_plan(RingA, RingB) ->
    (riak_core_ring:all_member_status(RingA) == riak_core_ring:all_member_status(RingB)) andalso
    (riak_core_ring:all_owners(RingA) == riak_core_ring:all_owners(RingB)) andalso
    (riak_core_ring:pending_changes(RingA) == riak_core_ring:pending_changes(RingB)).

schedule_tick() ->
    Tick = app_helper:get_env(riak_core,
                              claimant_tick,
                              10000),
    erlang:send_after(Tick, ?MODULE, tick).

tick(State=#state{last_ring_id=LastID}) ->
    maybe_enable_ensembles(),
    case riak_core_ring_manager:get_ring_id() of
        LastID ->
            schedule_tick(),
            State;
        RingID ->
            {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
            maybe_bootstrap_root_ensemble(Ring),
            maybe_force_ring_update(Ring),
            schedule_tick(),
            State#state{last_ring_id=RingID}
    end.

maybe_force_ring_update(Ring) ->
    IsClaimant = (riak_core_ring:claimant(Ring) == node()),
    IsReady = riak_core_ring:ring_ready(Ring),
    %% Do not force if we have any joining nodes unless any of them are
    %% auto-joining nodes. Otherwise, we will force update continuously.
    JoinBlock = (are_joining_nodes(Ring)
                 andalso (auto_joining_nodes(Ring) == [])),
    case IsClaimant and IsReady and (not JoinBlock) of
        true ->
            do_maybe_force_ring_update(Ring);
        false ->
            ok
    end.

do_maybe_force_ring_update(Ring) ->
    case compute_next_ring([], erlang:now(), Ring) of
        {ok, NextRing} ->
            case same_plan(Ring, NextRing) of
                false ->
                    lager:warning("Forcing update of stalled ring"),
                    riak_core_ring_manager:force_update();
                true ->
                    ok
            end;
        _ ->
            ok
    end.

%% @private
can_create_type(BucketType, undefined, Props) ->
    riak_core_bucket_props:validate(create, {BucketType, undefined}, undefined, Props);
can_create_type(BucketType, Existing, Props) ->
    Active = type_active(Existing),
    Claimed = node() =:= type_claimant(Existing),
    case {Active, Claimed} of
        %% if type is not active and this claimant has claimed it
        %% then we can re-create the type.
        {false, true} -> riak_core_bucket_props:validate(create, {BucketType, undefined},
                                                         Existing, Props);
        {true, _} -> {error, already_active};
        {_, false} -> {error, not_claimed}
    end.

%% @private
can_update_type(_BucketType, undefined, _Props) ->
    {error, undefined};
can_update_type(BucketType, Existing, Props) ->
    case type_active(Existing) of
        true -> riak_core_bucket_props:validate(update, {BucketType, undefined},
                                                Existing, Props);
        false -> {error, not_active}
    end.

%% @private
get_type_status(_BucketType, undefined) ->
    undefined;
get_type_status(BucketType, Props) ->
    case type_active(Props) of
        true -> active;
        false -> get_remote_type_status(BucketType, Props)
    end.

%% @private
get_remote_type_status(BucketType, Props) ->
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    Members = riak_core_ring:all_members(R),
    {AllProps, BadNodes} = rpc:multicall(lists:delete(node(), Members),
                                         riak_core_metadata,
                                         get, [?BUCKET_TYPE_PREFIX, BucketType, [{default, []}]]),
    SortedProps = lists:ukeysort(1, Props),
    %% P may be a {badrpc, ...} in addition to a list of properties when there are older nodes involved
    DiffProps = [P || P <- AllProps, (not is_list(P) orelse lists:ukeysort(1, P) =/= SortedProps)],
    case {DiffProps, BadNodes} of
        {[], []} -> ready;
        %% unreachable nodes may or may not have correct value, so we assume they dont
        {_, _} -> created
    end.

%% @private
maybe_activate_type(_BucketType, undefined, _Props) ->
    {error, undefined};
maybe_activate_type(_BucketType, created, _Props) ->
    {error, not_ready};
maybe_activate_type(_BucketType, active, _Props) ->
    ok;
maybe_activate_type(BucketType, ready, Props) ->
    ActiveProps = lists:keystore(active, 1, Props, {active, true}),
    riak_core_metadata:put(?BUCKET_TYPE_PREFIX, BucketType, ActiveProps).

%% @private
type_active(Props) ->
    {active, true} =:= lists:keyfind(active, 1, Props).

%% @private
type_claimant(Props) ->
    case lists:keyfind(claimant, 1, Props) of
        {claimant, Claimant} -> Claimant;
        false -> undefined
    end.

%% The consensus subsystem must be enabled by exactly one node in a cluster
%% via a call to riak_ensemble_manager:enable(). We accomplished this by
%% having the claimant be that one node. Likewise, we require that the cluster
%% includes at least three nodes before we enable consensus. This prevents the
%% claimant in a 1-node cluster from enabling consensus before being joined to
%% another cluster.
maybe_enable_ensembles() ->
    Desired = riak_core_sup:ensembles_enabled(),
    Enabled = riak_ensemble_manager:enabled(),
    case Enabled of
        Desired ->
            ok;
        _ ->
            {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
            IsReady = riak_core_ring:ring_ready(Ring),
            IsClaimant = (riak_core_ring:claimant(Ring) == node()),
            EnoughNodes = (length(riak_core_ring:ready_members(Ring)) >= 3),
            case IsReady and IsClaimant and EnoughNodes of
                true ->
                    enable_ensembles(Ring);
                false ->
                    ok
            end
    end.

%% We need to avoid a race where the current claimant enables consensus right
%% before going offline and being replaced by a new claimant. It could be
%% argued that this corner case is not important since changing the claimant
%% requires the user manually marking the current claimant as down. But, it's
%% better to be safe and handle things correctly.
%%
%% To solve this issue, the claimant first marks itself as the "ensemble
%% singleton" in the ring metadata. Once the ring has converged, the claimant
%% see that it previously marked itself as the singleton and will proceed to
%% enable the consensus subsystem. If the claimant goes offline after marking
%% itself the singleton, but before enabling consensus, then future claimants
%% will be unable to enable consensus. Consensus will be enabled once the
%% previous claimant comes back online.
%%
enable_ensembles(Ring) ->
    Node = node(),
    case ensemble_singleton(Ring) of
        undefined ->
            become_ensemble_singleton();
        Node ->
            %% Ring update is required after enabling consensus to ensure
            %% that ensembles are properly bootstrapped.
            riak_ensemble_manager:enable(),
            riak_core_ring_manager:force_update(),
            lager:info("Activated consensus subsystem for cluster");
        _ ->
            ok
    end.

ensemble_singleton(Ring) ->
    case riak_core_ring:get_meta('$ensemble_singleton', Ring) of
        undefined ->
            undefined;
        {ok, Node} ->
            Members = riak_core_ring:all_members(Ring),
            case lists:member(Node, Members) of
                true ->
                    Node;
                false ->
                    undefined
            end
    end.

become_ensemble_singleton() ->
    _ = riak_core_ring_manager:ring_trans(fun become_ensemble_singleton_trans/2,
                                          undefined),
    ok.

become_ensemble_singleton_trans(Ring, _) ->
    IsClaimant = (riak_core_ring:claimant(Ring) == node()),
    NoSingleton = (ensemble_singleton(Ring) =:= undefined),
    case IsClaimant and NoSingleton of
        true ->
            Ring2 = riak_core_ring:update_meta('$ensemble_singleton', node(), Ring),
            {new_ring, Ring2};
        false ->
            ignore
    end.

maybe_bootstrap_root_ensemble(Ring) ->
    IsEnabled = riak_ensemble_manager:enabled(),
    IsClaimant = (riak_core_ring:claimant(Ring) == node()),
    IsReady = riak_core_ring:ring_ready(Ring),
    case IsEnabled and IsClaimant and IsReady of
        true ->
            bootstrap_root_ensemble(Ring);
        false ->
            ok
    end.

bootstrap_root_ensemble(Ring) ->
    bootstrap_members(Ring),
    ok.

bootstrap_members(Ring) ->
    Name = riak_core_ring:cluster_name(Ring),
    Members = riak_core_ring:ready_members(Ring),
    RootMembers = riak_ensemble_manager:get_members(root),
    Known = riak_ensemble_manager:cluster(),
    Need = Members -- Known,
    L = [riak_core_util:proxy_spawn(
            fun() -> riak_ensemble_manager:join(node(), Member) end
        ) || Member <- Need, Member =/= node()],
    _ = maybe_reset_ring_id(L),

    RootNodes = [Node || {_, Node} <- RootMembers],
    RootAdd = Members -- RootNodes,
    RootDel = RootNodes -- Members,

    Res = [riak_core_util:proxy_spawn(
              fun() -> riak_ensemble_manager:remove(node(), N) end
           ) || N <- RootDel, N =/= node()],
    _ = maybe_reset_ring_id(Res),

    Changes =
        [{add, {Name, Node}} || Node <- RootAdd] ++
        [{del, {Name, Node}} || Node <- RootDel],
    case Changes of
        [] ->
            ok;
        _ ->
            Self = self(),
            spawn_link(fun() ->
                               async_bootstrap_members(Self, Changes)
                       end),
            ok
    end.

async_bootstrap_members(Claimant, Changes) ->
    RootLeader = riak_ensemble_manager:rleader_pid(),
    case riak_ensemble_peer:update_members(RootLeader, Changes, 10000) of
        ok ->
            ok;
        _ ->
            reset_ring_id(Claimant),
            ok
    end.

maybe_reset_ring_id(Results) ->
    Failed = [R || R <- Results, R =/= ok],
    (Failed =:= []) orelse reset_ring_id(self()).

%% Reset last_ring_id, ensuring future tick re-examines the ring even if the
%% ring has not changed.
reset_ring_id(Pid) ->
    Pid ! reset_ring_id.

%% =========================================================================
%% Claimant rebalance/reassign logic
%% =========================================================================

%% @private
compute_all_next_rings(Changes, Seed, Ring) ->
    compute_all_next_rings(Changes, Seed, Ring, []).

%% @private
compute_all_next_rings(Changes, Seed, Ring, Acc) ->
    case compute_next_ring(Changes, Seed, Ring) of
        {error, invalid_resize_claim}=Err ->
            Err;
        {ok, NextRing} ->
            Acc2 = [{Ring, NextRing}|Acc],
            case not same_plan(Ring, NextRing) of
                true ->
                    FutureRing = riak_core_ring:future_ring(NextRing),
                    compute_all_next_rings([], Seed, FutureRing, Acc2);
                false ->
                    {ok, lists:reverse(Acc2)}
            end
    end.

%% @private
compute_next_ring(Changes, Seed, Ring) ->
    Replacing = [{Node, NewNode} || {Node, {replace, NewNode}} <- Changes],

    Ring2 = apply_changes(Ring, Changes),
    {_, Ring3} = maybe_handle_joining(node(), Ring2),
    {_, Ring4} = do_claimant_quiet(node(), Ring3, Replacing, Seed),
    {Valid, Ring5} = maybe_compute_resize(Ring, Ring4),
    case Valid of
        false ->
            {error, invalid_resize_claim};
        true ->
            {ok, Ring5}
    end.

%% @private
maybe_compute_resize(Orig, MbResized) ->
    OrigSize = riak_core_ring:num_partitions(Orig),
    NewSize = riak_core_ring:num_partitions(MbResized),

    case OrigSize =/= NewSize of
        false -> {true, MbResized};
        true -> validate_resized_ring(compute_resize(Orig, MbResized))
    end.

%% @private
%% @doc Adjust resized ring and schedule first resize transfers.
%% Because riak_core_ring:resize/2 modifies the chash structure
%% directly the ring calculated in this plan (`Resized') is used
%% to determine the future ring but the changes are applied to
%% the currently installed ring (`Orig') so that the changes to
%% the chash are not committed to the ring manager
compute_resize(Orig, Resized) ->
    %% need to operate on balanced, future ring (apply changes determined by claim)
    CState0 = riak_core_ring:future_ring(Resized),

    Type = case riak_core_ring:num_partitions(Orig) < riak_core_ring:num_partitions(Resized) of
        true -> larger;
        false -> smaller
    end,

    %% Each index in the original ring must perform several transfers
    %% to properly resize the ring. The first transfer for each index
    %% is scheduled here. Subsequent transfers are scheduled by vnode
    CState1 = lists:foldl(fun({Idx, _} = IdxOwner, CStateAcc) ->
                                  %% indexes being abandoned in a shrinking ring have
                                  %% no next owner
                                  NextOwner = try riak_core_ring:index_owner(CStateAcc, Idx)
                                              catch error:{badmatch, false} -> none
                                              end,
                                  schedule_first_resize_transfer(Type,
                                                                 IdxOwner,
                                                                 NextOwner,
                                                                 CStateAcc)
                          end,
                          CState0,
                          riak_core_ring:all_owners(Orig)),

    riak_core_ring:set_pending_resize(CState1, Orig).

%% @private
%% @doc determine the first resize transfer a partition should perform with
%% the goal of ensuring the transfer will actually have data to send to the
%% target.
schedule_first_resize_transfer(smaller, {Idx,_}=IdxOwner, none, Resized) ->
    %% partition no longer exists in shrunk ring, first successor will be
    %% new owner of its data
    Target = hd(riak_core_ring:preflist(<<Idx:160/integer>>, Resized)),
    riak_core_ring:schedule_resize_transfer(Resized, IdxOwner, Target);
schedule_first_resize_transfer(_Type,{Idx, Owner}=IdxOwner, Owner, Resized) ->
    %% partition is not being moved during expansion, first predecessor will
    %% own at least a portion of its data
    Target = hd(chash:predecessors(Idx-1, riak_core_ring:chash(Resized))),
    riak_core_ring:schedule_resize_transfer(Resized, IdxOwner, Target);
schedule_first_resize_transfer(_,{Idx, _Owner}=IdxOwner, NextOwner, Resized) ->
    %% partition is being moved during expansion, schedule transfer to partition
    %% on new owner since it will still own some of its data
    riak_core_ring:schedule_resize_transfer(Resized, IdxOwner, {Idx, NextOwner}).

%% @doc verify that resized ring was properly claimed (no owners are the dummy
%%      resized owner) in both the current and future ring
validate_resized_ring(Ring) ->
    FutureRing = riak_core_ring:future_ring(Ring),
    Owners = riak_core_ring:all_owners(Ring),
    FutureOwners = riak_core_ring:all_owners(FutureRing),
    Members = riak_core_ring:all_members(Ring),
    FutureMembers = riak_core_ring:all_members(FutureRing),
    Invalid1 = [{Idx, Owner} || {Idx, Owner} <- Owners,
                               not lists:member(Owner, Members)],
    Invalid2 = [{Idx, Owner} || {Idx, Owner} <- FutureOwners,
                                not lists:member(Owner, FutureMembers)],
    case Invalid1 ++ Invalid2 of
        [] ->
            {true, Ring};
        _ ->
            {false, Ring}
    end.

%% @private
apply_changes(Ring, Changes) ->
    NewRing =
        lists:foldl(
          fun({Node, Cmd}, RingAcc2) ->
                  RingAcc3 = change({Cmd, Node}, RingAcc2),
                  RingAcc3
          end, Ring, Changes),
    NewRing.

%% @private
change({join, Node}, Ring) ->
    Ring2 = riak_core_ring:add_member(Node, Ring, Node),
    Ring2;
change({leave, Node}, Ring) ->
    Members = riak_core_ring:all_members(Ring),
    lists:member(Node, Members) orelse throw(invalid_member),
    Ring2 = riak_core_ring:leave_member(Node, Ring, Node),
    Ring2;
change({remove, Node}, Ring) ->
    Members = riak_core_ring:all_members(Ring),
    lists:member(Node, Members) orelse throw(invalid_member),
    Ring2 = riak_core_ring:remove_member(Node, Ring, Node),
    Ring2;
change({{replace, _NewNode}, Node}, Ring) ->
    %% Just treat as a leave, reassignment happens elsewhere
    Ring2 = riak_core_ring:leave_member(Node, Ring, Node),
    Ring2;
change({{force_replace, NewNode}, Node}, Ring) ->
    Indices = riak_core_ring:indices(Ring, Node),
    Reassign = [{Idx, NewNode} || Idx <- Indices],
    Ring2 = riak_core_ring:add_member(NewNode, Ring, NewNode),
    Ring3 = riak_core_ring:change_owners(Ring2, Reassign),
    Ring4 = riak_core_ring:remove_member(Node, Ring3, Node),
    case riak_core_ring:is_resizing(Ring4) of
        true -> replace_node_during_resize(Ring4, Node, NewNode);
        false -> Ring4
    end;
change({{resize, NewRingSize}, _Node}, Ring) ->
    riak_core_ring:resize(Ring, NewRingSize);
change({abort_resize, _Node}, Ring) ->
    riak_core_ring:set_pending_resize_abort(Ring).

internal_ring_changed(Node, CState) ->
    {Changed, CState5} = do_claimant(Node, CState, fun log/2),
    inform_removed_nodes(Node, CState, CState5),

    %% Start/stop converge and rebalance delay timers
    %% (converge delay)
    %%   -- Starts when claimant changes the ring
    %%   -- Stops when the ring converges (ring_ready)
    %% (rebalance delay)
    %%   -- Starts when next changes from empty to non-empty
    %%   -- Stops when next changes from non-empty to empty
    %%
    IsClaimant = (riak_core_ring:claimant(CState5) =:= Node),
    WasPending = ([] /= riak_core_ring:pending_changes(CState)),
    IsPending  = ([] /= riak_core_ring:pending_changes(CState5)),

    %% Outer case statement already checks for ring_ready
    case {IsClaimant, Changed} of
        {true, true} ->
            riak_core_stat:update(converge_timer_end),
            riak_core_stat:update(converge_timer_begin);
        {true, false} ->
            riak_core_stat:update(converge_timer_end);
        _ ->
            ok
    end,

    case {IsClaimant, WasPending, IsPending} of
        {true, false, true} ->
            riak_core_stat:update(rebalance_timer_begin);
        {true, true, false} ->
            riak_core_stat:update(rebalance_timer_end);
        _ ->
            ok
    end,

    %% Set cluster name if it is undefined
    case {IsClaimant, riak_core_ring:cluster_name(CState5)} of
        {true, undefined} ->
            ClusterName = {Node, erlang:now()},
            {_,_} = riak_core_util:rpc_every_member(riak_core_ring_manager,
                                                    set_cluster_name,
                                                    [ClusterName],
                                                    1000),
            ok;
        _ ->
            ClusterName = riak_core_ring:cluster_name(CState5),
            ok
    end,

    case Changed of
        true ->
            CState6 = riak_core_ring:set_cluster_name(CState5, ClusterName),
            riak_core_ring:increment_vclock(Node, CState6);
        false ->
            CState5
    end.

inform_removed_nodes(Node, OldRing, NewRing) ->
    CName = riak_core_ring:cluster_name(NewRing),
    Exiting = riak_core_ring:members(OldRing, [exiting]) -- [Node],
    Invalid = riak_core_ring:members(NewRing, [invalid]),
    Changed = ordsets:intersection(ordsets:from_list(Exiting),
                                   ordsets:from_list(Invalid)),
    %% Tell exiting node to shutdown.
    _ = [riak_core_ring_manager:refresh_ring(ExitingNode, CName) ||
            ExitingNode <- Changed],
    ok.

do_claimant_quiet(Node, CState, Replacing, Seed) ->
    do_claimant(Node, CState, Replacing, Seed, fun no_log/2).

do_claimant(Node, CState, Log) ->
    do_claimant(Node, CState, [], erlang:now(), Log).

do_claimant(Node, CState, Replacing, Seed, Log) ->
    AreJoining = are_joining_nodes(CState),
    {C1, CState2} = maybe_update_claimant(Node, CState),
    {C2, CState3} = maybe_handle_auto_joining(Node, CState2),
    case AreJoining of
        true ->
            %% Do not rebalance if there are joining nodes
            Changed = C1 or C2,
            CState5 = CState3;
        false ->
            {C3, CState4} =
                maybe_update_ring(Node, CState3, Replacing, Seed, Log),
            {C4, CState5} = maybe_remove_exiting(Node, CState4),
            Changed = (C1 or C2 or C3 or C4)
    end,
    {Changed, CState5}.

%% @private
maybe_update_claimant(Node, CState) ->
    Members = riak_core_ring:members(CState, [valid, leaving]),
    Claimant = riak_core_ring:claimant(CState),
    NextClaimant = hd(Members ++ [undefined]),
    ClaimantMissing = not lists:member(Claimant, Members),

    case {ClaimantMissing, NextClaimant} of
        {true, Node} ->
            %% Become claimant
            CState2 = riak_core_ring:set_claimant(CState, Node),
            CState3 = riak_core_ring:increment_ring_version(Claimant, CState2),
            {true, CState3};
        _ ->
            {false, CState}
    end.

%% @private
maybe_update_ring(Node, CState, Replacing, Seed, Log) ->
    Claimant = riak_core_ring:claimant(CState),
    case Claimant of
        Node ->
            case riak_core_ring:claiming_members(CState) of
                [] ->
                    %% Consider logging an error/warning here or even
                    %% intentionally crashing. This state makes no logical
                    %% sense given that it represents a cluster without any
                    %% active nodes.
                    {false, CState};
                _ ->
                    Resizing = riak_core_ring:is_resizing(CState),
                    {Changed, CState2} =
                        update_ring(Node, CState, Replacing, Seed, Log, Resizing),
                    {Changed, CState2}
            end;
        _ ->
            {false, CState}
    end.

%% @private
maybe_remove_exiting(Node, CState) ->
    Claimant = riak_core_ring:claimant(CState),
    case Claimant of
        Node ->
            %% Change exiting nodes to invalid, skipping this node.
            Exiting = riak_core_ring:members(CState, [exiting]) -- [Node],
            RootMembers = riak_ensemble_manager:get_members(root),
            CState2 =
                lists:foldl(fun(ENode, CState0) ->
                              L = [N || {_, N} <- RootMembers, N =:= ENode],
                              case L of
                                  [] ->
                                      ClearedCS =
                                          riak_core_ring:clear_member_meta(Node, CState0, ENode),
                                      riak_core_ring:set_member(Node, ClearedCS, ENode,
                                                                invalid, same_vclock);
                                  _ ->
                                      reset_ring_id(self()),
                                      CState0
                              end
                            end, CState, Exiting),
            Changed = (CState2 /= CState),
            {Changed, CState2};
        _ ->
            {false, CState}
    end.

%% @private
are_joining_nodes(CState) ->
    Joining = riak_core_ring:members(CState, [joining]),
    Joining /= [].

%% @private
auto_joining_nodes(CState) ->
    Joining = riak_core_ring:members(CState, [joining]),
    case riak_core_capability:get({riak_core, staged_joins}, false) of
        false ->
            Joining;
        true ->
            [Member || Member <- Joining,
                       riak_core_ring:get_member_meta(CState,
                                                      Member,
                                                      '$autojoin') == true]
    end.

%% @private
maybe_handle_auto_joining(Node, CState) ->
    Auto = auto_joining_nodes(CState),
    maybe_handle_joining(Node, Auto, CState).

%% @private
maybe_handle_joining(Node, CState) ->
    Joining = riak_core_ring:members(CState, [joining]),
    maybe_handle_joining(Node, Joining, CState).

%% @private
maybe_handle_joining(Node, Joining, CState) ->
    Claimant = riak_core_ring:claimant(CState),
    case Claimant of
        Node ->
            Changed = (Joining /= []),
            CState2 =
                lists:foldl(fun(JNode, CState0) ->
                                    riak_core_ring:set_member(Node, CState0, JNode,
                                                              valid, same_vclock)
                            end, CState, Joining),
            {Changed, CState2};
        _ ->
            {false, CState}
    end.

%% @private
update_ring(CNode, CState, Replacing, Seed, Log, false) ->
    Next0 = riak_core_ring:pending_changes(CState),

    ?ROUT("Members: ~p~n", [riak_core_ring:members(CState, [joining, valid,
                                                            leaving, exiting,
                                                            invalid])]),
    ?ROUT("Updating ring :: next0 : ~p~n", [Next0]),

    %% Remove tuples from next for removed nodes
    InvalidMembers = riak_core_ring:members(CState, [invalid]),
    Next2 = lists:filter(fun(NInfo) ->
                                 {Owner, NextOwner, _} = riak_core_ring:next_owner(NInfo),
                                 not lists:member(Owner, InvalidMembers) and
                                 not lists:member(NextOwner, InvalidMembers)
                         end, Next0),
    CState2 = riak_core_ring:set_pending_changes(CState, Next2),

    %% Transfer ownership after completed handoff
    {RingChanged1, CState3} = transfer_ownership(CState2, Log),
    ?ROUT("Updating ring :: next1 : ~p~n",
          [riak_core_ring:pending_changes(CState3)]),

    %% Ressign leaving/inactive indices
    {RingChanged2, CState4} = reassign_indices(CState3, Replacing, Seed, Log),
    ?ROUT("Updating ring :: next2 : ~p~n",
          [riak_core_ring:pending_changes(CState4)]),

    %% Rebalance the ring as necessary. If pending changes exist ring
    %% is not rebalanced
    Next3 = rebalance_ring(CNode, CState4),
    Log(debug,{"Pending ownership transfers: ~b~n",
               [length(riak_core_ring:pending_changes(CState4))]}),

    %% Remove transfers to/from down nodes
    Next4 = handle_down_nodes(CState4, Next3),

    NextChanged = (Next0 /= Next4),
    Changed = (NextChanged or RingChanged1 or RingChanged2),
    case Changed of
        true ->
            OldS = ordsets:from_list([{Idx,O,NO} || {Idx,O,NO,_,_} <- Next0]),
            NewS = ordsets:from_list([{Idx,O,NO} || {Idx,O,NO,_,_} <- Next4]),
            Diff = ordsets:subtract(NewS, OldS),
            _ = [Log(next, NChange) || NChange <- Diff],
            ?ROUT("Updating ring :: next3 : ~p~n", [Next4]),
            CState5 = riak_core_ring:set_pending_changes(CState4, Next4),
            CState6 = riak_core_ring:increment_ring_version(CNode, CState5),
            {true, CState6};
        false ->
            {false, CState}
    end;
update_ring(CNode, CState, _Replacing, _Seed, _Log, true) ->
    {Installed, CState1} = maybe_install_resized_ring(CState),
    {Aborted, CState2} = riak_core_ring:maybe_abort_resize(CState1),
    Changed = Installed orelse Aborted,
    case Changed of
        true ->
            CState3 = riak_core_ring:increment_ring_version(CNode, CState2),
            {true, CState3};
        false ->
            {false, CState}
    end.

maybe_install_resized_ring(CState) ->
    case riak_core_ring:is_resize_complete(CState) of
        true ->
            {true, riak_core_ring:future_ring(CState)};
        false -> {false, CState}
    end.

%% @private
transfer_ownership(CState, Log) ->
    Next = riak_core_ring:pending_changes(CState),
    %% Remove already completed and transfered changes
    Next2 = lists:filter(fun(NInfo={Idx, _, _, _, _}) ->
                                 {_, NewOwner, S} = riak_core_ring:next_owner(NInfo),
                                 not ((S == complete) and
                                      (riak_core_ring:index_owner(CState, Idx) =:= NewOwner))
                         end, Next),

    CState2 = lists:foldl(
                fun(NInfo={Idx, _, _, _, _}, CState0) ->
                        case riak_core_ring:next_owner(NInfo) of
                            {_, Node, complete} ->
                                Log(ownership, {Idx, Node, CState0}),
                                riak_core_ring:transfer_node(Idx, Node,
                                                             CState0);
                            _ ->
                                CState0
                        end
                end, CState, Next2),

    NextChanged = (Next2 /= Next),
    RingChanged = (riak_core_ring:all_owners(CState) /= riak_core_ring:all_owners(CState2)),
    Changed = (NextChanged or RingChanged),
    CState3 = riak_core_ring:set_pending_changes(CState2, Next2),
    {Changed, CState3}.


%% @private
reassign_indices(CState, Replacing, Seed, Log) ->
    Next = riak_core_ring:pending_changes(CState),
    Invalid = riak_core_ring:members(CState, [invalid]),
    CState2 =
        lists:foldl(fun(Node, CState0) ->
                            remove_node(CState0, Node, invalid,
                                        Replacing, Seed, Log)
                    end, CState, Invalid),
    CState3 = case Next of
                  [] ->
                      Leaving = riak_core_ring:members(CState, [leaving]),
                      lists:foldl(fun(Node, CState0) ->
                                          remove_node(CState0, Node, leaving,
                                                      Replacing, Seed, Log)
                                  end, CState2, Leaving);
                  _ ->
                      CState2
              end,
    Owners1 = riak_core_ring:all_owners(CState),
    Owners2 = riak_core_ring:all_owners(CState3),
    RingChanged = (Owners1 /= Owners2),
    NextChanged = (Next /= riak_core_ring:pending_changes(CState3)),
    {RingChanged or NextChanged, CState3}.

%% @private
rebalance_ring(CNode, CState) ->
    Next = riak_core_ring:pending_changes(CState),
    rebalance_ring(CNode, Next, CState).

rebalance_ring(_CNode, [], CState) ->
    CState2 = riak_core_claim:claim(CState),
    Owners1 = riak_core_ring:all_owners(CState),
    Owners2 = riak_core_ring:all_owners(CState2),
    Owners3 = lists:zip(Owners1, Owners2),
    Next = [{Idx, PrevOwner, NewOwner, [], awaiting}
            || {{Idx, PrevOwner}, {Idx, NewOwner}} <- Owners3,
               PrevOwner /= NewOwner],
    Next;
rebalance_ring(_CNode, Next, _CState) ->
    Next.

%% @private
handle_down_nodes(CState, Next) ->
    LeavingMembers = riak_core_ring:members(CState, [leaving, invalid]),
    DownMembers = riak_core_ring:members(CState, [down]),
    Next2 = [begin
                 OwnerLeaving = lists:member(O, LeavingMembers),
                 NextDown = lists:member(NO, DownMembers),
                 case (OwnerLeaving and NextDown) of
                     true ->
                         Active = riak_core_ring:active_members(CState) -- [O],
                         RNode = lists:nth(random:uniform(length(Active)),
                                           Active),
                         {Idx, O, RNode, Mods, Status};
                     _ ->
                         T
                 end
             end || T={Idx, O, NO, Mods, Status} <- Next],
    Next3 = [T || T={_, O, NO, _, _} <- Next2,
                  not lists:member(O, DownMembers),
                  not lists:member(NO, DownMembers)],
    Next3.

%% @private
reassign_indices_to(Node, NewNode, Ring) ->
    Indices = riak_core_ring:indices(Ring, Node),
    Reassign = [{Idx, NewNode} || Idx <- Indices],
    Ring2 = riak_core_ring:change_owners(Ring, Reassign),
    Ring2.

%% @private
remove_node(CState, Node, Status, Replacing, Seed, Log) ->
    Indices = riak_core_ring:indices(CState, Node),
    remove_node(CState, Node, Status, Replacing, Seed, Log, Indices).

%% @private
remove_node(CState, _Node, _Status, _Replacing, _Seed, _Log, []) ->
    CState;
remove_node(CState, Node, Status, Replacing, Seed, Log, Indices) ->
    CStateT1 = riak_core_ring:change_owners(CState,
                                            riak_core_ring:all_next_owners(CState)),
    case orddict:find(Node, Replacing) of
        {ok, NewNode} ->
            CStateT2 = reassign_indices_to(Node, NewNode, CStateT1);
        error ->
            CStateT2 = riak_core_gossip:remove_from_cluster(CStateT1, Node, Seed)
    end,

    Owners1 = riak_core_ring:all_owners(CState),
    Owners2 = riak_core_ring:all_owners(CStateT2),
    Owners3 = lists:zip(Owners1, Owners2),
    RemovedIndices = case Status of
                         invalid ->
                             Indices;
                         leaving ->
                             []
                     end,
    Reassign = [{Idx, NewOwner} || {Idx, NewOwner} <- Owners2,
                                   lists:member(Idx, RemovedIndices)],
    Next = [{Idx, PrevOwner, NewOwner, [], awaiting}
            || {{Idx, PrevOwner}, {Idx, NewOwner}} <- Owners3,
               PrevOwner /= NewOwner,
               not lists:member(Idx, RemovedIndices)],

    _ = [Log(reassign, {Idx, NewOwner, CState}) || {Idx, NewOwner} <- Reassign],

    %% Unlike rebalance_ring, remove_node can be called when Next is non-empty,
    %% therefore we need to merge the values. Original Next has priority.
    Next2 = lists:ukeysort(1, riak_core_ring:pending_changes(CState) ++ Next),
    CState2 = riak_core_ring:change_owners(CState, Reassign),
    CState3 = riak_core_ring:set_pending_changes(CState2, Next2),
    CState3.

replace_node_during_resize(CState0, Node, NewNode) ->
    PostResize = riak_core_ring:is_post_resize(CState0),
    CState1 = replace_node_during_resize(CState0, Node, NewNode, PostResize),
    riak_core_ring:increment_ring_version(riak_core_ring:claimant(CState1), CState1).

replace_node_during_resize(CState0, Node, NewNode, false) -> %% ongoing xfers
    %% for each of the indices being moved from Node to NewNode, reschedule resize
    %% transfers where the target is owned by Node.
    CState1 = riak_core_ring:reschedule_resize_transfers(CState0, Node, NewNode),

    %% since the resized chash is carried directly in state vs. being rebuilt via next
    %% list, perform reassignment
    {ok, FutureCHash} = riak_core_ring:resized_ring(CState1),
    FutureCState = riak_core_ring:set_chash(CState1, FutureCHash),
    ReassignedFuture = reassign_indices_to(Node, NewNode, FutureCState),
    ReassignedCHash = riak_core_ring:chash(ReassignedFuture),
    riak_core_ring:set_resized_ring(CState1, ReassignedCHash);
replace_node_during_resize(CState, Node, _NewNode, true) -> %% performing cleanup
    %% we are simply deleting data at this point, no reason to do that on either node
    NewNext = [{I,N,O,M,S} || {I,N,O,M,S} <- riak_core_ring:pending_changes(CState),
                              N =/= Node],
    riak_core_ring:set_pending_changes(CState, NewNext).

no_log(_, _) ->
    ok.

log(debug, {Msg, Args}) ->
    lager:debug(Msg, Args);
log(ownership, {Idx, NewOwner, CState}) ->
    Owner = riak_core_ring:index_owner(CState, Idx),
    lager:debug("(new-owner) ~b :: ~p -> ~p~n", [Idx, Owner, NewOwner]);
log(reassign, {Idx, NewOwner, CState}) ->
    Owner = riak_core_ring:index_owner(CState, Idx),
    lager:debug("(reassign) ~b :: ~p -> ~p~n", [Idx, Owner, NewOwner]);
log(next, {Idx, Owner, NewOwner}) ->
    lager:debug("(pending) ~b :: ~p -> ~p~n", [Idx, Owner, NewOwner]);
log(_, _) ->
    ok.
