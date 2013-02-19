%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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

%% API
-export([start_link/0]).
-export([leave_member/1,
         remove_member/1,
         force_replace/2,
         replace/2,
         resize_ring/1,
         plan/0,
         commit/0,
         clear/0,
         ring_changed/2]).
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

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    State2 = tick(State),
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
        legacy ->
            {{error, legacy}, State};
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
        {legacy, _} ->
            {ignore, legacy};
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
            valid_resize_request(NewRingSize, Changes, Ring)
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
    case {Capable, IsResizing} of
        {true, true} -> true;
        {false, _} -> {error, not_capable};
        {_, false} -> {error, same_size}
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
filter_changes_pred(_, {resize, _}, _, _) ->
    true;
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

tick(State) ->
    maybe_force_ring_update(),
    schedule_tick(),
    State.

maybe_force_ring_update() ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    IsClaimant = (riak_core_ring:claimant(Ring) == node()),
    IsReady = riak_core_ring:ring_ready(Ring),
    %% Do not force if we have any joining nodes unless any of them are
    %% auto-joining nodes. Otherwise, we will force update continuously.
    JoinBlock = (are_joining_nodes(Ring)
                 andalso (auto_joining_nodes(Ring) == [])),
    case IsClaimant and IsReady and (not JoinBlock) of
        true ->
            maybe_force_ring_update(Ring);
        false ->
            ok
    end.

maybe_force_ring_update(Ring) ->
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

%% =========================================================================
%% Claimant rebalance/reassign logic
%% =========================================================================

%% @private
compute_all_next_rings(Changes, Seed, Ring) ->
    compute_all_next_rings(Changes, Seed, Ring, []).

%% @private
compute_all_next_rings(Changes, Seed, Ring, Acc) ->
    case compute_next_ring(Changes, Seed, Ring) of
        {legacy, _} ->
            legacy;
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
    Ring5 = maybe_compute_resize(Ring, Ring4),
    Members = riak_core_ring:all_members(Ring5),
    case riak_core_gossip:any_legacy_gossip(Ring5, Members) of
        true ->
            {legacy, Ring};
        false ->
            {ok, Ring5}
    end.

%% @private
maybe_compute_resize(Orig, MbResized) ->
    OrigSize = riak_core_ring:num_partitions(Orig),
    NewSize = riak_core_ring:num_partitions(MbResized),

    case OrigSize =/= NewSize of
        false -> MbResized;
        true -> compute_resize(Orig, MbResized)
    end.

%% @private
compute_resize(Orig, Resized) ->
    %% work with the resized and balanced ring, the
    %% original ring will be reset when changes are scheduled
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
                                              catch error:badarg -> none
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
schedule_first_resize_transfer(smaller, {Idx,_}=IdxOwner, none, Resized) ->
    %% partition no longer exists in new ring, use first successor
    Target = hd(riak_core_ring:preflist(<<Idx:160/integer>>, Resized)),
    riak_core_ring:schedule_resize_transfer(Resized, IdxOwner, Target);
schedule_first_resize_transfer(_Type,{Idx, Owner}=IdxOwner, Owner, Resized) ->
    %% partition is not being moved during resizing, use first predecessor
    Target = hd(chash:predecessors(Idx-1, riak_core_ring:chash(Resized))),
    riak_core_ring:schedule_resize_transfer(Resized, IdxOwner, Target);
schedule_first_resize_transfer(_,{Idx, _Owner}=IdxOwner, NextOwner, Resized) ->
    %% index is being moved, schedule this resize transfer first
    riak_core_ring:schedule_resize_transfer(Resized, IdxOwner, {Idx, NextOwner}).

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
    Ring4;
change({{resize, NewRingSize}, _Node}, Ring) ->
    riak_core_ring:resize(Ring, NewRingSize).

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
            riak_core_util:rpc_every_member(riak_core_ring_manager,
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
    lists:map(fun(ExitingNode) ->
                      %% Tell exiting node to shutdown.
                      riak_core_ring_manager:refresh_ring(ExitingNode, CName)
              end, Changed),
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
            Changed = (Exiting /= []),
            CState2 =
                lists:foldl(fun(ENode, CState0) ->
                                    ClearedCS =
                                        riak_core_ring:clear_member_meta(Node, CState0, ENode),
                                    riak_core_ring:set_member(Node, ClearedCS, ENode,
                                                              invalid, same_vclock)
                            end, CState, Exiting),
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
            [Log(next, NChange) || NChange <- Diff],
            ?ROUT("Updating ring :: next3 : ~p~n", [Next4]),
            CState5 = riak_core_ring:set_pending_changes(CState4, Next4),
            CState6 = riak_core_ring:increment_ring_version(CNode, CState5),
            {true, CState6};
        false ->
            {false, CState}
    end;
update_ring(CNode, CState, _Replacing, _Seed, _Log, true) ->
    {Changed, CState1} = maybe_install_resized_ring(CState),
    case Changed of
        true ->
            CState2 = riak_core_ring:increment_ring_version(CNode, CState1),
            {true, CState2};
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

    [Log(reassign, {Idx, NewOwner, CState}) || {Idx, NewOwner} <- Reassign],

    %% Unlike rebalance_ring, remove_node can be called when Next is non-empty,
    %% therefore we need to merge the values. Original Next has priority.
    Next2 = lists:ukeysort(1, riak_core_ring:pending_changes(CState) ++ Next),
    CState2 = riak_core_ring:change_owners(CState, Reassign),
    CState3 = riak_core_ring:set_pending_changes(CState2, Next2),
    CState3.

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
