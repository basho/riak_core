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
-export([ring_changed/2]).

-define(ROUT(S,A),ok).
%%-define(ROUT(S,A),?debugFmt(S,A)).
%%-define(ROUT(S,A),io:format(S,A)).

%% =========================================================================
%% Claimant rebalance/reassign logic
%% =========================================================================

ring_changed(Node, CState) ->
    {C1, CState2} = maybe_update_claimant(Node, CState),
    {C2, CState3} = maybe_handle_joining(Node, CState2),
    case C2 of
        true ->
            Changed = true,
            CState5 = CState3;
        false ->
            {C3, CState4} = maybe_update_ring(Node, CState3),
            {C4, CState5} = maybe_remove_exiting(Node, CState4),
            Changed = (C1 or C2 or C3 or C4)
    end,

    %% Start/stop converge and rebalance delay timers
    %% (converge delay)
    %%   -- Starts when claimant changes the ring
    %%   -- Stops when the ring converges (ring_ready)
    %% (rebalance delay)
    %%   -- Starts when next changes from empty to non-empty
    %%   -- Stops when next changes from non-empty to empty
    %%
    IsClaimant = (CState5?CHSTATE.claimant =:= Node),
    WasPending = ([] /= pending_changes(CState)),
    IsPending  = ([] /= pending_changes(CState5)),

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
    case {IsClaimant, cluster_name(CState5)} of
        {true, undefined} ->
            ClusterName = {Node, erlang:now()},
            riak_core_util:rpc_every_member(riak_core_ring_manager,
                                            set_cluster_name,
                                            [ClusterName],
                                            1000),
            ok;
        _ ->
            ClusterName = cluster_name(CState5),
            ok
    end,

    case Changed of
        true ->
            VClock = vclock:increment(Node, CState5?CHSTATE.vclock),
            CState5?CHSTATE{vclock=VClock, clustername=ClusterName};
        false ->
            CState5
    end.

%% @private
maybe_update_claimant(Node, CState) ->
    Members = get_members(CState?CHSTATE.members, [valid, leaving]),
    Claimant = CState?CHSTATE.claimant,
    RVsn = CState?CHSTATE.rvsn,
    NextClaimant = hd(Members ++ [undefined]),
    ClaimantMissing = not lists:member(Claimant, Members),

    case {ClaimantMissing, NextClaimant} of
        {true, Node} ->
            %% Become claimant
            RVsn2 = vclock:increment(Claimant, RVsn),
            CState2 = CState?CHSTATE{claimant=Node, rvsn=RVsn2},
            {true, CState2};
        _ ->
            {false, CState}
    end.

%% @private
maybe_update_ring(Node, CState) ->
    Claimant = CState?CHSTATE.claimant,
    case Claimant of
        Node ->
            case claiming_members(CState) of
                [] ->
                    %% Consider logging an error/warning here or even
                    %% intentionally crashing. This state makes no logical
                    %% sense given that it represents a cluster without any
                    %% active nodes.
                    {false, CState};
                _ ->
                    {Changed, CState2} = update_ring(Node, CState),
                    {Changed, CState2}
            end;
        _ ->
            {false, CState}
    end.

%% @private
maybe_remove_exiting(Node, CState) ->
    Claimant = CState?CHSTATE.claimant,
    case Claimant of
        Node ->
            %% Change exiting nodes to invalid, skipping this node.
            Exiting = get_members(CState?CHSTATE.members, [exiting]) -- [Node],
            Changed = (Exiting /= []),
            CState2 =
                lists:foldl(fun(ENode, CState0) ->
                                    %% Tell exiting node to shutdown.
                                    CName = cluster_name(CState),
                                    riak_core_ring_manager:refresh_ring(ENode,
                                                                        CName),
                                    set_member(Node, CState0, ENode,
                                               invalid, same_vclock)
                            end, CState, Exiting),
            {Changed, CState2};
        _ ->
            {false, CState}
    end.

%% @private
maybe_handle_joining(Node, CState) ->
    Claimant = CState?CHSTATE.claimant,
    case Claimant of
        Node ->
            Joining = get_members(CState?CHSTATE.members, [joining]),
            Changed = (Joining /= []),
            CState2 =
                lists:foldl(fun(JNode, CState0) ->
                                    set_member(Node, CState0, JNode,
                                               valid, same_vclock)
                            end, CState, Joining),
            {Changed, CState2};
        _ ->
            {false, CState}
    end.

%% @private
update_ring(CNode, CState) ->
    Next0 = CState?CHSTATE.next,

    ?ROUT("Members: ~p~n", [CState?CHSTATE.members]),
    ?ROUT("Updating ring :: next0 : ~p~n", [Next0]),

    %% Remove tuples from next for removed nodes
    InvalidMembers = get_members(CState?CHSTATE.members, [invalid]),
    Next2 = lists:filter(fun(NInfo) ->
                                 {Owner, NextOwner, _} = next_owner(NInfo),
                                 not lists:member(Owner, InvalidMembers) and
                                 not lists:member(NextOwner, InvalidMembers)
                         end, Next0),
    CState2 = CState?CHSTATE{next=Next2},

    %% Transfer ownership after completed handoff
    {RingChanged1, CState3} = transfer_ownership(CState2),
    ?ROUT("Updating ring :: next1 : ~p~n", [CState3?CHSTATE.next]),

    %% Ressign leaving/inactive indices
    {RingChanged2, CState4} = reassign_indices(CState3),
    ?ROUT("Updating ring :: next2 : ~p~n", [CState4?CHSTATE.next]),

    %% Rebalance the ring as necessary
    Next3 = rebalance_ring(CNode, CState4),
    lager:debug("Pending ownership transfers: ~b~n",
                [length(pending_changes(CState4))]),
    
    %% Remove transfers to/from down nodes
    Next4 = handle_down_nodes(CState4, Next3),

    NextChanged = (Next0 /= Next4),
    Changed = (NextChanged or RingChanged1 or RingChanged2),
    case Changed of
        true ->
            OldS = ordsets:from_list([{Idx,O,NO} || {Idx,O,NO,_,_} <- Next0]),
            NewS = ordsets:from_list([{Idx,O,NO} || {Idx,O,NO,_,_} <- Next4]),
            Diff = ordsets:subtract(NewS, OldS),
            [log(next, NChange) || NChange <- Diff],
            RVsn2 = vclock:increment(CNode, CState4?CHSTATE.rvsn),
            ?ROUT("Updating ring :: next3 : ~p~n", [Next4]),
            {true, CState4?CHSTATE{next=Next4, rvsn=RVsn2}};
        false ->
            {false, CState}
    end.

%% @private
transfer_ownership(CState=?CHSTATE{next=Next}) ->
    %% Remove already completed and transfered changes
    Next2 = lists:filter(fun(NInfo={Idx, _, _, _, _}) ->
                                 {_, NewOwner, S} = next_owner(NInfo),
                                 not ((S == complete) and
                                      (index_owner(CState, Idx) =:= NewOwner))
                         end, Next),

    CState2 = lists:foldl(
                fun(NInfo={Idx, _, _, _, _}, CState0) ->
                        case next_owner(NInfo) of
                            {_, Node, complete} ->
                                log(ownership, {Idx, Node, CState0}),
                                riak_core_ring:transfer_node(Idx, Node,
                                                             CState0);
                            _ ->
                                CState0
                        end
                end, CState, Next2),

    NextChanged = (Next2 /= Next),
    RingChanged = (all_owners(CState) /= all_owners(CState2)),
    Changed = (NextChanged or RingChanged),
    {Changed, CState2?CHSTATE{next=Next2}}.

%% @private
reassign_indices(CState=?CHSTATE{next=Next}) ->
    Invalid = get_members(CState?CHSTATE.members, [invalid]),
    CState2 =
        lists:foldl(fun(Node, CState0) ->
                            remove_node(CState0, Node, invalid)
                    end, CState, Invalid),
    CState3 = case Next of
                  [] ->
                      Leaving = get_members(CState?CHSTATE.members, [leaving]),
                      lists:foldl(fun(Node, CState0) ->
                                          remove_node(CState0, Node, leaving)
                                  end, CState2, Leaving);
                  _ ->
                      CState2
              end,
    Owners1 = all_owners(CState),
    Owners2 = all_owners(CState3),
    RingChanged = (Owners1 /= Owners2),
    NextChanged = (Next /= CState3?CHSTATE.next),
    {RingChanged or NextChanged, CState3}.

%% @private
rebalance_ring(_CNode, CState=?CHSTATE{next=[]}) ->
    Members = claiming_members(CState),
    CState2 = lists:foldl(fun(Node, Ring0) ->
                                  riak_core_gossip:claim_until_balanced(Ring0,
                                                                        Node)
                          end, CState, Members),
    Owners1 = all_owners(CState),
    Owners2 = all_owners(CState2),
    Owners3 = lists:zip(Owners1, Owners2),
    Next = [{Idx, PrevOwner, NewOwner, [], awaiting}
            || {{Idx, PrevOwner}, {Idx, NewOwner}} <- Owners3,
               PrevOwner /= NewOwner],
    Next;
rebalance_ring(_CNode, _CState=?CHSTATE{next=Next}) ->
    Next.

%% @private
handle_down_nodes(CState, Next) ->
    LeavingMembers = get_members(CState?CHSTATE.members, [leaving, invalid]),
    DownMembers = get_members(CState?CHSTATE.members, [down]),
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
all_next_owners(?CHSTATE{next=Next}) ->
    [{Idx, NextOwner} || {Idx, _, NextOwner, _, _} <- Next].

%% @private
change_owners(CState, Reassign) ->
    lists:foldl(fun({Idx, NewOwner}, CState0) ->
                        riak_core_ring:transfer_node(Idx, NewOwner, CState0)
                end, CState, Reassign).

%% @private
remove_node(CState, Node, Status) ->
    Indices = indices(CState, Node),
    remove_node(CState, Node, Status, Indices).

%% @private
remove_node(CState, _Node, _Status, []) ->
    CState;
remove_node(CState, Node, Status, Indices) ->
    CStateT1 = change_owners(CState, all_next_owners(CState)),
    CStateT2 = riak_core_gossip:remove_from_cluster(CStateT1, Node),
    Owners1 = all_owners(CState),
    Owners2 = all_owners(CStateT2),
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

    [log(reassign, {Idx, NewOwner, CState}) || {Idx, NewOwner} <- Reassign],

    %% Unlike rebalance_ring, remove_node can be called when Next is non-empty,
    %% therefore we need to merge the values. Original Next has priority.
    Next2 = lists:ukeysort(1, CState?CHSTATE.next ++ Next),
    CState2 = change_owners(CState, Reassign),
    CState2?CHSTATE{next=Next2}.

log(ownership, {Idx, NewOwner, CState}) ->
    Owner = index_owner(CState, Idx),
    lager:debug("(new-owner) ~b :: ~p -> ~p~n", [Idx, Owner, NewOwner]);
log(reassign, {Idx, NewOwner, CState}) ->
    Owner = index_owner(CState, Idx),
    lager:debug("(reassign) ~b :: ~p -> ~p~n", [Idx, Owner, NewOwner]);
log(next, {Idx, Owner, NewOwner}) ->
    lager:debug("(pending) ~b :: ~p -> ~p~n", [Idx, Owner, NewOwner]);
log(_, _) ->
    ok.
