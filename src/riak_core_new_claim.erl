%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_core_new_claim).
-export([new_wants_claim/2, new_choose_claim/2]).

new_wants_claim(Ring, Node) ->
    Active = riak_core_ring:claiming_members(Ring),
    Owners = riak_core_ring:all_owners(Ring),
    Counts = get_counts(Active, Owners),
    NodeCount = erlang:length(Active),
    RingSize = riak_core_ring:num_partitions(Ring),
    Avg = RingSize div NodeCount,
    Count = proplists:get_value(Node, Counts, 0),
    case Count < Avg of
        false ->
            no;
        true ->
            {yes, Avg - Count}
    end.
    
new_choose_claim(Ring, Node) ->
    Active = riak_core_ring:claiming_members(Ring),
    Owners = riak_core_ring:all_owners(Ring),
    Counts = get_counts(Active, Owners),
    RingSize = riak_core_ring:num_partitions(Ring),
    NodeCount = erlang:length(Active),
    Avg = RingSize div NodeCount,
    Deltas = [{Member, Avg - Count} || {Member, Count} <- Counts],
    {_, Want} = lists:keyfind(Node, 1, Deltas),
    TargetN = app_helper:get_env(riak_core, target_n_val),
    AllIndices = lists:zip(lists:seq(0, length(Owners)-1),
                           [Idx || {Idx, _} <- Owners]),

    EnoughNodes =
        (NodeCount > TargetN) 
        or ((NodeCount == TargetN) and (RingSize rem TargetN =:= 0)),
    
    case EnoughNodes of
        true ->
            %% If we have enough nodes to meet target_n, then we prefer to
            %% claim indices that are currently causing violations, and then
            %% fallback to indices in linear order. The filtering steps below
            %% will ensure no new violations are introduced.
            Violated = lists:flatten(find_violations(Ring, TargetN)),
            Violated2 = [lists:keyfind(Idx, 2, AllIndices) || Idx <- Violated],
            Indices = Violated2 ++ (AllIndices -- Violated2);
        false ->
            %% If we do not have enough nodes to meet target_n, then we prefer
            %% claiming the same indices that would occur during a
            %% re-diagonalization of the ring with target_n nodes, falling
            %% back to linear offsets off these preferred indices when the
            %% number of indices desired is less than the computed set.
            Padding = lists:duplicate(TargetN, undefined),
            Expanded = lists:sublist(Active ++ Padding, TargetN),
            PreferredClaim = riak_core_claim:diagonal_stripe(Ring, Expanded),
            PreferredNth = [begin
                                {Nth, Idx} = lists:keyfind(Idx, 2, AllIndices),
                                Nth
                            end || {Idx,Owner} <- PreferredClaim,
                                   Owner =:= Node],
            Offsets = lists:seq(0, RingSize div length(PreferredNth)),
            AllNth = lists:sublist([(X+Y) rem RingSize || Y <- Offsets,
                                                          X <- PreferredNth],
                                   RingSize),
            Indices = [lists:keyfind(Nth, 1, AllIndices) || Nth <- AllNth]
    end,

    %% Filter out indices that conflict with the node's existing ownership
    Indices2 = prefilter_violations(Ring, Node, AllIndices, Indices,
                                    TargetN, RingSize),
    %% Claim indices from the remaining candidate set
    Claim = select_indices(Owners, Deltas, Indices2, TargetN, RingSize),
    Claim2 = lists:sublist(Claim, Want),
    NewRing = lists:foldl(fun(Idx, Ring0) ->
                                  riak_core_ring:transfer_node(Idx, Node, Ring0)
                          end, Ring, Claim2),

    RingChanged = ([] /= Claim2),
    RingMeetsTargetN = riak_core_claim:meets_target_n(NewRing, TargetN),
    case {RingChanged, EnoughNodes, RingMeetsTargetN} of
        {false, _, _} ->
            %% Unable to claim, fallback to re-diagonalization
            riak_core_claim:claim_rebalance_n(Ring, Node);
        {_, true, false} ->
            %% Failed to meet target_n, fallback to re-diagonalization
            riak_core_claim:claim_rebalance_n(Ring, Node);
        _ ->
            NewRing
    end.

%% Counts up the number of partitions owned by each node
get_counts(Nodes, Ring) ->
    Empty = [{Node, 0} || Node <- Nodes],
    Counts = lists:foldl(fun({_Idx, Node}, Counts) ->
                                 case lists:member(Node, Nodes) of
                                     true ->
                                         dict:update_counter(Node, 1, Counts);
                                     false ->
                                         Counts
                                 end
                         end, dict:from_list(Empty), Ring),
    dict:to_list(Counts).

%% Filter out candidate indices that would violate target_n given a node's
%% current partition ownership.
prefilter_violations(Ring, Node, AllIndices, Indices, TargetN, RingSize) ->
    CurrentIndices = riak_core_ring:indices(Ring, Node),
    CurrentNth = [lists:keyfind(Idx, 2, AllIndices) || Idx <- CurrentIndices],
    [{Nth, Idx} || {Nth, Idx} <- Indices,
                   lists:all(fun({CNth, _}) ->
                                     spaced_by_n(CNth, Nth, TargetN, RingSize)
                             end, CurrentNth)].

%% Select indices from a given candidate set, according to two goals.
%% 1. Ensure greedy/local target_n spacing between indices. Note that this
%%    goal intentionally does not reject overall target_n violations.
%%
%% 2. Select indices based on the delta between current ownership and
%%    expected ownership. In other words, if A owns 5 partitions and
%%    the desired ownership is 3, then we try to claim at most 2 partitions
%%    from A.
select_indices(_Owners, _Deltas, [], _TargetN, _RingSize) ->
    [];
select_indices(Owners, Deltas, Indices, TargetN, RingSize) ->
    OwnerDT = dict:from_list(Owners),
    {FirstNth, _} = hd(Indices),
    {Claim, _, _} =
        lists:foldl(fun({Nth, Idx}, {Out, LastNth, DeltaDT}) ->
                            Owner = dict:fetch(Idx, OwnerDT),
                            Delta = dict:fetch(Owner, DeltaDT),
                            First = (LastNth =:= Nth),
                            MeetsTN = spaced_by_n(LastNth, Nth, TargetN,
                                                  RingSize),
                            case (Delta < 0) and (First or MeetsTN) of
                                true ->
                                    NextDeltaDT =
                                        dict:update_counter(Owner, 1, DeltaDT),
                                    {[Idx|Out], Nth, NextDeltaDT};
                                false ->
                                    {Out, LastNth, DeltaDT}
                            end
                    end,
                    {[], FirstNth, dict:from_list(Deltas)},
                    Indices),
    lists:reverse(Claim).

%% Determines indices that violate the given target_n spacing property.
find_violations(Ring, TargetN) ->
    Owners = riak_core_ring:all_owners(Ring),
    Suffix = lists:sublist(Owners, TargetN-1),
    Owners2 = Owners ++ Suffix,
    %% Use a sliding window to determine violations
    {Bad, _} = lists:foldl(fun(P={Idx, Owner}, {Out, Window}) ->
                                   Window2 = lists:sublist([P|Window], TargetN-1),
                                   case lists:keyfind(Owner, 2, Window) of
                                       {PrevIdx, Owner} ->
                                           {[[PrevIdx, Idx] | Out], Window2};
                                       false ->
                                           {Out, Window2}
                                   end
                           end, {[], []}, Owners2),
    lists:reverse(Bad).

%% Determine if two positions in the ring meet target_n spacing.
spaced_by_n(NthA, NthB, TargetN, RingSize) ->
    case NthA > NthB of
        true ->
            NFwd = NthA - NthB,
            NBack = NthB - NthA + RingSize;
        false ->
            NFwd = NthA - NthB + RingSize,
            NBack = NthB - NthA
    end,
    (NFwd >= TargetN) and (NBack >= TargetN).
