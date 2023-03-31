%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
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

%% @doc choose and sequential claim functions for a more location friendly
%% claim algorithm

-module(riak_core_claim_location).

-export(
    [
        choose_claim_v4/2, choose_claim_v4/3,
        sequential_claim/2, sequential_claim/3,
        sort_members_for_choose/3
    ]).

-type location_finder() :: fun((node()) -> atom()).

-spec sort_members_for_choose(
        riak_core_ring:riak_core_ring(),
        list(node()),
        list({non_neg_integer(), node()})) ->
            list({non_neg_integer(), node()}).
sort_members_for_choose(Ring, Members, Owners) ->
    NodesLocations = riak_core_ring:get_nodes_locations(Ring),
    case riak_core_location:has_location_set_in_cluster(NodesLocations) of
        false ->
            Members;
        true ->
            LocationNodesD =
                riak_core_location:get_location_nodes(Members, NodesLocations),
            InitSort = initial_location_sort(dict:to_list(LocationNodesD)),
            lists:append(lists:subtract(InitSort, Owners), Owners)
    end.

initial_location_sort(LocationNodeList) ->
    NodeLists =
        sort_lists_by_length(
            lists:map(fun({_L, NL}) -> NL end, LocationNodeList)),
    roll_nodelists(NodeLists, []).

roll_nodelists(NodeLists, ListOfNodes) ->
    case length(hd(NodeLists)) of
        L when L > 1 ->
            {UpdNodeLists, UpdListOfNodes} =
                lists:mapfoldl(
                    fun(NL, Acc) ->
                        case length(NL) of
                            L when L > 1 ->
                                [H|T] = NL,
                                {T, [H|Acc]};
                            _ ->
                                {NL, Acc}
                        end
                    end,
                    ListOfNodes,
                    NodeLists),
            roll_nodelists(UpdNodeLists, UpdListOfNodes);
        1 ->
            ListOfNodes ++ lists:flatten(NodeLists)
    end.

choose_claim_v4(Ring, Node) ->
    Params = riak_core_membership_claim:default_choose_params(),
    choose_claim_v4(Ring, Node, Params).

-spec choose_claim_v4(
        riak_core_ring:riak_core_ring(), node(), list(tuple())) ->
            riak_core_ring:riak_core_ring().
choose_claim_v4(Ring, Node, Params0) ->
    Params = riak_core_membership_claim:default_choose_params(Params0),
    Active = riak_core_ring:claiming_members(Ring),
    Owners = riak_core_ring:all_owners(Ring),
    Ownerships = riak_core_membership_claim:get_counts(Active, Owners),
    RingSize = riak_core_ring:num_partitions(Ring),
    NodeCount = length(Active),
    {MinVnodes, MaxVnodes, Deltas}
        = assess_deltas(RingSize, NodeCount, Ownerships),
    {Node, CurrentOwnerships} =
        lists:keyfind(Node, 1, Ownerships),
    Want = MaxVnodes - CurrentOwnerships,
    TargetN = proplists:get_value(target_n_val, Params),

    NodesToClaim = lists:filter(fun({_N, O}) -> O == 0 end, Ownerships),
    NodesAllClaimed  =
        case NodesToClaim of
            [{Node, _}] ->
                true;
            [] ->
                true;
            _ ->
                false
        end,

    ZippedIndices =
        lists:zip(
            lists:seq(0, length(Owners) - 1),
            [Idx || {Idx, _} <- Owners]
            ),
    AllIndices =
        sort_indices_for_claim(
            ZippedIndices, length(Active), Owners, Deltas, NodesAllClaimed),
    
    EnoughNodes =
        (NodeCount > TargetN)
        or ((NodeCount == TargetN) and (RingSize rem TargetN =:= 0)),
    
    Indices =
        case EnoughNodes of
            true ->
                %% If we have enough nodes to meet target_n, then we prefer to
                %% claim indices that are currently causing violations, and
                %% then fallback to indices in linear order. The filtering
                %% steps below will ensure no new violations are introduced.
                NodeViolations = find_node_violations(Ring, TargetN),
                LocationViolations =
                    lists:subtract(
                        find_location_violations(Ring, TargetN),
                        NodeViolations),
                {DirtyNodeIndices, OtherIndices} =
                    lists:splitwith(
                        fun({_Nth, Idx}) ->
                            lists:member(Idx, NodeViolations)
                        end,
                        AllIndices),
                {DirtyLocationIndices, CleanIndices} =
                    lists:splitwith(
                        fun({_Nth, Idx}) ->
                            lists:member(Idx, LocationViolations)
                        end,
                        OtherIndices
                    ),
                DirtyNodeIndices ++ DirtyLocationIndices ++ CleanIndices;
            false ->
                AllIndices
        end,

    %% Filter out indices that conflict with the node's existing ownership
    ClaimableIdxs =
        prefilter_violations(
            Ring, Node, AllIndices, Indices, TargetN, RingSize),

    %% Claim indices from the remaining candidate set
    Claim2 = 
        case select_indices(
                Owners, Deltas, ClaimableIdxs, TargetN, RingSize) of
            [] ->
                [];
            Claim ->
                lists:sublist(Claim, Want)
        end,
    NewRing =
        lists:foldl(
            fun(Idx, Ring0) ->
                riak_core_ring:transfer_node(Idx, Node, Ring0)
            end,
            Ring,
            Claim2),

    BadRing = length(meets_target_n(NewRing, TargetN)) > 0,
    DeficientClaim = (length(Claim2) + CurrentOwnerships) < MinVnodes,
    BadClaim = EnoughNodes and BadRing and NodesAllClaimed,

    MaybeBalancedRing =
        case NodesAllClaimed and (MinVnodes < MaxVnodes) of
            true ->
                NewOwners = riak_core_ring:all_owners(NewRing),
                NewOwnerships =
                    riak_core_membership_claim:get_counts(Active, NewOwners),
                {MinVnodes, MaxVnodes, NewDeltas}
                    = assess_deltas(RingSize, NodeCount, NewOwnerships),
                NodesToGive =
                    lists:filter(
                        fun({_N, D}) ->
                            case D of
                                D when D < (MinVnodes - MaxVnodes) ->
                                    true;
                                _ ->
                                    false
                            end
                        end,
                        NewDeltas),
                NodesToTake =
                    lists:filtermap(
                        fun({N, D}) -> 
                            case D of 0 -> {true, N}; _ -> false end
                        end,
                        NewDeltas),
                give_partitions(
                    NodesToGive, NodesToTake, ZippedIndices, TargetN, NewRing);
            false ->
                NewRing
        end,
    
    case BadClaim or DeficientClaim of
        true ->
            sequential_claim(Ring, Node, TargetN);
        _ ->
            MaybeBalancedRing
    end.

-spec give_partitions(
        list({node(), integer()}),
        list(node()),
        list({non_neg_integer(), non_neg_integer()}),
        pos_integer(),
        riak_core_ring:riak_core_ring()) -> riak_core_ring:riak_core_ring().
give_partitions([], _TakeNodes, _ZipIndices, _TargetN, Ring) ->
    Ring;
give_partitions(_, [], _ZipIndices, _TargetN, Ring) ->
    Ring;
give_partitions([{_Node, -1}|Rest], TakeNodes, ZipIndices, TargetN, Ring) ->
    give_partitions(Rest, TakeNodes, ZipIndices, TargetN, Ring);
give_partitions([{Node, D}|Rest], TakeNodes, ZipIndices, TargetN, Ring) ->
    Owners = riak_core_ring:all_owners(Ring),
    Partitions =
        lists:filtermap(
            fun({Idx, N}) -> case N of Node -> {true, Idx}; _ -> false end end,
            Owners),
    {Success, ClaimableIdx, ReceivingNode} = 
        lists:foldl(
            fun (_Idx, {true, P, RcvNode}) ->
                    {true, P, RcvNode};
                (Idx, {false, undefined, undefined}) ->
                    PotentialHomes =
                        find_home(
                            Idx, TakeNodes, ZipIndices, TargetN, Owners, Ring),
                    case PotentialHomes of
                        [] ->
                            {false, undefined, undefined};
                        [HN|_Rest] ->
                            {true, Idx, HN}
                    end
            end,
            {false, undefined, undefined},
            Partitions),
    case {Success, ClaimableIdx, ReceivingNode} of
        {true, ClaimableIdx, ReceivingNode} ->
            give_partitions(
                [{Node, D + 1}|Rest],
                TakeNodes -- [ReceivingNode],
                ZipIndices,
                TargetN,
                riak_core_ring:transfer_node(
                    ClaimableIdx, ReceivingNode, Ring));
        {false, undefined, undefined} ->
            give_partitions(Rest, TakeNodes, ZipIndices, TargetN, Ring)
    end.


find_home(Idx, TakeNodes, ZippedIndices, TargetN, Owners, Ring) ->
    {Nth, Idx} = lists:keyfind(Idx, 2, ZippedIndices),
    RS = length(ZippedIndices),
    OwningNodes =
        lists:usort(
            lists:map(
                fun(N0) -> 
                    N1 =
                        case N0 of
                            N0 when N0 < 0 -> RS + N0;
                            N0 when N0 >= RS -> N0 - RS;
                            N0 -> N0
                        end,
                    {N1, I} = lists:keyfind(N1, 1, ZippedIndices),
                    {I, O} = lists:keyfind(I, 1, Owners),
                    O
                end,
                lists:seq((Nth + 1) - TargetN, Nth + TargetN - 1) -- [Nth])
        ),
    NodesLocations = riak_core_ring:get_nodes_locations(Ring),
    case riak_core_location:has_location_set_in_cluster(NodesLocations) of
        false ->
            TakeNodes -- OwningNodes;
        true ->
            Locations =
                lists:usort(
                    lists:map(
                        fun(N) ->
                            riak_core_location:get_node_location(
                                N, NodesLocations)
                        end,
                        OwningNodes)),
            lists:filter(
                fun(TN0) ->
                    not lists:member(
                            riak_core_location:get_node_location(
                                TN0, NodesLocations),
                            Locations)
                end,
                TakeNodes)
        end.


-spec sort_indices_for_claim(
    list({non_neg_integer(), non_neg_integer()}),
    pos_integer(),
    [{non_neg_integer(), node()}],
    [{node(), integer()}],
    boolean()) -> list({non_neg_integer(), non_neg_integer()}).
sort_indices_for_claim(
        ZippedIndices, ActiveMemberCount, Owners, Deltas, _NodesAllClaimed) ->
    StripeCount = max(1, (ActiveMemberCount - 1)),
    StripeList =
        lists:map(
            fun({Nth, I}) -> {Nth rem StripeCount, Nth, I} end,
            ZippedIndices),
    Counter =
        dict:from_list(
            lists:map(fun(I) -> {I, 0} end, lists:seq(0, StripeCount - 1))),
    Counted =
        lists:foldl(
            fun({R, _Nth, _I}, C) -> dict:update_counter(R, 1, C) end,
            Counter,
            StripeList),
    lists:map(
        fun({_OD, _RC, _R, Nth, I}) -> {Nth, I} end,
        lists:sort(
            lists:map(
                fun({R, Nth, I}) ->
                    {I, Owner} = lists:keyfind(I, 1, Owners),
                    {Owner, Delta} = lists:keyfind(Owner, 1, Deltas),
                    {Delta, dict:fetch(R, Counted), R, Nth, I}
                end,
                lists:reverse(StripeList)
                ))).

%% @doc
%% Assess what the minimum and maximum number of vnodes which should be owned
%% by each node, and return a list of nodes with the Deltas from the minimum
%% i.e. where a node has more vnodes than the minimum the delta will be a
%% negative number indicating the number of vnodes it can offer to a node with
%% wants.
-spec assess_deltas(
    pos_integer(), pos_integer(), [{node(), non_neg_integer()}]) ->
        {non_neg_integer(), pos_integer(), [{node(), integer()}]}.
assess_deltas(RingSize, NodeCount, Ownerships) ->
    MinVnodes = RingSize div NodeCount,
    MaxVnodes =
        case RingSize rem NodeCount of
            0 ->
                MinVnodes;
            _ ->
                MinVnodes + 1
        end,
    Deltas =
        lists:map(fun({N, VNs}) -> {N, MinVnodes - VNs} end, Ownerships),
    {MinVnodes, MaxVnodes, Deltas}.


%% @private
%%
%% @doc Filter out candidate indices that would violate target_n given
%% a node's current partition ownership.  Only interested in indices which
%% are not currently owned within a location
-spec prefilter_violations(
    riak_core_ring:riak_core_ring(),
    node(),
    list({non_neg_integer(), non_neg_integer()}),
    list({non_neg_integer(), non_neg_integer()}),
    pos_integer(),
    pos_integer()) -> list({non_neg_integer(), non_neg_integer()}).
prefilter_violations(Ring, Node, AllIndices, Indices, TargetN, RingSize) ->
    CurrentIndices =
        indices_nth_subset(AllIndices, riak_core_ring:indices(Ring, Node)),
    case riak_core_location:support_locations_claim(Ring, TargetN) of
        true ->
            OtherLocalNodes =
                riak_core_location:local_nodes(Ring, Node),
            LocalIndices =
                indices_nth_subset(
                    AllIndices,
                    lists:flatten(
                        lists:map(
                            fun(N) -> riak_core_ring:indices(Ring, N) end,
                            [Node|OtherLocalNodes]))),
            SafeRemoteIndices =
                safe_indices(
                    lists:subtract(Indices, LocalIndices),
                    LocalIndices, TargetN, RingSize),
            SafeLocalIndices =
                safe_indices(
                    lists:subtract(
                        lists:filter(
                            fun(NthIdx) -> lists:member(NthIdx, Indices) end,
                            LocalIndices),
                        CurrentIndices),
                    CurrentIndices, TargetN, RingSize),
            SafeRemoteIndices ++ SafeLocalIndices;
        false ->
            safe_indices(
                lists:subtract(AllIndices, CurrentIndices),
                CurrentIndices, TargetN, RingSize)
    end.

-spec indices_nth_subset(
        list({non_neg_integer(), non_neg_integer()}),
        list(non_neg_integer())) ->
            list({non_neg_integer(), non_neg_integer()}).
indices_nth_subset(IndicesNth, Indices) ->
    lists:filter(fun({_N, Idx}) -> lists:member(Idx, Indices) end, IndicesNth).

-spec safe_indices(
        list({non_neg_integer(), non_neg_integer()}),
        list({non_neg_integer(), non_neg_integer()}),
        pos_integer(),
        pos_integer()) ->
            list({non_neg_integer(), non_neg_integer()}).
safe_indices(
    IndicesToCheck, LocalIndicesToAvoid, TargetN, RingSize) ->
    lists:filter(
        fun({Nth, _Idx}) ->
            lists:all(
                fun({CNth, _}) ->
                    riak_core_membership_claim:spaced_by_n(
                        CNth, Nth, TargetN, RingSize)
                end,
                LocalIndicesToAvoid)
        end,
        IndicesToCheck 
    ).


-spec meets_target_n(
        riak_core_ring:riak_core_ring(), pos_integer()) ->
            list({non_neg_integer(), node(), list(node())}).
meets_target_n(Ring, TargetN) when TargetN > 1 ->
    {_RingSize, Mappings} = riak_core_ring:chash(Ring),
    Prefix = lists:sublist(Mappings, TargetN - 1),
    CheckableMap = Mappings ++ Prefix,
    {_, Failures} =
        lists:foldl(
            fun({Idx, N}, {LastNminus1, Fails}) ->
                case lists:member(N, LastNminus1) of
                    false ->
                        {[N|lists:sublist(LastNminus1, TargetN - 2)], Fails};
                    true ->
                        {[N|lists:sublist(LastNminus1, TargetN - 2)],
                            [{Idx, N, LastNminus1}|Fails]}
                end
            end,
            {[], []},
            CheckableMap),
    Failures;
meets_target_n(_Ring, _TargetN) ->
    true.

%% @private
%%
%% @doc Select indices from a given candidate set, according to two
%% goals.
%%
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
    %% Claim partitions and check that subsequent partitions claimed by this
    %% node do not break the target_n invariant.
    {Claims, _NClaims, _Deltas} =
        lists:foldl(
            fun({Nth, Idx}, {IdxClaims, NthClaims, DeltaDT}) ->
                Owner = dict:fetch(Idx, OwnerDT),
                Delta = dict:fetch(Owner, DeltaDT),
                MeetsTN = 
                    lists:all(
                        fun(ClaimedNth) ->
                            riak_core_membership_claim:spaced_by_n(
                                ClaimedNth, Nth, TargetN, RingSize)
                        end,
                        NthClaims),
                case (Delta < 0) and MeetsTN of
                    true ->
                        NextDeltaDT =
                            dict:update_counter(Owner, 1, DeltaDT),
                        {[Idx|IdxClaims], [Nth|NthClaims], NextDeltaDT};
                    false ->
                        {IdxClaims, NthClaims, DeltaDT}
                end
            end,
            {[], [], dict:from_list(Deltas)},
            Indices),
    lists:reverse(Claims).


%% @private
%%
%% @doc Determines indices that violate the given target_n spacing
%% property.
-spec find_node_violations(
    riak_core_ring:riak_core_ring(), pos_integer())
        -> list(non_neg_integer()).
find_node_violations(Ring, TargetN) ->
    Owners = riak_core_ring:all_owners(Ring),
    find_violations(Owners, TargetN).

-spec find_location_violations(
    riak_core_ring:riak_core_ring(), pos_integer())
        -> list(non_neg_integer()).
find_location_violations(Ring, TargetN) ->
    case riak_core_location:support_locations_claim(Ring, TargetN) of
        true ->
            find_violations(
                riak_core_location:get_location_owners(Ring), TargetN);
        false ->
            []
    end.

-spec find_violations(
    list({non_neg_integer(), atom()}), pos_integer())
        -> list(non_neg_integer()).
find_violations(Owners, TargetN) ->
    Suffix = lists:sublist(Owners, TargetN - 1),
    %% Add owners at the front to the tail, to confirm no tail violations
    OwnersWithTail = Owners ++ Suffix,
    %% Use a sliding window to determine violations
    {Bad, _} =
        lists:foldl(
            fun(P={Idx, Owner}, {Out, Window}) ->
                Window2 = lists:sublist([P|Window], TargetN-1),
                case lists:keyfind(Owner, 2, Window) of
                    {_PrevIdx, Owner} ->
                        {[Idx | Out], Window2};
                    false ->
                        {Out, Window2}
                end
            end,
            {[], lists:sublist(Owners, 2, TargetN - 1)},
            OwnersWithTail),
    lists:usort(Bad).

-spec sequential_claim(
    riak_core_ring:riak_core_ring(), node()) ->
        riak_core_ring:riak_core_ring().
sequential_claim(Ring, Node) ->
    TN = riak_core_membership_claim:get_target_n(),
    sequential_claim(Ring, Node, TN).

%% @private fall back to diagonal striping vnodes across nodes in a
%% sequential round robin (eg n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3
%% etc) However, different to `claim_rebalance_n', this function
%% attempts to eliminate tail violations (for example a ring that
%% starts/ends n1 | n2 | ...| n3 | n4 | n1)
-spec sequential_claim(
    riak_core_ring:riak_core_ring(), node(), integer()) ->
        riak_core_ring:riak_core_ring().
sequential_claim(Ring, Node, TargetN) ->
    OrigNodes = lists:usort([Node|riak_core_ring:claiming_members(Ring)]),
    Nodes = get_nodes_by_location(OrigNodes, Ring),
    NodeCount = length(Nodes),
    RingSize = riak_core_ring:num_partitions(Ring),

    Overhang = RingSize rem NodeCount,
    HasTailViolation = (Overhang > 0 andalso Overhang < TargetN),
    Shortfall = TargetN - Overhang,
    SolveableNodeViolation =
        solveable_violation(RingSize, NodeCount, TargetN, Shortfall)
        and HasTailViolation,

    LocationsSupported =
        riak_core_location:support_locations_claim(Ring, TargetN),
    {SolveableLocationViolation, LocationShortfall} =
        case {LocationsSupported, Overhang, RingSize div NodeCount} of
            {true, OH, Loops} when OH > 0, Loops > 1 ->
                MinDistance =
                    check_for_location_tail_violation(
                        Nodes, Ring, OH, TargetN),
                case MinDistance of
                    MD when MD =< TargetN ->
                        SLV = 
                            solveable_violation(
                                RingSize, NodeCount, TargetN, TargetN - MD),
                        {SLV, TargetN - MD};
                    _ ->
                        {false, 0}
                end;
            _ ->
                {false, 0}
        end,
    
    Partitions = lists:sort([ I || {I, _} <- riak_core_ring:all_owners(Ring) ]),
    Zipped = 
        case {SolveableLocationViolation, SolveableNodeViolation} of
            {true, _} ->
                F = location_finder(Ring),
                Nodelist =
                    solve_tail_violations(
                        RingSize, Nodes, LocationShortfall, TargetN, true, F),
                lists:zip(Partitions, Nodelist);
            {_, true} ->
                Nodelist =
                    solve_tail_violations(
                        RingSize, Nodes, Shortfall, TargetN, false, undefined),
                lists:zip(Partitions, Nodelist);
            _ ->
                riak_core_membership_claim:diagonal_stripe(Ring, Nodes)
        end,

    lists:foldl(
        fun({P, N}, Acc) -> riak_core_ring:transfer_node(P, N, Acc) end,
        Ring,
        Zipped).

-spec location_finder(riak_core_ring:riak_core_ring()) -> location_finder().
location_finder(Ring) ->
    LocationD = riak_core_ring:get_nodes_locations(Ring),
    fun(N) ->
        riak_core_location:get_node_location(N, LocationD)
    end.

-spec check_for_location_tail_violation(
    list(node()),
    riak_core_ring:riak_core_ring(),
    pos_integer(),
    pos_integer()) -> pos_integer().
check_for_location_tail_violation(Nodes, Ring, OH, TargetN) ->
    {LastLoop, ExtraNodes} = lists:split(OH, Nodes),
    LastNodes =
        lists:reverse(
            lists:sublist(
                lists:reverse(ExtraNodes ++ LastLoop), TargetN - 1)),
    FirstNodes = lists:sublist(Nodes, TargetN - 1),
    LocationFinder = location_finder(Ring),
    LastLocations = lists:map(LocationFinder, LastNodes),
    FirstLocations =
        lists:zip(
            lists:map(LocationFinder, FirstNodes),
            lists:seq(1, TargetN - 1)),
    {MinDistance, _} =
        lists:foldl(
            fun(L, {MinStep, TailStep}) ->
                case lists:keyfind(L, 1, FirstLocations) of
                    {L, N} ->
                        {min(TailStep + N, MinStep), TailStep - 1};
                    false ->
                        {MinStep, TailStep - 1}
                end
            end,
            {TargetN, TargetN - 2},
            LastLocations),
    MinDistance.


-spec solveable_violation(
    pos_integer(), pos_integer(), pos_integer(), pos_integer()) -> boolean().
solveable_violation(RingSize, NodeCount, TargetN, Shortfall) ->
    case RingSize div NodeCount of
        LoopCount when LoopCount >= Shortfall ->
            true;
        LoopCount ->
            SplitSize = Shortfall div LoopCount,
            BiggestTake = Shortfall - ((LoopCount - 1) * SplitSize),
            (NodeCount - BiggestTake) >= TargetN
    end.

%% @doc
%% The node list must be of length ring size.  It is made up of a set of
%% complete loops of the node list, and then a partial loop with the addition
%% of the shortfall.  The for each node in the shortfall a node in the complete
%% loops must be removed
-spec solve_tail_violations(
    pos_integer(),
    [node()],
    non_neg_integer(),
    pos_integer(),
    boolean(),
    undefined|location_finder()) ->
        [[node()]].
solve_tail_violations(
        RingSize, Nodes, Shortfall, _TargetN, false, _LocFinder) ->
    {LastLoop, Remainder} = 
        lists:split(RingSize rem length(Nodes), Nodes),
    ExcessLoop = lists:sublist(Remainder, Shortfall),
    Tail = LastLoop ++ ExcessLoop,
    LoopCount = RingSize div length(Nodes),
    RemoveList =
        divide_list_for_removes(lists:reverse(ExcessLoop), LoopCount),
    CompleteLoops =
        lists:append(
            lists:duplicate(LoopCount - length(RemoveList), Nodes)),
    PartialLoops =
        lists:map(
            fun(ENL) -> lists:subtract(Nodes, ENL) end, 
            RemoveList),
    CompleteLoops ++ lists:append(PartialLoops) ++ Tail;
solve_tail_violations(
        RingSize, Nodes, Shortfall, TargetN, true, LocFinder) ->
    {LastLoop, Remainder} = 
        lists:split(RingSize rem length(Nodes), Nodes),
    PostLoop = lists:sublist(Nodes, TargetN - 1),
    PreExcess =
        lists:reverse(
            lists:sublist(
                lists:reverse(Nodes ++ LastLoop), TargetN - 1)),
    {SafeList, SafeAdditions} =
        case safe_to_remove(Nodes, LastLoop, TargetN, LocFinder) of
            SL when length(SL) >= Shortfall ->
                {lists:sublist(SL, Shortfall), Remainder};
            SL ->
                RemovableExcess =
                    safe_to_remove(
                        Nodes, Remainder, TargetN, LocFinder),
                {SL, RemovableExcess}
        end,
    ExcessLoop =
        case length(SafeAdditions) of
            NodesToCheck when NodesToCheck >= Shortfall ->
                safe_to_add(
                    PreExcess, PostLoop, SafeAdditions, LocFinder, Shortfall);
            NodesToCheck ->
                CheckList = 
                    SafeAdditions ++
                    lists:sublist(
                        lists:subtract(Remainder, SafeAdditions),
                        Shortfall - NodesToCheck),
                safe_to_add(
                        PreExcess, PostLoop, CheckList, LocFinder, Shortfall)
        end,
                
    Tail = LastLoop ++ ExcessLoop,
    LoopCount = RingSize div length(Nodes),
    RemoveCount = length(ExcessLoop),
    RemoveList =
        divide_list_for_removes(
            lists:sublist(SafeList ++ ExcessLoop, RemoveCount), LoopCount),
    RemoveLoops = length(RemoveList),
    
    case LoopCount > (2 * RemoveLoops) of
        true ->
            PartialLoops =
                lists:map(
                    fun(ENL) -> lists:subtract(Nodes, ENL) ++ Nodes end, 
                    RemoveList),
            CompleteLoops =
                lists:flatten(
                    lists:duplicate(LoopCount - (2 * RemoveLoops), Nodes)),
            CompleteLoops ++ lists:append(PartialLoops) ++ Tail;
        false ->
            CompleteLoops =
                lists:flatten(
                    lists:duplicate(LoopCount - RemoveLoops, Nodes)),
            PartialLoops =
                lists:map(
                    fun(ENL) -> lists:subtract(Nodes, ENL) end, 
                    RemoveList),
            CompleteLoops ++ lists:append(PartialLoops) ++ Tail
    end.


-spec safe_to_add(
    list(node()),
    list(node()),
    list(node()),
    location_finder()|undefined,
    pos_integer()) -> list(node()).
safe_to_add(PreExcess, PostLoop, NodesToCheck, LocFinder, Shortfall) ->
    NodePositions =
        score_for_adding(
            lists:zip(
                lists:map(LocFinder, lists:reverse(PreExcess)),
                lists:seq(1, length(PreExcess))),
            lists:zip(
                lists:map(LocFinder, PostLoop),
                lists:seq(1, length(PostLoop))),
            lists:map(LocFinder, NodesToCheck),
            [],
            Shortfall),
    PositionsByNode = lists:zip(NodePositions, NodesToCheck),
    Positions = lists:seq(1, Shortfall),
    case choose_positions(Positions, PositionsByNode, [], {[], LocFinder}) of
        fail ->
            lists:sublist(NodesToCheck, Shortfall);
        NodeList ->
            lists:reverse(NodeList)
    end.

choose_positions([], _PositionsByNode, NodeList, _LocationCheck) ->
    NodeList;
choose_positions([Pos|RestPos], PositionsByNode, NodeList, {LocList, LocF}) ->
    SortedPositionsByNode =
        lists:filter(
            fun({PL, _N}) -> length(PL) > 0 end,
            lists:sort(PositionsByNode)),
    case SortedPositionsByNode of
        [{TopPL, TopN}|RestPBN] ->
            TopL = LocF(TopN),
            case {lists:member(Pos, TopPL), lists:member(TopL, LocList)} of
                {true, false} ->
                    choose_positions(
                        RestPos,
                        lists:map(
                            fun({PL, N}) -> {PL -- [Pos], N} end,
                            RestPBN),
                        [TopN|NodeList],
                        {[TopL|LocList], LocF});
                {true, true} ->
                    choose_positions(
                        [Pos|RestPos],
                        RestPBN,
                        NodeList,
                        {LocList, LocF});
                _ ->
                    fail
            end;
        _ ->
            fail
    end.


-spec score_for_adding(
    list({node()|atom(), pos_integer()}),
    list({node()|atom(), pos_integer()}),
    list(node()|atom()),
    list(list(pos_integer())),
    pos_integer()) ->
        list(list(pos_integer())).
score_for_adding(_PreExcess, _PostLoop, [], NodePositions, _Shortfall) ->
    lists:reverse(NodePositions);
score_for_adding(PreExcess, PostLoop, [HD|Rest], NodePositions, Shortfall) ->
    BackPositions =
        case lists:keyfind(HD, 1, PreExcess) of
            {HD, BS} ->
                lists:filter(
                    fun(P) -> (P + BS - 1) > length(PreExcess) end,
                    lists:seq(1, Shortfall) 
                );
            false ->
                lists:seq(1, Shortfall)
        end,
    ForwardPositions =
        case lists:keyfind(HD, 1, PostLoop) of
            {HD, FS} ->
                lists:filter(
                    fun(P) -> (FS + Shortfall - P) > length(PostLoop) end,
                    lists:seq(1, Shortfall));
            false ->
                lists:seq(1, Shortfall)
        end,
    SupportedPositions =
        lists:filter(
            fun(BP) -> lists:member(BP, ForwardPositions) end, BackPositions),
    score_for_adding(
        PreExcess,
        PostLoop,
        Rest,
        [SupportedPositions|NodePositions],
        Shortfall).


-spec safe_to_remove(
    list(node()),
    list(node()),
    pos_integer(),
    location_finder()|undefined) -> list(node()).
safe_to_remove(Nodes, NodesToCheck, TargetN, LocFinder) ->
    LocationFinder = fun(N) -> {N, LocFinder(N)} end,
    safe_to_remove_loop(
        lists:map(LocationFinder, Nodes),
        lists:map(LocationFinder, NodesToCheck),
        [],
        TargetN).

safe_to_remove_loop(_Nodes, [], SafeList, _TargetN) ->
    SafeList;
safe_to_remove_loop(Nodes, [HD|Rest], SafeList, TargetN) ->
    WrappedNodes = (Nodes -- [HD]) ++ lists:sublist(Nodes, 1, TargetN),
    {Node, _Location} = HD,
    CheckFun =
        fun({_N, L}, CheckList) ->
            case lists:keyfind(L, 2, CheckList) of
                false ->
                    false;
                _ ->
                    true
            end
        end,
    IsSafe =
        lists:foldl(
            fun(N, Acc) ->
                case Acc of
                    fail ->
                        fail;
                    LastNminus1 when is_list(LastNminus1) ->
                        case CheckFun(N, LastNminus1) of
                            false ->
                                [N|lists:sublist(LastNminus1, TargetN - 2)];
                            true ->
                                fail
                        end
                end
            end,
            [],
            WrappedNodes),
    case IsSafe of
        fail ->
            safe_to_remove_loop(Nodes, Rest, SafeList, TargetN);
        _ ->
            safe_to_remove_loop(Nodes, Rest, [Node|SafeList], TargetN)
    end.


%% @doc 
%% Normally need to remove one of the excess nodes each loop around the node
%% list.  However, if there are not enough loops, more than one can be removed
%% per loop - assuming the solveable_violation/4 condition passes (i.e. this
%% will not breach the TargetN).
-spec divide_list_for_removes(list(node()), pos_integer())
        -> list(list(node())).
divide_list_for_removes(Excess, LoopCount) when LoopCount >= length(Excess) ->
    lists:map(fun(N) -> [N] end, Excess);
divide_list_for_removes(Excess, 1) ->
    [Excess];
divide_list_for_removes(Excess, LoopCount) ->
    FetchesPerLoop = length(Excess) div LoopCount,
    LastFetch = length(Excess) - FetchesPerLoop * (LoopCount - 1),
    {[], GroupedFetches} =
        lists:foldl(
            fun(FC, {ENs, GroupedENs}) ->
                {NextGroup, Remainder} = lists:split(FC, ENs),
                {Remainder, GroupedENs ++ [NextGroup]}
            end,
            {Excess, []},
            lists:duplicate(LoopCount - 1, FetchesPerLoop) ++ [LastFetch]
        ),
    GroupedFetches.

%% @private
%% Get active nodes ordered by taking location parameters into account
-spec get_nodes_by_location(
    [node()|undefined], riak_core_ring:riak_core_ring()) -> [node()|undefined].
get_nodes_by_location(Nodes, Ring) ->
    NodesLocations = riak_core_ring:get_nodes_locations(Ring),
    case riak_core_location:has_location_set_in_cluster(NodesLocations) of
        false ->
            Nodes;
        true ->
            LocationNodesD =
                riak_core_location:get_location_nodes(Nodes, NodesLocations),
            stripe_nodes_by_location(LocationNodesD)
    end.

-spec stripe_nodes_by_location(dict:dict()) -> list(node()|undefined).
stripe_nodes_by_location(NodesByLocation) ->
    [LNodes|RestLNodes] =
        sort_lists_by_length(
            lists:map(fun({_L, NL}) -> NL end, dict:to_list(NodesByLocation))),
    stripe_nodes_by_location(RestLNodes, lists:map(fun(N) -> [N] end, LNodes)).

stripe_nodes_by_location([], Acc) ->
    lists:flatten(lists:reverse(sort_lists_by_length(Acc)));
stripe_nodes_by_location([LNodes|OtherLNodes], Acc) ->
    SortedAcc = lists:reverse(sort_lists_by_length(Acc)),
    {UpdatedAcc, []} =
        lists:mapfoldl(
            fun(NodeList, LocationNodesToAdd) ->
                case LocationNodesToAdd of
                    [NodeToAdd|TailNodes] ->
                        {NodeList ++ [NodeToAdd], TailNodes};
                    [] ->
                        {NodeList, []}
                end
            end,
            LNodes,
            SortedAcc),
    stripe_nodes_by_location(OtherLNodes, UpdatedAcc).

sort_lists_by_length(ListOfLists) ->
    lists:sort(fun(L1, L2) -> length(L1) >= length(L2) end, ListOfLists).


%% ===================================================================
%% eunit tests
%% ===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

choose_positions_test() ->
    NodePositions = [{[1,2],l4n5},{[1,2],l5n5},{[1,2],l6n5},{[],l1n6}],
    Positions = [1, 2],
    LocF = fun(N) -> list_to_atom(lists:sublist(atom_to_list(N), 2)) end,
    ?assertMatch(
        [l4n5, l5n5],
        lists:reverse(
            choose_positions(Positions, NodePositions, [], {[], LocF}))).


score_for_adding_test() ->
    PreExcess = [n2, n3, n4],
    PostLoop = [n1, n2, n3],
    PE = lists:zip(lists:reverse(PreExcess), lists:seq(1, length(PreExcess))),
    PL = lists:zip(PostLoop, lists:seq(1, length(PostLoop))),
    Candidates = [n1, n4, n5, n6, n7],
    Shortfall = 4,
    ExpectedResult =
        [[1], [4], [1, 2, 3, 4], [1, 2, 3, 4], [1, 2, 3, 4]],
    ActualResult =
        score_for_adding(PE, PL, Candidates, [], Shortfall),
    ?assertMatch(ExpectedResult, ActualResult).

simple_cluster_t1_test() ->
    RingSize = 32,
    TargetN = 4,
    NodeList = [n1, n2, n3, n4, n5, n6],
    R0 = riak_core_ring:fresh(RingSize, n1),
    R1 =
        lists:foldl(
            fun(N, AccR) -> riak_core_ring:add_member(n1, AccR, N) end,
            R0,
            NodeList -- [n1]),
    Props = [{target_n_val, TargetN}],
    RClaim =
        riak_core_membership_claim:claim(
            R1,
            {riak_core_membership_claim, default_wants_claim},
            {riak_core_claim_location, choose_claim_v4, Props}),
    Failures = meets_target_n(RClaim, TargetN),
    lists:foreach(fun(F) -> io:format("Failure ~p~n", [F]) end, Failures),
    ?assert(length(Failures) == 0).

sort_list_t1_test() ->
    OtherLoc = 
        [[l2n1, l2n2], [l3n1, l3n2], [l4n1, l4n2], [l5n1, l5n2],
            [l6n1], [l7n1], [l8n1]],
    FirstLoc = [[l1n1], [l1n2]],
    NodeList = stripe_nodes_by_location(OtherLoc, FirstLoc),
    ExpectedNodeList =
        [l1n1, l2n2, l3n1, l4n2, l5n1, l7n1,
            l1n2, l2n1, l3n2, l4n1, l5n2, l6n1, l8n1],
    ?assertMatch(
        ExpectedNodeList, NodeList
    ).

prefilter_violations_test_() ->
    % Be strict on test timeout.  Unrefined code took > 10s, whereas the
    % refactored code should be << 1s.
    {timeout, 5, fun prefilter_violations_perf/0}.

prefilter_violations_perf() ->
    JoiningNodes =
        [{l1n2, loc1}, {l1n3, loc1}, {l1n4, loc1},
            {l2n1, loc2}, {l2n2, loc2}, {l2n3, loc2}, {l2n4, loc2},
            {l3n1, loc3}, {l3n2, loc3}, {l3n3, loc3}, {l3n4, loc3},
            {l4n1, loc4}, {l4n2, loc4}, {l4n3, loc4}, {l4n4, loc4},
            {l5n1, loc5}, {l5n2, loc5}, {l5n3, loc5},
            {l6n1, loc6}, {l6n2, loc6}, {l6n3, loc6}, {l6n4, loc6},
            {l7n1, loc7}, {l7n2, loc7}],
    N1 = l1n1,
    N1Loc = loc1,
    RingSize = 4096,
    io:format(
        "Testing NodeList ~w with RingSize ~w~n",
        [[{N1, N1Loc}|JoiningNodes], RingSize]
    ),
    R1 = 
        riak_core_ring:set_node_location(
            N1,
            N1Loc,
            riak_core_ring:fresh(RingSize, N1)),

    RAll =
        lists:foldl(
            fun({N, L}, AccR) ->
                AccR0 = riak_core_ring:add_member(N1, AccR, N),
                riak_core_ring:set_node_location(N, L, AccR0)
            end,
            R1,
            JoiningNodes
        ),
    Owners = riak_core_ring:all_owners(RAll),
    AllIndices =
        lists:zip(
            lists:seq(0, length(Owners)-1), [Idx || {Idx, _} <- Owners]),

    {T0, FilteredIndices0} =
        timer:tc(
            fun prefilter_violations/6,
            [RAll, l1n2, AllIndices, AllIndices, 4, RingSize]),
    io:format("Prefilter violations took ~w ms~n", [T0 div 1000]),
    ?assertMatch(RingSize, length(FilteredIndices0)),
    
    {T1, FilteredIndices1} =
        timer:tc(
            fun prefilter_violations/6,
            [RAll, l2n3, AllIndices, AllIndices, 4, RingSize]),
    io:format("Prefilter violations took ~w ms~n", [T1 div 1000]),
    ?assertMatch(RingSize, length(FilteredIndices1)),
    
    RTrans = riak_core_ring:transfer_node(0, l2n3, RAll),
    {T2, FilteredIndices2} =
        timer:tc(
            fun prefilter_violations/6,
            [RTrans, l2n3, AllIndices, AllIndices, 4, RingSize]),
    io:format("Prefilter violations took ~w ms~n", [T2 div 1000]),
    ?assertMatch(RingSize, length(FilteredIndices2) + 7),
    
    {T3, FilteredIndices3} =
        timer:tc(
            fun prefilter_violations/6,
            [RTrans, l1n2, AllIndices, AllIndices, 4, RingSize]),
    io:format("Prefilter violations took ~w ms~n", [T3 div 1000]),
    io:format("Filtered instances ~w~n", [AllIndices -- FilteredIndices3]),
    ?assertMatch(RingSize, length(FilteredIndices3) + 1),
    
    {T4, FilteredIndices4} =
        timer:tc(
            fun prefilter_violations/6,
            [RTrans, l2n4, AllIndices, AllIndices, 4, RingSize]),
    io:format("Prefilter violations took ~w ms~n", [T4 div 1000]),
    ?assertMatch(RingSize, length(FilteredIndices4) + 7 - 1).

location_seqclaim_t1_test() ->
    JoiningNodes =
        [{n2, loc1},
        {n3, loc2}, {n4, loc2},
        {n5, loc3}, {n6, loc3},
        {n7, loc4}, {n8, loc4},
        {n9, loc5}, {n10, loc5}
    ],
    location_claim_tester(n1, loc1, JoiningNodes, 64),
    location_claim_tester(n1, loc1, JoiningNodes, 128),
    location_claim_tester(n1, loc1, JoiningNodes, 256),
    location_claim_tester(n1, loc1, JoiningNodes, 512),
    location_claim_tester(n1, loc1, JoiningNodes, 1024),
    location_claim_tester(n1, loc1, JoiningNodes, 2048).

location_seqclaim_t2_test() ->
    JoiningNodes =
        [{n2, loc1},
            {n3, loc2}, {n4, loc2},
            {n5, loc3}, {n6, loc3},
            {n7, loc4}, {n8, loc4}
        ],
    location_claim_tester(n1, loc1, JoiningNodes, 64),
    location_claim_tester(n1, loc1, JoiningNodes, 128),
    location_claim_tester(n1, loc1, JoiningNodes, 256),
    location_claim_tester(n1, loc1, JoiningNodes, 512),
    location_claim_tester(n1, loc1, JoiningNodes, 1024),
    location_claim_tester(n1, loc1, JoiningNodes, 2048).

location_seqclaim_t3_test() ->
    JoiningNodes =
        [{n2, loc1},
            {n3, loc2}, {n4, loc2},
            {n5, loc3}, {n6, loc3},
            {n7, loc4}, {n8, loc4},
            {n9, loc5}, {n10, loc5},
            {n11, loc6}, {n12, loc7}, {n13, loc8}
        ],
    location_claim_tester(n1, loc1, JoiningNodes, 64),
    location_claim_tester(n1, loc1, JoiningNodes, 128),
    location_claim_tester(n1, loc1, JoiningNodes, 256),
    location_claim_tester(n1, loc1, JoiningNodes, 512),
    location_claim_tester(n1, loc1, JoiningNodes, 1024),
    location_claim_tester(n1, loc1, JoiningNodes, 2048).

location_seqclaim_t4_test() ->
    JoiningNodes =
        [{l1n2, loc1}, {l1n3, loc1}, {l1n4, loc1},
            {l1n5, loc1}, {l1n6, loc1}, {l1n7, loc1}, {l1n8, loc1},
            {l2n1, loc2}, {l2n2, loc2}, {l2n3, loc2}, {l2n4, loc2},
            {l2n5, loc2}, {l2n6, loc2}, {l2n7, loc2}, {l2n8, loc2},
            {l3n1, loc3}, {l3n2, loc3}, {l3n3, loc3}, {l3n4, loc3},
            {l3n5, loc3}, {l3n6, loc3}, {l3n7, loc3}, {l3n8, loc3},
            {l4n1, loc4}, {l4n2, loc4}, {l4n3, loc4}, {l4n4, loc4},
            {l4n5, loc4}, {l4n6, loc4}, {l4n7, loc4}, {l4n8, loc4},
            {l5n1, loc5}, {l5n2, loc5}, {l5n3, loc5}, {l5n4, loc5},
            {l5n5, loc5}, {l5n6, loc5}, {l5n7, loc5},
            {l6n1, loc6}, {l6n2, loc6}, {l6n3, loc6}, {l6n4, loc6},
            {l6n5, loc6}, {l6n6, loc6}, {l6n7, loc6},
            {l7n1, loc7}, {l7n2, loc7}, {l7n3, loc7}],
    location_claim_tester(l1n1, loc1, JoiningNodes, 128),
    location_claim_tester(l1n1, loc1, JoiningNodes, 256),
    location_claim_tester(l1n1, loc1, JoiningNodes, 512),
    location_claim_tester(l1n1, loc1, JoiningNodes, 1024),
    location_claim_tester(l1n1, loc1, JoiningNodes, 2048).

location_seqclaim_t5_test() ->
    JoiningNodes =
        [{l1n2, loc1}, {l1n3, loc1}, {l1n4, loc1},
            {l2n1, loc2}, {l2n2, loc2}, {l2n3, loc2}, {l2n4, loc2},
            {l3n1, loc3}, {l3n2, loc3}, {l3n3, loc3}, {l3n4, loc3},
            {l4n1, loc4}, {l4n2, loc4}, {l4n3, loc4}, {l4n4, loc4},
            {l5n1, loc5}, {l5n2, loc5}, {l5n3, loc5},
            {l6n1, loc6}, {l6n2, loc6}, {l6n3, loc6}, {l6n4, loc6},
            {l7n1, loc7}, {l7n2, loc7}],
    location_claim_tester(l1n1, loc1, JoiningNodes, 128),
    location_claim_tester(l1n1, loc1, JoiningNodes, 256),
    location_claim_tester(l1n1, loc1, JoiningNodes, 512),
    location_claim_tester(l1n1, loc1, JoiningNodes, 1024),
    location_claim_tester(l1n1, loc1, JoiningNodes, 2048).

location_seqclaim_t6_test() ->
    JoiningNodes =
        [{l1n2, loc1}, {l1n3, loc1}, {l1n4, loc1},
            {l1n5, loc1}, {l1n6, loc1}, {l1n7, loc1}, {l1n8, loc1},
            {l2n1, loc2}, {l2n2, loc2}, {l2n3, loc2}, {l2n4, loc2},
            {l2n5, loc2}, {l2n6, loc2}, {l2n7, loc2}, {l2n8, loc2},
            {l3n1, loc3}, {l3n2, loc3}, {l3n3, loc3}, {l3n4, loc3},
            {l3n5, loc3}, {l3n6, loc3}, {l3n7, loc3}, {l3n8, loc3},
            {l4n1, loc4}, {l4n2, loc4}, {l4n3, loc4}, {l4n4, loc4},
            {l4n5, loc4}, {l4n6, loc4}, {l4n7, loc4}, {l4n8, loc4},
            {l5n1, loc5}, {l5n2, loc5},
            {l6n1, loc6}, {l6n2, loc6}, {l6n3, loc6}, {l6n4, loc6},
            {l6n5, loc6}, {l6n6, loc6}, {l6n7, loc6}, {l6n8, loc8}],
    location_claim_tester(l1n1, loc1, JoiningNodes, 256),
    location_claim_tester(l1n1, loc1, JoiningNodes, 512),
    location_claim_tester(l1n1, loc1, JoiningNodes, 1024),
    location_claim_tester(l1n1, loc1, JoiningNodes, 2048).

location_seqclaim_t7_test() ->
    JoiningNodes =
        [{l1n2, loc1}, {l1n3, loc1},
            {l2n1, loc2}, {l2n2, loc2}, {l2n3, loc2},
            {l3n1, loc3}, {l3n2, loc3}, {l3n3, loc3},
            {l4n1, loc4}, {l4n2, loc4},
            {l5n1, loc5}, {l5n2, loc5},
            {l6n1, loc6}, {l6n2, loc6}],
    location_claim_tester(l1n1, loc1, JoiningNodes, 256),
    location_claim_tester(l1n1, loc1, JoiningNodes, 512),
    location_claim_tester(l1n1, loc1, JoiningNodes, 1024),
    location_claim_tester(l1n1, loc1, JoiningNodes, 2048).

location_claim_tester(N1, N1Loc, NodeLocList, RingSize) ->
    location_claim_tester(
        N1, N1Loc, NodeLocList, RingSize, sequential_claim, 4).

location_claim_tester(
        N1, N1Loc, NodeLocList, RingSize, ClaimFun, TargetN) ->
    io:format(
        "Testing NodeList ~w with RingSize ~w~n",
        [[{N1, N1Loc}|NodeLocList], RingSize]
    ),
    R1 = 
        riak_core_ring:set_node_location(
            N1,
            N1Loc,
            riak_core_ring:fresh(RingSize, N1)),

    RAll =
        lists:foldl(
            fun({N, L}, AccR) ->
                AccR0 = riak_core_ring:add_member(N1, AccR, N),
                riak_core_ring:set_node_location(N, L, AccR0)
            end,
            R1,
            NodeLocList
        ),
    Params =
        case ClaimFun of
            sequential_claim ->
                TargetN;
            choose_claim_v4 ->
                [{target_n_val, 3}]
        end,
    RClaim =
        riak_core_membership_claim:claim(
            RAll,
            {riak_core_membership_claim, default_wants_claim},
            {riak_core_claim_location, ClaimFun, Params}),
    {RingSize, Mappings} = riak_core_ring:chash(RClaim),

    check_for_failures(Mappings, TargetN, RClaim).


check_for_failures(Mappings, TargetN, RClaim) ->
    NLs = riak_core_ring:get_nodes_locations(RClaim),
    LocationMap =
        lists:map(
            fun({Idx, N}) ->
                    {Idx, riak_core_location:get_node_location(N, NLs)}
            end,
            Mappings),
    Prefix = lists:sublist(LocationMap, 3),
    CheckableMap = LocationMap ++ Prefix,
    {_, Failures} =
        lists:foldl(
            fun({Idx, L}, {LastNminus1, Fails}) ->
                case lists:member(L, LastNminus1) of
                    false ->
                        {[L|lists:sublist(LastNminus1, TargetN - 2)], Fails};
                    true ->
                        {[L|lists:sublist(LastNminus1, TargetN - 2)],
                            [{Idx, L, LastNminus1}|Fails]}
                end
            end,
            {[], []},
            CheckableMap
        ),
    lists:foreach(fun(F) -> io:format("Failure ~p~n", [F]) end, Failures),
    ?assert(length(Failures) == 0).


location_multistage_t1_test_() ->
    {timeout, 60, fun location_multistage_t1_tester/0}.

location_multistage_t2_test_() ->
    {timeout, 60, fun location_multistage_t2_tester/0}.

% location_multistage_t3_test_() ->
%     {timeout, 60, fun location_multistage_t3_tester/0}.

location_multistage_t4_test_() ->
    {timeout, 60, fun location_multistage_t4_tester/0}.

location_multistage_t1_tester() ->
    %% This is a tricky corner case where we would fail to meet TargetN for
    %% locations if joining all 9 nodes in one claim (as sequential_claim will
    %% not succeed).  However, If we join 8 nodes, then add the 9th, TargetN
    %% is always achieved
    JoiningNodes =
        [{l1n2, loc1},
            {l2n3, loc2}, {l2n4, loc2},
            {l3n5, loc3}, {l3n6, loc3},
            {l4n7, loc4}, {l4n8, loc4}
        ],
    location_multistage_claim_tester(64, JoiningNodes, 4, l5n9, loc5, 4),
    location_multistage_claim_tester(128, JoiningNodes, 4, l5n9, loc5, 4),
    location_multistage_claim_tester(256, JoiningNodes, 4, l5n9, loc5, 4),
    location_multistage_claim_tester(512, JoiningNodes, 4, l5n9, loc5, 4),
    location_multistage_claim_tester(1024, JoiningNodes, 4, l5n9, loc5, 4),
    location_multistage_claim_tester(2048, JoiningNodes, 4, l5n9, loc5, 4).

location_multistage_t2_tester() ->
    %% This is a tricky corner case as with location_multistage_t1_tester/1,
    %% but now, because the TargetN does not divide evenly by the ring size
    %% only TargetN - 1 can be achieved for locations.
    JoiningNodes =
        [{l1n2, loc1},
            {l2n3, loc2}, {l2n4, loc2},
            {l3n5, loc3}, {l3n6, loc3}
        ],
    location_multistage_claim_tester(64, JoiningNodes, 3, l4n7, loc4, 2),
    location_multistage_claim_tester(128, JoiningNodes, 3, l4n7, loc4, 2),
    location_multistage_claim_tester(256, JoiningNodes, 3, l4n7, loc4, 2),
    location_multistage_claim_tester(512, JoiningNodes, 3, l4n7, loc4, 2),
    location_multistage_claim_tester(1024, JoiningNodes, 3, l4n7, loc4, 2),
    location_multistage_claim_tester(2048, JoiningNodes, 3, l4n7, loc4, 2).

% location_multistage_t3_tester() ->
%     %% This is a minimal case for having TargetN locations, and an uneven
%     %% Alloctaion around the locations.  Is TargetN - 1 still held up
%     JoiningNodes =
%         [{l1n2, loc1},
%             {l2n3, loc2}, {l2n6, loc2},
%             {l3n4, loc3},
%             {l4n5, loc4}
%         ],
%     location_multistage_claim_tester(64, JoiningNodes, 4, l3n7, loc3, 3),
%     location_multistage_claim_tester(128, JoiningNodes, 4, l3n7, loc3, 3),
%     location_multistage_claim_tester(256, JoiningNodes, 4, l3n7, loc3, 3),
%     location_multistage_claim_tester(512, JoiningNodes, 4, l3n7, loc3, 3),
%     location_multistage_claim_tester(1024, JoiningNodes, 4, l3n7, loc3, 3),
%     location_multistage_claim_tester(2048, JoiningNodes, 4, l3n7, loc3, 3).

location_multistage_t4_tester() ->
    JoiningNodes =
        [{l1n2, loc1},
            {l2n3, loc2}, {l2n4, loc2},
            {l3n5, loc3}, {l3n6, loc3},
            {l4n7, loc4}, {l4n8, loc4},
            {l5n9, loc5}
        ],

    location_multistage_claim_tester(64, JoiningNodes, 4, l5n10, loc5, 4),
    location_multistage_claim_tester(128, JoiningNodes, 4, l5n10, loc5, 4),
    location_multistage_claim_tester(256, JoiningNodes, 4, l5n10, loc5, 4),
    location_multistage_claim_tester(512, JoiningNodes, 4, l5n10, loc5, 4),
    location_multistage_claim_tester(1024, JoiningNodes, 4, l5n10, loc5, 4). %,
    % location_multistage_claim_tester(2048, JoiningNodes, 4, l5n10, loc5, 4).

location_multistage_claim_tester(
        RingSize, JoiningNodes, TargetN, NewNode, NewLocation, VerifyN) ->
    SW0 = os:timestamp(),
    N1 = l1n1,
    N1Loc = loc1,
    io:format(
        "Testing NodeList ~w with RingSize ~w~n",
        [[{N1, N1Loc}|JoiningNodes], RingSize]
    ),
    R1 = 
        riak_core_ring:set_node_location(
            N1,
            N1Loc,
            riak_core_ring:fresh(RingSize, N1)),

    RAll =
        lists:foldl(
            fun({N, L}, AccR) ->
                AccR0 = riak_core_ring:add_member(N1, AccR, N),
                riak_core_ring:set_node_location(N, L, AccR0)
            end,
            R1,
            JoiningNodes
        ),
    Params = [{target_n_val, TargetN}],
    SW1 = os:timestamp(),
    RClaimInit =
        riak_core_membership_claim:claim(
            RAll,
            {riak_core_membership_claim, default_wants_claim},
            {riak_core_claim_location, choose_claim_v4, Params}),
    SW2 = os:timestamp(),
    io:format("Reclaiming without committing~n"),

    RingExtendA =
        riak_core_ring:set_node_location(
            NewNode,
            NewLocation,
            riak_core_ring:add_member(N1, RClaimInit, NewNode)),
    RClaimExtendA =
        riak_core_membership_claim:claim(
            RingExtendA,
            {riak_core_membership_claim, default_wants_claim},
            {riak_core_claim_location, choose_claim_v4, Params}),
    
    io:format("Commit initial claim~n"),
    SW3 = os:timestamp(),

    RClaimInitCommit =
        riak_core_ring:increment_vclock(
            node(),
            riak_core_ring:clear_location_changed(RClaimInit)),
    
    io:format("Reclaiming following commit~n"),
    SW4 = os:timestamp(),

    RingExtendB =
        riak_core_ring:set_node_location(
            NewNode,
            NewLocation,
            riak_core_ring:add_member(N1, RClaimInitCommit, NewNode)),
    RClaimExtendB =
        riak_core_membership_claim:claim(
            RingExtendB,
            {riak_core_membership_claim, default_wants_claim},
            {riak_core_claim_location, choose_claim_v4, Params}),
    
    {_RingSizeInit, MappingsInit} = riak_core_ring:chash(RClaimInit),
    {RingSizeA, MappingsA} = riak_core_ring:chash(RClaimExtendA),
    {RingSizeB, MappingsB} = riak_core_ring:chash(RClaimExtendB),

    SW5 = os:timestamp(),

    ?assert(RingSizeA == RingSizeB),
    ?assert(MappingsA == MappingsB),

    io:format("Testing initial Mappings:~n~n~p~n", [MappingsInit]),
    check_for_failures(MappingsInit, VerifyN, RClaimInit),
    io:format("Testing secondary Mappings:~n~n~p~n", [MappingsB]),
    check_for_failures(MappingsB, VerifyN, RClaimExtendB),
    
    SW6 = os:timestamp(),
    io:format(
        "Test for RingSize ~w had timings:"
        "Setup ~w  First Claim ~w  Next Claim ~w Commit ~w Other Claims ~w Verify ~w~n",
        [RingSize,
            timer:now_diff(SW1, SW0) div 1000,
            timer:now_diff(SW2, SW1) div 1000,
            timer:now_diff(SW3, SW2) div 1000,
            timer:now_diff(SW4, SW3) div 1000,
            timer:now_diff(SW5, SW4) div 1000,
            timer:now_diff(SW6, SW5) div 1000]
    ).

location_typical_expansion_test() ->
    location_typical_expansion_tester(256),
    location_typical_expansion_tester(512).

location_typical_expansion_tester(RingSize) ->
    N1 = l1n1,
    N1Loc = loc1,
    TargetN = 4,
    InitJoiningNodes =
        [{l1n2, loc1},
            {l2n3, loc2}, {l2n4, loc2},
            {l3n5, loc3}, {l3n6, loc3},
            {l4n7, loc4}, {l4n8, loc4}],

    io:format(
        "Testing NodeList ~w with RingSize ~w~n",
        [[{N1, N1Loc}|InitJoiningNodes], RingSize]
    ),
    R1 = 
        riak_core_ring:set_node_location(
            N1,
            N1Loc,
            riak_core_ring:fresh(RingSize, N1)),

    RAll =
        lists:foldl(
            fun({N, L}, AccR) ->
                AccR0 = riak_core_ring:add_member(N1, AccR, N),
                riak_core_ring:set_node_location(N, L, AccR0)
            end,
            R1,
            InitJoiningNodes
        ),
    Params = [{target_n_val, TargetN}],
    RClaimInit =
        riak_core_membership_claim:claim(
            RAll,
            {riak_core_membership_claim, default_wants_claim},
            {riak_core_claim_location, choose_claim_v4, Params}),
    {RingSize, MappingsInit} = riak_core_ring:chash(RClaimInit),

    check_for_failures(MappingsInit, TargetN, RClaimInit),

    Stage1Ring =
        lists:foldl(
            fun(JN, R) ->
                riak_core_ring:set_member(node(), R, JN, valid, same_vclock)
            end,
            RClaimInit,
            riak_core_ring:members(RClaimInit, [joining])
        ),
    
    RClaimStage2 = add_node(Stage1Ring, N1, l5n9, loc5, Params),
    {RingSize, Mappings2} = riak_core_ring:chash(RClaimStage2),
    check_for_failures(Mappings2, TargetN, RClaimStage2),
    Stage2Ring = commit_change(RClaimStage2),
        
    RClaimStage3 = add_node(Stage2Ring, N1, l5n10, loc5, Params),
    {RingSize, Mappings3} = riak_core_ring:chash(RClaimStage3),
    check_for_failures(Mappings3, TargetN, RClaimStage3),
    Stage3Ring = commit_change(RClaimStage3),
    
    RClaimStage4 = add_node(Stage3Ring, N1, l6n11, loc6, Params),
    {RingSize, Mappings4} = riak_core_ring:chash(RClaimStage4),
    check_for_failures(Mappings4, TargetN, RClaimStage4),
    Stage4Ring = commit_change(RClaimStage4),
    
    RClaimStage5 = add_node(Stage4Ring, N1, l6n12, loc6, Params),
    {RingSize, Mappings5} = riak_core_ring:chash(RClaimStage5),
    check_for_failures(Mappings5, TargetN, RClaimStage5),
    Stage5Ring = commit_change(RClaimStage5),
    
    RClaimStage6 = add_node(Stage5Ring, N1, l1n13, loc1, Params),
    {RingSize, Mappings6} = riak_core_ring:chash(RClaimStage6),
    check_for_failures(Mappings6, TargetN, RClaimStage6),
    Stage6Ring = commit_change(RClaimStage6),
    
    RClaimStage7 = add_node(Stage6Ring, N1, l2n14, loc2, Params),
    {RingSize, Mappings7} = riak_core_ring:chash(RClaimStage7),
    check_for_failures(Mappings7, TargetN, RClaimStage7),
    Stage7Ring = commit_change(RClaimStage7),

    RClaimStage8 = add_node(Stage7Ring, N1, l3n15, loc3, Params),
    {RingSize, Mappings8} = riak_core_ring:chash(RClaimStage8),
    check_for_failures(Mappings8, TargetN, RClaimStage8),
    Stage8Ring = commit_change(RClaimStage8),
    
    RClaimStage9 = add_node(Stage8Ring, N1, l4n16, loc4, Params),
    {RingSize, Mappings9} = riak_core_ring:chash(RClaimStage9),
    check_for_failures(Mappings9, TargetN, RClaimStage9),
    _Stage9Ring = commit_change(RClaimStage9).


add_node(Ring, Claimant, Node, Location, Params) ->
    RingA = riak_core_ring:add_member(Claimant, Ring, Node),
    RingB = riak_core_ring:set_node_location(Node, Location, RingA),
    RingC =
        riak_core_membership_claim:claim(
            RingB,
            {riak_core_membership_claim, default_wants_claim},
            {riak_core_claim_location, choose_claim_v4, Params}),
    OwnersPre = riak_core_ring:all_owners(RingA),
    OwnersPost = riak_core_ring:all_owners(RingC),
    OwnersZip = lists:zip(OwnersPre, OwnersPost),
    Next =
        [{Idx, PrevOwner, NewOwner, [], awaiting} ||
            {{Idx, PrevOwner}, {Idx, NewOwner}} <- OwnersZip,
            PrevOwner /= NewOwner],
    % StartingNodes = riak_core_ring:all_members(Ring),
    % ExpectedTransferMax = 2 * (length(OwnersPre) div length(StartingNodes)),
    NodeCountD =
        lists:foldl(
            fun({_Idx, N}, D) ->
                dict:update_counter(N, 1, D)
            end,
            dict:new(),
            OwnersPost
        ),
    NodeCounts =
        lists:map(fun({_N, C}) -> C end, dict:to_list(NodeCountD)),
    io:format(
        % user,
        "NodeCounts~w~n",
        [dict:to_list(NodeCountD)]),
    io:format(
        % user,
        "Adding node ~w in location ~w - ~w transfers ~w max ~w min vnodes~n",
        [Node, Location,
            length(Next), lists:max(NodeCounts), lists:min(NodeCounts)]),
    ?assert(
        (lists:min(NodeCounts) == (lists:max(NodeCounts) - 1)) or
        (lists:min(NodeCounts) == lists:max(NodeCounts))
    ),
    % ?assert(length(Next) =< ExpectedTransferMax),
    RingC.

commit_change(Ring) ->
    lists:foldl(
        fun(JN, R) ->
            riak_core_ring:set_member(node(), R, JN, valid, same_vclock)
        end,
        Ring,
        riak_core_ring:members(Ring, [joining])
    ).

-endif.