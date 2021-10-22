%% -------------------------------------------------------------------
%%
%% riak_core_coverage_plan: Create a plan to cover a minimal set of VNodes.
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

%% @doc A module to calculate a plan to cover a minimal set of VNodes.
%%      There is also an option to specify a number of primary VNodes
%%      from each preference list to use in the plan.

-module(riak_core_coverage_plan).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([create_plan/5]).

-type index() :: chash:index_as_int().
-type req_id() :: non_neg_integer().
-type coverage_vnodes() :: [{index(), node()}].
-type vnode_filters() :: [{node(), [{index(), [index()]}]}].
-type coverage_plan() :: {coverage_vnodes(), vnode_filters()}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Create a coverage plan to distribute work to a set
%%      covering VNodes around the ring.
-spec create_plan(all | allup, pos_integer(), pos_integer(), 
                  req_id(), atom()) ->
                         {error, term()} | coverage_plan().
create_plan(VNodeSelector, NVal, PVC, ReqId, Service) ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionCount = chashbin:num_partitions(CHBin),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    %% Create a coverage plan with the requested primary
    %% preference list VNode coverage.
    %% Get a list of the VNodes owned by any unavailble nodes
    Members = riak_core_ring:all_members(Ring),
    NonCoverageNodes = [Node || Node <- Members,
                                      riak_core_ring:get_member_meta(Ring, Node, participate_in_coverage) == false],

    DownVNodes = [Index ||
                     {Index, _Node}
                         <- riak_core_apl:offline_owners(Service, CHBin, NonCoverageNodes)],

    RingIndexInc = chash:ring_increment(PartitionCount),
    UnavailableKeySpaces = [(DownVNode div RingIndexInc) || DownVNode <- DownVNodes],
    %% Create function to map coverage keyspaces to
    %% actual VNode indexes and determine which VNode
    %% indexes should be filtered.
    CoverageVNodeFun =
        fun({Position, KeySpaces}, Acc) ->
                %% Calculate the VNode index using the
                %% ring position and the increment of
                %% ring index values.
                VNodeIndex = (Position rem PartitionCount) * RingIndexInc,
                Node = chashbin:index_owner(VNodeIndex, CHBin),
                CoverageVNode = {VNodeIndex, Node},
                case length(KeySpaces) < NVal of
                    true ->
                        %% Get the VNode index of each keyspace to
                        %% use to filter results from this VNode.
                        KeySpaceIndexes = [(((KeySpaceIndex+1) rem
                                             PartitionCount) * RingIndexInc) ||
                                              KeySpaceIndex <- KeySpaces],
                        {CoverageVNode, [{VNodeIndex, KeySpaceIndexes} | Acc]};
                    false ->
                        {CoverageVNode, Acc}
                end
        end,
    
    CoveragePlanFun =
        case application:get_env(riak_core, legacy_coverage_planner, false) of
            true ->
                % Safety net for refactoring, we can still go back to old
                % function if necessary.  Note 35 x performance degradation
                % with this function with ring_size of 1024
                fun find_coverage/5;
            false ->
                fun initiate_plan/5
        end,

    %% The offset value serves as a tiebreaker in the
    %% compare_next_vnode function and is used to distribute
    %% work to different sets of VNodes.
    CoverageResult =
        CoveragePlanFun(ReqId,
                        NVal,
                        PartitionCount,
                        UnavailableKeySpaces,
                        lists:min([PVC, NVal])),
    case CoverageResult of
        {ok, CoveragePlan} ->
            %% Assemble the data structures required for
            %% executing the coverage operation.
            lists:mapfoldl(CoverageVNodeFun, [], CoveragePlan);
        {insufficient_vnodes_available, _KeySpace, PartialCoverage}  ->
            case VNodeSelector of
                allup ->
                    %% The allup indicator means generate a coverage plan
                    %% for any available VNodes.
                    lists:mapfoldl(CoverageVNodeFun, [], PartialCoverage);
                all ->
                    {error, insufficient_vnodes_available}
            end
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

-type vnode_covers() :: {non_neg_integer(), list(non_neg_integer())}.

%% @doc Produce a coverage plan
%% The coverage plan should include all partitions at least PVC times
%% Inputs:
%% AllVnodes - a list of integers, one for each vnode ID
%% Offset - used to produce different coverage plans, changing offset should
%% change the plan, so not all queries for same nval got to same vnodes
%% NVal - the n_val for the bucket used in the query
%% PartitionCount - ring_size, should be the length of AllVnodes
%% UnavailableVnodes - any primary vnodes not available, as either the node is
%% down, or set not to participate_in_coverage
%% PVC - Primary Vnode Count, in effect the r value for the query
-spec initiate_plan(non_neg_integer(),
                    pos_integer(),
                    pos_integer(),
                    list(non_neg_integer()),
                    pos_integer()) -> 
                        {ok, list(vnode_covers())} | 
                            {insufficient_vnodes_available, 
                                list(non_neg_integer()), 
                                list(vnode_covers())}.
initiate_plan(ReqId, NVal, PartitionCount, UnavailableVnodes, PVC) ->
    % Order the vnodes for the fold.  Will number each vnode in turn between
    % 0 and NVal - 1.  Then sort by this Offset, so that by default we visit
    % every NVal'th vnode first, then offset and repeat.
    %
    % Splitting at a random place is critical to ensuring an even distribution
    % of query load across vnodes.  If we always start at the front of the ring
    % then those vnodes that cover the tail of the ring will be involved in a
    % disproprtional number of queries.
    {L1, L2} =
        lists:split(ReqId rem PartitionCount,
                        lists:seq(0, PartitionCount - 1)),
    A0 = array:new(NVal, {default, []}),
    {A1, _} =
        lists:foldl(
            fun(I, {A, Offset}) ->
                    {array:set(Offset, [I|array:get(Offset, A)], A),
                        (Offset + 1) rem NVal}
            end,
            {A0, 0},
            L2 ++ L1
        ),
    OrderedVnodes = lists:flatten(array:to_list(A1)),

    % Setup an array for tracking which partition has "Wants" left, starting
    % with a value of PVC
    PartitionWants = array:new(PartitionCount, {default, PVC}),
    Countdown = PartitionCount * PVC,

    % Subtract any Unavailable vnodes.  Must only assign available primary
    % vnodes a role in the coverage plan
    AvailableVnodes = lists:subtract(OrderedVnodes, UnavailableVnodes),
    
    develop_plan(AvailableVnodes, NVal, PartitionWants, Countdown, []).


develop_plan(_UnusedVnodes, _NVal, _PartitionWants, 0, VnodeCovers) ->
    % Use the countdown to know when to stop, rather than having the cost of
    % checking each entry in the PartitionWants array each loop
    {ok, VnodeCovers};
develop_plan([], _NVal, _PartitionWants, _N, VnodeCovers) ->
    % The previous function coverage_plan/7 returns "KeySpaces" as the second
    % element, which is then ignored - so we don't bother calculating this here
    {insufficient_vnodes_available, [], VnodeCovers};
develop_plan([HeadVnode|RestVnodes], NVal,
                PartitionWants, PartitionCountdown,
                VnodeCovers) ->
    PartitionCount = array:size(PartitionWants),
    % Need to find what partitions are covered by this vnode
    LookBackFun =
        fun(I) -> (PartitionCount + HeadVnode - I) rem PartitionCount end,
    PartsCoveredByHeadNode =
        lists:sort(lists:map(LookBackFun, lists:seq(1, NVal))),
    % For these partitions covered by the vnode, are there any partitions with
    % non-zero wants
    PartsCoveredAndWanted =
        lists:filter(fun(P) -> array:get(P, PartitionWants) > 0 end,
                        PartsCoveredByHeadNode),
    % If there are partitions that are covered by the vnode and have wants,
    % then we should include this vnode in the coverage plan for these
    % partitions.  Otherwise, skip the vnode.
    case length(PartsCoveredAndWanted) of
        L when L > 0 ->
            % Add the vnode to the coverage plan
            VnodeCovers0 =
                lists:sort([{HeadVnode, PartsCoveredAndWanted}|VnodeCovers]),
            % Update the wants, for each partition that has been added to the
            % coverage plan
            UpdateWantsFun =
                fun(P, PWA) -> array:set(P, array:get(P, PWA) - 1, PWA) end,
            PartitionWants0 =
                lists:foldl(UpdateWantsFun,
                            PartitionWants,
                            PartsCoveredAndWanted),
            % Now loop, to find use of the remaining vnodes
            develop_plan(RestVnodes, NVal,
                            PartitionWants0, PartitionCountdown - L,
                            VnodeCovers0);
        _L ->
            develop_plan(RestVnodes, NVal,
                            PartitionWants, PartitionCountdown,
                            VnodeCovers)
    end.
    

find_coverage(ReqId, NVal, PartitionCount, UnavailableKeySpaces, PVC) ->
    AllKeySpaces = lists:seq(0, PartitionCount - 1),
    %% Calculate an offset based on the request id to offer
    %% the possibility of different sets of VNodes being
    %% used even when all nodes are available.
    Offset = ReqId rem NVal,
    find_coverage(AllKeySpaces, Offset, NVal, PartitionCount,
                    UnavailableKeySpaces, PVC, []).

%% @private
-spec find_coverage(list(non_neg_integer()), 
                    non_neg_integer(),
                    pos_integer(),
                    pos_integer(),
                    list(non_neg_integer()),
                    pos_integer(),
                    list(vnode_covers())) -> 
                        {ok, list(vnode_covers())} | 
                            {insufficient_vnodes_available, 
                                list(non_neg_integer()), 
                                list(vnode_covers())}.
find_coverage(AllKeySpaces, Offset, NVal, PartitionCount, UnavailableKeySpaces, PVC, []) ->
    %% Calculate the available keyspaces.
    AvailableKeySpaces = [{((VNode+Offset) rem PartitionCount),
                           VNode,
                           n_keyspaces(VNode, NVal, PartitionCount)}
                          || VNode <- (AllKeySpaces -- UnavailableKeySpaces)],
    case find_coverage_vnodes(
           ordsets:from_list(AllKeySpaces),
           AvailableKeySpaces,
           []) of
        {ok, CoverageResults} ->
            case PVC of
                1 ->
                    {ok, CoverageResults};
                _ ->
                    find_coverage(AllKeySpaces,
                                  Offset,
                                  NVal,
                                  PartitionCount,
                                  UnavailableKeySpaces,
                                  PVC-1,
                                  CoverageResults)
            end;
        Error ->
            Error
    end;
find_coverage(AllKeySpaces,
              Offset,
              NVal,
              PartitionCount,
              UnavailableKeySpaces,
              PVC,
              ResultsAcc) ->
    %% Calculate the available keyspaces. The list of
    %% keyspaces for each vnode that have already been
    %% covered by the plan are subtracted from the complete
    %% list of keyspaces so that coverage plans that
    %% want to cover more one preflist vnode work out
    %% correctly.
    AvailableKeySpaces = [{((VNode+Offset) rem PartitionCount),
                           VNode,
                           n_keyspaces(VNode, NVal, PartitionCount) --
                               proplists:get_value(VNode, ResultsAcc, [])}
                          || VNode <- (AllKeySpaces -- UnavailableKeySpaces)],
    case find_coverage_vnodes(ordsets:from_list(AllKeySpaces),
                              AvailableKeySpaces,
                              ResultsAcc) of
        {ok, CoverageResults} ->
            UpdateResultsFun =
                fun({Key, NewValues}, Results) ->
                        case proplists:get_value(Key, Results) of
                            undefined ->
                                [{Key, NewValues} | Results];
                            Values ->
                                UniqueValues = lists:usort(Values ++ NewValues),
                                [{Key, UniqueValues} |
                                 proplists:delete(Key, Results)]
                        end
                end,
            UpdatedResults =
                lists:foldl(UpdateResultsFun, ResultsAcc, CoverageResults),
            case PVC of
                1 ->
                    {ok, UpdatedResults};
                _ ->
                    find_coverage(AllKeySpaces,
                                  Offset,
                                  NVal,
                                  PartitionCount,
                                  UnavailableKeySpaces,
                                  PVC-1,
                                  UpdatedResults)
            end;
        Error ->
            Error
    end.

%% @private
%% @doc Find the N key spaces for a VNode
n_keyspaces(VNode, N, PartitionCount) ->
    ordsets:from_list([X rem PartitionCount ||
                          X <- lists:seq(PartitionCount + VNode - N,
                                         PartitionCount + VNode - 1)]).

%% @private
%% @doc Find a minimal set of covering VNodes
find_coverage_vnodes([], _, Coverage) ->
    {ok, lists:sort(Coverage)};
find_coverage_vnodes(KeySpace, [], Coverage) ->
    {insufficient_vnodes_available, KeySpace, lists:sort(Coverage)};
find_coverage_vnodes(KeySpace, Available, Coverage) ->
    Res = next_vnode(KeySpace, Available),
    case Res of
        {0, _, _} -> % out of vnodes
            find_coverage_vnodes(KeySpace, [], Coverage);
        {_NumCovered, VNode, _} ->
            {value, {_, VNode, Covers}, UpdAvailable} = lists:keytake(VNode, 2, Available),
            UpdCoverage = [{VNode, ordsets:intersection(KeySpace, Covers)} | Coverage],
            UpdKeySpace = ordsets:subtract(KeySpace, Covers),
            find_coverage_vnodes(UpdKeySpace, UpdAvailable, UpdCoverage)
    end.

%% @private
%% @doc Find the next vnode that covers the most of the
%% remaining keyspace. Use VNode id as tie breaker.
next_vnode(KeySpace, Available) ->
    CoverCount = [{covers(KeySpace, CoversKeys), VNode, TieBreaker} ||
                     {TieBreaker, VNode, CoversKeys} <- Available],
    hd(lists:sort(fun compare_next_vnode/2, CoverCount)).

%% @private
%% There is a potential optimization here once
%% the partition claim logic has been changed
%% so that physical nodes claim partitions at
%% regular intervals around the ring.
%% The optimization is for the case
%% when the partition count is not evenly divisible
%% by the n_val and when the coverage counts of the
%% two arguments are equal and a tiebreaker is
%% required to determine the sort order. In this
%% case, choosing the lower node for the final
%% vnode to complete coverage will result
%% in an extra physical node being involved
%% in the coverage plan so the optimization is
%% to choose the upper node to minimize the number
%% of physical nodes.
compare_next_vnode({CA, _VA, TBA}, {CB, _VB, TBB}) ->
    if
        CA > CB -> %% Descending sort on coverage
            true;
        CA < CB ->
            false;
        true ->
            TBA < TBB %% If equal coverage choose the lower node.
    end.

%% @private
%% @doc Count how many of CoversKeys appear in KeySpace
covers(KeySpace, CoversKeys) ->
    ordsets:size(ordsets:intersection(KeySpace, CoversKeys)).

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

%% Unit testing at moment, but there appear to be some obvious properties:
%% - The output of find_coverage is [{A, [B]}] - where accumulation of all [B]
%% should be equal to the KeySpaces
%% - The accumulation of [A] should not include any unavailable KeySpaces
%% - The length of the list should be optimal? 

eight_vnode_prefactor_test() ->
    Offset = 0,
    NVal = 3,
    PartitionCount = 8,
    UnavailableKeySpaces = [],
    PVC = 1,
    {ok, VnodeCovers0} =
        find_coverage(Offset, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC),
    R0 = [{0, [5, 6, 7]}, {3, [0, 1, 2]}, {5, [3, 4]}],
    ?assertMatch(R0, VnodeCovers0),
    
    UnavailableKeySpaces1 = [3, 7],
    {ok, VnodeCovers1} =
        find_coverage(Offset, NVal, PartitionCount,
                        UnavailableKeySpaces1,
                        PVC),
    % Is the result below the most efficient - still only 3 vnodes to be asked
    % R1 = [{0, [5, 6, 7]}, {2, [0, 1]}, {5, [2, 3, 4]}],
    % Actual result returned needs to cover 4 vnodes
    R1 = [{0, [5, 6, 7]}, {1, [0]}, {4, [1, 2, 3]}, {5, [4]}],
    ?assertMatch(R1, VnodeCovers1),
    
    Offset2 = 1,
    {ok, VnodeCovers2} =
        find_coverage(Offset2, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC),
    % The result here is effective - as we want the use of offset to lead to
    % distinct choices of cover vnodes, and [2, 4, 7] is distinct to [0, 3, 5]
    R2 = [{2, [0, 1, 7]}, {4, [2, 3]}, {7, [4, 5, 6]}],
    ?assertMatch(R2, VnodeCovers2),
    
    Offset3 = 2,
    {ok, VnodeCovers3} =
        find_coverage(Offset3, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC),
    R3 = [{1, [0, 6, 7]}, {3, [1, 2]}, {6, [3, 4, 5]}],
    ?assertMatch(R3, VnodeCovers3),
    
    %% The Primay Vnode Count- now set to 2 (effectively a r value of 2)
    %% Each partition must be included twice in the result
    PVC4 = 2,
    {ok, VnodeCovers4} =
        find_coverage(Offset, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC4),
    R4 = [{0, [5, 6, 7]}, {1, [0, 6, 7]}, {3, [0, 1, 2]}, {4, [1, 2, 3]},
            {5, [3, 4]}, {6, [4, 5]}],
    %% For r of 2 - need to check at n_val * 2 vnodes - so count is optimal
    ?assertMatch(R4, lists:keysort(1, VnodeCovers4)).


changing_repeated_vnode_CODEFAIL_prefactor_test() ->
    MaxCount = changing_repeated_vnode_tester(fun find_coverage/5, 100),

    % This assertion is bad!
    % This demonstrates that the prefactored code fails the test of providing
    % well distributed coverage plans
    ?assertEqual(true, MaxCount > 50).

changing_repeated_vnode_refactor_test() ->
    MaxCount = changing_repeated_vnode_tester(fun initiate_plan/5, 100),

    ?assertEqual(true, MaxCount < 50).


changing_repeated_vnode_tester(CoverageFun, Runs) ->
    % Confirm that the vnode that overlaps (i.e. appears in all the offsets)
    % will change between runs
    Partitions = lists:seq(0, 31),
    GetVnodesUsedFun =
        fun(_I) ->
            R = rand:uniform(99999),
            {Vnodes0, Coverage0} =
                ring_tester(32, CoverageFun, R, 3, [], 1),
            ?assertEqual(Partitions, Coverage0),
            Vnodes0
        end,
    AllVnodes = lists:flatten(lists:map(GetVnodesUsedFun, lists:seq(1, Runs))),
    CountVnodesFun =
        fun(V, Acc) ->
            case lists:keyfind(V, 1, Acc) of
                {V, C} ->
                    lists:ukeysort(1, [{V, C + 1}|Acc]);
                false ->
                    lists:ukeysort(1, [{V, 1}|Acc])
            end
        end,
    [{Vnode, MaxCount}|_RestCounts] = 
        lists:reverse(
            lists:keysort(2, lists:foldl(CountVnodesFun, [], AllVnodes))),
    
    io:format(user,
                "~nVnode=~w MaxCount=~w out of 100 queries~n",
                [Vnode, MaxCount]),

    MaxCount.


refactor_2048ring_test() ->
    ring_tester(2048, fun initiate_plan/5).

prefactor_1024ring_test() ->
    ring_tester(1024, fun find_coverage/5).

prefactor_ns1024ring_test() ->
    nonstandardring_tester(1024, fun find_coverage/5).

refactor_1024ring_test() ->
    ring_tester(1024, fun initiate_plan/5).

refactor_ns1024ring_test() ->
    nonstandardring_tester(1024, fun initiate_plan/5).

prefactor_512ring_test() ->
    ring_tester(512, fun find_coverage/5).

refactor_512ring_test() ->
    ring_tester(512, fun initiate_plan/5).

prefactor_256ring_test() ->
    ring_tester(256, fun find_coverage/5).

refactor_256ring_test() ->
    ring_tester(256, fun initiate_plan/5).

prefactor_128ring_test() ->
    ring_tester(256, fun find_coverage/5).

refactor_128ring_test() ->
    ring_tester(256, fun initiate_plan/5).

prefactor_64ring_test() ->
    ring_tester(64, fun find_coverage/5).

refactor_64ring_test() ->
    ring_tester(64, fun initiate_plan/5).

compare_vnodesused_test() ->
    compare_tester(64),
    compare_tester(128),
    compare_tester(256).

compare_tester(RingSize) ->
    PFC = nonstandardring_tester(RingSize, fun find_coverage/5),
    RFC = nonstandardring_tester(RingSize, fun initiate_plan/5),
    % With a little wiggle room - we've not made the number of vnodes
    % used worse
    ?assertMatch(true, RFC =< (PFC +  1)).

ring_tester(PartitionCount, CoverageFun) ->
    Offset = 0,
    NVal = 3,
    UnavailableKeySpaces = [],
    PVC = 1,
    {Vnodes, CoveredKeySpaces} =
        ring_tester(PartitionCount, CoverageFun,
                    Offset, NVal, UnavailableKeySpaces, PVC),
    ExpVnodeCount = (PartitionCount div NVal) +  2,
    KeySpaces = lists:seq(0, PartitionCount - 1),
    ?assertMatch(KeySpaces, CoveredKeySpaces),
    ?assertMatch(true, length(Vnodes) =< ExpVnodeCount).

ring_tester(PartitionCount, CoverageFun,
            Offset, NVal, UnavailableKeySpaces, PVC) ->
    {ok, VnodeCovers0} =
        CoverageFun(Offset, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC),
    AccFun =
        fun({A, L}, {VnodeAcc, CoverAcc}) -> {[A|VnodeAcc], CoverAcc ++ L} end,
    {Vnodes, Coverage} = lists:foldl(AccFun, {[], []}, VnodeCovers0),
    {Vnodes, lists:sort(Coverage)}.


nonstandardring_tester(PartitionCount, CoverageFun) ->
    Offset = 2,
    NVal = 3,
    OneDownVnode = rand:uniform(PartitionCount) - 1,
    UnavailableKeySpaces = [OneDownVnode],
    PVC = 2,
    {ok, VnodeCovers} =
        CoverageFun(Offset, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC),
    
    PC = array:new(PartitionCount, {default, 0}),
    PC1 =
        lists:foldl(
            fun({I, L}, Acc) ->
                ?assertNotEqual(OneDownVnode, I),
                lists:foldl(fun(P, IA) ->
                                    array:set(P, array:get(P, IA) + 1, IA)
                                end,
                                Acc,
                                L)
            end,
            PC,
            VnodeCovers),
    lists:foreach(fun(C) -> ?assertEqual(2, C) end, array:to_list(PC1)),
    length(VnodeCovers).


-endif.

