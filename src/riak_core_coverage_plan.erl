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

    %% Calculate an offset based on the request id to offer
    %% the possibility of different sets of VNodes being
    %% used even when all nodes are available.
    Offset = ReqId rem NVal,

    RingIndexInc = chash:ring_increment(PartitionCount),
    AllKeySpaces = lists:seq(0, PartitionCount - 1),
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
                fun find_coverage/6;
            false ->
                fun initiate_plan/6
        end,

    %% The offset value serves as a tiebreaker in the
    %% compare_next_vnode function and is used to distribute
    %% work to different sets of VNodes.
    CoverageResult =
        CoveragePlanFun(AllKeySpaces,
                        Offset,
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
-spec initiate_plan(list(non_neg_integer()), 
                    non_neg_integer(),
                    pos_integer(),
                    pos_integer(),
                    list(non_neg_integer()),
                    pos_integer()) -> 
                        {ok, list(vnode_covers())} | 
                            {insufficient_vnodes_available, 
                                list(non_neg_integer()), 
                                list(vnode_covers())}.
initiate_plan(AllVnodes, Offset, NVal, PartitionCount, UnavailableVnodes, PVC) ->
    % Order the vnodes for the fold.  Will number each vnode in turn between
    % 1 and NVal.  Then sort by this NVal, so that by default we visit every
    % NVal'th vnode first.
    %
    % In the simple case, with NVal of 3, the list will be sorted with every
    % 3rd vnode together e.g.
    % Offset 0 [0, 3, 6, 9, 12, 15, 1, 4, 7, 10, 13, 2, 5, 8, 11, 14]
    % Offset 1 [2, 5, 8, 11, 14, 0, 3, 6, 9, 12, 15, 1, 4, 7, 10, 13]
    % Offset 2 [1, 4, 7, 10, 13, 2, 5, 8, 11, 14, 0, 3, 6, 9, 12, 15]
    NumberVnodesToNFun =
        fun(Vnode, Incr) ->
            {{Incr, Vnode}, (Incr + 1) rem NVal}
        end,
    {VnodeList, _Incr} = lists:mapfoldl(NumberVnodesToNFun, Offset, AllVnodes),
    % Subtract any Unavailable vnodes.  Must only assign available primary
    % vnodes a role in the coverage plan
    AvailableVnodes =
        lists:subtract(
            lists:map(fun({_S, VN}) ->  VN end, lists:keysort(1, VnodeList)),
            UnavailableVnodes),
    
    % Setup an array for tracking which partition has "Wants" left, starting
    % with a value of PVC
    PartitionWants = array:new(PartitionCount, {default, PVC}),

    develop_plan(AvailableVnodes, NVal, PartitionWants, PartitionCount * PVC, []).


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
    

find_coverage(AllKeySpaces, Offset, NVal, PartitionCount, UnavailableKeySpaces, PVC) ->
    find_coverage(AllKeySpaces, Offset, NVal, PartitionCount, UnavailableKeySpaces, PVC, []).

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
    KeySpaces = lists:seq(0, PartitionCount - 1),
    UnavailableKeySpaces = [],
    PVC = 1,
    {ok, VnodeCovers0} =
        find_coverage(KeySpaces,
                        Offset, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC),
    R0 = [{0, [5, 6, 7]}, {3, [0, 1, 2]}, {5, [3, 4]}],
    ?assertMatch(R0, VnodeCovers0),
    
    UnavailableKeySpaces1 = [3, 7],
    {ok, VnodeCovers1} =
        find_coverage(KeySpaces,
                        Offset, NVal, PartitionCount,
                        UnavailableKeySpaces1,
                        PVC, []),
    % Is the result below the most efficient - still only 3 vnodes to be asked
    % R1 = [{0, [5, 6, 7]}, {2, [0, 1]}, {5, [2, 3, 4]}],
    % Actual result returned needs to cover 4 vnodes
    R1 = [{0, [5, 6, 7]}, {1, [0]}, {4, [1, 2, 3]}, {5, [4]}],
    ?assertMatch(R1, VnodeCovers1),
    
    Offset2 = 1,
    {ok, VnodeCovers2} =
        find_coverage(KeySpaces,
                        Offset2, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC),
    % The result here is effective - as we want the use of offset to lead to
    % distinct choices of cover vnodes, and [2, 4, 7] is distinct to [0, 3, 5]
    R2 = [{2, [0, 1, 7]}, {4, [2, 3]}, {7, [4, 5, 6]}],
    ?assertMatch(R2, VnodeCovers2),
    
    Offset3 = 2,
    {ok, VnodeCovers3} =
        find_coverage(KeySpaces,
                        Offset3, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC),
    R3 = [{1, [0, 6, 7]}, {3, [1, 2]}, {6, [3, 4, 5]}],
    ?assertMatch(R3, VnodeCovers3),
    
    %% The Primay Vnode Count- now set to 2 (effectively a r value of 2)
    %% Each partition must be included twice in the result
    PVC4 = 2,
    {ok, VnodeCovers4} =
        find_coverage(KeySpaces,
                        Offset, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC4),
    R4 = [{0, [5, 6, 7]}, {1, [0, 6, 7]}, {3, [0, 1, 2]}, {4, [1, 2, 3]},
            {5, [3, 4]}, {6, [4, 5]}],
    %% For r of 2 - need to check at n_val * 2 vnodes - so count is optimal
    ?assertMatch(R4, lists:keysort(1, VnodeCovers4)).


eight_vnode_refactor_test() ->
    Offset = 0,
    NVal = 3,
    PartitionCount = 8,
    KeySpaces = lists:seq(0, PartitionCount - 1),
    UnavailableKeySpaces = [],
    PVC = 1,
    {ok, VnodeCovers0} =
        initiate_plan(KeySpaces,
                        Offset, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC),
    % Variation from prefactor in vnode chosen at tail
    R0 = [{0, [5, 6, 7]}, {3, [0, 1, 2]}, {6, [3, 4]}],
    ?assertMatch(R0, VnodeCovers0),
    
    UnavailableKeySpaces1 = [3, 7],
    {ok, VnodeCovers1} =
        initiate_plan(KeySpaces,
                        Offset, NVal, PartitionCount,
                        UnavailableKeySpaces1,
                        PVC),
    % Is the result below the most efficient - still only 3 vnodes to be asked
    % R1 = [{0, [5, 6, 7]}, {2, [0, 1]}, {5, [2, 3, 4]}],
    % Actual result returned needs to cover 4 vnodes
    % Again subtle but unimportant difference with prefactor
    R1 = [{0, [5, 6, 7]}, {1, [0]}, {4, [1, 2]}, {6, [3, 4]}],
    ?assertMatch(R1, VnodeCovers1),
    
    Offset2 = 1,
    {ok, VnodeCovers2} =
        initiate_plan(KeySpaces,
                        Offset2, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC),
    % There is an overlap with vnode choice with Offset of 0 (vnode 0)
    R2 = [{0, [5, 6]}, {2, [0, 1, 7]}, {5, [2, 3, 4]}],
    ?assertMatch(R2, VnodeCovers2),
    
    Offset3 = 2,
    {ok, VnodeCovers3} =
        initiate_plan(KeySpaces,
                        Offset3, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC),
    % No overlap with offset of 1 or 0 - so overall offsets working as
    % expected
    R3 = [{1, [0, 6, 7]}, {4, [1, 2, 3]}, {7, [4, 5]}],
    ?assertMatch(R3, VnodeCovers3),
    
    %% The Primay Vnode Count- now set to 2 (effectively a r value of 2)
    %% Each partition must be included twice in the result
    PVC4 = 2,
    {ok, VnodeCovers4} =
        initiate_plan(KeySpaces,
                        Offset, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC4),
    R4 = [{0, [5, 6, 7]}, {1, [0, 6, 7]}, {3, [0, 1, 2]}, {4, [1, 2, 3]},
            {6, [3, 4, 5]}, {7, [4]}],
    %% For r of 2 - need to check at n_val * 2 vnodes - so count is optimal
    ?assertMatch(R4, lists:keysort(1, VnodeCovers4)).


insufficient_test() ->
    % Insufficient vnodes, requested r of 3 but one vnode is down
    Offset = 0,
    NVal = 3,
    PartitionCount = 16,
    KeySpaces = lists:seq(0, PartitionCount - 1),
    UnavailableKeySpaces = [0],
    PVC = 3,
    {insufficient_vnodes_available, _KS, VnodeCovers0} = 
        initiate_plan(KeySpaces,
                        Offset, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC),
    lists:foreach(fun({I, L}) ->
                        ?assertEqual(3, length(L)),
                        ?assertNotEqual(0, I)
                    end,
                    VnodeCovers0),
    ?assertEqual(VnodeCovers0, lists:ukeysort(1, VnodeCovers0)),
    
    UnavailableKeySpaces1 = [0, 1],
    PVC1 = 2,
    {insufficient_vnodes_available, _KS, VnodeCovers1} = 
        initiate_plan(KeySpaces,
                        Offset, NVal, PartitionCount,
                        UnavailableKeySpaces1,
                        PVC1),
    PC = array:new(PartitionCount, {default, 0}),
    PC1 =
        lists:foldl(
            fun({I, L}, Acc) ->
                ?assertMatch(false, lists:member(I, [0, 1])),
                lists:foldl(fun(P, IA) ->
                                    array:set(P, array:get(P, IA) + 1, IA)
                                end,
                                Acc,
                                L)
            end,
            PC,
            VnodeCovers1),
    lists:foreach(fun({P, C}) ->
                        case lists:member(P, [14, 15]) of
                            true ->
                                ?assertEqual(1, C);
                            false ->
                                ?assertEqual(2, C)
                        end
                    end,
                    array:to_orddict(PC1)).


sixtyfour_vnode_prefactor_test() ->
    sixtyfour_vnode_tester(fun find_coverage/6, 61).

sixtyfour_vnode_refactor_test() ->
    sixtyfour_vnode_tester(fun initiate_plan/6, 63).

sixtyfour_vnode_tester(CoverageFun, PartialVnode) ->
    Offset = 0,
    NVal = 3,
    PartitionCount = 64,
    KeySpaces = lists:seq(0, PartitionCount - 1),
    UnavailableKeySpaces = [],
    PVC = 1,
    {ok, VnodeCovers0} =
        CoverageFun(KeySpaces,
                        Offset, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC),
    R0 = 
        [{0, [61, 62, 63]}, {3, [0, 1, 2]}, {6, [3, 4, 5]},
         {9, [6, 7, 8]}, {12, [9, 10, 11]}, {15, [12, 13, 14]},
         {18, [15, 16, 17]}, {21, [18, 19, 20]}, {24, [21, 22, 23]},
         {27, [24, 25, 26]}, {30, [27, 28, 29]}, {33, [30, 31, 32]},
         {36, [33, 34, 35]}, {39, [36, 37, 38]}, {42, [39, 40, 41]},
         {45, [42, 43, 44]}, {48, [45, 46, 47]}, {51, [48, 49, 50]},
         {54, [51, 52, 53]}, {57, [54, 55, 56]}, {60, [57, 58, 59]},
         {PartialVnode, [60]}],
    ?assertMatch(R0, VnodeCovers0).


refactor_2048ring_test() ->
    ring_tester(2048, fun initiate_plan/6).

prefactor_1024ring_test() ->
    ring_tester(1024, fun find_coverage/6).

prefactor_ns1024ring_test() ->
    nonstandardring_tester(1024, fun find_coverage/6).

refactor_1024ring_test() ->
    ring_tester(1024, fun initiate_plan/6).

refactor_ns1024ring_test() ->
    nonstandardring_tester(1024, fun initiate_plan/6).

prefactor_512ring_test() ->
    ring_tester(512, fun find_coverage/6).

refactor_512ring_test() ->
    ring_tester(512, fun initiate_plan/6).

prefactor_256ring_test() ->
    ring_tester(256, fun find_coverage/6).

refactor_256ring_test() ->
    ring_tester(256, fun initiate_plan/6).

compare_vnodesused_test() ->
    compare_tester(64),
    compare_tester(128),
    compare_tester(256).

compare_tester(RingSize) ->
    PFC = nonstandardring_tester(RingSize, fun find_coverage/6),
    RFC = nonstandardring_tester(RingSize, fun initiate_plan/6),
    % With a little wiggle room - we've not made the number of vnodes
    % used worse
    ?assertMatch(true, RFC =< (PFC +  1)).

prefactor_128ring_test() ->
    ring_tester(256, fun find_coverage/6).

refactor_128ring_test() ->
    ring_tester(256, fun initiate_plan/6).

prefactor_64ring_test() ->
    ring_tester(64, fun find_coverage/6).

refactor_64ring_test() ->
    ring_tester(64, fun initiate_plan/6).

ring_tester(PartitionCount, CoverageFun) ->
    Offset = 0,
    NVal = 3,
    KeySpaces = lists:seq(0, PartitionCount - 1),
    UnavailableKeySpaces = [],
    PVC = 1,
    {ok, VnodeCovers0} =
        CoverageFun(KeySpaces,
                        Offset, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC),
    AccFun =
        fun({A, L}, {VnodeAcc, CoverAcc}) -> {[A|VnodeAcc], CoverAcc ++ L} end,
    {Vnodes, Coverage} = lists:foldl(AccFun, {[], []}, VnodeCovers0),
    CoveredKeySpaces = lists:sort(Coverage),
    ExpVnodeCount = (PartitionCount div NVal) +  1,
    ?assertMatch(KeySpaces, CoveredKeySpaces),
    ?assertMatch(ExpVnodeCount, length(Vnodes)).


nonstandardring_tester(PartitionCount, CoverageFun) ->
    Offset = 2,
    NVal = 3,
    KeySpaces = lists:seq(0, PartitionCount - 1),
    OneDownVnode = rand:uniform(PartitionCount) - 1,
    UnavailableKeySpaces = [OneDownVnode],
    PVC = 2,
    {ok, VnodeCovers} =
        CoverageFun(KeySpaces,
                        Offset, NVal, PartitionCount,
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

