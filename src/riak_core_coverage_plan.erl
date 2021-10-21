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
    %% The offset value serves as a tiebreaker in the
    %% compare_next_vnode function and is used to distribute
    %% work to different sets of VNodes.
    CoverageResult = find_coverage(AllKeySpaces,
                                   Offset,
                                   NVal,
                                   PartitionCount,
                                   UnavailableKeySpaces,
                                   lists:min([PVC, NVal]),
                                   []),
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

eight_vnode_test() ->
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
                        PVC, []),
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
                        PVC, []),
    % The result here is effective - as we want the use of offset to lead to
    % distinct choices of cover vnodes, and [2, 4, 7] is distinct to [0, 3, 5]
    R2 = [{2, [0, 1, 7]}, {4, [2, 3]}, {7, [4, 5, 6]}],
    ?assertMatch(R2, VnodeCovers2),
    
    Offset3 = 2,
    {ok, VnodeCovers3} =
        find_coverage(KeySpaces,
                        Offset3, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC, []),
    R3 = [{1, [0, 6, 7]}, {3, [1, 2]}, {6, [3, 4, 5]}],
    ?assertMatch(R3, VnodeCovers3).


sixtyfour_vnode_test() ->
    Offset = 0,
    NVal = 3,
    PartitionCount = 64,
    KeySpaces = lists:seq(0, PartitionCount - 1),
    UnavailableKeySpaces = [],
    PVC = 1,
    {ok, VnodeCovers0} =
        find_coverage(KeySpaces,
                        Offset, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC, []),
    R0 = 
        [{0, [61, 62, 63]}, {3, [0, 1, 2]}, {6, [3, 4, 5]},
         {9, [6, 7, 8]}, {12, [9, 10, 11]}, {15, [12, 13, 14]},
         {18, [15, 16, 17]}, {21, [18, 19, 20]}, {24, [21, 22, 23]},
         {27, [24, 25, 26]}, {30, [27, 28, 29]}, {33, [30, 31, 32]},
         {36, [33, 34, 35]}, {39, [36, 37, 38]}, {42, [39, 40, 41]},
         {45, [42, 43, 44]}, {48, [45, 46, 47]}, {51, [48, 49, 50]},
         {54, [51, 52, 53]}, {57, [54, 55, 56]}, {60, [57, 58, 59]},
         {61, [60]}],
    ?assertMatch(R0, VnodeCovers0).


bigring_test() ->
    Offset = 0,
    NVal = 3,
    PartitionCount = 1024,
    KeySpaces = lists:seq(0, PartitionCount - 1),
    UnavailableKeySpaces = [],
    PVC = 1,
    {ok, VnodeCovers0} =
        find_coverage(KeySpaces,
                        Offset, NVal, PartitionCount,
                        UnavailableKeySpaces,
                        PVC, []),
    AccFun =
        fun({A, L}, {VnodeAcc, CoverAcc}) -> {[A|VnodeAcc], CoverAcc ++ L} end,
    {Vnodes, Coverage} = lists:foldl(AccFun, {[], []}, VnodeCovers0),
    CoveredKeySpaces = lists:sort(Coverage),
    ExpVnodeCount = (PartitionCount div NVal) +  1,
    ?assertMatch(KeySpaces, CoveredKeySpaces),
    ?assertMatch(ExpVnodeCount, length(Vnodes)).

-endif.

