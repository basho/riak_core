%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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
%% There is also an option to specify a number of primary VNodes from
%% each preference list to use in the plan.

-module(riak_core_coverage_plan).

%% API
-export([create_plan/5,
         create_plan/6,
         print_coverage_plan/1]).

-type index() :: non_neg_integer().
-type vnode() :: {index(), node()}.
-type coverage_vnodes() :: [vnode()].
-type vnode_filters() :: [{node(), [{index(), [index()]}]}].
-type coverage_plan() :: {coverage_vnodes(), vnode_filters()}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Create a coverage plan to distribute work to a set of
%% covering VNodes around the ring.
-spec create_plan(all | allup, pos_integer(), pos_integer(),
                  non_neg_integer(), atom()) ->
                         {error, term()} | coverage_plan().
create_plan(VNodeConstraint, NVal, Primaries, Offset, Service) ->
    create_plan(VNodeConstraint, NVal, Primaries, Offset, Service, vnodes).

%% @doc Create a coverage plan to distribute work to a set
%% covering VNodes around the ring.
-spec create_plan(all | allup,
                  pos_integer(),
                  pos_integer(),
                  non_neg_integer(),
                  atom(),
                  nodes | vnodes) ->
                         {error, term()} | coverage_plan().
create_plan(VNodeConstraint, NVal, Primaries, Offset, Service, MinimizationTarget) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    PartitionCount = riak_core_ring:num_partitions(Ring),
    %% Check which nodes are up for the specified service
    %% so we can determine which VNodes are ineligible
    %% to be part of the coverage plan.
    UpNodes = riak_core_node_watcher:nodes(Service),
    %% Create a coverage plan with the requested primary
    %% preference list VNode coverage.
    %% Get a list of the VNodes owned by any unavailble nodes
    DownVNodes = [Index || {Index, Node} <- riak_core_ring:all_owners(Ring),
                           not lists:member(Node, UpNodes)],
    RingIndexInc = chash:ring_increment(PartitionCount),
    AllKeySpaces = lists:seq(0, PartitionCount - 1),
    %% The offset value serves as a tiebreaker in the
    %% compare_next_vnode function and is used to distribute
    %% work to different sets of VNodes.
    CoverageResult =
        find_coverage(AllKeySpaces,
                      Offset,
                      NVal,
                      PartitionCount,
                      MinimizationTarget,
                      [(DownVNode div RingIndexInc) ||
                          DownVNode <- DownVNodes], % Unavail keyspaces
                      lists:min([Primaries, NVal]),
                      [],
                      vnode_fun(Ring, RingIndexInc, PartitionCount)),
    handle_coverage_result(
      CoverageResult,
      VNodeConstraint,
      Ring,
      RingIndexInc,
      NVal,
      PartitionCount).

handle_coverage_result({ok, CoveragePlan},
                       _,
                       Ring,
                       RingIndexInc,
                       NVal,
                       PartitionCount) ->
    %% Assemble the data structures required for
    %% executing the coverage operation.
    FP = lists:mapfoldl(coverage_vnode_fun(Ring, RingIndexInc, NVal, PartitionCount),
                   [],
                        CoveragePlan),
    print_coverage_plan(FP),
    FP;
handle_coverage_result({insufficient_vnodes_available, _KeySpace, PartialCoverage},
                       allup,
                       Ring,
                       RingIndexInc,
                       NVal,
                       PartitionCount) ->
    %% The allup indicator means generate a coverage plan
    %% for any available VNodes.
    lists:mapfoldl(coverage_vnode_fun(Ring, RingIndexInc, NVal, PartitionCount),
                   [],
                   PartialCoverage);
handle_coverage_result({insufficient_vnodes_available, _, _},
                       all,
                       _, _, _, _) ->
    {error, insufficient_vnodes_available}.

print_coverage_plan({Vnodes, _Filters}) ->
    SortedVnodes = lists:sort(fun sort_by_node/2, Vnodes),
    io:format("Coverage Plan~n"),
    [io:format("~p~n", [Vnode]) || Vnode <- SortedVnodes],
    io:format("~n").

%% ====================================================================
%% Internal functions
%% ====================================================================

sort_by_node({Index1, Node}, {Index2, Node}) ->
    Index1 < Index2;
sort_by_node({_, Node1}, {_, Node2}) ->
    Node1 < Node2.

%% @doc Return a function to map coverage keyspaces to actual VNode
%% indexes and determine which VNode indexes should be filtered.
coverage_vnode_fun(Ring, RingIndexInc, NVal, PartitionCount) ->
    fun({Position, KeySpaces}, Acc)  when length(KeySpaces) < NVal ->
            %% Get the VNode index of each keyspace to
            %% use to filter results from this VNode.
            KeySpaceIndexes = [(((KeySpaceIndex+1) rem
                                 PartitionCount) * RingIndexInc) ||
                                  KeySpaceIndex <- KeySpaces],
            {VNodeIndex, _} = VNode = get_vnode(Ring,
                                                RingIndexInc,
                                                PartitionCount,
                                                Position),
            {VNode, [{VNodeIndex, KeySpaceIndexes} | Acc]};
       ({Position, _KeySpaces}, Acc) ->
            VNode = get_vnode(Ring, RingIndexInc, PartitionCount, Position),
            {VNode, Acc}

    end.

vnode_fun(Ring, RingIndexInc, PartitionCount) ->
    fun(Position) ->
            get_vnode(Ring, RingIndexInc, PartitionCount, Position)
    end.

-spec get_vnode(term(), pos_integer(), pos_integer(), pos_integer()) -> vnode().
get_vnode(Ring, RingIndexInc, PartitionCount, Position) ->
    %% Calculate the VNode index using the ring position and the
    %% increment of ring index values.
    VNodeIndex = (Position rem PartitionCount) * RingIndexInc,
    Node = riak_core_ring:index_owner(Ring, VNodeIndex),
    {VNodeIndex, Node}.

%% @private
find_coverage(_, _, _, _, _, _, 0, ResultsAcc, _) ->
    {ok, ResultsAcc};
find_coverage(AllKeySpaces, Offset, NVal, PartitionCount, MinimizeFor,
              UnavailableKeySpaces, Primaries, ResultsAcc, VnodeFun) ->
    %% Calculate the available keyspaces. The list of keyspaces for
    %% each vnode that have already been covered by the plan are
    %% subtracted from the complete list of keyspaces so that coverage
    %% plans that want to cover more one preflist vnode work out
    %% correctly.
    AvailableKeySpaces =
        [
          {VNode,
           n_keyspaces(VNode, NVal, PartitionCount) --
               proplists:get_value(VNode, ResultsAcc, []),
           (VNode+Offset) rem PartitionCount
          }
         || VNode <- (AllKeySpaces -- UnavailableKeySpaces)],
    case handle_coverage_vnodes(
           find_coverage_vnodes(ordsets:from_list(AllKeySpaces),
                                AvailableKeySpaces,
                                ResultsAcc,
                                VnodeFun,
                                dict:new(),
                                MinimizeFor), ResultsAcc) of
        {ok, CoverageResults} ->
            find_coverage(AllKeySpaces,
                          Offset,
                          NVal,
                          PartitionCount,
                          MinimizeFor,
                          UnavailableKeySpaces,
                          Primaries-1,
                          CoverageResults,
                          VnodeFun);
        Error ->
            Error
    end.

handle_coverage_vnodes({ok, _}=CoverageResults, []) ->
    CoverageResults;
handle_coverage_vnodes({ok, CoverageVnodes}, Acc) ->
    {ok, lists:foldl(fun augment_coverage_results/2, Acc, CoverageVnodes)};
handle_coverage_vnodes({error, _}=Error, _) ->
    Error.

augment_coverage_results({Key, NewValues}, Results) ->
    case proplists:get_value(Key, Results) of
        undefined ->
            [{Key, NewValues} | Results];
        Values ->
            UniqueValues = lists:usort(Values ++ NewValues),
            [{Key, UniqueValues} |
             proplists:delete(Key, Results)]
    end.

%% @private
%% @doc Find the N key spaces for a VNode
-spec n_keyspaces(non_neg_integer(), pos_integer(), pos_integer()) -> ordsets:new().
n_keyspaces(VNodeIndex, N, PartitionCount) ->
  LB = PartitionCount + VNodeIndex - N,
    UB = PartitionCount + VNodeIndex - 1,
    ordsets:from_list([X rem PartitionCount || X <- lists:seq(LB, UB)]).

%% @private
%% @doc Find a minimal set of covering VNodes
find_coverage_vnodes([], _, Coverage, _, _, _) ->
    {ok, lists:sort(Coverage)};
find_coverage_vnodes(KeySpace, [], Coverage, _, _, _) ->
    {insufficient_vnodes_available, KeySpace, lists:sort(Coverage)};
find_coverage_vnodes(KeySpace, Available, Coverage, VnodeFun, NodeCounts, MinimizeFor) ->
    {NumCovered, Position, TB, {_, Node}=_Vnode, _} =
        next_vnode(
          [{Pos, covers(KeySpace, CoversKeys), TB} ||
              {Pos, CoversKeys, TB} <- Available],
          VnodeFun,
          NodeCounts,
          MinimizeFor),
    lager:debug("Next vnode: ~p ~p ~p ~p", [Position, TB, NumCovered, Node]),
    UpdNodeCounts = dict:update_counter(Node, 1, NodeCounts),
    case NumCovered of
        0 -> % out of vnodes
            find_coverage_vnodes(KeySpace, [], Coverage, VnodeFun, UpdNodeCounts, MinimizeFor);
        _ ->
            {value, {Position, Covers, _}, UpdAvailable} =
                lists:keytake(Position, 1, Available),
            lager:debug("KS: ~p Covers: ~p", [KeySpace, Covers]),
            UpdCoverage = [{Position, ordsets:intersection(KeySpace, Covers)} | Coverage],
            UpdKeySpace = ordsets:subtract(KeySpace, Covers),
            find_coverage_vnodes(UpdKeySpace,
                                 UpdAvailable,
                                 UpdCoverage,
                                 VnodeFun,
                                 UpdNodeCounts,
                                 MinimizeFor)
    end.

%% @private
%% @doc Find the next vnode that covers the most of the
%% remaining keyspace. Use VNode id as tie breaker.
-spec next_vnode([{non_neg_integer(), [non_neg_integer()]}], function(), dict(), nodes | vnodes) ->
                        non_neg_integer().
next_vnode(EligibleVnodes, VnodeFun, NodeCounts, MinimizeFor) ->
    lists:foldl(vnode_compare_fun(VnodeFun, NodeCounts, MinimizeFor), [], EligibleVnodes).

vnode_compare_fun(VnodeFun, NodeCounts, vnodes) ->
    fun({A, ACoverCount, ATB}, []) ->
            VnodeA = VnodeFun(A),
            {ACoverCount, A, ATB, VnodeA, NodeCounts};
       ({A, ACoverCount, ATB}, {BCoverCount, B, BTB, {_, NodeB}=VnodeB, _}) ->
            VnodeA = VnodeFun(A),
            compare_vnodes({ACoverCount, A, ATB, VnodeA, 0},
                           {BCoverCount, B, BTB, VnodeB, 0})
    end;
vnode_compare_fun(VnodeFun, NodeCounts, nodes) ->
    fun({A, ACoverCount, ATB}, []) ->
            VnodeA = VnodeFun(A),
            {ACoverCount, A, ATB, VnodeA, NodeCounts};
       ({A, ACoverCount, ATB}, {BCoverCount, B, BTB, {_, NodeB}=VnodeB, _}) ->
            {_, NodeA} = VnodeA = VnodeFun(A),
            compare_vnodes({ACoverCount, A, ATB, VnodeA, dict:find(NodeA, NodeCounts)},
                           {BCoverCount, B, BTB, VnodeB, dict:find(NodeB, NodeCounts)})
    end.

compare_vnodes({ACoverCount, _, _, _, _}=A, {BCoverCount, _, _, _, _})
  when ACoverCount > BCoverCount ->
    A;
compare_vnodes({ACoverCount, _, _, _, _}, {BCoverCount, _, _, _, _}=B)
  when ACoverCount < BCoverCount ->
    B;
compare_vnodes({_, _, ATB, _, NodeCount}=A, {_, _, BTB, _, NodeCount})
  when ATB =< BTB ->
    A;
compare_vnodes({_, _, ATB, _, NodeCount}, {_, _, BTB, _, NodeCount}=B)
  when ATB > BTB ->
    B;
compare_vnodes({_, _, _, _, error}, {_, _, _, _, _}=B) ->
    B;
compare_vnodes({_, _, _, _, _}=A, {_, _, _, _, error}) ->
    A;
compare_vnodes({_, _, _, _, {ok, ANodeCount}}=A, {_, _, _, _, {ok, BNodeCount}})
  when ANodeCount >= BNodeCount ->
    A;
compare_vnodes({_, _, _, _, {ok, ANodeCount}}, {_, _, _, _, {ok, BNodeCount}}=B)
  when ANodeCount < BNodeCount ->
    B.

%% @private
%% @doc Count how many of CoversKeys appear in KeySpace
-spec covers(ordsets:ordset(), ordsets:ordset()) -> non_neg_integer().
covers(KeySpace, CoversKeys) ->
    ordsets:size(ordsets:intersection(KeySpace, CoversKeys)).
