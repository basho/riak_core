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

%% API
-export([create_plan/5]).

-type index() :: non_neg_integer().
-type vnode() :: {index(), node()}.
-type coverage_vnodes() :: [vnode()].
-type vnode_filters() :: [{node(), [{index(), [index()]}]}].
-type coverage_plan() :: {coverage_vnodes(), vnode_filters()}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Create a coverage plan to distribute work to a set
%%      covering VNodes around the ring.
-spec create_plan(all | allup, pos_integer(), pos_integer(),
                  non_neg_integer(), atom()) ->
                         {error, term()} | coverage_plan().
create_plan(VNodeSelector, NVal, PVC, Offset, Service) ->
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
    CoverageResult = find_coverage(AllKeySpaces,
                                   Offset,
                                   NVal,
                                   PartitionCount,
                                   [(DownVNode div RingIndexInc) ||
                                       DownVNode <- DownVNodes], % Unavail keyspaces
                                   lists:min([PVC, NVal]),
                                   []),
    case CoverageResult of
        {ok, CoveragePlan} ->
            %% Assemble the data structures required for
            %% executing the coverage operation.
            lists:mapfoldl(coverage_vnode_fun(Ring, RingIndexInc, NVal, PartitionCount),
                           [],
                           CoveragePlan);
        {insufficient_vnodes_available, _KeySpace, PartialCoverage}  ->
            case VNodeSelector of
                allup ->
                    %% The allup indicator means generate a coverage plan
                    %% for any available VNodes.
                    lists:mapfoldl(coverage_vnode_fun(Ring, RingIndexInc, NVal, PartitionCount),
                                   [],
                                   PartialCoverage);
                all ->
                    {error, insufficient_vnodes_available}
            end
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

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

-spec get_vnode(term(), pos_integer(), pos_integer(), pos_integer()) -> vnode().
get_vnode(Ring, RingIndexInc, PartitionCount, Position) ->
    %% Calculate the VNode index using the ring position and the
    %% increment of ring index values.
    VNodeIndex = (Position rem PartitionCount) * RingIndexInc,
    Node = riak_core_ring:index_owner(Ring, VNodeIndex),
    {VNodeIndex, Node}.

%% @private
find_coverage(_, _, _, _, _, 0, ResultsAcc) ->
    {ok, ResultsAcc};
find_coverage(AllKeySpaces, Offset, NVal, PartitionCount,
              UnavailableKeySpaces, PVC, ResultsAcc) ->
    %% Calculate the available keyspaces. The list of keyspaces for
    %% each vnode that have already been covered by the plan are
    %% subtracted from the complete list of keyspaces so that coverage
    %% plans that want to cover more one preflist vnode work out
    %% correctly.
    AvailableKeySpaces = [{((VNode+Offset) rem PartitionCount),
                           VNode,
                           n_keyspaces(VNode, NVal, PartitionCount) --
                               proplists:get_value(VNode, ResultsAcc, [])}
                          || VNode <- (AllKeySpaces -- UnavailableKeySpaces)],
    case handle_coverage_vnodes(find_coverage_vnodes(ordsets:from_list(AllKeySpaces),
                              AvailableKeySpaces,
                              ResultsAcc), ResultsAcc) of
        {ok, CoverageResults} ->
            find_coverage(AllKeySpaces,
                          Offset,
                          NVal,
                          PartitionCount,
                          UnavailableKeySpaces,
                          PVC-1,
                          CoverageResults);
        Error ->
            Error
    end.

handle_coverage_vnodes({ok, CoverageResults}, []) ->
    CoverageResults;
handle_coverage_vnodes({ok, CoverageResults}, Acc) ->
    lists:foldl(fun augment_coverage_results/2, Acc, CoverageResults);
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

%% @private There is a potential optimization here once the partition
%% claim logic has been changed so that physical nodes claim
%% partitions at regular intervals around the ring.  The optimization
%% is for the case when the partition count is not evenly divisible by
%% the n_val and when the coverage counts of the two arguments are
%% equal and a tiebreaker is required to determine the sort order. In
%% this case, choosing the lower node for the final vnode to complete
%% coverage will result in an extra physical node being involved in
%% the coverage plan so the optimization is to choose the upper node
%% to minimize the number of physical nodes.
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
