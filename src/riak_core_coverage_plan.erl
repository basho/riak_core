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


%% Example "traditional" coverage plan for a two node, 8 vnode cluster
%% at nval=3
%% {
%%   %% First component is a list of {vnode hash, node name} tuples
%%   [
%%    {0, 'dev1@127.0.0.1'},
%%    {548063113999088594326381812268606132370974703616, 'dev2@127.0.0.1'},
%%    {913438523331814323877303020447676887284957839360, 'dev2@127.0.0.1'}
%%   ],

%%   %% Second component is a list of {vnode hash, [partition list]}
%%   %% tuples representing filters when not all partitions managed by a
%%   %% vnode are required to complete the coverage plan
%%  [
%%   {913438523331814323877303020447676887284957839360,
%%    [730750818665451459101842416358141509827966271488,
%%     913438523331814323877303020447676887284957839360]
%%   }
%%  ]
%% }

%% Snippet from a new-style coverage plan for a two node, 8 vnode
%% cluster at nval=3, with each partition represented twice for up to
%% 16 parallel queries

%% XXX: think about including this in the comments here
%% {1, node, 0.0}
%% {1, node, 0.5}

%% [
%%  %% Second vnode, first half of first partition
%%  {182687704666362864775460604089535377456991567872,
%%   'dev2@127.0.0.1', {0, 156}
%%  },
%%  %% Second vnode, second half of first partition
%%  {182687704666362864775460604089535377456991567872,
%%   'dev2@127.0.0.1', {1, 156}},
%%  %% Third vnode, first half of second partition
%%  {365375409332725729550921208179070754913983135744,
%%   'dev1@127.0.0.1', {2, 156}},
%%  %% Third vnode, second half of second partition
%%  {365375409332725729550921208179070754913983135744,
%%   'dev1@127.0.0.1', {3, 156}},
%%  ...
%% ]

-module(riak_core_coverage_plan).

-include("riak_core_vnode.hrl").

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([create_plan/5, create_subpartition_plan/6]).
-export([replace_subpartition_chunk/7, replace_traditional_chunk/7]).

%% Indexes are values in the full 2^160 hash space
-type index() :: chash:index_as_int().
%% IDs (vnode or partition) are integers in the [0, RingSize) space
%% (and trivially map to indexes). Private functions deal with IDs
%% instead of indexes as much as possible
-type vnode_id() :: non_neg_integer().
-type partition_id() :: riak_core_ring:partition_id().
-type subpartition_id() :: non_neg_integer().

-type req_id() :: non_neg_integer().
-type coverage_vnodes() :: [{index(), node()}].
-type vnode_filters() :: [{index(), [index()]}].
-type coverage_plan() :: {coverage_vnodes(), vnode_filters()}.

%% Each: Node, Vnode hash, { Subpartition id, BSL }
-type subp_plan() :: [{index(), node(), { subpartition_id(), pos_integer() }}].

-export_type([coverage_plan/0, coverage_vnodes/0, vnode_filters/0]).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Create a coverage plan to distribute work to a set
%%      of covering VNodes around the ring. If the first argument
%%      is a vnode_coverage record, that means we've previously
%%      generated a coverage plan and we're being fed back one
%%      element of it. Return that element in the proper format.
-spec create_plan(vnode_selector(),
                  pos_integer(),
                  pos_integer(),
                  req_id(), atom()) ->
                         {error, term()} | coverage_plan().
create_plan(#vnode_coverage{vnode_identifier=TargetHash,
                            subpartition={Mask, BSL}},
            _NVal, _PVC, _ReqId, _Service) ->
    {[{TargetHash, node()}], [{TargetHash, {Mask, BSL}}]};
create_plan(#vnode_coverage{vnode_identifier=TargetHash,
                            partition_filters=[]},
            _NVal, _PVC, _ReqId, _Service) ->
    {[{TargetHash, node()}], []};
create_plan(#vnode_coverage{vnode_identifier=TargetHash,
                            partition_filters=HashFilters},
            _NVal, _PVC, _ReqId, _Service) ->
    {[{TargetHash, node()}], [{TargetHash, HashFilters}]};
create_plan(VNodeTarget, NVal, PVC, ReqId, Service) ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    create_traditional_plan(VNodeTarget, NVal, PVC, ReqId, Service, CHBin).

partition_id_to_preflist(PartIdx, NVal, Offset, UpNodes) ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    OrigPreflist =
        lists:map(fun({{Idx, Node}, primary}) -> {Idx, Node} end,
                  riak_core_apl:get_primary_apl_chbin(PartIdx, NVal, CHBin, UpNodes)),
    rotate_list(OrigPreflist, length(OrigPreflist), Offset).

rotate_list(List, _Len, 0) ->
    %% Unnecessary special case, slightly more efficient
    List;
rotate_list(List, Len, Offset) when Offset >= Len ->
    List;
rotate_list(List, _Len, Offset) ->
    {Head, Tail} = lists:split(Offset, List),
    Tail ++ Head.

-spec replace_traditional_chunk(VnodeIdx :: index(),
                                Node :: node(),
                                Filters :: list(index()),
                                NVal :: pos_integer(),
                                ReqId :: req_id(),
                                DownNodes :: list(node()),
                                Service :: atom()) ->
                                       {error, term()} | subp_plan().
replace_traditional_chunk(VnodeIdx, Node, Filters, NVal,
                          ReqId, DownNodes, Service) ->
    Offset = ReqId rem NVal,
    %% We have our own idea of what nodes are available. The client
    %% may have a different idea of offline nodes based on network
    %% partitions, so we take that into account.
    %%
    %% The client can't really tell us what nodes it thinks are up (it
    %% only knows hostnames at best) but the opaque coverage chunks it
    %% uses have node names embedded in them, and it can tell us which
    %% chunks do *not* work.
    UpNodes = riak_core_node_watcher:nodes(Service) -- [Node|DownNodes],
    NeededPartitions = partitions_by_index_or_filter(
                         VnodeIdx, NVal, Filters),

    Preflists =
      lists:map(
        fun(PartIdx) ->
                {PartIdx,
                 {
                   safe_hd(
                     partition_id_to_preflist(PartIdx, NVal, Offset, UpNodes)),
                   []
                 }
                }
        end,
        NeededPartitions),
    maybe_create_traditional_replacement(Preflists, lists:keyfind({[], []}, 2, Preflists)).

maybe_create_traditional_replacement(Preflists, false) ->
    %% We do not go to great lengths to minimize the number of
    %% coverage plan components we'll return, but we do at least sort
    %% the vnodes we find so that we can consolidate filters later.
    create_traditional_replacement(lists:sort(Preflists));
maybe_create_traditional_replacement(_Preflists, _) ->
    {error, primary_partition_unavailable}.


%% Argument to this should be sorted
create_traditional_replacement(Preflists) ->
    dechunk_traditional_replacement(
      lists:foldl(
        %% First, see if the previous vnode in our accumulator is the
        %% same as this one; if so, add our partition to the list of
        %% filters
        fun({PartIdx, {{PrevVnode, ANode}, []}},
            [{{PrevVnode, ANode}, Partitions}|Tail]) ->
                [{{PrevVnode, ANode},
                  lists:sort([PartIdx|Partitions])}|Tail];
           %% If this is a new vnode to our accumulator
           ({PartIdx, {{NewVnode, ANode}, []}}, Coverage) ->
                [{{NewVnode, ANode}, [PartIdx]}|Coverage]
        end,
        [],
        Preflists)).

%% Take our replacement traditional coverage and make it look like a
%% "real" traditional coverage plan
dechunk_traditional_replacement(Coverage) ->
    {
      lists:map(fun({{Vnode, Node}, _Filters}) ->
                        {Vnode, Node}
                end, Coverage),
      lists:filtermap(fun({{_Vnode, _Node}, []}) ->
                              false;
                         ({{Vnode, _Node}, Filters}) ->
                              {true, {Vnode, Filters}}
                      end,
                      Coverage)
    }.


safe_hd([]) ->
    [];
safe_hd(List) ->
    hd(List).

find_vnode_partitions(Index, N) ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionCount = chashbin:num_partitions(CHBin),
    RingIndexInc = chash:ring_increment(PartitionCount),

    %% n_keyspaces deals with short IDs but we need full index values
    lists:map(fun(Id) -> Id * RingIndexInc end,
              n_keyspaces(Index div RingIndexInc, N, PartitionCount)).

partitions_by_index_or_filter(Idx, NVal, []) ->
    find_vnode_partitions(Idx, NVal);
partitions_by_index_or_filter(_Idx, _NVal, Filters) ->
    Filters.

%% replace_subpartition_chunk

%% This is relatively easy: to replace all of a missing vnode, we'd
%% have to create multiple chunks from multiple alternatives. Since
%% subpartitions are at most one partition (and often smaller than a
%% partition) we just need to find one alternative vnode.
-spec replace_subpartition_chunk(VnodeIdx :: index(),
                                 Node :: node(),
                                 {Mask :: non_neg_integer(),
                                  Bits :: non_neg_integer()},
                                 NVal :: pos_integer(),
                                 ReqId :: req_id(),
                                 DownNodes :: list(node()),
                                 Service :: atom()) ->
                                        {error, term()} | subp_plan().
replace_subpartition_chunk(VnodeIdx, Node, {Mask, Bits}, NVal,
                           ReqId, DownNodes, Service) ->
    Offset = ReqId rem NVal,
    %% We have our own idea of what nodes are available. The client
    %% may have a different idea of offline nodes based on network
    %% partitions, so we take that into account.
    %%
    %% The client can't really tell us what nodes it thinks are up (it
    %% only knows hostnames at best) but the opaque coverage chunks it
    %% uses have node names embedded in them, and it can tell us which
    %% chunks do *not* work.
    UpNodes = riak_core_node_watcher:nodes(Service) -- [Node|DownNodes],
    PrefList =
        partition_id_to_preflist(<<(Mask bsl Bits):160/integer>>,
                                 NVal, Offset, UpNodes),
    singular_preflist_to_chunk(VnodeIdx, Node, PrefList, Mask, Bits).

singular_preflist_to_chunk(_VnodeIdx, _Node, [], _Mask, _Bits) ->
    {error, primary_partition_unavailable};
singular_preflist_to_chunk(VnodeIdx, Node,
                           [{VnodeIdx, Node}]=PrefList,
                           Mask, Bits) ->
    %% This can only mean that the preflist calculation ignored our down nodes list.
    lager:error("Preflist calculation ignored down node ~p", [Node]),
    %% What else can we do but return it and hope the client can
    %% successfully reach it this time?
    singular_preflist_to_chunk(VnodeIdx + 1, dummy_node, PrefList, Mask, Bits);
singular_preflist_to_chunk(_OldVnodeIdx, _OldNode,
                           [{VnodeIdx, Node}],
                           Mask, Bits) ->
    [{VnodeIdx, Node, {Mask, Bits}}].


-spec create_subpartition_plan('all'|'allup',
                               pos_integer(),
                               pos_integer(),
                               pos_integer(),
                               req_id(), atom()) ->
                                      {error, term()} | subp_plan().
create_subpartition_plan(VNodeTarget, NVal, Count, PVC, ReqId, Service) ->
    {ok, ChashBin} = riak_core_ring_manager:get_chash_bin(),
    create_subpartition_plan(VNodeTarget, NVal, Count, PVC, ReqId, Service, ChashBin).

%% Must be able to comply with PVC if the target is 'all'
check_pvc(List, _PVC, allup) ->
    List;
check_pvc(List, PVC, all) ->
    check_pvc2(List, length(List), PVC).

check_pvc2(List, Len, PVC) when Len >= PVC ->
    List;
check_pvc2(_List, _Len, _PVC) ->
    [].

%% @private
create_subpartition_plan(VNodeTarget, NVal, Count, PVC, ReqId, Service, CHBin) ->
    MaskBSL = data_bits(Count),

    %% Calculate an offset based on the request id to offer the
    %% possibility of different sets of VNodes being used even when
    %% all nodes are available.
    Offset = ReqId rem NVal,

    UpNodes = riak_core_node_watcher:nodes(Service),
    SubpList =
        lists:map(fun(X) ->
                          PartID = chashbin:responsible_position(X bsl MaskBSL,
                                                                 CHBin),
                          PartIdx = <<(PartID bsl MaskBSL):160/integer>>,

                          %% PVC is much like R; if the number of
                          %% available primary partitions won't reach
                          %% the specified PVC value, don't bother
                          %% including this partition at all. We can
                          %% decide later (based on all vs allup)
                          %% whether to return the successful
                          %% components of the coverage plan
                          {PartID, X, check_pvc(partition_id_to_preflist(PartIdx, NVal, Offset, UpNodes), PVC, VNodeTarget)}
                  end,
                  lists:seq(0, Count - 1)),

    %% Now we have a list of tuples; each subpartition maps to a
    %% partition ID, subpartition ID, and a list of zero or more
    %% {vnode_index, node} tuples for that partition
    maybe_create_subpartition_plan(VNodeTarget, SubpList, MaskBSL, PVC).

maybe_create_subpartition_plan(allup, SubpList, Bits, PVC) ->
    map_subplist_to_plan(SubpList, Bits, PVC);
maybe_create_subpartition_plan(all, SubpList, Bits, PVC) ->
    maybe_create_subpartition_plan(all, lists:keyfind([], 3, SubpList), SubpList, Bits, PVC).

maybe_create_subpartition_plan(all, false, SubpList, Bits, PVC) ->
    map_subplist_to_plan(SubpList, Bits, PVC);
maybe_create_subpartition_plan(all, _, _SubpList, _Bits, _PVC) ->
    {error, insufficient_vnodes_available}.

map_subplist_to_plan(SubpList, Bits, PVC) ->
    lists:flatten(
      lists:filtermap(fun({_PartID, _SubpID, []}) ->
                              false;
                         ({_PartID, SubpID, Vnodes}) ->
                              {true,
                               map_pvc_vnodes(Vnodes, SubpID, Bits, PVC)}
                    end, SubpList)).


map_pvc_vnodes(Vnodes, SubpID, Bits, PVC) ->
    map_pvc_vnodes(Vnodes, SubpID, Bits, PVC, []).

map_pvc_vnodes(_Vnodes, _SubpID, _Bits, 0, Accum) ->
    lists:reverse(Accum);
map_pvc_vnodes([{VnodeIdx, Node}|Tail], SubpID, Bits, PVC, Accum) ->
    map_pvc_vnodes(Tail, SubpID, Bits, PVC-1,
                   [{ VnodeIdx, Node, { SubpID, Bits } }] ++ Accum).


%% @private
%% Make it easier to unit test create_plan/5.
create_traditional_plan(VNodeTarget, NVal, PVC, ReqId, Service, CHBin) ->
    PartitionCount = chashbin:num_partitions(CHBin),

    %% Calculate an offset based on the request id to offer the
    %% possibility of different sets of VNodes being used even when
    %% all nodes are available. Used in compare_vnode_keyspaces as a
    %% tiebreaker.
    Offset = ReqId rem NVal,

    RingIndexInc = chash:ring_increment(PartitionCount),
    AllVnodes = lists:seq(0, PartitionCount - 1),
    UnavailableVnodes = identify_unavailable_vnodes(CHBin, RingIndexInc, Service),

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
    %% compare_vnode_keyspaces function and is used to distribute work
    %% to different sets of VNodes.
    CoverageResult = find_minimal_coverage(AllVnodes,
                                           Offset,
                                           NVal,
                                           PartitionCount,
                                           UnavailableVnodes,
                                           lists:min([PVC, NVal]),
                                           []),
    case CoverageResult of
        {ok, CoveragePlan} ->
            %% Assemble the data structures required for
            %% executing the coverage operation.
            lists:mapfoldl(CoverageVNodeFun, [], CoveragePlan);
        {insufficient_vnodes_available, _KeySpace, PartialCoverage}  ->
            case VNodeTarget of
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

%% @private
-spec identify_unavailable_vnodes(chashbin:chashbin(), pos_integer(), atom()) -> list(vnode_id()).
identify_unavailable_vnodes(CHBin, PartitionSize, Service) ->
    %% Get a list of the VNodes owned by any unavailable nodes
    DownVNodes = [Index ||
                     {Index, _Node}
                         <- riak_core_apl:offline_owners(Service, CHBin)],
    [(DownVNode div PartitionSize) || DownVNode <- DownVNodes].

%% @private
merge_coverage_results({VnodeId, PartitionIds}, Acc) ->
    case proplists:get_value(VnodeId, Acc) of
        undefined ->
            [{VnodeId, PartitionIds} | Acc];
        MorePartitionIds ->
            UniqueValues =
                lists:usort(PartitionIds ++ MorePartitionIds),
            [{VnodeId, UniqueValues} |
             proplists:delete(VnodeId, Acc)]
    end.


%% @private
%% @doc Generates a minimal set of vnodes and partitions to find the requested data
-spec find_minimal_coverage(list(partition_id()), non_neg_integer(), non_neg_integer(), non_neg_integer(), list(vnode_id()), non_neg_integer(), list({vnode_id(), list(partition_id())})) -> {ok, list({vnode_id(), list(partition_id())})} | {error, term()}.
find_minimal_coverage(_AllVnodes, _Offset, _NVal, _PartitionCount,
                      _UnavailableVnodes, 0, Results) ->
    {ok, Results};
find_minimal_coverage(AllVnodes,
                      Offset,
                      NVal,
                      PartitionCount,
                      UnavailableVnodes,
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
                          || VNode <- (AllVnodes -- UnavailableVnodes)],
    case find_coverage_vnodes(ordsets:from_list(AllVnodes),
                              AvailableKeySpaces,
                              ResultsAcc) of
        {ok, CoverageResults} ->
            UpdatedResults =
                lists:foldl(fun merge_coverage_results/2, ResultsAcc, CoverageResults),
            find_minimal_coverage(AllVnodes,
                                  Offset,
                                  NVal,
                                  PartitionCount,
                                  UnavailableVnodes,
                                  PVC-1,
                                  UpdatedResults);
        Error ->
            Error
    end.

%% @private
%% @doc Find the N key spaces for a VNode
-spec n_keyspaces(vnode_id(), pos_integer(), pos_integer()) -> list(partition_id()).
n_keyspaces(VNode, N, PartitionCount) ->
    ordsets:from_list([X rem PartitionCount ||
                          X <- lists:seq(PartitionCount + VNode - N,
                                         PartitionCount + VNode - 1)]).

%% @private
%% @doc Find a minimal set of covering VNodes.
%% All parameters and return values are expressed as IDs in the [0,
%% RingSize) range.
%% Takes:
%%   A list of all vnode IDs still needed for coverage
%%   A list of available vnode IDs
%%   An accumulator for results
%% Returns a list of {vnode_id, [partition_id,...]} tuples.
-spec find_coverage_vnodes(list(vnode_id()), list(vnode_id()), list({vnode_id(), list(partition_id())})) ->
                                  {ok, list({vnode_id(), list(partition_id())})}|
                                  {insufficient_vnodes_available, list(vnode_id()), list({vnode_id(), list(partition_id())})}.
find_coverage_vnodes([], _, Coverage) ->
    {ok, lists:sort(Coverage)};
find_coverage_vnodes(Vnodes, [], Coverage) ->
    {insufficient_vnodes_available, Vnodes, lists:sort(Coverage)};
find_coverage_vnodes(Vnodes, AvailableVnodes, Coverage) ->
    case find_best_vnode_for_keyspace(Vnodes, AvailableVnodes) of
        {error, no_coverage} ->
            %% Bail
            find_coverage_vnodes(Vnodes, [], Coverage);
        VNode ->
            {value, {_, VNode, Covers}, UpdAvailable} = lists:keytake(VNode, 2, AvailableVnodes),
            UpdCoverage = [{VNode, ordsets:intersection(Vnodes, Covers)} | Coverage],
            UpdVnodes = ordsets:subtract(Vnodes, Covers),
            find_coverage_vnodes(UpdVnodes, UpdAvailable, UpdCoverage)
    end.

%% @private
%% @doc Find the vnode that covers the most of the remaining
%% keyspace. Use VNode ID + offset (determined by request ID) as the
%% tiebreaker
find_best_vnode_for_keyspace(KeySpace, Available) ->
    CoverCount = [{covers(KeySpace, CoversKeys), VNode, TieBreaker} ||
                     {TieBreaker, VNode, CoversKeys} <- Available],
    interpret_best_vnode(hd(lists:sort(fun compare_vnode_keyspaces/2,
                                       CoverCount))).

%% @private
interpret_best_vnode({0, _, _}) ->
    {error, no_coverage};
interpret_best_vnode({_, VNode, _}) ->
    VNode.

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
compare_vnode_keyspaces({CA, _VA, TBA}, {CB, _VB, TBB}) ->
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

%% @private
%% Determines the number of non-mask bits in the 2^160 keyspace.
%% Note that PartitionCount does not have to be ring size; we could be
%% creating a coverage plan for subpartitions
data_bits(PartitionCount) ->
    160 - round(math:log(PartitionCount) / math:log(2)).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-define(SET(X), ordsets:from_list(X)).

bits_test() ->
    %% 160 - log2(8)
    ?assertEqual(157, data_bits(8)),
    %% 160 - log2(65536)
    ?assertEqual(144, data_bits(65536)).

n_keyspaces_test() ->
    %% First vnode in a cluster with ring size 64 should (with nval 3)
    %% cover keyspaces 61-63
    ?assertEqual([61, 62, 63], n_keyspaces(0, 3, 64)),
    %% 4th vnode in a cluster with ring size 8 should (with nval 5)
    %% cover the first 3 and last 2 keyspaces
    ?assertEqual([0, 1, 2, 6, 7], n_keyspaces(3, 5, 8)),
    %% First vnode in a cluster with a single partition should (with
    %% any nval) cover the only keyspace
    ?assertEqual([0], n_keyspaces(0, 1, 1)).

covers_test() ->
    %% Count the overlap between the sets
    ?assertEqual(2, covers(?SET([1, 2]),
                           ?SET([0, 1, 2, 3]))),
    ?assertEqual(1, covers(?SET([1, 2]),
                           ?SET([0, 1]))),
    ?assertEqual(0, covers(?SET([1, 2, 3]),
                           ?SET([4, 5, 6, 7]))).

best_vnode_test() ->
    %% Given two vnodes 0 and 7, pick 0 because it has more of the
    %% desired keyspaces
    ?assertEqual(0, find_best_vnode_for_keyspace(
                      ?SET([0, 1, 2, 3, 4]),
                      [{2, 0, ?SET([6, 7, 0, 1, 2])},
                       {1, 7, ?SET([5, 6, 7, 0, 1])}])),
    %% Given two vnodes 0 and 7, pick 7 because they cover the same
    %% keyspaces and 7 has the lower tiebreaker
    ?assertEqual(7, find_best_vnode_for_keyspace(
                      ?SET([0, 1, 2, 3, 4]),
                      [{2, 0, ?SET([6, 7, 0, 1, 2])},
                       {1, 7, ?SET([6, 7, 0, 1, 2])}])),
    %% Given two vnodes 0 and 7, pick 0 because they cover the same
    %% keyspaces and 0 has the lower tiebreaker
    ?assertEqual(0, find_best_vnode_for_keyspace(
                      ?SET([0, 1, 2, 3, 4]),
                      [{2, 0, ?SET([6, 7, 0, 1, 2])},
                       {3, 7, ?SET([6, 7, 0, 1, 2])}])).

create_plan_test_() ->
    {setup,
     fun cpsetup/0,
     fun cpteardown/1,
     fun test_create_plan/1}.

cpsetup() ->
    meck:new(riak_core_node_watcher, []),
    meck:expect(riak_core_node_watcher, nodes, 1, [mynode]),
    CHash = chash:fresh(8, mynode),
    chashbin:create(CHash).

cpteardown(_) ->
    meck:unload().

test_create_plan(CHBin) ->
    Plan =
        {[{1278813932664540053428224228626747642198940975104,
           mynode},
          {730750818665451459101842416358141509827966271488,
           mynode},
          {365375409332725729550921208179070754913983135744,
           mynode}],
         [{730750818665451459101842416358141509827966271488,
           [548063113999088594326381812268606132370974703616,
            730750818665451459101842416358141509827966271488]}]},
    [?_assertEqual(Plan,
                   create_traditional_plan(all, 3, 1, 1234, riak_kv, CHBin))].

-endif.
