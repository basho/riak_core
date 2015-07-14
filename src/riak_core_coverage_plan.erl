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

%% @doc A module to calculate plans to cover a set of VNodes.
%%
%%      There are two types of plans available: a "traditional"
%%      coverage plan which minimizes the set of VNodes to be
%%      contacted. This is used internally for functionality such as
%%      2i and Riak Pipe.
%%
%%      There is also a new "subpartition" coverage plan designed to
%%      achieve the opposite: allow clients to make parallel requests
%%      across as much of the cluster as is desired, designed for use
%%      with Basho Data Platform.
%%
%%      Both plan types are now available through the protocol buffers
%%      API, delivered as discrete opaque binary chunks to be sent
%%      with queries that support the functionality (primarily 2i).


%% Example traditional coverage plan for a two node, 8 vnode cluster
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

%% Snippet from a subpartition coverage plan for a two node, 8 vnode
%% cluster at nval=3, with each partition represented twice for up to
%% 16 parallel queries.

%% The nested tuple in the third position is a representation of the
%% mask against the cluster's full keyspace and bits necessary to
%% shift keys right to compare against the mask (or shift the mask
%% left to represent its actual value).

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
-export([interpret_plan/1]).
-export([replace_subpartition_chunk/7, replace_traditional_chunk/7]).

-type ring_size() :: pos_integer().

-type index() :: chash:index_as_int().

%% This module is awash with lists of integers with the same range but
%% and similar but distinct meanings (vnodes and partitions), or
%% different ranges but same meanings (partition short id vs partition
%% hash), or different values depending on context (partitions are
%% incremented by a full RingIndexInc when used as traditional cover
%% filters).
%%
%% Thus, tagged tuples are used in many places to make it easier for
%% maintainers to keep track of what data is flowing where.

%% IDs (vnode, partition) are integers in the [0, RingSize) space
%% (and trivially map to indexes). Private functions deal with IDs
%% instead of indexes as much as possible.

%% ID, ring size, ring index increment
-type vnode_id() :: {'vnode_id', non_neg_integer(), ring_size(), pos_integer()}.
-type partition_id() :: {'partition_id', non_neg_integer(), ring_size(), pos_integer()}.

%% Not a tagged tuple. ID + bits necessary to shift the ID to the left
%% to create the subpartition index.
-type subpartition_id() :: {non_neg_integer(), pos_integer()}.

-type req_id() :: non_neg_integer().
-type coverage_vnodes() :: [{index(), node()}].
-type vnode_filters() :: [{index(), [index()]}].
-type coverage_plan() :: {coverage_vnodes(), vnode_filters()}.

%% Each: Node, Vnode hash, { Subpartition id, BSL }
-type subp_plan() :: [{index(), node(), subpartition_id()}].

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
create_plan(VNodeTarget, NVal, PVC, ReqId, Service) ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    create_traditional_plan(VNodeTarget, NVal, PVC, ReqId, Service, CHBin).


interpret_plan(#vnode_coverage{vnode_identifier=TargetHash,
                               subpartition={Mask, BSL}}) ->
    {[{TargetHash, node()}], [{TargetHash, {Mask, BSL}}]};
interpret_plan(#vnode_coverage{vnode_identifier=TargetHash,
                               partition_filters=[]}) ->
    {[{TargetHash, node()}], []};
interpret_plan(#vnode_coverage{vnode_identifier=TargetHash,
                            partition_filters=HashFilters}) ->
    {[{TargetHash, node()}], [{TargetHash, HashFilters}]}.

%% The `riak_core_apl' functions we rely on assume a key hash, not a
%% partition hash.
partition_to_preflist(Partition, NVal, Offset, UpNodes) ->
    DocIdx = convert(Partition, doc_index),
    docidx_to_preflist(DocIdx, NVal, Offset, UpNodes).

docidx_to_preflist(DocIdx, NVal, Offset, UpNodes) ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),

    %% `chashbin' would be fine with a straight integer, but
    %% `riak_core_apl' has a -spec that mandates binary(), so we'll
    %% play its game
    OrigPreflist =
        lists:map(fun({{Idx, Node}, primary}) -> {Idx, Node} end,
                  riak_core_apl:get_primary_apl_chbin(
                    <<DocIdx:160/integer>>, NVal, CHBin, UpNodes)),
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
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    RingSize = chashbin:num_partitions(CHBin),
    RingIndexInc = chash:ring_increment(RingSize),

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
                         VnodeIdx, NVal, Filters, RingSize, RingIndexInc),

    Preflists =
      lists:map(
        fun(PartIdx) ->
                {PartIdx,
                 {
                   safe_hd(
                     partition_to_preflist(PartIdx, NVal, Offset, UpNodes)),
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

find_vnode_partitions(Index, N, RingSize, RingIndexInc) ->
    %% n_keyspaces deals with short IDs but we need full index values
    lists:map(fun({partition_id, Id, _, _}) -> Id * RingIndexInc end,
              n_keyspaces({vnode_id, Index div RingIndexInc,
                           RingSize, RingIndexInc}, N)).

partitions_by_index_or_filter(Idx, NVal, [], RingSize, RingIndexInc) ->
    find_vnode_partitions(Idx, NVal, RingSize, RingIndexInc);
partitions_by_index_or_filter(_Idx, _NVal, Filters, RingSize, RingIndexInc) ->
    lists:map(fun(P) -> {partition_id, P, RingSize, RingIndexInc} end,
              Filters).

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
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    replace_subpartition_chunk(VnodeIdx, Node, {Mask, Bits}, NVal,
                               ReqId, DownNodes, Service, CHBin).


replace_subpartition_chunk(_VnodeIdx, Node, {_Mask, _Bits}=SubpID, NVal,
                           ReqId, DownNodes, Service, CHBin) ->
    RingSize = chashbin:num_partitions(CHBin),
    RingIndexInc = chash:ring_increment(RingSize),

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

    %% We don't know what partition this subpartition is in, but we
    %% can request a preflist for it by subtracting RingIndexInc from
    %% the subpartition index to jump back a keyspace and send that
    %% new value to `riak_core_apl' as a document index.
    DocIdx = convert(SubpID, subpartition_index) - RingIndexInc,
    PrefList =
        docidx_to_preflist(DocIdx, NVal, Offset, UpNodes),
    singular_preflist_to_chunk(PrefList, SubpID).

singular_preflist_to_chunk([], _SubpID) ->
    {error, primary_partition_unavailable};
singular_preflist_to_chunk([{VnodeIdx, Node}], SubpID) ->
    [{VnodeIdx, Node, SubpID}].


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

    RingSize = chashbin:num_partitions(CHBin),
    RingIndexInc = chash:ring_increment(RingSize),

    UpNodes = riak_core_node_watcher:nodes(Service),
    SubpList =
        lists:map(fun(SubpCounter) ->
                          SubpID = {SubpCounter, MaskBSL},
                          SubpIndex = convert(SubpID, subpartition_index),
                          PartID =
                              {partition_id,
                               chashbin:responsible_position(SubpIndex, CHBin),
                               RingSize, RingIndexInc},

                          %% PVC is much like R; if the number of
                          %% available primary partitions won't reach
                          %% the specified PVC value, don't bother
                          %% including this partition at all. We can
                          %% decide later (based on all vs allup)
                          %% whether to return the successful
                          %% components of the coverage plan
                          {PartID, SubpID,
                           check_pvc(
                             docidx_to_preflist(SubpIndex, NVal, Offset, UpNodes),
                             PVC, VNodeTarget)}
                  end,
                  lists:seq(0, Count - 1)),

    %% Now we have a list of tuples; each subpartition maps to a
    %% partition ID, subpartition ID, and a list of zero or more
    %% {vnode_index, node} tuples for that partition
    maybe_create_subpartition_plan(VNodeTarget, SubpList, PVC).

maybe_create_subpartition_plan(allup, SubpList, PVC) ->
    map_subplist_to_plan(SubpList, PVC);
maybe_create_subpartition_plan(all, SubpList, PVC) ->
    maybe_create_subpartition_plan(all, lists:keyfind([], 3, SubpList), SubpList, PVC).

maybe_create_subpartition_plan(all, false, SubpList, PVC) ->
    map_subplist_to_plan(SubpList, PVC);
maybe_create_subpartition_plan(all, _, _SubpList, _PVC) ->
    {error, insufficient_vnodes_available}.

map_subplist_to_plan(SubpList, PVC) ->
    lists:flatten(
      lists:filtermap(fun({_PartID, _SubpID, []}) ->
                              false;
                         ({_PartID, SubpID, Vnodes}) ->
                              {true,
                               map_pvc_vnodes(Vnodes, SubpID, PVC)}
                    end, SubpList)).


map_pvc_vnodes(Vnodes, SubpID, PVC) ->
    map_pvc_vnodes(Vnodes, SubpID, PVC, []).

map_pvc_vnodes(_Vnodes, _SubpID, 0, Accum) ->
    lists:reverse(Accum);
map_pvc_vnodes([{VnodeIdx, Node}|Tail], SubpID, PVC, Accum) ->
    map_pvc_vnodes(Tail, SubpID, PVC-1,
                   [{ VnodeIdx, Node, SubpID }] ++ Accum).


%% @private
%% Make it easier to unit test create_plan/5.
create_traditional_plan(VNodeTarget, NVal, PVC, ReqId, Service, CHBin) ->
    RingSize = chashbin:num_partitions(CHBin),

    %% Calculate an offset based on the request id to offer the
    %% possibility of different sets of VNodes being used even when
    %% all nodes are available. Used in compare_vnode_keyspaces as a
    %% tiebreaker.
    Offset = ReqId rem NVal,

    RingIndexInc = chash:ring_increment(RingSize),
    AllVnodes = list_all_vnode_ids(RingSize, RingIndexInc),
    %% Older versions of this call chain used the same list of
    %% integers for both vnode IDs and partition IDs, which made for
    %% confusing reading. We can cheat a little less obnoxiously
    AllPartitions = lists:map(fun({vnode_id, Id, PC, Inc}) ->
                                      {partition_id, Id, PC, Inc} end,
                              AllVnodes),
    UnavailableVnodes = identify_unavailable_vnodes(CHBin, RingIndexInc,
                                                    RingSize, Service),

    %% Create function to map coverage keyspaces to
    %% actual VNode indexes and determine which VNode
    %% indexes should be filtered.
    CoverageVNodeFun =
        fun({VNodeID, KeySpaces}, Acc) ->
                %% Calculate the VNode index using the
                %% ring position and the increment of
                %% ring index values.
                VNodeIndex = convert(VNodeID, vnode_index),
                Node = chashbin:index_owner(VNodeIndex, CHBin),
                CoverageVNode = {VNodeIndex, Node},
                case length(KeySpaces) < NVal of
                    true ->
                        KeySpaceIndexes = [convert(PartitionID, keyspace_filter) ||
                                              PartitionID <- KeySpaces],
                        {CoverageVNode, [{VNodeIndex, KeySpaceIndexes} | Acc]};
                    false ->
                        {CoverageVNode, Acc}
                end
        end,
    %% The offset value serves as a tiebreaker in the
    %% compare_vnode_keyspaces function and is used to distribute work
    %% to different sets of VNodes.
    CoverageResult = find_minimal_coverage(AllPartitions,
                                           AllVnodes -- UnavailableVnodes,
                                           Offset,
                                           NVal,
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
%% Convert(from, to)
%% We spend a lot of code mapping between data types, mostly
%% integer-based. Consolidate that as much as possible here.
convert({partition_id, PartitionID, RingSize, RingIndexInc}, keyspace_filter) ->
    %% Because data is stored one partition higher than the keyspace
    %% into which it directly maps, we have to increment a partition
    %% ID by one to find the relevant keyspace index for traditional
    %% coverage plan filters
    ((PartitionID + 1) rem RingSize) * RingIndexInc;
convert({partition_id, 0, _RingSize, _RingIndexInc}, doc_index) ->
    %% Conversely, if we need an example index value inside a
    %% partition for functions that expect a document index, we need
    %% to move "back" in the hash space. Thus we'll subtract 1 from
    %% the partition *index* value.
    %%
    %% If we start at the bottom of the hash range (partition 0),
    %% return the top of the range
    (1 bsl 160) - 1;
convert({partition_id, _ID, _RingSize, _RingIndexInc}=PartID, doc_index) ->
    convert(PartID, partition_index) - 1;
convert({partition_id, PartitionID, _RingSize, RingIndexInc}, partition_index) ->
    PartitionID * RingIndexInc;
convert({vnode_id, VNodeID, _RingSize, RingIndexInc}, vnode_index) ->
    VNodeID * RingIndexInc;
convert({vnode_id, VNodeID, _RingSize, _RingIndexInc}, int) ->
    VNodeID;
convert({partition_id, PartitionID, _RingSize, _RingIndexInc}, int) ->
    PartitionID;
convert({SubpID, Bits}, subpartition_index) ->
    SubpID bsl Bits.

%% @private
increment_vnode({vnode_id, Position, RingSize, RingIndexInc}, Offset) ->
    {vnode_id, (Position + Offset) rem RingSize, RingSize, RingIndexInc}.

%% @private
list_all_vnode_ids(RingSize, Increment) ->
    lists:map(fun(Id) -> {vnode_id, Id, RingSize, Increment} end,
              lists:seq(0, RingSize - 1)).


%% @private
-spec identify_unavailable_vnodes(chashbin:chashbin(), pos_integer(), pos_integer(), atom()) -> list(vnode_id()).
identify_unavailable_vnodes(CHBin, PartitionSize, RingSize, Service) ->
    %% Get a list of the VNodes owned by any unavailable nodes
    [{vnode_id, Index div PartitionSize, RingSize, PartitionSize} ||
        {Index, _Node}
            <- riak_core_apl:offline_owners(Service, CHBin)].

%% @private
%% Note that these Id values are tagged tuples, not integers
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
-spec find_minimal_coverage(list(partition_id()), list(vnode_id()),
                            non_neg_integer(),
                            non_neg_integer(),
                            non_neg_integer(),
                            list({vnode_id(), list(partition_id())})) ->
                                   {ok,
                                    list({vnode_id(), list(partition_id())})} |
                                   {error, term()}.
find_minimal_coverage(_AllPartitions, _AvailableVnodes, _Offset, _NVal,
                      0, Results) ->
    {ok, Results};
find_minimal_coverage(AllPartitions,
                      AvailableVnodes,
                      Offset,
                      NVal,
                      PVC,
                      ResultsAcc) ->
    %% Calculate the available keyspaces. The list of
    %% keyspaces for each vnode that have already been
    %% covered by the plan are subtracted from the complete
    %% list of keyspaces so that coverage plans that
    %% want to cover more one preflist vnode work out
    %% correctly.
    AvailableKeySpaces = [{increment_vnode(VNode, Offset),
                           VNode,
                           n_keyspaces(VNode, NVal) --
                               proplists:get_value(VNode, ResultsAcc, [])}
                          || VNode <- (AvailableVnodes)],
    case find_coverage_vnodes(ordsets:from_list(AllPartitions),
                              AvailableKeySpaces,
                              ResultsAcc) of
        {ok, CoverageResults} ->
            UpdatedResults =
                lists:foldl(fun merge_coverage_results/2, ResultsAcc, CoverageResults),
            find_minimal_coverage(AllPartitions,
                                  AvailableVnodes,
                                  Offset,
                                  NVal,
                                  PVC-1,
                                  UpdatedResults);
        Error ->
            Error
    end.

%% @private
%% @doc Find the N key spaces for a VNode. These are *not* the same as
%% the filters for the traditional cover plans; the filters are
%% incremented by 1
-spec n_keyspaces(vnode_id(), pos_integer()) -> list(partition_id()).
n_keyspaces({vnode_id, VNode, RingSize, RingIndexInc}, N) ->
    ordsets:from_list([{partition_id, X rem RingSize, RingSize, RingIndexInc} ||
                          X <- lists:seq(RingSize + VNode - N,
                                         RingSize + VNode - 1)]).

%% @private
%% @doc Find a minimal set of covering VNodes.
%%
%% Takes:
%%   A list of all partition IDs still needed for coverage
%%   A list of available vnode IDs with partitions they cover
%%   An accumulator for results
%% Returns a list of {vnode_id, [partition_id,...]} tuples.
-spec find_coverage_vnodes(list(partition_id()), list(vnode_id()), list({vnode_id(), list(partition_id())})) ->
                                  {ok, list({vnode_id(), list(partition_id())})}|
                                  {insufficient_vnodes_available, list(vnode_id()), list({vnode_id(), list(partition_id())})}.
find_coverage_vnodes([], _, Coverage) ->
    {ok, lists:sort(Coverage)};
find_coverage_vnodes(Partitions, [], Coverage) ->
    {insufficient_vnodes_available, Partitions, lists:sort(Coverage)};
find_coverage_vnodes(Partitions, AvailableVnodes, Coverage) ->
    case find_best_vnode_for_keyspace(Partitions, AvailableVnodes) of
        {error, no_coverage} ->
            %% Bail
            find_coverage_vnodes(Partitions, [], Coverage);
        VNode ->
            {value, {_, VNode, Covers}, UpdAvailable} = lists:keytake(VNode, 2, AvailableVnodes),
            UpdCoverage = [{VNode, ordsets:intersection(Partitions, Covers)} | Coverage],
            UpdPartitions = ordsets:subtract(Partitions, Covers),
            find_coverage_vnodes(UpdPartitions, UpdAvailable, UpdCoverage)
    end.

%% @private
%% @doc Find the vnode that covers the most of the remaining
%% keyspace. Use VNode ID + offset (determined by request ID) as the
%% tiebreaker
find_best_vnode_for_keyspace(PartitionIDs, Available) ->
    CoverCount = [{covers(PartitionIDs, CoversKeys), VNode, TieBreaker} ||
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

partid(Partitions) ->
    lists:map(fun({partition_id, P, _, _}) -> P end,
              Partitions).

n_keyspaces_test() ->
    %% First vnode in a cluster with ring size 64 should (with nval 3)
    %% cover keyspaces 61-63
    ?assertEqual([61, 62, 63], partid(n_keyspaces({vnode_id, 0, 64, 0}, 3))),
    %% 4th vnode in a cluster with ring size 8 should (with nval 5)
    %% cover the first 3 and last 2 keyspaces
    ?assertEqual([0, 1, 2, 6, 7], partid(n_keyspaces({vnode_id, 3, 8, 0}, 5))),
    %% First vnode in a cluster with a single partition should (with
    %% any nval) cover the only keyspace
    ?assertEqual([0], partid(n_keyspaces({vnode_id, 0, 1, 0}, 1))).

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
