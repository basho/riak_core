%% -------------------------------------------------------------------
%%
%% riak_core_coverage_plan: Create a plan to cover a minimal set of VNodes.
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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
-export([create_plan/5, create_plan/6, create_subpartition_plan/6]).
-export([replace_subpartition_chunk/7, replace_traditional_chunk/7]).

%% For other riak_core applications' coverage plan modules
-export([identify_unavailable_vnodes/3, identify_unavailable_vnodes/4]).
-export([add_offset/3]).

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
-type vnode_id() :: {'vnode_id', non_neg_integer(), ring_size()}.
-type partition_id() :: {'partition_id', non_neg_integer(), ring_size()}.

%% Not a tagged tuple. ID + bits necessary to shift the ID to the left
%% to create the subpartition index.
-type subpartition_id() :: {non_neg_integer(), pos_integer()}.

-type req_id() :: non_neg_integer().
-type coverage_vnodes() :: [{index(), node()}].
-type vnode_filters() :: [{index(), [index()]}].
-type coverage_plan() :: {coverage_vnodes(), vnode_filters()}.

%% Vnode index, node, { Subpartition id, BSL }
-type subp_plan() :: [{index(), node(), subpartition_id()}].

-export_type([coverage_plan/0, coverage_vnodes/0, vnode_filters/0]).

%% Function to determine nodes currently available. This can be
%% swapped out for testing to avoid using meck. The argument ignored
%% here is the chashbin, potentially useful for testing.
-define(AVAIL_NODE_FUN, fun(Svc, _) -> riak_core_node_watcher:nodes(Svc) end).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Create Riak's traditional coverage plan to distribute work to
%%      a minimal set of covering VNodes around the ring.
-spec create_plan(vnode_selector(),
                  pos_integer(),
                  pos_integer(),
                  req_id(), atom()) ->
                         {error, term()} | coverage_plan().
create_plan(VNodeTarget, NVal, PVC, ReqId, Service) ->
    %% jdaily added a sixth parameter, _Request, which is ignored
    %% here, apparently used in coverage API work. See
    %% riak_kv_qry_coverage_plan:create_plan for more clues.
    create_plan(VNodeTarget, NVal, PVC, ReqId, Service, undefined).

-spec create_plan(vnode_selector(),
                  pos_integer(),
                  pos_integer(),
                  req_id(), atom(), term()) ->
                         {error, term()} | coverage_plan().
create_plan(VNodeTarget, NVal, PVC, ReqId, Service, _Request) ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    create_traditional_plan(VNodeTarget, NVal, PVC, ReqId, Service,
                            CHBin, ?AVAIL_NODE_FUN).

%% @doc Create a "mini" traditional coverage plan to replace
%%      components of a previously-generated plan that are not useful
%%      to the client because the node is unavailable
-spec replace_traditional_chunk(VnodeIdx :: index(),
                                Node :: node(),
                                Filters :: list(index()),
                                NVal :: pos_integer(),
                                ReqId :: req_id(),
                                DownNodes :: list(node()),
                                Service :: atom()) ->
                                       {error, term()} | coverage_plan().
replace_traditional_chunk(VnodeIdx, Node, Filters, NVal,
                          ReqId, DownNodes, Service) ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    replace_traditional_chunk(VnodeIdx, Node, Filters, NVal,
                              ReqId, DownNodes, Service, CHBin,
                              ?AVAIL_NODE_FUN).

%% @doc Create a coverage plan with at least one slice per partition,
%%      originally designed for parallel extraction of data via 2i.
-spec create_subpartition_plan('all'|'allup',
                               pos_integer(),
                               pos_integer() | {pos_integer(), pos_integer()},
                               pos_integer(),
                               req_id(), atom()) ->
                                      {error, term()} | subp_plan().
create_subpartition_plan(VNodeTarget, NVal, {MinPar, RingSize}, PVC, ReqId, Service) ->
    Count =
        if
            MinPar =< RingSize ->
                RingSize;
            true ->
                next_power_of_two(MinPar)
        end,
    create_subpartition_plan(VNodeTarget, NVal, Count, PVC, ReqId, Service);
create_subpartition_plan(VNodeTarget, NVal, Count, PVC, ReqId, Service) ->
    %% IMPORTANT: `Count' is assumed to be a power of 2. Anything else
    %% will behave badly.
    {ok, ChashBin} = riak_core_ring_manager:get_chash_bin(),
    create_subpartition_plan(VNodeTarget, NVal, Count, PVC, ReqId, Service,
                             ChashBin, ?AVAIL_NODE_FUN).


%% @doc Create a "mini" traditional coverage plan to replace
%%      components of a previously-generated plan that are not useful
%%      to the client because the node is unavailable
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
                               ReqId, DownNodes, Service, CHBin,
                               ?AVAIL_NODE_FUN).

%% ====================================================================
%% Internal functions that are useful for other riak_core applications
%% that need to generate custom coverage plans
%% ====================================================================


-spec identify_unavailable_vnodes(chashbin:chashbin(), pos_integer(), atom()) ->
                                         list(vnode_id()).
identify_unavailable_vnodes(CHBin, RingSize, Service) ->
    identify_unavailable_vnodes(CHBin, RingSize, Service, ?AVAIL_NODE_FUN).

-spec identify_unavailable_vnodes(chashbin:chashbin(), pos_integer(), atom(),
                                  fun((atom(), binary()) -> list(node()))) ->
                                         list(vnode_id()).
identify_unavailable_vnodes(CHBin, RingSize, Service, AvailNodeFun) ->
    %% Get a list of the VNodes owned by any unavailable nodes
    [{vnode_id, index_to_id(Index, RingSize), RingSize} ||
        {Index, _Node}
            <- riak_core_apl:offline_owners(
                 AvailNodeFun(Service, CHBin), CHBin)].

%% Adding an offset while keeping the result inside [0, Top). Adding
%% Top to the left of `rem' allows offset to be negative without
%% violating the lower bound of zero
add_offset(Position, Offset, Top) ->
    (Position + (Top + Offset)) rem Top.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @private
create_traditional_plan(VNodeTarget, NVal, PVC, ReqId, Service, CHBin, AvailNodeFun) ->
    RingSize = chashbin:num_partitions(CHBin),

    %% Calculate an offset based on the request id to offer the
    %% possibility of different sets of VNodes being used even when
    %% all nodes are available. Used in compare_vnode_keyspaces as a
    %% tiebreaker.
    Offset = ReqId rem NVal,

    AllVnodes = list_all_vnode_ids(RingSize),
    %% Older versions of this call chain used the same list of
    %% integers for both vnode IDs and partition IDs, which made for
    %% confusing reading. We can cheat a little less obnoxiously
    AllPartitions = lists:map(fun({vnode_id, Id, RS}) ->
                                      {partition_id, Id, RS} end,
                              AllVnodes),
    UnavailableVnodes = identify_unavailable_vnodes(CHBin, RingSize,
                                                    Service, AvailNodeFun),

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

%% @private
replace_traditional_chunk(VnodeIdx, Node, Filters, NVal,
                          ReqId, DownNodes, Service, CHBin,
                          AvailNodeFun) ->

    RingSize = chashbin:num_partitions(CHBin),
    Offset = ReqId rem NVal,
    %% We have our own idea of what nodes are available. The client
    %% may have a different idea of offline nodes based on network
    %% partitions, so we take that into account.
    %%
    %% The client can't really tell us what nodes it thinks are up (it
    %% only knows hostnames at best) but the opaque coverage chunks it
    %% uses have node names embedded in them, and it can tell us which
    %% chunks do *not* work.
    UpNodes = AvailNodeFun(Service, CHBin) -- [Node|DownNodes],
    NeededPartitions = partitions_by_index_or_filter(
                         VnodeIdx, NVal, Filters, RingSize),

    %% For each partition, create a tuple with that partition (as a
    %% filter index) mapped to a nested tuple of preflist + (initially
    %% empty) filter list list.
    Preflists =
      lists:map(
        fun(Partition) ->
                {convert(Partition, keyspace_filter),
                   safe_hd(
                     partition_to_preflist(Partition, NVal, Offset, UpNodes, CHBin))
                }
        end,
        NeededPartitions),
    maybe_create_traditional_replacement(Preflists, lists:keyfind([], 2, Preflists)).

%% @private
maybe_create_traditional_replacement(Preflists, false) ->
    %% We do not go to great lengths to minimize the number of
    %% coverage plan components we'll return, but we do at least sort
    %% the vnodes we find so that we can consolidate filters later.
    create_traditional_replacement(lists:sort(Preflists));
maybe_create_traditional_replacement(_Preflists, _) ->
    {error, primary_partition_unavailable}.


%% @private
%% Argument to this should be sorted
create_traditional_replacement(Preflists) ->
    dechunk_traditional_replacement(
      lists:foldl(
        %% Pattern match the current vnode against the head of the
        %% accumulator; if the vnodes match, we can consolidate
        %% partition filters
        fun({PartIdx, VNode}, [{VNode, Partitions}|Tail]) ->
                [{VNode,
                  lists:sort([PartIdx|Partitions])}|Tail];
           %% Instead if this vnode is new, place it into the
           %% accumulator as is
           ({PartIdx, VNode}, Accum) ->
                [{VNode, [PartIdx]}|Accum]
        end,
        [],
        Preflists)).

%% @private
%% Take our replacement traditional coverage chunks and consolidate it
%% into a traditional coverage plan (that another layer will rechunk,
%% but whatchagonnado)
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


%% @private
safe_hd([]) ->
    [];
safe_hd(List) ->
    hd(List).

%% @private
replace_subpartition_chunk(_VnodeIdx, Node, {_Mask, _Bits}=SubpID, NVal,
                           ReqId, DownNodes, Service, CHBin,
                           AvailNodeFun) ->
    Offset = ReqId rem NVal,

    %% We have our own idea of what nodes are available. The client
    %% may have a different idea of offline nodes based on network
    %% partitions, so we take that into account.
    %%
    %% The client can't really tell us what nodes it thinks are up (it
    %% only knows hostnames at best) but the opaque coverage chunks it
    %% uses have node names embedded in them, and it can tell us which
    %% chunks do *not* work.
    UpNodes = AvailNodeFun(Service, CHBin) -- [Node|DownNodes],

    %% We don't know what partition this subpartition is in, but we
    %% can request a preflist for it by converting the subpartition to
    %% a document index.
    %%
    %% Unlike traditional coverage filters to document key hash
    %% mappings which have off-by-one adjustments, subpartition masks
    %% map directly against the relevant key hashes.
    DocIdx = convert(SubpID, subpartition_index),
    PrefList =
        docidx_to_preflist(DocIdx, NVal, Offset, UpNodes, CHBin),
    singular_preflist_to_chunk(safe_hd(PrefList), SubpID).

%% @private
singular_preflist_to_chunk([], _SubpID) ->
    {error, primary_partition_unavailable};
singular_preflist_to_chunk({VnodeIdx, Node}, SubpID) ->
    [{VnodeIdx, Node, SubpID}].



%% ====================================================================
%%% Conversion functions

%% @private
%% We spend a lot of code mapping between data types, mostly
%% integer-based. Consolidate that as much as possible here.
-spec convert(partition_id() | vnode_id() | subpartition_id(), atom()) ->
                     non_neg_integer().
convert({partition_id, PartitionID, RingSize}, keyspace_filter) ->
    %% Because data is stored one partition higher than the keyspace
    %% into which it directly maps, we have to increment a partition
    %% ID by one to find the relevant keyspace index for traditional
    %% coverage plan filters
    id_to_index(add_offset(PartitionID, 1, RingSize), RingSize);
convert({partition_id, PartitionID, RingSize}, partition_index) ->
    id_to_index(PartitionID, RingSize);
convert({vnode_id, VNodeID, RingSize}, vnode_index) ->
    id_to_index(VNodeID, RingSize);
convert({SubpID, Bits}, subpartition_index) ->
    SubpID bsl Bits.

%% @private
partition_to_preflist(Partition, NVal, Offset, UpNodes, CHBin) ->
    DocIdx = convert(Partition, partition_index),
    docidx_to_preflist(DocIdx, NVal, Offset, UpNodes, CHBin).

%% @private
docidx_to_preflist(DocIdx, NVal, Offset, UpNodes, CHBin) ->
    %% `chashbin' would be fine with a straight integer, but
    %% `riak_core_apl' has a -spec that mandates binary(), so we'll
    %% play its game
    OrigPreflist =
        lists:map(fun({{Idx, Node}, primary}) -> {Idx, Node} end,
                  riak_core_apl:get_primary_apl_chbin(
                    <<DocIdx:160/integer>>, NVal, CHBin, UpNodes)),
    rotate_list(OrigPreflist, length(OrigPreflist), Offset).

%% @private
rotate_list(List, Len, Offset) when Offset >= Len ->
    List;
rotate_list(List, _Len, Offset) ->
    {Head, Tail} = lists:split(Offset, List),
    Tail ++ Head.

%% @private
index_to_id(Index, RingSize) ->
    RingIndexInc = chash:ring_increment(RingSize),
    Index div RingIndexInc.

%% @private
id_to_index(Id, RingSize) when Id < RingSize->
    RingIndexInc = chash:ring_increment(RingSize),
    Id * RingIndexInc.

%% @private
find_vnode_partitions(Index, N, RingSize) ->
    n_keyspaces({vnode_id, index_to_id(Index, RingSize), RingSize}, N).

%% @private
partitions_by_index_or_filter(Idx, NVal, [], RingSize) ->
    find_vnode_partitions(Idx, NVal, RingSize);
partitions_by_index_or_filter(_Idx, _NVal, Filters, RingSize) ->
    %% Filters for traditional coverage plans are offset by 1, so when
    %% converting to partition indexes or IDs, we have to subtract 1
    lists:map(fun(P) ->
                      {partition_id,
                       add_offset(index_to_id(P, RingSize),
                                  -1, RingSize),
                       RingSize} end,
              Filters).



%% @private
%% Must be able to comply with PVC if the target is 'all'
check_pvc(List, _PVC, allup) ->
    List;
check_pvc(List, PVC, all) ->
    check_pvc2(List, length(List), PVC).

%% @private
check_pvc2(List, Len, PVC) when Len >= PVC ->
    List;
check_pvc2(_List, _Len, _PVC) ->
    [].

%% @private
create_subpartition_plan(VNodeTarget, NVal, Count, PVC, ReqId, Service, CHBin, AvailNodeFun) ->
    MaskBSL = data_bits(Count),

    %% Calculate an offset based on the request id to offer the
    %% possibility of different sets of VNodes being used even when
    %% all nodes are available.
    Offset = ReqId rem NVal,

    RingSize = chashbin:num_partitions(CHBin),

    UpNodes = AvailNodeFun(Service, CHBin),
    SubpList =
        lists:map(fun(SubpCounter) ->
                          SubpID = {SubpCounter, MaskBSL},
                          SubpIndex = convert(SubpID, subpartition_index),
                          PartID =
                              {partition_id,
                               chashbin:responsible_position(SubpIndex, CHBin),
                               RingSize},

                          %% PVC is much like R; if the number of
                          %% available primary partitions won't reach
                          %% the specified PVC value, don't bother
                          %% including this partition at all. We can
                          %% decide later (based on all vs allup)
                          %% whether to return the successful
                          %% components of the coverage plan
                          {PartID, SubpID,
                           check_pvc(
                             docidx_to_preflist(SubpIndex, NVal, Offset, UpNodes, CHBin),
                             PVC, VNodeTarget)}
                  end,
                  lists:seq(0, Count - 1)),

    %% Now we have a list of tuples; each subpartition maps to a
    %% partition ID, subpartition ID, and a list of zero or more
    %% {vnode_index, node} tuples for that partition
    maybe_create_subpartition_plan(VNodeTarget, SubpList, PVC).

%% @private
maybe_create_subpartition_plan(allup, SubpList, PVC) ->
    map_subplist_to_plan(SubpList, PVC);
maybe_create_subpartition_plan(all, SubpList, PVC) ->
    maybe_create_subpartition_plan(all, lists:keyfind([], 3, SubpList), SubpList, PVC).

%% @private
maybe_create_subpartition_plan(all, false, SubpList, PVC) ->
    map_subplist_to_plan(SubpList, PVC);
maybe_create_subpartition_plan(all, _, _SubpList, _PVC) ->
    {error, insufficient_vnodes_available}.

%% @private
map_subplist_to_plan(SubpList, PVC) ->
    lists:flatten(
      lists:filtermap(fun({_PartID, _SubpID, []}) ->
                              false;
                         ({_PartID, SubpID, Vnodes}) ->
                              {true,
                               map_pvc_vnodes(Vnodes, SubpID, PVC)}
                    end, SubpList)).


%% @private
map_pvc_vnodes(Vnodes, SubpID, PVC) ->
    map_pvc_vnodes(Vnodes, SubpID, PVC, []).

%% @private
map_pvc_vnodes(_Vnodes, _SubpID, 0, Accum) ->
    lists:reverse(Accum);
map_pvc_vnodes([{VnodeIdx, Node}|Tail], SubpID, PVC, Accum) ->
    map_pvc_vnodes(Tail, SubpID, PVC-1,
                   [{ VnodeIdx, Node, SubpID }] ++ Accum).


%% @private
increment_vnode({vnode_id, Position, RingSize}, Offset) ->
    {vnode_id, add_offset(Position, Offset, RingSize), RingSize}.

%% @private
list_all_vnode_ids(RingSize) ->
    lists:map(fun(Id) -> {vnode_id, Id, RingSize} end,
              lists:seq(0, RingSize - 1)).


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
%% the filters for the traditional cover plans; the filters would be
%% incremented by 1
-spec n_keyspaces(vnode_id(), pos_integer()) -> list(partition_id()).
n_keyspaces({vnode_id, VNode, RingSize}, N) ->
    ordsets:from_list([{partition_id, X rem RingSize, RingSize} ||
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
%% Find the vnode that covers the most of the remaining keyspace. Use
%% VNode ID + offset (determined by request ID) as the tiebreaker
%% (more precisely, the tagged tuple that contains the offset vnode's
%% ID)
find_best_vnode_for_keyspace(PartitionIDs, Available) ->
    CoverCount = [{covers(PartitionIDs, CoversKeys), VNode, TieBreaker} ||
                     {TieBreaker, VNode, CoversKeys} <- Available],

    %% Head of the list is the best result unless all of them have
    %% zero overlap with the partitions for which we need coverage
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

%% @private

next_power_of_two(X) ->
    round(math:pow(2, round(log2(X - 1) + 0.5))).

log2(X) -> math:log(X) / math:log(2.0).

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
    lists:map(fun({partition_id, P, _RingSize}) -> P end,
              Partitions).

n_keyspaces_test() ->
    %% First vnode in a cluster with ring size 64 should (with nval 3)
    %% cover keyspaces 61-63
    ?assertEqual([61, 62, 63], partid(n_keyspaces({vnode_id, 0, 64}, 3))),
    %% 4th vnode in a cluster with ring size 8 should (with nval 5)
    %% cover the first 3 and last 2 keyspaces
    ?assertEqual([0, 1, 2, 6, 7], partid(n_keyspaces({vnode_id, 3, 8}, 5))),
    %% First vnode in a cluster with a single partition should (with
    %% any nval) cover the only keyspace
    ?assertEqual([0], partid(n_keyspaces({vnode_id, 0, 1}, 1))).

covers_test() ->
    %% Count the overlap between the sets
    ?assertEqual(2, covers(?SET([1, 2]),
                           ?SET([0, 1, 2, 3]))),
    ?assertEqual(1, covers(?SET([1, 2]),
                           ?SET([0, 1]))),
    ?assertEqual(0, covers(?SET([1, 2, 3]),
                           ?SET([4, 5, 6, 7]))).

%% `find_best_vnode_for_keyspace' actually takes tagged tuples, but it
%% works fine with plain ol' integers, and the logic is much easier to
%% see this way
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
    {foreach,
     fun cpsetup/0,
     [fun test_create_traditional_plan/1,
      fun test_create_subpartition_plan/1,
      fun test_replace_traditional/1,
      fun test_replace_traditional2/1,
      fun test_replace_traditional3/1,
      fun test_replace_subpartition/1]
    }.

chash_init() ->
    {8,
     [{0,node1},
      {182687704666362864775460604089535377456991567872,node2},
      {365375409332725729550921208179070754913983135744,node3},
      {548063113999088594326381812268606132370974703616,node1},
      {730750818665451459101842416358141509827966271488,node2},
      {913438523331814323877303020447676887284957839360,node3},
      {1096126227998177188652763624537212264741949407232,node1},
      {1278813932664540053428224228626747642198940975104,node2}]}.

cpsetup() ->
    CHash = chash_init(),
    chashbin:create(CHash).

test_replace_traditional3(CHBin) ->
    %% Unlike test_replace_traditional, this function will iterate
    %% through through N request IDs. Seems like an obvious place to
    %% use QuickCheck for more coverage
    OldFilters = [1278813932664540053428224228626747642198940975104, 0],
    ?_assertMatch([true, true, true],
                  lists:map(fun(N) ->
                                    {NewVnodes, NewFilters} =
                                        replace_traditional_chunk(
                                          182687704666362864775460604089535377456991567872,
                                          node2, OldFilters, 3, N, [], riak_kv, CHBin,
                                          fun(_, _) -> [node1, node3] end),

                                    equivalent_coverage(
                                      OldFilters,
                                      NewFilters) andalso
                                        %% Make sure none of the new
                                        %% vnodes live on our "down"
                                        %% node, node3
                                        [] ==
                                        lists:filter(fun({_, node2}) -> true;
                                                        (_) -> false
                                                     end, NewVnodes)
                  end, lists:seq(0, 2))).

test_replace_traditional2(CHBin) ->
    %% Unlike test_replace_traditional, this function will iterate
    %% through through N request IDs. Seems like an obvious place to
    %% use QuickCheck for more coverage

    ?_assertMatch([true, true, true],
                  lists:map(fun(N) ->
                                    {NewVnodes, NewFilters} =
                                        replace_traditional_chunk(
                                          913438523331814323877303020447676887284957839360,
                                          node3, [], 3, N, [], riak_kv, CHBin,
                                          fun(_, _) -> [node1, node2] end),
                                    equivalent_coverage(
                                      913438523331814323877303020447676887284957839360,
                                      NewFilters,
                                      3, CHBin) andalso
                                        %% Make sure none of the new
                                        %% vnodes live on our "down"
                                        %% node, node3
                                        [] ==
                                        lists:filter(fun({_, node3}) -> true;
                                                        (_) -> false
                                                     end, NewVnodes)
                  end, lists:seq(0, 2))).

equivalent_coverage(OldVNode, NewFilters, NVal, CHBin) ->
    RingSize = chashbin:num_partitions(CHBin),
    PartitionSize = chash:ring_increment(RingSize),
    OldFilters = test_vnode_to_filters(OldVNode, NVal, RingSize, PartitionSize),
    equivalent_coverage(OldFilters, NewFilters).

equivalent_coverage(OldFilters, NewFilters) ->
    %% This logic relies on the fact that all of the new vnodes must
    %% have an explicit filter list since we're replacing a full vnode
    %% and there is no other vnode which has exactly the same
    %% partitions
    ConsolidatedFilters = lists:foldl(
                            fun({_VNode, FilterList}, Acc) -> Acc ++ FilterList end,
                            [],
                            NewFilters),
    lists:sort(OldFilters) == lists:sort(ConsolidatedFilters).

test_vnode_to_filters(Index, NVal, RingSize, PartitionSize) ->
    lists:map(fun(N) -> (((Index div PartitionSize) + (RingSize - N)) rem RingSize) * PartitionSize end,
              lists:seq(0, NVal-1)).




test_replace_traditional(CHBin) ->
    %% We're asking or a replacement for the 4th vnode (id 3), with
    %% nval of 3. This means it is responsible for partitions 0, 1, 2,
    %% but given the off-by-one behavior for filter lists the implicit
    %% filters for this vnode are 1, 2, and 3.

    %% Because we need to replace all 3 partitions that this vnode is
    %% responsible for, we're going to need at least 2 vnodes in the
    %% new chunk.

    %% We're reporting that node2 is the only node online, so the
    %% vnodes we have available with relevant partitions are limited
    %% to these two: 182687704666362864775460604089535377456991567872 (id 1)
    %% and 730750818665451459101842416358141509827966271488 (id 4)

    %% vnode 1 has partition 0 (filter 1)
    %% vnode 4 has partitions 1 and 2 (filters 2 and 3)

    {ExpectedVnodes, ExpectedFilters} = {
      [
       {182687704666362864775460604089535377456991567872, node2},
       {730750818665451459101842416358141509827966271488, node2}
      ],
      [
       {182687704666362864775460604089535377456991567872,
        [182687704666362864775460604089535377456991567872]
       },
       {730750818665451459101842416358141509827966271488,
        [365375409332725729550921208179070754913983135744,
         548063113999088594326381812268606132370974703616]
       }
      ]
     },

    {NewVnodes, NewFilters} =
        replace_traditional_chunk(
          548063113999088594326381812268606132370974703616,
          node1, [], 3, 0, [], riak_kv, CHBin,
          fun(_, _) -> [node2] end),

    [?_assertEqual(ExpectedVnodes, lists:sort(NewVnodes)),
     ?_assertEqual(ExpectedFilters, lists:sort(NewFilters))].


test_replace_subpartition(CHBin) ->
    %% Our riak_core_node_watcher replacement function says only node1
    %% is up, so the code will have no choice but to give us back the
    %% only node1 vnode with the zeroth partition
    NewChunk = [{548063113999088594326381812268606132370974703616,
                 node1, {0, 156}}],

    [?_assertEqual(NewChunk,
                   replace_subpartition_chunk(182687704666362864775460604089535377456991567872,
                                              node2, {0, 156}, 3,
                                              0, [], riak_kv, CHBin,
                                              fun(_, _) -> [node1] end))].

test_create_subpartition_plan(CHBin) ->
    Plan =
        [
         {182687704666362864775460604089535377456991567872,
          node2, {0, 156}},
         {182687704666362864775460604089535377456991567872,
          node2, {1, 156}},
         {365375409332725729550921208179070754913983135744,
          node3, {2, 156}},
         {365375409332725729550921208179070754913983135744,
          node3, {3, 156}},
         {548063113999088594326381812268606132370974703616,
          node1, {4, 156}},
         {548063113999088594326381812268606132370974703616,
          node1, {5, 156}},
         {730750818665451459101842416358141509827966271488,
          node2, {6, 156}},
         {730750818665451459101842416358141509827966271488,
          node2, {7, 156}},
         {913438523331814323877303020447676887284957839360,
          node3, {8, 156}},
         {913438523331814323877303020447676887284957839360,
          node3, {9, 156}},
         {1096126227998177188652763624537212264741949407232,
          node1, {10, 156}},
         {1096126227998177188652763624537212264741949407232,
          node1, {11, 156}},
         {1278813932664540053428224228626747642198940975104,
          node2, {12, 156}},
         {1278813932664540053428224228626747642198940975104,
          node2, {13, 156}},
         {0,node1, {14, 156}},
         {0,node1, {15, 156}}
        ],
    [?_assertEqual(Plan,
                   create_subpartition_plan(all, 3, 16, 1, 3, riak_kv, CHBin, fun(_, _) -> [node1, node2, node3] end))].

test_create_traditional_plan(CHBin) ->
    Plan =
        {[{1278813932664540053428224228626747642198940975104,
           node2},
          {730750818665451459101842416358141509827966271488,
           node2},
          {365375409332725729550921208179070754913983135744,
           node3}],
         [{730750818665451459101842416358141509827966271488,
           [548063113999088594326381812268606132370974703616,
            730750818665451459101842416358141509827966271488]}]},
    [?_assertEqual(Plan,
                   create_traditional_plan(all, 3, 1, 1234, riak_kv, CHBin, fun(_, _) -> [node1, node2, node3] end))].

-endif.
