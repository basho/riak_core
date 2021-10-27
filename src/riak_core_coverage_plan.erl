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

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([prop_cover_partitions/0,
            prop_distribution/0,
            prop_find_coverage_partitions/0,
            prop_pvc/0]).
-endif.
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
%% ReqId - a random integer identifier for the request which will be used to
%% provide a randomised input to vary the plans.
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
    % The use of the offset will tend to give an optimal coverage plan in the
    % happy-day scenario (when all vnodes are available).  There is a balance
    % between time of calculation, and how optimal the plan needs to be.  The
    % plan is not necessarily optimal (in terms of involving the fewest number
    % of vnodes).  Nor does it consider location (trying to query as a local
    % to the planning node as possible).
    %
    % There does need to be an even spread of load across plans.  To achieve
    % this we don't treat the ring as a list always starting at 0, instead the
    % ring is first split at a random place.  Otherwise, if the planning were
    % to always start at the front of the ring then those vnodes that cover the
    % tail of the ring will be involved in a disproportionate number of
    % queries.
    {L1, L2} =
        lists:split(ReqId rem PartitionCount,
                        lists:seq(0, PartitionCount - 1)),
    % Use an array to hold a list for each offset, before flattening the array
    % back to a list to rejoin together
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

-spec find_coverage(non_neg_integer(),
                    pos_integer(),
                    pos_integer(),
                    list(non_neg_integer()),
                    pos_integer()) ->
                        {ok, list(vnode_covers())} |
                            {insufficient_vnodes_available,
                                list(non_neg_integer()),
                                list(vnode_covers())}.
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

-ifdef(EQC).

pow(N, Gen) when not is_integer(Gen) ->
    ?LET(Exp, Gen, pow(N, Exp));
pow(_, 0) ->
    1;
pow(N, Exp) when is_integer(Exp), Exp > 0 ->
    N * pow(N, Exp - 1).

uniq_n_out_of_m(N, M) when N =< M ->
    ?LET({Candidates, Split}, {shuffle(lists:seq(0, M - 1)), choose(0, N - 1)},
         begin
             {L, _} = lists:split(Split, Candidates), L
         end).

prop_cover_partitions() ->
    prop_cover_partitions(fun initiate_plan/5).

prop_find_coverage_partitions() ->
    prop_cover_partitions(fun find_coverage/5).

%% A coverage plan should contain all partitions
%% and should not return an unavailable node
prop_cover_partitions(F) ->
    ?FORALL({NVal, PartitionCount}, {choose(3,5), pow(2, choose(3, 10))},
    ?FORALL({ReqId, Unavailable}, {choose(0, PartitionCount - 1), uniq_n_out_of_m(NVal, PartitionCount)},
            begin
                KeySpaces = lists:seq(0, PartitionCount - 1),
                PVC = 1,
                {ok, Plan} = F(ReqId, NVal, PartitionCount, Unavailable, PVC),
                conjunction([{partitions,
                              equals(lists:sort([ P || {_, Ps} <- Plan, P <- Ps ]),
                                     KeySpaces)},
                             {no_unavailable,
                              equals(Unavailable -- [ N || {N, _} <- Plan ],
                                     Unavailable)}])
            end)).

prop_distribution() ->
    % Test only on new function
    % legacy function - find_coverage/5 - will consistently fail on this
    % property
    prop_distribution(fun initiate_plan/5).

%% Compute all possible plans and check that there is no "hot" node that is
%% used more than all other nodes in the plan.
%% Also check that the resulting plans have approximately the same
%% number of nodes to select partitions from (not one plan that takes it from 3
%% nodes and another taking it from 8.
%% If one of the plans fails, then they should all fail. Failing should not
%% depend on the choosen ReqId.
prop_distribution(F) ->
    ?FORALL({NVal, PartitionCount}, {choose(3,5), pow(2, choose(3, 9))},
    ?FORALL(Unavailable, uniq_n_out_of_m(NVal, PartitionCount),
            begin
                PVC = 1,
                Plans = [ F(ReqId, NVal, PartitionCount, Unavailable, PVC)
                          || ReqId <- lists:seq(0, PartitionCount -1) ],
                Tags = lists:usort([ element(1, Plan) || Plan <- Plans ]),
                ?WHENFAIL(eqc:format("Plans: ~p with distribution ~p\n", [Plans, distribution(Plans)]),
                          conjunction([{nok_all_nok, length(Tags) == 1}] ++
                                      [{similar_set, max_diff(length(Unavailable), [ length(Plan) || {_, Plan} <- Plans ])}
                                       || Tags == [ok]] ++
                                          [{hot_node, same_count(distribution(Plans))} || Unavailable == []]))
            end)).

%% Computing with PVC larger than 1 takes a lot of time.
%% We take slightly smaller paertition size here to test more in same time
%%
%% If PVC is 2 then each partition should appear on two returned nodes
%% But, if nodes are unavailable, then this might not work out, then
%% a next best solution should be presented.
prop_pvc() ->
    ?FORALL({NVal, PartitionCount}, {choose(3,5), pow(2, choose(3, 8))},
    ?FORALL({ReqId, Unavailable}, {choose(0, PartitionCount),
                                   uniq_n_out_of_m(NVal, PartitionCount)},
    ?FORALL(PVC, choose(1, NVal),
            collect({pvc, PVC},
            begin
                PVCPlan = initiate_plan(ReqId, NVal, PartitionCount, Unavailable, PVC),
                OnePlan = initiate_plan(ReqId, NVal, PartitionCount, Unavailable, 1),
                case {OnePlan, PVCPlan} of
                    {{ok, _}, {ok, Plan}} ->
                         Distribution = distribution([ P || {_, Ps} <- Plan, P <- Ps ], []),
                         ?WHENFAIL(eqc:format("Plan: ~p with distribution ~p\n", [Plan, Distribution]),
                                   conjunction([{partition_count, same_count([{x, PVC} | Distribution])}]));
                    {{ok, _}, {insufficient_vnodes_available, _, Plan}} ->
                        Distribution = distribution([ P || {_, Ps} <- Plan, P <- Ps ], []),
                        conjunction([{unavailable, PVC > NVal - length(Unavailable)},
                                     {partitions,
                                      equals(lists:usort([ P || {_, Ps} <- Plan, P <- Ps ]),
                                             lists:seq(0, PartitionCount - 1))},
                                     {no_unavailable,
                                      equals(Unavailable -- [ N || {N, _} <- Plan ],
                                             Unavailable)},
                                     {good_enough, equals([ Count || {_, Count} <- Distribution,
                                                                     Count <  NVal - length(Unavailable)], [])}]);
                    _ ->
                        equals(element(1, PVCPlan), insufficient_vnodes_available)
                end
            end)))).

max_diff(N, List) ->
    lists:max(List) - lists:min(List) =< N.

distribution(Plans) ->
    distribution([ Node || {_, Plan} <- Plans, {Node, _} <- Plan ], []).

distribution([], Dist) ->
    Dist;
distribution([N|Ns], Dist) ->
    case lists:keyfind(N, 1, Dist) of
        false ->
            distribution(Ns, [{N, 1} | Dist]);
        {_, Count} ->
             distribution(Ns, lists:keyreplace(N, 1, Dist, {N, Count +1}))
    end.

same_count([]) ->
    true;
same_count([{_, Count} | Rest]) ->
    equals([ C || {_, C} <- Rest, C /= Count ], []).


-endif.

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
    MaxCount = changing_repeated_vnode_tester(fun find_coverage/5, 200),

    % This assertion is bad!
    % This demonstrates that the prefactored code fails the test of providing
    % well distributed coverage plans
    ?assertEqual(true, MaxCount > 100).

changing_repeated_vnode_refactor_test() ->
    MaxCount = changing_repeated_vnode_tester(fun initiate_plan/5, 200),

    ?assertEqual(true, MaxCount < 100).


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
                "~nVnode=~w MaxCount=~w out of ~w queries~n",
                [Vnode, MaxCount, Runs]),

    MaxCount.

all_refactor_1024ring_test_() ->
    {timeout, 1200, fun all_refactor_1024_ring_tester/0}.

all_refactor_1024_ring_tester() ->
    PVC = 1,
    TestFun =
        fun(ReqId) ->
            {ok, VnodeCovers} = initiate_plan(ReqId, 3, 1024, [], PVC),
            PC = array:new(1024, {default, 0}),
            PC1 =
                lists:foldl(
                    fun({_I, L}, Acc) ->
                        lists:foldl(fun(P, IA) ->
                                            array:set(P,
                                                        array:get(P, IA) + 1,
                                                        IA)
                                        end,
                                        Acc,
                                        L)
                    end,
                    PC,
                    VnodeCovers),
            lists:foreach(fun(C) -> ?assertEqual(PVC, C) end, array:to_list(PC1))
        end,

    lists:foreach(fun(I) -> TestFun(I) end, lists:seq(0, 1023)).


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
    ring_tester(PartitionCount, CoverageFun, 0).

ring_tester(PartitionCount, CoverageFun, ReqId) ->
    NVal = 3,
    UnavailableKeySpaces = [],
    PVC = 1,
    {Vnodes, CoveredKeySpaces} =
        ring_tester(PartitionCount, CoverageFun,
                    ReqId, NVal, UnavailableKeySpaces, PVC),
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

multifailure_r2_post_test() ->
    multi_failure_tester(32, fun initiate_plan/5),
    multi_failure_tester(64, fun initiate_plan/5),
    multi_failure_tester(128, fun initiate_plan/5),
    multi_failure_tester(256, fun initiate_plan/5),
    multi_failure_tester(512, fun initiate_plan/5),
    multi_failure_tester(1024, fun initiate_plan/5).

multifailure_r2_pre_test() ->
    multi_failure_tester(32, fun find_coverage/5),
    multi_failure_tester(64, fun find_coverage/5),
    multi_failure_tester(128, fun find_coverage/5),
    multi_failure_tester(256, fun find_coverage/5),
    multi_failure_tester(512, fun find_coverage/5).

multi_failure_tester(PartitionCount, CoverageFun) when PartitionCount >= 32 ->
    % If there are failures at least target_n_val appart, a r=2 coverage plan
    % can still be produced
    ReqId = rand:uniform(99999),
    NVal = 3,
    PVC = 2,
    C = rand:uniform(PartitionCount div 8),
    UnavailableKeySpaces = lists:map(fun(I) -> I * 4 - C end, lists:seq(1, 8)),
    {ok, VnodeCovers} =
        CoverageFun(ReqId, NVal, PartitionCount,
                    UnavailableKeySpaces, PVC),
    PC = array:new(PartitionCount, {default, 0}),
    PC0 =
        lists:foldl(
            fun({I, L}, Acc) ->
                ?assertNotEqual(true, lists:member(I, UnavailableKeySpaces)),
                lists:foldl(fun(P, IA) ->
                                    array:set(P, array:get(P, IA) + 1, IA)
                                end,
                                Acc,
                                L)
            end,
            PC,
            VnodeCovers),
    lists:foreach(fun(Cnt) -> ?assertEqual(2, Cnt) end, array:to_list(PC0)),

    % Fail two vnodes together - now can only get partial coverage from a r=2
    % plan
    RVN = rand:uniform(PartitionCount),
    UnavailableKeySpaces1 = [RVN - 1, (RVN - 2) rem PartitionCount],
    {insufficient_vnodes_available, _, VnodeCovers1} =
        CoverageFun(ReqId, NVal, PartitionCount,
                    UnavailableKeySpaces1, PVC),
    PC1 =
        lists:foldl(
            fun({I, L}, Acc) ->
                ?assertNotEqual(true, lists:member(I, UnavailableKeySpaces1)),
                lists:foldl(fun(P, IA) ->
                                    array:set(P, array:get(P, IA) + 1, IA)
                                end,
                                Acc,
                                L)
            end,
            PC,
            VnodeCovers1),
    Covered =
        length(lists:filter(fun(Cnt) -> Cnt == 2 end, array:to_list(PC1))),
    PartiallyCovered =
        length(lists:filter(fun(Cnt) -> Cnt == 1 end, array:to_list(PC1))),
    ?assertEqual(2, PartiallyCovered),
    ?assertEqual(2, PartitionCount - Covered).


-endif.
