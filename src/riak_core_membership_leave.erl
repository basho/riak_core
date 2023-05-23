%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_core_membership_leave).

-include("riak_core_ring.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([remove_from_cluster/2, remove_from_cluster/3, remove_from_cluster/4]).

remove_from_cluster(Ring, ExitingNode) ->
    remove_from_cluster(
        Ring, ExitingNode, rand:seed(exrop, os:timestamp())).

remove_from_cluster(Ring, ExitingNode, Seed) ->
    ForceRebalance =
        app_helper:get_env(riak_core, full_rebalance_onleave, false),
    remove_from_cluster(Ring, ExitingNode, Seed, ForceRebalance).


remove_from_cluster(Ring, ExitingNode, Seed, ForceRebalance) ->
    % Transfer indexes to other nodes...
    Owners = riak_core_ring:all_owners(Ring),
    Members = riak_core_ring:claiming_members(Ring),

    STR = 
        case ForceRebalance of
            true ->
                force_rebalance;
            false ->
                attempt_simple_transfer(
                    Ring, ExitingNode, Seed, Owners, Members)
        end,

    ExitRing =
        case STR of
            {ok, NR} ->
                NR;
            _ ->
                %% re-diagonalize
                %% first hand off all claims to *any* one else,
                %% just so rebalance doesn't include exiting node
                HN = hd(lists:delete(ExitingNode, Members)),
                TempRing =
                    lists:foldl(fun({I,N}, R) when N == ExitingNode ->
                                        riak_core_ring:transfer_node(I, HN, R);
                                    (_, R) ->
                                        R
                                end,
                                Ring,
                                Owners),
                riak_core_membership_claim:full_rebalance(TempRing, HN)
        end,
    ExitRing.

-ifdef(TEST).
-type transfer_ring() :: [{integer(), term()}].
-else.
-type transfer_ring() :: riak_core_ring:riak_core_ring().
-endif.

%% @doc Simple transfer of leaving node's vnodes to safe place
%% Where safe place is any node that satisfies target_n_val for that vnode -
%% but with a preference to transfer to a node that has a lower number of 
%% vnodes currently allocated.
%% If safe places cannot be found for all vnodes returns `target_n_fail`
%% Simple transfer is not location aware, but generally this wll be an initial
%% phase of a plan, and hence a temporary home - so location awareness is not
%% necessary.
%% `riak_core.full_rebalance_onleave = true` may be used to avoid this step,
%% although this may result in a large number of transfers
-spec attempt_simple_transfer(transfer_ring(),
                                term(),
                                random:ran(),
                                [{integer(), term()}],
                                [term()]) ->
                                    {ok, transfer_ring()}|
                                        target_n_fail|
                                        force_rebalance.
attempt_simple_transfer(Ring, ExitingNode, Seed, Owners, Members) ->
    TargetN = app_helper:get_env(riak_core, target_n_val),
    Counts =
        riak_core_membership_claim:get_counts(Members, Owners),
    RingFun = 
        fun(Partition, Node, R) ->
            riak_core_ring:transfer_node(Partition, Node, R),
            R
        end,
    simple_transfer(Owners,
                    {RingFun, TargetN, ExitingNode},
                    Ring,
                    {Seed, [], Counts}).

%% @doc Simple transfer of leaving node's vnodes to safe place
%% Iterates over Owners, which must be sorted by Index (from 0...), and
%% attempts to safely re-allocate each ownerhsip which is currently set to
%% the exiting node
-spec simple_transfer([{integer(), term()}],
                        {fun((integer(),
                                term(),
                                transfer_ring()) -> transfer_ring()),
                            pos_integer(),
                            term()},
                        transfer_ring(),
                        {random:ran(),
                            [{integer(), term()}],
                            [{term(), non_neg_integer()}]}) ->
                                {ok, transfer_ring()}|target_n_fail.
simple_transfer([{P, ExitingNode}|Rest],
                        {RingFun, TargetN, ExitingNode},
                        Ring,
                        {Seed, Prev, Counts}) ->
    %% The ring is split into two parts:
    %% Rest - this is forward looking from the current partition, in partition
    %% order (ascending by partition number)
    %% Prev - this is the part of the ring that has already been processed, 
    %% which is also in partition order (but descending by index number)
    %%
    %% With a ring size of 8, having looped to partition 3:
    %% Rest = [{4, N4}, {5, N5}, {6, N6}, {7, N7}]
    %% Prev = [{2, N2}, {1, N1}, {0, N0}]
    %%
    %% If we have a partition that is on the Exiting Node it is necessary to
    %% look forward (TargetN - 1) allocations in Rest.  It is also necessary
    %% to look backward (TargetN - 1) allocations in Prev (from the rear of the
    %% Prev list).
    %%
    %% This must be treated as a Ring though - as we reach an end of the list
    %% the search must wrap around to the other end of the alternate list (i.e.
    %% from 0 -> 7 and from 7 -> 0).
    CheckRingFun =
        fun(ForwardL, BackL) ->
            Steps = TargetN - 1,
            UnsafeNodeTuples =
                case length(ForwardL) of 
                    L when L < Steps ->
                        ForwardL ++
                            lists:sublist(lists:reverse(BackL), Steps - L);
                    _ ->
                        lists:sublist(ForwardL, Steps)
                end,
            fun({Node, _Count}) ->
                %% Nodes will remain as candidates if they are not in the list
                %% of unsafe nodes
                not lists:keymember(Node, 2, UnsafeNodeTuples)
            end
        end,
    %% Filter candidate Nodes looking back in the ring at previous allocations.
    %% The starting list of candidates is the list the claiming members in
    %% Counts.
    CandidatesB = lists:filter(CheckRingFun(Prev, Rest), Counts),
    %% Filter candidate Nodes looking forward in the ring at existing
    %% allocations
    CandidatesF = lists:filter(CheckRingFun(Rest, Prev), CandidatesB),

    %% Qualifying candidates will be tuples of {Node, Count} where the Count
    %% is that node's current count of allocated vnodes
    case CandidatesF of
        [] ->
            target_n_fail;
        Qualifiers ->
            %% Look at the current allocated vnode counts for each qualifying
            %% node, and find all qualifying nodes with the lowest of these
            %% counts
            [{Q0, BestCnt}|Others] = lists:keysort(2, Qualifiers),
            PreferredCandidates =
                [{Q0, BestCnt}|
                    lists:takewhile(fun({_, C}) -> C == BestCnt end, Others)],
            
            %% Final selection of a node as a destination for this partition,
            %% The node Counts must be updated to reflect this allocation, and
            %% the RingFun applied to actually queue the transfer
            {Rand, Seed2} = rand:uniform_s(length(PreferredCandidates), Seed),
            {Chosen, BestCnt} = lists:nth(Rand, PreferredCandidates),
            UpdRing = RingFun(P, Chosen, Ring),
            UpdCounts =
                lists:keyreplace(Chosen, 1, Counts, {Chosen, BestCnt + 1}),
            simple_transfer(Rest,
                            {RingFun, TargetN, ExitingNode},
                            UpdRing,
                            {Seed2, [{P, Chosen}|Prev], UpdCounts})
    end;
simple_transfer([{P, N}|Rest], Statics, Ring, {Seed, Prev, Counts}) ->
    %% This is already allocated to a node other than the exiting node, so
    %% simply transition to the Previous ring accumulator
    simple_transfer(Rest, Statics, Ring, {Seed, [{P, N}|Prev], Counts});
simple_transfer([], _Statics, Ring, _LoopAccs) ->
    {ok, Ring}.


%% ===================================================================
%% Unit tests
%% ===================================================================

-ifdef(TEST).

test_ring_fun(P, N, R) ->
    lists:keyreplace(P, 1, R, {P, N}).

count_nodes(TestRing) ->
    CountFun =
        fun({_P, N}, Acc) ->
            case lists:keyfind(N, 1, Acc) of
                false ->
                    lists:ukeysort(1, [{N, 1}|Acc]);
                {N, C} ->
                    lists:ukeysort(1, [{N, C + 1}|Acc])
            end
        end,
    lists:foldl(CountFun, [], TestRing).

simple_transfer_simple_test() ->
    R0 = [{0, n5}, {1, n1}, {2, n2}, {3, n3},
            {4, n4}, {5, n5}, {6, n3}, {7, n2}],
    SomeTime = {1632,989499,279637},
    FixedSeed = rand:seed(exrop, SomeTime),
    {ok, R1} =
        simple_transfer(R0,
                        {fun test_ring_fun/3, 3, n4},
                        R0,
                        {FixedSeed,
                            [],
                            lists:keydelete(n4, 1, count_nodes(R0))}),
    ?assertMatch({4, n1}, lists:keyfind(4, 1, R1)),
    
    {ok, R2} =
        simple_transfer(R0,
                        {fun test_ring_fun/3, 3, n5},
                        R0,
                        {FixedSeed,
                            [],
                            lists:keydelete(n5, 1, count_nodes(R0))}),
    ?assertMatch({0, n4}, lists:keyfind(0, 1, R2)),
    ?assertMatch({5, n1}, lists:keyfind(5, 1, R2)),

    {ok, R3} =
        simple_transfer(R0,
                        {fun test_ring_fun/3, 3, n1},
                        R0,
                        {FixedSeed,
                            [],
                            lists:keydelete(n1, 1, count_nodes(R0))}),
    ?assertMatch({1, n4}, lists:keyfind(1, 1, R3)),

    target_n_fail =
        simple_transfer(R0,
                        {fun test_ring_fun/3, 3, n3},
                        R0,
                        {FixedSeed,
                            [],
                            lists:keydelete(n3, 1, count_nodes(R0))}),
    
    target_n_fail =
        simple_transfer(R0,
                        {fun test_ring_fun/3, 3, n2},
                        R0,
                        {FixedSeed,
                            [],
                            lists:keydelete(n2, 1, count_nodes(R0))}),
    
    %% Target n failures due to wrap-around tail violations
    R4 = [{0, n5}, {1, n1}, {2, n2}, {3, n3},
            {4, n4}, {5, n2}, {6, n3}, {7, n4}],
    
    target_n_fail =
        simple_transfer(R4,
                        {fun test_ring_fun/3, 3, n5},
                        R4,
                        {FixedSeed,
                            [],
                            lists:keydelete(n5, 1, count_nodes(R4))}),
    
    target_n_fail =
        simple_transfer(R4,
                        {fun test_ring_fun/3, 3, n4},
                        R4,
                        {FixedSeed,
                            [],
                            lists:keydelete(n4, 1, count_nodes(R4))}).

simple_transfer_needstobesorted_test() ->
    lists:foreach(fun transfer_needstobesorted_tester/1, lists:seq(1, 100)).

transfer_needstobesorted_tester(I) ->
    R0 = [{6,n3}, {13,n3}, {12,n6}, {11,n5}, {10,n4}, {9,n3}, {8,n2},
            {7,n1}, {5,n6}, {4,n5}, {3,n4}, {2,n3}, {1,n2}, {0,n1}],
    VariableSeed = rand:seed(exrop, {1632, 989499, I * 13}),
    {ok, R1} =
        simple_transfer(lists:keysort(1, R0),
                        {fun test_ring_fun/3, 3, n3},
                        R0,
                        {VariableSeed,
                            [],
                            lists:keydelete(n3, 1, count_nodes(R0))}),
    ?assertMatch({13, n4}, lists:keyfind(13, 1, R1)).

simple_transfer_evendistribution_test() ->
    R0 = [{0, n1}, {1, n2}, {2, n3}, {3, n4}, {4, n5}, 
            {5, n6}, {6, n7}, {7, n8}, {8, n9}, {9, n10},
            {10, n1}, {11, n2}, {12, n3}, {13, n4}, {14, n5},
            {15, n6}, {16, n7}, {17, n8}, {18, n9}, {19, n10},
            {20, n1}, {21, n2}, {22, n3}, {23, n4}, {24, n5}, 
            {25, n6}, {26, n7}, {27, n8}, {28, n9}, {29, n10},
            {30, n1}, {31, n2}, {32, n3}, {33, n4}, {34, n5},
            {35, n6}, {36, n7}, {37, n8}, {38, n9}, {39, n10},
            {40, n1}, {41, n2}, {42, n3}, {43, n4}, {44, n5}, 
            {45, n6}, {46, n7}, {47, n8}, {48, n9}, {49, n10},
            {50, n1}, {51, n2}, {52, n3}, {53, n4}, {54, n5},
            {55, n6}, {56, n1}, {57, n2}, {58, n3}, {59, n10},
            {60, n5}, {61, n6}, {62, n7}, {63, n8}],
    
    SomeTime = {1632,989499,279637},
    FixedSeed = rand:seed(exrop, SomeTime),
    {ok, R1} =
        simple_transfer(R0,
                        {fun test_ring_fun/3, 3, n1},
                        R0,
                        {FixedSeed,
                            [],
                            lists:keydelete(n1, 1, count_nodes(R0))}),
    
    NodeCounts = lists:keysort(2, count_nodes(R1)),
    io:format("NodeCounts ~w~n", [NodeCounts]),
    [{_LN, LC}|Rest] = NodeCounts,
    [{_HN, HC}|_] = lists:reverse(Rest),
    true = HC - LC == 2.


-endif.

