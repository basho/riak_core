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

%% @doc riak_core_gossip takes care of the mechanics of shuttling a from one
%% node to another upon request by other Riak processes.
%%
%% Additionally, it occasionally checks to make sure the current node has its
%% fair share of partitions, and also sends a copy of the ring to some other
%% random node, ensuring that all nodes eventually synchronize on the same
%% understanding of the Riak cluster. This interval is configurable, but
%% defaults to once per minute.

-module(riak_core_gossip).

-behaviour(gen_server).

-export([start_link/0, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export ([distribute_ring/1, send_ring/1, send_ring/2, remove_from_cluster/2,
          remove_from_cluster/3, random_gossip/1,
          recursive_gossip/1, random_recursive_gossip/1, rejoin/2,
          gossip_version/0]).

-include("riak_core_ring.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Default gossip rate: allow at most 45 gossip messages every 10 seconds
-define(DEFAULT_LIMIT, {45, 10000}).

-record(state, {gossip_versions,
                gossip_tokens}).

%% ===================================================================
%% Public API
%% ===================================================================

%% distribute_ring/1 -
%% Distribute a ring to all members of that ring.
distribute_ring(Ring) ->
    gen_server:cast({?MODULE, node()}, {distribute_ring, Ring}).

%% send_ring/1 -
%% Send the current node's ring to some other node.
send_ring(ToNode) -> send_ring(node(), ToNode).

%% send_ring/2 -
%% Send the ring from one node to another node.
%% Does nothing if the two nodes are the same.
send_ring(Node, Node) ->
    ok;
send_ring(FromNode, ToNode) ->
    gen_server:cast({?MODULE, FromNode}, {send_ring_to, ToNode}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).

rejoin(Node, Ring) ->
    gen_server:cast({?MODULE, Node}, {rejoin, Ring}).


%% @doc Gossip state to a random node in the ring.
random_gossip(Ring) ->
    case riak_core_ring:random_other_active_node(Ring) of
        no_node -> % must be single node cluster
            ok;
        RandomNode ->
            send_ring(node(), RandomNode)
    end.

%% @doc Gossip state to a fixed set of nodes determined from a binary
%%      tree decomposition of the membership state. Recursive gossip
%%      converts the list of node members into a binary tree and
%%      gossips to the given node's right/left children. The gossip
%%      is considered recursive, because each receiving node may also
%%      call recursive_gossip therefore gossiping to their children.
%%      The fan-out therefore expands logarithmically to cover the
%%      entire cluster.
recursive_gossip(Ring, Node) ->
    Nodes = riak_core_ring:active_members(Ring),
    Tree = riak_core_util:build_tree(2, Nodes, [cycles]),
    Children = orddict:fetch(Node, Tree),
    _ = [send_ring(node(), OtherNode) || OtherNode <- Children],
    ok.
recursive_gossip(Ring) ->
    %% A non-active member will not show-up in the tree decomposition
    %% and therefore we fallback to random_recursive_gossip as necessary.
    Active = riak_core_ring:active_members(Ring),
    case lists:member(node(), Active) of
        true ->
            recursive_gossip(Ring, node());
        false ->
            random_recursive_gossip(Ring)
    end.

random_recursive_gossip(Ring) ->
    Active = riak_core_ring:active_members(Ring),
    RNode = lists:nth(rand:uniform(length(Active)), Active),
    recursive_gossip(Ring, RNode).

%% ===================================================================
%% gen_server behaviour
%% ===================================================================

%% @private
init(_State) ->
    schedule_next_reset(),
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    {Tokens, _} = app_helper:get_env(riak_core, gossip_limit, ?DEFAULT_LIMIT),
    State = update_known_versions(Ring,
                                  #state{gossip_versions=orddict:new(),
                                         gossip_tokens=Tokens}),
    {ok, State}.

handle_call(_, _From, State) ->
    {reply, ok, State}.

update_gossip_version(Ring) ->
    CurrentVsn = riak_core_ring:get_member_meta(Ring, node(), gossip_vsn),
    DesiredVsn = gossip_version(),
    case CurrentVsn of
        DesiredVsn ->
            Ring;
        _ ->
            Ring2 = riak_core_ring:update_member_meta(node(), Ring, node(),
                                                      gossip_vsn, DesiredVsn),
            Ring2
    end.


update_known_version(Node, {OtherRing, GVsns}) ->
    case riak_core_ring:get_member_meta(OtherRing, Node, gossip_vsn) of
        undefined ->
            case riak_core_ring:owner_node(OtherRing) of
                Node ->
                    %% Ring owner defaults to legacy gossip if unspecified.
                    {OtherRing, orddict:store(Node, ?LEGACY_RING_VSN, GVsns)};
                _ ->
                    {OtherRing, GVsns}
            end;
        GossipVsn ->
            {OtherRing, orddict:store(Node, GossipVsn, GVsns)}
    end.

update_known_versions(OtherRing, State=#state{gossip_versions=GVsns}) ->
    {_, GVsns2} = lists:foldl(fun update_known_version/2,
                              {OtherRing, GVsns},
                              riak_core_ring:all_members(OtherRing)),
    State#state{gossip_versions=GVsns2}.

gossip_version() ->
    %% Now that we can safely assume all nodes support capabilities, this
    %% should be replaced with a capability someday.
    ?CURRENT_RING_VSN.

rpc_gossip_version(Ring, Node) ->
    GossipVsn = riak_core_ring:get_member_meta(Ring, Node, gossip_vsn),
    case GossipVsn of
        undefined ->
            case riak_core_util:safe_rpc(Node, riak_core_gossip, gossip_version, [], 1000) of
                {badrpc, _} ->
                    ?LEGACY_RING_VSN;
                Vsn ->
                    Vsn
            end;
        _ ->
            GossipVsn
    end.

%% @private
handle_cast({send_ring_to, _Node}, State=#state{gossip_tokens=0}) ->
    %% Out of gossip tokens, ignore the send request
    {noreply, State};
handle_cast({send_ring_to, Node}, State) ->
    {ok, MyRing0} = riak_core_ring_manager:get_raw_ring(),
    MyRing = update_gossip_version(MyRing0),
    GossipVsn = rpc_gossip_version(MyRing, Node),
    RingOut = riak_core_ring:downgrade(GossipVsn, MyRing),
    riak_core_ring:check_tainted(RingOut,
                                 "Error: riak_core_gossip/send_ring_to :: "
                                 "Sending tainted ring over gossip"),
    gen_server:cast({?MODULE, Node}, {reconcile_ring, RingOut}),
    Tokens = State#state.gossip_tokens - 1,
    {noreply, State#state{gossip_tokens=Tokens}};

handle_cast({distribute_ring, Ring}, State) ->
    Nodes = riak_core_ring:active_members(Ring),
    riak_core_ring:check_tainted(Ring,
                                 "Error: riak_core_gossip/distribute_ring :: "
                                 "Sending tainted ring over gossip"),
    gen_server:abcast(Nodes, ?MODULE, {reconcile_ring, Ring}),
    {noreply, State};

handle_cast({reconcile_ring, RingIn}, State) ->
    OtherRing = riak_core_ring:upgrade(RingIn),
    State2 = update_known_versions(OtherRing, State),
    %% Compare the two rings, see if there is anything that
    %% must be done to make them equal...
    riak_core_stat:update(gossip_received),
    riak_core_ring_manager:ring_trans(fun reconcile/2, [OtherRing]),
    {noreply, State2};

handle_cast(gossip_ring, State) ->
    % Gossip the ring to some random other node...
    {ok, MyRing} = riak_core_ring_manager:get_raw_ring(),

    random_gossip(MyRing),
    {noreply, State};

handle_cast({rejoin, RingIn}, State) ->
    OtherRing = riak_core_ring:upgrade(RingIn),
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    SameCluster =
        (riak_core_ring:cluster_name(Ring) =:=
            riak_core_ring:cluster_name(OtherRing)),
    case SameCluster of
        true ->
            OtherNode = riak_core_ring:owner_node(OtherRing),
            ok = 
                case riak_core:join(false, node(), OtherNode, true, true) of
                    ok ->
                        ok;
                    {error, Reason} ->
                        lager:error("Could not rejoin cluster: ~p", [Reason]),
                        ok
                end,
            {noreply, State};
        false ->
            {noreply, State}
    end;

handle_cast(_, State) ->
    {noreply, State}.

handle_info(reset_tokens, State) ->
    schedule_next_reset(),
    gen_server:cast(?MODULE, gossip_ring),
    {Tokens, _} = app_helper:get_env(riak_core, gossip_limit, ?DEFAULT_LIMIT),
    {noreply, State#state{gossip_tokens=Tokens}};

handle_info(_Info, State) -> {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ===================================================================
%% Internal functions
%% ===================================================================

schedule_next_reset() ->
    {_, Reset} = app_helper:get_env(riak_core, gossip_limit, ?DEFAULT_LIMIT),
    erlang:send_after(Reset, ?MODULE, reset_tokens).

reconcile(Ring0, [OtherRing0]) ->
    %% Due to rolling upgrades and legacy gossip, a ring's cluster name
    %% may be temporarily undefined. This is eventually fixed by the claimant.
    {Ring, OtherRing} = riak_core_ring:reconcile_names(Ring0, OtherRing0),
    Node = node(),
    OtherNode = riak_core_ring:owner_node(OtherRing),
    Members = riak_core_ring:reconcile_members(Ring, OtherRing),
    WrongCluster = (riak_core_ring:cluster_name(Ring) /=
                    riak_core_ring:cluster_name(OtherRing)),
    PreStatus = riak_core_ring:member_status(Members, OtherNode),
    IgnoreGossip = (WrongCluster or
                    (PreStatus =:= invalid) or
                    (PreStatus =:= down)),
    {Changed, Ring2} = 
        case IgnoreGossip of
            true ->
                {false, Ring};
            false ->
                riak_core_ring:reconcile(OtherRing, Ring)
        end,
    OtherStatus = riak_core_ring:member_status(Ring2, OtherNode),
    case {WrongCluster, OtherStatus, Changed} of
        {true, _OS, _C} ->
            %% TODO: Tell other node to stop gossiping to this node.
            riak_core_stat:update(ignored_gossip),
            ignore;
        {false, down, _C} ->
            %% Tell other node to rejoin the cluster.
            riak_core_gossip:rejoin(OtherNode, Ring2),
            ignore;
        {false, invalid, _C} ->
            %% Exiting/Removed node never saw shutdown cast, re-send.
            ClusterName = riak_core_ring:cluster_name(Ring),
            riak_core_ring_manager:refresh_ring(OtherNode, ClusterName),
            ignore;
        {false, _OS, new_ring} ->
            Ring3 = riak_core_ring:ring_changed(Node, Ring2),
            riak_core_stat:update(rings_reconciled),
            log_membership_changes(Ring, Ring3),
            {reconciled_ring, Ring3};
        {false, _OS, _C} ->
            ignore
    end.

log_membership_changes(OldRing, NewRing) ->
    OldStatus = riak_core_ring:all_member_status(OldRing),
    NewStatus = riak_core_ring:all_member_status(NewRing),

    do_log_membership_changes(lists:sort(OldStatus), lists:sort(NewStatus)).

do_log_membership_changes([], []) ->
    ok;
do_log_membership_changes([{Node, Status}|Old], [{Node, Status}|New]) ->
    %% No change
    do_log_membership_changes(Old, New);
do_log_membership_changes([{Node, Status1}|Old], [{Node, Status2}|New]) ->
    %% State changed, did not join or leave
    log_node_changed(Node, Status1, Status2),
    do_log_membership_changes(Old, New);
do_log_membership_changes([{OldNode, _OldStatus}|_]=Old, [{NewNode, NewStatus}|New]) when NewNode < OldNode->
    %% Node added
    log_node_added(NewNode, NewStatus),
    do_log_membership_changes(Old, New);
do_log_membership_changes([{OldNode, OldStatus}|Old], [{NewNode, _NewStatus}|_]=New) when OldNode < NewNode ->
    %% Node removed
    log_node_removed(OldNode, OldStatus),
    do_log_membership_changes(Old, New);
do_log_membership_changes([{OldNode, OldStatus}|Old], []) ->
    %% Trailing nodes were removed
    log_node_removed(OldNode, OldStatus),
    do_log_membership_changes(Old, []);
do_log_membership_changes([], [{NewNode, NewStatus}|New]) ->
    %% Trailing nodes were added
    log_node_added(NewNode, NewStatus),
    do_log_membership_changes([], New).

log_node_changed(Node, Old, New) ->
    lager:info("'~s' changed from '~s' to '~s'~n", [Node, Old, New]).

log_node_added(Node, New) ->
    lager:info("'~s' joined cluster with status '~s'~n", [Node, New]).

log_node_removed(Node, Old) ->
    lager:info("'~s' removed from cluster (previously: '~s')~n", [Node, Old]).

remove_from_cluster(Ring, ExitingNode) ->
    remove_from_cluster(Ring, ExitingNode, rand:seed(exrop, os:timestamp())).

remove_from_cluster(Ring, ExitingNode, Seed) ->
    % Transfer indexes to other nodes...
    Owners = riak_core_ring:all_owners(Ring),
    Members = riak_core_ring:claiming_members(Ring),
    ExitRing =
        case attempt_simple_transfer(Ring, ExitingNode, Seed,
                                        Owners, Members) of
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
                riak_core_claim:sequential_claim(TempRing, HN)
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
    ForceRebalance =
        app_helper:get_env(riak_core, full_rebalance_onleave, false),
    case ForceRebalance of
        true ->
            force_rebalance;
        false ->
            TargetN = app_helper:get_env(riak_core, target_n_val),
            Counts =
                riak_core_claim:get_counts(Members, Owners),
            RingFun = 
                fun(Partition, Node, R) ->
                    riak_core_ring:transfer_node(Partition, Node, R),
                    R
                end,
            simple_transfer(Owners,
                            {RingFun, TargetN, ExitingNode},
                            Ring,
                            {Seed, [], Counts})
    end.

%% @doc Simple transfer of leaving node's vnodes to safe place
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
    %% order
    %% Prev - this is the part of the ring that has already been processed, 
    %% which is also in partition order
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
                not lists:keymember(Node, 2, UnsafeNodeTuples)
            end
        end,
    %% Filter candidate Nodes looking back in the ring at previous allocations
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

