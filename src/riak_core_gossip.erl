%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
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
    RNode = lists:nth(random:uniform(length(Active)), Active),
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
                    {OtherRing, orddict:store(Node, ?CURRENT_RING_VSN, GVsns)};
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
    SameCluster = (riak_core_ring:cluster_name(Ring) =:=
                       riak_core_ring:cluster_name(OtherRing)),
    case SameCluster of
        true ->
            OtherNode = riak_core_ring:owner_node(OtherRing),
            case riak_core:join(node(), OtherNode, true, true) of
                ok -> ok;
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
    case IgnoreGossip of
        true ->
            Ring2 = Ring,
            Changed = false;
        false ->
            {Changed, Ring2} =
                riak_core_ring:reconcile(OtherRing, Ring)
    end,
    OtherStatus = riak_core_ring:member_status(Ring2, OtherNode),
    case {WrongCluster, OtherStatus, Changed} of
        {true, _, _} ->
            %% TODO: Tell other node to stop gossiping to this node.
            riak_core_stat:update(ignored_gossip),
            ignore;
        {_, down, _} ->
            %% Tell other node to rejoin the cluster.
            riak_core_gossip:rejoin(OtherNode, Ring2),
            ignore;
        {_, invalid, _} ->
            %% Exiting/Removed node never saw shutdown cast, re-send.
            ClusterName = riak_core_ring:cluster_name(Ring),
            riak_core_ring_manager:refresh_ring(OtherNode, ClusterName),
            ignore;
        {_, _, new_ring} ->
            Ring3 = riak_core_ring:ring_changed(Node, Ring2),
            riak_core_stat:update(rings_reconciled),
            log_membership_changes(Ring, Ring3),
            {reconciled_ring, Ring3};
        {_, _, _} ->
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
    remove_from_cluster(Ring, ExitingNode, erlang:now()).

remove_from_cluster(Ring, ExitingNode, Seed) ->
    % Get a list of indices owned by the ExitingNode...
    AllOwners = riak_core_ring:all_owners(Ring),

    % Transfer indexes to other nodes...
    ExitRing =
        case attempt_simple_transfer(Seed, Ring, AllOwners, ExitingNode) of
            {ok, NR} ->
                NR;
            target_n_fail ->
                %% re-diagonalize
                %% first hand off all claims to *any* one else,
                %% just so rebalance doesn't include exiting node
                Members = riak_core_ring:claiming_members(Ring),
                Other = hd(lists:delete(ExitingNode, Members)),
                TempRing = lists:foldl(
                             fun({I,N}, R) when N == ExitingNode ->
                                     riak_core_ring:transfer_node(I, Other, R);
                                (_, R) -> R
                             end,
                             Ring,
                             AllOwners),
                riak_core_claim:claim_rebalance_n(TempRing, Other)
        end,
    ExitRing.

attempt_simple_transfer(Seed, Ring, Owners, ExitingNode) ->
    TargetN = app_helper:get_env(riak_core, target_n_val),
    attempt_simple_transfer(Seed, Ring, Owners,
                            TargetN,
                            ExitingNode, 0,
                            [{O,-TargetN} || O <- riak_core_ring:claiming_members(Ring),
                                             O /= ExitingNode]).
attempt_simple_transfer(Seed, Ring, [{P, Exit}|Rest], TargetN, Exit, Idx, Last) ->
    %% handoff
    case [ N || {N, I} <- Last, Idx-I >= TargetN ] of
        [] ->
            target_n_fail;
        Candidates ->
            %% these nodes don't violate target_n in the reverse direction
            StepsToNext = fun(Node) ->
                                  length(lists:takewhile(
                                           fun({_, Owner}) -> Node /= Owner end,
                                           Rest))
                          end,
            case lists:filter(fun(N) ->
                                 Next = StepsToNext(N),
                                 (Next+1 >= TargetN)
                                          orelse (Next == length(Rest))
                              end,
                              Candidates) of
                [] ->
                    target_n_fail;
                Qualifiers ->
                    %% these nodes don't violate target_n forward
                    {Rand, Seed2} = random:uniform_s(length(Qualifiers), Seed),
                    Chosen = lists:nth(Rand, Qualifiers),
                    %% choose one, and do the rest of the ring
                    attempt_simple_transfer(
                      Seed2,
                      riak_core_ring:transfer_node(P, Chosen, Ring),
                      Rest, TargetN, Exit, Idx+1,
                      lists:keyreplace(Chosen, 1, Last, {Chosen, Idx}))
            end
    end;
attempt_simple_transfer(Seed, Ring, [{_, N}|Rest], TargetN, Exit, Idx, Last) ->
    %% just keep track of seeing this node
    attempt_simple_transfer(Seed, Ring, Rest, TargetN, Exit, Idx+1,
                            lists:keyreplace(N, 1, Last, {N, Idx}));
attempt_simple_transfer(_, Ring, [], _, _, _, _) ->
    {ok, Ring}.
