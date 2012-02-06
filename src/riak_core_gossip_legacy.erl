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

-module(riak_core_gossip_legacy).

-behaviour(gen_server).

-export([start_link/0, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export ([distribute_ring/1, send_ring/1, send_ring/2, remove_from_cluster/1]).

-include("riak_core_ring.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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


%% ===================================================================
%% gen_server behaviour
%% ===================================================================

%% @private
init(_State) ->
    schedule_next_gossip(),
    {ok, true}.


%% @private
handle_call(_, _From, State) ->
    {reply, ok, State}.


%% @private
handle_cast({send_ring_to, Node}, RingChanged) ->
    {ok, MyRing} = riak_core_ring_manager:get_raw_ring(),
    gen_server:cast({?MODULE, Node}, {reconcile_ring, MyRing}),
    {noreply, RingChanged};

handle_cast({distribute_ring, Ring}, RingChanged) ->
    Nodes = riak_core_ring:all_members(Ring),
    gen_server:abcast(Nodes, ?MODULE, {reconcile_ring, Ring}),
    {noreply, RingChanged};

handle_cast({reconcile_ring, OtherRing}, RingChanged) ->
    % Compare the two rings, see if there is anything that
    % must be done to make them equal...
    {ok, MyRing0} = riak_core_ring_manager:get_raw_ring(),
    MyRing = riak_core_ring:downgrade(?LEGACY_RING_VSN, MyRing0),
    case riak_core_ring:legacy_reconcile(OtherRing, MyRing) of
        {no_change, _} ->
            {noreply, RingChanged};

        {new_ring, ReconciledRing} ->
            % Rebalance the new ring and save it
            BalancedRing = claim_until_balanced(ReconciledRing),
            riak_core_ring_manager:set_my_ring(BalancedRing),

            BalancedRing2 = riak_core_ring:upgrade(BalancedRing),
            riak_core_gossip:random_gossip(BalancedRing2),
            {noreply, true}
    end;

handle_cast(gossip_ring, _RingChanged) ->
    % First, schedule the next round of gossip...
    schedule_next_gossip(),

    % Gossip the ring to some random other node...
    {ok, MyRing} = riak_core_ring_manager:get_raw_ring(),
    case riak_core_ring:random_other_node(MyRing) of
        no_node -> % must be single node cluster
            ok;
        RandomNode ->
            send_ring(node(), RandomNode)
    end,
    {noreply, false};

handle_cast(_, State) ->
    {noreply, State}.

%% @private
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

schedule_next_gossip() ->
    MaxInterval = app_helper:get_env(riak_core, gossip_interval),
    Interval = random:uniform(MaxInterval),
    timer:apply_after(Interval, gen_server, cast, [?MODULE, gossip_ring]).

claim_until_balanced(Ring) ->
    {WMod, WFun} = app_helper:get_env(riak_core, wants_claim_fun),
    NeedsIndexes = apply(WMod, WFun, [Ring]),
    case NeedsIndexes of
        no ->
            Ring;
        {yes, _NumToClaim} ->
            {CMod, CFun} = app_helper:get_env(riak_core, choose_claim_fun),
            NewRing0 = CMod:CFun(Ring),
            NewRing = riak_core_ring:downgrade(?LEGACY_RING_VSN, NewRing0),
            claim_until_balanced(NewRing)
    end.


remove_from_cluster(ExitingNode) ->
    % Set the remote node to stop claiming.
    % Ignore return of rpc as this should succeed even if node is offline
    rpc:call(ExitingNode, application, set_env,
             [riak_core, wants_claim_fun, {riak_core_claim, never_wants_claim}]),

    % Get a list of indices owned by the ExitingNode...
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    AllOwners = riak_core_ring:all_owners(Ring),

    % Transfer indexes to other nodes...
    ExitRing0 =
        case attempt_simple_transfer(Ring, AllOwners, ExitingNode) of
            {ok, NR} ->
                NR;
            target_n_fail ->
                %% re-diagonalize
                %% first hand off all claims to *any* one else,
                %% just so rebalance doesn't include exiting node
                Members = riak_core_ring:all_members(Ring),
                Other = hd(lists:delete(ExitingNode, Members)),
                TempRing = lists:foldl(
                             fun({I,N}, R) when N == ExitingNode ->
                                     riak_core_ring:transfer_node(I, Other, R);
                                (_, R) -> R
                             end,
                             Ring,
                             AllOwners),
                TempRing2 = riak_core_ring:downgrade(?LEGACY_RING_VSN,
                                                     TempRing),
                TempRing3 = riak_core_claim:claim_rebalance_n(TempRing2, Other),
                riak_core_ring:upgrade(TempRing3)
        end,

    ExitRing = riak_core_ring:downgrade(?LEGACY_RING_VSN, ExitRing0),

    % Send the new ring to all nodes except the exiting node
    riak_core_gossip:distribute_ring(ExitRing0),

    % Set the new ring on the exiting node. This will trigger
    % it to begin handoff and cleanly leave the cluster.
    rpc:call(ExitingNode, riak_core_ring_manager, set_my_ring, [ExitRing]),

    ok.


attempt_simple_transfer(Ring, Owners, ExitingNode) ->
    TargetN = app_helper:get_env(riak_core, target_n_val),
    attempt_simple_transfer(Ring, Owners,
                            TargetN,
                            ExitingNode, 0,
                            [{O,-TargetN} || O <- riak_core_ring:all_members(Ring),
                                             O /= ExitingNode]).
attempt_simple_transfer(Ring, [{P, Exit}|Rest], TargetN, Exit, Idx, Last) ->
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
                    Chosen = lists:nth(random:uniform(length(Qualifiers)),
                                       Qualifiers),
                    %% choose one, and do the rest of the ring
                    attempt_simple_transfer(
                      riak_core_ring:transfer_node(P, Chosen, Ring),
                      Rest, TargetN, Exit, Idx+1,
                      lists:keyreplace(Chosen, 1, Last, {Chosen, Idx}))
            end
    end;
attempt_simple_transfer(Ring, [{_, N}|Rest], TargetN, Exit, Idx, Last) ->
    %% just keep track of seeing this node
    attempt_simple_transfer(Ring, Rest, TargetN, Exit, Idx+1,
                            lists:keyreplace(N, 1, Last, {N, Idx}));
attempt_simple_transfer(Ring, [], _, _, _, _) ->
    {ok, Ring}.
