%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_core_broadcast).

-behaviour(gen_server).

%% API
-export([start_link/0,
         start_link/4,
         broadcast/2,
         ring_update/1,
         broadcast_members/0,
         broadcast_members/1,
         exchanges/0,
         exchanges/1,
         cancel_exchanges/1]).

%% Debug API
-export([debug_get_peers/2,
         debug_get_peers/3,
         debug_get_tree/2]).


%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-type nodename()        :: any().
-type message_id()      :: any().
-type message_round()   :: non_neg_integer().
-type outstanding()     :: {message_id(), module(), message_round(), nodename()}.
-type exchange()        :: {module(), node(), reference(), pid()}.
-type exchanges()       :: [exchange()].

-record(state, {
          %% Initially trees rooted at each node are the same.
          %% Portions of that tree belonging to this node are
          %% shared in this set.
          common_eagers :: ordsets:ordset(nodename()),

          %% Initially trees rooted at each node share the same lazy links.
          %% Typically this set will contain a single element. However, it may
          %% contain more in large clusters and may be empty for clusters with
          %% less than three nodes.
          common_lazys  :: ordsets:ordset(nodename()),

          %% A mapping of sender node (root of each broadcast tree)
          %% to this node's portion of the tree. Elements are
          %% added to this structure as messages rooted at a node
          %% propogate to this node. Nodes that are never the
          %% root of a message will never have a key added to
          %% `eager_sets'
          eager_sets    :: [{nodename(), ordsets:ordset(nodename())}],

          %% A Mapping of sender node (root of each spanning tree)
          %% to this node's set of lazy peers. Elements are added
          %% to this structure as messages rooted at a node
          %% propogate to this node. Nodes that are never the root
          %% of a message will never have a key added to `lazy_sets'
          lazy_sets     :: [{nodename(), ordsets:ordset(nodename())}],

          %% Lazy messages that have not been acked. Messages are added to
          %% this set when a node is sent a lazy message (or when it should be
          %% sent one sometime in the future). Messages are removed when the lazy
          %% pushes are acknowleged via graft or ignores. Entries are keyed by their
          %% destination
          outstanding   :: [{nodename(), outstanding()}],

          %% Set of registered modules that may handle messages that
          %% have been broadcast
          mods          :: [module()],

          %% List of outstanding exchanges
          exchanges     :: exchanges(),

          %% Set of all known members. Used to determine
          %% which members have joined and left during a membership update
          all_members   :: ordsets:ordset(nodename())
         }).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Starts the broadcast server on this node. The initial membership list is
%% fetched from the ring. If the node is a singleton then the initial eager and lazy
%% sets are empty. If there are two nodes, each will be in the others eager set and the
%% lazy sets will be empty. When number of members is less than 5, each node will initially
%% have one other node in its eager set and lazy set. If there are more than five nodes
%% each node will have at most two other nodes in its eager set and one in its lazy set, initally.
%% In addition, after the broadcast server is started, a callback is registered with ring_events
%% to generate membership updates as the ring changes.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Members = all_broadcast_members(Ring),
    {InitEagers, InitLazys} = init_peers(Members),
    Mods = app_helper:get_env(riak_core, broadcast_mods, [riak_core_metadata_manager]),
    Res = start_link(Members, InitEagers, InitLazys, Mods),
    riak_core_ring_events:add_sup_callback(fun ?MODULE:ring_update/1),
    Res.

%% @doc Starts the broadcast server on this node. `InitMembers' must be a list
%% of all members known to this node when starting the broadcast server.
%% `InitEagers' are the initial peers of this node for all broadcast trees.
%% `InitLazys' is a list of random peers not in `InitEagers' that will be used
%% as the initial lazy peer shared by all trees for this node. If the number
%% of nodes in the cluster is less than 3, `InitLazys' should be an empty list.
%% `InitEagers' and `InitLazys' must also be subsets of `InitMembers'. `Mods' is
%% a list of modules that may be handlers for broadcasted messages. All modules in
%% `Mods' should implement the `riak_core_broadcast_handler' behaviour.
%%
%% NOTE: When starting the server using start_link/2 no automatic membership update from
%% ring_events is registered. Use start_link/0.
-spec start_link([nodename()], [nodename()], [nodename()], [module()]) ->
                        {ok, pid()} | ignore | {error, term}.
start_link(InitMembers, InitEagers, InitLazys, Mods) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE,
                          [InitMembers, InitEagers, InitLazys, Mods], []).

%% @doc Broadcasts a message originating from this node. The message will be delivered to
%% each node at least once. The `Mod' passed is responsible for handling the message on remote
%% nodes as well as providing some other information both locally and and on other nodes.
%% `Mod' must be loaded on all members of the clusters and implement the
%% `riak_core_broadcast_handler' behaviour.
-spec broadcast(any(), module()) -> ok.
broadcast(Broadcast, Mod) ->
    {MessageId, Payload} = Mod:broadcast_data(Broadcast),
    gen_server:cast(?SERVER, {broadcast, MessageId, Payload, Mod}).


%% @doc Notifies broadcast server of membership update given a new ring
-spec ring_update(riak_core_ring:riak_core_ring()) -> ok.
ring_update(Ring) ->
    gen_server:cast(?SERVER, {ring_update, Ring}).

%% @doc Returns the broadcast servers view of full cluster membership.
%% Wait indefinitely for a response is returned from the process
-spec broadcast_members() -> ordsets:ordset(nodename()).
broadcast_members() ->
    broadcast_members(infinity).

%% @doc Returns the broadcast servers view of full cluster membership.
%% Waits `Timeout' ms for a response from the server
-spec broadcast_members(infinity | pos_integer()) -> ordsets:ordset(nodename()).
broadcast_members(Timeout) ->
    gen_server:call(?SERVER, broadcast_members, Timeout).

%% @doc return a list of exchanges, started by broadcast on thisnode, that are running
-spec exchanges() -> exchanges().
exchanges() ->
    exchanges(node()).

%% @doc returns a list of exchanges, started by broadcast on `Node', that are running
-spec exchanges(node()) -> exchanges().
exchanges(Node) ->
    gen_server:call({?SERVER, Node}, exchanges, infinity).

%% @doc cancel exchanges started by this node.
-spec cancel_exchanges(all              |
                       {peer, node()}   |
                       {mod, module()}  |
                       reference()      |
                       pid()) -> exchanges().
cancel_exchanges(WhichExchanges) ->
    gen_server:call(?SERVER, {cancel_exchanges, WhichExchanges}, infinity).

%%%===================================================================
%%% Debug API
%%%===================================================================

%% @doc return the peers for `Node' for the tree rooted at `Root'.
%% Wait indefinitely for a response is returned from the process
-spec debug_get_peers(node(), node()) -> {ordsets:ordset(node()), ordsets:ordset(node())}.
debug_get_peers(Node, Root) ->
    debug_get_peers(Node, Root, infinity).

%% @doc return the peers for `Node' for the tree rooted at `Root'.
%% Waits `Timeout' ms for a response from the server
-spec debug_get_peers(node(), node(), infinity | pos_integer()) ->
                             {ordsets:ordset(node()), ordsets:ordset(node())}.
debug_get_peers(Node, Root, Timeout) ->
    gen_server:call({?SERVER, Node}, {get_peers, Root}, Timeout).

%% @doc return peers for all `Nodes' for tree rooted at `Root'
%% Wait indefinitely for a response is returned from the process
-spec debug_get_tree(node(), [node()]) ->
                            [{node(), {ordsets:ordset(node()), ordsets:ordset(node())}}].
debug_get_tree(Root, Nodes) ->
    [begin
         Peers = try debug_get_peers(Node, Root)
                 catch _:_ -> down
                 end,
         {Node, Peers}
     end || Node <- Nodes].

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([[nodename()]]) -> {ok, #state{}} |
                                  {ok, #state{}, non_neg_integer() | infinity} |
                                  ignore |
                                  {stop, term()}.
init([AllMembers, InitEagers, InitLazys, Mods]) ->
    schedule_lazy_tick(),
    schedule_exchange_tick(),
    State1 =  #state{
                 outstanding   = orddict:new(),
                 mods = lists:usort(Mods),
                 exchanges=[]
                },
    State2 = reset_peers(AllMembers, InitEagers, InitLazys, State1),
    {ok, State2}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
                         {reply, term(), #state{}} |
                         {reply, term(), #state{}, non_neg_integer()} |
                         {noreply, #state{}} |
                         {noreply, #state{}, non_neg_integer()} |
                         {stop, term(), term(), #state{}} |
                         {stop, term(), #state{}}.
handle_call({get_peers, Root}, _From, State) ->
    EagerPeers = all_peers(Root, State#state.eager_sets, State#state.common_eagers),
    LazyPeers = all_peers(Root, State#state.lazy_sets, State#state.common_lazys),
    {reply, {EagerPeers, LazyPeers}, State};
handle_call(broadcast_members, _From, State=#state{all_members=AllMembers}) ->
    {reply, AllMembers, State};
handle_call(exchanges, _From, State=#state{exchanges=Exchanges}) ->
    {reply, Exchanges, State};
handle_call({cancel_exchanges, WhichExchanges}, _From, State) ->
    Cancelled = cancel_exchanges(WhichExchanges, State#state.exchanges),
    {reply, Cancelled, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}} |
                                       {noreply, #state{}, non_neg_integer()} |
                                       {stop, term(), #state{}}.
handle_cast({broadcast, MessageId, Message, Mod}, State) ->
    State1 = eager_push(MessageId, Message, Mod, State),
    State2 = schedule_lazy_push(MessageId, Mod, State1),
    {noreply, State2};
handle_cast({broadcast, MessageId, Message, Mod, Round, Root, From}, State) ->
    Valid = Mod:merge(MessageId, Message),
    State1 = handle_broadcast(Valid, MessageId, Message, Mod, Round, Root, From, State),
    {noreply, State1};
handle_cast({prune, Root, From}, State) ->
    State1 = add_lazy(From, Root, State),
    {noreply, State1};
handle_cast({i_have, MessageId, Mod, Round, Root, From}, State) ->
    Stale = Mod:is_stale(MessageId),
    State1 = handle_ihave(Stale, MessageId, Mod, Round, Root, From, State),
    {noreply, State1};
handle_cast({ignored_i_have, MessageId, Mod, Round, Root, From}, State) ->
    State1 = ack_outstanding(MessageId, Mod, Round, Root, From, State),
    {noreply, State1};
handle_cast({graft, MessageId, Mod, Round, Root, From}, State) ->
    Result = Mod:graft(MessageId),
    State1 = handle_graft(Result, MessageId, Mod, Round, Root, From, State),
    {noreply, State1};
handle_cast({ring_update, Ring}, State=#state{all_members=BroadcastMembers}) ->
    CurrentMembers = ordsets:from_list(all_broadcast_members(Ring)),
    New = ordsets:subtract(CurrentMembers, BroadcastMembers),
    Removed = ordsets:subtract(BroadcastMembers, CurrentMembers),
    State1 = case ordsets:size(New) > 0 of
                 false -> State;
                 true ->
                     {EagerPeers, LazyPeers} = init_peers(CurrentMembers),
                     reset_peers(CurrentMembers, EagerPeers, LazyPeers, State)
             end,
    State2 = neighbors_down(Removed, State1),
    {noreply, State2}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}} |
                                       {noreply, #state{}, non_neg_integer()} |
                                       {stop, term(), #state{}}.
handle_info(lazy_tick, State) ->
    schedule_lazy_tick(),
    _ = send_lazy(State),
    {noreply, State};
handle_info(exchange_tick, State) ->
    schedule_exchange_tick(),
    State1 = maybe_exchange(State),
    {noreply, State1};
handle_info({'DOWN', Ref, process, _Pid, _Reason}, State=#state{exchanges=Exchanges}) ->
    Exchanges1 = lists:keydelete(Ref, 3, Exchanges),
    {noreply, State#state{exchanges=Exchanges1}}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
handle_broadcast(false, _MessageId, _Message, _Mod, _Round, Root, From, State) -> %% stale msg
    State1 = add_lazy(From, Root, State),
    _ = send({prune, Root, node()}, From),
    State1;
handle_broadcast(true, MessageId, Message, Mod, Round, Root, From, State) -> %% valid msg
    State1 = add_eager(From, Root, State),
    State2 = eager_push(MessageId, Message, Mod, Round+1, Root, From, State1),
    schedule_lazy_push(MessageId, Mod, Round+1, Root, From, State2).

handle_ihave(true, MessageId, Mod, Round, Root, From, State) -> %% stale i_have
    _ = send({ignored_i_have, MessageId, Mod, Round, Root, node()}, From),
    State;
handle_ihave(false, MessageId, Mod, Round, Root, From, State) -> %% valid i_have
    %% TODO: don't graft immediately
    _ = send({graft, MessageId, Mod, Round, Root, node()}, From),
    add_eager(From, Root, State).

handle_graft(stale, MessageId, Mod, Round, Root, From, State) ->
    %% There has been a subsequent broadcast that is causally newer than this message
    %% according to Mod. We ack the outstanding message since the outstanding entry
    %% for the newer message exists
    ack_outstanding(MessageId, Mod, Round, Root, From, State);
handle_graft({ok, Message}, MessageId, Mod, Round, Root, From, State) ->
    %% we don't ack outstanding here because the broadcast may fail to be delivered
    %% instead we will allow the i_have to be sent once more and let the subsequent
    %% ignore serve as the ack.
    State1 = add_eager(From, Root, State),
    _ = send({broadcast, MessageId, Message, Mod, Round, Root, node()}, From),
    State1;
handle_graft({error, Reason}, _MessageId, Mod, _Round, _Root, _From, State) ->
    lager:error("unable to graft message from ~p. reason: ~p", [Mod, Reason]),
    State.

neighbors_down(Removed, State=#state{common_eagers=CommonEagers,eager_sets=EagerSets,
                                     common_lazys=CommonLazys,lazy_sets=LazySets,
                                     outstanding=Outstanding}) ->
    NewCommonEagers = ordsets:subtract(CommonEagers, Removed),
    NewCommonLazys  = ordsets:subtract(CommonLazys, Removed),
    %% TODO: once we have delayed grafting need to remove timers
    NewEagerSets = ordsets:from_list([{Root, ordsets:subtract(Existing, Removed)} ||
                                         {Root, Existing} <- ordsets:to_list(EagerSets)]),
    NewLazySets  = ordsets:from_list([{Root, ordsets:subtract(Existing, Removed)} ||
                                         {Root, Existing} <- ordsets:to_list(LazySets)]),
    %% delete outstanding messages to removed peers
    NewOutstanding = ordsets:fold(fun(RPeer, OutstandingAcc) ->
                                          orddict:erase(RPeer, OutstandingAcc)
                                  end,
                                  Outstanding, Removed),
    State#state{common_eagers=NewCommonEagers,
                common_lazys=NewCommonLazys,
                eager_sets=NewEagerSets,
                lazy_sets=NewLazySets,
                outstanding=NewOutstanding}.

eager_push(MessageId, Message, Mod, State) ->
    eager_push(MessageId, Message, Mod, 0, node(), node(), State).

eager_push(MessageId, Message, Mod, Round, Root, From, State) ->
    Peers = eager_peers(Root, From, State),
    _ = send({broadcast, MessageId, Message, Mod, Round, Root, node()}, Peers),
    State.

schedule_lazy_push(MessageId, Mod, State) ->
    schedule_lazy_push(MessageId, Mod, 0, node(), node(), State).

schedule_lazy_push(MessageId, Mod, Round, Root, From, State) ->
    Peers = lazy_peers(Root, From, State),
    add_all_outstanding(MessageId, Mod, Round, Root, Peers, State).

send_lazy(#state{outstanding=Outstanding}) ->
    [send_lazy(Peer, Messages) || {Peer, Messages} <- orddict:to_list(Outstanding)].

send_lazy(Peer, Messages) ->
    [send_lazy(MessageId, Mod, Round, Root, Peer) ||
        {MessageId, Mod, Round, Root} <- ordsets:to_list(Messages)].

send_lazy(MessageId, Mod, Round, Root, Peer) ->
    send({i_have, MessageId, Mod, Round, Root, node()}, Peer).

maybe_exchange(State) ->
    Root = random_root(State),
    Peer = random_peer(Root, State),
    maybe_exchange(Peer, State).

maybe_exchange(undefined, State) ->
    State;
maybe_exchange(Peer, State=#state{mods=[Mod | _],exchanges=Exchanges}) ->
    %% limit the number of exchanges this node can start concurrently.
    %% the exchange must (currently?) implement any "inbound" concurrency limits
    ExchangeLimit = app_helper:get_env(riak_core, broadcast_start_exchange_limit, 1),
    BelowLimit = not (length(Exchanges) >= ExchangeLimit),
    FreeMod = lists:keyfind(Mod, 1, Exchanges) =:= false,
    case BelowLimit and FreeMod of
        true -> exchange(Peer, State);
        false -> State
    end.

exchange(Peer, State=#state{mods=[Mod | Mods],exchanges=Exchanges}) ->
    State1 = case Mod:exchange(Peer) of
                 {ok, Pid} ->
                     lager:debug("started ~p exchange with ~p (~p)", [Mod, Peer, Pid]),
                     Ref = monitor(process, Pid),
                     State#state{exchanges=[{Mod, Peer, Ref, Pid} | Exchanges]};
                 {error, _Reason} ->
                     State
             end,
    State1#state{mods=Mods ++ [Mod]}.

cancel_exchanges(all, Exchanges) ->
    kill_exchanges(Exchanges);
cancel_exchanges(WhichProc, Exchanges) when is_reference(WhichProc) orelse
                                            is_pid(WhichProc) ->
    KeyPos = case is_reference(WhichProc) of
              true -> 3;
              false -> 4
          end,
    case lists:keyfind(WhichProc, KeyPos, Exchanges) of
        false -> [];
        Exchange ->
            kill_exchange(Exchange),
            [Exchange]
    end;
cancel_exchanges(Which, Exchanges) ->
    Filter = exchange_filter(Which),
    ToCancel = [Ex || Ex <- Exchanges, Filter(Ex)],
    kill_exchanges(ToCancel).

kill_exchanges(Exchanges) ->
    _ = [kill_exchange(Exchange) || Exchange <- Exchanges],
    Exchanges.

kill_exchange({_, _, _, ExchangePid}) ->
    exit(ExchangePid, cancel_exchange).


exchange_filter({peer, Peer}) ->
    fun({_, ExchangePeer, _, _}) ->
            Peer =:= ExchangePeer
    end;
exchange_filter({mod, Mod}) ->
    fun({ExchangeMod, _, _, _}) ->
            Mod =:= ExchangeMod
    end.


%% picks random root uniformly
random_root(#state{all_members=Members}) ->
    random_other_node(Members).

%% picks random peer favoring peers not in eager or lazy set and ensuring
%% peer is not this node
random_peer(Root, State=#state{all_members=All}) ->
    Eagers = all_eager_peers(Root, State),
    Lazys  = all_lazy_peers(Root, State),
    Union  = ordsets:union([Eagers, Lazys]),
    Other  = ordsets:del_element(node(), ordsets:subtract(All, Union)),
    case ordsets:size(Other) of
        0 ->
            random_other_node(ordsets:del_element(node(), All));
        _ ->
            random_other_node(Other)
    end.

%% picks random node from ordset
random_other_node(OrdSet) ->
    Size = ordsets:size(OrdSet),
    case Size of
        0 -> undefined;
        _ ->
            lists:nth(rand:uniform(Size),
                     ordsets:to_list(OrdSet))
    end.

ack_outstanding(MessageId, Mod, Round, Root, From, State=#state{outstanding=All}) ->
    Existing = existing_outstanding(From, All),
    Updated = set_outstanding(From,
                              ordsets:del_element({MessageId, Mod, Round, Root}, Existing),
                              All),
    State#state{outstanding=Updated}.

add_all_outstanding(MessageId, Mod, Round, Root, Peers, State) ->
    lists:foldl(fun(Peer, SAcc) -> add_outstanding(MessageId, Mod, Round, Root, Peer, SAcc) end,
                State,
                ordsets:to_list(Peers)).

add_outstanding(MessageId, Mod, Round, Root, Peer, State=#state{outstanding=All}) ->
    Existing = existing_outstanding(Peer, All),
    Updated = set_outstanding(Peer,
                              ordsets:add_element({MessageId, Mod, Round, Root}, Existing),
                              All),
    State#state{outstanding=Updated}.

set_outstanding(Peer, Outstanding, All) ->
    case ordsets:size(Outstanding) of
        0 -> orddict:erase(Peer, All);
        _ -> orddict:store(Peer, Outstanding, All)
    end.

existing_outstanding(Peer, All) ->
    case orddict:find(Peer, All) of
        error -> ordsets:new();
        {ok, Outstanding} -> Outstanding
    end.

add_eager(From, Root, State) ->
    update_peers(From, Root, fun ordsets:add_element/2, fun ordsets:del_element/2, State).

add_lazy(From, Root, State) ->
    update_peers(From, Root, fun ordsets:del_element/2, fun ordsets:add_element/2, State).

update_peers(From, Root, EagerUpdate, LazyUpdate, State) ->
    CurrentEagers = all_eager_peers(Root, State),
    CurrentLazys = all_lazy_peers(Root, State),
    NewEagers = EagerUpdate(From, CurrentEagers),
    NewLazys  = LazyUpdate(From, CurrentLazys),
    set_peers(Root, NewEagers, NewLazys, State).

set_peers(Root, Eagers, Lazys, State=#state{eager_sets=EagerSets,lazy_sets=LazySets}) ->
    NewEagers = orddict:store(Root, Eagers, EagerSets),
    NewLazys = orddict:store(Root, Lazys, LazySets),
    State#state{eager_sets=NewEagers, lazy_sets=NewLazys}.

all_eager_peers(Root, State) ->
    all_peers(Root, State#state.eager_sets, State#state.common_eagers).

all_lazy_peers(Root, State) ->
    all_peers(Root, State#state.lazy_sets, State#state.common_lazys).

eager_peers(Root, From, #state{eager_sets=EagerSets, common_eagers=CommonEagers}) ->
    all_filtered_peers(Root, From, EagerSets, CommonEagers).

lazy_peers(Root, From, #state{lazy_sets=LazySets, common_lazys=CommonLazys}) ->
    all_filtered_peers(Root, From, LazySets, CommonLazys).

all_filtered_peers(Root, From, Sets, Common) ->
    All = all_peers(Root, Sets, Common),
    ordsets:del_element(From, All).

all_peers(Root, Sets, Default) ->
    case orddict:find(Root, Sets) of
        error -> Default;
        {ok, Peers} -> Peers
    end.

send(Msg, Peers) when is_list(Peers) ->
    [send(Msg, P) || P <- Peers];
send(Msg, P) ->
    %% TODO: add debug logging
    gen_server:cast({?SERVER, P}, Msg).

schedule_lazy_tick() ->
    schedule_tick(lazy_tick, broadcast_lazy_timer, 1000).

schedule_exchange_tick() ->
    schedule_tick(exchange_tick, broadcast_exchange_timer, 10000).

schedule_tick(Message, Timer, Default) ->
    TickMs = app_helper:get_env(riak_core, Timer, Default),
    erlang:send_after(TickMs, ?MODULE, Message).

reset_peers(AllMembers, EagerPeers, LazyPeers, State) ->
    State#state{
      common_eagers = ordsets:del_element(node(), ordsets:from_list(EagerPeers)),
      common_lazys  = ordsets:del_element(node(), ordsets:from_list(LazyPeers)),
      eager_sets    = orddict:new(),
      lazy_sets     = orddict:new(),
      all_members   = ordsets:from_list(AllMembers)
     }.

all_broadcast_members(Ring) ->
    riak_core_ring:all_members(Ring).

init_peers(Members) ->
    case length(Members) of
        1 ->
            %% Single member, must be ourselves
            InitEagers = [],
            InitLazys  = [];
        2 ->
            %% Two members, just eager push to the other
            InitEagers = Members -- [node()],
            InitLazys  = [];
        N when N < 5 ->
            %% 2 to 4 members, start with a fully connected tree
            %% with cycles. it will be adjusted as needed
            Tree = riak_core_util:build_tree(1, Members, [cycles]),
            InitEagers = orddict:fetch(node(), Tree),
            InitLazys  = [lists:nth(rand:uniform(N - 2), Members -- [node() | InitEagers])];
        N when N < 10 ->
            %% 5 to 9 members, start with gossip tree used by
            %% riak_core_gossip. it will be adjusted as needed
            Tree = riak_core_util:build_tree(2, Members, [cycles]),
            InitEagers = orddict:fetch(node(), Tree),
            InitLazys  = [lists:nth(rand:uniform(N - 3), Members -- [node() | InitEagers])];
        N ->
            %% 10 or more members, use a tree similar to riak_core_gossip
            %% but with higher fanout (larger initial eager set size)
            NEagers = round(math:log(N) + 1),
            Tree = riak_core_util:build_tree(NEagers, Members, [cycles]),
            InitEagers = orddict:fetch(node(), Tree),
            InitLazys  = [lists:nth(rand:uniform(N - (NEagers + 1)), Members -- [node() | InitEagers])]
    end,
    {InitEagers, InitLazys}.
