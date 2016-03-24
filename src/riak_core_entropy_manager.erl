%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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
%% This is a direct copy of:
%% https://github.com/basho/riak_kv/blob/develop/src/riak_kv_entropy_manager.erl
%% -------------------------------------------------------------------

-module(riak_core_entropy_manager).
-behaviour(gen_server).

%% API
-export([start_link/2,
         manual_exchange/2,
         enabled/0,
         enable/1,
         disable/1,
         set_mode/2,
         set_debug/1,
         cancel_exchange/2,
         cancel_exchanges/1,
         get_lock/2,
         get_lock/3,
         requeue_poke/2,
         start_exchange_remote/4,
         exchange_status/5,
         supervisor_spec/2]).
-export([all_pairwise_exchanges/2]).
-export([get_aae_throttle/0,
         set_aae_throttle/1,
         get_aae_throttle_kill/0,
         set_aae_throttle_kill/1,
         get_aae_throttle_limits/0,
         set_aae_throttle_limits/1,
         get_max_local_vnodeq/0]).
-export([multicall/5]).                         % for meck twiddle-testing

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-export([query_and_set_aae_throttle/1]).        % for eunit twiddle-testing
-export([make_state/0, get_last_throttle/1]).   % for eunit twiddle-testing
-include_lib("eunit/include/eunit.hrl").
-endif.

-type index() :: non_neg_integer().
-type index_n() :: {index(), pos_integer()}.
-type vnode() :: {index(), node()}.
-type exchange() :: {index(), index(), index_n()}.
-type riak_core_ring() :: riak_core_ring:riak_core_ring().

-record(state, {mode           = automatic :: automatic | manual,
                trees          = []        :: [{index(), pid()}],
                tree_queue     = []        :: [{index(), pid()}],
                locks          = []        :: [{pid(), reference()}],
                build_tokens   = 0         :: non_neg_integer(),
                exchange_queue = []        :: [exchange()],
                exchanges      = []        :: [{index(), reference(), pid()}],
                service        = undefined :: atom(),
                vnode          = undefined :: atom(),
                vnode_status_pid = undefined :: 'undefined' | pid(),
                last_throttle  = undefined :: 'undefined' | non_neg_integer()
               }).

-type state() :: #state{}.

-define(DEFAULT_CONCURRENCY, 2).
-define(DEFAULT_BUILD_LIMIT, {1, 3600000}). %% Once per hour
-define(AAE_THROTTLE_ENV_KEY, aae_throttle_sleep_time).
-define(AAE_THROTTLE_KILL_ENV_KEY, aae_throttle_kill_switch).


-spec supervisor_spec(Service::atom(), VNode::atom()) ->
                             {atom(),
                              {riak_core_entropy_manager, start_link,
                               [atom()]},
                              permanent, 30000, worker, [riak_core_entropy_manager]}.

supervisor_spec(Service, VNode) ->
    {gen_name(Service),
     {riak_core_entropy_manager, start_link,
      [Service, VNode]},
     permanent, 30000, worker, [riak_core_entropy_manager]}.

gen_name(Service) ->
    list_to_atom(atom_to_list(Service) ++ "_entropy_manager").
%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(atom(), atom()) -> {ok, pid()} | {error, term()}.
start_link(Service, VNode) ->
    Name = gen_name(Service),
    gen_server:start_link({local, Name}, ?MODULE, [Service, VNode], []).

%% @doc Acquire an exchange concurrency lock if available, and associate
%%      the lock with the calling process.
-spec get_lock(atom(), any()) -> ok | max_concurrency.
get_lock(Service, Type) ->
    get_lock(Service, Type, self()).

%% @doc Acquire an exchange concurrency lock if available, and associate
%%      the lock with the provided pid.
-spec get_lock(atom(), any(), pid()) -> ok | max_concurrency.
get_lock(Service, Type, Pid) ->
    Name = gen_name(Service),
    gen_server:call(Name, {get_lock, Type, Pid}, infinity).

%% @doc Acquire the necessary locks for an entropy exchange with the specified
%%      remote vnode. The request is sent to the remote entropy manager which
%%      will try to acquire a concurrency lock. If successsful, the request is
%%      then forwarded to the relevant index_hashtree to acquire a tree lock.
%%      If both locks are acquired, the pid of the remote index_hashtree is
%%      returned.
-spec start_exchange_remote(atom(), {index(), node()}, index_n(), pid())
                           -> {remote_exchange, pid()} |
                              {remote_exchange, anti_entropy_disabled} |
                              {remote_exchange, max_concurrency} |
                              {remote_exchange, not_built} |
                              {remote_exchange, already_locked}.
start_exchange_remote(Service, _VNode={Index, Node}, IndexN, FsmPid) ->
    Name = gen_name(Service),
    gen_server:call({Name, Node},
                    {start_exchange_remote, FsmPid, Index, IndexN},
                    infinity).

%% @doc Used by {@link riak_core_index_hashtree} to requeue a poke on
%%      build failure.
-spec requeue_poke(atom(), index()) -> ok.
requeue_poke(Service, Index) ->
    Name = gen_name(Service),
    gen_server:cast(Name, {requeue_poke, Index}).

%% @doc Used by {@link riak_core_exchange_fsm} to inform the entropy
%%      manager about the status of an exchange (ie. completed without
%%      issue, failed, etc)
-spec exchange_status(atom(), vnode(), vnode(), index_n(), any()) -> ok.
exchange_status(Service, LocalVN, RemoteVN, IndexN, Reply) ->
    Name = gen_name(Service),
    gen_server:cast(Name,
                    {exchange_status,
                     self(), LocalVN, RemoteVN, IndexN, Reply}).

%% @doc Returns true of AAE is enabled, false otherwise.
-spec enabled() -> boolean().
enabled() ->
    {Enabled, _} = settings(),
    Enabled.

%% @doc Set AAE to either `automatic' or `manual' mode. In automatic mode, the
%%      entropy manager triggers all necessary hashtree exchanges. In manual
%%      mode, exchanges must be triggered using {@link manual_exchange/1}.
%%      Regardless of exchange mode, the entropy manager will always ensure
%%      local hashtrees are built and rebuilt as necessary.
-spec set_mode(atom(), automatic | manual) -> ok.
set_mode(Service, Mode=automatic) ->
    Name = gen_name(Service),
    ok = gen_server:call(Name, {set_mode, Mode}, infinity);
set_mode(Service, Mode=manual) ->
    Name = gen_name(Service),
    ok = gen_server:call(Name, {set_mode, Mode}, infinity).

%% @doc Toggle debug mode, which prints verbose AAE information to the console.
-spec set_debug(boolean()) -> ok.
set_debug(Enabled) ->
    Modules = [riak_core_index_hashtree,
               riak_core_entropy_manager,
               riak_core_exchange_fsm],
    case Enabled of
        true ->
            [lager:trace_console([{module, Mod}]) || Mod <- Modules];
        false ->
            [begin
                 {ok, Trace} = lager:trace_console([{module, Mod}]),
                 lager:stop_trace(Trace)
             end || Mod <- Modules]
    end,
    ok.

enable(Service) ->
    gen_server:call(gen_name(Service), enable, infinity).
disable(Service) ->
    gen_server:call(gen_name(Service), disable, infinity).


%% @doc Manually trigger hashtree exchanges.
%%      -- If an index is provided, trigger exchanges between the index and all
%%         sibling indices for all index_n.
%%      -- If both an index and index_n are provided, trigger exchanges between
%%         the index and all sibling indices associated with the specified
%%         index_n.
%%      -- If an index, remote index, and index_n are provided, trigger an
%%         exchange between the index and remote index for the specified
%%         index_n.
-spec manual_exchange(atom(),
                      index() |                      {index(), index_n()} |
                      {index(), index(), index_n()}) -> ok.
manual_exchange(Service, Exchange) ->
    gen_server:call(gen_name(Service), {manual_exchange, Exchange}, infinity).

-spec cancel_exchange(atom(), index()) -> ok | undefined.
cancel_exchange(Service, Index) ->
    gen_server:call(gen_name(Service), {cancel_exchange, Index}, infinity).

-spec cancel_exchanges(atom()) -> [index()].
cancel_exchanges(Service) ->
    gen_server:call(gen_name(Service), cancel_exchanges, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init([atom()]) -> {'ok',state()}.
init([Service, VNode]) ->
    %% Side-effects section
    set_aae_throttle(0),
    %% riak_core.app has some sane limits already set, or config via Cuttlefish
    %% If the value is not sane, the pattern match below will fail.
    Limits = case app_helper:get_env(riak_core, aae_throttle_limits, []) of
                 [] ->
                     [{-1,0}, {200,10}, {500,50}, {750,250}, {900,1000}, {1100,5000}];
                 OtherLs ->
                     OtherLs
             end,
    case set_aae_throttle_limits(Limits) of
        ok ->
            ok;
        {error, DiagProps} ->
            _ = [lager:error("aae_throttle_limits/anti_entropy.throttle.limits "
                         "list fails this test: ~p\n", [Check]) ||
                {Check, false} <- DiagProps],
            error(invalid_aae_throttle_limits)
    end,
    schedule_tick(gen_name(Service)),

    {_, Opts} = settings(),
    Mode = case proplists:is_defined(manual, Opts) of
               true ->
                   manual;
               false ->
                   automatic
           end,
    set_debug(proplists:is_defined(debug, Opts)),
    State = #state{mode=Mode,
                   trees=[],
                   tree_queue=[],
                   locks=[],
                   exchanges=[],
                   exchange_queue=[],
                   service = Service,
                   vnode = VNode},
    State2 = reset_build_tokens(State),
    schedule_reset_build_tokens(),
    {ok, State2}.

handle_call({set_mode, Mode}, _From, State=#state{mode=CurrentMode}) ->
    State2 = case {CurrentMode, Mode} of
                 {automatic, manual} ->
                     %% Clear exchange queue when switching to manual mode
                     State#state{exchange_queue=[]};
                 _ ->
                     State
             end,
    {reply, ok, State2#state{mode=Mode}};
handle_call({manual_exchange, Exchange}, _From, State) ->
    State2 = enqueue_exchange(Exchange, State),
    {reply, ok, State2};
handle_call(enable, _From, State) ->
    {_, Opts} = settings(),
    application:set_env(riak_core, anti_entropy, {on, Opts}),
    {reply, ok, State};
handle_call(disable, _From, State) ->
    {_, Opts} = settings(),
    application:set_env(riak_core, anti_entropy, {off, Opts}),
    _ = [riak_core_index_hashtree:stop(T) || {_,T} <- State#state.trees],
    {reply, ok, State};
handle_call({get_lock, Type, Pid}, _From, State) ->
    {Reply, State2} = do_get_lock(Type, Pid, State),
    {reply, Reply, State2};
handle_call({start_exchange_remote, FsmPid, Index, IndexN}, From, State) ->
    case {enabled(),
          orddict:find(Index, State#state.trees)} of
        {false, _} ->
            {reply, {remote_exchange, anti_entropy_disabled}, State};
        {_, error} ->
            {reply, {remote_exchange, not_built}, State};
        {_, {ok, Tree}} ->
            case do_get_lock(exchange_remote, FsmPid, State) of
                {ok, State2} ->
                    %% Concurrency lock acquired, now forward to index_hashtree
                    %% to acquire tree lock.
                    riak_core_index_hashtree:start_exchange_remote(FsmPid, From, IndexN, Tree),
                    {noreply, State2};
                {Reply, State2} ->
                    {reply, {remote_exchange, Reply}, State2}
            end
    end;
handle_call({cancel_exchange, Index}, _From, State) ->
    case lists:keyfind(Index, 1, State#state.exchanges) of
        false ->
            {reply, undefined, State};
        {Index, _Ref, Pid} ->
            exit(Pid, kill),
            {reply, ok, State}
    end;
handle_call(cancel_exchanges, _From, State=#state{exchanges=Exchanges}) ->
    Indices = [begin
                   exit(Pid, kill),
                   Index
               end || {Index, _Ref, Pid} <- Exchanges],
    {reply, Indices, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({requeue_poke, Index}, State) ->
    State2 = requeue_poke_int(Index, State),
    {noreply, State2};
handle_cast({exchange_status, Pid, LocalVN, RemoteVN, IndexN, Reply}, State) ->
    State2 = do_exchange_status(Pid, LocalVN, RemoteVN, IndexN, Reply, State),
    {noreply, State2};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    State1 = maybe_tick(State),
    {noreply, State1};
handle_info(reset_build_tokens, State) ->
    State2 = reset_build_tokens(State),
    schedule_reset_build_tokens(),
    {noreply, State2};
handle_info({{hashtree_pid, Index}, Reply}, State) ->
    case Reply of
        {ok, Pid} when is_pid(Pid) ->
            State2 = add_hashtree_pid(Index, Pid, State),
            {noreply, State2};
        _ ->
            {noreply, State}
    end;
handle_info({'DOWN', _, _, Pid, Status}, #state{vnode_status_pid=Pid}=State) ->
    case Status of
        {result, _} = RES ->
            State2 = query_and_set_aae_throttle3(RES, State#state{vnode_status_pid=undefined}),
            {noreply, State2};
        Else ->
            lager:error("query_and_set_aae_throttle error: ~p",[Else]),
            {noreply, State}
    end;
handle_info({'DOWN', Ref, _, Obj, Status}, State) ->
    State2 = maybe_release_lock(Ref, State),
    State3 = maybe_clear_exchange(Ref, Status, State2),
    State4 = maybe_clear_registered_tree(Obj, State3),
    {noreply, State4};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

schedule_reset_build_tokens() ->
    {_, Reset} = app_helper:get_env(riak_core, anti_entropy_build_limit,
                                    ?DEFAULT_BUILD_LIMIT),
    erlang:send_after(Reset, self(), reset_build_tokens).

reset_build_tokens(State) ->
    {Tokens, _} = app_helper:get_env(riak_core, anti_entropy_build_limit,
                                     ?DEFAULT_BUILD_LIMIT),
    State#state{build_tokens=Tokens}.

-spec settings() -> {boolean(), proplists:proplist()}.
settings() ->
    case app_helper:get_env(riak_core, anti_entropy, {off, []}) of
        {on, Opts} ->
            {true, Opts};
        {off, Opts} ->
            {false, Opts};
        X ->
            lager:warning("Invalid setting for riak_core/anti_entropy: ~p", [X]),
            application:set_env(riak_core, anti_entropy, {off, []}),
            {false, []}
    end.

-spec maybe_reload_hashtrees(riak_core_ring(), state()) -> state().
maybe_reload_hashtrees(Ring, State) ->
    case lists:member(State#state.service,
                      riak_core_node_watcher:services(node())) of
        true ->
            reload_hashtrees(Ring, State);
        false ->
            State
    end.

%% Determine the index_hashtree pid for each running primary vnode. This
%% function is called each tick to ensure that any newly spawned vnodes are
%% queried.
-spec reload_hashtrees(riak_core_ring(), state()) -> state().
reload_hashtrees(Ring, State=#state{trees=Trees}) ->
    Indices = riak_core_ring:my_indices(Ring),
    Existing = dict:from_list(Trees),
    MissingIdx = [Idx || Idx <- Indices,
                         not dict:is_key(Idx, Existing)],
    VNode = State#state.vnode,
    Master = VNode:master(),
    _ = [riak_core_aae_vnode:request_hashtree_pid(Master, Idx) 
         || Idx <- MissingIdx],
    State.

add_hashtree_pid(Index, Pid, State) ->
    add_hashtree_pid(enabled(), Index, Pid, State).

add_hashtree_pid(false, _Index, Pid, State) ->
    riak_core_index_hashtree:stop(Pid),
    State;
add_hashtree_pid(true, Index, Pid, State=#state{trees=Trees}) ->
    case orddict:find(Index, Trees) of
        {ok, Pid} ->
            %% Already know about this hashtree
            State;
        _ ->
            monitor(process, Pid),
            Trees2 = orddict:store(Index, Pid, Trees),
            State2 = State#state{trees=Trees2},
            State3 = add_index_exchanges(Index, State2),
            State3
    end.

-spec do_get_lock(any(),pid(),state())
                 -> {ok | max_concurrency | build_limit_reached, state()}.
do_get_lock(Type, Pid, State=#state{locks=Locks}) ->
    Concurrency = app_helper:get_env(riak_core,
                                     anti_entropy_concurrency,
                                     ?DEFAULT_CONCURRENCY),
    case length(Locks) >= Concurrency of
        true ->
            {max_concurrency, State};
        false ->
            case check_lock_type(Type, State) of
                {ok, State2} ->
                    Ref = monitor(process, Pid),
                    State3 = State2#state{locks=[{Pid,Ref}|Locks]},
                    {ok, State3};
                Error ->
                    {Error, State}
            end
    end.

check_lock_type(build, State=#state{build_tokens=Tokens}) ->
    if Tokens > 0 ->
            {ok, State#state{build_tokens=Tokens-1}};
       true ->
            build_limit_reached
    end;
check_lock_type(_Type, State) ->
    {ok, State}.

-spec maybe_release_lock(reference(), state()) -> state().
maybe_release_lock(Ref, State) ->
    Locks = lists:keydelete(Ref, 2, State#state.locks),
    State#state{locks=Locks}.

-spec maybe_clear_exchange(reference(), term(), state()) -> state().
maybe_clear_exchange(Ref, Status, State) ->
    case lists:keytake(Ref, 2, State#state.exchanges) of
        false ->
            State;
        {value, {Idx,Ref,_Pid}, Exchanges} ->
            lager:debug("Untracking exchange: ~p :: ~p", [Idx, Status]),
            State#state{exchanges=Exchanges}
    end.

-spec maybe_clear_registered_tree(pid(), state()) -> state().
maybe_clear_registered_tree(Pid, State) when is_pid(Pid) ->
    Trees = lists:keydelete(Pid, 2, State#state.trees),
    State#state{trees=Trees};
maybe_clear_registered_tree(_, State) ->
    State.

-spec next_tree(state()) -> {pid(), state()}.
next_tree(#state{trees=[]}) ->
    throw(no_trees_registered);
next_tree(State=#state{tree_queue=[], trees=Trees}) ->
    State2 = State#state{tree_queue=Trees},
    next_tree(State2);
next_tree(State=#state{tree_queue=Queue}) ->
    [{_Index,Pid}|Rest] = Queue,
    State2 = State#state{tree_queue=Rest},
    {Pid, State2}.

-spec schedule_tick(Service::atom()) -> ok.
schedule_tick(Service) ->
    %% Perform tick every 15 seconds
    DefaultTick = 15000,
    Tick = app_helper:get_env(riak_core,
                              anti_entropy_tick,
                              DefaultTick),
    erlang:send_after(Tick, Service, tick),
    ok.

-spec maybe_tick(state()) -> state().
maybe_tick(State) ->
    case enabled() of
        true ->
            case riak_core_capability:get({State#state.service, anti_entropy},
                                          disabled) of
                disabled ->
                    NextState = State;
                enabled_v1 ->
                    NextState = tick(State)
            end;
        false ->
            %% Ensure we do not have any running index_hashtrees, which can
            %% happen when disabling anti-entropy on a live system.
            _ = [riak_core_index_hashtree:stop(T) || {_,T} <- State#state.trees],
            NextState = State
    end,
    schedule_tick(gen_name(State#state.service)),
    NextState.

-spec tick(state()) -> state().
tick(State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    State1 = query_and_set_aae_throttle(State),
    State2 = maybe_reload_hashtrees(Ring, State1),
    State3 = lists:foldl(fun(_,S) ->
                                 maybe_poke_tree(S)
                         end, State2, lists:seq(1,10)),
    State4 = maybe_exchange(Ring, State3),
    State4.

-spec maybe_poke_tree(state()) -> state().
maybe_poke_tree(State=#state{trees=[]}) ->
    State;
maybe_poke_tree(State) ->
    {Tree, State2} = next_tree(State),
    riak_core_index_hashtree:poke(Tree),
    State2.

%%%===================================================================
%%% Exchanging
%%%===================================================================

-spec do_exchange_status(pid(), vnode(), vnode(), index_n(), any(), state()) -> state().
do_exchange_status(_Pid, LocalVN, RemoteVN, IndexN, Reply, State) ->
    {LocalIdx, _} = LocalVN,
    {RemoteIdx, RemoteNode} = RemoteVN,
    case Reply of
        ok ->
            State;
        {remote, anti_entropy_disabled} ->
            lager:warning("Active anti-entropy is disabled on ~p", [RemoteNode]),
            State;
        _ ->
            State2 = requeue_exchange(LocalIdx, RemoteIdx, IndexN, State),
            State2
    end.

-spec enqueue_exchange(index() |
                       {index(), index_n()} |
                       {index(), index(), index_n()}, state()) -> state().
enqueue_exchange(E={Index, _RemoteIdx, _IndexN}, State) ->
    %% Verify that the exchange is valid
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Exchanges = all_pairwise_exchanges(Index, Ring),
    case lists:member(E, Exchanges) of
        true ->
            enqueue_exchanges([E], State);
        false ->
            State
    end;
enqueue_exchange({Index, IndexN}, State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Exchanges = all_pairwise_exchanges(Index, Ring),
    Exchanges2 = [Exchange || Exchange={_, _, IdxN} <- Exchanges,
                              IdxN =:= IndexN],
    enqueue_exchanges(Exchanges2, State);
enqueue_exchange(Index, State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Exchanges = all_pairwise_exchanges(Index, Ring),
    enqueue_exchanges(Exchanges, State).

-spec enqueue_exchanges([exchange()], state()) -> state().
enqueue_exchanges(Exchanges, State) ->
    EQ = prune_exchanges(State#state.exchange_queue ++ Exchanges),
    State#state{exchange_queue=EQ}.

-spec start_exchange(vnode(),
                     {index(), index_n()},
                     riak_core_ring(),
                     state()) -> {any(), state()}.
start_exchange(LocalVN, {RemoteIdx, IndexN}, Ring, State) ->
    %% in rare cases, when the ring is resized, there may be an
    %% exchange enqueued for an index that no longer exists. catch
    %% the case here and move on
    try riak_core_ring:index_owner(Ring, RemoteIdx) of
        Owner ->
            Nodes = lists:usort([node(), Owner]),
            DownNodes = Nodes -- riak_core_node_watcher:nodes(State#state.service),
            case DownNodes of
                [] ->
                    RemoteVN = {RemoteIdx, Owner},
                    start_exchange(LocalVN, RemoteVN, IndexN, Ring, State);
                _ ->
                    {{down, State#state.service, DownNodes}, State}
            end
    catch
        error:{badmatch,_} ->
            lager:warning("ignoring exchange to non-existent index: ~p", [RemoteIdx]),
            {ok, State}
    end.

start_exchange(LocalVN, RemoteVN, IndexN, Ring, State) ->
    {LocalIdx, _} = LocalVN,
    {RemoteIdx, _} = RemoteVN,
    case riak_core_ring:vnode_type(Ring, LocalIdx) of
        primary ->
            VNode = State#state.vnode,
            case orddict:find(LocalIdx, State#state.trees) of
                error ->
                    %% The local vnode has not yet registered it's
                    %% index_hashtree. Likewise, the vnode may not even
                    %% be running (eg. after a crash).  Send request to
                    %% the vnode to trigger on-demand start and requeue
                    %% exchange.
                    riak_core_aae_vnode:request_hashtree_pid(VNode:master(), LocalIdx),
                    State2 = requeue_exchange(LocalIdx, RemoteIdx, IndexN, State),
                    {not_built, State2};
                {ok, Tree} ->
                    case riak_core_exchange_fsm:start(State#state.service,
                                                      LocalVN, RemoteVN,
                                                      IndexN, Tree, self(),
                                                      VNode) of
                        {ok, FsmPid} ->
                            Ref = monitor(process, FsmPid),
                            Exchanges = State#state.exchanges,
                            Exchanges2 = [{LocalIdx, Ref, FsmPid} | Exchanges],
                            {ok, State#state{exchanges=Exchanges2}};
                        {error, Reason} ->
                            {Reason, State}
                    end
            end;
        _ ->
            %% No longer owner of this partition or partition is
            %% part or larger future ring, ignore exchange
            {not_responsible, State}
    end.

-spec all_pairwise_exchanges(index(), riak_core_ring())
                            -> [exchange()].
all_pairwise_exchanges(Index, Ring) ->
    LocalIndexN = riak_core_util:responsible_preflists(Index, Ring),
    Sibs = riak_core_util:preflist_siblings(Index),
    lists:flatmap(
      fun(RemoteIdx) ->
              RemoteIndexN = riak_core_util:responsible_preflists(RemoteIdx, Ring),
              SharedIndexN = ordsets:intersection(ordsets:from_list(LocalIndexN),
                                                  ordsets:from_list(RemoteIndexN)),
              [{Index, RemoteIdx, IndexN} || IndexN <- SharedIndexN]
      end, Sibs).

-spec all_exchanges(node(), riak_core_ring(), state())
                   -> [exchange()].
all_exchanges(_Node, Ring, #state{trees=Trees}) ->
    Indices = orddict:fetch_keys(Trees),
    lists:flatmap(fun(Index) ->
                          all_pairwise_exchanges(Index, Ring)
                  end, Indices).

-spec add_index_exchanges(index(), state()) -> state().
add_index_exchanges(_Index, State) when State#state.mode == manual ->
    State;
add_index_exchanges(Index, State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Exchanges = all_pairwise_exchanges(Index, Ring),
    EQ = State#state.exchange_queue ++ Exchanges,
    EQ2 = prune_exchanges(EQ),
    State#state{exchange_queue=EQ2}.

-spec prune_exchanges([exchange()])
                     -> [exchange()].
prune_exchanges(Exchanges) ->
    L = [if A < B ->
                 {A, B, IndexN};
            true ->
                 {B, A, IndexN}
         end || {A, B, IndexN} <- Exchanges],
    lists:usort(L).

-spec already_exchanging(index() ,state()) -> boolean().
already_exchanging(Index, #state{exchanges=E}) ->
    case lists:keyfind(Index, 1, E) of
        false ->
            false;
        {Index,_,_} ->
            true
    end.

-spec maybe_exchange(riak_core_ring(), state()) -> state().
maybe_exchange(Ring, State) ->
    case next_exchange(Ring, State) of
        {none, State2} ->
            State2;
        {NextExchange, State2} ->
            {LocalIdx, RemoteIdx, IndexN} = NextExchange,
            case already_exchanging(LocalIdx, State) of
                true ->
                    requeue_exchange(LocalIdx, RemoteIdx, IndexN, State2);
                false ->
                    LocalVN = {LocalIdx, node()},
                    case start_exchange(LocalVN, {RemoteIdx, IndexN}, Ring, State2) of
                        {ok, State3} ->
                            State3;
                        {_Reason, State3} ->
                            State3
                    end
            end
    end.

-spec next_exchange(riak_core_ring(), state()) -> {'none' | exchange(), state()}.
next_exchange(_Ring, State=#state{exchange_queue=[], trees=[]}) ->
    {none, State};
next_exchange(_Ring, State=#state{exchange_queue=[],
                                  mode=Mode}) when Mode == manual ->
    {none, State};
next_exchange(Ring, State=#state{exchange_queue=[]}) ->
    case prune_exchanges(all_exchanges(node(), Ring, State)) of
        [] ->
            {none, State};
        [Exchange|Rest] ->
            State2 = State#state{exchange_queue=Rest},
            {Exchange, State2}
    end;
next_exchange(_Ring, State=#state{exchange_queue=Exchanges}) ->
    [Exchange|Rest] = Exchanges,
    State2 = State#state{exchange_queue=Rest},
    {Exchange, State2}.

-spec requeue_poke_int(index(), state()) -> state().
requeue_poke_int(Index, State=#state{trees=Trees}) ->
    case orddict:find(Index, Trees) of
        {ok, Tree} ->
            Queue = State#state.tree_queue ++ [{Index,Tree}],
            State#state{tree_queue=Queue};
        _ ->
            State
    end.

-spec requeue_exchange(index(), index(), index_n(), state()) -> state().
requeue_exchange(LocalIdx, RemoteIdx, IndexN, State) ->
    Exchange = {LocalIdx, RemoteIdx, IndexN},
    case lists:member(Exchange, State#state.exchange_queue) of
        true ->
            State;
        false ->
            lager:debug("Requeue: ~p", [{LocalIdx, RemoteIdx, IndexN}]),
            Exchanges = State#state.exchange_queue ++ [Exchange],
            State#state{exchange_queue=Exchanges}
    end.

get_aae_throttle() ->
    app_helper:get_env(riak_core, ?AAE_THROTTLE_ENV_KEY, 0).

set_aae_throttle(Milliseconds) when is_integer(Milliseconds), Milliseconds >= 0 ->
    application:set_env(riak_core, ?AAE_THROTTLE_ENV_KEY, Milliseconds).

get_aae_throttle_kill() ->
    case app_helper:get_env(riak_core, ?AAE_THROTTLE_KILL_ENV_KEY, undefined) of
        true ->
            true;
        _ ->
            false
    end.

set_aae_throttle_kill(Bool) when Bool == true; Bool == false ->
    application:set_env(riak_core, ?AAE_THROTTLE_KILL_ENV_KEY, Bool).

get_max_local_vnodeq() ->
    try
	{ok, [{max,M}]} =
	    exometer:get_value(
	      [riak_core_stat:prefix(),riak_core,vnodeq,riak_core_vnode],
	      [max]),
	{M, node()}
    catch _X:_Y ->
            %% This can fail locally if riak_core & riak_core haven't finished their setup.
            {0, node()}
    end.

get_aae_throttle_limits() ->
    %% init() should have already set a sane default, so the default below should never be used.
    app_helper:get_env(riak_core, aae_throttle_limits, [{-1, 0}]).

%% @doc Set AAE throttle limits list
%%
%% Limit list = [{max_vnode_q_len, per-repair-delay}]
%% A tuple with max_vnode_q_len=-1 must be present.
%% List sorting is not required: the throttle is robust with any ordering.

set_aae_throttle_limits(Limits) ->
    case {lists:keyfind(-1, 1, Limits),
          catch lists:all(fun({Min, Lim}) ->
                                  is_integer(Min) andalso is_integer(Lim) andalso
                                      Lim >= 0
                          end, Limits)} of
        {{-1, _}, true} ->
            lager:info("Setting AAE throttle limits: ~p\n", [Limits]),
            application:set_env(riak_core, aae_throttle_limits, Limits),
            ok;
        {Else1, Else2} ->
            {error, [{negative_one_length_is_present, Else1},
                     {all_sleep_times_are_non_negative_integers, Else2}]}
    end.

query_and_set_aae_throttle(#state{last_throttle=LastThrottle} = State) ->
    case get_aae_throttle_kill() of
        false ->
            query_and_set_aae_throttle2(State);
        true ->
            perhaps_log_throttle_change(LastThrottle, 0, kill_switch),
            set_aae_throttle(0),
            State#state{last_throttle=0}
    end.

query_and_set_aae_throttle2(#state{vnode_status_pid = undefined} = State) ->
    {Pid, _Ref} = spawn_monitor(fun() ->
        RES = fix_up_rpc_errors(
                ?MODULE:multicall([node()|nodes()],
                                  ?MODULE, get_max_local_vnodeq, [], 10*1000)),
              exit({result, RES})
         end),
    State#state{vnode_status_pid = Pid};
query_and_set_aae_throttle2(State) ->
    State.

query_and_set_aae_throttle3({result, {MaxNds, BadNds}},
                            #state{last_throttle=LastThrottle} = State) ->
    Limits = lists:sort(get_aae_throttle_limits()),
    %% If a node is really hosed, then this RPC call is going to fail
    %% for that node.  We might also delay the 'tick' processing by
    %% several seconds.  But the tick processing is OK if it's delayed
    %% while we wait for slow nodes here.
    {WorstVMax, WorstNode} =
        case {BadNds, lists:reverse(lists:sort(MaxNds))} of
            {[], [{VMax, Node}|_]} ->
                {VMax, Node};
            {BadNodes, _} ->
                %% If anyone couldn't respond, let's assume the worst.
                %% If that node is actually down, then the net_kernel
                %% will mark it down for us soon, and then we'll
                %% calculate a real value after that.  Note that a
                %% tuple as WorstVMax will always be bigger than an
                %% integer, so we can avoid using false information
                %% like WorstVMax=99999999999999 and also give
                %% something meaningful in the user's info msg.
                {{unknown_mailbox_sizes, node_list, BadNds}, BadNodes}
        end,
    {Sat, _NonSat} = lists:partition(fun({X, _Limit}) -> X < WorstVMax end, Limits),
    [{_, NewThrottle}|_] = lists:reverse(lists:sort(Sat)),
    perhaps_log_throttle_change(LastThrottle, NewThrottle,
                                {WorstVMax, WorstNode}),
    set_aae_throttle(NewThrottle),
    State#state{last_throttle=NewThrottle}.

perhaps_log_throttle_change(Last, New, kill_switch) when Last /= New ->
    _ = lager:info("Changing AAE throttle from ~p -> ~p msec/key, "
                   "based on kill switch on local node",
                   [Last, New]);
perhaps_log_throttle_change(Last, New, {WorstVMax, WorstNode}) when Last /= New ->
    _ = lager:info("Changing AAE throttle from ~p -> ~p msec/key, "
                   "based on maximum vnode mailbox size ~p from ~p",
                   [Last, New, WorstVMax, WorstNode]);
perhaps_log_throttle_change(_, _, _) ->
    ok.

%% Wrapper for meck interception for testing.
multicall(A, B, C, D, E) ->
    rpc:multicall(A, B, C, D, E).

fix_up_rpc_errors({ResL, BadL}) ->
    lists:foldl(fun({N, _}=R, {Rs, Bs}) when is_integer(N) ->
                        {[R|Rs], Bs};
                   (_, {Rs, Bs}) ->
                        {Rs, [bad_rpc_result|Bs]}
                end, {[], BadL}, ResL).

-ifdef(TEST).

make_state() ->
    #state{}.

get_last_throttle(State) ->
    State2 = wait_for_vnode_status(State),
    {State2#state.last_throttle, State2}.

wait_for_vnode_status(State) ->
    case get_aae_throttle_kill() of
        true ->
            State;
        false ->
            receive
                {'DOWN',_,_,_,_} = Msg ->
                    element(2, handle_info(Msg, State))
            after 10*1000 ->
                    error({?MODULE, wait_for_vnode_status, timeout})
            end
    end.

-endif.
