%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
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

-module(riak_core_vnode_manager).

-behaviour(gen_server).

-export([start_link/0, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([all_vnodes/0, all_vnodes/1, all_vnodes_status/0, ring_changed/1,
         force_handoffs/0]).
-export([all_index_pid/1, get_vnode_pid/2, start_vnode/2,
         unregister_vnode/2, unregister_vnode/3, vnode_event/4]).
%% Field debugging
-export([get_tab/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(idxrec, {key, idx, mod, pid, monref}).
-record(state, {idxtab, 
                forwarding :: [pid()],
                handoff :: [{term(), integer(), pid(), node()}],
                known_modules :: [term()],
                never_started :: [{integer(), term()}],
                vnode_start_tokens :: integer()
               }).

-define(DEFAULT_OWNERSHIP_TRIGGER, 8).

%% ===================================================================
%% Public API
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).

all_vnodes() ->
    gen_server:call(?MODULE, all_vnodes).

all_vnodes(Mod) ->
    gen_server:call(?MODULE, {all_vnodes, Mod}).

all_vnodes_status() ->
    gen_server:call(?MODULE, all_vnodes_status).

ring_changed(_TaintedRing) ->
    %% The ring passed into ring events is the locally modified tainted ring.
    %% Since the vnode manager uses operations that cannot work on the
    %% tainted ring, we must retreive the raw ring directly.
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    gen_server:cast(?MODULE, {ring_changed, Ring}).

%% @doc Provided for support/debug purposes. Forces all running vnodes to start
%%      handoff. Limited by handoff_concurrency setting and therefore may need
%%      to be called multiple times to force all handoffs to complete.
force_handoffs() ->
    gen_server:cast(?MODULE, force_handoffs).

unregister_vnode(Index, VNodeMod) ->
    unregister_vnode(Index, self(), VNodeMod).

unregister_vnode(Index, Pid, VNodeMod) ->
    gen_server:cast(?MODULE, {unregister, Index, VNodeMod, Pid}).

all_index_pid(VNodeMod) ->
    gen_server:call(?MODULE, {all_index_pid, VNodeMod}, infinity).

get_vnode_pid(Index, VNodeMod) ->
    gen_server:call(?MODULE, {Index, VNodeMod, get_vnode}, infinity).

start_vnode(Index, VNodeMod) ->
    gen_server:cast(?MODULE, {Index, VNodeMod, start_vnode}).

vnode_event(Mod, Idx, Pid, Event) ->
    gen_server:cast(?MODULE, {vnode_event, Mod, Idx, Pid, Event}).

get_tab() ->
    gen_server:call(?MODULE, get_tab, infinity).

%% ===================================================================
%% gen_server behaviour
%% ===================================================================

%% @private
init(_State) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    Mods = [Mod || {_, Mod} <- riak_core:vnode_modules()],
    State = #state{forwarding=[], handoff=[],
                   known_modules=[], never_started=[], vnode_start_tokens=0},
    State2 = find_vnodes(State),
    AllVNodes = get_all_vnodes(Mods, State2),
    State3 = update_forwarding(AllVNodes, Mods, Ring, State2),
    State4 = update_handoff(AllVNodes, Ring, State3),
    schedule_management_timer(),
    {ok, State4}.

%% @private
find_vnodes(State) ->
    %% Get the current list of vnodes running in the supervisor. We use this
    %% to rebuild our ETS table for routing messages to the appropriate
    %% vnode.
    VnodePids = [Pid || {_, Pid, worker, _}
                            <- supervisor:which_children(riak_core_vnode_sup),
                        is_pid(Pid) andalso is_process_alive(Pid)],
    IdxTable = ets:new(ets_vnodes, [{keypos, 2}, private]),

    %% If the vnode manager is being restarted, scan the existing
    %% vnode children and work out which module and index they are
    %% responsible for.  During startup it is possible that these
    %% vnodes may be shutting down as we check them if there are
    %% several types of vnodes active.
    PidIdxs = lists:flatten(
                [try
                     [{Pid, riak_core_vnode:get_mod_index(Pid)}]
                 catch
                     _:_Err ->
                         []
                 end || Pid <- VnodePids]),

    %% Populate the ETS table with processes running this VNodeMod (filtered
    %% in the list comprehension)
    F = fun(Pid, Idx, Mod) ->
                Mref = erlang:monitor(process, Pid),
                #idxrec { key = {Idx,Mod}, idx = Idx, mod = Mod, pid = Pid,
                          monref = Mref }
        end,
    IdxRecs = [F(Pid, Idx, Mod) || {Pid, {Mod, Idx}} <- PidIdxs],
    true = ets:insert_new(IdxTable, IdxRecs),
    State#state{idxtab=IdxTable}.

%% @private
handle_call(all_vnodes_status, _From, State) ->
    Reply = get_all_vnodes_status(State),
    {reply, Reply, State};
handle_call(all_vnodes, _From, State) ->
    Reply = get_all_vnodes(State),
    {reply, Reply, State};
handle_call({all_vnodes, Mod}, _From, State) ->
    Reply = get_all_vnodes(Mod, State),
    {reply, Reply, State};
handle_call({all_index_pid, Mod}, _From, State) ->
    Reply = get_all_index_pid(Mod, State),
    {reply, Reply, State};
handle_call({Partition, Mod, get_vnode}, _From, State) ->
    Pid = get_vnode(Partition, Mod, State),
    {reply, {ok, Pid}, State};
handle_call(get_tab, _From, State) ->
    {reply, ets:tab2list(State#state.idxtab), State};
handle_call(_, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast({Partition, Mod, start_vnode}, State) ->
    get_vnode(Partition, Mod, State),
    {noreply, State};
handle_cast({unregister, Index, Mod, Pid}, #state{idxtab=T} = State) ->
    %% Update forwarding state to ensure vnode is not restarted in
    %% incorrect forwarding state if next request arrives before next
    %% ring event.
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    State2 = update_forwarding({Mod, Index}, Ring, State),
    ets:match_delete(T, {idxrec, {Index, Mod}, Index, Mod, Pid, '_'}),
    riak_core_vnode_proxy:unregister_vnode(Mod, Index, Pid),
    {noreply, State2};
handle_cast({vnode_event, Mod, Idx, Pid, Event}, State) ->
    handle_vnode_event(Event, Mod, Idx, Pid, State);
handle_cast(force_handoffs, State) ->
    AllVNodes = get_all_vnodes(State),
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    State2 = update_handoff(AllVNodes, Ring, State),

    [maybe_trigger_handoff(Mod, Idx, Pid, State2)
     || {Mod, Idx, Pid} <- AllVNodes],

    {noreply, State2};
handle_cast({ring_changed, Ring}, State) ->
    %% Update vnode forwarding state
    AllVNodes = get_all_vnodes(State),
    Mods = [Mod || {_, Mod} <- riak_core:vnode_modules()],
    State2 = update_forwarding(AllVNodes, Mods, Ring, State),

    %% Update handoff state
    State3 = update_handoff(AllVNodes, Ring, State2),

    %% Trigger ownership transfers.
    trigger_ownership_handoff(Mods, Ring, State3),

    {noreply, State3};

handle_cast(management_tick, State) ->
    schedule_management_timer(),
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    Mods = [Mod || {_, Mod} <- riak_core:vnode_modules()],
    AllVNodes = get_all_vnodes(Mods, State),
    State2 = update_handoff(AllVNodes, Ring, State),
    trigger_ownership_handoff(Mods, Ring, State2),

    MaxStart = app_helper:get_env(riak_core, vnode_rolling_start, 16),
    State3 = State2#state{vnode_start_tokens=MaxStart},
    State4 = maybe_start_vnodes(Ring, State3),
    {noreply, State4};

handle_cast(maybe_start_vnodes, State) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    State2 = maybe_start_vnodes(Ring, State),
    {noreply, State2};

handle_cast(_, State) ->
    {noreply, State}.

handle_info({'DOWN', MonRef, process, _P, _I}, State) ->
    delmon(MonRef, State),
    {noreply, State}.

%% @private
handle_vnode_event(inactive, Mod, Idx, Pid, State) ->
    maybe_trigger_handoff(Mod, Idx, Pid, State),
    {noreply, State};
handle_vnode_event(handoff_complete, Mod, Idx, Pid, State) ->
    NewHO = orddict:erase({Mod, Idx}, State#state.handoff),
    gen_fsm:send_all_state_event(Pid, finish_handoff),
    {noreply, State#state{handoff=NewHO}};
handle_vnode_event(handoff_error, Mod, Idx, Pid, State) ->
    NewHO = orddict:erase({Mod, Idx}, State#state.handoff),
    gen_fsm:send_all_state_event(Pid, cancel_handoff),
    {noreply, State#state{handoff=NewHO}}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ===================================================================
%% Internal functions
%% ===================================================================

schedule_management_timer() ->
    ManagementTick = app_helper:get_env(riak_core,
                                        vnode_management_timer,
                                        10000),
    timer:apply_after(ManagementTick, gen_server, cast, [?MODULE, management_tick]).

trigger_ownership_handoff(Mods, Ring, State) ->
    Transfers = riak_core_ring:pending_changes(Ring),
    Limit = app_helper:get_env(riak_core,
                               forced_ownership_handoff,
                               ?DEFAULT_OWNERSHIP_TRIGGER),
    Throttle = lists:sublist(Transfers, Limit),
    Awaiting = [{Mod, Idx} || {Idx, Node, _, CMods, S} <- Throttle,
                              Mod <- Mods,
                              S =:= awaiting,
                              Node =:= node(),
                              not lists:member(Mod, CMods)],
    [maybe_trigger_handoff(Mod, Idx, State) || {Mod, Idx} <- Awaiting],
    ok.

get_all_index_pid(Mod, State) ->
    [list_to_tuple(L) 
     || L <- ets:match(State#state.idxtab, {idxrec, '_', '$1', Mod, '$2', '_'})].

get_all_vnodes(State) ->
    Mods = [Mod || {_App, Mod} <- riak_core:vnode_modules()],
    get_all_vnodes(Mods, State).

get_all_vnodes(Mods, State) when is_list(Mods) ->
    lists:flatmap(fun(Mod) -> get_all_vnodes(Mod, State) end, Mods);
get_all_vnodes(Mod, State) ->
    try get_all_index_pid(Mod, State) of
        IdxPids ->
            [{Mod, Idx, Pid} || {Idx, Pid} <- IdxPids]
    catch
        _:_ ->
            []
    end.

%% @private
idx2vnode(Idx, Mod, _State=#state{idxtab=T}) ->
    case ets:lookup(T, {Idx, Mod}) of
        [I] -> I#idxrec.pid;
        []  -> no_match
    end.

%% @private
delmon(MonRef, _State=#state{idxtab=T}) ->
    ets:match_delete(T, {idxrec, '_', '_', '_', '_', MonRef}).

%% @private
add_vnode_rec(I,  _State=#state{idxtab=T}) -> ets:insert(T,I).

%% @private
get_vnode(Idx, Mod, State) ->
    case idx2vnode(Idx, Mod, State) of
        no_match ->
            ForwardTo = get_forward(Mod, Idx, State),
            {ok, Pid} = riak_core_vnode_sup:start_vnode(Mod, Idx, ForwardTo),
            MonRef = erlang:monitor(process, Pid),
            add_vnode_rec(#idxrec{key={Idx,Mod},idx=Idx,mod=Mod,pid=Pid,
                                  monref=MonRef}, State),
            Pid;
        X -> X
    end.

get_forward(Mod, Idx, #state{forwarding=Fwd}) ->
    case orddict:find({Mod, Idx}, Fwd) of
        {ok, ForwardTo} ->
            ForwardTo;
        _ ->
            undefined
    end.

check_forward(Ring, Mod, Index) ->
    Node = node(),
    case riak_core_ring:next_owner(Ring, Index, Mod) of
        {Node, NextOwner, complete} ->
            {{Mod, Index}, NextOwner};
        _ ->
            {{Mod, Index}, undefined}
    end.

compute_forwarding(Mods, Ring) ->
    {AllIndices, _} = lists:unzip(riak_core_ring:all_owners(Ring)),
    Forwarding = [check_forward(Ring, Mod, Index) || Index <- AllIndices,
                                                     Mod <- Mods],
    orddict:from_list(Forwarding).

update_forwarding(AllVNodes, Mods, Ring,
                  State=#state{forwarding=Forwarding}) ->
    NewForwarding = compute_forwarding(Mods, Ring),

    %% Inform vnodes that have changed forwarding status
    VNodes = lists:sort([{{Mod, Idx}, Pid} || {Mod, Idx, Pid} <- AllVNodes]),
    Diff = NewForwarding -- Forwarding,
    [change_forward(VNodes, Mod, Idx, ForwardTo)
     || {{Mod, Idx}, ForwardTo} <- Diff],

    State#state{forwarding=NewForwarding}.

update_forwarding({Mod, Idx}, Ring, State=#state{forwarding=Forwarding}) ->
    {_, ForwardTo} = check_forward(Ring, Mod, Idx),
    NewForwarding = orddict:store({Mod, Idx}, ForwardTo, Forwarding),
    State#state{forwarding=NewForwarding}.

change_forward(VNodes, Mod, Idx, ForwardTo) ->
    case orddict:find({Mod, Idx}, VNodes) of
        error ->
            ok;
        {ok, Pid} ->
            riak_core_vnode:set_forwarding(Pid, ForwardTo),
            ok
    end.

update_handoff(AllVNodes, Ring, State) ->
    case riak_core_ring:ring_ready(Ring) of
        false ->
            State;
        true ->
            NewHO = lists:flatten([case should_handoff(Ring, Mod, Idx) of
                                       false ->
                                           [];
                                       {true, TargetNode} ->
                                           [{{Mod, Idx}, TargetNode}]
                                   end || {Mod, Idx, _Pid} <- AllVNodes]),
            State#state{handoff=orddict:from_list(NewHO)}
    end.

should_handoff(Ring, Mod, Idx) ->
    {_, NextOwner, _} = riak_core_ring:next_owner(Ring, Idx),
    Owner = riak_core_ring:index_owner(Ring, Idx),
    Ready = riak_core_ring:ring_ready(Ring),
    case determine_handoff_target(Ready, Owner, NextOwner) of
        undefined ->
            false;
        TargetNode ->
            case app_for_vnode_module(Mod) of
                undefined -> false;
                {ok, App} ->
                    case lists:member(TargetNode, 
                                      riak_core_node_watcher:nodes(App)) of
                        false  -> false;
                        true -> {true, TargetNode}
                    end
            end
    end.

determine_handoff_target(Ready, Owner, NextOwner) ->
    Me = node(),
    TargetNode = case {Ready, Owner, NextOwner} of
                     {_, _, Me} ->
                         Me;
                     {_, Me, undefined} ->
                         Me;
                     {true, Me, _} ->
                         NextOwner;
                     {_, _, undefined} ->
                         Owner;
                     {_, _, _} ->
                         Me
                 end,
    case TargetNode of
        Me ->
            undefined;
        _ ->
            TargetNode
    end.

app_for_vnode_module(Mod) when is_atom(Mod) ->
    case application:get_env(riak_core, vnode_modules) of
        {ok, Mods} ->
            case lists:keysearch(Mod, 2, Mods) of
                {value, {App, Mod}} ->
                    {ok, App};
                false ->
                    undefined
            end;
        undefined -> undefined
    end.

maybe_trigger_handoff(Mod, Idx, State) ->
    Pid = get_vnode(Idx, Mod, State),
    maybe_trigger_handoff(Mod, Idx, Pid, State).

maybe_trigger_handoff(Mod, Idx, Pid, _State=#state{handoff=HO}) ->
    case orddict:find({Mod, Idx}, HO) of
        {ok, TargetNode} ->
            riak_core_vnode:trigger_handoff(Pid, TargetNode),
            ok;
        error ->
            ok
    end.

get_all_vnodes_status(State=#state{forwarding=Forwarding, handoff=HO}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Owners = riak_core_ring:all_owners(Ring),
    VNodes = get_all_vnodes(State),
    Mods = [Mod || {_App, Mod} <- riak_core:vnode_modules()],

    ThisNode = node(),
    Types = [case Owner of
                 ThisNode ->
                     {{Mod, Idx}, {type, primary}};
                 _ ->
                     {{Mod, Idx}, {type, secondary}}
             end || {Idx, Owner} <- Owners,
                    Mod <- Mods],
    Types2 = lists:keysort(1, Types),
    Pids = [{{Mod, Idx}, {pid, Pid}} || {Mod, Idx, Pid} <- VNodes],
    Pids2 = lists:keysort(1, Pids),
    Forwarding2 = [{MI, {forwarding, Node}} || {MI,Node} <- Forwarding,
                                               Node /= undefined],
    Handoff2 = [{MI, {should_handoff, Node}} || {MI,Node} <- HO],

    MergeFn = fun(_, V1, V2) when is_list(V1) and is_list(V2) ->
                      V1 ++ V2;
                 (_, V1, V2) when is_list(V1) ->
                      V1 ++ [V2];
                 (_, V1, V2) ->
                      [V1, V2]
              end,
    Status = lists:foldl(fun(B, A) ->
                                 orddict:merge(MergeFn, A, B)
                         end, Types2, [Pids2, Forwarding2, Handoff2]),
    Status.

update_never_started(Ring, State) ->
    {Indices, _} = lists:unzip(riak_core_ring:all_owners(Ring)),
    lists:foldl(fun({_App, Mod}, StateAcc) ->
                        case lists:member(Mod, StateAcc#state.known_modules) of
                            false ->
                                update_never_started(Mod, Indices, StateAcc);
                            true ->
                                StateAcc
                        end
                end, State, riak_core:vnode_modules()).

update_never_started(Mod, Indices, State) ->
    IdxPids =
        try
            get_all_index_pid(Mod, State)
        catch
            _:_ -> []
        end,
    AlreadyStarted = [Idx || {Idx, _Pid} <- IdxPids],
    NeverStarted = ordsets:subtract(ordsets:from_list(Indices),
                                    ordsets:from_list(AlreadyStarted)),
    NeverStarted2 = [{Idx, Mod} || Idx <- NeverStarted],
    NeverStarted3 = NeverStarted2 ++ State#state.never_started,
    KnownModules = [Mod | State#state.known_modules],
    State#state{known_modules=KnownModules, never_started=NeverStarted3}.

maybe_start_vnodes(Ring, State) ->
    State2 = update_never_started(Ring, State),
    State3 = maybe_start_vnodes(State2),
    State3.

maybe_start_vnodes(State=#state{vnode_start_tokens=Tokens,
                                never_started=NeverStarted}) ->
    case {Tokens, NeverStarted} of
        {0, _} ->
            State;
        {_, []} ->
            State;
        {_, [{Idx, Mod} | NeverStarted2]} ->
            get_vnode(Idx, Mod, State),
            gen_server:cast(?MODULE, maybe_start_vnodes),
            State#state{vnode_start_tokens=Tokens-1,
                        never_started=NeverStarted2}
    end.
