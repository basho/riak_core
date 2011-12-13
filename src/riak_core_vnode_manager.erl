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
-export([all_vnodes/0, all_vnodes/1, ring_changed/1, force_handoffs/0]).
-export([all_nodes/1, all_index_pid/1, get_vnode_pid/2, start_vnode/2,
         unregister_vnode/2, unregister_vnode/3, vnode_event/4]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(idxrec, {key, idx, mod, pid, monref}).
-record(state, {idxtab, 
                forwarding :: [pid()],
                handoff :: [{term(), integer(), pid(), node()}]
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

%% Request a list of Pids for all vnodes
all_nodes(VNodeMod) ->
    gen_server:call(?MODULE, {all_nodes, VNodeMod}, infinity).

all_index_pid(VNodeMod) ->
    gen_server:call(?MODULE, {all_index_pid, VNodeMod}, infinity).

get_vnode_pid(Index, VNodeMod) ->
    gen_server:call(?MODULE, {Index, VNodeMod, get_vnode}, infinity).

start_vnode(Index, VNodeMod) ->
    gen_server:cast(?MODULE, {Index, VNodeMod, start_vnode}).

vnode_event(Mod, Idx, Pid, Event) ->
    gen_server:cast(?MODULE, {vnode_event, Mod, Idx, Pid, Event}).

%% ===================================================================
%% gen_server behaviour
%% ===================================================================

%% @private
init(_State) ->
    State = #state{forwarding=[], handoff=[]},
    State2 = update_forwarding(State),
    State3 = find_vnodes(State2),
    {ok, State3}.

%% @private
find_vnodes(State) ->
    %% Get the current list of vnodes running in the supervisor. We use this
    %% to rebuild our ETS table for routing messages to the appropriate
    %% vnode.
    VnodePids = [Pid || {_, Pid, worker, _}
                            <- supervisor:which_children(riak_core_vnode_sup)],
    IdxTable = ets:new(ets_vnodes, [{keypos, 2}]),

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
handle_call({all_nodes, Mod}, _From, State) ->
    {reply, lists:flatten(ets:match(State#state.idxtab, {idxrec, '_', '_', Mod, '$1', '_'})), State};
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
handle_call(_, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast({Partition, Mod, start_vnode}, State) ->
    get_vnode(Partition, Mod, State),
    {noreply, State};
handle_cast({unregister, Index, Mod, Pid}, #state{idxtab=T} = State) ->
    ets:match_delete(T, {idxrec, {Index, Mod}, Index, Mod, Pid, '_'}),
    riak_core_vnode_proxy:unregister_vnode(Mod, Index),
    gen_fsm:send_event(Pid, unregistered),
    {noreply, State};
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

    [maybe_trigger_handoff(Mod, Idx, State3) || {Mod, Idx} <- Awaiting],

    {noreply, State3};

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

get_all_index_pid(Mod, State) ->
    [list_to_tuple(L) 
     || L <- ets:match(State#state.idxtab, {idxrec, '_', '$1', Mod, '$2', '_'})].

get_all_vnodes(State) ->
    Mods = [Mod || {_App, Mod} <- riak_core:vnode_modules()],
    lists:flatmap(fun(Mod) -> get_all_vnodes(Mod, State) end, Mods).

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
    case ets:match(T, {idxrec, {Idx,Mod}, Idx, Mod, '$1', '_'}) of
        [[VNodePid]] -> VNodePid;
        [] -> no_match
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

update_forwarding(State) ->
    AllVNodes = get_all_vnodes(State),
    Mods = [Mod || {_, Mod} <- riak_core:vnode_modules()],
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    update_forwarding(AllVNodes, Mods, Ring, State).

update_forwarding(AllVNodes, Mods, Ring,
                  State=#state{forwarding=Forwarding}) ->
    VNodes = lists:sort([{{Mod, Idx}, Pid} || {Mod, Idx, Pid} <- AllVNodes]),
    {AllIndices, _} = lists:unzip(riak_core_ring:all_owners(Ring)),
    NewForwarding = [check_forward(Ring, Mod, Index) || Index <- AllIndices,
                                                        Mod <- Mods],

    %% Inform vnodes that have changed forwarding status
    Diff = NewForwarding -- Forwarding,
    [change_forward(VNodes, Mod, Idx, ForwardTo)
     || {{Mod, Idx}, ForwardTo} <- Diff],

    State#state{forwarding=NewForwarding}.

change_forward(VNodes, Mod, Idx, ForwardTo) ->
    case orddict:find({Mod, Idx}, VNodes) of
        error ->
            ok;
        {ok, Pid} ->
            %% Changing the forwarding state of a vnode FSM that is
            %% concurrently shutting down may result in an error,
            %% which we can safely ignore. Same with a noproc error
            %% occuring if the Pid for this Mod/Index is now stale.
            try
                riak_core_vnode:set_forwarding(Pid, ForwardTo),
                ok
            catch
                _:_ ->
                    ok
            end
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
    Me = node(),
    {_, NextOwner, _} = riak_core_ring:next_owner(Ring, Idx),
    Owner = riak_core_ring:index_owner(Ring, Idx),
    Ready = riak_core_ring:ring_ready(Ring),
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
            false;
        _ ->
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
            try
                riak_core_vnode:trigger_handoff(Pid, TargetNode),
                ok
            catch
                _:_ ->
                    ok
            end;
        error ->
            ok
    end.
