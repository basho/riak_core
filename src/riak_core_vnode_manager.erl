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
-export([all_vnodes/0, all_vnodes/1, ring_changed/1, set_not_forwarding/2,
         force_handoffs/0]).
-export([all_nodes/1, all_index_pid/1, get_vnode_pid/2, start_vnode/2,
         unregister_vnode/2, unregister_vnode/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(idxrec, {key, idx, mod, pid, monref}).
-record(state, {idxtab, 
                not_forwarding :: [pid()]
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

ring_changed(Ring) ->
    gen_server:cast(?MODULE, {ring_changed, Ring}).

set_not_forwarding(Pid, Value) ->
    gen_server:cast(?MODULE, {set_not_forwarding, Pid, Value}).

%% @doc Provided for support/debug purposes. Forces all running vnodes to start
%%      handoff. Limited by handoff_concurrency setting and therefore may need
%%      to be called multiple times to force all handoffs to complete.
force_handoffs() -> 
    [riak_core_vnode:trigger_handoff(Pid) || {_, _, Pid} <- all_vnodes()],
    ok.

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

%% ===================================================================
%% gen_server behaviour
%% ===================================================================

%% @private
init(_State) ->
    State = #state{not_forwarding=[]},
    State2 = find_vnodes(State),
    {ok, State2}.

%% @private
find_vnodes(State) ->
    %% Get the current list of vnodes running in the supervisor. We use this
    %% to rebuild our ETS table for routing messages to the appropriate
    %% vnode.
    VnodePids = [Pid || {_, Pid, worker, _}
                            <- supervisor:which_children(riak_core_vnode_sup)],
    IdxTable = ets:new(ets_vnodes, [{keypos, 2}]),

    %% In case this the vnode master is being restarted, scan the existing
    %% vnode children and work out which module and index they are responsible
    %% for.  During startup it is possible that these vnodes may be shutting
    %% down as we check them if there are several types of vnodes active.
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
handle_cast({ring_changed, Ring}, State=#state{not_forwarding=NF}) ->
    %% Inform vnodes that the ring has changed so they can update their
    %% forwarding state.
    AllVNodes = get_all_vnodes(State),
    AllPids = [Pid || {_Mod, _Idx, Pid} <- AllVNodes],
    Notify = AllPids -- NF,
    [riak_core_vnode:update_forwarding(Pid, Ring) || Pid <- Notify],

    %% Trigger ownership transfers.
    Transfers = riak_core_ring:pending_changes(Ring),
    Limit = app_helper:get_env(riak_core,
                               forced_ownership_handoff,
                               ?DEFAULT_OWNERSHIP_TRIGGER),
    Throttle = lists:sublist(Transfers, Limit),
    Mods = [Mod || {_, Mod} <- riak_core:vnode_modules()],
    Awaiting = [{Mod, Idx} || {Idx, Node, _, CMods, S} <- Throttle,
                              Mod <- Mods,
                              S =:= awaiting,
                              Node =:= node(),
                              not lists:member(Mod, CMods)],

    [riak_core_vnode:trigger_handoff(Pid) || {ModA, IdxA, Pid} <- AllVNodes,
                                             {ModB, IdxB} <- Awaiting,
                                             ModA =:= ModB,
                                             IdxA =:= IdxB],
    
    %% Filter out dead pids from the not_forwarding list.
    NF2 = lists:filter(fun(Pid) ->
                               is_pid(Pid) andalso is_process_alive(Pid)
                       end, NF),
    {noreply, State#state{not_forwarding=NF2}};

handle_cast({set_not_forwarding, Pid, Value},
            State=#state{not_forwarding=NF}) ->
    NF2 = case Value of
              true ->
                  lists:usort([Pid | NF]);
              false ->
                  NF -- [Pid]
          end,
    {noreply, State#state{not_forwarding=NF2}};

handle_cast(_, State) ->
    {noreply, State}.

handle_info({'DOWN', MonRef, process, _P, _I}, State) ->
    delmon(MonRef, State),
    {noreply, State}.

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
            {ok, Pid} = riak_core_vnode_sup:start_vnode(Mod, Idx),
            MonRef = erlang:monitor(process, Pid),
            add_vnode_rec(#idxrec{key={Idx,Mod},idx=Idx,mod=Mod,pid=Pid,
                                  monref=MonRef}, State),
            Pid;
        X -> X
    end.
