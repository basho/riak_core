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
-export([all_vnodes/0, ring_changed/1, set_not_forwarding/2,
         force_handoffs/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {not_forwarding :: [pid()]}).

-define(DEFAULT_OWNERSHIP_TRIGGER, 8).

%% ===================================================================
%% Public API
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).

all_vnodes() ->
    Mods = [Mod || {_App, Mod} <- riak_core:vnode_modules()],
    lists:flatmap(fun all_vnodes/1, Mods).

all_vnodes(Mod) ->
    try riak_core_vnode_master:all_index_pid(Mod) of
        IdxPids ->
            [{Mod, Idx, Pid} || {Idx, Pid} <- IdxPids]
    catch
        _:_ ->
            []
    end.

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
    
%% ===================================================================
%% gen_server behaviour
%% ===================================================================

%% @private
init(_State) ->
    State = #state{not_forwarding=[]},
    {ok, State}.

%% @private
handle_call(_, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast({ring_changed, Ring}, State=#state{not_forwarding=NF}) ->
    %% Inform vnodes that the ring has changed so they can update their
    %% forwarding state.
    AllVNodes = all_vnodes(),
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

