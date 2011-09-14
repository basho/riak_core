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
-module(riak_core_vnode_worker).

-behaviour(gen_server).

-export([behaviour_info/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
        code_change/3]).
-export([start_link/1, handle_work/4]).

-record(state, {
        module :: atom(),
        modstate :: any()
    }).

-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
behaviour_info(callbacks) ->
    [{init_worker,3},
     {handle_work,3}];
behaviour_info(_Other) ->
    undefined.

start_link(Args) ->
    WorkerMod = proplists:get_value(worker_callback_mod, Args),
    [VNodeIndex, WorkerArgs, WorkerProps] = proplists:get_value(worker_args, Args),
    gen_server:start_link(?MODULE, [WorkerMod, VNodeIndex, WorkerArgs, WorkerProps], []).

handle_work(Pid, Pool, Work, From) ->
    gen_server:call(Pid, {work, Pool, Work, From}).

init([Module, VNodeIndex, WorkerArgs, WorkerProps]) ->
    {ok, WorkerState} = Module:init_worker(VNodeIndex, WorkerArgs, WorkerProps),
    {ok, #state{module=Module, modstate=WorkerState}}.

handle_call({work, Pool, Work, WorkFrom}, {Pid, _} = From, #state{module = Mod,
        modstate = ModState} = State) ->
    gen_server:reply(From, ok), %% unblock the caller
    NewModState = case Mod:handle_work(Work, WorkFrom, ModState) of
        {reply, Reply, NS} ->
            riak_core_vnode:reply(WorkFrom, Reply),
            NS;
        {noreply, NS} ->
            NS
    end,
    %% check the worker back into the pool
    poolboy:checkin(Pool, self()),
    gen_fsm:send_all_state_event(Pid, {checkin, self()}),
    {noreply, State#state{modstate=NewModState}};
handle_call(_Event, _From, State) ->
    {reply, ok, State}.

handle_cast(_Event, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

