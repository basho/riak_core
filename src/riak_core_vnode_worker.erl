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
-module(riak_core_vnode_worker).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
        code_change/3]).
-export([start_link/1, handle_work/3, handle_work/4]).

-include("riak_core_vnode.hrl").

-ifdef(PULSE).
-compile(export_all).
-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module, [{gen_fsm, pulse_gen_fsm},
                                 {gen_server, pulse_gen_server}]}).
-endif.

-record(state, {
        module :: atom(),
        modstate :: any()
    }).

-callback init_worker(VNodeIndex::partition(),
                      Args::list(), Props::proplists:proplist()) ->
    {ok, State::term()}.
-callback handle_work({Op::atom(),
                       FoldFun::function(), FinishFun::function()},
                      _Sender::pid(), State::term()) ->
    {noreply, State::term()}.


start_link(Args) ->
    WorkerMod = proplists:get_value(worker_callback_mod, Args),
    [VNodeIndex, WorkerArgs, WorkerProps, Caller] = proplists:get_value(worker_args, Args),
    gen_server:start_link(?MODULE, [WorkerMod, VNodeIndex, WorkerArgs, WorkerProps, Caller], []).

handle_work(Worker, Work, From) ->
    handle_work(Worker, Work, From, self()).

handle_work(Worker, Work, From, Caller) ->
    gen_server:cast(Worker, {work, Work, From, Caller}).

init([Module, VNodeIndex, WorkerArgs, WorkerProps, Caller]) ->
    {ok, WorkerState} = Module:init_worker(VNodeIndex, WorkerArgs, WorkerProps),
    %% let the pool queue manager know there might be a worker to checkout
    gen_fsm:send_all_state_event(Caller, worker_start),
    {ok, #state{module=Module, modstate=WorkerState}}.

handle_call(Event, _From, State) ->
    lager:debug("Vnode worker received synchronous event: ~p.", [Event]),
    {reply, ok, State}.

handle_cast({work, Work, WorkFrom, Caller},
            #state{module = Mod, modstate = ModState} = State) ->
    NewModState = case Mod:handle_work(Work, WorkFrom, ModState) of
        {reply, Reply, NS} ->
            riak_core_vnode:reply(WorkFrom, Reply),
            NS;
        {noreply, NS} ->
            NS
    end,
    %% check the worker back into the pool
    gen_fsm:send_all_state_event(Caller, {checkin, self()}),
    {noreply, State#state{modstate=NewModState}};
handle_cast(_Event, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

