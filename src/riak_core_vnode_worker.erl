%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.
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
-export([start_link/1, handle_work/3, handle_work/4]).

-include("riak_core_vnode.hrl").

-ifdef(PULSE).
-compile(export_all).
-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module, [{gen_fsm, pulse_gen_fsm},
                                 {gen_server, pulse_gen_server}]}).
-endif.

%% It would be nice to be able to provide proper specs for callbacks, but the
%% behavior of the optional_callbacks attribute is nonsensical, so we implement
%% behaviour_info/1 ourselves and live without the dialyzer type information.
%%
%% Were they properly specified, the callback specs would be:
%%
%%  init_worker(VNodeIndex, WorkerArgs, WorkerProps) -> {ok, WorkerState}.
%%  where:
%%      VNodeIndex  :: partition()
%%      WorkerArgs  :: list()
%%      WorkerProps :: [atom() | {atom(), term()}]
%%      WorkerState :: term()
%%
%%  handle_work(WorkSpec, Sender, WorkerState) -> Response.
%%  where:
%%      WorkSpec    :: tuple() | record()
%%      Sender      :: sender()
%%      WorkerState :: term()
%%      Response    :: {reply, Reply, WorkerState} | {noreply, WorkerState}
%%      Reply       :: term()
%%
%% Optionally, the following can be provided to tell the vnode worker pool
%% whether a particular type of work specification is recognized by
%% handle_work/3:
%%
%%  worker_supports(RecordType :: atom()) -> boolean()
%%
-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
behaviour_info(callbacks) ->
    [{init_worker, 3}, {handle_work, 3}];
behaviour_info(optional_callbacks) ->
    [{worker_supports, 1}];
behaviour_info(_) ->
    undefined.

-record(state, {
    module :: atom(),
    modstate :: any()
}).

start_link(Args) ->
    WorkerMod = proplists:get_value(worker_callback_mod, Args),
    [VNodeIndex, WorkerArgs, WorkerProps, Caller] = proplists:get_value(worker_args, Args),
    gen_server:start_link(?MODULE, [WorkerMod, VNodeIndex, WorkerArgs, WorkerProps, Caller], []).

handle_work(Worker, Work, From) ->
    handle_work(Worker, Work, From, self()).

%% TODO: should workers be required to handle ?VNODE_JOB{} ?
handle_work(Worker, ?VNODE_JOB{work = Work}, From, Caller) ->
    handle_work(Worker, Work, From, Caller);
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

