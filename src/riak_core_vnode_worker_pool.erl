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
-module(riak_core_vnode_worker_pool).

-behaviour(gen_fsm).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
        terminate/3, code_change/4]).

%% gen_fsm states
-export([ready/2, queueing/2, ready/3, queueing/3, shutdown/2, shutdown/3]).

%% API
-export([start_link/5, stop/2, shutdown_pool/2, handle_work/3]).

-record(state, {
        queue = queue:new(),
        pool :: pid(),
        monitors = [] :: list(),
        shutdown :: undefined | {pid(), reference()}
    }).

start_link(WorkerMod, PoolSize, VNodeIndex, WorkerArgs, WorkerProps) ->
    gen_fsm:start_link(?MODULE, [WorkerMod, PoolSize,  VNodeIndex, WorkerArgs, WorkerProps], []).

handle_work(Pid, Work, From) ->
    gen_fsm:send_event(Pid, {work, Work, From}).

stop(Pid, Reason) ->
    gen_fsm:sync_send_all_state_event(Pid, {stop, Reason}).

%% wait for all the workers to finish any current work
shutdown_pool(Pid, Wait) ->
    gen_fsm:sync_send_all_state_event(Pid, {shutdown, Wait}, infinity).

init([WorkerMod, PoolSize, VNodeIndex, WorkerArgs, WorkerProps]) ->
    {ok, Pid} = poolboy:start_link([{worker_module, riak_core_vnode_worker},
            {worker_args, [VNodeIndex, WorkerArgs, WorkerProps]},
            {worker_callback_mod, WorkerMod},
            {size, PoolSize}, {max_overflow, 0}]),
    {ok, ready, #state{pool=Pid}}.

ready(_Event, _From, State) ->
    {reply, ok, ready, State}.

ready({work, Work, From} = Msg, #state{pool=Pool, queue=Q, monitors=Monitors} = State) ->
    case poolboy:checkout(Pool, false) of
        full ->
            {next_state, queueing, State#state{queue=queue:in(Msg, Q)}};
        Pid when is_pid(Pid) ->
            NewMonitors = monitor_worker(Pid, From, Work, Monitors),
            ok = riak_core_vnode_worker:handle_work(Pid, Pool, Work, From),
            {next_state, ready, State#state{monitors=NewMonitors}}
    end;
ready(_Event, State) ->
    {next_state, ready, State}.

queueing(_Event, _From, State) ->
    {reply, ok, queueing, State}.

queueing({work, _Work, _From} = Msg, #state{queue=Q} = State) ->
    {next_state, queueing, State#state{queue=queue:in(Msg, Q)}};
queueing(_Event, State) ->
    {next_state, queueing, State}.

shutdown(_Event, _From, State) ->
    {reply, ok, shutdown, State}.

shutdown({work, _Work, From}, State) ->
    %% tell the process requesting work that we're shutting down
    riak_core_vnode:reply(From, {error, vnode_shutdown}),
    {next_state, shutdown, State};
shutdown(_Event, State) ->
    {next_state, shutdown, State}.

handle_event({checkin, Pid}, shutdown, #state{monitors=Monitors0} = State) ->
    Monitors = lists:keydelete(Pid, 1, Monitors0),
    case Monitors of
        [] -> %% work all done, time to exit!
            {stop, shutdown, State};
        _ ->
            {next_state, shutdown, State#state{monitors=Monitors}}
    end;
handle_event({checkin, Worker}, _, #state{pool = Pool, queue=Q, monitors=Monitors0} = State) ->
    Monitors = lists:keydelete(Worker, 1, Monitors0),
    case queue:out(Q) of
        {{value, {work, Work, From}}, Rem} ->
            case poolboy:checkout(Pool, false) of
                full ->
                    {next_state, queueing, State#state{queue=Q,
                            monitors=Monitors}};
                Pid when is_pid(Pid) ->
                    NewMonitors = monitor_worker(Pid, From, Work, Monitors),
                    ok = riak_core_vnode_worker:handle_work(Pid, Pool, Work, From),
                    {next_state, queueing, State#state{queue=Rem,
                            monitors=NewMonitors}}
            end;
        {empty, Empty} ->
            {next_state, ready, State#state{queue=Empty, monitors=Monitors}}
    end;
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event({stop, Reason}, _From, _StateName, State) ->
    {stop, Reason, ok, State}; 
handle_sync_event({shutdown, Time}, From, _StateName, #state{queue=Q,
        monitors=Monitors} = State) ->
    discard_queued_work(Q),
    case Monitors of
        [] ->
            {stop, shutdown, ok, State};
        _ ->
            case Time of
                infinity ->
                    ok;
                _ when is_integer(Time) ->
                    erlang:send_after(Time, self(), shutdown)
            end,
            {next_state, shutdown, State#state{shutdown=From, queue=queue:new()}}
    end;
handle_sync_event(_Event, _From, StateName, State) ->
    {reply, {error, unknown_message}, StateName, State}.

handle_info({'DOWN', _Ref, _, Pid, Info}, StateName, #state{monitors=Monitors} = State) ->
    %% remove the listing for the dead worker
    case lists:keyfind(Pid, 1, Monitors) of
        {Pid, _, From, Work} ->
            riak_core_vnode:reply(From, {error, {worker_crash, Info, Work}}),
            NewMonitors = lists:keydelete(Pid, 1, Monitors),
            %% pretend a worker just checked in so that any queued work can
            %% sent to the new worker just started to replace this dead one
            gen_fsm:send_all_state_event(self(), {checkin, undefined}),
            {next_state, StateName, State#state{monitors=NewMonitors}};
        false ->
            {next_state, StateName, State}
    end;
handle_info(shutdown, shutdown, #state{monitors=Monitors} = State) ->
    %% we've waited too long to shutdown, time to force the issue.
    [riak_core_vnode:reply(From, {error, vnode_shutdown}) || {_, _, From, _}
        <- Monitors],
    {stop, shutdown, State};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, #state{pool=Pool}) ->
    %% stop poolboy
    gen_fsm:sync_send_all_state_event(Pool, stop),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% Keep track of which worker we pair with what work/from and monitor the
%% worker. Only active workers are tracked
monitor_worker(Worker, From, Work, Monitors) ->
    case lists:keyfind(Worker, 1, Monitors) of
        {Worker, Ref, _OldFrom, _OldWork} ->
            %% reuse old monitor and just update the from & work
            lists:keyreplace(Worker, 1, Monitors, {Worker, Ref, From, Work});
        false ->
            Ref = erlang:monitor(process, Worker),
            [{Worker, Ref, From, Work} | Monitors]
    end.

discard_queued_work(Q) ->
    case queue:out(Q) of
        {{value, {work, _Work, From}}, Rem} ->
            riak_core_vnode:reply(From, {error, vnode_shutdown}),
            discard_queued_work(Rem);
        {empty, _Empty} ->
            ok
    end.

