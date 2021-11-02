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

%% @doc This is a wrapper around a poolboy pool, that implements a
%% queue. That is, this process maintains a queue of work, and checks
%% workers out of a poolboy pool to consume it.
%%
%% The workers it uses send two messages to this process.
%%
%% The first message is at startup, the bare atom
%% `worker_started'. This is a clue to this process that a worker was
%% just added to the poolboy pool, so a checkout request has a chance
%% of succeeding. This is most useful after an old worker has exited -
%% `worker_started' is a trigger guaranteed to arrive at a time that
%% will mean an immediate poolboy:checkout will not beat the worker
%% into the pool.
%%
%% The second message is when the worker finishes work it has been
%% handed, the two-tuple, `{checkin, WorkerPid}'. This message gives
%% this process the choice of whether to give the worker more work or
%% check it back into poolboy's pool at this point. Note: the worker
%% should *never* call poolboy:checkin itself, because that will
%% confuse (or cause a race) with this module's checkout management.
-module(riak_core_worker_pool).

-behaviour(gen_fsm).

-compile({nowarn_deprecated_function, 
            [{gen_fsm, start_link, 3},
                {gen_fsm, send_event, 2},
                {gen_fsm, sync_send_all_state_event, 2},
                {gen_fsm, sync_send_all_state_event, 3}]}).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
        terminate/3, code_change/4]).

-export([start_link/3, handle_work/3, stop/2, shutdown_pool/2]).

%% gen_fsm states
-export([queueing/2, ready/2, ready/3, queueing/3, shutdown/2, shutdown/3]).

-export([monitor_worker/4]).

-ifdef(PULSE).
-compile(export_all).
-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module, [{gen_fsm, pulse_gen_fsm}]}).
-endif.

-define(SHUTDOWN_WAIT, 60000).

-record(state, {queue = queue:new(),
                pool :: pid(),
                monitors = [] :: list(),
                shutdown :: undefined | {pid(), reference()},
                callback_mod :: atom(),
                pool_name :: atom(),
                checkouts = 0 :: non_neg_integer()
    }).


-callback handle_work(Pid::pid(), Work::term(), From::term()) -> any().

-callback stop(Pid::pid(), Reason::term()) -> any().

-callback shutdown_pool(Pid::pid(), Wait::integer()) -> any().

-callback do_init(PoolBoyArgs::list()) -> {ok, pid()}.

-callback reply(Term::term(), Term::term()) -> any().

-callback do_work(Pid::pid(), Work::term(), From::term()) -> any().


start_link(PoolBoyArgs, CallbackMod, PoolName) ->
    gen_fsm:start_link(?MODULE, [PoolBoyArgs, CallbackMod, PoolName], []).

handle_work(Pid, Work, From) ->
    gen_fsm:send_event(Pid, {work, Work, From}).

stop(Pid, Reason) ->
    gen_fsm:sync_send_all_state_event(Pid, {stop, Reason}).

%% wait for all the workers to finish any current work
shutdown_pool(Pid, Wait) ->
    gen_fsm:sync_send_all_state_event(Pid, {shutdown, Wait}, infinity).

init([PoolBoyArgs, CallbackMod, PoolName]) ->
    {ok, Pid} = CallbackMod:do_init(PoolBoyArgs),
    {ok,
        ready,
        #state{pool=Pid, callback_mod = CallbackMod, pool_name = PoolName}}.
	
ready(_Event, _From, State) ->
    {reply, ok, ready, State}.

queueing(_Event, _From, State) ->
    {reply, ok, queueing, State}.

shutdown(_Event, _From, State) ->
    {reply, ok, shutdown, State}.

ready({work, Work, From} = Msg,
        #state{pool=Pool, queue=Q, monitors=Monitors} = State) ->
    case poolboy_checkout(Pool, State) of
        {full, State0} ->
            riak_core_stat:update({worker_pool,
                                    queue_event,
                                    State#state.pool_name}),
            {next_state, queueing, State0#state{queue=queue:in(Msg, Q)}};
        {Pid, State0} when is_pid(Pid) ->
            NewMonitors =
                riak_core_worker_pool:monitor_worker(Pid,
                                                        From,
                                                        Work,
                                                        Monitors),
            do_work(Pid, Work, From, State0),
            {next_state, ready, State0#state{monitors=NewMonitors}}
    end;
ready(_Event, State) ->
    {next_state, ready, State}.

queueing({work, _Work, _From} = Msg, #state{queue=Q} = State) ->
    {next_state, queueing, State#state{queue=queue:in(Msg, Q)}};
queueing(_Event, State) ->
    {next_state, queueing, State}.

shutdown({work, _Work, From}, State) ->
    %% tell the process requesting work that we're shutting down
    Mod = State#state.callback_mod,
    Mod:reply(From, {error, vnode_shutdown}),
    {next_state, shutdown, State};
shutdown(_Event, State) ->
    {next_state, shutdown, State}.

handle_event({checkin, Pid}, shutdown, #state{pool=Pool, monitors=Monitors0} = State) ->
    Monitors = demonitor_worker(Pid, Monitors0),
    {ok, State0} = poolboy_checkin(Pool, Pid, State),
    case Monitors of
        [] -> %% work all done, time to exit!
            {stop, shutdown, State0};
        _ ->
            {next_state, shutdown, State0#state{monitors=Monitors}}
    end;
handle_event({checkin, Worker}, _, #state{pool = Pool, queue=Q, monitors=Monitors} = State) ->
    case queue:out(Q) of
        {{value, {work, Work, From}}, Rem} ->
            %% there is outstanding work to do - instead of checking
            %% the worker back in, just hand it more work to do
            NewMonitors = monitor_worker(Worker, From, Work, Monitors),
            do_work(Worker, Work, From, State),
            {next_state,
                queueing, State#state{queue=Rem, monitors=NewMonitors}};
        {empty, Empty} ->
            NewMonitors = demonitor_worker(Worker, Monitors),
            {ok, State0} = poolboy_checkin(Pool, Worker, State),
            {next_state, ready, State0#state{queue=Empty, monitors=NewMonitors}}
    end;
handle_event(worker_start, StateName,
                #state{pool=Pool, queue=Q, monitors=Monitors}=State) ->
    %% a new worker just started - if we have work pending, try to do it
    case queue:out(Q) of
        {{value, {work, Work, From}}, Rem} ->
            case poolboy_checkout(Pool, State) of
                {full, State0} ->
                    {next_state, queueing, State0};
                {Pid, State0} when is_pid(Pid) ->
                    NewMonitors = monitor_worker(Pid, From, Work, Monitors),
                    do_work(Pid, Work, From, State0),
                    {next_state,
                        queueing,
                        State0#state{queue=Rem, monitors=NewMonitors}}
            end;
        {empty, _} ->
            {next_state,
                %% If we are in state queueing with nothing in the queue,
                %% move to the ready state so that the next incoming job
                %% checks out the new worker from poolboy.
                if StateName==queueing ->
                        ready;
                    true ->
                        StateName
                end,
                State}
    end;
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event({stop, Reason}, _From, _StateName, State) ->
    {stop, Reason, ok, State}; 
handle_sync_event({shutdown, Time}, From, _StateName, #state{queue=Q,
        monitors=Monitors} = State) ->
    discard_queued_work(Q, State#state.callback_mod),
    case Monitors of
        [] ->
            {stop, shutdown, ok, State};
        _ ->
            case Time of
                infinity ->
                    ok;
                _ when is_integer(Time) ->
                    erlang:send_after(Time, self(), shutdown),
                    ok
            end,
            {next_state, shutdown, State#state{shutdown=From, queue=queue:new()}}
    end;
handle_sync_event(_Event, _From, StateName, State) ->
    {reply, {error, unknown_message}, StateName, State}.

handle_info({'DOWN', _Ref, _, Pid, Info}, StateName, #state{monitors=Monitors} = State) ->
    %% remove the listing for the dead worker
    case lists:keyfind(Pid, 1, Monitors) of
        {Pid, _, From, Work} ->
            Mod = State#state.callback_mod,
            Mod:reply(From, {error, {worker_crash, Info, Work}}),
            NewMonitors = lists:keydelete(Pid, 1, Monitors),
            %% trigger to do more work will be 'worker_start' message
            %% when poolboy replaces this worker (if not a 'checkin'
            %% or 'handle_work')
            {next_state, StateName, State#state{monitors=NewMonitors}};
        false ->
            {next_state, StateName, State}
    end;
handle_info(shutdown, shutdown, #state{monitors=Monitors} = State) ->
    %% we've waited too long to shutdown, time to force the issue
    Mod = State#state.callback_mod,
    _ = [Mod:reply(From, {error, vnode_shutdown})
            ||  {_, _, From, _} <- Monitors],
    {stop, shutdown, State};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(shutdown, _StateName, #state{pool=Pool, queue=Q, callback_mod=Mod}) ->
    discard_queued_work(Q, Mod),
    %% stop poolboy
    gen_fsm:sync_send_all_state_event(Pool, stop),
    ok;
terminate(_Reason, _StateName, #state{pool=Pool}) ->
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

demonitor_worker(Worker, Monitors) ->
    case lists:keyfind(Worker, 1, Monitors) of
        {Worker, Ref, _From, _Work} ->
            erlang:demonitor(Ref),
            lists:keydelete(Worker, 1, Monitors);
        false ->
            %% not monitored?
            Monitors
    end.

discard_queued_work(Q, Mod) ->
    case queue:out(Q) of
        {{value, {work, _Work, From}}, Rem} ->
            Mod:reply(From, {error, vnode_shutdown}),
            discard_queued_work(Rem, Mod);
        {empty, _Empty} ->
            ok
    end.

poolboy_checkin(Pool, Worker, State) ->
    R = poolboy:checkin(Pool, Worker),
    {R, State#state{checkouts = max(State#state.checkouts - 1, 0)}}.

poolboy_checkout(Pool, State) ->
    R = poolboy:checkout(Pool, false),
    {R, State#state{checkouts = State#state.checkouts + 1}}.

do_work(Pid, Work, From, State) ->
    % We must have checked out to do some work, raise the stat as the number
    % of checkouts prior to this one
    riak_core_stat:update({worker_pool,
                            work_event,
                            State#state.pool_name,
                            max(State#state.checkouts - 1, 0)}),
    Mod = State#state.callback_mod,
    Mod:do_work(Pid, Work, From).