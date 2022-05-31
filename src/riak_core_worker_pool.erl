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

-include_lib("kernel/include/logger.hrl").

-ifdef(PULSE).
-compile(export_all).
-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module, [{gen_fsm, pulse_gen_fsm}]}).
-endif.

-define(SHUTDOWN_WAIT, 60000).
-define(LOG_LOOP_MS, 60000).

-record(state, {queue = queue:new(),
                pool :: pid(),
                monitors = [] :: list(),
                shutdown :: undefined | {pid(), reference()},
                callback_mod :: atom(),
                pool_name :: atom(),
                checkouts = [] :: list(checkout())
    }).

-type checkout() :: {pid(), erlang:timestamp()}.

-callback handle_work(Pid::pid(), Work::term(), From::term()) -> any().

-callback stop(Pid::pid(), Reason::term()) -> any().

-callback shutdown_pool(Pid::pid(), Wait::integer()) -> any().

-callback do_init(PoolBoyArgs::list()) -> {ok, pid()}.

-callback reply(Term::term(), Term::term()) -> any().

-callback do_work(Pid::pid(), Work::term(), From::term()) -> any().

-callback to_log() -> boolean().


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
    erlang:send_after(?LOG_LOOP_MS, self(), log_timer),
    {ok,
        ready,
        #state{pool=Pid,
                callback_mod = CallbackMod,
                pool_name = PoolName}}.
	
ready(_Event, _From, State) ->
    {reply, ok, ready, State}.

queueing(_Event, _From, State) ->
    {reply, ok, queueing, State}.

shutdown(_Event, _From, State) ->
    {reply, ok, shutdown, State}.

ready({work, Work, From} = Msg,
        #state{pool=Pool,
                queue=Q,
                pool_name=PoolName,
                checkouts=Checkouts0,
                monitors=Monitors0} = State) ->
    case poolboy_checkout(Pool, PoolName, Checkouts0) of
        full ->
            {next_state, queueing, State#state{queue=push_to_queue(Msg, Q)}};
        {Pid, Checkouts} when is_pid(Pid) ->
            Monitors =
                riak_core_worker_pool:monitor_worker(Pid,
                                                        From,
                                                        Work,
                                                        Monitors0),
            do_work(Pid, Work, From, State#state.callback_mod),
            {next_state,
                ready,
                State#state{monitors=Monitors, checkouts = Checkouts}}
    end;
ready(_Event, State) ->
    {next_state, ready, State}.

queueing({work, _Work, _From} = Msg, #state{queue=Q} = State) ->
    {next_state, queueing, State#state{queue=push_to_queue(Msg, Q)}};
queueing(_Event, State) ->
    {next_state, queueing, State}.

shutdown({work, _Work, From}, State) ->
    %% tell the process requesting work that we're shutting down
    Mod = State#state.callback_mod,
    Mod:reply(From, {error, vnode_shutdown}),
    {next_state, shutdown, State};
shutdown(_Event, State) ->
    {next_state, shutdown, State}.

handle_event({checkin, Pid},
                shutdown,
                #state{pool=Pool,
                        pool_name=PoolName,
                        monitors=Monitors0,
                        checkouts=Checkouts0} = State) ->
    Monitors = demonitor_worker(Pid, Monitors0),
    {ok, Checkouts} =
        poolboy_checkin(Pool, Pid, PoolName, Checkouts0),
    case Monitors of
        [] -> %% work all done, time to exit!
            {stop, shutdown, State};
        _ ->
            {next_state,
                shutdown,
                State#state{monitors=Monitors, checkouts=Checkouts}}
    end;
handle_event({checkin, Worker},
                _,
                #state{pool=Pool,
                        queue=Q,
                        pool_name=PoolName,
                        checkouts=Checkouts0,
                        monitors=Monitors0} = State) ->
    case consume_from_queue(Q, State#state.pool_name) of
        {{value, {work, Work, From}}, Rem} ->
            %% there is outstanding work to do - instead of checking
            %% the worker back in, just hand it more work to do
            Monitors = monitor_worker(Worker, From, Work, Monitors0),
            do_work(Worker, Work, From, State#state.callback_mod),
            {next_state,
                queueing, State#state{queue=Rem, monitors=Monitors}};
        {empty, Empty} ->
            Monitors = demonitor_worker(Worker, Monitors0),
            {ok, Checkouts} =
                poolboy_checkin(Pool, Worker, PoolName, Checkouts0),
            {next_state,
                ready,
                State#state{queue=Empty,
                            monitors=Monitors,
                            checkouts=Checkouts}}
    end;
handle_event(worker_start, StateName,
                #state{pool=Pool,
                        queue=Q,
                        pool_name=PoolName,
                        checkouts=Checkouts0,
                        monitors=Monitors0}=State) ->
    %% a new worker just started - if we have work pending, try to do it
    case consume_from_queue(Q, State#state.pool_name) of
        {{value, {work, Work, From}}, Rem} ->
            case poolboy_checkout(Pool, PoolName, Checkouts0) of
                full ->
                    {next_state, queueing, State};
                {Pid, Checkouts} when is_pid(Pid) ->
                    Monitors = monitor_worker(Pid, From, Work, Monitors0),
                    do_work(Pid, Work, From, State#state.callback_mod),
                    {next_state,
                        queueing,
                        State#state{queue=Rem,
                                        monitors=Monitors,
                                        checkouts=Checkouts}}
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

handle_info(log_timer, StateName, State) ->
    Mod = State#state.callback_mod,
    case {Mod:to_log(), State#state.checkouts} of
        {true, Checkouts} when length(Checkouts) > 0 ->
            [{_P, LastChOutTime}|_Rest] =
                lists:reverse(lists:keysort(2, State#state.checkouts)),
            LastCheckout =
                timer:now_diff(os:timestamp(), LastChOutTime),
            QL = queue:len(State#state.queue),
            _ = 
                lager:info(
                    "worker_pool=~w has qlen=~w with last_checkout=~w s ago",
                    [State#state.pool_name,
                        QL,
                        LastCheckout  div (1000 * 1000)]),
            ok;
        {true, []} ->
            _ =
                lager:info(
                    "worker_pool=~w has qlen=0 and no items checked out",
                    [State#state.pool_name]);
        _ ->
            ok
    end,
    NextLogTimer = ?LOG_LOOP_MS - rand:uniform(max(?LOG_LOOP_MS div 10, 1)),
    erlang:send_after(NextLogTimer, self(), log_timer),
    {next_state, StateName, State};
handle_info({'DOWN', _Ref, _, Pid, Info},
                StateName,
                #state{monitors=Monitors0} = State) ->
    %% remove the listing for the dead worker
    case lists:keyfind(Pid, 1, Monitors0) of
        {Pid, _, From, Work} ->
            Mod = State#state.callback_mod,
            Mod:reply(From, {error, {worker_crash, Info, Work}}),
            Monitors = lists:keydelete(Pid, 1, Monitors0),
            %% trigger to do more work will be 'worker_start' message
            %% when poolboy replaces this worker (if not a 'checkin'
            %% or 'handle_work')
            {next_state, StateName, State#state{monitors=Monitors}};
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
        {{value, {_QT, {work, _Work, From}}}, Rem} ->
            Mod:reply(From, {error, vnode_shutdown}),
            discard_queued_work(Rem, Mod);
        {empty, _Empty} ->
            ok
    end.

-spec poolboy_checkin(pid(), pid(), atom(), list(checkout()))
                                                -> {ok, list(checkout())}.
poolboy_checkin(Pool, Worker, PoolName, Checkouts) ->
    R = poolboy:checkin(Pool, Worker),
    case lists:keytake(Worker, 1, Checkouts) of
        {value, {Worker, WT}, Checkouts0} ->
            riak_core_stat:update({worker_pool,
                                    work_time,
                                    PoolName,
                                    timer:now_diff(os:timestamp(), WT)}),
            {R, Checkouts0};
        _ ->
            ?LOG_WARNING(
                "Unexplained poolboy behaviour - failure to track checkouts"),
            {R, Checkouts}
    end.

-spec poolboy_checkout(pid(), atom(), list(checkout()))
                                        -> full | {pid(), list(checkout())}.
poolboy_checkout(Pool, PoolName, Checkouts) ->
    case poolboy:checkout(Pool, false) of
        full ->
            full;
        P when is_pid(P) ->
            riak_core_stat:update({worker_pool,
                                    queue_time,
                                    PoolName,
                                    0}),
            {P, [{P, os:timestamp()}|Checkouts]}
    end.

do_work(Pid, Work, From, Mod) ->
    Mod:do_work(Pid, Work, From).

-spec push_to_queue({work, term(), term()}, queue:queue()) -> queue:queue().
push_to_queue(Msg, Q) ->
    QT = os:timestamp(),
    queue:in({QT, Msg}, Q).

-spec consume_from_queue(queue:queue(), atom()) ->
                            {empty | {value, {work, term(), term()}}, 
                                queue:queue()}.
consume_from_queue(Q, PoolName) ->
    case queue:out(Q) of
        {empty, Empty} ->
            {empty, Empty};
        {{value, {QT, {work, Work, From}}}, Rem} ->
            riak_core_stat:update({worker_pool,
                                    queue_time,
                                    PoolName,
                                    timer:now_diff(os:timestamp(), QT)}),
            {{value, {work, Work, From}}, Rem}
    end.
