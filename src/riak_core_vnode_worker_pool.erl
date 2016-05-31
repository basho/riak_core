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
-module(riak_core_vnode_worker_pool).

-behaviour(gen_fsm).

%% gen_fsm callbacks
-export([
    init/1,
    handle_event/3,
    handle_sync_event/4,
    handle_info/3,
    terminate/3,
    code_change/4
]).

%% gen_fsm states
-export([
    ready/2, ready/3,
    queueing/2, queueing/3,
    shutdown/2, shutdown/3
]).

%% API
-export([
    handle_work/2, handle_work/3,
    start_link/5,
    stats/1,
    stop/2,
    shutdown_pool/2
]).

-include("riak_core_vnode.hrl").

%% If not otherwise set, the maximum work queue length is the pool size times
%% this multiplier.
-define(MAX_QUEUE_DEFAULT_MULT, 3).

%% If defined, the State is validated before return whenever it's changed.
%% The validation code is conditional on whether this macro is defined, not
%% its value.
%% This is fairly heavyweight, and should NOT be defined in production builds.
-define(VALIDATE_STATE, true).

-ifdef(PULSE).
-compile(export_all).
-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module, [{gen_fsm, pulse_gen_fsm}]}).
-endif.

%% In case we want to change the type of dict used for stats. Unlikely, but ...
-define(StatsDict,  dict).

-ifdef(namespaced_types).
-type work_stats()  :: ?StatsDict:?StatsDict().
-else.
-type work_stats()  :: ?StatsDict().
-endif.
-define(inc_stat(Stat, Stats),  ?StatsDict:update_counter(Stat, 1, Stats)).
-define(stats_list(Stats),      ?StatsDict:to_list(Stats)).

-type fsm_state()   :: 'queueing' | 'ready' | 'shutdown'.

%% discriminate pid types just for clarity
-type pool_pid()    :: pid().
-type worker_pid()  :: pid().

-type incoming()    ::  riak_core_job:job()
                    |   riak_core_job:work()
                    |   riak_core_job:legacy().

-type mon_rec()     :: {worker_pid(), reference(), riak_core_job:job()}.
-type que_rec()     :: riak_core_job:job().
%% MUST keep queue/quelen and monitors/running in sync!
%% They're separate to allow easy decisions based on active/queued work,
%% but obviously require close attention.
-record(state, {
    pool                    :: pool_pid(),
    queue     = []          :: [que_rec()],
    quelen    = 0           :: non_neg_integer(),
    maxque    = 0           :: non_neg_integer(),
    running   = 0           :: non_neg_integer(),
    monitors  = []          :: [mon_rec()],
    supports                :: false | {module(), [{atom(), boolean()}]},
    shutdown  = undefined   :: undefined | sender() | {sender(), reference()},
    stats = ?StatsDict:new():: work_stats()
}).

-define(shutdown_queued_jobs(Js), lists:foreach(fun shutdown_queued_job/1, Js)).

-ifdef(VALIDATE_STATE).
-define(validate(State),  validate(State)).
-else.
-define(validate(State),  State).
-endif.

%% TODO: switch this to use a proplist so knobs can be adjusted
%% At present, only PoolSize is externally adjustable.
%% We'd like to at least be able to adjust the maximum queue depth.
%% Unclear whether we want to filter work classes here or farther up the
%% stack, though here is where we have visibility into what's actually running.
start_link(WorkerMod, PoolSize, VNodeIndex, WorkerArgs, WorkerProps) ->
    gen_fsm:start_link(?MODULE,
        [WorkerMod, PoolSize,  VNodeIndex, WorkerArgs, WorkerProps], []).

handle_work(Pid, ?VNODE_JOB{} = Job) ->
    gen_fsm:send_event(Pid, Job).

handle_work(Pid, Work, From) ->
    gen_fsm:send_event(Pid, {work, Work, From}).

-spec stats(pool_pid()) -> {stats, [{atom(), term()}]}.
stats(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, stats).

stop(Pid, Reason) ->
    gen_fsm:sync_send_all_state_event(Pid, {stop, Reason}).

%% wait for all the workers to finish any current work
shutdown_pool(Pid, Wait) ->
    gen_fsm:sync_send_all_state_event(Pid, {shutdown, Wait}, infinity).

init([WorkerMod, PoolSize, VNodeIndex, WorkerArgs, WorkerProps]) ->
    {ok, Pool} = poolboy:start_link([
        {worker_module, riak_core_vnode_worker},
        {worker_args, [VNodeIndex, WorkerArgs, WorkerProps, self()]},
        {worker_callback_mod, WorkerMod},
        {size, PoolSize},
        {max_overflow, 0}
    ]),
    MaxQ = (PoolSize * ?MAX_QUEUE_DEFAULT_MULT),
    Sup = case lists:member({worker_supports, 1}, WorkerMod:module_info(exports)) of
        true ->
            {WorkerMod, []};
        _ ->
            false
    end,
    %% initialize the stats with some constant values
    Stats = ?StatsDict:store(vnode_index, VNodeIndex,
            ?StatsDict:store(pool_size,   PoolSize,
            ?StatsDict:store(work_module, WorkerMod,
            ?StatsDict:new()))),
    {ok, ready, ?validate(#state{
        pool = Pool, maxque = MaxQ, supports = Sup, stats = Stats})}.

%% ===================================================================
%% Handle incoming work
%% ===================================================================

ready(_Event, _From, State) ->
    {reply, ok, ready, State}.

ready(?VNODE_JOB{} = Work, State) ->
    incoming_work(ready, Work, State);
ready({work, _, _} = Work, State) ->
    incoming_work(ready, Work, State);

ready(Event, State) ->
    lager:debug("Unrecognized ~s state event: ~p", [ready, Event]),
    {next_state, ready, State}.

queueing(_Event, _From, State) ->
    {reply, ok, queueing, State}.

queueing(?VNODE_JOB{} = Work, State) ->
    incoming_work(queueing, Work, State);
queueing({work, _, _} = Work, State) ->
    incoming_work(queueing, Work, State);

queueing(Event, State) ->
    lager:debug("Unrecognized ~s state event: ~p", [queueing, Event]),
    {next_state, queueing, State}.

shutdown(_Event, _From, State) ->
    {reply, ok, shutdown, State}.

shutdown(?VNODE_JOB{from = From}, State) ->
    riak_core_vnode:reply(From, {error, vnode_shutdown}),
    {next_state, shutdown, State};
shutdown({work, _, From}, State) ->
    riak_core_vnode:reply(From, {error, vnode_shutdown}),
    {next_state, shutdown, State};

shutdown(Event, State) ->
    lager:debug("Unrecognized ~s state event: ~p", [shutdown, Event]),
    {next_state, shutdown, State}.

%%
%% ALL incoming work in ALL states comes through here for application of load
%% handling rules. We may end up with a LOT of heads for this function.
%%
%% Although at present the 'shutdown' state always rejects work, it's
%% conceivable that there could arise a case where we want to be able to
%% insert work even then, so it's redirected through here too.
%%
-spec incoming_work(fsm_state(), incoming(), #state{})
        -> {next_state, fsm_state(), #state{}}.

incoming_work(shutdown = StateName, Work, State) ->
    %% tell the process requesting work that we're shutting down
    riak_core_vnode:reply(
        riak_core_job:work_from(Work), {error, vnode_shutdown}),
    {next_state, StateName, State};

incoming_work(StateName, Work, #state{quelen = MaxQ, maxque = MaxQ} = State) ->
    Err = worker_queue_full,
    lager:notice("Job ~p rejected: ~p", [riak_core_job:work_label(Work), Err]),
    riak_core_vnode:reply(riak_core_job:work_from(Work), {error, Err}),
    {next_state, StateName, State};

incoming_work(StateName, Work, #state{quelen = QL, maxque = QM} = State)
        when QL > QM ->
    %% Programming error!
    lager:error("Inconsistent state, oversized queue ~b/~b", [QL, QM]),
    Err = worker_queue_full,
    lager:notice("Job ~p rejected: ~p", [riak_core_job:work_label(Work), Err]),
    riak_core_vnode:reply(riak_core_job:work_from(Work), {error, Err}),
    {next_state, StateName, State};

incoming_work(StateName, {work, Work, From}, State) ->
    incoming_work(StateName, riak_core_job:new([
        {work, Work}, {from, From}, {module, ?MODULE}]), State);

incoming_work(queueing, ?VNODE_JOB{} = Job, State) ->
    queue_job(Job, State);

incoming_work(_, ?VNODE_JOB{} = Job, #state{pool = Pool} = State) ->
    % nonblocking checkout can only return 'full' or pid()
    case poolboy:checkout(Pool, false) of
        full ->
            queue_job(Job, State);
        Worker ->
            dispatch_job(Worker, ready, Job, State)
    end;

incoming_work(StateName, Work, State) ->
    lager:error("Unrecognized ~s state work: ~p", [StateName, Work]),
    {next_state, StateName, State}.

%% ===================================================================
%% Handle worker becoming available
%% ===================================================================

handle_event(worker_start, shutdown, State) ->
    {next_state, shutdown, State};

%% This should be what we see when the last worker finishes ...
handle_event({checkin, Worker}, shutdown,
        #state{pool = Pool, monitors = [{Worker, _, _}]} = State) ->
    poolboy:checkin(Pool, Worker),
    {stop, shutdown, demonitor_worker(Worker, State)};

%% ... and these are states we shouldn't ever see, so make some noise
handle_event({checkin, Worker}, shutdown,
        #state{pool = Pool, running = Running} = StateIn) when Running =< 1 ->
    lager:error("Inconsistent shutdown checkin: ~p ~p", [Worker, StateIn]),
    poolboy:checkin(Pool, Worker),
    State = demonitor_worker(Worker, StateIn),
    case State of
        #state{running = 0} ->
            {stop, shutdown, State};
        _ ->
            {next_state, shutdown, State}
    end;

%% More work to finish, keep going.
handle_event({checkin, Worker}, shutdown, #state{pool = Pool} = State) ->
    poolboy:checkin(Pool, Worker),
    {next_state, shutdown, demonitor_worker(Worker, State)};

%% We should never be in 'queueing' state with an empty queue, or 'ready' state
%% with a non-empty queue.
%% These duplicate the more general patterns below, but also log an error.
%% They may or may not be considered debugging artifacts, and can be commented
%% out without altering the overall behavior, but they're indicative of a
%% programming mistake somewhere if they ever do get invoked, so I'm leaving
%% them in.

handle_event(worker_start, queueing, #state{quelen = 0} = State) ->
    lager:error("Invalid 'queueing' state with empty queue"),
    {next_state, ready, State};

handle_event({checkin, Worker}, queueing,
        #state{pool = Pool, quelen = 0} = State) ->
    lager:error("Invalid 'queueing' state with empty queue"),
    poolboy:checkin(Pool, Worker),
    {next_state, ready, demonitor_worker(Worker, State)};

handle_event(worker_start, ready, #state{quelen = Len} = State) when Len > 0 ->
    lager:error("Invalid 'ready' state with ~b queued jobs", [Len]),
    {next_state, queueing, State};

handle_event({checkin, Worker}, ready,
        #state{pool = Pool, quelen = Len} = State) when Len > 0 ->
    lager:error("Invalid 'ready' state with ~b queued jobs", [Len]),
    poolboy:checkin(Pool, Worker),
    {next_state, queueing, demonitor_worker(Worker, State)};

%% End of the inconsistent state traps.
%% Below here are states we should expect to see in normal operation.

handle_event({checkin, Worker}, _, #state{queue = [Job]} = State) ->
    dispatch_job(Worker, ready, Job,
        ?validate(State#state{queue = [], quelen = 0}));

handle_event({checkin, Worker}, _,
        #state{queue = [Job | Jobs], quelen = Len} = State) ->
    dispatch_job(Worker, queueing, Job,
        ?validate(State#state{queue = Jobs, quelen = (Len - 1)}));

handle_event(worker_start, _, #state{quelen = 0} = State) ->
    {next_state, ready, State};

handle_event(worker_start, _, #state{pool = Pool, queue = [Job]} = State) ->
    % nonblocking checkout can only return 'full' or pid()
    case poolboy:checkout(Pool, false) of
        full ->
            {next_state, queueing, State};
        Worker ->
            dispatch_job(Worker, ready, Job,
                ?validate(State#state{queue = [], quelen = 0}))
    end;

handle_event(worker_start, _,
        #state{pool = Pool, queue = [Job | Jobs], quelen = Len} = State) ->
    % nonblocking checkout can only return 'full' or pid()
    case poolboy:checkout(Pool, false) of
        full ->
            {next_state, queueing, State};
        Worker ->
            dispatch_job(Worker, queueing, Job,
                ?validate(State#state{queue = Jobs, quelen = (Len - 1)}))
    end;

handle_event(Event, StateName, State) ->
    lager:debug("Unrecognized ~s state event: ~p", [StateName, Event]),
    {next_state, StateName, State}.

%% ===================================================================
%% Handle being told to stop, in various forms
%% ===================================================================

handle_sync_event({stop, Reason}, _From, _StateName, State) ->
    {stop, Reason, ok, State};

%% Already shut down, easy to recognize and skip
handle_sync_event({shutdown, _}, _From, _StateName,
        #state{running = 0, shutdown = Shutdown} = State)
        when Shutdown /= undefined ->
    {stop, shutdown, ok, State};

handle_sync_event({shutdown, _}, From, _StateName,
        #state{running = 0, queue = Que, shutdown = undefined} = State) ->
    ?shutdown_queued_jobs(Que),
    {stop, shutdown, ok,
        ?validate(State#state{queue = [], quelen = 0, shutdown = From})};

handle_sync_event({shutdown, infinity}, From, _StateName,
    #state{running = 0, queue = Que, shutdown = undefined} = State) ->
    ?shutdown_queued_jobs(Que),
    {stop, shutdown, ok,
        ?validate(State#state{queue = [], quelen = 0, shutdown = From})};

handle_sync_event({shutdown, infinity}, From, _StateName,
        #state{queue = Que} = State) ->
    ?shutdown_queued_jobs(Que),
    {next_state, shutdown,
        ?validate(State#state{queue = [], quelen = 0, shutdown = From})};

handle_sync_event({shutdown, Time}, From, _StateName,
        #state{queue = Que} = State) when erlang:is_integer(Time) ->
    ?shutdown_queued_jobs(Que),
    Ref = erlang:send_after(Time, erlang:self(), shutdown),
    {next_state, shutdown,
        ?validate(State#state{queue = [], quelen = 0, shutdown = {From, Ref}})};

handle_sync_event({shutdown, _} = Event, _From, _StateName, _State) ->
    erlang:error(badarg, [Event]);

handle_sync_event(stats, _, StateName, State) ->
    Stats = report_stats(StateName, State),
    {reply, {stats, lists:sort(Stats)}, StateName, State};

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, {error, unknown_message}, StateName, State}.

%% ===================================================================
%% Handle worker death
%% ===================================================================

handle_info({'DOWN', _Ref, _, Worker, Info}, StateName,
        #state{monitors = Mons, running = Run} = State) ->
    %% Remove the worker from our state and notify the originator that it died.
    %% Everything should recover as poolboy replaces the dead worker.
    case lists:keytake(Worker, 1, Mons) of
        {value, {_, _, ?VNODE_JOB{from = From, work = Work}}, NewMons} ->
            riak_core_vnode:reply(From, {error, {worker_crash, Info, Work}}),
            {next_state, StateName,
                ?validate(State#state{monitors = NewMons, running = (Run - 1)})};
        false ->
            lager:debug("Unmonitored worker ~p died ~p", [Worker, Info]),
            {next_state, StateName, State}
    end;

handle_info(shutdown, shutdown, #state{running = 0} = State) ->
    %% Shouldn't happen, but complain quietly in case anyone cares.
    lager:debug("Shutdown delayed with no running jobs"),
    {stop, shutdown, State};

handle_info(shutdown, shutdown, #state{monitors = Mons} = State) ->
    %% We've waited too long to shut down, time to force the issue.
    %% poolboy will kill the job workers, but it's possible a 'killed' callback
    %% could be invoked for a job that finishes just before its worker is killed.
    lists:foreach(fun shutdown_monitored_job/1, Mons),
    {stop, shutdown, ?validate(State#state{monitors = [], running = 0})};

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, #state{pool = Pool}) ->
    %% stop poolboy
    gen_fsm:sync_send_all_state_event(Pool, stop),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% ===================================================================
%% Internal
%% ===================================================================

-spec queue_job(riak_core_job:job(), #state{})
        -> {next_state, queueing, #state{}}.
queue_job(Job, #state{queue = Que, quelen = Len} = State) ->
    {next_state, queueing, ?validate(State#state{
        queue = Que ++ [riak_core_job:update(Job, queue)], quelen = (Len + 1)})}.

-spec dispatch_job(worker_pid(), fsm_state(), riak_core_job:job(), #state{})
        -> {next_state, fsm_state(), #state{}}.
dispatch_job(Worker, NextState, Job0, State0) ->
    %% Extract the record type element directly from the tuple so we don't
    %% have to worry about getting out of sync with the pattern if job record
    %% types are added.
    Job1 = riak_core_job:update(Job0, start),
    {Work, #state{monitors = Mon0, running = Run0} = State1} =
        case worker_supports(erlang:element(1, Job1), State0) of
            {true, NewState} ->
                {Job1, NewState};
            {_, NewState} ->
                {riak_core_job:get(work, Job1), NewState}
        end,
    {Mon1, Run1} =
        case lists:keyfind(Worker, 1, Mon0) of
            {Worker, Ref, _OldJob} ->
                %% reuse old monitor and just update the job
                {lists:keyreplace(Worker, 1, Mon0, {Worker, Ref, Job1}), Run0};
            false ->
                Ref = erlang:monitor(process, Worker),
                {[{Worker, Ref, Job1} | Mon0], (Run0 + 1)}
        end,
    riak_core_vnode_worker:handle_work(
        Worker, Work, riak_core_job:get(from, Job1)),
    {next_state, NextState,
        ?validate(State1#state{monitors = Mon1, running = Run1})}.

-spec demonitor_worker(worker_pid(), #state{}) -> #state{}.
demonitor_worker(Worker, #state{monitors = Mons, running = Run} = State) ->
    case lists:keytake(Worker, 1, Mons) of
        {value, {_Worker, Ref, _Job}, NewMons} ->
            erlang:demonitor(Ref),
            ?validate(State#state{monitors = NewMons, running = (Run - 1)});
        false ->
            lager:notice("Worker ~p not present in monitors ~p", [Worker, Mons]),
            State
    end.

-spec shutdown_queued_job(riak_core_job:job()) -> term().
shutdown_queued_job(Job) ->
    riak_core_vnode:reply(riak_core_job:get(from, Job), {error, vnode_shutdown}).

-spec shutdown_monitored_job(mon_rec()) -> any().
shutdown_monitored_job({_, Ref, ?VNODE_JOB{from = From, kill_cb = KCB}}) ->
    erlang:demonitor(Ref),
    catch invoke_callback(KCB),
    riak_core_vnode:reply(From, {error, vnode_shutdown}).

-spec invoke_callback(riak_core_job:fun_rec() | riak_core_job:mfa_rec())
        -> any() | no_return().
invoke_callback({Fun, Args}) ->
    erlang:apply(Fun, Args);
invoke_callback({Mod, Func, Args}) ->
    erlang:apply(Mod, Func, Args);
invoke_callback(_) ->
    ok.

-spec worker_supports(atom(), #state{}) -> {boolean(), #state{}}.
worker_supports(JobType, #state{supports = {Mod, SupIn}} = State) ->
    case proplists:get_value(JobType, SupIn) of
        undefined ->
            Val = Mod:worker_supports(JobType),
            Sup = [{JobType, Val} | SupIn],
            {Val, State#state{supports = {Mod, Sup}}};
        Bool ->
            {Bool, State}
    end;
worker_supports(_, State) ->
    {false, State}.

-spec report_stats(fsm_state(), #state{}) -> [{atom(), non_neg_integer()}].
report_stats(StateName, #state{stats = Stats, shutdown = Shutdown} = State) ->
    Result = [
        {state,   StateName},
        {queued,  State#state.quelen},
        {maxque,  State#state.maxque},
        {running, State#state.running}
        | ?stats_list(Stats)],
    case Shutdown of
        undefined ->
            Result;
        Val ->
            [{shutdown, Val} | Result]
    end.

-ifdef(VALIDATE_STATE).
-spec validate(#state{}) -> #state{} | no_return().
validate(State) ->
    QL = erlang:length(State#state.queue),
    R1 = case QL /= State#state.quelen of
        true ->
            [{quelen, QL, State#state.quelen}];
        _ ->
            []
    end,
    R2 = case QL > State#state.maxque of
        true ->
            [{maxque, QL, State#state.maxque} | R1];
        _ ->
            R1
    end,
    ML = erlang:length(State#state.monitors),
    R3 = case ML /= State#state.running of
        true ->
            [{running, ML, State#state.running} | R2];
        _ ->
            R2
    end,
    case R3 of
        [] ->
            State;
        Err ->
            lager:error("Inconsistent state: ~p", State),
            erlang:error({invalid_state, Err}, [State])
    end.
-endif.


