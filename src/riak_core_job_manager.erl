%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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

%%
%% @doc Public Job Management API.
%%
%% Configuration:
%%
%%  Calculated :: {Val :: 'concur' | 'cores' | 'scheds', Mult :: pos_integer()}.
%%  where Val represents a derived value:
%%
%%      `concur' -  The effective value of ?JOB_SVC_CONCUR_LIMIT. If it has
%%                  not yet been evaluated, the result is calculated as if
%%                  Val == `scheds'.
%%
%%      `cores'  -  The value of erlang:system_info('logical_processors').
%%                  If the system returns anything other than a pos_integer(),
%%                  the result is calculated as if Val == `scheds'.
%%
%%      `scheds' -  The value of erlang:system_info('schedulers'). This value
%%                  is always a pos_integer(), so there's no fallback.
%%
%% Calculated values are determined at initialization, they ARE NOT dynamic!
%%
%% Application Keys:
%%
%% Note that keys are scoped to the application that started the Jobs
%% components, they are NOT explicitly scoped to 'riak_core', though that's
%% their expected use case and the default if no application is defined.
%%
%%  {?JOB_SVC_CONCUR_LIMIT, pos_integer() | Calculated}
%%      Maximum number of jobs to execute concurrently.
%%      Note that if this is initialized as {'concur', Mult} then the result
%%      is calculated as {'scheds', Mult}.
%%      Default: ?JOB_SVC_DEFAULT_CONCUR.
%%
%%  {?JOB_SVC_QUEUE_LIMIT, non_neg_integer() | Calculated}
%%      Maximum number of jobs to queue for future execution.
%%      Default: ?JOB_SVC_DEFAULT_QUEUE.
%%
%%  {?JOB_SVC_HIST_LIMIT, non_neg_integer() | Calculated}
%%      Maximum number of completed jobs' histories to maintain.
%%      Default: ?JOB_SVC_DEFAULT_HIST.
%%
%%  {?JOB_SVC_IDLE_MIN, non_neg_integer() | Calculated}
%%      Minimum number of idle runner processes to keep available.
%%      Idle processes are added opportunistically; the actual count at any
%%      given instant can be lower.
%%      Default: max(({'concur', 1} div 8), 3).
%%
%%  {?JOB_SVC_IDLE_MAX, non_neg_integer() | Calculated}
%%      Maximum number of idle runner processes to keep available.
%%      Idle processes are culled opportunistically; the actual count at any
%%      given instant can be higher.If the specified value resolves to
%%      < ?JOB_SVC_IDLE_MIN, then (?JOB_SVC_IDLE_MIN * 2) is used.
%%      Default: max((?JOB_SVC_IDLE_MIN * 2), ({'scheds', 1} - 1)).
%%
-module(riak_core_job_manager).
-behaviour(gen_server).

% Public API
-export([
    cancel/2,
    config/0,
    find/1,
    stats/0,
    stats/1,
    submit/1
]).

% Public Types
-export_type([
    cfg_concur_max/0,
    cfg_hist_max/0,
    cfg_queue_max/0,
    cfg_mult/0,
    cfg_prop/0,
    config/0,
    jid/0,
    job/0,
    stat/0,
    stat_key/0,
    stat_val/0
]).

% Private API
-export([
    cleanup/2,
    finished/3,
    running/2,
    starting/2
]).

% Gen_server API
-export([
    code_change/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1,
    start_link/0,
    terminate/2
]).

-include("riak_core_job_internal.hrl").

%% ===================================================================
%% Types
%% ===================================================================

-define(StatsDict,  orddict).
-type stats()   ::  orddict_t(stat_key(), stat_val()).

% MUST keep hist/hcnt, que/qcnt and run/rcnt in sync!
% They're separate to allow easy decisions based on active/queued work,
% but obviously require close attention.
-record(state, {
    rmax                            ::  pos_integer(),
    qmax                            ::  non_neg_integer(),
    hmax                            ::  non_neg_integer(),
    rcnt        = 0                 ::  non_neg_integer(),
    qcnt        = 0                 ::  non_neg_integer(),
    hcnt        = 0                 ::  non_neg_integer(),
    pending     = 'false'           ::  boolean(),
    shutdown    = 'false'           ::  boolean(),
    loc         = dict:new()        ::  locs(), % jid() => jloc()
    run         = dict:new()        ::  mons(), % rkey() => mon()
    que         = queue:new()       ::  jque(), % jrec()
    hist        = queue:new()       ::  jque(), % jrec()
    stats       = ?StatsDict:new()  ::  stats() % stat_key() => stat_val()
}).

-type cfg_concur_max()  ::  {?JOB_SVC_CONCUR_LIMIT, pos_integer() | cfg_mult()}.
-type cfg_hist_max()    ::  {?JOB_SVC_HIST_LIMIT, non_neg_integer() | cfg_mult()}.
-type cfg_queue_max()   ::  {?JOB_SVC_QUEUE_LIMIT, non_neg_integer() | cfg_mult()}.
-type cfg_mult()        ::  riak_core_job_service:cfg_mult().
-type cfg_prop()        ::  cfg_concur_max() | cfg_hist_max() | cfg_queue_max()
                        |   riak_core_job_service:cfg_prop().

-type config()      ::  [cfg_prop()].
-type jid()         ::  riak_core_job:gid().
-type jloc()        ::  'queued' | 'history' | rkey().
-type job()         ::  riak_core_job:job().
-type jque()        ::  queue_t(jrec()).
-type jrec()        ::  {jid(), job()}.
-type locs()        ::  dict_t(jid(), jloc()).
-type mon()         ::  {runner(), job()}.
-type mons()        ::  dict_t(rkey(), mon()).
-type rkey()        ::  reference().
-type runner()      ::  riak_core_job_service:runner().
-type service()     ::  atom() | pid().
-type stat()        ::  {stat_key(), stat_val()}.
-type stat_key()    ::  atom() | tuple().
-type stat_val()    ::  term().
-type state()       ::  #state{}.

%
% Implementation Notes:
%
%   Where feasible, decisions based on State are made with distinct function
%   heads for efficiency and clarity.
%
%   Also for clarity, only State elements used for pattern matching are bound
%   in function heads ... most of the time.
%
%   Operations that have to find (or worse, remove) Jobs that are in a state
%   other than 'running' are inefficient.
%   This is a deliberate trade-off to make advancing jobs through the
%   'queued' -> 'running' -> 'history' states efficient.
%

%% ===================================================================
%% Public API
%% ===================================================================

-spec cancel(JobOrId :: job() | jid(), Kill :: boolean())
        ->  {'ok', 'killed' | 'canceled' | 'history' | 'running', job()}
            | 'false' | {'error', term()}.
%%
%% @doc Cancel, or optionally kill, the specified Job.
%%
%% If Kill is `true' the job will de-queued or killed, as appropriate.
%% If Kill is `false' the job will only be de-queued.
%%
%% Returns:
%%
%%  {`ok', `running', Job}
%%      Only returned when Kill is `false', indicating that the Job is
%%      currently active (and remains so).
%%
%%  {`ok', Status, Job}
%%      Status indicates whether the Job was `killed', `canceled' (de-queued),
%%      or had recently `finished' running.
%%      Note that what constitutes "recently finished" is subject to load and
%%      history configuration.
%%      Job is the fully-updated instance of the Job.
%%
%%  `false'
%%      The Job is not (or no longer) known to the Manager.
%%
cancel(JobOrId, Kill) ->
    JobId = case riak_core_job:version(JobOrId) of
        {'job', _} ->
            riak_core_job:gid(JobOrId);
        _ ->
            JobOrId
    end,
    gen_server:call(?JOBS_MGR_NAME, {'cancel', JobId, Kill}).

-spec find(JobOrId :: job() | jid())
        -> {'ok', job()} | 'false' | {'error', term()}.
%%
%% @doc Return the latest instance of the specified Job.
%%
%% `false' is returned if the Job is not (or no longer) known to the Manager.
%%
find(JobOrId) ->
    JobId = case riak_core_job:version(JobOrId) of
        {'job', _} ->
            riak_core_job:gid(JobOrId);
        _ ->
            JobOrId
    end,
    gen_server:call(?JOBS_MGR_NAME, {'find', JobId}).

-spec config() -> config() | {'error', term()}.
%%
%% @doc Return the current effective Jobs Management configuration.
%%
config() ->
    gen_server:call(?JOBS_MGR_NAME, 'config').

-spec stats() -> [stat()] | {'error', term()}.
%%
%% @doc Return statistics from the Jobs Management system.
%%
stats() ->
    gen_server:call(?JOBS_MGR_NAME, 'stats').

-spec stats(JobOrId :: job() | jid()) -> [stat()] | 'false' | {'error', term()}.
%%
%% @doc Return statistics from the specified Job.
%%
%% `false' is returned if the Job is not (or no longer) known to the Manager.
%%
stats(JobOrId) ->
    JobId = case riak_core_job:version(JobOrId) of
        {'job', _} ->
            riak_core_job:gid(JobOrId);
        _ ->
            JobOrId
    end,
    gen_server:call(?JOBS_MGR_NAME, {'stats', JobId}).

-spec submit(Job :: job()) -> 'ok' | {'error', term()}.
%%
%% @doc Submit a job to run on the specified scope(s).
%%
submit(Job) ->
    case riak_core_job:version(Job) of
        {'job', _} ->
            gen_server:call(?JOBS_MGR_NAME, {'submit', Job});
        _ ->
            erlang:error('badarg', [Job])
    end.

%% ===================================================================
%% Private API
%% ===================================================================

-spec cleanup(Manager :: service(), Ref :: rkey()) -> 'ok'.
%%
%% @doc Callback for the job runner indicating the UOW is being cleaned up.
%%
cleanup(Manager, Ref) ->
    update_job(Manager, Ref, 'cleanup').

-spec finished(Manager :: service(), Ref :: rkey(), Result :: term()) -> 'ok'.
%%
%% @doc Callback for the job runner indicating the UOW is finished.
%%
finished(Manager, Ref, Result) ->
    update_job(Manager, Ref, 'finished', Result).

-spec running(Manager :: service(), Ref :: rkey()) -> 'ok'.
%%
%% @doc Callback for the job runner indicating the UOW is being run.
%%
running(Manager, Ref) ->
    update_job(Manager, Ref, 'running').

-spec starting(Manager :: service(), Ref :: rkey()) -> 'ok'.
%%
%% @doc Callback for the job runner indicating the UOW is being started.
%%
starting(Manager, Ref) ->
    update_job(Manager, Ref, 'started').

%% ===================================================================
%% Gen_server API
%% ===================================================================

-spec code_change(OldVsn :: term(), State :: state(), Extra :: term())
        -> {'ok', state()}.
%
% we don't care, just carry on
%
code_change(_, State, _) ->
    {'ok', State}.

-spec handle_call(Msg :: term(), From :: {pid(), term()}, State :: state())
        -> {'reply', term(), state()} | {'stop', term(), term(), state()}.
%
% config() -> config() | {'error', term()}.
%
handle_call('config', _, State) ->
    {'reply', [
        {?JOB_SVC_CONCUR_LIMIT, State#state.rmax},
        {?JOB_SVC_HIST_LIMIT,   State#state.hmax},
        {?JOB_SVC_QUEUE_LIMIT,  State#state.qmax}
    ] ++ gen_server:call(?JOBS_SVC_NAME, 'config'), State};
%
% cancel(JobId :: jid(), Kill :: boolean())
%   ->  {'ok', 'killed' | 'canceled' | 'history' | 'running', Job :: job()}
%     | 'false' | {'error', term()}.
%
handle_call({'cancel', JobId, Kill}, _, State) ->
    case dict:find(JobId, State#state.loc) of
        {'ok', Ref} when erlang:is_reference(Ref) ->
            case dict:find(Ref, State#state.run) of
                {'ok', {Runner, RJob}} ->
                    case Kill of
                        'true' ->
                            Job = riak_core_job:update('killed', RJob),
                            _ = notify({Ref, {Runner, Job}}, ?JOB_ERR_KILLED),
                            {'reply', {'ok', 'killed', Job},
                                append_history({JobId, Job}, State#state{
                                    run = dict:erase(Ref, State#state.run),
                                    rcnt = (State#state.rcnt - 1) })};
                        _ ->
                            {'reply', {'ok', 'running', RJob}, State}
                    end;
                _ ->
                    RErr = {'invalid_state', ?LINE},
                    {'stop', RErr, {'error', RErr}, State}
            end;
        {'ok', 'queued'} ->
            case extract_queued(JobId, State#state.que) of
                {QJob, Que} ->
                    Job = riak_core_job:update('canceled', QJob),
                    {'reply', {'ok', 'canceled', Job},
                        append_history({JobId, Job}, State#state{
                            que = Que,
                            qcnt = (State#state.qcnt - 1) })};
                'false' ->
                    QErr = {'invalid_state', ?LINE},
                    {'stop', QErr, {'error', QErr}, State}
            end;
        {'ok', 'history'} ->
            case lists:keyfind(JobId, 1, queue:to_list(State#state.hist)) of
                {_, HJob} ->
                    {'ok', 'history', HJob};
                'false' ->
                    HErr = {'invalid_state', ?LINE},
                    {'stop', HErr, {'error', HErr}, State}
            end;
        'false' ->
            {'reply', 'false', State}
    end;
%
% find(JobId :: jid()) -> {'ok', Job :: job()} | 'false' | {'error', term()}.
%
handle_call({'find', JobId}, _, State) ->
    case find_job_by_id(JobId, State) of
        {'ok', _, Job} ->
            {'reply', {'ok', Job}, State};
        'false' ->
            {'reply', 'false', State};
        {'error', What} = Error ->
            {'stop', What, Error, State}
    end;
%
% stats() -> [stat()] | {'error', term()}.
%
handle_call('stats', _, State) ->
    Status = if State#state.shutdown -> 'stopping'; ?else -> 'active' end,
    Result = [
        {'status',  Status},
        {'concur',  State#state.rmax},
        {'maxque',  State#state.qmax},
        {'maxhist', State#state.hmax},
        {'running', State#state.rcnt},
        {'inqueue', State#state.qcnt},
        {'history', State#state.hcnt},
        {'service', gen_server:call(?JOBS_SVC_NAME, 'stats')}
        | ?StatsDict:to_list(State#state.stats) ],
    {'reply', Result, State};
%
% stats(JobId :: jid()) -> [stat()] | 'false' | {'error', term()}.
%
handle_call({'stats', JobId}, _, State) ->
    case find_job_by_id(JobId, State) of
        {'ok', _, Job} ->
            {'reply', riak_core_job:stats(Job), State};
        'false' ->
            {'reply', 'false', State};
        {'error', What} = Error ->
            {'stop', What, Error, State}
    end;
%
% submit(job()) -> 'ok' | {'error', term()}.
%
handle_call({'submit' = Phase, Job}, From, StateIn) ->
    case submit_job(Phase, Job, From, StateIn) of
        {'error', Error, State} ->
            {'stop', Error, Error, State};
        {Result, State} ->
            {'reply', Result, State}
    end;
%
% unrecognized message
%
handle_call(Msg, {Who, _}, State) ->
    _ = lager:error(
        "~s received unhandled call from ~p: ~p", [?JOBS_MGR_NAME, Who, Msg]),
    {'reply', {'error', {'badarg', Msg}}, inc_stat('unhandled', State)}.

-spec handle_cast(Msg :: term(), State :: state())
        -> {'noreply', state()} | {'stop', term(), state()}.
%
% internal 'pending' message
%
% State#state.pending MUST be 'true' at the time when we encounter a 'pending'
% message - if it's not the State is invalid.
%
handle_cast('pending', #state{pending = Flag} = State) when Flag /= 'true' ->
    _ = lager:error("Invalid State: ~p", [State]),
    {'stop', 'invalid_state', State};
handle_cast('pending', StateIn) ->
    case pending(StateIn#state{pending = 'false'}) of
        {'ok', State} ->
            {'noreply', State};
        {'shutdown', State} ->
            {'stop', 'shutdown', State};
        {'error', _} = Error ->
            {'stop', Error, StateIn}
    end;
%
% status update from a running (or finishing) job
%
% These are sent directly from the riak_core_job_service module's private
% interface used by the riak_core_job_runner.
%
handle_cast({Ref, 'update', 'finished' = Stat, TS, Result},
        #state{rmax = RMax, qcnt = QCnt, rcnt = RCnt} = State)
        % Main case is when the queue is empty, but also accommodate dynamic
        % reduction of the concurrency limit.
        when QCnt =:= 0 orelse RCnt > RMax ->
    _ = erlang:demonitor(Ref, ['flush']),
    case dict:find(Ref, State#state.run) of
        {ok, {Runner, Done}} ->
            _ = riak_core_job_service:release(Runner),
            {'noreply', append_history({riak_core_job:gid(Done),
                riak_core_job:update('result', Result,
                    riak_core_job:update(Stat, TS, Done))},
                State#state{
                    run = dict:erase(Ref, State#state.run),
                    rcnt = (RCnt - 1),
                    stats = inc_stat('history', State#state.stats) })};
        _ ->
            _ = lager:error(
                "~s received completion message ~p:~p "
                "for unknown job ref ~p", [?JOBS_MGR_NAME, Stat, Result, Ref]),
            {'noreply', inc_stat('update_errors', State)}
    end;
handle_cast({Ref, 'update', 'finished' = Stat, TS, Result}, StateIn) ->
    case dict:find(Ref, StateIn#state.run) of
        {ok, {Runner, Done}} ->
            {{'value', {JobId, Job}}, Que} = queue:out(StateIn#state.que),
            State = append_history({JobId,
                riak_core_job:update('result', Result,
                    riak_core_job:update(Stat, TS, Done))},
                StateIn#state{
                    que = Que,
                    qcnt = (StateIn#state.qcnt - 1),
                    stats = inc_stat('history', StateIn#state.stats) }),
            case start_job(Runner, Ref, Job) of
                'ok' ->
                    {'noreply', State#state{
                        run = dict:store(Ref, {Runner, Job}, State#state.run),
                        loc = dict:store(JobId, Runner, State#state.loc) }};
                RunErr ->
                    _ = erlang:demonitor(Ref, ['flush']),
                    _ = riak_core_job_service:release(Runner),
                    {'stop', RunErr, State#state{
                        run = dict:erase(Ref, State#state.run),
                        rcnt = (State#state.rcnt - 1),
                        loc = dict:erase(JobId, State#state.loc) }}
            end;
        _ ->
            _ = lager:error(
                "~s received completion message ~p:~p for unknown job ref ~p",
                [?JOBS_MGR_NAME, Stat, Result, Ref]),
            {'noreply', inc_stat('update_errors', StateIn)}
    end;
handle_cast({Ref, 'update', Stat, TS}, State) ->
    case dict:find(Ref, State#state.run) of
        {ok, {Runner, Job}} ->
            {'noreply', State#state{
                run = dict:store(Ref,
                    {Runner, riak_core_job:update(Stat, TS, Job)},
                    State#state.run) }};
        _ ->
            _ = lager:error(
                "~s received ~p update for unknown job ref ~p",
                [?JOBS_MGR_NAME, Stat, Ref]),
            {'noreply', inc_stat('update_errors', State)}
    end;
%
% unrecognized message
%
handle_cast(Msg, State) ->
    _ = lager:error("~s received unhandled cast: ~p", [?JOBS_MGR_NAME, Msg]),
    {'noreply', inc_stat('unhandled', State)}.

-spec handle_info(Msg :: term(), State :: state())
        -> {'noreply', state()} | {'stop', term(), state()}.
%
% A monitored job crashed or was killed.
% If it completed normally, a 'done' update arrived in our mailbox before this
% and caused the monitor to be released and flushed, so the only way we get
% this is an exit before completion.
%
handle_info({'DOWN', Ref, _, Pid, Info}, StateIn) ->
    State = inc_stat('crashed', StateIn),
    case dict:find(Ref, State#state.run) of
        {ok, {Runner, Job}} ->
            _ = notify(Job, {?JOB_ERR_CRASHED, Info}),
            if Runner =:= Pid ->
                {'noreply', append_history(
                    {riak_core_job:gid(Job),
                        riak_core_job:update('crashed', Job)},
                    State#state{
                        run = dict:erase(Ref, State#state.run),
                        rcnt = (State#state.rcnt - 1) })};
            ?else ->
                _ = lager:error(
                    "~s Ref/Runner/Pid mismatch: ~p ~p ~p",
                    [?JOBS_MGR_NAME, Ref, Runner, Pid]),
                {'stop', 'invalid_state', State}
            end;
        _ ->
            _ = lager:error(
                "~s received 'DOWN' message "
                "for unrecognized process ~p", [?JOBS_MGR_NAME, Pid]),
            {'noreply', inc_stat('update_errors', State)}
    end;
%
% unrecognized message
%
handle_info(Msg, State) ->
    _ = lager:error("~s received unhandled info: ~p", [?JOBS_MGR_NAME, Msg]),
    {'noreply', inc_stat('unhandled', State)}.

-spec init(?MODULE) -> {'ok', state()} | {'stop', {'error', term()}}.
%
% initialize from the application environment
%
init(?MODULE) ->
    App = riak_core_job_service:default_app(),
    RMax = riak_core_job_service:app_config(
        App, ?JOB_SVC_CONCUR_LIMIT, 1, 'undefined', ?JOB_SVC_DEFAULT_CONCUR),
    %
    % The above does lots of good stuff for us, but we don't know if what we
    % get back is exactly the same as what's in the application environment,
    % so rather than reading and comparing just reset it and be done with it.
    %
    _ = application:set_env(App, ?JOB_SVC_CONCUR_LIMIT, RMax),

    QMax = riak_core_job_service:app_config(
        App, ?JOB_SVC_QUEUE_LIMIT, 0, RMax, ?JOB_SVC_DEFAULT_QUEUE),
    HMax = riak_core_job_service:app_config(
        App, ?JOB_SVC_HIST_LIMIT, 0, RMax, ?JOB_SVC_DEFAULT_HIST),

    % Make sure we get shutdown signals from the supervisor
    _ = erlang:process_flag('trap_exit', 'true'),

    % Tell the riak_core_job_service server to [re]configure itself.
    IMin = riak_core_job_service:app_config(
        App, ?JOB_SVC_IDLE_MIN, 0, RMax, erlang:max((RMax div 8), 3)),
    IMax = riak_core_job_service:app_config(
        App, ?JOB_SVC_IDLE_MAX, 0, RMax,
        erlang:max((IMin * 2), (erlang:system_info('schedulers') - 1))),
    gen_server:cast(?JOBS_SVC_NAME, {?job_svc_cfg_token,
        [{?JOB_SVC_IDLE_MIN, IMin}, {?JOB_SVC_IDLE_MAX, IMax}]}),

    {'ok', #state{rmax = RMax, qmax = QMax, hmax = HMax}}.

-spec start_link() -> {'ok', pid()}.
%
% start named service
%
start_link() ->
    gen_server:start_link({'local', ?JOBS_MGR_NAME}, ?MODULE, ?MODULE, []).

-spec terminate(Why :: term(), State :: state()) -> ok.
%
% no matter why we're terminating, de-monitor everything we're watching
%
terminate({'invalid_state', Line}, State) ->
    _ = lager:error(
        "~s terminated due to invalid state:~b: ~p",
        [?JOBS_MGR_NAME, Line, State]),
    terminate('shutdown', State);
terminate('invalid_state', State) ->
    _ = lager:error(
        "~s terminated due to invalid state: ~p", [?JOBS_MGR_NAME, State]),
    terminate('shutdown', State);
terminate(_, State) ->
    _ = notify(State, ?JOB_ERR_SHUTTING_DOWN),
    'ok'.

%% ===================================================================
%% Internal
%% ===================================================================

-spec append_history(JRec :: jrec(), State :: state()) -> state().
%
% Store the specified completed Job at the end of the history list, possibly
% triggering history pruning.
%
append_history({JobId, _} = JRec, State) ->
    check_pending(State#state{
        hist = queue:in(JRec, State#state.hist),
        hcnt = (State#state.hcnt + 1),
        loc = dict:store(JobId, 'history', State#state.loc) }).

-spec check_pending(State :: state()) -> state().
%
% Ensure that there's a 'pending' message in the inbox if there's background
% work to be done.
%
check_pending(#state{qcnt = 0, hmax = HM, hcnt = HC} = State) when HM >= HC ->
    State;
check_pending(State) ->
    set_pending(State).

-spec extract_queued(JobId :: jid(), Queue :: jque())
        -> {job(), jque()} | 'false'.
%
% Remove and return the full Job for JobId in Queue.
%
extract_queued(JobId, Queue) ->
    case lists:keytake(JobId, 1, queue:to_list(Queue)) of
        {'value', {_, Job}, QList} ->
            {Job, queue:from_list(QList)};
        'false' ->
            'false'
    end.

-spec find_job_by_id(JobId :: jid(), State :: state())
        -> {'ok', jloc(), job()} | 'false' | {'error', term()}.
%
% Find the current instance of the specified Job and its location.
%
% `false' is returned if the Job is not (or no longer) known to the Manager.
%
% I hate this function, but unrolling it was even uglier, and at least this
% way the compiler has a shot at optimizing it.
% By design, anything found in the #state.loc dictionary *should* be in the
% collection it points to, which would eliminate the inner case clauses, but
% the code's too new to make that leap.
%
find_job_by_id(JobId, State) ->
    case dict:find(JobId, State#state.loc) of
        {'ok', Ref} when erlang:is_reference(Ref) ->
            case dict:find(Ref, State#state.run) of
                {'ok', {_, Job}} ->
                    {'ok', Ref, Job};
                _ ->
                    {'error', {'invalid_state', Ref, ?LINE}}
            end;
        {'ok', 'queued'} ->
            case lists:keyfind(JobId, 1, queue:to_list(State#state.que)) of
                {_, Job} ->
                    {'ok', 'queued', Job};
                'false' ->
                    {'error', {'invalid_state', ?LINE}}
            end;
        {'ok', 'history'} ->
            case lists:keyfind(JobId, 1, queue:to_list(State#state.hist)) of
                {_, Job} ->
                    {'ok', 'history', Job};
                'false' ->
                    {'error', {'invalid_state', ?LINE}}
            end;
        'false' ->
            'false'
    end.

-spec inc_stat(stat_key() | [stat_key()], state()) -> state()
        ;     (stat_key() | [stat_key()], stats()) -> stats().
%
% Increment one or more statistics counters.
%
inc_stat(Stat, #state{stats = Stats} = State) ->
    State#state{stats = inc_stat(Stat, Stats)};
inc_stat(Stat, Stats) when not erlang:is_list(Stat) ->
    ?StatsDict:update_counter(Stat, 1, Stats);
inc_stat([Stat], Stats) ->
    inc_stat(Stat, Stats);
inc_stat([Stat | More], Stats) ->
    inc_stat(More, inc_stat(Stat, Stats));
inc_stat([], Stats) ->
    Stats.

-spec notify(
    What    :: job() | jrec() | [{rkey(), mon()} | jrec() | job()] | mons() | state(),
    Why     :: term())
        -> term().
%
% Notifies whoever may care of the premature disposal of the job(s) in What
% and returns Why.
% For recursive (or lists:fold) use.
%
notify([], Why) ->
    Why;
notify([Elem | Elems], Why) ->
    notify(Elems, notify(Elem, Why));
notify(#state{que = Que, run = Run}, Why) ->
    _ = notify(queue:to_list(Que), {?JOB_ERR_CANCELED, Why}),
    dict:fold(
        fun(Ref, Rec, Reason) ->
            notify({Ref, Rec}, Reason)
        end, Why, Run);
notify({Ref, {Pid, Job}}, Why) ->
    _ = erlang:demonitor(Ref, ['flush']),
    _ = erlang:exit(Pid, 'kill'),
    notify(Job, Why);
notify({_JobId, Job}, Why) ->
    notify(Job, Why);
notify(Job, Why) ->
    case riak_core_job:killed(Job) of
        'undefined' ->
            _ = riak_core_job:reply(Job, {'error', Why});
        Killed ->
            try
                _ = riak_core_job:invoke(Killed, Why)
            catch
                Class:What ->
                    _ = lager:error("Job ~p 'killed' failure: ~p:~p",
                        [riak_core_job:gid(Job), Class, What]),
                    _ = riak_core_job:reply(Job, {'error', Why})
            end
    end,
    Why.

-spec pending(State :: state())
        -> {'ok' | 'shutdown', state()} | {'error', term()}.
%
% Dequeue and dispatch at most one job. If there are more jobs waiting, ensure
% that we'll get to them after handling whatever may already be waiting.
%
% When the queue's caught up to concurrency capacity, prune the history if
% needed, one entry per iteration. Once in 'shutdown' mode, the history is
% completely ignored.
%
% Implementation Notes:
%
%   Queued job counts of zero, one, and more have their own distinct behavior,
%   so they get their own function heads.
%
%   Yes, I've checked - in the case where there's one item in the queue,
%   queue:out/2 is still more efficient than peeking at the item and creating
%   a new queue, because the queue implementation is optimized for the case.
%
pending(#state{shutdown = 'true', rcnt = 0, qcnt = 0} = State) ->
    {'shutdown', State};
pending(#state{shutdown = 'true', qcnt = 0} = State) ->
    {'ok', State};
pending(#state{shutdown = 'true', rcnt = 0, qcnt = 1} = State) ->
    {{'value', Job}, Que} = queue:out(State#state.que),
    _ = notify(Job, {?JOB_ERR_CANCELED, ?JOB_ERR_SHUTTING_DOWN}),
    {'shutdown', State#state{que = Que, qcnt = 0}};
pending(#state{shutdown = 'true'} = State) ->
    {{'value', {JobId, Job}}, Que} = queue:out(State#state.que),
    _ = notify(Job, {?JOB_ERR_CANCELED, ?JOB_ERR_SHUTTING_DOWN}),
    {'ok', check_pending(State#state{
        que = Que,
        qcnt = (State#state.qcnt - 1),
        loc = dict:erase(JobId, State#state.loc) })};
pending(State) when State#state.qcnt > 0
        andalso State#state.rcnt < State#state.rmax ->
    case riak_core_job_service:runner() of
        Runner when erlang:is_pid(Runner) ->
            Ref = erlang:monitor('process', Runner),
            {{'value', {JobId, JobIn}}, Que} = queue:out(State#state.que),
            case start_job(Runner, Ref, JobIn) of
                'ok' ->
                    Job = riak_core_job:update('dispatched', JobIn),
                    {'ok', check_pending(State#state{
                        que = Que,
                        qcnt = (State#state.qcnt - 1),
                        run = dict:store(Ref, {Runner, Job}, State#state.run),
                        rcnt = (State#state.rcnt + 1),
                        loc = dict:store(JobId, Runner, State#state.loc),
                        stats = inc_stat('dispatched', State#state.stats) })};
                RunErr ->
                    RunErr
            end;
        SvcErr ->
            SvcErr
    end;
pending(State) when State#state.hcnt > State#state.hmax ->
    {{'value', {JobId, _}}, Hist} = queue:out(State#state.hist),
    {'ok', check_pending(State#state{
        hist = Hist,
        hcnt = (State#state.hcnt - 1),
        loc = dict:erase(JobId, State#state.loc) })};
pending(State) ->
    {'ok', State}.

-spec set_pending(State :: state()) -> state().
%
% Ensure that there's a 'pending' message in the inbox.
%
set_pending(#state{pending = 'false'} = State) ->
    gen_server:cast(erlang:self(), 'pending'),
    State#state{pending = 'true'};
set_pending(State) ->
    State.

-spec start_job(Runner :: pid(), Ref :: reference(), Job :: job())
        -> 'ok' | {'error', term()}.
%
% Wrapper around riak_core_job_runner:run/4 that fills in the blanks.
%
start_job(Runner, Ref, Job) ->
    riak_core_job_runner:run(Runner, erlang:self(), Ref, Job).

-spec submit_job(
    Phase   :: atom() | {atom(), term()},
    Job     :: job(),
    From    :: {pid(), term()},
    State   :: state())
        -> {'ok' | {'error', term()}, state()} | {'error', term(), state()}.
%
% Accept and dispatch, or reject, the specified Job, updating statistics.
% On entry from the external call Phase is 'submit'; other phases are internal.
%
submit_job('submit', _, _, #state{shutdown = 'true'} = State) ->
    {{'error', ?JOB_ERR_SHUTTING_DOWN}, inc_stat('rejected_shutdown', State)};
submit_job('submit', _, _, #state{qcnt = C, qmax = M} = State) when C >= M ->
    {{'error', ?JOB_ERR_QUEUE_OVERFLOW}, inc_stat('rejected_overflow', State)};
submit_job('submit', Job, From, State) ->
    submit_job({'runnable', riak_core_job:runnable(Job)}, Job, From, State);
submit_job({'runnable', 'true'}, Job, {Caller, _} = From, State) ->
    Class   = riak_core_job:class(Job),
    Accept  = riak_core_util:job_class_enabled(Class),
    _ = riak_core_util:report_job_request_disposition(
            Accept, Class, ?MODULE, 'submit_job', ?LINE, Caller),
    submit_job(Accept, Job, From, State);
submit_job({'runnable', Error}, _, _, State) ->
    {Error, inc_stat('accept_errors', State)};
submit_job('queue', JobIn, _From, State) ->
    Job = riak_core_job:update('queued', JobIn),
    JobId = riak_core_job:gid(Job),
    {'ok', set_pending(State#state{
        que = queue:in({JobId, Job}, State#state.que),
        qcnt = (State#state.qcnt + 1),
        loc = dict:store(JobId, 'queued', State#state.loc),
        stats = inc_stat('queued', State#state.stats)}) };
submit_job({'runner', Runner}, JobIn, _, State) when erlang:is_pid(Runner) ->
    Ref = erlang:monitor('process', Runner),
    case start_job(Runner, Ref, JobIn) of
        'ok' ->
            Job = riak_core_job:update('dispatched', JobIn),
            JobId = riak_core_job:gid(Job),
            {'ok', State#state{
                run = dict:store(Ref, {Runner, Job}, State#state.run),
                rcnt = (State#state.rcnt + 1),
                loc = dict:store(JobId, Runner, State#state.loc),
                stats = inc_stat('dispatched', State#state.stats) }};
        Error ->
            _ = erlang:demonitor(Ref, ['flush']),
            _ = riak_core_job_service:release(Runner),
            {'error', Error, inc_stat('service_errors', State)}
    end;
submit_job({'runner', Error}, _, _, State) ->
    {'error', Error, inc_stat('service_errors', State)};
submit_job('true', Job, From, #state{rcnt = C, rmax = M} = State) when C < M ->
    submit_job(
        {'runner', riak_core_job_service:runner()},
        Job, From, inc_stat('accepted', State));
submit_job('true', Job, From, State) ->
    submit_job('queue', Job, From, inc_stat('accepted', State));
submit_job('false', _, _, State) ->
    {{'error', ?JOB_ERR_REJECTED}, inc_stat('rejected', State)};
submit_job(Phase, Job, From, State) ->
    erlang:error('badarg', [Phase, Job, From, State]).

-spec update_job(Manager :: service(), Ref :: rkey(), Stat :: atom()) -> 'ok'.
%
% Update a Job's Stat with the current time.
%
update_job(Manager, Ref, Stat) ->
    Update = {Ref, 'update', Stat, riak_core_job:timestamp()},
    gen_server:cast(Manager, Update).

-spec update_job(
    Manager :: service(), Ref :: rkey(), Stat :: atom(), Info :: term())
        -> 'ok'.
%
% Update a Job's Stat with the current time and additional Info.
%
update_job(Manager, Ref, Stat, Info) ->
    Update = {Ref, 'update', Stat, riak_core_job:timestamp(), Info},
    gen_server:cast(Manager, Update).
