%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016-2017 Basho Technologies, Inc.
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
%% Recognized configuration settings in the application environment:
%%
%% <ul>
%%  <li>{@link cfg_concur_max()}</li>
%%  <li>{@link cfg_queue_max()}</li>
%%  <li>{@link cfg_hist_max()}</li>
%%  <li>{@link riak_core_job_service:cfg_idle_min()}</li>
%%  <li>{@link riak_core_job_service:cfg_idle_max()}</li>
%%  <li>{@link riak_core_job_service:cfg_recycle()}</li>
%% </ul>
%%
%% These configuration keys and their defaults are defined as macros in
%% `riak_core_job.hrl'.
%%
%% Dynamic reconfiguration is supported by the {@link reconfigure/0} function.
%%
-module(riak_core_job_manager).
-behaviour(gen_server).

%% Public API
-export([
    cancel/2,
    config/0,
    find/1,
    reconfigure/0,
    stats/0,
    stats/1,
    submit/1
]).

%% Public Types
-export_type([
    cfg_concur_max/0,
    cfg_hist_max/0,
    cfg_queue_max/0,
    cfg_mult/0,
    cfg_prop/0,
    config/0,
    mgr_key/0,
    stat/0,
    stat_key/0,
    stat_val/0
]).

%% Work Callback API
-export([
    running_job/1
]).

%% Runner Callback API
-export([
    cleanup/1,
    finished/2,
    running/1,
    starting/1
]).

%% Gen_server API
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
%% Internal Types
%% ===================================================================

-define(StatsDict,  orddict).
-type stats()   ::  ?orddict_t(stat_key(), stat_val()).

%% mgr_key() opaque type
-record(mgrkey, {
    mgr     ::  pid(),  % manager process
    key     ::  rkey()  % reference into the 'run' dictionary
}).

%% JobId/Job pair, always kept together
-record(jrec, {
    id      ::  jid(),
    job     ::  job()
}).

%% Full running job record
-record(rrec, {
    rref    ::  rref(),
    jrec    ::  jrec()
}).

%% RunnerRef/RunnerPid, always kept together
-record(rref, {
    ref     ::  rkey(),
    pid     ::  runner()
}).

%% Job queue of JobId/Job pairs
-record(jq, {
    c   = 0             ::  non_neg_integer(),      % count
    d   = queue:new()   ::  ?queue_t(jrec())        % data
}).

%% 'location' dictionary, JobId => location atom or ref
%% count is not maintained
-record(ld, {
    d   = dict:new()    ::  ?dict_t(jid(), jloc())  % data
}).

%% 'running' dictionary, RunnerRef => full record
-record(rd, {
    c   = 0             ::  non_neg_integer(),      % count
    d   = dict:new()    ::  ?dict_t(rkey(), rrec()) % data
}).

%%
%% 'rmax', 'qmax', and 'hmax' are the maximum sizes, via configuration, of
%% the 'run', 'que', and 'hist' collections, respectively.
%%
%% Any job we're managing is in exactly one of the 'run', 'que', or 'hist'
%% collections, and the 'loc' dictionary tells us which one.
%%
-record(state, {
    rmax                            ::  pos_integer(),
    qmax                            ::  non_neg_integer(),
    hmax                            ::  non_neg_integer(),
    pending     = false             ::  boolean(),
    shutdown    = false             ::  boolean(),
    loc         = #ld{}             ::  ldict(),    % jid() => jloc()
    run         = #rd{}             ::  rdict(),    % rkey() => rrec()
    que         = #jq{}             ::  jque(),     % jrec()
    hist        = #jq{}             ::  jque(),     % jrec()
    stats       = ?StatsDict:new()  ::  stats()     % stat_key() => stat_val()
}).

-type ckey()        ::  jid() | rkey().
-type coll()        ::  jque() | ldict() | rdict().
-type crec()        ::  jrec() | rrec() | {jid(), jloc()}.
-type jid()         ::  riak_core_job:gid().
-type jloc()        ::  queue | history | rkey().
-type job()         ::  riak_core_job:job().
-type jque()        ::  #jq{}.
-type jrec()        ::  #jrec{}.
-type ldict()       ::  #ld{}.
-type rdict()       ::  #rd{}.
-type rkey()        ::  reference().
-type rrec()        ::  #rrec{}.
-type rref()        ::  #rref{}.
-type runner()      ::  riak_core_job_service:runner().
-type state()       ::  #state{}.

-define(is_job_loc(Term),   (erlang:is_reference(Term)
        orelse Term =:= queue orelse Term =:= history)).

%% ===================================================================
%% Public Types
%% ===================================================================

-type cfg_concur_max() :: riak_core_job_service:cfg_concur_max().
%% Maximum number of jobs to execute concurrently.
%%
%% Scoped to the application returned by
%% {@link riak_core_job_service:default_app/0}.
%%
%% Default: <code>{scheds, 6}</code>.

-type cfg_hist_max()  :: {?JOB_SVC_HIST_LIMIT, non_neg_integer() | cfg_mult()}.
%% Maximum number of completed jobs' histories to maintain.
%%
%% Scoped to the application returned by
%% {@link riak_core_job_service:default_app/0}.
%%
%% Default: <code>{concur, 1}</code>.

-type cfg_queue_max() :: {?JOB_SVC_QUEUE_LIMIT, non_neg_integer() | cfg_mult()}.
%% Maximum number of jobs to queue for future execution.
%%
%% Scoped to the application returned by
%% {@link riak_core_job_service:default_app/0}.
%%
%% Default: <code>{concur, 3}</code>.

-type cfg_mult() :: riak_core_job_service:cfg_mult().

-type cfg_prop() :: cfg_concur_max() | cfg_hist_max() | cfg_queue_max()
                  | riak_core_job_service:cfg_prop().
%% Any of the configuration properties returned by {@link config/0}.

-type config() :: [cfg_prop()].
%% All of the configuration properties returned by {@link config/0}.

?opaque mgr_key() :: #mgrkey{}.
%% An object used by running Jobs' UoWs to refer back to their state in the
%% Manager running them.

-type stat() :: {stat_key(), stat_val()}.
%% A single statistic.
%% <i>Most</i> Service statistics are integral counters in the form
%% <code>{<i>Key</i>, <i>Count</i> :: non_neg_integer()}</code>.

-type stat_key() :: atom() | tuple().
%% The Key by which a statistic is referenced.
%% <i>Most</i> statistics keys are simple, single-word atoms.

-type stat_val() :: term().
%% The value of a statistic, most often an integral count greater than zero.

%% ===================================================================
%% Public API
%% ===================================================================

-spec cancel(
    JobOrId :: riak_core_job:job() | riak_core_job:gid(),
    Kill    :: boolean())
        ->  {ok, killed | canceled | history, riak_core_job:job()}
            | {error, running, riak_core_job:job()} | false | {error, term()}.
%%
%% @doc Cancel, or optionally kill, the specified Job.
%%
%% If <i>Kill</i> is `true' the job will de-queued or killed, as appropriate.
%% If <i>Kill</i> is `false' the job will only be de-queued.
%%
%% Returns:
%% <dl>
%%  <dt><code>{ok, <i>Status</i>, <i>Job</i>}</code></dt>
%%  <dd>Status indicates whether the Job was `killed', `canceled' (de-queued),
%%      or had recently `finished' running.</dd>
%%  <dd>Note that what constitutes "recently finished" is subject to load and
%%      history configuration.</dd>
%%  <dd><i>Job</i> is the current instance of <i>JobOrId</i>.</dd>
%%
%%  <dt><code>{error, running, <i>Job</i>}</code></dt>
%%  <dd>Only returned when <i>Kill</i> is `false', indicating that <i>Job</i>
%%      is currently active (and remains so).</dd>
%%  <dd><i>Job</i> is the current instance of <i>JobOrId</i>.</dd>
%%
%%  <dt><code>false</code></dt>
%%  <dd><i>JobOrId</i> is not, or is no longer, known to the Manager.</dd>
%% </dl>
%%
cancel(JobOrId, Kill) ->
    JobId = case riak_core_job:version(JobOrId) of
        {job, _} ->
            riak_core_job:gid(JobOrId);
        _ ->
            JobOrId
    end,
    gen_server:call(?JOBS_MGR_NAME, {cancel, JobId, Kill}).

-spec config() -> config() | {error, term()}.
%%
%% @doc Return the current effective Jobs Management configuration.
%%
config() ->
    gen_server:call(?JOBS_MGR_NAME, config).

-spec find(JobOrId :: riak_core_job:job() | riak_core_job:gid())
        -> {ok, riak_core_job:job()} | false | {error, term()}.
%%
%% @doc Return the latest instance of the specified Job.
%%
%% `false' is returned if the Job is not, or is no longer, known to the Manager.
%%
find(JobOrId) ->
    JobId = case riak_core_job:version(JobOrId) of
        {job, _} ->
            riak_core_job:gid(JobOrId);
        _ ->
            JobOrId
    end,
    gen_server:call(?JOBS_MGR_NAME, {find, JobId}).

-spec reconfigure() -> ok.
%%
%% @doc Re-read application environment configuration and adjust accordingly.
%%
reconfigure() ->
    gen_server:cast(?JOBS_MGR_NAME, reconfigure).

-spec stats() -> [stat()] | {error, term()}.
%%
%% @doc Return statistics from the Jobs Management system.
%%
stats() ->
    gen_server:call(?JOBS_MGR_NAME, stats).

-spec stats(JobOrId :: riak_core_job:job() | riak_core_job:gid())
        -> [riak_core_job:stat()] | false | {error, term()}.
%%
%% @doc Return statistics from the specified Job.
%%
%% `false' is returned if the Job is not, or is no longer, known to the Manager.
%%
stats(JobOrId) ->
    JobId = case riak_core_job:version(JobOrId) of
        {job, _} ->
            riak_core_job:gid(JobOrId);
        _ ->
            JobOrId
    end,
    gen_server:call(?JOBS_MGR_NAME, {stats, JobId}).

-spec submit(Job :: riak_core_job:job()) -> ok | {error, term()}.
%%
%% @doc Submit a Job to be run.
%%
%% Some (though possibly not all) of the results that may be returned are:
%%
%% <dl>
%%  <dt><code>ok</code></dt>
%%  <dd>The Job was accepted and is either running or queued to run.</dd>
%%
%%  <dt><code>{error, service_shutdown}</code></dt>
%%  <dd>The service is shutting down; no further Jobs will be accepted.</dd>
%%
%%  <dt><code>{error, job_queue_full}</code></dt>
%%  <dd>The service is running its maximum number of Jobs, and has queued its
%%      maximum number of Jobs. This <i>should</i> be a transient error.</dd>
%%
%%  <dt><code>{error, job_rejected}</code></dt>
%%  <dd>The Job was rejected by <i>[the equivalent of]</i>
%%      {@link riak_core_util:job_class_enabled/2}.</dd>
%%
%%  <dt><code>{error, <i>Error</i> :: term()}</code></dt>
%%  <dd>Most likely the Job is not runnable and <i>Error</i> came from
%%      {@link riak_core_job:runnable/1}.</dd>
%% </dl>
%%
%% Other errors are possible from various system components, but should be rare
%% and relatively obvious.
%%
submit(Job) ->
    case riak_core_job:version(Job) of
        {job, _} ->
            gen_server:call(?JOBS_MGR_NAME, {submit, Job});
        _ ->
            erlang:error(badarg, [Job])
    end.

%% ===================================================================
%% Work Callback API
%% ===================================================================

-spec running_job(MgrKey :: mgr_key()) -> riak_core_job:job() | {error, term()}.
%%
%% @doc Returns the running Job associated with MgrKey.
%%
%% This function should only be invoked by a UoW while it's running.
%% In any other case, it returns `{error, not_running}'.
%%
running_job(#mgrkey{mgr = Mgr, key = Key}) ->
    gen_server:call(Mgr, {running_job, Key}).

%% ===================================================================
%% Runner Callback API
%% ===================================================================

-spec cleanup(MgrKey :: mgr_key()) -> ok.
%% @private
%% @doc Callback for the job runner indicating the UOW is being cleaned up.
%%
cleanup(MgrKey) ->
    update_job(MgrKey, cleanup).

-spec finished(MgrKey :: mgr_key(), Result :: term()) -> ok.
%% @private
%% @doc Callback for the job runner indicating the UOW is finished.
%%
finished(MgrKey, Result) ->
    update_job(MgrKey, finished, Result).

-spec running(MgrKey :: mgr_key()) -> ok.
%% @private
%% @doc Callback for the job runner indicating the UOW is being run.
%%
running(MgrKey) ->
    update_job(MgrKey, running).

-spec starting(MgrKey :: mgr_key()) -> ok.
%% @private
%% @doc Callback for the job runner indicating the UOW is being started.
%%
starting(MgrKey) ->
    update_job(MgrKey, started).

%% ===================================================================
%% Gen_server API
%% ===================================================================

-spec code_change(OldVsn :: term(), State :: state(), Extra :: term())
        -> {ok, state()}.
%% @private
%% we don't care, just carry on
%%
code_change(_, State, _) ->
    {ok, State}.

-spec handle_call(Msg :: term(), From :: {pid(), term()}, State :: state())
        -> {reply, term(), state()} | {stop, term(), term(), state()}.
%% @private
%% @end
%%
%% cancel(JobId :: jid(), Kill :: boolean())
%%   ->  {ok, killed | canceled | history, job()}
%%       | {error, running, job()} | false | {error, term()}
%%
handle_call({cancel, JobId, Kill}, _, State) ->
    case c_find(JobId, State#state.loc) of

        {_, Ref} when erlang:is_reference(Ref) ->
            {Result, StateOut} = cancel_running(Ref, Kill, State),
            {reply, Result, StateOut};

        {_, queue} ->
            {#jrec{job = QJob} = QRec, Que} = c_remove(JobId, State#state.que),
            QJOut = riak_core_job:update(canceled, QJob),
            _ = notify(QRec#jrec{job = QJOut}, {?JOB_ERR_CANCELED, cancel}),
            QState = State#state{
                que = Que, loc = c_erase(JobId, State#state.loc),
                stats = inc_stat(canceled, State#state.stats)},
            {reply, {ok, canceled, QJOut}, QState};

        {_, history} ->
            #jrec{job = HJob} = c_find(JobId, State#state.hist),
            {reply, {ok, history, HJob}, State};

        false ->
            {reply, false, State}
    end;
%%
%% config() -> config() | {error, term()}.
%%
handle_call(config, _, State) ->
    {reply, [
        {?JOB_SVC_CONCUR_LIMIT, State#state.rmax},
        {?JOB_SVC_HIST_LIMIT,   State#state.hmax},
        {?JOB_SVC_QUEUE_LIMIT,  State#state.qmax}
    ] ++ riak_core_job_service:config(), State};
%%
%% find(JobId :: jid()) -> {'ok', Job :: job()} | false | {'error', term()}.
%%
handle_call({find, JobId}, _, State) ->
    case find_job_by_id(JobId, State) of
        {ok, _, #jrec{job = Job}} ->
            {reply, {ok, Job}, State};
        false ->
            {reply, false, State}
    end;
%%
%% running_job(MgrKey :: mgr_key()) -> riak_core_job:job() | {'error', term()}.
%%
handle_call({running_job, Key}, _, State) ->
    case c_find(Key, State#state.run) of
        #rrec{jrec = #jrec{job = Job}} ->
            {reply, Job, State};
        false ->
            {reply, {error, not_running}, State}
    end;
%%
%% stats() -> [stat()] | {'error', term()}.
%%
handle_call(stats, _, State) ->
    Status = if State#state.shutdown -> stopping; ?else -> active end,
    Result = [
        {status,    Status},
        {concur,    State#state.rmax},
        {maxque,    State#state.qmax},
        {maxhist,   State#state.hmax},
        {running,   State#state.run#rd.c},
        {inqueue,   State#state.que#jq.c},
        {history,   State#state.hist#jq.c},
        {service,   riak_core_job_service:stats()}
        | ?StatsDict:to_list(State#state.stats) ],
    {reply, Result, State};
%%
%% stats(JobId :: jid()) -> [stat()] | false | {'error', term()}.
%%
handle_call({stats, JobId}, _, State) ->
    case find_job_by_id(JobId, State) of
        {ok, _, #jrec{job = Job}} ->
            {reply, riak_core_job:stats(Job), State};
        false ->
            {reply, false, State}
    end;
%%
%% submit(Job :: job()) -> 'ok' | {'error', term()}.
%%
handle_call({submit = Phase, Job}, From, StateIn) ->
    case submit_job(Phase, Job, From, StateIn) of
        {error, Error, State} ->
            {stop, Error, Error, State};
        {Result, State} ->
            {reply, Result, State}
    end;
%%
%% unrecognized message
%%
handle_call(Msg, {Who, _}, State) ->
    _ = lager:error(
        "~s received unhandled call from ~p: ~p", [?JOBS_MGR_NAME, Who, Msg]),
    {reply, {error, {badarg, Msg}}, inc_stat(unhandled, State)}.

-spec handle_cast(Msg :: term(), State :: state())
        -> {noreply, state()} | {stop, term(), state()}.
%% @private
%% @end
%%
%% internal 'pending' message
%%
%% State#state.pending MUST be 'true' at the time when we encounter a 'pending'
%% message - if it's not the State is invalid.
%%
handle_cast(pending, #state{pending = Flag} = State) when Flag /= true ->
    _ = lager:error("Invalid State: ~p", [State]),
    {stop, invalid_state, State};

handle_cast(pending, StateIn) ->
    case pending(StateIn#state{pending = false}) of
        {ok, State} ->
            {noreply, State};
        {shutdown, State} ->
            {stop, shutdown, State};
        {{error, _} = Error, State} ->
            {stop, Error, State}
    end;
%%
%% update_job(MgrKey :: mgr_key(), Stat :: atom(), Info :: term()) -> ok.
%%
handle_cast({Ref, update, finished = Stat, TS, Result}, State) ->
    case c_remove(Ref, State#state.run) of
        {#rrec{rref = RRef, jrec = JRec}, Run} ->
            State1 = State#state{run = Run},
            _ = release_runner(RRef),
            Job = riak_core_job:update(
                result, Result, riak_core_job:update(Stat, TS, JRec#jrec.job)),
            case advance(JRec#jrec{job = Job}, history, State1) of
                {ok, State2} ->
                    {noreply, State2};
                {{error, Error}, State2} ->
                    {stop, Error, State2}
            end;
        {false, _} ->
            _ = lager:error(
                "~s received completion message ~p:~p for unknown job ref ~p",
                [?JOBS_MGR_NAME, Stat, Result, Ref]),
            {noreply, inc_stat(update_errors, State)}
    end;
%%
%% update_job(MgrKey :: mgr_key(), Stat :: stat_key()) -> ok.
%%
handle_cast({Ref, update, Stat, TS}, State) ->
    case c_find(Ref, State#state.run) of
        #rrec{jrec = JRec} = RRec ->
            Job = riak_core_job:update(Stat, TS, JRec#jrec.job),
            Rec = RRec#rrec{jrec = JRec#jrec{job = Job}},
            {noreply, State#state{run = c_store(Rec, State#state.run)}};
        false ->
            _ = lager:error(
                "~s received ~p update for unknown job ref ~p",
                [?JOBS_MGR_NAME, Stat, Ref]),
            {noreply, inc_stat(update_errors, State)}
    end;
%%
%% configuration message
%%
handle_cast(reconfigure,
        #state{rmax = RMax, qmax = QMax, hmax = HMax} = State) ->

    % Tell the riak_core_job_service server to [re]configure itself too, right
    % away, so it can get a jump on possibly cranking up available runners.
    gen_server:cast(?JOBS_SVC_NAME, reconfigure),

    % Update the settings in the application environment.
    App     = riak_core_job_service:default_app(),
    Concur  = app_config(App, ?JOB_SVC_CONCUR_LIMIT),
    QueMax  = app_config(App, ?JOB_SVC_QUEUE_LIMIT),
    HistMax = app_config(App, ?JOB_SVC_HIST_LIMIT),

    if
        Concur /= RMax orelse QueMax /= QMax orelse HistMax /= HMax ->
            _ = lager:info(
                "Updating configuration from {~b, ~b, ~b} to {~b, ~b, ~b}",
                [RMax, QMax, HMax, Concur, QueMax, HistMax]),
            {noreply, check_pending(
                State#state{rmax = Concur, qmax = QueMax, hmax = HistMax})};
        ?else ->
            {noreply, State}
    end;
%%
%% unrecognized message
%%
handle_cast(Msg, State) ->
    _ = lager:error("~s received unhandled cast: ~p", [?JOBS_MGR_NAME, Msg]),
    {noreply, inc_stat(unhandled, State)}.

-spec handle_info(Msg :: term(), State :: state())
        -> {noreply, state()} | {stop, term(), state()}.
%% @private
%% @end
%%
%% A monitored job crashed or was killed.
%% If it completed normally, a 'done' update arrived in our mailbox before this
%% and caused the monitor to be released and flushed, so the only way we get
%% this is an exit before completion.
%%
handle_info({'DOWN', Ref, _, Pid, Info}, StateIn) ->
    case c_remove(Ref, StateIn#state.run) of
        {#rrec{rref = RRef, jrec = JRec}, Run} ->
            _ = notify(JRec, {?JOB_ERR_CRASHED, Info}),
            Job = riak_core_job:update(crashed, JRec#jrec.job),
            case advance(JRec#jrec{job = Job}, history,
                    StateIn#state{run = Run,
                    stats = inc_stat(crashed, StateIn#state.stats)}) of
                {ok, State} ->
                    case RRef#rref.pid of
                        Pid ->
                            {noreply, State};
                        _ ->
                            _ = lager:error(
                                "~s Ref/Runner/Pid mismatch: ~p ~p ~p",
                                [?JOBS_MGR_NAME, Ref, RRef#rref.pid, Pid]),
                            {stop, invalid_state, State}
                    end;
                {{error, Error}, State} ->
                    {stop, Error, State}
            end;
        {false, _} ->
            _ = lager:error(
                "~s received 'DOWN' message for unrecognized process ~p",
                [?JOBS_MGR_NAME, Pid]),
            {noreply, inc_stat(update_errors, StateIn)}
    end;
%%
%% unrecognized message
%%
handle_info(Msg, State) ->
    _ = lager:error("~s received unhandled info: ~p", [?JOBS_MGR_NAME, Msg]),
    {noreply, inc_stat(unhandled, State)}.

-spec init(?MODULE) -> {ok, state()} | {stop, {error, term()}}.
%% @private
%%
%% initialize from the application environment
%%
init(?MODULE) ->

    % Make sure we get shutdown signals from the supervisor
    _ = erlang:process_flag(trap_exit, true),

    % Get/update application environment values.
    App     = riak_core_job_service:default_app(),
    Concur  = app_config(App, ?JOB_SVC_CONCUR_LIMIT),
    QueMax  = app_config(App, ?JOB_SVC_QUEUE_LIMIT),
    HistMax = app_config(App, ?JOB_SVC_HIST_LIMIT),

    {ok, #state{rmax = Concur, qmax = QueMax, hmax = HistMax}}.

-spec start_link() -> {ok, pid()}.
%% @private
%%
%% start named service
%%
start_link() ->
    gen_server:start_link({local, ?JOBS_MGR_NAME}, ?MODULE, ?MODULE, []).

-spec terminate(Why :: term(), State :: state()) -> ok.
%% @private
%%
%% no matter why we're terminating, de-monitor everything we're watching
%%
terminate({invalid_state, Line}, State) ->
    _ = lager:error(
        "~s terminated due to invalid state:~b: ~p",
        [?JOBS_MGR_NAME, Line, State]),
    terminate(shutdown, State);

terminate(invalid_state, State) ->
    _ = lager:error(
        "~s terminated due to invalid state: ~p", [?JOBS_MGR_NAME, State]),
    terminate(shutdown, State);

terminate(Why, State) ->
    _ = notify(State, Why),
    ok.

%% ===================================================================
%% Internal
%% ===================================================================

-spec acquire_runner() -> rref() | {error, term()}.
%%
%% Acquires a Runner, adds a monitor to it, and returns it.
%%
acquire_runner() ->
    case riak_core_job_service:runner() of
        Runner when erlang:is_pid(Runner) ->
            Ref = erlang:monitor(process, Runner),
            #rref{ref = Ref, pid = Runner};
        Error ->
            Error
    end.

-spec advance(
    From    :: queue | job() | jrec(),
    Dest    :: queue | running | history | rref(),
    State   :: state())
        -> {ok | {error, term()}, state()}.
%%
%% Advance a Job to Dest.
%% When Passing in a RRef, be sure any job associated with it has been
%% separately disposed of beforehand, as it will be lost.
%%
advance(queue, running, State)
        when    State#state.que#jq.c =:= 0
        orelse  State#state.que#rd.c >= State#state.rmax ->
    {ok, State};

advance(queue, #rref{} = RRef, State) when State#state.que#jq.c =:= 0 ->
    _ = release_runner(RRef),
    {ok, State#state{run = c_erase(RRef#rref.ref, State#state.run)}};

advance(queue, Dest, State)
        when Dest =:= running orelse erlang:is_record(Dest, rref) ->
    {JRec, Que} = q_pop(State#state.que),
    case advance(JRec, Dest, State) of
        {ok, StateOut} ->
            {ok, StateOut#state{que = Que}};
        Error ->
            Error
    end;

advance(#jrec{} = JRec, queue, State) ->
    Job = riak_core_job:update(queued, JRec#jrec.job),
    {ok, check_pending(State#state{
        que = c_store(JRec#jrec{job = Job}, State#state.que),
        loc = c_store({JRec#jrec.id, queue}, State#state.loc),
        stats = inc_stat(queued, State#state.stats)})};

%%%% not currently used, hide but maintain order for later use
%%advance(#jrec{}, running, State)
%%        when State#state.que#rd.c >= State#state.rmax ->
%%    % this is a programming error, shouldn't have gotten here with run full
%%    {{error, run_overflow}, inc_stat(run_overflow, State)};

advance(#jrec{} = JRec, running, State) ->
    case acquire_runner() of
        #rref{} = RRef ->
            advance(JRec, RRef, State);
        SvcErr ->
            {SvcErr, inc_stat(service_errors, State)}
    end;

advance(#jrec{} = JRec, #rref{} = RRef, State) ->
    case start_job(RRef#rref.pid, RRef#rref.ref, JRec#jrec.job) of
        ok ->
            Job = riak_core_job:update(dispatched, JRec#jrec.job),
            RRec = #rrec{rref = RRef, jrec = JRec#jrec{job = Job}},
            {ok, check_pending(State#state{
                run = c_store(RRec, State#state.run),
                loc = c_store({JRec#jrec.id, RRef#rref.ref}, State#state.loc),
                stats = inc_stat(dispatched, State#state.stats)})};
        StartErr ->
            % Bad Runner? Kill it ...
            _ = exit_runner(RRef, kill),
            % ... and make sure we're not holding a reference to it
            {StartErr, State#state{
                run = c_erase(RRef#rref.ref, State#state.run),
                stats = inc_stat(runner_errors, State#state.stats)}}
    end;

advance(#jrec{} = JRec, history, State) ->
    Job = riak_core_job:update(archived, JRec#jrec.job),
    {ok, check_pending(State#state{
        hist = c_store(JRec#jrec{job = Job}, State#state.hist),
        loc = c_store({JRec#jrec.id, history}, State#state.loc),
        stats = inc_stat(archived, State#state.stats)})};

advance(Job, Dest, State) ->
    case riak_core_job:version(Job) of
        {job, _} ->
            advance(#jrec{id = riak_core_job:gid(Job), job = Job}, Dest, State);
        _ ->
            ?UNMATCHED_ARGS([Job, Dest])
    end.

-spec app_config(App :: atom(), Key :: atom()) -> term().
%%
%% Returns the configured value for Key in the specified application scope.
%%
app_config(App, ?JOB_SVC_QUEUE_LIMIT = Key) ->
    riak_core_job_service:app_config(App, Key, 0, ?JOB_SVC_DEFAULT_QUEUE);
app_config(App, ?JOB_SVC_HIST_LIMIT = Key) ->
    riak_core_job_service:app_config(App, Key, 0, ?JOB_SVC_DEFAULT_HIST);
app_config(App, Key) ->
    riak_core_job_service:app_config(App, Key).

-spec cancel_running(RunRef :: rkey(), Kill :: boolean(), State :: state()) ->
        {{ok, killed, job()} | {error, running, job()}, state()}.
%%
%% Cancel a running job, or report that we're not allowed to kill it.
%%
cancel_running(RunRef, true, State) ->
    {#rrec{rref = RRef, jrec = JRec}, Run} = c_remove(RunRef, State#state.run),
    % kill it right away, notify the originator after updates
    _ = exit_runner(RRef, kill),
    % RRef is permanently invalidated
    JRIn = JRec#jrec{job = riak_core_job:update(killed, JRec#jrec.job)},
    {ok, StateOut} = advance(JRIn, history,
        State#state{run = Run, stats = inc_stat(killed, State#state.stats)}),
    % get the updated job from history - it has to be there
    % ... we actually know where, but use the API what's there
    JROut = c_find(JRIn#jrec.id, StateOut#state.hist),
    % NOW tell the owner what we did to their job
    _ = notify(JROut, {?JOB_ERR_KILLED, cancel}),
    {{ok, killed, JROut#jrec.job}, StateOut};

cancel_running(RunRef, false, State) ->
    #rrec{jrec = #jrec{job = RJob}} = c_find(RunRef, State#state.run),
    {{error, running, RJob}, State}.

-spec check_pending(State :: state()) -> state().
%%
%% Ensure that there's a 'pending' message in the inbox if there's background
%% work to be done.
%%
check_pending(State)
        when    State#state.que#jq.c =:= 0
        andalso State#state.hist#jq.c =< State#state.hmax ->
    State;
check_pending(State) ->
    set_pending(State).

-spec exit_runner(RRef :: rref(), Why :: term()) -> ok.
%%
%% De-monitors and exits a Runner.
%%
exit_runner(#rref{ref = Ref, pid = Runner}, Why) ->
    _ = erlang:demonitor(Ref, [flush]),
    _ = erlang:exit(Runner, Why),
    ok.

-spec find_job_by_id(JobId :: jid(), State :: state())
        -> {ok, jloc(), jrec()} | false | {error, term()}.
%%
%% Find the current instance of the specified Job and its location.
%%
%% `false' is returned if the Job is not (or no longer) known to the Manager.
%%
find_job_by_id(JobId, State) ->
    case c_find(JobId, State#state.loc) of
        {_, JLoc} when erlang:is_reference(JLoc) ->
            #rrec{jrec = JRec} = c_find(JLoc, State#state.run),
            {ok, JLoc, JRec};
        {_, JLoc} when JLoc =:= queue ->
            #jrec{} = JRec = c_find(JobId, State#state.que),
            {ok, JLoc, JRec};
        {_, JLoc} when JLoc =:= history ->
            #jrec{} = JRec  = c_find(JobId, State#state.hist),
            {ok, JLoc, JRec};
        false ->
            false
    end.

-spec inc_stat(stat_key() | [stat_key()], state()) -> state()
        ;     (stat_key() | [stat_key()], stats()) -> stats().
%%
%% Increment one or more statistics counters.
%%
inc_stat(Stat, #state{stats = Stats} = State) ->
    State#state{stats = inc_stat(Stat, Stats)};
inc_stat(Stat, Stats) ->
    ?StatsDict:update_counter(Stat, 1, Stats).
%%%% not currently used, hide but maintain order for later use
%%inc_stat(Stat, Stats) when not erlang:is_list(Stat) ->
%%    ?StatsDict:update_counter(Stat, 1, Stats);
%%inc_stat([Stat], Stats) ->
%%    inc_stat(Stat, Stats);
%%inc_stat([Stat | More], Stats) ->
%%    inc_stat(More, inc_stat(Stat, Stats));
%%inc_stat([], Stats) ->
%%    Stats.

-spec notify(
    What    :: jrec() | rrec() | jque() | rdict() | state(),
    Why     :: term())
        -> term().
%%
%% Notifies whoever may care of the premature disposal of the job(s) in What
%% and returns Why.
%%
notify(#state{que = Que, run = Run}, Why) ->
    _ = notify(Que, {?JOB_ERR_CANCELED, Why}),
    _ = notify(Run, {?JOB_ERR_KILLED, Why}),
    Why;

notify(#rrec{rref = RRef, jrec = JRec}, Why) ->
    _ = exit_runner(RRef, kill),
    notify(JRec, Why);

notify(#jrec{id = JobId, job = Job}, Why) ->
    case riak_core_job:killed(Job) of
        undefined ->
            _ = riak_core_job:reply(Job, {error, Job, Why});
        Killed ->
            try
                _ = riak_core_job:invoke(Killed, Why)
            catch
                Class:What ->
                    _ = lager:error(
                        "Job ~p 'killed' failure: ~p:~p", [JobId, Class, What]),
                    _ = riak_core_job:reply(Job, {error, Job, Why})
            end
    end,
    Why;

notify(Coll, Why) ->
    c_fold(fun notify/2, Why, Coll).

-spec pending(State :: state())
        -> {ok | shutdown | {error, term()}, state()}.
%%
%% Dequeue and dispatch at most one job. If there are more jobs waiting, ensure
%% that we'll get to them after handling whatever may already be waiting.
%%
%% When the queue's caught up to concurrency capacity, prune the history if
%% needed, one entry per iteration. Once in 'shutdown' mode, the history is
%% completely ignored.
%%
%% Implementation Notes:
%%
%%   Queued job counts of zero, one, and more have their own distinct behavior,
%%   so they get their own function heads. As elsewhere, we let the compile
%%   figure out the best optimization.
%%
pending(#state{shutdown = true} = State)
        when State#state.run#rd.c == 0 andalso State#state.que#jq.c == 0 ->
    {shutdown, State};

pending(#state{shutdown = true} = State) when State#state.que#jq.c == 0 ->
    {ok, State};

pending(#state{shutdown = true} = State)
        when State#state.run#rd.c == 0 andalso State#state.que#jq.c == 1 ->
    {JRec, Que} = q_pop(State#state.que),
    _ = notify(JRec, {?JOB_ERR_CANCELED, ?JOB_ERR_SHUTTING_DOWN}),
    {shutdown, State#state{
        que = Que, loc = c_erase(JRec#jrec.id, State#state.loc) }};

pending(#state{shutdown = true} = State) ->
    {JRec, Que} = q_pop(State#state.que),
    _ = notify(JRec, {?JOB_ERR_CANCELED, ?JOB_ERR_SHUTTING_DOWN}),
    {ok, check_pending(State#state{
        que = Que, loc = c_erase(JRec#jrec.id, State#state.loc) })};

pending(State)
        when    State#state.que#jq.c > 0
        andalso State#state.run#rd.c < State#state.rmax ->
    case advance(queue, running, State) of
        {ok, StateOut} ->
            {ok, check_pending(StateOut)};
        Ret ->
            Ret
    end;

pending(State) when State#state.hist#jq.c > State#state.hmax ->
    {#jrec{id = JobId}, Hist} = q_pop(State#state.hist),
    {ok, check_pending(State#state{
        hist = Hist, loc = c_erase(JobId, State#state.loc) })};

pending(State) ->
    {ok, State}.

-spec release_runner(RRef :: rref()) -> ok.
%%
%% De-monitors and releases a Runner.
%%
release_runner(#rref{ref = Ref, pid = Runner}) ->
    _ = erlang:demonitor(Ref, [flush]),
    riak_core_job_service:release(Runner).

-spec set_pending(State :: state()) -> state().
%%
%% Ensure that there's a 'pending' message in the inbox.
%%
set_pending(#state{pending = false} = State) ->
    gen_server:cast(erlang:self(), pending),
    State#state{pending = true};
set_pending(State) ->
    State.

-spec start_job(Runner :: pid(), Ref :: reference(), Job :: job())
        -> ok | {error, term()}.
%%
%% Wrapper around riak_core_job_runner:run/4 that fills in the blanks.
%%
start_job(Runner, Ref, Job) ->
    riak_core_job_runner:run(Runner, ?job_run_ctl_token,
        #mgrkey{mgr = erlang:self(), key = Ref}, Job).

-spec submit_job(
    Phase   :: atom() | {atom(), term()},
    Job     :: job(),
    From    :: {pid(), term()},
    State   :: state())
        -> {ok | {error, term()}, state()} | {error, term(), state()}.
%%
%% Accept and dispatch, or reject, the specified Job, updating statistics.
%% On entry from the external call Phase is 'submit'; other phases are internal.
%%
submit_job(submit, _, _, #state{shutdown = true} = State) ->
    {{error, ?JOB_ERR_SHUTTING_DOWN}, inc_stat(rejected_shutdown, State)};

submit_job(submit, _, _, State)
        when State#state.que#jq.c >= State#state.qmax ->
    {{error, ?JOB_ERR_QUEUE_OVERFLOW}, inc_stat(rejected_overflow, State)};

submit_job(submit, Job, From, State) ->
    submit_job({runnable, riak_core_job:runnable(Job)}, Job, From, State);

submit_job({runnable, true}, Job, {Caller, _} = From, State) ->
    Class   = riak_core_job:class(Job),
    Accept  = riak_core_util:job_class_enabled(Class),
    _ = riak_core_util:report_job_request_disposition(
            Accept, Class, ?MODULE, submit_job, ?LINE, Caller),
    submit_job(Accept, Job, From, State);

submit_job({runnable, Error}, _, _, State) ->
    {Error, inc_stat(not_runnable, State)};

submit_job(true, Job, _From, State) ->
    Dest = if
        State#state.run#rd.c < State#state.rmax ->
            running;
        ?else ->
            queue
    end,
    case advance(Job, Dest, inc_stat(accepted, State)) of
        {ok, _} = Ret ->
            Ret;
        {{error, Error}, ErrState} ->
            {error, Error, ErrState}
    end;

submit_job(false, _, _, State) ->
    {{error, ?JOB_ERR_REJECTED}, inc_stat(rejected, State)}.

-spec update_job(MgrKey :: mgr_key(), Stat :: stat_key()) -> ok.
%%
%% Update a Job's Stat with the current time.
%%
update_job(#mgrkey{mgr = Mgr, key = Key}, Stat) ->
    Update = {Key, update, Stat, riak_core_job:timestamp()},
    gen_server:cast(Mgr, Update).

-spec update_job(MgrKey :: mgr_key(), Stat :: atom(), Info :: term()) -> ok.
%%
%% Update a Job's Stat with the current time and additional Info.
%%
update_job(#mgrkey{mgr = Mgr, key = Key}, Stat, Info) ->
    Update = {Key, update, Stat, riak_core_job:timestamp(), Info},
    gen_server:cast(Mgr, Update).

%% ===================================================================
%% Collections
%% ===================================================================
%%
%% These collections provide a common interface to the assorted dictionaries
%% and queues in the state.
%% Among other things, they fail gracefully so for the most part their
%% results don't need to be checked, and their input and output patterns are
%% the same across types.
%% In all cases they raise an informative exception if any parameter does not
%% strictly match the expected type.
%% There are a couple of places where case statements are strongly typed to
%% result values that are indicative of what's already been put into a
%% collection, and if the wrong data was there they'd cause a case clause
%% exception, but it's hoped we won't hit that and the safety code would be
%% overly pessimistic.
%%
%% Implementation Notes:
%%
%%  These collections are specific to this module, they're not intended to be
%%  general purpose. For instance, inserting a key that already exists into a
%%  collection other than the location dictionary is an error and *may* be
%%  flagged as such, depending on how onerous it is to do so. Some of the
%%  state consistency checking may be removed in time ... or not.
%%
%%  For the most part, the collections functions do not call themselves
%%  recursively when peeling apart records. This is deliberate, since we
%%  assume that when a key (the usual culprit) is found within a typed record
%%  that we can trust its type, but if it's passed as a naked parameter we
%%  should verify the type with guards, some of which are hefty. Instead, the
%%  function code is duplicated, in the hope that the compiler will jump over
%%  the redundant type check and optimize away the duplicate.
%%  That's the plan, anyway.
%%
%%  Find, remove, and erase operations are always performed by the Key even
%%  if the entire record is provided, on the assumption that an update may be
%%  underway and the full record *may* not match.
%%

-spec c_erase(Key :: ckey() | crec(), Coll :: coll()) -> coll().
%%
%% Erase Key/Rec from collection Coll.
%% If you care whether it was there in the first place, use c_remove/2.
%%
c_erase({Key, Val}, #ld{d = D} = Coll) when ?is_job_loc(Val) ->
    Coll#ld{d = dict:erase(Key, D)};
c_erase(Key, #ld{d = D} = Coll) when ?is_job_gid(Key) ->
    Coll#ld{d = dict:erase(Key, D)};
c_erase(#rrec{}, #rd{c = 0} = Coll) ->
    Coll;
c_erase(Key, #rd{c = 0} = Coll) when erlang:is_reference(Key) ->
    Coll;
c_erase(#rrec{rref = #rref{ref = Key}}, #rd{d = D} = Coll) ->
    Dict = dict:erase(Key, D),
    Coll#rd{c = dict:size(Dict), d = Dict};
c_erase(Key, #rd{d = D} = Coll) when erlang:is_reference(Key) ->
    Dict = dict:erase(Key, D),
    Coll#rd{c = dict:size(Dict), d = Dict};
%%%% not currently used, hide but maintain order for later use
%%c_erase(Key, #jq{c = 0} = Coll) when ?is_job_gid(Key) ->
%%    Coll;
%%c_erase(#jrec{id = Key}, #jq{c = C, d = D} = Coll) ->
%%    List = queue:to_list(D),
%%    case lists:keytake(Key, #jrec.id, List) of
%%        {value, #jrec{}, Que} ->
%%            Coll#jq{c = (C - 1), d = queue:from_list(Que)};
%%        false ->
%%            Coll
%%    end;
%%c_erase(Key, #jq{c = C, d = D} = Coll) when ?is_job_gid(Key) ->
%%    List = queue:to_list(D),
%%    case lists:keytake(Key, #jrec.id, List) of
%%        {value, #jrec{}, Que} ->
%%            Coll#jq{c = (C - 1), d = queue:from_list(Que)};
%%        false ->
%%            Coll
%%    end;
c_erase(Key, Coll) ->
    ?UNMATCHED_ARGS([Key, Coll]).

-spec c_find(Key :: ckey() | crec(), Coll :: coll()) -> crec() | false.
%%
%% Return Rec from collection Coll, or false if it is not found.
%%
c_find({Key, Val}, #ld{} = Coll) when ?is_job_loc(Val) ->
    c_find(Key, Coll);
c_find(Key, #ld{d = D}) when ?is_job_gid(Key) ->
    case dict:find(Key, D) of
        {ok, Val} ->
            {Key, Val};
        error ->
            false
    end;
c_find(#rrec{}, #rd{c = 0}) ->
    false;
c_find(Key, #rd{c = 0}) when erlang:is_reference(Key) ->
    false;
c_find(#rrec{rref = #rref{ref = Key}}, #rd{d = D}) ->
    case dict:find(Key, D) of
        {ok, Rec} ->
            Rec;
        error ->
            false
    end;
c_find(Key, #rd{d = D}) when erlang:is_reference(Key) ->
    case dict:find(Key, D) of
        {ok, Rec} ->
            Rec;
        error ->
            false
    end;
c_find(Key, #jq{c = 0}) when ?is_job_gid(Key) ->
    false;
c_find(#jrec{id = Key}, #jq{d = D}) ->
    lists:keyfind(Key, #jrec.id, queue:to_list(D));
c_find(Key, #jq{d = D}) when ?is_job_gid(Key) ->
    lists:keyfind(Key, #jrec.id, queue:to_list(D));
c_find(Key, Coll) ->
    ?UNMATCHED_ARGS([Key, Coll]).

-spec c_fold(
    Fun :: fun((crec(), term()) -> term()), Accum :: term(), Coll :: coll())
        -> term().
%%
%% Fold over all of the records in Coll.
%%
c_fold(Fun, Accum, #ld{d = D}) when erlang:is_function(Fun, 2) ->
    dict:fold(fun(K, V, A) -> Fun({K, V}, A) end, Accum, D);
c_fold(Fun, Accum, #rd{c = 0}) when erlang:is_function(Fun, 2) ->
    Accum;
c_fold(Fun, Accum, #rd{d = D}) when erlang:is_function(Fun, 2) ->
    dict:fold(fun(_, Rec, A) -> Fun(Rec, A) end, Accum, D);
c_fold(Fun, Accum, #jq{c = 0}) when erlang:is_function(Fun, 2) ->
    Accum;
c_fold(Fun, Accum, #jq{d = D}) when erlang:is_function(Fun, 2) ->
    lists:foldl(Fun, Accum, queue:to_list(D));
c_fold(Fun, Accum, Coll) ->
    ?UNMATCHED_ARGS([Fun, Accum, Coll]).

-spec c_remove(Key :: ckey() | crec(), Coll :: coll())
        -> {crec() | false, coll()}.
%%
%% Remove and return Rec from collection Coll, or false if it is not found.
%%
%%%% not currently used, hide but maintain order for later use
%%c_remove({Key, Val}, #ld{} = Coll) when ?is_job_loc(Val) ->
%%    c_remove(Key, Coll);
%%c_remove(Key, #ld{d = D} = Coll) when ?is_job_gid(Key) ->
%%    case dict:find(Key, D) of
%%        {ok, Val} ->
%%            {{Key, Val}, Coll#ld{d = dict:erase(Key, D)}};
%%        error ->
%%            {false, Coll}
%%    end;
c_remove(#rrec{}, #rd{c = 0} = Coll) ->
    {false, Coll};
c_remove(Key, #rd{c = 0} = Coll) when erlang:is_reference(Key) ->
    {false, Coll};
c_remove(#rrec{rref = #rref{ref = Key}}, #rd{c = C, d = D} = Coll) ->
    case dict:find(Key, D) of
        {ok, Rec} ->
            {Rec, Coll#rd{c = (C - 1), d = dict:erase(Key, D)}};
        error ->
            {false, Coll}
    end;
c_remove(Key, #rd{c = C, d = D} = Coll) when erlang:is_reference(Key) ->
    case dict:find(Key, D) of
        {ok, Rec} ->
            {Rec, Coll#rd{c = (C - 1), d = dict:erase(Key, D)}};
        error ->
            {false, Coll}
    end;
c_remove(Key, #jq{c = 0} = Coll) when ?is_job_gid(Key) ->
    {false, Coll};
c_remove(#jrec{id = Key}, #jq{c = C, d = D} = Coll) ->
    List = queue:to_list(D),
    case lists:keytake(Key, #jrec.id, List) of
        {value, #jrec{} = Rec, Que} ->
            {Rec, Coll#jq{c = (C - 1), d = queue:from_list(Que)}};
        false ->
            {false, Coll}
    end;
c_remove(Key, #jq{c = C, d = D} = Coll) when ?is_job_gid(Key) ->
    List = queue:to_list(D),
    case lists:keytake(Key, #jrec.id, List) of
        {value, #jrec{} = Rec, Que} ->
            {Rec, Coll#jq{c = (C - 1), d = queue:from_list(Que)}};
        false ->
            {false, Coll}
    end;
c_remove(Key, Coll) ->
    ?UNMATCHED_ARGS([Key, Coll]).

-spec c_store(Rec :: crec(), Coll :: coll()) -> coll().
%%
%% Add/Update Rec in collection Coll.
%%
c_store({Key, Val}, #ld{d = D} = Coll)
        when ?is_job_loc(Val) andalso ?is_job_gid(Key) ->
    Coll#ld{d = dict:store(Key, Val, D)};
c_store(#rrec{rref = #rref{ref = Key}} = Val, #rd{d = D} = Coll) ->
    % It's not an error for the key to already be present (because jobs get
    % updated as they run), so we get the size after the update as that's
    % more efficient than checking for the key beforehand.
    Dict = dict:store(Key, Val, D),
    Coll#rd{c = dict:size(Dict), d = Dict};
c_store(#jrec{} = Rec, #jq{c = C, d = D} = Coll) ->
    Coll#jq{c = (C + 1), d = queue:in(Rec, D)};
c_store(Rec, Coll) ->
    ?UNMATCHED_ARGS([Rec, Coll]).

-spec q_pop(Que :: jque()) -> {jrec() | false, jque()}.
%%
%% Remove and return the record at the front of Que, or false if the queue is
%% empty.
%%
q_pop(#jq{c = 0} = Que) ->
    {false, Que};
q_pop(#jq{c = C, d = D} = Que) ->
    {{value, Val}, New} = queue:out(D),
    {Val, Que#jq{c = (C - 1), d = New}}.
