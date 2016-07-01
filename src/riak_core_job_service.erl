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

-module(riak_core_job_service).
-behaviour(gen_server).

% Public API
-export([
    config/1,
    stats/1,
    submit/2
]).

% Public types
-export_type([
    cbfunc/0,
    config/0,
    scope_id/0,
    scope_index/0,
    scope_type/0,
    paccept/0,
    pconcur/0,
    pquemax/0,
    prop/0
]).

% Private API
-export([
    accept_any/2,
    cleanup/2,
    done/3,
    register/3,
    running/2,
    shutdown/2,
    start_link/3,
    starting/2
]).

% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("riak_core_job_internal.hrl").

-type cbfunc()  ::  {module(), atom(), [term()]} | {fun(), [term()]}.

%% The 'accept' callback is invoked as if by
%%
%%  accept(node_id(), job(), Arg1 .. ArgN) -> boolean()
%%
%% As such, the arity of the supplied function must be 2 + length(Args).
%% If provided, the callback is invoked with riak_core_job:dummy() during
%% service initialization, and startup fails if the result is anything other
%% than a boolean().
%% If this property is not provided, all jobs are accepted.
-type paccept() ::  {'job_svc_accept_func', cbfunc()}.

%% Defaults to ?JOB_SVC_DEFAULT_CONCUR.
-type pconcur() ::  {'job_svc_concurrency_limit', pos_integer()}.

%% Defaults to job_svc_concurrency_limit * ?JOB_SVC_DEFAULT_QUEMULT.
-type pquemax() ::  {'job_svc_queue_limit', non_neg_integer()}.

-type prop()    ::  paccept() | pconcur() | pquemax().
-type config()  ::  [prop()].

%% If defined, the State is validated before return whenever it's co-dependent
%% fields are changed.
%% The validation code is conditional on whether this macro is defined, not
%% its value.
%% This is fairly heavyweight, and should NOT be defined in production builds.
-define(VALIDATE_STATE, true).

%% In case we want to change the type of dict used for stats. Unlikely, but ...
-define(StatsDict,  dict).
-ifdef(namespaced_types).
-type stats()   ::  dict:dict().
-else.
-type stats()   ::  dict().
-endif.

-define(inc_stat(Stat, Stats),  ?StatsDict:update_counter(Stat, 1, Stats)).
-define(stats_list(Stats),      ?StatsDict:to_list(Stats)).

-type job() :: riak_core_job:job().
-record(mon, {
    pid     ::  pid(),
    mon     ::  reference(),
    job     ::  job()
}).
-type mon() :: #mon{}.

-define(DefaultAccept,  {?MODULE, 'accept_any', []}).
-define(DefaultConcur,  ?JOB_SVC_DEFAULT_CONCUR).

%% MUST keep queue/quelen and monitors/running in sync!
%% They're separate to allow easy decisions based on active/queued work,
%% but obviously require close attention.
-record(state, {
    work_sup    = 'undefined'       ::  pid() | 'undefined',
    scope_id    = {?MODULE, 0}      ::  scope_id(),
    vnode       = 'undefined'       ::  pid() | 'undefined',
    config      = []                ::  config(),
    accept      = ?DefaultAccept    ::  cbfunc(),
    concur      = ?DefaultConcur    ::  pos_integer(),
    maxque      = 0                 ::  non_neg_integer(),
    dqpending   = 'false'           ::  boolean(),
    queue       = []                ::  [job()],
    quelen      = 0                 ::  non_neg_integer(),
    running     = 0                 ::  non_neg_integer(),
    monitors    = []                ::  [mon()],
    pending     = []                ::  [{reference(), job()}],
    shutdown    = 'false'           ::  boolean(),
    stats       = ?StatsDict:new()  ::  stats()
}).
-type state()   ::  #state{}.

-ifdef(VALIDATE_STATE).
-define(validate(State),  validate(State)).
-else.
-define(validate(State),  State).
-endif.

%% ===================================================================
%% Public API
%% ===================================================================

-spec config(pid()) -> config() | {'error', term()}.
%%
%% @doc Return the configuration the scope's service was started with.
%%
%% This is mainly so the riak_core_job_manager can repopulate its cache from a
%% running supervision tree after a restart. It matters that this is the input
%% configuration and not the result of applying defaults to come up with what's
%% running.
%%
%% The actual running configuration is reported by stats/1.
%%
config(Svc) when erlang:is_pid(Svc) ->
    gen_server:call(Svc, 'config');
config({NType, _} = ScopeID) when erlang:is_atom(NType) ->
    case riak_core_job_manager:lookup(?SCOPE_SVC_ID(ScopeID)) of
        Svc when erlang:is_pid(Svc) ->
            gen_server:call(Svc, 'config');
        _ ->
            {'error', 'noproc'}
    end.

-spec stats(scope_id() | pid()) -> [{atom(), term()}] | {'error', term()}.
%%
%% @doc Return statistics from the specified scope's service.
%%
stats(Svc) when erlang:is_pid(Svc) ->
    gen_server:call(Svc, 'stats');
stats({NType, _} = ScopeID) when erlang:is_atom(NType) ->
    case riak_core_job_manager:lookup(?SCOPE_SVC_ID(ScopeID)) of
        Svc when erlang:is_pid(Svc) ->
            gen_server:call(Svc, 'stats');
        _ ->
            {'error', 'noproc'}
    end.

-spec submit(pid() | scope_id() | scope_type() | [scope_id()], job())
        -> ok | {'error', term()}.
%%
%% @doc Submit a job to run on the specified scope(s).
%%
submit(Svc, Job) when erlang:is_pid(Svc) ->
    case riak_core_job:version(Job) of
        {'job', _} ->
            gen_server:call(Svc, {'submit', Job});
        _ ->
            erlang:error('badarg', [Job])
    end;
submit({NType, _} = ScopeID, Job) when erlang:is_atom(NType) ->
    case riak_core_job:version(Job) of
        {'job', _} ->
            case riak_core_job_manager:job_svc(ScopeID) of
                Svc when erlang:is_pid(Svc) ->
                    gen_server:call(Svc, {'submit', Job});
                {'error', _} = Error ->
                    Error
            end;
        _ ->
            erlang:error('badarg', [Job])
    end;
submit(Multi, Job) ->
    riak_core_job_manager:submit_mult(Multi, Job).

%% ===================================================================
%% Private API
%% ===================================================================

-spec shutdown(pid(), non_neg_integer() | 'infinity') -> ok.
%%
%% @doc Tell the service to shut down within Timeout milliseconds.
%%
%% There's no facility here to notify the caller when the shutdown is
%% completed. If the caller wants to be notified when the shutdown completes,
%% they should monitor the service's Pid BEFORE calling this function.
%%
%% Note that the service is normally part of a supervision tree that will
%% restart it when it exits for any reason. To shut it down properly, the
%% shutdown should be initiated by the controlling manager's stop_scope/1
%% function.
%%
shutdown(Svc, Timeout)
        when erlang:is_integer(Timeout) andalso Timeout >= (1 bsl 32) ->
    shutdown(Svc, 'infinity');
shutdown(Svc, 'infinity' = Timeout) when erlang:is_pid(Svc) ->
    gen_server:cast(Svc, {'shutdown', Timeout});
shutdown(Svc, Timeout) when erlang:is_pid(Svc)
        andalso erlang:is_integer(Timeout) andalso Timeout >= 0 ->
    gen_server:cast(Svc, {'shutdown', Timeout}).

-spec start_link(scope_svc_id(), config(), job())
        -> {'ok', pid()} | {'error', term()}.
%%
%% @doc Start a new process linked to the calling process.
%%
start_link(?SCOPE_SVC_ID(_) = Id, Config, DummyJob) ->
    gen_server:start_link(?MODULE, {Id, Config, DummyJob}, []).

-spec register(pid(), work_sup_id(), pid()) -> 'ok'.
%%
%% @doc Notify the server where to find its worker supervisor.
%%
%% This is being called from the riak_core_job_manager, so it can't call into
%% it to look up the service, hence the direct call to the Pid.
%%
register(Svc, ?WORK_SUP_ID(_) = Id, Sup) ->
    gen_server:cast(Svc, {'register', Id, Sup}).

-spec starting(pid(), reference()) -> 'ok'.
%%
%% @doc Callback for the job runner indicating the job's UOW is being started.
%%
starting(Svc, JobRef) ->
    update_job(Svc, JobRef, 'started').

-spec running(pid(), reference()) -> 'ok'.
%%
%% @doc Callback for the job runner indicating the job's UOW is being run.
%%
running(Svc, JobRef) ->
    update_job(Svc, JobRef, 'running').

-spec cleanup(pid(), reference()) -> 'ok'.
%%
%% @doc Callback for the job runner indicating the job's UOW is being cleaned up.
%%
cleanup(Svc, JobRef) ->
    update_job(Svc, JobRef, 'cleanup').

-spec done(pid(), reference(), term()) -> 'ok'.
%%
%% @doc Callback for the job runner indicating the job's UOW is finished.
%%
done(Svc, JobRef, Result) ->
    update_job(Svc, JobRef, 'done', Result).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

-spec init({scope_svc_id(), config(), job()})
        -> {'ok', state()} | {'stop', {'error', term()}}.
init({?SCOPE_SVC_ID(ScopeID) = Id, Config, DummyJob}) ->
    case init_state(#state{scope_id = ScopeID, config = Config}, DummyJob) of
        #state{} = State ->
            _ = erlang:process_flag('trap_exit', 'true'),
            _ = riak_core_job_manager:register(Id, erlang:self()),
            {'ok', State};
        Error ->
            {'stop', Error}
    end.

-spec handle_call(term(), {pid(), term()}, state())
        -> {'reply', term(), state()}.
%
% submit(node_id() | pid(), job()) -> ok | {'error', term()}.
%
handle_call({'submit', _}, _, #state{shutdown = 'true', stats = S} = State) ->
    {'reply', {'error', ?JOB_ERR_SHUTTING_DOWN},
        State#state{stats = ?inc_stat('rejected_shutdown', S)}};
handle_call({'submit', _}, _, #state{
        quelen = L, maxque = M, stats = S} = State) when L >= M ->
    {'reply', {'error', ?JOB_ERR_QUEUE_OVERFLOW},
        State#state{stats = ?inc_stat('rejected_overflow', S)}};
handle_call({'submit', Job}, _, #state{
        concur = C, running = R, stats = S} = State) ->
    case riak_core_job:runnable(Job) of
        'true' ->
            case accept_job(Job, State) of
                'true' ->
                    NewState = State#state{stats = ?inc_stat('accepted', S)},
                    if
                        R < C ->
                            {'reply', 'ok', start_job(Job, NewState)};
                        ?else ->
                            {'reply', 'ok', queue_job(Job, NewState)}
                    end;
                'false' ->
                    {'reply', {'error', ?JOB_ERR_REJECTED},
                        State#state{stats = ?inc_stat('rejected', S)}}
            end;
        Error ->
            {'reply', Error, State#state{stats = ?inc_stat('accept_errors', S)}}
    end;
%
% stats(node_id() | pid()) -> [{atom(), term()}] | {'error', term()}.
%
handle_call('stats', _, State) ->
    Result = [
        {'status',  if
                        State#state.shutdown ->
                            'stopping';
                        ?else ->
                            'active'
                    end},
        {'concur',  State#state.concur},
        {'maxque',  State#state.maxque},
        {'running', State#state.running},
        {'inqueue', State#state.quelen},
        {'pending', erlang:length(State#state.pending)}
        | ?stats_list(State#state.stats)],
    {'reply', Result, State};
%
% config(pid()) -> config() | {'error', term()}.
%
handle_call('config', _, #state{config = Config} = State) ->
    {'reply', Config, State};
%
% unrecognized message
%
handle_call(Message, From, #state{scope_id = ScopeID, stats = S} = State) ->
    _ = lager:error(
            "~p job service received unhandled call from ~p: ~p",
            [ScopeID, From, Message]),
    {'reply', {'error', {'badarg', Message}},
        State#state{stats = ?inc_stat('unhandled', S)}}.

-spec handle_cast(term(), state())
        -> {'noreply', state()} | {'stop', term(), state()}.
%
% internal deque message
%
handle_cast('dequeue', State) ->
    dequeue(State);
%
% status update from a running job
%
handle_cast({Ref, 'update', 'done' = Stat, _TS, Result}, #state{
        scope_id = ScopeID, running = R, monitors = M, stats = S} = State) ->
    _ = erlang:demonitor(Ref, ['flush']),
    case lists:keytake(Ref, #mon.mon, M) of
        {'value', _, NewM} ->
            dequeue(?validate(State#state{
                monitors = NewM, running = (R - 1),
                stats = ?inc_stat('finished', S)}));
        _ ->
            _ = lager:error(
                    "~p job service received completion message ~p:~p "
                    "for unknown job ref ~p", [ScopeID, Stat, Result, Ref]),
            {'noreply', State#state{stats = ?inc_stat('update_errors', S)}}
    end;
handle_cast({Ref, 'update', Stat, TS}, #state{
        scope_id = ScopeID, monitors = M} = State) ->
    case lists:keyfind(Ref, #mon.mon, M) of
        #mon{job = Job} = Rec ->
            New = Rec#mon{job = riak_core_job:update(Stat, TS, Job)},
            {'noreply', ?validate(State#state{
                monitors = lists:keystore(Ref, #mon.mon, M, New)})};
        _ ->
            _ = lager:error(
                    "~p job service received ~p update for unknown job ref ~p",
                    [ScopeID, Stat, Ref]),
            {'noreply', State}
    end;
%
% register(WorkSupId)
%
handle_cast({'register', ?WORK_SUP_ID(_), Sup}, #state{quelen = 0} = State) ->
    _ = erlang:monitor('process', Sup),
    {'noreply', State#state{work_sup = Sup}};
handle_cast({'register', ?WORK_SUP_ID(_), Sup}, State) ->
    _ = erlang:monitor('process', Sup),
    dequeue(State#state{work_sup = Sup});
%
% shutdown(Timeout)
%
handle_cast({'shutdown' = Why, _}, #state{quelen = 0, running = 0} = State) ->
    {'stop', Why, State#state{shutdown = 'true'}};
handle_cast({'shutdown' = Why, 0}, State) ->
    _ = discard(State, ?JOB_ERR_SHUTTING_DOWN),
    {'stop', Why, State#state{shutdown = 'true',
        queue = [], quelen = 0, monitors = [], running = 0}};
handle_cast({'shutdown', 'infinity'}, State) ->
    dequeue(State#state{shutdown = 'true'});
handle_cast({'shutdown', Timeout}, State) ->
    {'ok', _} = timer:apply_after(Timeout,
        'gen_server', 'cast', [erlang:self(), {'shutdown', 0}]),
    handle_cast({'shutdown', 'infinity'}, State);
%
% unrecognized message
%
handle_cast(Message, #state{scope_id = ScopeID, stats = S} = State) ->
    _ = lager:error(
            "~p job service received unhandled cast: ~p", [ScopeID, Message]),
    {'noreply', State#state{stats = ?inc_stat('unhandled', S)}}.

-spec handle_info(term(), state())
        -> {'noreply', state()} | {'stop', term(), state()}.
%
% Our work supervisor exited - not good at all.
% When the supervisor went down, it took all of the running jobs with it, so
% we'll get 'DOWN' messages from each of them - no need to handle them here.
%
handle_info({'DOWN', _, _, Sup, Info},
        #state{work_sup = Sup, scope_id = ScopeID} = State) ->
    _ = lager:error("~p work supervisor ~p exited: ~p", [ScopeID, Sup, Info]),
    {'noreply', State#state{work_sup = 'undefined'}};
%
% A monitored job crashed or was killed.
% If it completed normally, a 'done' update arrived in our mailbox before this
% and caused the monitor to be released and flushed, so the only way we get
% this is an exit before completion.
%
handle_info({'DOWN', Ref, _, Pid, Info}, #state{
        scope_id = ScopeID, running = R, monitors = M, stats = S} = State) ->
    case lists:keytake(Ref, #mon.mon, M) of
        {'value', #mon{pid = Pid, job = Job}, NewM} ->
            _ = discard(Job, {?JOB_ERR_CRASHED, Info}),
            dequeue(?validate(State#state{monitors = NewM, running = (R - 1),
                stats = ?inc_stat('crashed', S)}));
        _ ->
            _ = lager:error(
                    "~p job service received 'DOWN' message "
                    "for unrecognized process ~p", [ScopeID, Pid]),
            {'noreply', State#state{stats = ?inc_stat('update_errors', S)}}
    end;
%
% unrecognized message
%
handle_info(Message, #state{scope_id = ScopeID, stats = S} = State) ->
    _ = lager:error(
            "~p job service received unhandled info: ~p", [ScopeID, Message]),
    {'noreply', State#state{stats = ?inc_stat('unhandled', S)}}.

-spec terminate(term(), state()) -> ok.
%
% no matter why we're terminating, de-monitor everything we're watching
%
terminate('inconsistent', #state{scope_id = ScopeID} = State) ->
    _ = lager:error(
            "~p job service terminated due to inconsistent state: ~p",
            [ScopeID, State]),
    terminate('shutdown', State);
terminate(_, State) ->
    _ = discard(State, ?JOB_ERR_SHUTTING_DOWN),
    'ok'.

-spec code_change(term(), state(), term()) -> {'ok', state()}.
%
% at present we don't care, so just carry on
%
code_change(_, State, _) ->
    {'ok', State}.

%% ===================================================================
%% Internal
%% ===================================================================

-spec accept_job(job(), state()) -> boolean().
accept_job(Job, #state{scope_id = ScopeID, accept = Accept}) ->
    invoke(Accept, [ScopeID, Job]).

-spec accept_any(scope_id(), job()) -> 'true'.
accept_any(_, _) ->
    'true'.

-spec dequeue(state()) -> {'noreply' | 'stop', state()}.
%% Dequeue and dispatch at most one job. If there are more jobs waiting, ensure
%% that we'll get to them after handling whatever may already be waiting.
dequeue(#state{shutdown = 'true', quelen = 0, running = 0} = State) ->
    {'stop', State};
dequeue(#state{quelen = 0} = State) ->
    {'noreply', State};
dequeue(#state{shutdown = 'true', queue = [Job | Q], quelen = L} = State) ->
    _ = discard(Job, {?JOB_ERR_CANCELED, ?JOB_ERR_SHUTTING_DOWN}),
    {'noreply', maybe_dequeue(
        ?validate(State#state{queue = Q, quelen = (L - 1)}))};
dequeue(#state{concur = C, running = R} = State) when R >= C ->
    {'noreply', State};
dequeue(#state{work_sup = 'undefined'} = State) ->
    {'noreply', maybe_dequeue(State)};
dequeue(#state{queue = [Job | Q], quelen = L} = State) ->
    {'noreply', maybe_dequeue(start_job(Job,
        ?validate(State#state{queue = Q, quelen = (L - 1)})))}.

-spec discard(job() | mon() | [job()] | [mon()] | state(), term()) -> term().
%% Disposes of the job(s) in the specified item and returns the Why parameter
%% for recursive (or lists:fold) use.
discard([], Why) ->
    Why;
discard([Elem | Elems], Why) ->
    discard(Elems, discard(Elem, Why));
discard(#state{queue = Q, monitors = M}, Why) ->
    _ = discard(Q, {?JOB_ERR_CANCELED,  Why}),
    _ = discard(M, {?JOB_ERR_KILLED,    Why}),
    Why;
discard(#mon{pid = Pid, mon = Ref, job = Job}, Why) ->
    _ = erlang:demonitor(Ref, ['flush']),
    _ = erlang:exit(Pid, 'kill'),
    discard(Job, Why);
discard(Job, Why) ->
    case riak_core_job:get('killed', Job) of
        'undefined' ->
            _ = riak_core_job:reply(Job, {'error', Why});
        Killed ->
            try
                _ = invoke(Killed, [Why])
            catch
                Class:What ->
                    _ = lager:error("Job ~p 'killed' failure: ~p:~p",
                            [riak_core_job:get('gid', Job), Class, What]),
                    _ = riak_core_job:reply(Job, {'error', Why})
            end
    end,
    Why.

-spec queue_job(job(), state()) -> state().
%% Queue the specified Job. This assumes ALL checks have been performed
%% beforehand - NONE are performed here!
queue_job(Job, #state{queue = Q, quelen = L, stats = S} = State) ->
    maybe_dequeue(?validate(State#state{
        queue   = Q ++ [riak_core_job:update('queued', Job)],
        quelen  = (L + 1),
        stats   = ?inc_stat('queued', S)})).

-spec start_job(job(), state()) -> state().
%% Dispatch the specified Job on a runner process. This assumes ALL checks
%% have been performed beforehand - NONE are performed here!
start_job(Job, #state{
        work_sup = W, monitors = M, running = R, stats = S} = State) ->
    {'ok', Pid} = supervisor:start_child(W, []),
    Mon = erlang:monitor('process', Pid),
    Rec = #mon{pid = Pid, mon = Mon, job = Job},
    'ok' = riak_core_job_runner:run(
        Pid, erlang:self(), Mon, riak_core_job:get('work', Job)),
    ?validate(State#state{
        monitors = [Rec | M], running = (R + 1),
        stats = ?inc_stat('dispatched', S)}).

-spec maybe_dequeue(state()) -> state().
%% If there are queued jobs make sure there's a message in our inbox to get to
%% them after handling whatever may already be waiting.
maybe_dequeue(#state{dqpending = 'true'} = State) ->
    State;
maybe_dequeue(#state{quelen = 0} = State) ->
    State;
maybe_dequeue(#state{
        shutdown = 'false', concur = C, running = R} = State) when R >= C ->
    State;
maybe_dequeue(State) ->
    ?cast('dequeue'),
    State#state{dqpending = 'true'}.

-spec update_job(pid(), reference(), atom()) -> 'ok'.
update_job(Svc, JobRef, Key) ->
    gen_server:cast(Svc,
        {JobRef, 'update', Key, riak_core_job:timestamp()}).

-spec update_job(pid(), reference(), atom(), term()) -> 'ok'.
update_job(Svc, JobRef, Key, Info) ->
    gen_server:cast(Svc,
        {JobRef, 'update', Key, riak_core_job:timestamp(), Info}).

-spec init_state(state(), job()) -> state() | {'error', term()}.
init_state(State, Dummy) ->
    % field order matters!
    init_state(['accept', 'concur', 'maxque'], Dummy, State).

-spec init_state([atom()], job(), state()) -> state() | {'error', term()}.
init_state(['accept' | Fields], Dummy,
        #state{config = Config, scope_id = ScopeID} = State) ->
    case proplists:get_value('job_svc_accept_func', Config) of
        'undefined' ->
            init_state(Fields, Dummy, State#state{accept = ?DefaultAccept});
        {Mod, Func, Args} = Accept
                when erlang:is_atom(Mod)
                andalso erlang:is_atom(Func)
                andalso erlang:is_list(Args) ->
            Arity = (erlang:length(Args) + 2),
            case erlang:function_exported(Mod, Func, Arity) of
                true ->
                    case init_test_accept(Accept, ScopeID, Dummy) of
                        'ok' ->
                            init_state(Fields, Dummy,
                                State#state{accept = Accept});
                        Error ->
                            Error
                    end;
                _ ->
                    {'error',
                        {'job_svc_accept_func', {'undef', {Mod, Func, Arity}}}}
            end;
        {Fun, Args} = Accept
                when erlang:is_function(Fun)
                andalso erlang:is_list(Args) ->
            Arity = (erlang:length(Args) + 2),
            case erlang:fun_info(Fun, 'arity') of
                {_, Arity} ->
                    case init_test_accept(Accept, ScopeID, Dummy) of
                        'ok' ->
                            init_state(Fields, Dummy,
                                State#state{accept = Accept});
                        Error ->
                            Error
                    end;
                _ ->
                    {'error',
                        {'job_svc_accept_func', {'badarity', {Fun, Arity}}}}
            end;
        Spec ->
            {'error', {'job_svc_accept_func', {'badarg', Spec}}}
    end;
init_state(['concur' | Fields], Dummy, #state{config = Config} = State) ->
    case proplists:get_value('job_svc_concurrency_limit', Config) of
        'undefined' ->
            init_state(Fields, Dummy,
                State#state{concur = ?JOB_SVC_DEFAULT_CONCUR});
        Concur when erlang:is_integer(Concur) andalso Concur > 0 ->
            init_state(Fields, Dummy, State#state{concur = Concur});
        BadArg ->
            {'error', {'job_svc_concurrency_limit', {'badarg', BadArg}}}
    end;
init_state(['maxque' | Fields], Dummy,
        #state{config = Config, concur = Concur} = State) ->
    case proplists:get_value('job_svc_queue_limit', Config) of
        'undefined' ->
            init_state(Fields, Dummy,
                State#state{maxque = (Concur * ?JOB_SVC_DEFAULT_QUEMULT)});
        MaxQue when erlang:is_integer(MaxQue) andalso MaxQue >= 0 ->
            init_state(Fields, Dummy, State#state{maxque = MaxQue});
        BadArg ->
            {'error', {'job_svc_queue_limit', {'badarg', BadArg}}}
    end;
init_state([], _, State) ->
    State.

-spec init_test_accept(cbfunc(), scope_id(), job()) -> 'ok' | {'error', term()}.
init_test_accept(Accept, ScopeID, Job) ->
    try
        case invoke(Accept, [ScopeID, Job]) of
            'true' ->
                'ok';
            'false' ->
                'ok';
            Other ->
                {'error', {'job_svc_accept_func', {'badmatch', Other}}}
        end
    catch
        Class:What ->
            {'error', {'job_svc_accept_func', {Class, What}}}
    end.

-spec invoke(cbfunc(), list()) -> term() | no_return().
invoke({Mod, Func, Args}, Prepend) ->
    erlang:apply(Mod, Func, Prepend ++ Args);
invoke({Fun, Args}, Prepend) ->
    erlang:apply(Fun, Prepend ++ Args).

-ifdef(VALIDATE_STATE).
-spec validate(state()) -> state() | no_return().
validate(State) ->
    QL = erlang:length(State#state.queue),
    R1 = if
        QL /= State#state.quelen ->
            [{'quelen', QL, State#state.quelen}];
        ?else ->
            []
    end,
    R2 = if
        QL > State#state.maxque ->
            [{'maxque', QL, State#state.maxque} | R1];
        ?else ->
            R1
    end,
    ML = erlang:length(State#state.monitors),
    R3 = if
        ML /= State#state.running ->
            [{'running', ML, State#state.running} | R2];
        ?else ->
            R2
    end,
    R4 = if
        ML > State#state.concur ->
            [{'concur', ML, State#state.concur} | R3];
        ?else ->
            R3
    end,
    case R4 of
        [] ->
            State;
        Err ->
            lager:error("Inconsistent state: ~p", State),
            erlang:error({'invalid_state', Err}, [State])
    end.
-endif.
