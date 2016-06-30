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

-module(riak_core_vnode_worker_pool).
%%
%% @doc This module is deprecated and will be removed in v3!
%%
%% This is a facade over riak_core_job_service, and ALL new code should be
%% using that API, not this one. Refer to ../README_JOBS.md for the basics.
%%
%% Although job services are visible beyond those knowing their pid, we assume
%% that there is exactly a one-to-one correspondence between an instance of
%% this process and its associated service. That is, for a given service with
%% ScopeID {X, Index}, there are no processes other than this one submitting
%% work to it, as it just gets too complicated to bother handling the
%% many-to-one case in a deprecated API.
%%
%% Specifically, when shutdown_pool/2 is invoked on this, it shuts down the
%% job service associated with it.
%%
%% @see ../README_JOBS.md
%% @deprecated Use module {@link riak_core_job_service}.
%%
-deprecated(module).

-behaviour(gen_server).

% Public API
-export([
    handle_work/3,
    shutdown_pool/2,
    start_link/5,
    stop/2
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

% Private API
-export([
    job_killed/5,
    work_fini/1,
    work_init/6,
    work_run/3
]).

-include("riak_core_job_internal.hrl").

%
% Some local types just to keep me sane.
%
-type facade()  ::  pid().      % riak_core_vnode_worker_pool process
-type job_svc() ::  pid().      % riak_core_job_service process
%%-type worker()  ::  pid().      % riak_core_job_runner process
-type wmodule() ::  module().   % implements riak_core_vnode_worker
-type wstate()  ::  term().     % riak_core_vnode_worker state
-type workrec() ::  tuple().    % riak_core_vnode_worker work object

-record(jobrec, {
    ref         ::  reference(),
    gid         ::  riak_core_job:id()
}).
-type jobrec()  ::  #jobrec{}.

-record(state, {
    scope_id            ::  scope_id(),
    svc_pid             ::  'undefined' | job_svc(),
    svc_mon             ::  'undefined' | reference(),
    wmod                ::  wmodule(),
    wargs               ::  term(),
    wprops              ::  term(),
    jobs      = []      ::  [jobrec()],
    shutdown  = 'false' ::  boolean()
}).
-type state()   ::  #state{}.

-record(context, {
    scope_id    ::  scope_id(),
    owner       ::  facade(),
    job_svc     ::  job_svc(),
    wmod        ::  wmodule(),
    wref        ::  reference(),
    state       ::  wstate()
}).
-type context() ::  #context{}.

%% ===================================================================
%% Public API
%% ===================================================================

-spec handle_work(facade(), workrec(), sender()) -> 'ok' | {'error', term()}.
handle_work(Srv, Work, From) ->
    gen_server:call(Srv, {'work', Work, From}).

-spec shutdown_pool(facade(), non_neg_integer() | 'infinity')
        -> 'ok' | {'error', term()}.
shutdown_pool(Srv, Timeout)
        when erlang:is_integer(Timeout) andalso Timeout >= (1 bsl 32) ->
    shutdown_pool(Srv, 'infinity');
shutdown_pool(Srv, 'infinity' = Timeout) when erlang:is_pid(Srv) ->
    sync_shutdown(Srv, Timeout);
shutdown_pool(Srv, Timeout) when erlang:is_pid(Srv)
        andalso erlang:is_integer(Timeout) andalso Timeout >= 0 ->
    sync_shutdown(Srv, Timeout).

-spec start_link(wmodule(), non_neg_integer(), partition(), term(), term())
        -> {'ok', pid()} | {'error', term()}.
start_link(WMod, PoolSize, VNodeIndex, WArgs, WProps) ->
    gen_server:start_link(?MODULE,
        {WMod, PoolSize, VNodeIndex, WArgs, WProps}, []).

stop(Srv, Reason) ->
    gen_server:cast(Srv, {'stop', Reason}).

%% ===================================================================
%% Private API
%% ===================================================================

-spec job_killed(term(), facade(), sender(), reference(), workrec()) -> 'ok'.
%
% riak_core_job:job.killed callback
%
job_killed(_, Owner, 'ignore', Ref, _) ->
    gen_server:cast(Owner, {'done', Ref});
job_killed({?JOB_ERR_CRASHED, Info} = Reason, Owner, Origin, Ref, Work) ->
    riak_core_vnode:reply(Origin, {'error', {'worker_crash', Info, Work}}),
    job_killed(Reason, Owner, 'ignore', Ref, Work);
job_killed({?JOB_ERR_KILLED, Info} = Reason, Owner, Origin, Ref, Work) ->
    riak_core_vnode:reply(Origin, {'error', {'worker_killed', Info, Work}}),
    job_killed(Reason, Owner, 'ignore', Ref, Work);
job_killed({?JOB_ERR_CANCELED, Info} = Reason, Owner, Origin, Ref, Work) ->
    riak_core_vnode:reply(Origin, {'error', {'worker_canceled', Info, Work}}),
    job_killed(Reason, Owner, 'ignore', Ref, Work);
job_killed(Reason, Owner, Origin, Ref, Work) ->
    riak_core_vnode:reply(Origin, {'error', Reason}),
    job_killed(Reason, Owner, 'ignore', Ref, Work).

-spec work_init({scope_id(), job_svc()},
                facade(), reference(), wmodule(), term(), term())
        -> context().
%
% riak_core_job:job.work.init callback
%
work_init({{_, VNodeIndex} = ScopeID, ScopeSvc},
        Owner, OwnerRef, WMod, WArgs, WProps) ->
    {'ok', WState} = WMod:init_worker(VNodeIndex, WArgs, WProps),
    #context{
        scope_id    = ScopeID,
        owner       = Owner,
        job_svc     = ScopeSvc,
        wmod        = WMod,
        wref        = OwnerRef,
        state       = WState }.

-spec work_run(context(), tuple(), sender()) -> context().
%
% riak_core_job:job.work.run callback
%
work_run(#context{wmod = M, state = S} = Context, Work, Origin) ->
    State = case M:handle_work(Work, Origin, S) of
        {'reply', Reply, NewState} ->
            riak_core_vnode:reply(Origin, Reply),
            NewState;
        {'noreply', NewState} ->
            NewState
    end,
    Context#context{state = State}.

-spec work_fini(context()) -> term().
%
% riak_core_job:job.work.fini callback
%
work_fini(#context{owner = O, wref = R}) ->
    gen_server:cast(O, {'done', R}).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

-spec init({module(), non_neg_integer(), partition(), term(), term()})
        -> {'ok', state()} | {'stop', term()}.
init({WMod, PoolSize, VNodeIndex, WArgs, WProps}) ->
    ScopeID = {WMod, VNodeIndex},
    Config  = [{'job_svc_concurrency_limit', PoolSize}],
    case riak_core_job_manager:start_scope(ScopeID, Config) of
        'ok' ->
            case riak_core_job_manager:lookup(?SCOPE_SVC_ID(ScopeID)) of
                Svc when erlang:is_pid(Svc) ->
                    _ = erlang:process_flag('trap_exit', 'true'),
                    {'ok', #state{
                        scope_id    = ScopeID,
                        svc_pid     = Svc,
                        svc_mon     = erlang:monitor('process', Svc),
                        wmod        = WMod,
                        wargs       = WArgs,
                        wprops      = WProps }};
                {'error', _} = Error ->
                    {'stop', Error}
            end;
        {'error', _} = Error ->
            {'stop', Error}
    end.

-spec handle_call(term(), {pid(), term()}, state())
        -> {'reply', term(), state()}.
%
% handle_work(facade(), workrec(), sender()) -> 'ok' | {'error', term()}
%
handle_call({'work', _Work, Origin}, _, #state{shutdown = 'true'} = State) ->
    riak_core_vnode:reply(Origin, {'error', 'vnode_shutdown'}),
    {'reply', 'ok', State};
handle_call({'work', Work, Origin}, _, #state{scope_id = ScopeID, svc_pid = Svc,
        wmod = Mod, wargs = Arg, wprops = Prp, jobs = JRs} = State) ->
    Own = erlang:self(),
    Ref = erlang:make_ref(),
    Wrk = riak_core_job:work([
        {'init',  {?MODULE, 'work_init',  [Own, Ref, Mod, Arg, Prp, Origin]}},
        {'run',   {?MODULE, 'work_run',   [Work]}},
        {'fini',  {?MODULE, 'work_fini',  []}}
    ]),
    Jin = [
        {'module',  ?MODULE},
        {'class',   'legacy'},
        {'cid',     ScopeID},
        {'work',    Wrk},
        {'from',    Own},
        {'killed',  {?MODULE, 'job_killed', [Own, Origin, Ref, Work]}}
    ],
    Job = riak_core_job:job(Jin),
    case riak_core_job_service:submit(Svc, Job) of
        'ok' ->
            JR = #jobrec{ref = Ref, gid = riak_core_job:get('gid', Job)},
            {'reply', 'ok', State#state{jobs = [JR | JRs]}};
        {'error', ?JOB_ERR_SHUTTING_DOWN} ->
            {'reply', {'error', 'vnode_shutdown'}, State};
        {'error', ?JOB_ERR_QUEUE_OVERFLOW} ->
            {'reply', {'error', 'vnode_overload'}, State};
        {'error', ?JOB_ERR_REJECTED} ->
            {'reply', {'error', 'vnode_rejected'}, State};
        {'error', _} = Error ->
            {'reply', Error, State}
    end.

-spec handle_cast(term(), state())
        -> {'noreply', state()} | {'stop', term(), state()}.
%
% no matter what arrives, if we've been asked to shut down and don't have any
% jobs running, just leave
%
handle_cast(_, #state{
        shutdown = 'true', jobs = [], svc_mon = 'undefined'} = State) ->
    {'stop', 'shutdown', State};
handle_cast(_, #state{shutdown = 'true', jobs = [], svc_mon = Ref} = State) ->
    _ = erlang:demonitor(Ref, ['flush']),
    {'stop', 'shutdown', State#state{svc_mon = 'undefined'}};
%
% job_killed/5
% work_fini/1
%
handle_cast({'done', Ref}, #state{
        shutdown = 'true', jobs = [#jobrec{ref = Ref}]} = State) ->
    {'stop', 'shutdown', State#state{jobs = []}};
handle_cast({'done', Ref}, #state{jobs = Jobs} = State) ->
    {'noreply', State#state{jobs = lists:keydelete(Ref, #jobrec.ref, Jobs)}};
%
% shutdown_pool(facade(), non_neg_integer() | 'infinity')
% this is the initial shutdown message, so tell the job service to shut down
%
handle_cast({'shutdown', Timeout}, #state{
        shutdown = 'false', scope_id = ScopeID} = State) ->
    _ = riak_core_job_manager:stop_scope(ScopeID),
    case Timeout of
        'infinity' ->
            'ok';
        0 ->
            ?cast({'shutdown', 0});
        _ ->
            erlang:send_after(Timeout, erlang:self(), 'shutdown_timeout')
    end,
    {'noreply', State#state{shutdown = 'true', svc_pid = 'undefined'}};
%
% out of time
% not much we can do here - there must be jobs remaining or this would have
% matched the general "shutdown while empty" head above, but the service has
% already been told to stop and our time has come
%
handle_cast({'shutdown' = Why, 0}, State) ->
    {'stop', Why, State};
%
% unrecognized message
%
handle_cast(_Msg, State) ->
    {'noreply', State}.

-spec handle_info(term(), state())
        -> {'noreply', state()} | {'stop', term(), state()}.
%
% shutdown timeout has elapsed
%
handle_info('shutdown_timeout', State) ->
    handle_cast({'shutdown', 0}, State);
%
% our job service went down, let's hope it was because we asked it to ...
%
handle_info({'DOWN', Ref, _, _, _}, #state{
        shutdown = 'true', svc_mon = Ref} = State) ->
    handle_cast({'shutdown', 0}, State#state{svc_mon = 'undefined'});
handle_info({'DOWN', Ref, _, _, Info}, #state{
        scope_id = ScopeID, svc_mon = Ref} = State) ->
    _ = lager:error("~p worker pool job service crashed: ~p", [ScopeID, Info]),
    handle_cast({'shutdown', 0}, State#state{
        shutdown = 'true', svc_mon = 'undefined'});
%
% no matter what arrives, if we've been asked to shut down and don't have any
% jobs running, let handle_cast clean up and leave
%
handle_info(Msg, #state{shutdown = 'true', jobs = []} = State) ->
    handle_cast(Msg, State);
%
% unrecognized message
%
handle_info(_Msg, State) ->
    {'noreply', State}.

-spec terminate('normal' | 'shutdown' | {'shutdown', term()} | term(), state())
        -> 'ok'.
%
% hopefully we got here via a controlled shutdown, just do the best we can ...
%
terminate(Why, #state{shutdown = 'false', scope_id = ScopeID} = State) ->
    _ = riak_core_job_manager:stop_scope(ScopeID),
    terminate(Why, State#state{shutdown = 'true', svc_pid = 'undefined'});
terminate(_, #state{jobs = [], svc_mon = 'undefined'}) ->
    'ok';
terminate(Why, #state{jobs = [], svc_mon = Ref} = State) ->
    _ = erlang:demonitor(Ref, ['flush']),
    terminate(Why, State#state{svc_mon = 'undefined'});
terminate(Why, State) ->
    % nothing we can do at this point but throw them away
    terminate(Why, State#state{jobs = []}).

-spec code_change(term(), state(), term()) -> {'ok', state()}.
%
% don't care, carry on
%
code_change(_, State, _) ->
    {'ok', State}.

%% ===================================================================
%% Internal
%% ===================================================================

-spec sync_shutdown(facade(), non_neg_integer() | 'infinity')
        -> 'ok' | {'error', term()}.
%
% Parameters are assumed to be valid, no checking done here.
%
sync_shutdown(Srv, Timeout) ->
    % monitor + cast so a late response doesn't choke the caller
    Ref = erlang:monitor('process', Srv),
    gen_server:cast(Srv, {'shutdown', Timeout}),
    receive
        {'DOWN', Ref, _, Srv, _} ->
            'ok'
    after
        Timeout ->
            _ = erlang:demonitor(Ref, ['flush']),
            {'error', 'timeout'}
    end.

