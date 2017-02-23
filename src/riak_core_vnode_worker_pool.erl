%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2017 Basho Technologies, Inc.
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
%% @doc This module is deprecated and will be removed in v3!
%%
%% This is a facade over {@link riak_core_job_manager}, and ALL new code should
%% use that API, not this one.
%%
%% @deprecated Use module {@link riak_core_job_manager}.
%%
-module(riak_core_vnode_worker_pool).
-deprecated(module).

-behaviour(gen_server).

%% Public API
-export([
    handle_work/3,
    shutdown_pool/2,
    start_link/5,
    stats/1,
    stop/2
]).

%% Public Types
-export_type([
    stat/0,
    stat_key/0,
    stat_val/0
]).

%% Private API
-export([
    job_killed/4,
    work_cleanup/1,
    work_main/3,
    work_setup/4
]).

%% Gen_server API
-export([
    code_change/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1,
    terminate/2
]).

-include("riak_core_job_internal.hrl").

%% ===================================================================
%% Internal Types
%% ===================================================================

-define(StatsDict,  orddict).
-type stats()   ::  ?orddict_t(stat_key(), stat_val()).

%% This is filled in incrementally, so it allows a lot of undefined values.
-record(ctx, {
    vnode       ::  partition(),
    pool        ::  facade() | undefined,
    mgr_key     ::  riak_core_job_manager:mgr_key() | undefined,
    wmod        ::  wmodule(),
    wref        ::  jobkey() | undefined,
    state       ::  wstate() | undefined
}).

-record(jobrec, {
    key         ::  jobkey(),
    id          ::  jobid()
}).

-record(state, {
    ctx                             ::  ctx(),
    wargs                           ::  term(),
    wprops                          ::  term(),
    jobs        = []                ::  [jobrec()],
    shutdown    = false             ::  boolean(),
    stats       = ?StatsDict:new()  ::  stats()     % stat_key() => stat_val()
}).

-type ctx()     ::  #ctx{}.
-type jobid()   ::  riak_core_job:gid().
-type jobkey()  ::  reference().
-type jobrec()  ::  #jobrec{}.
-type state()   ::  #state{}.
-type wstate()  ::  term().     % riak_core_vnode_worker state

%% ===================================================================
%% Public Types
%% ===================================================================

-type facade() :: pid().
%% A {@link riak_core_vnode_worker_pool} process.

-type stat() :: {stat_key(), stat_val()}.
%% A single statistic.

-type stat_key() :: atom() | tuple().
%% The Key by which a statistic is referenced.

-type stat_val() :: term().
%% The value of a statistic.

-type wmodule() :: module().
%% Module implementing {@link riak_core_vnode_worker} behaviour.

-type workrec() :: tuple().
%% A unit of work for {@link riak_core_vnode_worker:handle_work/3}.

%% ===================================================================
%% Public API
%% ===================================================================

-spec handle_work(
    Pool :: facade(), Work :: workrec(), From :: sender())
        -> ok | {error, term()}.
%%
%% @doc Submits the specified Work to the Pool.
%%
handle_work(Pool, Work, From) ->
    gen_server:call(Pool, {work, Work, From}).

-spec shutdown_pool(
    Pool :: facade(), Timeout :: non_neg_integer() | infinity)
        -> ok | {error, term()}.
%%
%% @doc Shuts the Pool down.
%%
%% Running and queued work is killed or cancelled, respectively.
%%
shutdown_pool(Pool, Timeout)
        when not (erlang:is_pid(Pool)
        andalso (Timeout =:= infinity
        orelse  (erlang:is_integer(Timeout) andalso Timeout >= 0))) ->
    erlang:error(badarg, [Pool, Timeout]);
shutdown_pool(Pool, infinity) ->
    sync_shutdown(Pool, 1 bsl 32);
shutdown_pool(Pool, Timeout) when Timeout > 1 bsl 32 ->
    sync_shutdown(Pool, 1 bsl 32);
shutdown_pool(Pool, Timeout) ->
    sync_shutdown(Pool, Timeout).

-spec start_link(
    WorkerMod   :: wmodule(),
    PoolSize    :: non_neg_integer(),
    VNodeIndex  :: partition(),
    WorkerArgs  :: term(),
    WorkerProps :: term())
        -> {ok, pid()} | {error, term()}.
%%
%% @doc Starts a worker pool, linked to the calling process.
%%
%% Note that the PoolSize parameter is ignored entirely - the
%% riak_core_job_manager configuration applies.
%%
start_link(WorkerMod, _PoolSize, VNodeIndex, WorkerArgs, WorkerProps) ->
    Context = #ctx{
        vnode   = VNodeIndex,
        pool    = undefined,
        mgr_key = undefined,
        wmod    = WorkerMod,
        wref    = undefined,
        state   = undefined
    },
    State = #state{
        ctx     = Context,
        wargs   = WorkerArgs,
        wprops  = WorkerProps
    },
    gen_server:start_link(?MODULE, State, []).

-spec stats(Pool :: facade()) -> [stat()] | {error, term()}.
%%
%% @doc Shuts the Pool down.
%%
stats(Pool) ->
    gen_server:call(Pool, stats).

-spec stop(Pool :: facade(), Reason :: term()) -> ok.
%%
%% @doc Shuts the Pool down.
%%
stop(Pool, Reason) ->
    gen_server:cast(Pool, {stop, Reason}).

%% ===================================================================
%% Private API
%% ===================================================================

-spec job_killed(
    Reason :: term(), Context :: ctx(), Origin :: sender(), Work :: tuple())
        -> ok.
%% @private
%%
%% riak_core_job:job.killed callback
%% The old worker pool wasn't very informative, so map things to the couple of
%% messages it could send.
%%
job_killed(_, #ctx{pool = Owner, wref = Ref}, ignore, _) ->
    gen_server:cast(Owner, {done, Ref});
job_killed({?JOB_ERR_CANCELED, ?JOB_ERR_SHUTTING_DOWN} = Reason,
        Context, Origin, Work) ->
    riak_core_vnode:reply(Origin, {error, vnode_shutdown}),
    job_killed(Reason, Context, ignore, Work);
job_killed({_, Info} = Reason, Context, Origin, Work) ->
    riak_core_vnode:reply(Origin, {error, {worker_crash, Info, Work}}),
    job_killed(Reason, Context, ignore, Work);
job_killed(Reason, Context, Origin, Work) ->
    riak_core_vnode:reply(Origin, {error, {worker_crash, Reason, Work}}),
    job_killed(Reason, Context, ignore, Work).

-spec work_setup(
    MgrKey  :: riak_core_job_manager:mgr_key(),
    Context :: ctx(),
    WArgs   :: term(),
    WProps  :: term())
        -> ctx().
%% @private
%%
%% riak_core_job:job.work.setup callback
%%
work_setup(MgrKey, #ctx{vnode = VNode, wmod = WMod} = Ctx, WArgs, WProps) ->
    {ok, WState} = WMod:init_worker(VNode, WArgs, WProps),
    Ctx#ctx{mgr_key = MgrKey, state = WState}.

-spec work_main(Context :: ctx(), Work :: tuple(), Origin :: sender())
        -> ctx().
%% @private
%%
%% riak_core_job:job.work.main callback
%%
work_main(#ctx{wmod = WMod, state = WStateIn} = Ctx, Work, Origin) ->
    WState = case WMod:handle_work(Work, Origin, WStateIn) of
        {reply, Reply, NewWState} ->
            riak_core_vnode:reply(Origin, Reply),
            NewWState;
        {noreply, NewWState} ->
            NewWState
    end,
    Ctx#ctx{state = WState}.

-spec work_cleanup(Context :: ctx()) -> term().
%% @private
%%
%% riak_core_job:job.work.cleanup callback
%%
work_cleanup(#ctx{pool = Owner, wref = WRef}) ->
    gen_server:cast(Owner, {done, WRef}).

%% ===================================================================
%% Gen_server API
%% ===================================================================

-spec code_change(term(), state(), term()) -> {ok, state()}.
%% @private
%%
%% don't care, carry on
%%
code_change(_, State, _) ->
    {ok, State}.

-spec handle_call(Msg :: term(), From :: {pid(), term()}, State :: state())
        -> {reply, term(), state()}.
%% @private
%%
%% handle_work(facade(), workrec(), sender()) -> ok | {error, term()}
%%
handle_call({work, _Work, Origin}, _, #state{shutdown = true} = State) ->
    riak_core_vnode:reply(Origin, {error, vnode_shutdown}),
    {reply, ok, State};

handle_call({work, Work, Origin}, _,
        #state{ctx = Context, wargs = WArgs, wprops = WProps} = State) ->
    Ref = erlang:make_ref(),
    Ctx = Context#ctx{wref = Ref},
    Wrk = riak_core_job:work([
        {setup,     {?MODULE, work_setup,   [Ctx, WArgs, WProps]}},
        {main,      {?MODULE, work_main,    [Work, Origin]}},
        {cleanup,   {?MODULE, work_cleanup, []}}
    ]),
    Job = riak_core_job:job([
        {module,    ?MODULE},
        {class,     {Ctx#ctx.wmod, legacy}},
        {cid,       {Ctx#ctx.wmod, Ctx#ctx.vnode}},
        {work,      Wrk},
        {from,      Ctx#ctx.pool},
        {killed,    {?MODULE, job_killed, [Ctx, Origin, Work]}}
    ]),
    Ret = case riak_core_job_manager:submit(Job) of
        ok ->
            JR = #jobrec{key = Ref, id = riak_core_job:gid(Job)},
            {reply, ok, State#state{jobs = [JR | State#state.jobs]}};
        {error, ?JOB_ERR_SHUTTING_DOWN} ->
            {reply, {error, vnode_shutdown}, State};
        {error, ?JOB_ERR_QUEUE_OVERFLOW}  ->
            {reply, {error, vnode_overload}, State};
        {error, ?JOB_ERR_REJECTED}  ->
            {reply, {error, vnode_rejected}, State};
        {error, _} = Error ->
            {reply, Error, State}
    end,
    case Ret of
        {_, {error, _} = Reply, _} ->
            riak_core_vnode:reply(Origin, Reply),
            Ret;
        _ ->
            Ret
    end;
%%
%% stats(Pool :: facade()) -> [stat()] | {error, term()}.
%%
handle_call(stats, _, State) ->
    Status = if State#state.shutdown -> stopping; ?else -> active end,
    Result = [
        {status,  Status},
        {jobs,    erlang:length(State#state.jobs)}
        | ?StatsDict:to_list(State#state.stats) ],
    {reply, Result, State};
%%
%% unrecognized message
%%
handle_call(Msg, {Who, _}, State) ->
    _ = lager:error(
        "~s received unhandled call from ~p: ~p", [?MODULE, Who, Msg]),
    {reply, {error, {badarg, Msg}}, inc_stat(unhandled, State)}.

-spec handle_cast(Msg :: term(), State :: state())
        -> {noreply, state()} | {stop, term(), state()}.
%% @private
%%
%% no matter what arrives, if we've been asked to shut down and don't have any
%% jobs running, just leave
%%
handle_cast(_, #state{shutdown = true, jobs = []} = State) ->
    {stop, shutdown, State};
%%
%% job_killed/4
%% work_cleanup/1
%%
%% This is how our Job's worker wrapper tells us it's done.
%% Special case when shutting down and the last Job exits.
%%
handle_cast({done, Ref}, #state{
        shutdown = true, jobs = [#jobrec{key = Ref}]} = State) ->
    {stop, shutdown, State#state{jobs = []}};

handle_cast({done, Ref}, State) ->
    {noreply, State#state{
        jobs = lists:keydelete(Ref, #jobrec.key, State#state.jobs)}};
%%
%% There are multiple ways to receive this, but they all have the same result
%% - it's time to go.
%%
handle_cast({shutdown = Why, 0}, State) ->
    {stop, Why, State};
%%
%% shutdown_pool(Pool :: facade(), Timeout:: non_neg_integer() | infinity)
%% This is the initial shutdown message, so try to shut down asynchronously.
%%
handle_cast({shutdown = Why, _}, #state{jobs = []} = State) ->
    {stop, Why, State};

handle_cast({shutdown, infinity}, #state{shutdown = false} = State) ->
    % Only do this once to clear out queued jobs. With the shutdown flag set,
    % we won't be sending any more.
    Jobs = lists:filter(
        fun(#jobrec{id = Id}) ->
            case riak_core_job_manager:cancel(Id, false) of
                {error, running, _} ->
                    true;
                {error, _} ->
                    true;
                _ ->
                    false
            end
        end, State#state.jobs),
    {noreply, State#state{shutdown = true, jobs = Jobs}};

handle_cast({shutdown, Timeout}, #state{shutdown = false} = State) ->
    {ok, _} = timer:apply_after(Timeout,
        gen_server, cast, [erlang:self(), {shutdown, 0}]),
    handle_cast({shutdown, infinity}, State);
%%
%% stop/2
%%
handle_cast({stop, Why}, State) ->
    {stop, Why, State};
%%
%% unrecognized message
%%
handle_cast(Msg, State) ->
    _ = lager:error("~s received unhandled cast: ~p", [?MODULE, Msg]),
    {noreply, inc_stat(unhandled, State)}.

-spec handle_info(term(), state())
        -> {noreply, state()} | {stop, term(), state()}.
%% @private
%%
%% no matter what arrives, if we've been asked to shut down and don't have any
%% jobs running, let handle_cast clean up and leave
%%
handle_info(Msg, #state{shutdown = true, jobs = []} = State) ->
    handle_cast(Msg, State);
%%
%% unrecognized message
%%
handle_info(Msg, State) ->
    _ = lager:error("~s received unhandled info: ~p", [?MODULE, Msg]),
    {noreply, inc_stat(unhandled, State)}.

-spec init(State :: state()) -> {ok, state()} | {stop, term()}.
%% @private
%%
%% The incoming State is almost complete, just needs our Pid.
%%
init(#state{ctx = Context} = State) ->
    {ok, State#state{ctx = Context#ctx{pool = erlang:self()}}}.

-spec terminate(normal | shutdown | {shutdown, term()} | term(), state())
        -> ok.
%% @private
%%
%% hopefully we got here via a controlled shutdown, just do the best we can ...
%%
terminate(_Why, #state{jobs = []}) ->
    ok;
terminate(_Why, State) ->
    % nothing we can do at this point but kill them off
    _ = [riak_core_job_manager:cancel(Id, true)
        || #jobrec{id = Id} <- State#state.jobs],
    ok.

%% ===================================================================
%% Internal
%% ===================================================================

-spec inc_stat(stat_key() | [stat_key()], state()) -> state()
        ;     (stat_key() | [stat_key()], stats()) -> stats().
%%
%% Increment one or more statistics counters.
%%
inc_stat(Stat, #state{stats = Stats} = State) ->
    State#state{stats = inc_stat(Stat, Stats)};
inc_stat(Stat, Stats) ->
    ?StatsDict:update_counter(Stat, 1, Stats).
%%inc_stat(Stat, Stats) when not erlang:is_list(Stat) ->
%%    ?StatsDict:update_counter(Stat, 1, Stats);
%%inc_stat([Stat], Stats) ->
%%    inc_stat(Stat, Stats);
%%inc_stat([Stat | More], Stats) ->
%%    inc_stat(More, inc_stat(Stat, Stats));
%%inc_stat([], Stats) ->
%%    Stats.

-spec sync_shutdown(Pool :: facade(), Timeout :: non_neg_integer())
        -> ok | {error, term()}.
%%
%% Makes an async shutdown look synchronous.
%%
sync_shutdown(Pool, Timeout) ->
    %
    % Allow a little extra time in case the Pool has to kill a bunch of jobs.
    % Longer timeouts give us more wiggle room, but less need for it since
    % most jobs will finish on their own.
    %
    {MonTO, PoolTO} = if
        Timeout > 9999 ->
            {(Timeout + 1000), (Timeout - 1000)};
        Timeout > 4999 ->
            {(Timeout + 677), (Timeout - 677)};
        Timeout > 1999 ->
            {(Timeout + 333), (Timeout - 333)};
        ?else ->
            {(Timeout + 500), Timeout}
    end,
    %
    % Use monitor + cast to avoid a late message arriving from a call with
    % async reply.
    %
    Mon = erlang:monitor(process, Pool),
    gen_server:cast(Pool, {shutdown, PoolTO}),
    receive
        {'DOWN', Mon, _, _, _} ->
            ok
    after
        MonTO ->
            _ = erlang:demonitor(Mon, [flush]),
            {error, timeout}
    end.
