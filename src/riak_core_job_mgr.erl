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

-module(riak_core_job_mgr).
-behaviour(gen_server).

% Public API
-export([
    submit/2
]).

% Public types
-export_type([
    cbfunc/0,
    config/0,
    node_id/0,
    node_type/0,
    paccept/0,
    pconcur/0,
    pquemax/0,
    prop/0
]).

% Private API
-export([
    cleanup/2,
    config/1,
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
%% manager initialization, and startup fails if the result is anything other
%% than a boolean().
%% If this property is not provided, all jobs are accepted.
-type paccept() ::  {node_job_accept,   cbfunc()}.

%% Defaults to ?VNODE_JOB_DFLT_CONCUR.
-type pconcur() ::  {node_job_concur,   pos_integer()}.

%% Defaults to node_job_concur * ?VNODE_JOB_DFLT_QUEMULT.
-type pquemax() ::  {node_job_queue,    non_neg_integer()}.

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
    ref     ::  reference(),
    job     ::  job()
}).
-type mon() :: {pid(), reference(), job()}.

%% MUST keep queue/quelen and monitors/running in sync!
%% They're separate to allow easy decisions based on active/queued work,
%% but obviously require close attention.
-record(state, {
    work_sup    = 'undefined'       ::  pid() | 'undefined',
    node_id     = {?MODULE, 0}      ::  node_id(),
    config      = []                ::  config(),
    accept      = fun accept_any/2  ::  cbfunc(),
    concur      = 0                 ::  pos_integer(),
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

-spec submit(node_type() | node_id() | [node_id()] | pid(), job())
        -> ok | {error, term()}.
%%
%% @doc Submit a job to run on the specified vnode(s).
%%
submit(Mgr, Job) when erlang:is_pid(Mgr) ->
    case riak_core_job:version(Job) of
        {job, _} ->
            gen_server:call(Mgr, {'submit', Job});
        _ ->
            erlang:error(badarg, [Job])
    end;
submit({NType, _} = VNodeID, Job) when erlang:is_atom(NType) ->
    case riak_core_job:version(Job) of
        {job, _} ->
            case riak_core_job_svc:job_mgr(VNodeID) of
                Mgr when erlang:is_pid(Mgr) ->
                    gen_server:call(Mgr, {'submit', Job});
                {error, _} = Error ->
                    Error;
                Other ->
                    {error, Other}
            end;
        _ ->
            erlang:error(badarg, [Job])
    end;
submit(Multi, Job) ->
    riak_core_job_svc:submit_mult(Multi, Job).

%% ===================================================================
%% Private API
%% ===================================================================

-spec shutdown(pid(), non_neg_integer() | 'infinity') -> ok.
%%
%% @doc Tell the manager to shut down within Timeout milliseconds.
%%
%% There's no facility here to notify the caller when the shutdown is
%% completed. If the caller wants to be notified when the shutdown completes,
%% they should monitor the manager's Pid BEFORE calling this function.
%%
%% Note that the manager is normally part of a supervision tree that will
%% restart it when it exits for any reason. To shut it down properly, the
%% shutdown should be initiated by the controlling service's stop_node/1
%% function.
%%
shutdown(Mgr, Timeout)
        when erlang:is_pid(Mgr) andalso (Timeout =:= 'infinity'
        orelse (erlang:is_integer(Timeout) andalso Timeout >= 0)) ->
    gen_server:cast(Mgr, {'shutdown', Timeout}).

-spec start_link(node_id(), config(), job()) -> {ok, pid()} | {error, term()}.
%%
%% @doc Start a new process linked to the calling process.
%%
start_link(?NODE_MGR_ID(_) = Id, Config, DummyJob) ->
    gen_server:start_link(?MODULE, {Id, Config, DummyJob}, []).

-spec config(pid()) -> config() | {error, term()}.
%%
%% @doc Return the configuration the manager was started with.
%%
%% This is mainly so the riak_core_job_svc can repopulate its cache from a
%% running supervision tree after a restart. It matters that this is the input
%% configuration and not the result of applying defaults to come up with what's
%% running.
%%
%% The actual running configuration is reported by stats(...).
%%
config(Mgr) ->
    gen_server:call(Mgr, 'config').

-spec register(pid(), work_sup_id(), pid()) -> ok.
%%
%% @doc Notify the server where to find its worker supervisor.
%%
%% This is being called from the riak_core_job_svc, so it can't call into that
%% service to look up the manager, hence the direct call to the Pid.
%%
register(Mgr, {'register', ?WORK_SUP_ID(_) = Id}, Sup) ->
    gen_server:cast(Mgr, {Id, Sup}).

-spec starting(pid(), reference()) -> ok.
%%
%% @doc Callback for the job runner indicating the job's UOW is being started.
%%
starting(Mgr, JobRef) ->
    update_job(Mgr, JobRef, 'started').

-spec running(pid(), reference()) -> ok.
%%
%% @doc Callback for the job runner indicating the job's UOW is being run.
%%
running(Mgr, JobRef) ->
    update_job(Mgr, JobRef, 'running').

-spec cleanup(pid(), reference()) -> ok.
%%
%% @doc Callback for the job runner indicating the job's UOW is being cleaned up.
%%
cleanup(Mgr, JobRef) ->
    update_job(Mgr, JobRef, 'cleanup').

-spec done(pid(), reference(), term()) -> ok.
%%
%% @doc Callback for the job runner indicating the job's UOW is finished.
%%
done(Mgr, JobRef, Result) ->
    update_job(Mgr, JobRef, 'done', Result).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

-spec init({node_id(), config(), job()})
        -> {ok, state()} | {stop, {error, term()}}.
init({?NODE_MGR_ID(VNodeID) = Id, Config, DummyJob}) ->
    case init_state(#state{node_id = VNodeID, config = Config}, DummyJob) of
        #state{} = State ->
            riak_core_job_svc:register(Id, erlang:self()),
            {ok, State};
        Error ->
            {stop, Error}
    end.

-spec handle_call(term(), pid(), state()) -> {reply, term(), state()}.
%
% submit_work(NodeId, Job)
%
handle_call({'submit', _}, _, #state{shutdown = true} = State) ->
    {reply, {error, vnode_shutdown}, State};
handle_call({'submit', _}, _, #state{
        quelen = L, maxque = M} = State) when L >= M ->
    {reply, {error, job_queue_full}, State};
handle_call({'submit', Job}, _, #state{concur = C, running = R} = State) ->
    case riak_core_job:runnable(Job) of
        true ->
            case catch accept_job(Job, State) of
                true ->
                    case R < C of
                        true ->
                            {reply, ok, start_job(Job, State)};
                        _ ->
                            {reply, ok, queue_job(Job, State)}
                    end;
                false ->
                    {reply, {error, job_rejected}, State};
                {error, _} = Error ->
                    {reply, Error, State};
                Error ->
                    {reply, {error, Error}, State}
            end;
        Err ->
            {reply, Err, State}
    end;
%
% config(pid()) -> config() | {error, term()}.
%
handle_call('config', _, #state{config = Config} = State) ->
    {reply, Config, State};
%
% unrecognized message
%
handle_call(Message, From, #state{node_id = NodeId} = State) ->
    _ = lager:error(
            "~p job manager received unhandled call from ~p: ~p",
            [NodeId, From, Message]),
    {reply, {error, {badarg, Message}}, State}.

-spec update_job(pid(), reference(), atom()) -> 'ok'.
update_job(Mgr, JobRef, Key) ->
    gen_server:cast(Mgr,
        {JobRef, 'update', Key, riak_core_job:timestamp()}).

-spec update_job(pid(), reference(), atom(), term()) -> 'ok'.
update_job(Mgr, JobRef, Key, Info) ->
    gen_server:cast(Mgr,
        {JobRef, 'update', Key, riak_core_job:timestamp(), Info}).

-spec handle_cast(term(), state()) -> {noreply, state()}.
%
% internal deque message
%
handle_cast('dequeue', #state{shutdown = true} = State) ->
    {noreply, State};
handle_cast('dequeue', #state{quelen = 0} = State) ->
    {noreply, State};
handle_cast('dequeue', State) ->
    {noreply, dequeue(State)};
%
% status update from a running job
%
handle_cast({Ref, 'update', 'done' = Stat, _TS, Result}, #state{
        node_id = NodeId, running = R, monitors = M} = State) ->
    _ = erlang:demonitor(Ref, [flush]),
    case lists:keytake(Ref, #mon.ref, M) of
        {value, _, NewM} ->
            {noreply, dequeue(?validate(
                State#state{monitors = NewM, running = (R - 1)}))};
        _ ->
            _ = lager:error(
                    "~p job manager received completion message ~p:~p "
                    "for unknown job ref ~p", [NodeId, Stat, Result, Ref]),
            {noreply, State}
    end;
handle_cast({Ref, 'update', Stat, TS}, #state{
        node_id = NodeId, monitors = M} = State) ->
    case lists:keyfind(Ref, #mon.ref, M) of
        #mon{job = Job} = Rec ->
            New = Rec#mon{job = riak_core_job:update(Stat, TS, Job)},
            {noreply, ?validate(State#state{
                monitors = lists:keystore(Ref, #mon.ref, M, New)})};
        _ ->
            _ = lager:error(
                    "~p job manager received ~p update for unknown job ref ~p",
                    [NodeId, Stat, Ref]),
            {noreply, State}
    end;
%
% register(WorkSupId)
%
handle_cast({'register', ?WORK_SUP_ID(_) = Sup}, #state{quelen = 0} = State) ->
    {noreply, State#state{work_sup = Sup}};
handle_cast({'register', ?WORK_SUP_ID(_) = Sup}, State) ->
    _ = erlang:monitor(process, Sup),
    {noreply, dequeue(State#state{work_sup = Sup})};
%
% unrecognized message
%
handle_cast(Message, #state{node_id = NodeId} = State) ->
    _ = lager:error(
            "~p job manager received unhandled cast: ~p", [NodeId, Message]),
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
%
% Our work supervisor exited - not good at all.
% When the supervisor went down, it took all of the running jobs with it, so
% we'll get 'DOWN' messages from each of them - no need to handle them here.
%
handle_info({'DOWN', _, _, Pid, Info},
        #state{work_sup = Pid, node_id = NodeId} = State) ->
    _ = lager:error("~p work supervisor ~p exited: ~p", [NodeId, Pid, Info]),
    {noreply, State#state{work_sup = undefined}};
%
% A monitored job finished.
%
handle_info({'DOWN', Ref, _, Pid, _},
        #state{node_id = NodeId, running = R, monitors = M} = State) ->
    case lists:keytake(Ref, #mon.ref, M) of
        {value, #mon{pid = Pid}, NewM} ->
            {noreply, dequeue(?validate(
                State#state{monitors = NewM, running = (R - 1)}))};
        _ ->
            _ = lager:error(
                    "~p job manager received 'DOWN' message "
                    "for unrecognized process ~p", [NodeId, Pid]),
            {noreply, State}
    end;
%
% unrecognized message
%
handle_info(Message, #state{node_id = NodeId} = State) ->
    _ = lager:error(
            "~p job manager received unhandled info: ~p", [NodeId, Message]),
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
%
% no matter why we're terminating, de-monitor everything we're watching
%
terminate(inconsistent, #state{node_id = NodeId} = State) ->
    _ = lager:error(
            "~p job manager terminated due to inconsistent state: ~p",
            [NodeId, State]),
    ok;
terminate(_, _State) ->
    ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
%
% at present we don't care, so just carry on
%
code_change(_, State, _) ->
    {ok, State}.

%% ===================================================================
%% Internal
%% ===================================================================

-spec accept_job(job(), state() | cbfunc()) -> boolean().
accept_job(Job, #state{node_id = NodeId, accept = Accept}) ->
    accept_job(Accept, NodeId, Job).

-spec accept_job(cbfunc(), node_id(), job()) -> boolean().
accept_job({Mod, Func, Args}, NodeId, Job) ->
    erlang:apply(Mod, Func, [NodeId, Job | Args]);
accept_job({Fun, Args}, NodeId, Job) ->
    erlang:apply(Fun, [NodeId, Job | Args]).

-spec accept_any(node_id(), job()) -> true.
accept_any(_, _) ->
    true.

-spec dequeue(state()) -> state().
%% Dequeue and dispatch at most one job. If there are more jobs waiting, ensure
%% that we'll get to them after handling whatever may already be waiting.
dequeue(#state{quelen = 0} = State) ->
    State;
dequeue(#state{shutdown = true, queue = [J | Js], quelen = L} = State) ->
    case riak_core_job:get(killed, J) of
        undefined ->
            _ = riak_core_vnode:reply(
                    riak_core_job:get(from, J), {error, vnode_shutdown});
        {Mod, Func, Args} ->
            catch erlang:apply(Mod, Func, [vnode_shutdown | Args]);
        {Fun, Args} ->
            catch erlang:apply(Fun, [vnode_shutdown | Args])
    end,
    maybe_dequeue(?validate(State#state{queue = Js, quelen = (L - 1)}));
dequeue(#state{concur = C, running = R} = State) when R >= C ->
    State;
dequeue(#state{work_sup = undefined} = State) ->
    maybe_dequeue(State);
dequeue(#state{queue = [Job | Q], quelen = L} = State) ->
    maybe_dequeue(start_job(Job,
        ?validate(State#state{queue = Q, quelen = (L - 1)}))).

-spec queue_job(job(), state()) -> state().
%% Queue the specified Job. This assumes ALL checks have been performed
%% beforehand - NONE are performed here!
queue_job(Job, #state{queue = Q, quelen = L} = State) ->
    maybe_dequeue(?validate(State#state{
        queue = Q ++ [riak_core_job:update(queued, Job)], quelen = (L + 1)})).

-spec start_job(job(), state()) -> state().
%% Dispatch the specified Job on a runner process. This assumes ALL checks
%% have been performed beforehand - NONE are performed here!
start_job(Job, #state{work_sup = S, monitors = M, running = R} = State) ->
    {ok, Pid} = supervisor:start_child(S, []),
    Ref = erlang:monitor(process, Pid),
    Rec = #mon{pid = Pid, ref = Ref, job = Job},
    ok = riak_core_job_run:run(
        Pid, erlang:self(), Ref, riak_core_job:get(work, Job)),
    ?validate(State#state{monitors = [Rec | M], running = (R + 1)}).

-spec maybe_dequeue(state()) -> state().
%% If there are queued jobs make sure there's a message in our inbox to get to
%% them after handling whatever may already be waiting.
maybe_dequeue(#state{dqpending = true} = State) ->
    State;
maybe_dequeue(#state{quelen = 0} = State) ->
    State;
maybe_dequeue(#state{
        shutdown = false, concur = C, running = R} = State) when R >= C ->
    State;
maybe_dequeue(State) ->
    gen_server:cast(erlang:self(), dequeue),
    State#state{dqpending = true}.

-spec init_state(state(), job()) -> state() | {error, term()}.
init_state(State, Dummy) ->
    % field order matters!
    init_state([accept, concur, maxque], Dummy, State).

-spec init_state([atom()], job(), state()) -> state() | {error, term()}.
init_state([accept | Fields], Dummy,
        #state{config = Config, node_id = NodeId} = State) ->
    case proplists:get_value(node_job_accept, Config) of
        undefined ->
            init_state(Fields, Dummy, State#state{accept = fun accept_any/2});
        {Mod, Func, Args} = Accept
                when erlang:is_atom(Mod)
                andalso erlang:is_atom(Func)
                andalso erlang:is_list(Args) ->
            Arity = (erlang:length(Args) + 2),
            case erlang:function_exported(Mod, Func, Arity) of
                true ->
                    case init_test_accept(Accept, NodeId, Dummy) of
                        ok ->
                            init_state(Fields, Dummy,
                                State#state{accept = Accept});
                        Error ->
                            Error
                    end;
                _ ->
                    {error, {node_job_accept, {undef, {Mod, Func, Arity}}}}
            end;
        {Fun, Args} = Accept
                when erlang:is_function(Fun)
                andalso erlang:is_list(Args) ->
            Arity = (erlang:length(Args) + 2),
            case erlang:fun_info(Fun, arity) of
                {_, Arity} ->
                    case init_test_accept(Accept, NodeId, Dummy) of
                        ok ->
                            init_state(Fields, Dummy,
                                State#state{accept = Accept});
                        Error ->
                            Error
                    end;
                _ ->
                    {error, {node_job_accept, {badarity, {Fun, Arity}}}}
            end;
        Spec ->
            {error, {node_job_accept, {badarg, Spec}}}
    end;
init_state([concur | Fields], Dummy, #state{config = Config} = State) ->
    case proplists:get_value(node_job_concur, Config) of
        undefined ->
            init_state(Fields, Dummy,
                State#state{concur = ?VNODE_JOB_DFLT_CONCUR});
        Concur when erlang:is_integer(Concur) andalso Concur > 0 ->
            init_state(Fields, Dummy, State#state{concur = Concur});
        BadArg ->
            {error, {node_job_concur, {badarg, BadArg}}}
    end;
init_state([maxque | Fields], Dummy,
        #state{config = Config, concur = Concur} = State) ->
    case proplists:get_value(node_job_queue, Config) of
        undefined ->
            init_state(Fields, Dummy,
                State#state{maxque = (Concur * ?VNODE_JOB_DFLT_QUEMULT)});
        MaxQue when erlang:is_integer(MaxQue) andalso MaxQue >= 0 ->
            init_state(Fields, Dummy, State#state{maxque = MaxQue});
        BadArg ->
            {error, {node_job_queue, {badarg, BadArg}}}
    end;
init_state([], _, State) ->
    State.

-spec init_test_accept(cbfunc(), node_id(), job()) -> 'ok' | {error, term()}.
init_test_accept(Accept, NodeId, Job) ->
    try accept_job(Accept, NodeId, Job) of
        true ->
            ok;
        false ->
            ok;
        Other ->
            {error, {node_job_accept, badmatch, Other}}
    catch
        Class:What ->
            {error, {node_job_accept, Class, What}}
    end.

-ifdef(VALIDATE_STATE).
-spec validate(state()) -> state() | no_return().
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
    R4 = case ML > State#state.concur of
        true ->
            [{concur, ML, State#state.concur} | R3];
        _ ->
            R1
    end,
    case R4 of
        [] ->
            State;
        Err ->
            lager:error("Inconsistent state: ~p", State),
            erlang:error({invalid_state, Err}, [State])
    end.
-endif.

