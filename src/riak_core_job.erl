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
%% @doc This module provides operations for creating and manipulating
%% {@link job(). Job} objects for use withing the Riak Core Async Jobs
%% Subsystem.
%%
%% @see job/1
%% @see work/1
%% @see riak_core_job_manager
%%
-module(riak_core_job).

% Public API
-export([
    cap/2,
    cid/1,
    class/1,
    cleanup/1,
    client_id/1,
    dummy/0,
    from/1,
    gid/1,
    global_id/1,
    iid/1,
    iid/2,
    internal_id/1,
    internal_id/2,
    invoke/1,
    invoke/2,
    job/1,
    killed/1,
    label/1,
    main/1,
    passthrough/1,
    prio/1,
    priority/1,
    reply/2,
    runnable/1,
    setup/1,
    stats/1,
    timestamp/0,
    update/2,
    update/3,
    version/1,
    versions/0,
    work/1,
    work/2
]).

% Public Types
-export_type([
    cid/0,
    class/0,
    from/0,
    func/0,
    gid/0,
    iid/0,
    job/0,
    priority/0,
    stat/0,
    stat_key/0,
    stat_val/0,
    time/0,
    work/0
]).

-include("riak_core_job_internal.hrl").

%% ===================================================================
%% Jobs handled by riak_core_job_manager
%% ===================================================================

-define(StatsDict,  orddict).
-type stats()   ::  ?orddict_t(stat_key(), stat_val()).

%%
%% Fields without defaults are REQUIRED, even though the language can't
%% enforce that - though functions here that create instances do.
%% A record where required fields are 'undefined' is malformed.
%%
-record(riak_core_job_v1, {
    %% The work, its 'class' (for management grouping), and where it's from.
    class   = anon              :: unspec() | class(),
    from    = ignore            :: sender() | pid(),
    work    = undefined         :: undefined | work(),

    %% Currently unimplemented, but here so we don't need to define a new
    %% version of the record when (or if) it is ...
    prio    = ?JOB_PRIO_DEFAULT :: priority(),

    %% If supplied, the 'cid' is presumed to mean something to the
    %% originator of the work, and is included as an element in the 'gid'.
    %% If supplied, 'iid' is an internal tracking ID that means something to
    %% the originator and is carried around untouched.
    gid                         :: gid(),
    cid     = anon              :: unspec() | cid(),
    iid     = anon              :: unspec() | iid(),

    %% Optional callback to be invoked if the job is killed or cancelled.
    killed  = undefined         :: undefined | func(),

    %% Keep some statistics around, including, but not limited to, state
    %% transition timestamps.
    stats   = ?StatsDict:new()  :: stats()
}).

%%
%% The specification of a Unit of Work (UoW).
%%
%% Refer to the work/1 function for details.
%%
-record(riak_core_work_v1, {
    setup   = {?MODULE, passthrough, []}  :: func(),
    main    = {?MODULE, passthrough, []}  :: func(),
    cleanup = {?MODULE, passthrough, []}  :: func()
}).

-type uid()         ::  uuid() | {node(), time(), reference()}.
-type uuid()        ::  <<_:128>>.
-type unspec()      ::  anon.

-define(MAX_INIT_SKEW_uSECS,    1500000).
-define(CUR_TIMESTAMP,          os:timestamp()).

% This should be unique across a cluster. If/when proper UUIDs are readily
% available in riak_core we should consider using them instead, though there
% may be CS value in this structure's information.
-define(NEW_UID,    {erlang:node(), ?CUR_TIMESTAMP, erlang:make_ref()}).

% A global id includes the id provided by the client for correlation.
% MUST be kept in sync with the is_job_gid() macro defined in the .hrl file.
-define(NEW_GID(Mod, CID, UID), {Mod, CID, UID}).

% Arbitrary valid UUID constant from www.uuidgenerator.net because we want v1
% for guaranteed uniqueness and most OS generators don't use a MAC.
-define(DUMMY_JOB_UID,          <<16#2086f0fa48f611e6beb89e71128cae77:128>>).

-define(GLOBAL_ID(Mod, CID),    ?NEW_GID(Mod, CID, ?NEW_UID)).
-define(DUMMY_JOB_ID,           ?NEW_GID(?MODULE, dummy, ?DUMMY_JOB_UID)).

-define(is_job(Obj),    erlang:is_record(Obj, riak_core_job_v1)).
-define(is_work(Obj),   erlang:is_record(Obj, riak_core_work_v1)).

%% ===================================================================
%% Public Types
%% ===================================================================

-type cid() :: term().
%% A Client ID provided when a Job is created (and immutable thereafter).
%% NOTE that this does NOT identify a client, at least as far as the jobs
%% system is concerned - the value is entirely opaque from the perspective of
%% the containing job.
%% This element is incorporated into the {@link gid()} type.

-type class() :: atom() | {atom(), atom()}.
%% The class of a Job, used to determine whether the job is enabled as if by
%% {@link riak_core_util:job_class_enabled/2}.

-type from() :: sender() | pid().
%% The recipient of status messages, if any, about a Job.

-type func() :: {module(), atom(), list()} | {fun(), list()}.
%% A callback function.

?opaque gid() :: {module(), cid(), uid()}.
%% A Job's Global ID, which is unique at least across a cluster. Values of this
%% type are created with their associated Job and are immutable.

-type iid() :: term().
%% An updatable opaque Internal ID within a Job for any use handlers of the
%% Job choose.

?opaque job() :: #riak_core_job_v1{}.
%% The type returned by {@link job/1}.

-type priority() :: ?JOB_PRIO_MIN..?JOB_PRIO_MAX.
%% Job priority.

-type stat() :: {stat_key(), stat_val()}.
%% A single statistic.

-type stat_key() :: atom() | tuple().
%% The Key by which a statistic is referenced.

-type stat_val() :: term().
%% The value of a statistic.

-type time() :: erlang:timestamp().
%% The type returned by the {@link timestamp/1} function, and embedded in
%% statistics updated with the {@link update/2} function.

?opaque work() :: #riak_core_work_v1{}.
%% The type returned by {@link work/1}.

%% ===================================================================
%% Public API
%% ===================================================================

-spec cap(atom(), job() | work()) -> job() | work().
%%
%% @doc Make sure a Job/Work object is understandable at the specified version.
%%
%% If necessary, the object is re-created at a lower version that is no higher
%% than the specified version.
%%
cap(v1, Object) when erlang:is_record(Object, riak_core_job_v1) ->
    Object;
cap(v1, Object) when erlang:is_record(Object, riak_core_work_v1) ->
    Object.

-spec cid(Job :: job()) -> cid().
%%
%% @doc Retrieves a Job's Client ID.
%%
cid(#riak_core_job_v1{cid = Val}) ->
    Val.

-spec class(Job :: job()) -> class().
%%
%% @doc Retrieves a Job's Class.
%%
class(#riak_core_job_v1{class = Class}) ->
    Class.

-spec cleanup(Work :: work()) -> func().
%%
%% @doc Retrieve Work's `cleanup' function.
%%
cleanup(#riak_core_work_v1{cleanup = Func}) ->
    Func.

-spec client_id(Job :: job()) -> cid().
%%
%% @equiv cid(Job)
%%
client_id(Job) ->
    cid(Job).

-spec dummy() -> job().
%%
%% @doc Returns a valid Job object for testing/validation.
%%
%% The returned instance's `work' element does nothing.
%% Every element of the returned Job is constant across nodes and time,
%% including the {@link gid(). Global ID}, which can never appear anywhere else.
%% The implementation strategy allows the returned value to effectively be a
%% static constant, so it's a cheap call that doesn't create multiple instances.
%%
dummy() ->
    #riak_core_job_v1{
        class   = dummy,
        cid     = dummy,
        gid     = ?DUMMY_JOB_ID,
        prio    = ?JOB_PRIO_MIN,
        work    = #riak_core_work_v1{}
    }.

-spec from(Job :: job()) -> from().
%%
%% @doc Retrieves a Job's From field.
%%
from(#riak_core_job_v1{from = Val}) ->
    Val.

-spec gid(Job :: job()) -> gid().
%%
%% @doc Retrieves a Job's Global ID.
%%
gid(#riak_core_job_v1{gid = Val}) ->
    Val.

-spec global_id(Job :: job()) -> gid().
%%
%% @equiv gid(Job)
%%
global_id(Job) ->
    gid(Job).

-spec iid(Job :: job()) -> iid().
%%
%% @doc Retrieves a Job's Internal ID.
%%
iid(#riak_core_job_v1{iid = Val}) ->
    Val.

-spec iid(Job :: job(), IID :: iid()) -> job().
%%
%% @doc Sets a Job's Internal ID.
%%
iid(#riak_core_job_v1{} = Job, IID) ->
    Job#riak_core_job_v1{iid = IID}.

-spec internal_id(Job :: job()) -> iid().
%%
%% @equiv iid(Job)
%%
internal_id(Job) ->
    iid(Job).

-spec internal_id(Job :: job(), IID :: iid()) -> job().
%%
%% @equiv iid(Job, IID)
%%
internal_id(Job, IID) ->
    iid(Job, IID).

-spec invoke(Func :: func()) -> term().
%%
%% @doc Invokes the specified function.
%%
%% Accepts functions as specified throughout the jobs API.
%%
invoke({Fun, FunArgs})
        when    erlang:is_function(Fun)
        andalso erlang:is_list(FunArgs) ->
    erlang:apply(Fun, FunArgs);
invoke({Mod, Func, FunArgs})
        when    erlang:is_atom(Mod)
        andalso erlang:is_atom(Func)
        andalso erlang:is_list(FunArgs) ->
    erlang:apply(Mod, Func, FunArgs).

-spec invoke(Func :: func(), AddArg :: term()) -> term().
%%
%% @doc Invokes the specified function with one prepended argument.
%%
%% Accepts functions as specified throughout the jobs API.
%%
invoke({Fun, FunArgs}, AddArg)
        when    erlang:is_function(Fun)
        andalso erlang:is_list(FunArgs) ->
    erlang:apply(Fun, [AddArg | FunArgs]);
invoke({Mod, Func, FunArgs}, AddArg)
        when    erlang:is_atom(Mod)
        andalso erlang:is_atom(Func)
        andalso erlang:is_list(FunArgs) ->
    erlang:apply(Mod, Func, [AddArg | FunArgs]).

-spec job([{atom(), term()} | term()]) -> job().
%%
%% @doc Create and initialize a new job object.
%%
%% A Job can be created without a unit of work, to allow "filling in the
%% blanks" at a later point in its traversal of the cluster.
%% Obviously, a unit of work must be assigned before the Job is runnable.
%%
%% The provided property list includes Key/Value tuples with which to
%% initialize the object. The following tuples are recognized:
%%
%% {`work', Work} - Work is an object of the type returned by {@link work/2}.
%%
%% {`class', class()} - The job's class; default is `anon'.
%%
%% {`from', sender() | pid()} - The job's originator; default is `ignore'.
%% Used by the {@link reply/2} function, which itself is only called by the job
%% management system if the job crashes or is canceled AND `killed' is unset
%% (or raises an error on invocation), in which case a message in the form
%% `{error, Job, Why}' is sent to it - if you set this field, be prepared
%% to receive that message.
%%
%% Warning: Because there are so many valid representations of the sender()
%% type in riak_core (and because there appear to be formats in use that are
%% not covered by the type specification at all), it's not feasible to fully
%% check them when the job is created, so exceptions from reply/2 are possible
%% if you try to roll your own.
%%
%% {`cid', cid()} - A client job identifier; default is `anon'.
%% This field is incorporated into the job's global ID.
%%
%% {`prio', priority()} - The job's priority (currently ignored).
%%
%% {`iid', iid()} - Any identifier the originator wants to save in the job.
%% This field is ignored by all job handling operations; defaults to `anon'.
%%
%% {`killed', func()} - A function, with arguments, to be called if the job
%% is killed, cancelled, or crashed. The function may be specified as {M, F, A}
%% or {F, A}; the default is `undefined'.
%% The function will be invoked with a Reason term prepended to the specified
%% Args list, so the arity of the function must be one more than length(A).
%%
%% Unrecognized properties are ignored, so it IS NOT an error if the provided
%% property list includes irrelevant values.
%%
%% @see work/2
%%
job(Props) when erlang:is_list(Props) ->
    InitTS = ?CUR_TIMESTAMP,
    {Work, PW} = case lists:keytake(work, 1, Props) of
        false ->
            {undefined, Props};
        {value, {_, WorkVal}, NewPW} when ?is_work(WorkVal) ->
            {WorkVal, NewPW};
        {value, Spec, _} ->
            erlang:error(badarg, [Spec])
    end,
    %% 'cid' and 'module' get special handling because they're used in 'gid'
    {CID, PC} = case lists:keytake(cid, 1, PW) of
        {value, {_, CidVal}, NewPC} ->
            {CidVal, NewPC};
        false ->
            {anon, PW}
    end,
    {Mod, PM} = case lists:keytake(module, 1, PC) of
        {value, {_, ModVal}, NewPM} ->
            {ModVal, NewPM};
        false ->
            {?MODULE, PC}
    end,
    %% An earlier 'created' is allowed to accommodate passing through when
    %% the job was created by the client.
    %% Allow a little wiggle room for clock skew between hosts, but default to
    %% when this operation was called if it's too far in the future.
    {TS, PT} = case lists:keytake(created, 1, PM) of
        {value, {_, TsVal}, NewPT} ->
            case timer:now_diff(InitTS, TsVal) < ?MAX_INIT_SKEW_uSECS of
                true ->
                    {TsVal, NewPT};
                _ ->
                    {InitTS, PM}
            end;
        false ->
            {InitTS, PM}
    end,
    jobp(PT, #riak_core_job_v1{
        gid   = ?GLOBAL_ID(Mod, CID),
        cid   = CID,
        work  = Work,
        stats = [{created, TS}]
    }).

-spec killed(Job :: job()) -> func() | undefined.
%%
%% @doc Retrieve Job's Killed function.
%%
killed(#riak_core_job_v1{killed = Func}) ->
    Func.

-spec label(Job :: job()) -> term().
%%
%% @doc Retrieve Job's Label.
%%
label(#riak_core_job_v1{class = Class, gid = GID})
    -> {Class, GID}.

-spec main(Work :: work()) -> func().
%%
%% @doc Retrieve Work's `main' function.
%%
main(#riak_core_work_v1{main = Func}) ->
    Func.

-spec passthrough(Arg :: term()) -> term().
%%
%% @doc Returns the supplied argument.
%%
%% This is the default function used to "fill in the blanks" when populating
%% a work record.
%%
passthrough(Arg) ->
    Arg.

-spec prio(Job :: job()) -> priority().
%%
%% @doc Retrieve Job's Priority.
%%
prio(#riak_core_job_v1{prio = Val}) ->
    Val.

-spec priority(Job :: job()) -> priority().
%%
%% @doc Retrieve Job's Priority.
%%
priority(Job) ->
    prio(Job).

-spec reply(From :: job() | sender() | pid(), Message :: term()) -> term().
%%
%% @doc Sends the specified Message to a Job's `from' field.
%%
%% If the `from' value is a Pid, Message is sent as if by the `!' operator.
%%
%% If `from' is a tuple, it is assumed to be recognized by, and Message is
%% sent with, the {@link riak_core_vnode:reply/2} function.
%%
%% If `from' is `ignore' Message is not sent.
%%
%% The message is always sent unreliably without blocking, so delivery is
%% NOT assured.
%%
%% Message is returned regardless of the outcome of the operation.
%%
reply(#riak_core_job_v1{from = From}, Msg) ->
    reply(From, Msg);
reply(ignore, Msg) ->
    Msg;
reply(From, Msg) when erlang:is_pid(From) ->
    _ = erlang:send(From, Msg, [noconnect, nosuspend]),
    Msg;
reply(From, Msg) when erlang:is_tuple(From) ->
    _ = riak_core_vnode:reply(From, Msg),
    Msg.

-spec runnable(JobWorkFunc :: job() | work() | func())
        -> true | {error, term()}.
%%
%% @doc Checks whether the specified JobWorkFunc object can be run locally.
%%
%% The order of tests is NOT specified, and the result of the first test that
%% fails is returned. If all tests pass, `true' is returned, which does not
%% guarantee that the object will run successfully, only that it can be invoked
%% in the local environment.
%%
%% Following are some (though possibly not all) of the errors that may be
%% reported:
%%
%% {`error', `no_work'} - No unit of work has been assigned to the Job yet.
%%
%% {`error', {`no_module', {Module, Function, Arity}}} - The specified Module
%% is not present in the system.
%%
%% {`error', {`no_function', {Module, Function, Arity}}} - The specified Module
%% does not export Function/Arity.
%%
%% {`error', {`arity_mismatch', {Function, Arity}, Expected}} - The specified
%% Function accepts Arity arguments, but the specification would call it with
%% Expected arguments.
%%
%% @see job/1
%% @see work/1
%% @see work/2
%%
runnable({Fun, Args})
        when erlang:is_function(Fun) andalso erlang:is_list(Args) ->
    NArgs = (erlang:length(Args) + 1),
    case erlang:fun_info(Fun, arity) of
        {_, NArgs} ->
            true;
        {_, Arity} ->
            {error, {arity_mismatch, {Fun, Arity}, NArgs}}
    end;
runnable({Mod, Func, Args})
        when erlang:is_atom(Mod)
        andalso erlang:is_atom(Func)
        andalso erlang:is_list(Args) ->
    % Note: doing this the long way instead of with erlang:function_exported/3
    % because that function returns 'false' for BIFs ... this way also allows
    % us to provide some information about what wasn't found.
    Arity = (erlang:length(Args) + 1),
    case code:which(Mod) of
        non_existing ->
            {error, {no_module, {Mod, Func, Arity}}};
        _ ->
            case lists:member({Func, Arity}, Mod:module_info(exports)) of
                true ->
                    true;
                _ ->
                    {error, {no_function, {Mod, Func, Arity}}}
            end
    end;
runnable(#riak_core_work_v1{setup = Setup, main = Main, cleanup = Cleanup}) ->
    case runnable(Setup) of
        true ->
            case runnable(Main) of
                true ->
                    runnable(Cleanup);
                MErr ->
                    MErr
            end;
        SErr ->
            SErr
    end;
runnable(#riak_core_job_v1{work = Work, killed = undefined}) ->
    runnable(Work);
runnable(#riak_core_job_v1{work = Work, killed = Killed}) ->
    case runnable(Work) of
        true ->
            runnable(Killed);
        Err ->
            Err
    end;
runnable(undefined) ->
    {error, no_work}.

-spec setup(Work :: work()) -> func().
%%
%% @doc Retrieve Work's `setup' function.
%%
setup(#riak_core_work_v1{setup = Func}) ->
    Func.

-spec stats(Job :: job()) -> [stat()].
%%
%% @doc Retrieve Job's Statistics.
%%
stats(#riak_core_job_v1{stats = Stats}) ->
    ?StatsDict:to_list(Stats).

-spec timestamp() -> time().
%%
%% @doc Returns the current system time.
%%
timestamp() ->
    ?CUR_TIMESTAMP.

-spec update(Stat :: stat_key(), Job :: job()) -> job().
%%
%% @doc Sets the specified statistic to the current timestamp.
%%
update(Stat, Job) ->
    update(Stat, ?CUR_TIMESTAMP, Job).

-spec update(Stat :: stat_key(), Value :: stat_val(), Job :: job()) -> job().
%%
%% @doc Sets the specified statistic to the supplied value.
%%
update(Stat, Value, #riak_core_job_v1{stats = Stats} = Job) ->
    Job#riak_core_job_v1{stats = ?StatsDict:store(Stat, Value, Stats)}.

-spec versions() -> [atom()].
%%
%% @doc Return the version atoms supported by the module, in descending order.
%%
versions() ->
    [v1].

-spec version(Object :: term()) -> {atom(), atom()} | undefined.
%%
%% @doc Return the type and version atom of the specified Job/Work object.
%%
%% If the object is not a recognized record format `undefined' is returned.
%%
version(Object) when erlang:is_record(Object, riak_core_job_v1) ->
    {job, v1};
version(Object) when erlang:is_record(Object, riak_core_work_v1) ->
    {work, v1};
version(_) ->
    undefined.

-spec work(Props :: [{setup | main | cleanup, func()} | term()]) -> work()
        ; (Job :: job()) -> work() | undefined .
%%
%% @doc Create a new unit of work OR retrieve a Job's unit of work.
%%
%% The specification of a unit of work consists of three functions, specified
%% as the vallues associated with the following keys in the input proplist:
%%
%%  `setup' - the Setup function receives its arguments preceded by a
%%  riak_core_job_manager:mgr_rec() up the running Job in the Manager, and
%%  returns Context1.
%%  If the remaining functions need to call their Manager, then Setup should
%%  store the Manager and Key in the context that it returns for propagation.
%%
%%  `main' - the Main function receives its arguments preceded by Context1 and
%%  returns Context2.
%%  This function is assumed to perform the bulk of the work.
%%
%%  `cleanup' - the Cleanup function receives its arguments preceded by
%%  Context2 and returns the result of the unit of work, which is stored as
%%  a statistic in the Job under the key `result'.
%%
%% Because the argument lists are variable, the function types can't be fully
%% specified, but if we could they'd look something like this:
%%
%%  setup_fun()   :: fun((mgr_rec(), Arg1 ... ArgN) -> context1()).
%%
%%  main_fun()    :: fun((context1(), Arg1 ... ArgN) -> context2()).
%%
%%  cleanup_fun() :: fun((context2(), Arg1 ... ArgN) -> result()).
%%
%% In short, the arity of each of the functions is one more than the length
%% of the arguments list its initialized with.
%%
%% A work object's defaults are such that each unspecified function simply
%% returns the argument passed to it, however one or more functions MUST be
%% specified in the input properties - a `badarg' error is raised if none of
%% the keys [`setup', `main', `cleanup'] are present in the input proplist.
%%
%% Depending how and where they're specified, functions may not be fully
%% validated until the unit of work reaches the Manager upon which it is to
%% be executed, so it's entirely possible to create a unit of work using this
%% function that cannot be executed, or does nothing worthwhile, at its
%% destination.
%%
%% Unrecognized properties are ignored, so it IS NOT an error if the input
%% list contains irrelevant values.
%%
%% @see job/1
%% @see riak_core_job_manager:get_job/1
%%
work(#riak_core_job_v1{work = Work}) ->
    Work;
work([_|_] = Props) ->
    case work_check(Props) of
        true ->
            #riak_core_work_v1{
                setup   = workp(setup,    Props),
                main    = workp(main,     Props),
                cleanup = workp(cleanup,  Props) };
        _ ->
            erlang:error(badarg, [Props])
    end.

-spec work(Job :: job(), Work :: work()) -> job().
%%
%% @doc Sets a Job's unit of work.
%%
work(#riak_core_job_v1{} = Job, Work) when ?is_work(Work) ->
    Job#riak_core_job_v1{work = Work}.

%% ===================================================================
%% Internal
%% ===================================================================

-spec jobp(Props :: [{atom(), term()} | term()], Job :: job()) -> job().
jobp([], Job) ->
    Job;
jobp([{class, Class} | Props], Job)
        when    erlang:is_atom(Class) ->
    jobp(Props, Job#riak_core_job_v1{class = Class});
jobp([{class, {C1, C2} = Class} | Props], Job)
        when    erlang:is_atom(C1)
        andalso erlang:is_atom(C2) ->
    jobp(Props, Job#riak_core_job_v1{class = Class});
jobp([{from, From} | Props], Job)
        when    erlang:is_pid(From)
        orelse  erlang:is_tuple(From)
        orelse  From =:= ignored ->
    jobp(Props, Job#riak_core_job_v1{from = From});
jobp([{prio, Prio} | Props], Job)
        when    erlang:is_integer(Prio)
        andalso Prio >= ?JOB_PRIO_MIN
        andalso Prio =< ?JOB_PRIO_MAX ->
    jobp(Props, Job#riak_core_job_v1{prio = Prio});
jobp([{iid, ID} | Props], Job) ->
    jobp(Props, Job#riak_core_job_v1{iid = ID});
jobp([{killed, {Mod, Func, Args} = CB} | Props], Job)
        when    erlang:is_atom(Mod)
        andalso erlang:is_atom(Func)
        andalso erlang:is_list(Args) ->
    jobp(Props, Job#riak_core_job_v1{killed = CB});
jobp([{killed, {Fun, Args} = CB} | Props], Job)
        when    erlang:is_list(Args)
        andalso erlang:is_function(Fun, (erlang:length(Args) + 1)) ->
    jobp(Props, Job#riak_core_job_v1{killed = CB});
jobp([_ | Props], Job) ->
    jobp(Props, Job).

-spec work_check(Props :: [{atom(), term()} | term()]) -> boolean().
work_check([{setup, _} | _]) ->
    true;
work_check([{main, _} | _]) ->
    true;
work_check([{cleanup, _} | _]) ->
    true;
work_check([_ | Props]) ->
    work_check(Props);
work_check([]) ->
    false.

-spec workp(Key :: atom(), Props :: [{atom(), term()} | term()]) -> func().
workp(Key, Props) ->
    case lists:keyfind(Key, 1, Props) of
        false ->
            {?MODULE, passthrough, []};
        {_, {Mod, Func, Args} = MFA}
                when    erlang:is_atom(Mod)
                andalso erlang:is_atom(Func)
                andalso erlang:is_list(Args) ->
            MFA;
        {_, {Fun, Args} = FA}
                when    erlang:is_list(Args)
                andalso erlang:is_function(Fun, (erlang:length(Args) + 1)) ->
            FA;
        Bad ->
            erlang:error(badarg, [Bad])
    end.

%% ===================================================================
%% Internal
%% ===================================================================

-ifdef(TEST).

gid_guard_test() ->
    Dummy = dummy(),
    DummyId = gid(Dummy),
    DummyIdOk = ?is_job_gid(DummyId),
    ?assert(DummyIdOk),
    Job = job([
        {cid, {test, ?LINE}},
        {work, work(Dummy)}
    ]),
    JobId = gid(Job),
    ?debugVal(JobId),
    JobIdOk = ?is_job_gid(JobId),
    ?assert(JobIdOk).

-endif.
