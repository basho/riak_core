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
%% A Job is a wrapper around a logical unit of work (UoW) that can be executed
%% independently on multiple physical and/or virtual nodes in a cluster. All of
%% the executions of a given job are correlatable through the job's globally
%% unique identifier.
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
    unspec/0,
    version/0,
    work/0
]).

-ifdef(TEST).
-export([
    test_func/2,
    test_func/3
]).
-endif.

-include("riak_core_job_internal.hrl").

%% ===================================================================
%% Jobs handled by riak_core_job_manager
%% ===================================================================

-define(StatsDict,  orddict).
-type stats()   ::  ?orddict_t(stat_key(), stat_val()).

-define(UNSPEC, anon).

%%
%% Fields without defaults are REQUIRED, even though the language can't
%% enforce that - though functions here that create instances do.
%% A record where required fields are 'undefined' is malformed.
%%
-record(riak_core_job_v1, {
    %% The work, its 'class' (for management grouping), and where it's from.
    class   = ?UNSPEC           :: unspec() | class(),
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
    cid     = ?UNSPEC           :: unspec() | cid(),
    iid     = ?UNSPEC           :: unspec() | iid(),

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
%%
%% <em>Note:</em> This does <i>NOT</i> identify a client, at least as far as
%% the jobs system is concerned - the value is entirely opaque from the
%% perspective of the containing job.
%%
%% This element is incorporated into the {@link gid()} type.

-type class() :: atom() | {atom(), atom()}.
%% The class of a Job, used to determine whether the job is enabled as if by
%% {@link riak_core_util:job_class_enabled/2}.

-type from() :: sender() | pid().
%% The recipient of status messages, if any, about a Job.

-type func() :: {module(), atom(), list()} | {fun(), list()}.
%% A callback function.

?opaque gid() :: {module(), cid(), uid()}.
%% A Job's Global ID, which is unique at least across a cluster.
%% Values of this type are created with their associated Job and are immutable.

-type iid() :: term().
%% An updatable opaque Internal ID within a Job for any use handlers of the
%% Job choose.

?opaque job() :: #riak_core_job_v1{}.
%% The type returned by {@link job/1}.

-type priority() :: ?JOB_PRIO_MIN..?JOB_PRIO_MAX.
%% Job priority. Lower numbers have lower priority.
%%
%% <em>Note:</em> The current implementation ignores priorities, though they
%% are validated if provided.

-type stat() :: {stat_key(), stat_val()}.
%% A single statistic.
%% <i>Most</i> Job statistics are in the form
%% <code>{<i>Key</i>, {@link time()}}</code>.

-type stat_key() :: atom() | tuple().
%% The Key by which a statistic is referenced.
%% <i>Most</i> statistics keys are simple, single-word atoms.

-type stat_val() :: term().
%% The value of a statistic, most often a {@link time(). timestamp}.

-type time() :: erlang:timestamp().
%% The type returned by the {@link timestamp/1} function, and embedded in
%% statistics updated with the {@link update/2} function.

-type unspec() :: ?UNSPEC.
%% The value of <i>most</i> unspecified fields in a {@link job()}.

-type version() :: v1.
%% A supported object version.

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

-spec cid(Job :: job() | gid()) -> cid().
%%
%% @doc Retrieves a Job's Client ID.
%%
%% Note that the Client ID can be extracted from a Global ID as well.
%%
cid(#riak_core_job_v1{cid = CId}) ->
    CId;
cid(GId) when ?is_job_gid(GId) ->
    erlang:element(2, GId).

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

-spec client_id(Job :: job() | gid()) -> cid().
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
%%
%% Every element of the returned Job is constant across nodes and time,
%% including the {@link gid(). Global ID}, which can never appear anywhere
%% else.
%%
%% The implementation strategy allows the returned value to effectively be a
%% static constant, so it's a cheap call that doesn't create multiple
%% instances.
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
%% A Job can be created without a unit of work (UoW), to allow "filling in the
%% blanks" at a later point in its traversal of the cluster.
%%
%% Obviously, a UoW must be assigned before the Job is
%% {@link runnable/1. runnable}.
%%
%% The provided property list includes Key/Value tuples with which to
%% initialize the object. The following tuples are recognized:
%%
%% <dl>
%%  <dt><code>{work, <i>Work</i> :: {@link work()}}</code></dt>
%%  <dd>The Job's UoW; <i>Work</i> is an object of the type returned by
%%      {@link work/1}.</dd>
%%
%%  <dt><code>{class, <i>Class</i> :: {@link class()}}</code></dt>
%%  <dd>The Job's <i>Class</i>; default is {@link unspec()}.</dd>
%%
%%  <dt><code>{from, <i>From</i> :: {@link from()}}</code></dt>
%%  <dd>The Job's originator; default is `ignore'.</dd>
%%  <dd>Used by the {@link reply/2} function, which itself is only called by
%%      the job management system if the job crashes or is canceled <i>and</i>
%%      `killed' is unset (or raises an error on invocation). In that case
%%      a message in the form
%%      <code>{error, <i>Job</i> :: job(), <i>Why</i> :: term()}</code> is sent
%%      to it - if you set this field, be prepared to receive that message.</dd>
%%  <dd><em>Warning:</em> Because there are so many valid representations of
%%      the {@link sender()} type in riak_core (and because there appear to be
%%      formats in use that are not covered by the type specification at all),
%%      it's not feasible to fully check them when the job is created, so
%%      exceptions from {@link reply/2} are possible if you try to roll your
%%      own.</dd>
%%
%%  <dt><code>{cid, <i>ClientID</i> :: {@link cid()}}</code></dt>
%%  <dd>The Job's client-assigned identifier; default is {@link unspec()}.</dd>
%%  <dd>This field is incorporated into the Job's {@link gid(). Global ID}.</dd>
%%
%%  <dt><code>{created, <i>Created</i> :: {@link time()}}</code></dt>
%%  <dd>Sets the Job's `created' statistic to the specified timestamp; default
%%      is the current time as returned by {@link timestamp/0}.</dd>
%%  <dd>This statistic can be set to a time in the past to incorporate knowledge
%%      the originator has about the true provenance of the Job.</dd>
%%  <dd>Some clock skew is accepted, but if an attempt is made to set the value
%%      more than approximately 1.5 seconds into the future (according to the
%%      local clock) then the default is silently used.</dd>
%%  <dd><i>Note that because this is a statistic, not a distinct field in the
%%      Job structure, it can be updated (without the clock skew constraints),
%%      though doing so is not recommended.</i></dd>
%%
%%  <dt><code>{prio, <i>Priority</i> :: {@link priority()}}</code></dt>
%%  <dd>The Job's priority; currently ignored.</dd>
%%
%%  <dt><code>{iid, <i>InternalID</i> :: {@link iid()}}</code></dt>
%%  <dd>Any identifier the originator wants to save in the Job;
%%      default is {@link unspec()}.</dd>
%%  <dd>This field is ignored by all job handling operations and can be changed
%%      with {@link internal_id/2}.</dd>
%%
%%  <dt><code>{killed, <i>Killed</i> :: {@link func()}}</code></dt>
%%  <dd>A function, with arguments, to be called if the job crashes or is
%%      killed or canceled. The function may be specified as
%%      <code>{<i>Mod</i>, <i>Func</i>, <i>Args</i>}</code> or
%%      <code>{<i>Fun</i>, <i>Args</i>}</code>; the default is `undefined'.</dd>
%%  <dd>The function will be invoked with a <i>Reason</i> term prepended to the
%%      specified <i>Args</i> list, so the arity of the function must be one
%%      more than <code>length(<i>Args</i>)</code>.</dd>
%% </dl>
%%
%% Unrecognized properties are ignored, so it is not an error if the provided
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
            {?UNSPEC, PW}
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
%% @doc Checks whether the specified <i>JobWorkFunc</i> can be run locally.
%%
%% The order of tests is <i>NOT</i> specified, and the result of the first test
%% that fails is returned. If all tests pass, `true' is returned, which does
%% not guarantee that the object will run successfully, only that it can be
%% invoked in the local environment.
%%
%% Following are some (though possibly not all) of the errors that may be
%% reported:
%%
%% <dl>
%%  <dt><code>{error, no_work}</code></dt>
%%  <dd>Input is a {@link job(). Job} to which no UoW has been assigned.</dd>
%%
%%  <dt><code>{error, {no_module,
%%      {<i>Module</i>, <i>Function</i>, <i>Arity</i>}}}</code></dt>
%%  <dd>The specified <i>Module</i> is not present in the system.</dd>
%%
%%  <dt><code>{error, {no_function,
%%      {<i>Module</i>, <i>Function</i>, <i>Arity</i>}}}</code></dt>
%%  <dd>The specified <i>Module</i> does not export <i>Function/Arity</i>.</dd>
%%
%%  <dt><code>{error, {arity_mismatch,
%%      {<i>Function</i>, <i>Arity</i>}, <i>Expected</i>}}</code></dt>
%%  <dd>The specified <i>Function</i> accepts <i>Arity</i> arguments, but the
%%      system would call it with <i>Expected</i> arguments.</dd>
%% </dl>
%%
%% <i>Note:</i> Errors do <i>NOTt</i> indicate which function within a Job or
%% UoW did not pass validation - one more reason to prefer MFA specifications
%% over closures.
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
%% @doc Sets the specified statistic to the current
%% {@link timestamp/0. timestamp}.
%%
update(Stat, Job) ->
    update(Stat, ?CUR_TIMESTAMP, Job).

-spec update(Stat :: stat_key(), Value :: stat_val(), Job :: job()) -> job().
%%
%% @doc Sets the specified statistic to the supplied value.
%%
update(Stat, Value, #riak_core_job_v1{stats = Stats} = Job) ->
    Job#riak_core_job_v1{stats = ?StatsDict:store(Stat, Value, Stats)}.

-spec versions() -> [version()].
%%
%% @doc Return the version atoms supported by the module, in descending order.
%%
versions() ->
    [v1].

-spec version(Object :: term()) -> {job | work, version()} | undefined.
%%
%% @doc Return the type and version of the specified Job/Work object.
%%
%% If the object is not a recognized record format `undefined' is returned.
%%
version(Object) when erlang:is_record(Object, riak_core_job_v1) ->
    {job, v1};
version(Object) when erlang:is_record(Object, riak_core_work_v1) ->
    {work, v1};
version(_) ->
    undefined.

-spec work(JobOrProps :: job() | [{setup | main | cleanup, func()} | term()])
        -> work() | undefined.
%%
%% @doc Create a new UoW <i>OR</i> retrieve a Job's UoW.
%%
%% The specification of a UoW consists of three functions, any (but not all) of
%% which may be excluded.
%%
%% <i>This model has been chosen to allow sharing common functions across UoWs
%% in an effort to avoid large, opaque closures - use it wisely.</i>
%%
%% The functions are specified as follows in the input proplist:
%%
%% <dl>
%%  <dt><code>{setup, <i>Setup</i> :: func()}</code></dt>
%%  <dd>The <i>Setup</i> function receives its arguments preceded by a
%%      {@link riak_core_job_manager:mgr_key(). MgrKey} and returns
%%      <i>Context1</i>.</dd>
%%  <dd>If the remaining functions need to access the current state of the
%%      running Job, then <i>Setup</i> should store <i>MgrKey</i> in the
%%      <i>Context1</i> that it returns for propagation.</dd>
%%
%%  <dt><code>{main, <i>Main</i> :: func()}</code></dt>
%%  <dd>The <i>Main</i> function receives its arguments preceded by
%%      <i>Context1</i> and returns <i>Context2</i>.</dd>
%%  <dd>This function is assumed to perform the bulk of the work.</dd>
%%
%%  <dt><code>{cleanup, <i>Cleanup</i> :: func()}</code></dt>
%%  <dd>The <i>Cleanup</i> function receives its arguments preceded by
%%      <i>Context2</i> and returns the <i>Result</i> of the UoW.</dd>
%%  <dd>If the enclosing Job is executed by the {@link riak_core_job_manager}
%%      then <i>Result</i> is stored as a statistic in the Job under the key
%%      `result'.</dd>
%% </dl>
%%
%% Because the argument lists are variable, the function types can't be fully
%% specified, but if we could they'd look something like this:
%%
%%  <code>setup_fun() :: fun((<i>MgrKey</i>, <i>Arg1</i>...<i>ArgN</i>)
%%      -> <i>Context1</i>).</code>
%%
%%  <code>main_fun() :: fun((<i>Context1</i>, <i>Arg1</i>...<i>ArgN</i>)
%%      -> <i>Context2</i>).</code>
%%
%%  <code>cleanup_fun() :: fun((<i>Context2</i>, <i>Arg1</i>...<i>ArgN</i>)
%%      -> <i>Result</i>).</code>
%%
%% In short, the arity of each of the functions is one more than the length
%% of the arguments list it's initialized with.
%%
%% <i>Note:</i> While the specifications above are of closures, providing
%% functions as MFA tuples is preferred for visibility and debugging.
%%
%% A work object's defaults are such that each unspecified function simply
%% returns the argument passed to it, however one or more functions <i>MUST</i>
%% be specified in the input properties. A `badarg' error is raised if none of
%% the keys `setup', `main', or `cleanup' are present in the input proplist.
%%
%% Depending how and where they're specified, functions may not be fully
%% validated until the unit of work reaches the Manager upon which it is to
%% be executed, so it's entirely possible to create a unit of work using this
%% function that cannot be executed, or does nothing worthwhile, at its
%% destination.
%%
%% Unrecognized properties are ignored, so it is not an error if the provided
%% property list includes irrelevant values.
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
    JobId = global_id(Job),
    JobIdOk = ?is_job_gid(JobId),
    ?assert(JobIdOk).

id_test() ->
    DummyJob = dummy(),
    DummyWork = work(DummyJob),
    DummyGId = gid(DummyJob),

    CId = {erlang:phash2(os:timestamp()), erlang:phash2(erlang:self())},
    Mod = 'Some@nonexistant$thing',
    Job = job([
        {module,    Mod},
        {cid,       CId},
        {work,      DummyWork}
    ]),
    GId = gid(Job),

    ?assertMatch({_, _, _}, GId),
    {GIdMod, GIdCId, GIdUId} = GId,
    ?assertEqual(Mod, GIdMod),
    ?assertEqual(CId, GIdCId),
    ?assertMatch({_, _, _}, GIdUId),

    ?assertMatch({_, _, _}, DummyGId),
    ?assertMatch(<<_:128>>, element(3, DummyGId)),
    ?assertEqual(?MODULE, element(1, DummyGId)),
    ?assertEqual(dummy, element(2, DummyGId)),
    ?assertEqual(?DUMMY_JOB_UID, element(3, DummyGId)),

    ?assertEqual(CId, client_id(Job)),
    ?assertEqual(CId, client_id(GId)),
    ?assertEqual(GId, global_id(Job)),

    ?assertEqual(?UNSPEC, iid(DummyJob)),
    ?assertEqual(?UNSPEC, internal_id(Job)),

    AltIId = [some, other, stuff],
    AltJob = internal_id(Job, AltIId),
    ?assertEqual(AltIId, internal_id(AltJob)).

runnable_test() ->
    Bogus = 'No$Such-Name...We-Hope',
    Fun1 = fun(Arg) -> Arg end,
    ?assertEqual(true, runnable({?MODULE, test_func, [2]})),
    ?assertEqual(true, runnable({?MODULE, test_func, [2, 3]})),
    ?assertMatch({error, _}, runnable({?MODULE, test_func, [2, 3, 4]})),
    ?assertMatch({error, _}, runnable({Bogus, test_func, []})),
    ?assertMatch({error, _}, runnable({?MODULE, Bogus, []})),
    ?assertEqual(true, runnable({Fun1, []})),
    ?assertMatch({error, _}, runnable({Fun1, [2]})).

test_func(Arg1, _) ->
    Arg1.

test_func(Error, _, _) ->
    erlang:error(Error).

-endif.
