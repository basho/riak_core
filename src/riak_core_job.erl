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

-module(riak_core_job).

-export([
    cap/2,
    dummy/0,
    get/2,
    job/1,
    passthrough/1,
    runnable/1,
    timestamp/0,
    update/2, update/3,
    version/1, versions/0,
    work/1
]).

-export_type([
    class/0,
    func/0,
    id/0,
    job/0,
    priority/0,
    stat/0,
    time/0,
    unspec/0,
    work/0
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-include("riak_core_vnode.hrl").

%% ===================================================================
%% Vnode Jobs handled by riak_core_job_mgr
%% ===================================================================

%% Fields without defaults are REQUIRED, even though the language can't
%% enforce that. A record where required fields are 'undefined' is malformed.
-record(riak_core_job_v1, {
    %% The work, its 'class' (for management grouping), and where it's from.
    class   = 'anon'                    :: unspec() | class(),
    from    = 'ignore'                  :: sender(),
    work                                :: work(),

    %% Currently unimplemented, but here so we don't need to define a new
    %% version of the record when (or if) it is ...
    prio    = ?VNODE_JOB_PRIO_DEFAULT   :: priority(),

    %% If supplied, the 'cid' is presumed to mean something to the
    %% originator of the work, and is included as an element in the 'gid'.
    %% If supplied, 'iid' is an internal tracking ID that means something to
    %% the originator and is carried around untouched.
    gid                                 :: id(),
    cid     = 'anon'                    :: unspec() | id(),
    iid     = 'anon'                    :: unspec() | id(),

    %% Optional callback to be invoked if the job is killed or cancelled.
    killed  = 'undefined'               :: 'undefined' | func(),

    %% Keep some statistics around, including, but not limited to, state
    %% transition timestamps.
    stats   = []                        :: [stat()]
}).

%%
%% The basic idea is that the specification of a unit of work is 3 functions:
%% init, run, and fini. 'init' receives a tuple with two elements, the vnode
%% id and manager, followed by its arguments and returns a context, which is
%% provided to 'run' as its first argument, followed by its arguments. The
%% pattern is repeated with 'fini', whose result is
%% TODO: [how] is fini's result handled?
%%
%% We can't properly specify the types, but if we could they'd look something
%% like this:
%%  init_fun()  :: fun(({node_id(), pid()}, arg1(), ..., argN()) -> context1().
%%  run_fun()   :: fun((context1(), arg1(), ..., argN()) -> context2().
%%  fini_fun()  :: fun((context2(), arg1(), ..., argN()) -> result().
%% In short, the arity of a function 'F' is one more than length(Args).
%%
%% A work object's defaults are such that each unspecified function simply
%% returns the argument passed to it, making an uninitialized instance well
%% formed but otherwise useless.
%%
-record(riak_core_work_v1, {
    init  = {?MODULE, passthrough, []}  :: func(),
    run   = {?MODULE, passthrough, []}  :: func(),
    fini  = {?MODULE, passthrough, []}  :: func()
}).

-type class()       :: unspec() | atom().
-type func()        :: {module(), atom(), [term()]} | {fun(), [term()]}.
-type id()          :: term().
-type job()         :: #riak_core_job_v1{}.
-type priority()    :: ?VNODE_JOB_PRIO_MIN..?VNODE_JOB_PRIO_MAX.
-type stat()        :: {atom(), term()}.
-type time()        :: erlang:timestamp().
-type unspec()      :: 'anon'.
-type work()        :: #riak_core_work_v1{}.

-define(MAX_INIT_SKEW_uSECS,    1500000).
-define(UNIQUE_ID(Mod, CID),    {CID, Mod, erlang:node(), erlang:make_ref()}).
-define(CUR_TIMESTAMP,          os:timestamp()).

%% ===================================================================
%% Public API
%% ===================================================================

-spec get(atom(), job() | work()) -> term() | no_return().
%%
%% @doc Return the value of the specified record field.
%%
get(cid,    #riak_core_job_v1{cid = Val}) -> Val;
get(class,  #riak_core_job_v1{class = Val}) -> Val;
get(from,   #riak_core_job_v1{from = Val}) -> Val;
get(gid,    #riak_core_job_v1{gid = Val}) -> Val;
get(iid,    #riak_core_job_v1{iid = Val}) -> Val;
get(killed, #riak_core_job_v1{killed = Val}) -> Val;
get(label,  #riak_core_job_v1{class = Class, gid = GID}) -> {Class, GID};
get(prio,   #riak_core_job_v1{prio = Val}) -> Val;
get(stats,  #riak_core_job_v1{stats = Val}) -> Val;
get(work,   #riak_core_job_v1{work = Val}) -> Val;
get(fini,   #riak_core_work_v1{fini = Val}) -> Val;
get(init,   #riak_core_work_v1{init = Val}) -> Val;
get(run,    #riak_core_work_v1{run = Val}) -> Val;
get(Field, Record) ->
    erlang:error(badarg, [Field, Record]).

-spec job([{atom(), term()}]) -> job() | no_return().
%%
%% @doc Create and initialize a new job record.
%%
%% The provided property list includes Key/Value tuples with which to
%% initialize the object. The following tuples are recognized:
%%
%% {'work', Work} - Work is an object of the type returned by the work/1
%% function. This initializer is REQUIRED!
%%
%% {'class', atom()} - The job's class; default is 'anon'.
%%
%% {'from', sender()} - The job's originator; default is 'ignore'.
%%
%% {'cid', term()} - A client job identifier. This field is incorporated in
%% the job's global ID; defaults to 'anon'.
%%
%% {'prio', priority()} - The job's priority (currently ignored).
%%
%% {'iid', term()} - Any identifier the originator wants to save in the job.
%% This field is ignored by all job handling operations; defaults to 'anon'.
%%
%% {'killed', func()} - A function, with arguments, to be called if the job
%% is killed, cancelled, or crashed. The function may be specified as {M, F, A}
%% or {F, A}; the default is 'undefined'.
%% The function will be invoked with a Reason term prepended to the specified
%% Args list, so the arity of the function must be one more than length(A).
%%
job(Props) ->
    InitTS = ?CUR_TIMESTAMP,
    %% 'work' is the only required field
    {Work, PW} = case lists:keytake(work, 1, Props) of
        {value, {_, W}, NewPW} when erlang:is_record(W, riak_core_work_v1) ->
            {W, NewPW};
        _ ->
            erlang:error(badarg, [Props])
    end,
    %% 'cid' and 'module' get special handling because they're used in 'gid'
    {CID, PC} = case lists:keytake(cid, 1, PW) of
        {value, {_, C}, NewPC} ->
            {C, NewPC};
        false ->
            {'anon', PW}
    end,
    {Mod, PM} = case lists:keytake(module, 1, PC) of
        {value, {_, M}, NewPM} ->
            {M, NewPM};
        false ->
            {?MODULE, PC}
    end,
    %% An earlier 'created' is allowed to accommodate passing through when
    %% the job was created by the client.
    %% Allow a little wiggle room for clock skew between hosts, but default to
    %% when this operation was called if it's too far in the future.
    {TS, PT} = case lists:keytake(created, 1, PM) of
        {value, {_, T}, NewPT} ->
            case timer:now_diff(InitTS, T) < ?MAX_INIT_SKEW_uSECS of
                true ->
                    {T, NewPT};
                _ ->
                    {InitTS, PM}
            end;
        false ->
            {InitTS, PM}
    end,
    jobp(PT, #riak_core_job_v1{
        gid = ?UNIQUE_ID(Mod, CID),
        cid = CID,
        work = Work,
        stats = [{created, TS}]
    }).

-spec work([{init | run | fini, func()}]) -> work() | no_return().
%%
%% @doc Create and initialize a new unit of work.
%%
%% One or more functions MUST be specified in the input properties - a `badarg'
%% exception is raised if none of the keys ['init', 'run', 'fini'] are present
%% in the input Props.
%%
%% The function types can't be properly specified with their unknown arities,
%% but if they could they'd look something like this:
%%  init_fun()  :: fun(({node_id(), pid()}, arg1(), ..., argN()) -> context1().
%%  run_fun()   :: fun((context1(), arg1(), ..., argN()) -> context2().
%%  fini_fun()  :: fun((context2(), arg1(), ..., argN()) -> result().
%% In short, the arity of each specified function is one more than length(Args).
%%
%% The defaults are such that each unspecified function simply returns the
%% initial, or context, argument passed to it. As mentioned above, at least one
%% non-default function must be provided, but functions are not fully validated
%% until the unit of work reaches the vnode upon which it is to me executed, so
%% it's entirely possible to create one using his function that cannot be
%% executed, or does nothing worthwhile, at its destination.
%%
%% Unrecognized properties are ignored, so it IS NOT an error if the provided
%% property list includes irrelevant values.
%%
work([_|_] = Props) ->
    case lists:any(fun work_check/1, Props) of
        true ->
            I = workp(init, Props),
            R = workp(run,  Props),
            F = workp(fini, Props),
            #riak_core_work_v1{init = I, run = R, fini = F};
        _ ->
            erlang:error(badarg, [Props])
    end;
work(Input) ->
    erlang:error(badarg, [Input]).

-spec dummy() -> job().
%%
%% @doc Returns a valid Job object for testing/validation.
%%
%% The returned instance's 'work' element does nothing.
%%
dummy() ->
    job([
        {class, 'dummy'},
        {cid,   'dummy'},
        {prio,  ?VNODE_JOB_PRIO_MIN},
        {work,  #riak_core_work_v1{}}
    ]).

-spec cap(atom(), job() | work()) -> job() | work() | no_return().
%%
%% @doc Make sure a Job/Work object is understandable at the specified version.
%%
%% If necessary, the object is re-created at a lower version that is no higher
%% than the specified version.
%%
cap(v1, Object) when erlang:is_record(Object, riak_core_job_v1) ->
    Object;
cap(v1, Object) when erlang:is_record(Object, riak_core_work_v1) ->
    Object;
cap(Ver, Object) ->
    erlang:error(badarg, [Ver, Object]).

-spec passthrough(term()) -> term().
%%
%% @doc Returns the supplied argument.
%%
%% This is the default function used to "fill in the blanks" when populating
%% a work record.
%%
passthrough(Arg) ->
    Arg.

-spec runnable(job() | work() | func()) -> true | {error, term()}.
%%
%% @doc Checks whether the specified Job/Work/Function object can be run locally.
%%
%% The order of tests is NOT specified, and the result of the first test that
%% fails is returned. If all tests pass, 'true' is returned, which does not
%% guarantee that the object will run successfully, only that it can be invoked
%% in the local environment.
%%
%% Following are some (though possibly not all) of the errors that may be
%% reported:
%%
%% {'error', {'no_module', {Module, Function, Arity}}} - The specified Module
%% is not present in the system.
%%
%% {'error', {'no_function', {Module, Function, Arity}}} - The specified Module
%% does not export Function/Arity.
%%
%% {'error', {'arity_mismatch', {Function, Arity}, Expected}} - The specified
%% Function accepts Arity arguments, but the specification would call it with
%% Expected arguments.
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
runnable(#riak_core_work_v1{init = I, run = R, fini = F}) ->
    case runnable(I) of
        true ->
            case runnable(R) of
                true ->
                    runnable(F);
                RR ->
                    RR
            end;
        RI ->
            RI
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
runnable(Object) ->
    erlang:error(badarg, [Object]).

-spec timestamp() -> time().
timestamp() ->
    ?CUR_TIMESTAMP.

-spec update(Stat :: atom(), Job :: job()) -> job().
%%
%% @doc Sets the specified statistic to the current timestamp.
%%
update(Stat, Job) ->
    update(Stat, ?CUR_TIMESTAMP, Job).

-spec update(Stat :: atom(), Value :: term(), Job :: job()) -> job().
%%
%% @doc Sets the specified statistic to the specified value.
%%
update(Stat, Value, #riak_core_job_v1{stats = Stats} = Job) ->
    Job#riak_core_job_v1{stats = lists:keystore(Stat, 1, Stats, {Stat, Value})}.

-spec versions() -> [atom()].
%%
%% @doc Return the version atoms supported by the module, in descending order.
%%
versions() ->
    [v1].

-spec version(term()) -> {atom(), atom()} | undefined.
%%
%% @doc Return the type and version atom of the specified Job/Work object.
%%
%% If the object is not a recognized record format 'undefined' is returned.
%%
version(Object) when erlang:is_record(Object, riak_core_job_v1) ->
    {job, v1};
version(Object) when erlang:is_record(Object, riak_core_work_v1) ->
    {work, v1};
version(_) ->
    undefined.

%% ===================================================================
%% Internal
%% ===================================================================

jobp([], Job) ->
    Job;
jobp([{class, Class} | Props], Job)
        when erlang:is_atom(Class) ->
    jobp(Props, Job#riak_core_job_v1{class = Class});
jobp([{from, From} | Props], Job)
        when erlang:is_tuple(From)
        orelse From =:= 'ignored' ->
    jobp(Props, Job#riak_core_job_v1{from = From});
jobp([{prio, Prio} | Props], Job)
        when erlang:is_integer(Prio)
        andalso Prio >= ?VNODE_JOB_PRIO_MIN
        andalso Prio =< ?VNODE_JOB_PRIO_MAX ->
    jobp(Props, Job#riak_core_job_v1{prio = Prio});
jobp([{iid, ID} | Props], Job) ->
    jobp(Props, Job#riak_core_job_v1{iid = ID});
jobp([{killed, {Mod, Func, Args} = CB} | Props], Job)
        when erlang:is_atom(Mod)
        andalso erlang:is_atom(Func)
        andalso erlang:is_list(Args) ->
    jobp(Props, Job#riak_core_job_v1{killed = CB});
jobp([{killed, {Fun, Args} = CB} | Props], Job)
        when erlang:is_function(Fun)
        andalso erlang:is_list(Args) ->
    jobp(Props, Job#riak_core_job_v1{killed = CB});
jobp([Prop | _], _) ->
    erlang:error(badarg, [Prop]).

work_check({init, _}) ->
    true;
work_check({run, _}) ->
    true;
work_check({fini, _}) ->
    true;
work_check(_) ->
    false.

workp(Key, Props) ->
    case lists:keyfind(Key, 1, Props) of
        false ->
            {?MODULE, passthrough, []};
        {_, {Mod, Func, Args} = F}
                when erlang:is_atom(Mod)
                andalso erlang:is_atom(Func)
                andalso erlang:is_list(Args) ->
            F;
        {_, {Fun, Args} = F}
                when erlang:is_function(Fun)
                andalso erlang:is_list(Args) ->
            F;
        Bad ->
            erlang:error(badarg, [Bad])
    end.
