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
    get/2,
    job/2, job/3,
    new/1,
    update/2,
    work_class/1,
    work_from/1,
    work_label/1
]).

-export_type([
    arg/0,
    context/0,
    done_cb/0,
    fini_rec/0,
    fun_rec/0,
    id/0,
    init_rec/0,
    job/0,
    kill_cb/0,
    legacy/0,
    mfa_rec/0,
    priority/0,
    prop/0,
    props/0,
    result/0,
    run_rec/0,
    stat/0,
    time/0,
    unspec/0,
    vnode_prop/0,
    what/0,
    work/0
]).

%% ===================================================================
%% Vnode Jobs handled by worker pools
%% ===================================================================
%%
%% There are a lot of discrete named types here that resolve to very general
%% types. They exist only to try to make the type specifications sensible, but
%% clearly can't be carefully checked.
%%

%% partition() is defined in "riak_core_vnode.hrl", which can't be included
%% yet because it uses types defined here. As of this writing, it's a
%% chash:index_as_int(), which is typed as an unbounded integer, but things
%% would get very weird if it was not an integer >= 0, so use the more
%% restrictive type here.
-type vnode_prop()  :: {vnode_index, non_neg_integer()}.

%% Statistics are generally {atom(), non_neg_integer()}, but leave some wiggle
%% room for structured values.
-type stat()        :: {atom(), term()}.

%% TODO: consider not supporting this type in Riak-3/4+
%% The legacy() type will probably be with us for a long time, as it's been
%% around as the structure specifying vnode worker pool operations from the
%% start and we can't know how much 3rd-party code we'd break by removing
%% support for it.
%% ALL Riak code, however, is expected to be updated to the new formats
%% in short order.
%% The prevailing pattern for legacy() is
%%  {atom(), fun(() -> term()), fun((term()) -> term())}
%% but we can't spec it as such because it was originally set up to allow
%% anything the worker_callback_mod's handle_work function could operate on.
%% Hence, the entirely uncheckable type specification :(
-type legacy()      :: term().

-type prop()        :: vnode_prop() | atom() | {atom(), term()}.
-type props()       :: [prop()].

%%
%% The basic idea is that the preferred specification of a unit of work is 3
%% functions: init, run, and fini. 'init' receives a list of properties and
%% returns a context, which is provided to 'run' as its first argument,
%% followed by whatever arguments were provided. The pattern is repeated with
%% 'fini', whose result is TODO: how is fini's result handled?
%%
%% We can't properly specify the types, but if we could they'd look something
%% like this:
%%  init_fun()  :: fun((props(), ...) -> context().
%%  run_fun()   :: fun((context(), ...) -> context().
%%  fini_fun()  :: fun((context(), ...) -> result().
%% And be called like this:
%%  {F, A} = ...    % job_fun()
%%  P = ...         % props() | context()
%%  R = erlang:apply(F, [P | A])
%% Or this:
%%  {M, F, A} = ... % job_mfa()
%%  P = ...         % props() | context()
%%  R = erlang:apply(M, F, [P | A])
%% In short, the arity of the function 'F' is one more than length(A).
%%

-type arg()         :: term().
-type context()     :: term().
-type result()      :: term().

-type fun_rec()     :: {fun((...) -> term()), [arg()]}.
-type mfa_rec()     :: {module(), atom(), [arg()]}.

-type init_rec()    :: {fun((...) -> context()), [arg()]}
                    |  {module(), atom(), [arg()]}.
-type run_rec()     :: {fun((...) -> context()), [arg()]}
                    |  {module(), atom(), [arg()]}.
-type fini_rec()    :: {fun((...) -> result()), [arg()]}
                    |  {module(), atom(), [arg()]}.

-type unspec()      :: 'anon'.
-type what()        :: unspec() | atom().
-type id()          :: term().
-type time()        :: 0 | erlang:timestamp().

-type done_cb()     :: 'undefined' | fun_rec() | mfa_rec().
-type kill_cb()     :: 'undefined' | fun_rec() | mfa_rec().

-include("riak_core_vnode.hrl").

%% Types depending on "riak_core_vnode.hrl"
-type priority()    :: ?VNODE_JOB_PRIO_MIN..?VNODE_JOB_PRIO_MAX.
-type job()         :: #riak_core_job_v1{}.
-type work()        :: #riak_core_work_v1{} | legacy().

%% Internal, record types for get/2 spec.
-type get_rec()     :: #riak_core_job_v1{} | #riak_core_work_v1{}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(MAX_INIT_SKEW_uSECS,    1500000).
-define(UNIQUE_ID(Mod, CID),    {CID, Mod, erlang:node(), erlang:make_ref()}).
-define(CUR_TIMESTAMP,          os:timestamp()).

-define(JOB_GET(Rec, Fld, Key), get(Key, #Rec{Fld = Val}) -> Val).
-define(JOB_GET(Rec, Fld),      ?JOB_GET(Rec, Fld, Fld)).

%% ===================================================================
%% Public API
%% ===================================================================

-spec job(module(), job() | legacy()) -> job().
job(_, #riak_core_job_v1{} = Job) ->
    Job;
job(Module, Work) ->
    new([{type, work_class(Work)}, {module, Module}, {work, Work}]).

-spec job(what(), module(), job() | legacy()) -> job().
job(_, _, #riak_core_job_v1{} = Job) ->
    Job;
job(Type, Module, Work) ->
    new([{type, Type}, {module, Module}, {work, Work}]).

-spec get(atom(), get_rec()) -> term() | no_return().

?JOB_GET(riak_core_job_v1,  type);
?JOB_GET(riak_core_job_v1,  work);
?JOB_GET(riak_core_job_v1,  from);
?JOB_GET(riak_core_job_v1,  priority,   prio);
?JOB_GET(riak_core_job_v1,  global_id,  gid);
?JOB_GET(riak_core_job_v1,  client_id,  cid);
?JOB_GET(riak_core_job_v1,  priority);
?JOB_GET(riak_core_job_v1,  global_id);
?JOB_GET(riak_core_job_v1,  client_id);
?JOB_GET(riak_core_job_v1,  internal_id);
?JOB_GET(riak_core_job_v1,  init_time);
?JOB_GET(riak_core_job_v1,  queue_time);
?JOB_GET(riak_core_job_v1,  start_time);
?JOB_GET(riak_core_job_v1,  kill_cb);
?JOB_GET(riak_core_job_v1,  done_cb);
?JOB_GET(riak_core_job_v1,  stats);

?JOB_GET(riak_core_work_v1, init);
?JOB_GET(riak_core_work_v1, run);
?JOB_GET(riak_core_work_v1, fini);

get(Field, Record) ->
    erlang:error(badarg, [Field, Record]).

-spec new([{atom(), term()}]) -> job() | no_return().
new(Props) ->
    InitTS = ?CUR_TIMESTAMP,
    %% 'work' is the only required field
    {Work, PW} = case lists:keytake(work, 1, Props) of
        {value, {_, W}, NewPW} ->
            {W, NewPW};
        false ->
            erlang:error(badarg, [Props])
    end,
    %% If 'from' is not specified, see if it can be extracted from 'work'
    {From, PF} = case lists:keytake(from, 1, PW) of
        {value, {_, F}, NewPF} ->
            {F, NewPF};
        false ->
            {work_from(Work), PW}
    end,
    %% 'client_id' (or 'cid') and 'module' get special handling, because
    %% they're used in the global id
    {CID, PC} = case lists:keytake(client_id, 1, PF) of
        {value, {_, C1}, NewPC1} ->
            {C1, NewPC1};
        false ->
            case lists:keytake(cid, 1, PF) of
                {value, {_, C2}, NewPC2} ->
                    {C2, NewPC2};
                false ->
                    {'anon', PF}
            end
    end,
    {Mod, PM} = case lists:keytake(module, 1, PC) of
        {value, {_, M}, NewPM} ->
            {M, NewPM};
        false ->
            {?MODULE, PC}
    end,
    %% An earlier 'init_time' is allowed to accommodate passing through when
    %% the job was created by the client
    %% Allow a little wiggle room for clock skew between hosts, but default to
    %% when this operation was called if it's too far in the future
    {TS, PT} = case lists:keytake(init_time, 1, PM) of
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
    newp(PT, #riak_core_job_v1{
        global_id = ?UNIQUE_ID(Mod, CID),
        client_id = CID,
        init_time = TS,
        from = From,
        work = Work
    }).

-spec update(job() | legacy(), atom()) -> work() | legacy().
update(#riak_core_job_v1{} = Job, queue) ->
    Job#riak_core_job_v1{queue_time = ?CUR_TIMESTAMP};
update(#riak_core_job_v1{} = Job, start) ->
    Job#riak_core_job_v1{start_time = ?CUR_TIMESTAMP};
update(Work, _) ->
    Work.

-spec work_class(job() | legacy()) -> what().
work_class(#riak_core_job_v1{type = Type}) ->
    Type;
work_class(Work)
        when erlang:is_tuple(Work)
        andalso erlang:tuple_size(Work) > 1
        andalso erlang:is_atom(erlang:element(1, Work)) ->
    erlang:element(1, Work);
work_class(_) ->
    anon.

-spec work_label(job() | legacy()) -> term().
work_label(#riak_core_job_v1{type = Type, global_id = ID}) ->
    {Type, ID};
work_label(Work) ->
    {work_class(Work), anon}.

-spec work_from(job() | legacy()) -> sender().
work_from(#riak_core_job_v1{from = ignore, work = Work}) ->
    work_from(Work);
work_from(#riak_core_job_v1{from = From}) ->
    From;
work_from({work, _Work, From}) ->
    From;
%% Add 'work' patterns as they're discovered
work_from(Work) ->
    lager:debug("Unrecognized work: ~p", [Work]),
    ignore.

%% ===================================================================
%% Internal
%% ===================================================================

newp([], Job) ->
    Job;
newp([{type, Type} | Props], Job) when erlang:is_atom(Type) ->
    newp(Props, Job#riak_core_job_v1{type = Type});
newp([{priority, Prio} | Props], Job) when erlang:is_integer(Prio)
    andalso Prio >= ?VNODE_JOB_PRIO_MIN andalso Prio =< ?VNODE_JOB_PRIO_MAX ->
    newp(Props, Job#riak_core_job_v1{priority = Prio});
newp([{kill_cb, CB} | Props], Job) ->
    newp(Props, Job#riak_core_job_v1{kill_cb = CB});
newp([{done_cb, CB} | Props], Job) ->
    newp(Props, Job#riak_core_job_v1{done_cb = CB});
newp(Props, _) ->
    erlang:error(badarg, [Props]).
