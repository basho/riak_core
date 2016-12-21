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

-module(jobs_test_util).

% Utility API
-export([
    create_job/1,
    create_job/4,
    create_jobs/1,
    create_jobs/4,
    set_config/1,
    wait_for_job/1,
    wait_for_jobs/1
]).

% API Types
-export_type([
    work_delay/0,
    work_id/0
]).

% Exported Callbacks
-export([
    job_killed/2,
    work_cleanup/1,
    work_main/1,
    work_setup/2
]).

-include("../src/riak_core_job_internal.hrl").

%% ===================================================================
%% Types
%% ===================================================================

-record(wc, {
    id  ::  work_id(),              % client id
    own ::  pid(),                  % owner process (the test)
    mgr ::  atom() | pid(),         % riak_core_job_manager
    sd  ::  work_delay(),           % setup delay ms
    md  ::  work_delay(),           % main delay ms
    cd  ::  work_delay()            % cleanup delay ms
}).

-type work_ctx()    ::  #wc{}.
-type work_delay()  ::  non_neg_integer().      % ms delay
-type work_id()     ::  riak_core_job:cid().    % defaults are pos_integer()

%% ===================================================================
%% Utility API
%% ===================================================================

-spec set_config(Props :: [{atom(), term()}]) -> ok.
%
% Sets the specified configuration values and clears any relevant ones that
% aren't specified in Props.
% If Props is [], the effect is that all properties take their default values
% when the services are started.
%
set_config(Props) ->
    set_config([
        ?JOB_SVC_CONCUR_LIMIT,
        ?JOB_SVC_QUEUE_LIMIT,
        ?JOB_SVC_HIST_LIMIT,
        ?JOB_SVC_IDLE_MIN,
        ?JOB_SVC_IDLE_MAX
    ], riak_core_job_service:default_app(), Props).

-spec set_config(Keys :: [atom()], App :: atom(), Props :: [{atom(), term()}])
        -> ok.
%
% Sets the specified configuration values and clears any relevant ones that
% aren't specified in Props.
%
set_config([Key | Keys], App, Props) ->
    case proplists:get_value(Key, Props) of
        undefined ->
            application:unset_env(App, Key);
        Val ->
            application:set_env(App, Key, Val)
    end,
    set_config(Keys, App, Props);
set_config([], _, _) ->
    ok.

-spec create_jobs(Count :: pos_integer())
        -> [riak_core_job:job()].
%
% Creates Count jobs with IDs 1..Count and default operation delays.
%
create_jobs(Count) ->
    [create_job(Id) || Id <- lists:seq(1, Count)].

-spec create_jobs(
    Count       :: pos_integer(),
    SetupDelay  :: work_delay(),
    MainDelay   :: work_delay(),
    CleanupDelay :: work_delay())
        -> [riak_core_job:job()].
%
% Creates Count jobs with IDs 1..Count and the specified operation delays.
%
create_jobs(Count, SetupDelay, MainDelay, CleanupDelay) ->
    [create_job(Id, SetupDelay, MainDelay, CleanupDelay)
        || Id <- lists:seq(1, Count)].

-spec create_job(Id :: work_id())
        -> riak_core_job:job().
%
% Creates a Job with the specified Id and default operation delays.
%
create_job(Id) ->
    create_job(Id, 0, 7, 0).

-spec create_job(
    Id          :: work_id(),
    SetupDelay  :: work_delay(),
    MainDelay   :: work_delay(),
    CleanupDelay :: work_delay())
        -> riak_core_job:job().
%
% Creates a Job with the specified Id and operation delays.
%
create_job(Id, SetupDelay, MainDelay, CleanupDelay) ->
    Ctx = #wc{
        id  = Id,
        own = erlang:self(),
        mgr = ?JOBS_MGR_NAME,
        sd  = SetupDelay,
        md  = MainDelay,
        cd  = CleanupDelay
    },
    riak_core_job:job([
        {'module',  ?MODULE},
        {'class',   {?MODULE, 'test'}},
        {'cid',     Id},
        {'killed',  {?MODULE, job_killed,   [Ctx]}},
        {'work',    riak_core_job:work([
            {'setup',   {?MODULE, work_setup,   [Ctx]}},
            {'main',    {?MODULE, work_main,    []}},
            {'cleanup', {?MODULE, work_cleanup, []}}
        ])}
    ]).

-spec wait_for_job(Job :: riak_core_job:job()) -> term().
%
% Wait for the specified job to send a 'cleanup' or 'killed' message and
% return it.
%
wait_for_job(Job) ->
    JId = riak_core_job:cid(Job),
    receive
        {?MODULE, 'work_cleanup', JId} = Msg->
            Msg;
        {?MODULE, 'job_killed', JId, _Why} = Msg->
            Msg
    after
        1500 ->
            'timeout'
    end.

-spec wait_for_jobs(Jobs :: [riak_core_job:job()]) -> ok.
%
% Wait for each of the specified jobs to send a 'cleanup' or 'killed' message.
%
wait_for_jobs([Job | Jobs]) ->
    ?assertNotEqual('timeout', wait_for_job(Job)),
    wait_for_jobs(Jobs);
wait_for_jobs([]) ->
    ok.

%% ===================================================================
%% Job Exports
%% ===================================================================

-spec job_killed(Why :: term(), Ctx :: work_ctx()) -> ok.
%
% Job 'killed' callback.
%
job_killed(Why, #wc{id = Id, own = Own}) ->
    Own ! {?MODULE, 'job_killed', Id, Why},
    ok.

-spec work_cleanup(Ctx :: work_ctx()) -> work_id().
%
% Work 'cleanup' callback.
%
work_cleanup(#wc{id = Id, own = Own, sd = 0}) ->
    Own ! {?MODULE, 'work_cleanup', Id},
    Id;
work_cleanup(#wc{id = Id, own = Own, sd = Delay}) ->
    Own ! {?MODULE, 'work_cleanup', Id},
    ok = timer:sleep(Delay),
    Id.

-spec work_main(Ctx :: work_ctx()) -> work_ctx().
%
% Work 'main' callback.
%
work_main(#wc{id = Id, own = Own, sd = 0} = Ctx) ->
    Own ! {?MODULE, 'work_main', Id},
    Ctx;
work_main(#wc{id = Id, own = Own, sd = Delay} = Ctx) ->
    Own ! {?MODULE, 'work_main', Id},
    ok = timer:sleep(Delay),
    Ctx.

-spec work_setup(Mgr :: atom() | pid(), Ctx :: work_ctx()) -> work_ctx().
%
% Work 'setup' callback.
%
work_setup(Mgr, #wc{id = Id, own = Own, sd = 0} = Ctx) ->
    Own ! {?MODULE, 'work_setup', Id},
    Ctx#wc{mgr = Mgr};
work_setup(Mgr, #wc{id = Id, own = Own, sd = Delay} = Ctx) ->
    Own ! {?MODULE, 'work_setup', Id},
    ok = timer:sleep(Delay),
    Ctx#wc{mgr = Mgr}.
