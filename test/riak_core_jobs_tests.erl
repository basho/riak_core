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

-module(riak_core_jobs_tests).

-include("../src/riak_core_job_internal.hrl").

%% ===================================================================
%% Tests
%% ===================================================================

supervisor_test() ->
    ?assertEqual('ok', jobs_test_util:set_config([])),
    TestRet = riak_core_job_sup:start_test_sup(),
    ?assertMatch({'ok', P} when is_pid(P), TestRet),
    {'ok', TestSup} = TestRet,

    ?assertMatch(P when is_pid(P), erlang:whereis(?JOBS_MGR_NAME)),
    ?assertMatch(P when is_pid(P), erlang:whereis(?JOBS_SVC_NAME)),
    ?assertMatch(P when is_pid(P), erlang:whereis(?WORK_SUP_NAME)),

    % The supervisor configuration seems to linger in the VM's ETS even after
    % the process itself is killed, so we can't confirm the proper results
    % from supervisor:count_children/1 as previous tests may have polluted
    % the table.

    ?assertEqual('ok', riak_core_job_sup:stop_test_sup(TestSup)),

    ?assertEqual('undefined', erlang:whereis(?JOBS_MGR_NAME)),
    ?assertEqual('undefined', erlang:whereis(?JOBS_SVC_NAME)),
    ?assertEqual('undefined', erlang:whereis(?WORK_SUP_NAME)),

    'ok'.

default_conf_test() ->
    ?assertEqual('ok', jobs_test_util:set_config([])),
    {'ok', TestSup} = riak_core_job_sup:start_test_sup(),

    Conf = riak_core_job_manager:config(),
    Scheds = erlang:system_info('schedulers'),

    RMax = (Scheds * 6),
    ?assertEqual(RMax, proplists:get_value(?JOB_SVC_CONCUR_LIMIT, Conf)),
    QMax = (RMax * 3),
    ?assertEqual(QMax, proplists:get_value(?JOB_SVC_QUEUE_LIMIT, Conf)),
    HMax = RMax,
    ?assertEqual(HMax, proplists:get_value(?JOB_SVC_HIST_LIMIT, Conf)),
    IMin = erlang:max((RMax div 8), 3),
    ?assertEqual(IMin, proplists:get_value(?JOB_SVC_IDLE_MIN, Conf)),
    IMax = erlang:max((IMin * 2), (Scheds - 1)),
    ?assertEqual(IMax, proplists:get_value(?JOB_SVC_IDLE_MAX, Conf)),
    ?assertEqual(false, proplists:get_value(?JOB_SVC_RECYCLE, Conf)),

    riak_core_job_sup:stop_test_sup(TestSup).

conf_test() ->
    Props = [
        {?JOB_SVC_CONCUR_LIMIT, {'scheds', 2}},
        {?JOB_SVC_QUEUE_LIMIT,  {'concur', 4}},
        {?JOB_SVC_HIST_LIMIT,   {'cores', 7}}
    ],
    ?assertEqual('ok', jobs_test_util:set_config(Props)),
    {'ok', TestSup} = riak_core_job_sup:start_test_sup(),

    Conf = riak_core_job_manager:config(),
    Scheds = erlang:system_info('schedulers'),
    Cores = case erlang:system_info('logical_processors') of
        LCPUs when erlang:is_integer(LCPUs) andalso LCPUs > 0 ->
            LCPUs;
        _ ->
            Scheds
    end,

    RMax = (Scheds * 2),
    ?assertEqual(RMax, proplists:get_value(?JOB_SVC_CONCUR_LIMIT, Conf)),
    QMax = (RMax * 4),
    ?assertEqual(QMax, proplists:get_value(?JOB_SVC_QUEUE_LIMIT, Conf)),
    HMax = (Cores * 7),
    ?assertEqual(HMax, proplists:get_value(?JOB_SVC_HIST_LIMIT, Conf)),
    IMin = erlang:max((RMax div 8), 3),
    ?assertEqual(IMin, proplists:get_value(?JOB_SVC_IDLE_MIN, Conf)),
    IMax = erlang:max((IMin * 2), (Scheds - 1)),
    ?assertEqual(IMax, proplists:get_value(?JOB_SVC_IDLE_MAX, Conf)),

    riak_core_job_sup:stop_test_sup(TestSup).

reconf_test() ->
    ?assertMatch({ok, P} when erlang:is_pid(P), riak_core_job_sup:start_test_sup()),

    InitConf = riak_core_job_manager:config(),
    InitRMax = proplists:get_value(?JOB_SVC_CONCUR_LIMIT, InitConf),
    InitQMax = proplists:get_value(?JOB_SVC_QUEUE_LIMIT,  InitConf),
    InitHMax = proplists:get_value(?JOB_SVC_HIST_LIMIT,   InitConf),
    InitIMin = proplists:get_value(?JOB_SVC_IDLE_MIN,     InitConf),
    InitIMax = proplists:get_value(?JOB_SVC_IDLE_MAX,     InitConf),

    TestRMax = (InitRMax + 1),
    TestQMax = (InitQMax + 1),
    TestHMax = (InitHMax + 1),
    TestIMin = (InitIMin + 1),
    TestIMax = (InitIMax + 1),

    ?assertEqual(ok, jobs_test_util:set_config([
        {?JOB_SVC_CONCUR_LIMIT, TestRMax},
        {?JOB_SVC_QUEUE_LIMIT,  TestQMax},
        {?JOB_SVC_HIST_LIMIT,   TestHMax},
        {?JOB_SVC_IDLE_MIN,     TestIMin},
        {?JOB_SVC_IDLE_MAX,     TestIMax}
    ])),
    % make sure setting the configuration didn't make it live ...
    ?assertEqual(lists:sort(InitConf), lists:sort(riak_core_job_manager:config())),

    % ... and that making it live does
    riak_core_job_manager:reconfigure(),
    TestConf = riak_core_job_manager:config(),

    ?assertEqual(TestRMax, proplists:get_value(?JOB_SVC_CONCUR_LIMIT, TestConf)),
    ?assertEqual(TestQMax, proplists:get_value(?JOB_SVC_QUEUE_LIMIT,  TestConf)),
    ?assertEqual(TestHMax, proplists:get_value(?JOB_SVC_HIST_LIMIT,   TestConf)),
    ?assertEqual(TestIMin, proplists:get_value(?JOB_SVC_IDLE_MIN,     TestConf)),
    ?assertEqual(TestIMax, proplists:get_value(?JOB_SVC_IDLE_MAX,     TestConf)),

    riak_core_job_sup:stop_test_sup().

submit_test() ->
    Props = [
        {?JOB_SVC_CONCUR_LIMIT, 3},
        {?JOB_SVC_QUEUE_LIMIT,  9}
    ],
    ?assertEqual('ok', jobs_test_util:set_config(Props)),
    {'ok', TestSup} = riak_core_job_sup:start_test_sup(),

    Jobs = jobs_test_util:create_jobs(11),
    [?assertEqual('ok', riak_core_job_manager:submit(J)) || J <- Jobs],

    jobs_test_util:wait_for_jobs(Jobs),

    riak_core_job_sup:stop_test_sup(TestSup).

queue_reject_test() ->
    Props = [
        {?JOB_SVC_CONCUR_LIMIT, 2},
        {?JOB_SVC_QUEUE_LIMIT,  3}
    ],
    ?assertEqual('ok', jobs_test_util:set_config(Props)),
    {'ok', TestSup} = riak_core_job_sup:start_test_sup(),

    AllJobs = jobs_test_util:create_jobs(11, 7, 7, 7),
    % the first 5 should be accepted, the rest rejected
    {AccJobs, RejJobs} =  lists:split(5, AllJobs),
    AccRets = [riak_core_job_manager:submit(J) || J <- AccJobs],
    RejRets = [riak_core_job_manager:submit(J) || J <- RejJobs],

    [?assertEqual('ok', R) || R <- AccRets],
    [?assertEqual({'error', 'job_queue_full'}, R) || R <- RejRets],

    jobs_test_util:wait_for_jobs(AccJobs),

    riak_core_job_sup:stop_test_sup(TestSup).

class_reject_test() ->
    ?assertEqual('ok', jobs_test_util:set_config([])),
    {'ok', TestSup} = riak_core_job_sup:start_test_sup(),

    [Job1, Job2] = jobs_test_util:create_jobs(2),

    % The job enable/disable system has a quirk - if the list is undefined and
    % you disable a class it doesn't reset it to an empty list (as it probably
    % should), leaving everything enabled, so make sure it's got at least one
    % unrelated class in it.
    FillClass = riak_core_job:class(riak_core_job:dummy()),
    JobsClass = riak_core_job:class(Job1),
    riak_core_util:enable_job_class(FillClass),

    riak_core_util:enable_job_class(JobsClass),
    ?assertEqual('ok', riak_core_job_manager:submit(Job1)),

    riak_core_util:disable_job_class(JobsClass),
    ?assertEqual({'error', 'job_rejected'}, riak_core_job_manager:submit(Job2)),

    % Clear out the list so we don't interfere with other tests.
    application:unset_env('riak_core', 'job_accept_class'),

    jobs_test_util:wait_for_job(Job1),

    riak_core_job_sup:stop_test_sup(TestSup).

history_test() ->
    Props = [
        {?JOB_SVC_CONCUR_LIMIT, 1},
        {?JOB_SVC_QUEUE_LIMIT,  1},
        {?JOB_SVC_HIST_LIMIT,   3}
    ],
    ?assertEqual('ok', jobs_test_util:set_config(Props)),
    {'ok', TestSup} = riak_core_job_sup:start_test_sup(),

    [Job1, Job2, Job3] = Jobs = jobs_test_util:create_jobs(3, 7, 7, 7),

    ?assertEqual(
        ['ok', 'ok', {'error', 'job_queue_full'}],
        [riak_core_job_manager:submit(J) || J <- Jobs]),

    jobs_test_util:wait_for_jobs([Job1, Job2]),

    ?assertMatch([_|_], riak_core_job_manager:stats()),
    ?assertMatch([_|_], riak_core_job_manager:stats(Job1)),
    ?assertMatch([_|_], riak_core_job_manager:stats(Job2)),
    ?assertEqual(false, riak_core_job_manager:stats(Job3)),

    riak_core_job_sup:stop_test_sup(TestSup).

