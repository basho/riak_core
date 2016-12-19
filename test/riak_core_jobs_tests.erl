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

-module(riak_core_jobs_tests).

-include("../src/riak_core_job_internal.hrl").

supervisor_test() ->
    TestRet = riak_core_job_sup:start_test_sup(),
    ?assertMatch({'ok', P} when is_pid(P), TestRet),
    {'ok', TestSup} = TestRet,

    ?assertMatch(P when is_pid(P), erlang:whereis(?JOBS_MGR_NAME)),
    ?assertMatch(P when is_pid(P), erlang:whereis(?JOBS_SVC_NAME)),
    ?assertMatch(P when is_pid(P), erlang:whereis(?WORK_SUP_NAME)),

    ?assertEqual('ok', riak_core_job_sup:stop_test_sup(TestSup)),

    ?assertEqual('undefined', erlang:whereis(?JOBS_MGR_NAME)),
    ?assertEqual('undefined', erlang:whereis(?JOBS_SVC_NAME)),
    ?assertEqual('undefined', erlang:whereis(?WORK_SUP_NAME)),

    'ok'.

default_conf_test() ->
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

    riak_core_job_sup:stop_test_sup(TestSup).

conf_test() ->
    Props = [
        {?JOB_SVC_CONCUR_LIMIT, {'scheds', 2}},
        {?JOB_SVC_QUEUE_LIMIT, {'concur', 4}},
        {?JOB_SVC_HIST_LIMIT, {'cores', 7}}
    ],
    ?assertEqual('ok', conf(Props)),
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

submit_test() ->
    Props = [
        {?JOB_SVC_CONCUR_LIMIT, 3},
        {?JOB_SVC_QUEUE_LIMIT,  9}
    ],
    ?assertEqual('ok', conf(Props)),
    {'ok', TestSup} = riak_core_job_sup:start_test_sup(),

    Jobs = create_jobs(11),
    [?assertEqual('ok', riak_core_job_manager:submit(J)) || J <- Jobs],

    [begin
        JID = riak_core_job:cid(J),
        Ret = receive
            {?MODULE, 'work_cleanup', JID} ->
                JID
        after
            2000 ->
                'timeout'
        end,
        ?assertEqual(JID, Ret)
    end || J <- Jobs],

    riak_core_job_sup:stop_test_sup(TestSup).


conf(Props) ->
    App = riak_core_job_service:default_app(),
    [application:set_env(App, K, V) || {K, V} <- Props],
    'ok'.

create_jobs(Count) ->
    [create_job(ID) || ID <- lists:seq(1, Count)].

create_job(ID) ->
    riak_core_job:job([
        {'module',  ?MODULE},
        {'class',   {?MODULE, 'test'}},
        {'cid',     ID},
        {'work',    riak_core_job:work([
            {'setup',   {fun work_setup/3,    [erlang:self(), ID]}},
            {'main',    {fun work_main/1,     []}},
            {'cleanup', {fun work_cleanup/1,  []}}
        ])}
    ]).

work_setup(_Manager, Caller, ID) ->
    Caller ! {?MODULE, 'work_setup', ID},
    {Caller, ID}.

work_main({Caller, ID} = Context) ->
    Caller ! {?MODULE, 'work_main', ID},
    ok = timer:sleep(7),
    Context.

work_cleanup({Caller, ID}) ->
    Caller ! {?MODULE, 'work_cleanup', ID},
    ID.

