%% -------------------------------------------------------------------
%%
%% Eunit test cases for the Service Manager
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_core_service_mgr_tests).
-author("Chris Tilt").
-include_lib("eunit/include/eunit.hrl").

%%-define(TRACE(Stmt),Stmt).
-define(TRACE(Stmt),ok).

%% internal functions
-export([testService/5,
         connected/6, connect_failed/3
        ]).

%% my name and remote same because I need to talk to myself for testing
-define(MY_CLUSTER_NAME, "bob").
-define(REMOTE_CLUSTER_NAME, "bob").

-define(REMOTE_CLUSTER_ADDR, {"127.0.0.1", 4096}).
-define(TEST_ADDR, {"127.0.0.1", 4097}).
-define(MAX_CONS, 2).
-define(TCP_OPTIONS, [{keepalive, true},
                      {nodelay, true},
                      {packet, 4},
                      {reuseaddr, true},
                      {active, false}]).

service_test_() ->
    {timeout, 60000, {setup, fun() ->
        ok = application:start(ranch),
        {ok, _Pid} = riak_core_service_mgr:start_link(?TEST_ADDR)
    end,
    fun(_) ->
        case whereis(riak_core_service_manager) of
            Pid when is_pid(Pid) ->
                riak_core_service_mgr:stop(),
                {ok, _Mon} = erlang:monitor(process, Pid),
                receive
                    {'DOWN', _, _, _, _} ->
                        ok
                after
                    1000 ->
                        ok
                end;
            undefined ->
                ok
        end
    end,
    fun(_) -> [

        {"started", ?_assert(is_pid(whereis(riak_core_service_manager)))},

        {"get services", ?_assertEqual([], gen_server:call(riak_core_service_manager, get_services))},

        {"register service", fun() ->
            ExpectedRevs = [{1,0}, {1,0}],
            TestProtocol = {{testproto, [{1,0}]}, {?TCP_OPTIONS, ?MODULE, testService, ExpectedRevs}},
            riak_core_service_mgr:register_service(TestProtocol, {round_robin,?MAX_CONS}),
            ?assert(riak_core_service_mgr:is_registered(testproto))
        end},

        {"unregister service", fun() ->
            TestProtocolId = testproto,
            riak_core_service_mgr:unregister_service(TestProtocolId),
            ?assertNot(riak_core_service_mgr:is_registered(testproto))
        end},

        {"register stats fun", fun() ->
            Self = self(),
            Fun = fun(Stats) ->
                Self ! Stats
            end,
            riak_core_service_mgr:register_stats_fun(Fun),
            GotStats = receive
                Term ->
                    Term
            after 5500 ->
                timeout
            end,
            ?assertEqual([], GotStats)
        end},

        {"start service test", fun() ->
            %% re-register the test protocol and confirm registered
            TestProtocol = {{testproto, [{1,0}]}, {?TCP_OPTIONS, ?MODULE, testService, [{1,0}, {1,0}]}},
            riak_core_service_mgr:register_service(TestProtocol, {round_robin, ?MAX_CONS}),
            ?assert(riak_core_service_mgr:is_registered(testproto)),
            %register_service_test_d(),
            %% try to connect via a client that speaks our test protocol
            ExpectedRevs = {expectedToPass, [{1,0}, {1,0}]},
            riak_core_connection:connect(?TEST_ADDR, {{testproto, [{1,0}]},
                                                      {?TCP_OPTIONS, ?MODULE, ExpectedRevs}}),
            %% allow client and server to connect and make assertions of success/failure
            timer:sleep(1000),
            Stats = riak_core_service_mgr:get_stats(),
            ?assertEqual([{testproto,0}], Stats)
        end},

        {"pause existing services", fun() ->
            riak_core_service_mgr:stop(),
            %% there should be no services running now.
            %% now start a client and confirm failure to connect
            ExpectedArgs = expectedToFail,
            riak_core_connection:connect(?TEST_ADDR, {{testproto, [{1,0}]},
                                                      {?TCP_OPTIONS, ?MODULE, ExpectedArgs}}),
            %% allow client and server to connect and make assertions of success/failure
            timer:sleep(1000)
        end}

    ] end} }.

%%------------------------
%% Helper functions
%%------------------------

%% Protocol Service functions
testService(_Socket, _Transport, {error, _Reason}, _Args, _Props) ->
    ?assert(false);
testService(_Socket, _Transport, {ok, {Proto, MyVer, RemoteVer}}, Args, _Props) ->
    ?TRACE(?debugMsg("testService started")),
    [ExpectedMyVer, ExpectedRemoteVer] = Args,
    ?assertEqual(ExpectedMyVer, MyVer),
    ?assertEqual(ExpectedRemoteVer, RemoteVer),
    ?assertEqual(Proto, testproto),
%%    timer:sleep(2000),
    {ok, self()}.

%% Client side protocol callbacks
connected(_Socket, _Transport, {_IP, _Port}, {Proto, MyVer, RemoteVer}, Args, _Props) ->
    ?TRACE(?debugMsg("testClient started")),
    {_TestType, [ExpectedMyVer, ExpectedRemoteVer]} = Args,
    ?assertEqual(Proto, testproto),
    ?assertEqual(ExpectedMyVer, MyVer),
    ?assertEqual(ExpectedRemoteVer, RemoteVer),
    timer:sleep(2000).

connect_failed({_Proto,_Vers}, {error, Reason}, Args) ->
    case Args of
        expectedToFail ->
            ?assertEqual(Reason, econnrefused);
        {retry_test, _Stuff} ->
            ?TRACE(?debugFmt("connect_failed: during retry test: ~p", [Reason])),
            ok;
        _Other ->
            ?TRACE(?debugFmt("connect_failed: ~p with args = ~p", [Reason, _Other])),
            ?assert(false)
    end,
    timer:sleep(1000).
