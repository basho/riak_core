%% -------------------------------------------------------------------
%%
%% Eunit test cases for the Connection Manager
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

-module(riak_core_connection_mgr_tests).
-author("Chris Tilt").
-include_lib("eunit/include/eunit.hrl").

-define(TRACE(Stmt),Stmt).
%%-define(TRACE(Stmt),ok).

%% internal functions
-export([testService/5,
         connected/6, connect_failed/3
        ]).

%% Locator selector types
-define(REMOTE_LOCATOR_TYPE, remote).
-define(ADDR_LOCATOR_TYPE, addr).

%% My cluster
-define(MY_CLUSTER_NAME, "bob").
-define(MY_CLUSTER_ADDR, {"127.0.0.1", 4097}).

%% Remote cluster
-define(REMOTE_CLUSTER_NAME, "betty").
-define(REMOTE_CLUSTER_ADDR, {"127.0.0.1", 4096}).
-define(REMOTE_ADDRS, [{"127.0.0.1",5001}, {"127.0.0.1",5002}, {"127.0.0.1",5003},
                       ?REMOTE_CLUSTER_ADDR]).

-define(MAX_CONS, 2).
-define(TCP_OPTIONS, [{keepalive, true},
                      {nodelay, true},
                      {packet, 4},
                      {reuseaddr, true},
                      {active, false}]).

%% Tell the connection manager how to find out "remote" end points.
%% For testing, we just make up some local addresses.
register_remote_locator() ->
    Remotes = orddict:from_list([{?REMOTE_CLUSTER_NAME, ?REMOTE_ADDRS}]),
    Locator = fun(Name, _Policy) ->
                      case orddict:find(Name, Remotes) of
                          false ->
                              {error, {unknown, Name}};
                          OKEndpoints ->
                              OKEndpoints
                      end
              end,
    ok = riak_core_connection_mgr:register_locator(?REMOTE_LOCATOR_TYPE, Locator).

register_addr_locator() ->
    Locator = fun(Name, _Policy) -> {ok, [Name]} end,
    ok = riak_core_connection_mgr:register_locator(?ADDR_LOCATOR_TYPE, Locator).

register_empty_locator() ->
    Locator = fun(_Name, _Policy) -> {ok, []} end,
    ok = riak_core_connection_mgr:register_locator(?REMOTE_LOCATOR_TYPE, Locator).

connections_test_() ->
    {timeout, 6000, {setup, fun() ->
        ok = application:start(ranch),
        {ok, _} = riak_core_service_mgr:start_link(?REMOTE_CLUSTER_ADDR),
        {ok, _} = riak_core_connection_mgr:start_link()
    end,
    fun(_) ->
        riak_core_connection_mgr:stop(),
        riak_core_service_mgr:stop(),
        application:stop(ranch)
    end,
    fun(_) -> [

        {"regsiter remote locator", fun() ->
            register_remote_locator(),
            Target = {?REMOTE_LOCATOR_TYPE, ?REMOTE_CLUSTER_NAME},
            Strategy = default,
            Got = riak_core_connection_mgr:apply_locator(Target, Strategy),
            ?assertEqual({ok, ?REMOTE_ADDRS}, Got)
        end},

        {"register locator addr", fun() ->
            register_addr_locator(),
            Target = {?ADDR_LOCATOR_TYPE, ?REMOTE_CLUSTER_ADDR},
            Strategy = default,
            Got = riak_core_connection_mgr:apply_locator(Target, Strategy),
            ?assertEqual({ok, [?REMOTE_CLUSTER_ADDR]}, Got)
        end},

        {"bad locator args", fun() ->
            register_addr_locator(),
            %% bad args for 'addr'
            Target = {?REMOTE_LOCATOR_TYPE, ?REMOTE_CLUSTER_ADDR},
            Strategy = default,
            Expected = {error, {bad_target_name_args, remote, ?REMOTE_CLUSTER_ADDR}},
            Got = riak_core_connection_mgr:apply_locator(Target, Strategy),
            ?assertEqual(Expected, Got)
        end},

        {"is paused", ?_assertNot(riak_core_connection_mgr:is_paused())},

        {"pause", fun() ->
            riak_core_connection_mgr:pause(),
            ?assert(riak_core_connection_mgr:is_paused())
        end},

        {"resume", fun() ->
            riak_core_connection_mgr:resume(),
            ?assertNot(riak_core_connection_mgr:is_paused())
        end},

        {"client connection", fun() ->
            %% start a test service
            start_service(),
            %% do async connect via connection_mgr
            ExpectedArgs = {expectedToPass, [{1,0}, {1,0}]},
            Target = {?ADDR_LOCATOR_TYPE, ?REMOTE_CLUSTER_ADDR},
            Strategy = default,
            riak_core_connection_mgr:connect(Target,
                                             {{testproto, [{1,0}]},
                                              {?TCP_OPTIONS, ?MODULE, ExpectedArgs}},
                                             Strategy),
            timer:sleep(1000)
        end},

        {"client connect via cluster name", fun() ->
            start_service(),
            %% do async connect via connection_mgr
            ExpectedArgs = {expectedToPass, [{1,0}, {1,0}]},
            Target = {?REMOTE_LOCATOR_TYPE, ?REMOTE_CLUSTER_NAME},
            Strategy = default,
            riak_core_connection_mgr:connect(Target,
                                             {{testproto, [{1,0}]},
                                              {?TCP_OPTIONS, ?MODULE, ExpectedArgs}},
                                             Strategy),
            timer:sleep(1000)
        end},

        {"client retries", fun() ->
            %% do async connect via connection_mgr
            ExpectedArgs = {retry_test, [{1,0}, {1,0}]},
            Target = {?REMOTE_LOCATOR_TYPE, ?REMOTE_CLUSTER_NAME},
            Strategy = default,

            riak_core_connection_mgr:connect(Target,
                                             {{testproto, [{1,0}]},
                                              {?TCP_OPTIONS, ?MODULE, ExpectedArgs}},
                                             Strategy),
            %% delay so the client will keep trying
            ?TRACE(?debugMsg(" ------ sleeping 3 sec")),
            timer:sleep(3000),
            %% resume and confirm not paused, which should cause service to start and connection :-)
            ?TRACE(?debugMsg(" ------ resuming services")),
            start_service(),
            %% allow connection to setup
            ?TRACE(?debugMsg(" ------ sleeping 2 sec")),
            timer:sleep(1000)
        end},

        {"empty locator", fun() ->
            register_empty_locator(), %% replace remote locator with one that returns empty list
            start_service(),
            %% do async connect via connection_mgr
            ExpectedArgs = {expectedToPass, [{1,0}, {1,0}]},
            Target = {?REMOTE_LOCATOR_TYPE, ?REMOTE_CLUSTER_NAME},
            Strategy = default,
            riak_core_connection_mgr:connect(Target,
                                             {{testproto, [{1,0}]},
                                              {?TCP_OPTIONS, ?MODULE, ExpectedArgs}},
                                             Strategy),
            %% allow conn manager to try and schedule a few retries
            timer:sleep(1000),

            %% restore the remote locator that gives a good endpoint
            register_remote_locator(),
            Got = riak_core_connection_mgr:apply_locator(Target, Strategy),
            ?assertEqual({ok, ?REMOTE_ADDRS}, Got),

            %% allow enough time for retry mechanism to kick in
            timer:sleep(2000)
            %% we should get a connection
        end}

    ] end} }.

%%------------------------
%% Helper functions
%%------------------------

start_service() ->
    %% start dispatcher
    ExpectedRevs = [{1,0}, {1,0}],
    TestProtocol = {{testproto, [{1,0}]}, {?TCP_OPTIONS, ?MODULE, testService, ExpectedRevs}},
    riak_core_service_mgr:register_service(TestProtocol, {round_robin,10}),
    ?assert(riak_core_service_mgr:is_registered(testproto) == true).

%% Protocol Service functions
testService(_Socket, _Transport, {error, _Reason}, _Args, _Props) ->
    ?assert(false);
testService(_Socket, _Transport, {ok, {Proto, MyVer, RemoteVer}}, Args, _Props) ->
    ?TRACE(?debugMsg("testService started")),
    [ExpectedMyVer, ExpectedRemoteVer] = Args,
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer),
    ?assert(Proto == testproto),
    timer:sleep(2000),
    {ok, self()}.

%% Client side protocol callbacks
connected(_Socket, _Transport, {_IP, _Port}, {Proto, MyVer, RemoteVer}, Args, _Props) ->
    ?TRACE(?debugFmt("testClient started, connected to ~p:~p", [_IP,_Port])),
    {_TestType, [ExpectedMyVer, ExpectedRemoteVer]} = Args,
    ?assert(Proto == testproto),
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer).

connect_failed({_Proto,_Vers}, {error, Reason}, Args) ->

    case Args of
        expectedToFail ->
            ?TRACE(?debugFmt("connect_failed: (EXPECTED) when expected to fail: ~p with ~p",
                             [Reason, Args])),
            ?assert(Reason == econnrefused);
        {retry_test, _Stuff} ->
            ?TRACE(?debugFmt("connect_failed: (EXPECTED) during retry test: ~p",
                             [Reason])),
            ok;
        Other ->
            ?TRACE(?debugFmt("connect_failed: (UNEXPECTED) ~p with args = ~p",
                             [Reason, Other])),
            ?assert(false == Other)
    end.
