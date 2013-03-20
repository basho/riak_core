%% -------------------------------------------------------------------
%%
%% Eunit test cases for the Conn Manager Connection
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


-module(riak_core_connection_tests).
-author("Chris Tilt").
-include_lib("eunit/include/eunit.hrl").

-export([test1service/5, connected/6, connect_failed/3]).

-define(TEST_ADDR, { "127.0.0.1", 4097}).
-define(MAX_CONS, 2).
-define(TCP_OPTIONS, [{keepalive, true},
                      {nodelay, true},
                      {packet, 4},
                      {reuseaddr, true},
                      {active, false}]).

%% host service functions
test1service(_Socket, _Transport, {error, Reason}, Args, _Props) ->
    ?debugFmt("test1service failed with {error, ~p}", [Reason]),
    ?assert(Args == failed_host_args),
    ?assert(Reason == protocol_version_not_supported),
    {error, Reason};
test1service(_Socket, _Transport, {ok, {Proto, MyVer, RemoteVer}}, Args, Props) ->
    [ExpectedMyVer, ExpectedRemoteVer] = Args,
    RemoteClusterName = proplists:get_value(clustername, Props),
    ?debugFmt("test1service started with Args ~p Props ~p", [Args, Props]),
    ?assert(RemoteClusterName == "undefined"),
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer),
    ?assert(Proto == test1proto),
    timer:sleep(2000),
    {ok, self()}.

%% client connection callbacks
connected(_Socket, _Transport, {_IP, _Port}, {Proto, MyVer, RemoteVer}, Args, Props) ->
    [ExpectedMyVer, ExpectedRemoteVer] = Args,
    RemoteClusterName = proplists:get_value(clustername, Props),
    ?debugFmt("connected with Args ~p Props ~p", [Args, Props]),
    ?assert(RemoteClusterName == "undefined"),
    ?assert(Proto == test1proto),
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer),
    timer:sleep(2000).

connect_failed({Proto,_Vers}, {error, Reason}, Args) ->
    ?debugFmt("connect_failed: Reason = ~p Args = ~p", [Reason, Args]),
    ?assert(Args == failed_client_args),
    ?assert(Reason == protocol_version_not_supported),
    ?assert(Proto == test1protoFailed).

conection_test_() ->
    {timeout, 60000, {setup, fun() ->
        ok = application:start(ranch),
        {ok, _} = riak_core_service_mgr:start_link(?TEST_ADDR)
    end,
    fun(_) ->
        riak_core_service_mgr:stop(),
        application:stop(ranch)
    end,
    fun(_) -> [

        {"started", ?_assert(is_pid(whereis(riak_core_service_manager)))},

        {"set and get name", fun() ->
            riak_core_connection:set_symbolic_clustername("undefined"),
            Got = riak_core_connection:symbolic_clustername(),
            ?assertEqual("undefined", Got)
        end},

        {"register service", fun() ->
            ExpectedRevs = [{1,0}, {1,1}],
            ServiceProto = {test1proto, [{2,1}, {1,0}]},
            ServiceSpec = {ServiceProto, {?TCP_OPTIONS, ?MODULE, test1service, ExpectedRevs}},
            riak_core_service_mgr:register_service(ServiceSpec, {round_robin,?MAX_CONS}),
            ?assert(riak_core_service_mgr:is_registered(test1proto))
        end},

        {"unregister service", fun() ->
            TestProtocolId = test1proto,
            riak_core_service_mgr:unregister_service(TestProtocolId),
            ?assertNot(riak_core_service_mgr:is_registered(TestProtocolId))
        end},

        {"protocol match", fun() ->
            ExpectedRevs = [{1,0}, {1,1}],
            ServiceProto = {test1proto, [{2,1}, {1,0}]},
            ServiceSpec = {ServiceProto, {?TCP_OPTIONS, ?MODULE, test1service, ExpectedRevs}},
            riak_core_service_mgr:register_service(ServiceSpec, {round_robin,?MAX_CONS}),

            % test protocal match
            ClientProtocol = {test1proto, [{0,1},{1,1}]},
            ClientSpec = {ClientProtocol, {?TCP_OPTIONS, ?MODULE, [{1,1},{1,0}]}},
            riak_core_connection:connect(?TEST_ADDR, ClientSpec),
            timer:sleep(1000)
        end},

        {"failed protocal match", fun() ->
            %% start service
            SubProtocol = {{test1protoFailed, [{2,1}, {1,0}]},
                           {?TCP_OPTIONS, ?MODULE, test1service, failed_host_args}},
            riak_core_service_mgr:register_service(SubProtocol, {round_robin,?MAX_CONS}),
            ?assert(riak_core_service_mgr:is_registered(test1protoFailed) == true),

            %% try to connect via a client that speaks 0.1 and 3.1. No Match with host!
            ClientProtocol = {test1protoFailed, [{0,1},{3,1}]},
            ClientSpec = {ClientProtocol, {?TCP_OPTIONS, ?MODULE, failed_client_args}},
            riak_core_connection:connect(?TEST_ADDR, ClientSpec),

            timer:sleep(2000)
        end}

    ] end } }.
