%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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
-module(sync_command_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_core_vnode.hrl").

sync_test_() ->
    {foreach,
     fun setup_simple/0,
     fun stop_servers/1,
     [ {<<"Assert ok throw">>,
        fun() ->
            ?assertEqual(ok, mock_vnode:sync_error({0, node()}, goodthrow))
        end},
       {<<"Assert error throw">>,
        fun() ->
            ?assertEqual({error, terrible}, mock_vnode:sync_error({0, node()}, badthrow))
        end},
       {<<"Assert sync error">>,
        fun() ->
            ?assertError(core_breach, mock_vnode:sync_error({0, node()}, error))
        end},
       {<<"Assert sync exit">>,
        fun() ->
            ?assertError(core_breach, mock_vnode:sync_error({0, node()}, exit))
        end},
       {<<"Assert non-blocking sync error">>,
        fun() ->
            ?assertError(core_breach, mock_vnode:spawn_error({0, node()}, error))
        end},
       {<<"Assert non-blocking sync exit">>,
        fun() ->
            ?assertError(core_breach, mock_vnode:spawn_error({0, node()}, exit))
        end}
       ]
    }.


setup_simple() ->
    stop_servers(self()),
    Vars = [{ring_creation_size, 8},
            {ring_state_dir, "<nostore>"},
            %% Don't allow rolling start of vnodes as it will cause a
            %% race condition with `all_nodes'.
            {core_vnode_eqc_pool_size, 0},
            {vnode_rolling_start, 0}],
    error_logger:tty(false),
    _ = [begin
        Old = app_helper:get_env(riak_core, AppKey),
        ok = application:set_env(riak_core, AppKey, Val),
        {AppKey, Old}
     end || {AppKey, Val} <- Vars],
    exometer:start(),
    riak_core_ring_events:start_link(),
    riak_core_ring_manager:start_link(test),
    riak_core_vnode_proxy_sup:start_link(),

    {ok, _Sup} = riak_core_vnode_sup:start_link(),
    {ok, _} = riak_core_vnode_manager:start_link(),
    {ok, _VMaster} = riak_core_vnode_master:start_link(mock_vnode),


    ok = mock_vnode:start_vnode(0),
    %% NOTE: The return value from this call is currently not being
    %%       used.  This call is made to ensure that all msgs sent to
    %%       the vnode mgr before this call have been handled.  This
    %%       guarantees that the vnode mgr ets tab is up-to-date

    riak_core:register([{vnode_module, mock_vnode}]).


stop_servers(_Pid) ->
    %% Make sure VMaster is killed before sup as start_vnode is a cast
    %% and there may be a pending request to start the vnode.
    riak_core_test_util:stop_pid(mock_vnode_master),
    riak_core_test_util:stop_pid(riak_core_vnode_manager),
    riak_core_test_util:stop_pid(riak_core_ring_events),
    riak_core_test_util:stop_pid(riak_core_vnode_sup),
    riak_core_test_util:stop_pid(riak_core_ring_manager),
    application:stop(exometer),
    application:stop(lager),
    application:stop(goldrush).
