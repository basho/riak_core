%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_core_sup).

-behaviour(supervisor).

-include("riak_core_bg_manager.hrl").
-include("riak_core_token_manager.hrl").

%% API
-export([start_link/0, stop_webs/0, restart_webs/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(DEFAULT_TIMEOUT, 5000).
-define(CHILD(I, Type, Timeout, Args), {I, {I, start_link, Args}, permanent, Timeout, Type, [I]}).
-define(CHILD(I, Type, Timeout), ?CHILD(I, Type, Timeout, [])).
-define(CHILD(I, Type), ?CHILD(I, Type, ?DEFAULT_TIMEOUT)).

%% ETS tables to be created and maintained by riak_core_table_manager, which is not linked
%% to any processes except this supervisor. Please keep it that way so tables don't get lost
%% when their user processes crash. Implement ETS-TRANSFER handler for user processes.
-define(TBL_MGR_ARGS, [{?TM_ETS_TABLE, ?TM_ETS_OPTS},
                       {?LM_ETS_TABLE, ?LM_ETS_OPTS}
                      ]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop_webs() ->
    Specs = riak_web_childspecs(),
    [supervisor:terminate_child(?MODULE, Id) || {Id, _, _, _, _, _} <- Specs].

restart_webs() ->
    Specs = riak_web_childspecs(),
    [supervisor:restart_child(?MODULE, Id) || {Id, _, _, _, _, _} <- Specs].

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Children = lists:flatten(
                 [?CHILD(riak_core_sysmon_minder, worker),
                  ?CHILD(riak_core_table_manager, worker, ?DEFAULT_TIMEOUT, [?TBL_MGR_ARGS]),
                  ?CHILD(riak_core_bg_manager_sup, supervisor),
                  ?CHILD(riak_core_vnode_sup, supervisor, 305000),
                  ?CHILD(riak_core_eventhandler_sup, supervisor),
                  ?CHILD(riak_core_ring_events, worker),
                  ?CHILD(riak_core_ring_manager, worker),
                  ?CHILD(riak_core_vnode_proxy_sup, supervisor),
                  ?CHILD(riak_core_node_watcher_events, worker),
                  ?CHILD(riak_core_node_watcher, worker),
                  ?CHILD(riak_core_vnode_manager, worker),
                  ?CHILD(riak_core_capability, worker),
                  ?CHILD(riak_core_handoff_sup, supervisor),
                  ?CHILD(riak_core_gossip, worker),
                  ?CHILD(riak_core_claimant, worker),
                  ?CHILD(riak_core_stat_sup, supervisor),
                  riak_web_childspecs()
                 ]),

    {ok, {{one_for_one, 10, 10}, Children}}.

riak_web_childspecs() ->
    case lists:flatten(riak_core_web:bindings(http),
                       riak_core_web:bindings(https)) of
        [] ->
            %% check for old settings, in case app.config
            %% was not updated
            riak_core_web:old_binding();
        Binding ->
            Binding
    end.
