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

%% API
-export([start_link/0]).
-export([ensembles_enabled/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Timeout, Args), {I, {I, start_link, Args}, permanent, Timeout, Type, [I]}).
-define(CHILD(I, Type, Timeout), ?CHILD(I, Type, Timeout, [])).
-define(CHILD(I, Type), ?CHILD(I, Type, 5000)).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    DistMonEnabled = app_helper:get_env(riak_core, enable_dist_mon,
                                        true),
    {ok, Root} = application:get_env(riak_core, platform_data_dir),

    EnsembleSup = {riak_ensemble_sup,
                   {riak_ensemble_sup, start_link, [Root]},
                   permanent, 30000, supervisor, [riak_ensemble_sup]},

    Children = lists:flatten(
                 [?CHILD(riak_core_bg_manager, worker),
                  ?CHILD(riak_core_sysmon_minder, worker),
                  ?CHILD(riak_core_vnode_sup, supervisor, 305000),
                  ?CHILD(riak_core_eventhandler_sup, supervisor),
                  [?CHILD(riak_core_dist_mon, worker) || DistMonEnabled],
                  ?CHILD(riak_core_handoff_sup, supervisor),
                  ?CHILD(riak_core_ring_events, worker),
                  ?CHILD(riak_core_ring_manager, worker),
                  ?CHILD(riak_core_metadata_evt_sup, supervisor),
                  ?CHILD(riak_core_metadata_manager, worker),
                  ?CHILD(riak_core_metadata_hashtree, worker),
                  ?CHILD(riak_core_broadcast, worker),
                  ?CHILD(riak_core_vnode_proxy_sup, supervisor),
                  ?CHILD(riak_core_node_watcher_events, worker),
                  ?CHILD(riak_core_node_watcher, worker),
                  ?CHILD(riak_core_vnode_manager, worker),
                  ?CHILD(riak_core_capability, worker),
                  ?CHILD(riak_core_gossip, worker),
                  ?CHILD(riak_core_claimant, worker),
                  ?CHILD(riak_core_table_owner, worker),
                  ?CHILD(riak_core_stat_sup, supervisor),
                  [EnsembleSup || ensembles_enabled()]
                 ]),

    {ok, {{one_for_one, 10, 10}, Children}}.

ensembles_enabled() ->
    Exists = (code:which(riak_ensemble_sup) =/= non_existing),
    Enabled = app_helper:get_env(riak_core, enable_consensus, false),
    Exists and Enabled.
