%% -------------------------------------------------------------------
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc starts and supervises a process per stat
%% register stats through riak_core:register/2

-module(riak_core_metric_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).
-export([start_stat/3, stop_stat/2, start_stats/2]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% Populate supervisor list with stats for already registered app,stat
    %% modules. Ensures restart of stat procs after a crash of this supervisor.
    Refs0 = [mod_refs(App, Mod) || {App, Mod} <- riak_core:stat_specs()],
    Refs = lists:flatten(Refs0),
    {ok, {{one_for_one, 5, 10}, Refs}}.

start_stat(App, Stat, Args) ->
    Ref = stat_ref(App, Stat, Args),
    Pid = case supervisor:start_child(?MODULE, Ref) of
              {ok, Child} -> Child;
              {error, {already_started, Child}} -> Child
          end,
    Pid.

stop_stat(App, Stat) ->
    supervisor:terminate_child(?MODULE, {App, Stat}),
    supervisor:delete_child(?MODULE, {App, Stat}),
    ok.

start_stats(App, Mod) ->
    Stats = Mod:stat_specs(),
    [start_stat(App, Stat, Args) || {Stat, Args} <- Stats],
    ok.

%% @private
mod_refs(App, Mod) ->
    Stats = Mod:stat_specs(),
    [stat_ref(App, Stat, Args) || {Stat, Args} <- Stats].

stat_ref(App, Stat, Args) ->
    {{App, Stat}, {riak_core_metric_proc, start_link, [App, Stat, Args]},
     permanent, 5000, worker, [riak_core_metric_proc]}.

