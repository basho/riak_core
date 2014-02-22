%% -------------------------------------------------------------------
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
-module(riak_core_stats_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).
-export([start_server/1, stop_server/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Timeout), {I, {I, start_link, []}, permanent, Timeout, Type, [I]}).
-define(CHILD(I, Type), ?CHILD(I, Type, 5000)).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children = [stat_server(Mod) || {_App, Mod} <- riak_core:stat_mods()],
    {ok, {{one_for_one, 5, 10}, [?CHILD(riak_core_stat_cache, worker)|Children]}}.

start_server(Mod) ->
    Ref = stat_server(Mod),
    Pid = case supervisor:start_child(?MODULE, Ref) of
              {ok, Child} -> Child;
              {error, {already_started, Child}} -> Child
          end,
    Pid.

stop_server(Mod) ->
    _ = supervisor:terminate_child(?MODULE, Mod),
    _ = supervisor:delete_child(?MODULE, Mod),
    ok.

%% @private
stat_server(Mod) ->
    ?CHILD(Mod, worker).
