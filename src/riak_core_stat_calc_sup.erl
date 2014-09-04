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
-module(riak_core_stat_calc_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).
-export([calc_proc/1, stop_proc/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Timeout), {I, {riak_core_stat_calc_proc, start_link, [I]}, permanent, Timeout, Type, [riak_core_stat_calc_proc]}).
-define(CHILD(I, Type), ?CHILD(I, Type, 5000)).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 5, 10}, []}}.

%% start a process for the given stat
%% {@see riak_core_stat_calc_proc}
calc_proc(Stat) ->
    Ref = calc_proc_ref(Stat),
    Pid = case supervisor:start_child(?MODULE, Ref) of
              {ok, Child} -> Child;
              {error, {already_started, Child}} -> Child
          end,
    Pid.

stop_proc(Stat) ->
    _ = supervisor:terminate_child(?MODULE, Stat),
    _ = supervisor:delete_child(?MODULE, Stat),
    ok.

%% @private
calc_proc_ref(Stat) ->
    ?CHILD(Stat, worker).
