%% -------------------------------------------------------------------
%%
%% riak_vnode_sup: supervise riak_vnode processes
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc supervise riak_vnode processes

-module(riak_core_vnode_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).
-export([start_vnode/2]).

start_vnode(Mod, Index) when is_integer(Index) -> 
    supervisor:start_child(?MODULE, [Mod, Index]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @private
init([]) ->
  Config = app_helper:get_env(riak_core_vnode_sup, lifecycle_options, {
    {simple_one_for_one, 10, 10}, 
    [
      {undefined, {riak_core_vnode, start_link, []}, temporary, brutal_kill, worker, dynamic}
    ]
  }),
  {ok, Config}.
