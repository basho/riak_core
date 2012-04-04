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
-module(riak_core_vnode_proxy_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).
-export([start_proxy/2, stop_proxy/2, start_proxies/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% Populate supervisor list with proxies for already registered vnode
    %% modules. Ensures restart of proxies after a crash of this supervisor.
    Indices = get_indices(),
    VMods = riak_core:vnode_modules(),
    Proxies = [proxy_ref(Mod, Index) || {_, Mod} <- VMods,
                                        Index <- Indices],
    {ok, {{one_for_one, 5, 10}, Proxies}}.

start_proxy(Mod, Index) ->
    Ref = proxy_ref(Mod, Index),
    Pid = case supervisor:start_child(?MODULE, Ref) of
              {ok, Child} -> Child;
              {error, {already_started, Child}} -> Child
          end,
    Pid.

stop_proxy(Mod, Index) ->
    supervisor:terminate_child(?MODULE, {Mod, Index}),
    supervisor:delete_child(?MODULE, {Mod, Index}),
    ok.

start_proxies(Mod) ->
    lager:debug("Starting vnode proxies for: ~p", [Mod]),
    Indices = get_indices(),
    [start_proxy(Mod, Index) || Index <- Indices],
    ok.

%% @private
proxy_ref(Mod, Index) ->
    {{Mod, Index}, {riak_core_vnode_proxy, start_link, [Mod, Index]},
     permanent, 5000, worker, [riak_core_vnode_proxy]}.

%% @private
get_indices() ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    AllOwners = riak_core_ring:all_owners(Ring),
    [Idx || {Idx, _} <- AllOwners].
