%% -------------------------------------------------------------------
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
-module(riak_core_node_worker_pool_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).
-export([start_pool/4]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Args, Type, Timeout),
		{I, {I, start_link, Args}, permanent, Timeout, Type, [I]}).
-define(CHILD(I, Args, Type),
		?CHILD(I, Args, Type, 5000)).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children =
		[pool(WorkerMod, PoolSize, WArgs, WProps)
			|| {_App, {WorkerMod, PoolSize, WArgs, WProps}}
				<- riak_core:pool_mods()],
    {ok, {{one_for_one, 5, 10}, Children}}.

start_pool(WorkerMod, PoolSize, WorkerArgs, WorkerProps) ->
    Ref = pool(WorkerMod, PoolSize, WorkerArgs, WorkerProps),
    _ =  supervisor:start_child(?MODULE, Ref),
    ok.

pool(WorkerMod, PoolSize, WorkerArgs, WorkerProps) ->
	?CHILD(riak_core_node_worker_pool,
			[WorkerMod, PoolSize, WorkerArgs, WorkerProps],
			worker).
	