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
-export([start_pool/5]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, PoolType, Args, Type, Timeout),
		{PoolType,
			{I, start_link, Args},
			permanent, Timeout, Type, [I]}).
-define(CHILD(I, PoolType, Args, Type),
		?CHILD(I, PoolType, Args, Type, 5000)).

-type worker_pool() :: riak_core_node_worker_pool:worker_pool().

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 5, 10}, []}}.

%% @doc
%% Start a node_worker_pool - can be either assuredforwardng_pool or
%% a besteffort_pool (which will also be registered as a node_worker_pool for
%% backwards compatability)
-spec start_pool(atom(), pos_integer(), list(), list(), worker_pool()) ->
                        ok | {error, Reason::term()}.
start_pool(WorkerMod, PoolSize, WorkerArgs, WorkerProps, QueueType) ->
    Ref = pool(WorkerMod, PoolSize, WorkerArgs, WorkerProps, QueueType),
    case supervisor:start_child(?MODULE, Ref) of
        {ok, _} -> ok;
        {ok, _, _} -> ok;
        {error, already_present} -> ok;
        {error, {already_started, _}} -> ok;
        {error, OtherErr} -> {error, OtherErr}
    end.

pool(WorkerMod, PoolSize, WorkerArgs, WorkerProps, QueueType) ->
    ?CHILD(riak_core_node_worker_pool,
           QueueType,
           [WorkerMod, PoolSize, WorkerArgs, WorkerProps, QueueType],
           worker).

