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

-module(riak_core_node_worker_pool).

-behaviour(riak_core_worker_pool).

-export([do_init/1, reply/2, do_work/3]).

%% API
-export([start_link/5, stop/2, shutdown_pool/2, handle_work/3]).

-type worker_pool()
	% Allows you to set up a DSCP-style set of pools (assuming the
	% vnode_wroker_pool counts as ef.  Otherwise can just have a single
	% node_worker_pool
	:: be_pool|af1_pool|af2_pool|af3_pool|af4_pool|node_worker_pool.

-export_type([worker_pool/0]).

-spec start_link(atom(), pos_integer(), list(), list(), worker_pool())
																-> {ok, pid()}.
%% @doc
%% Start a worker pool, and register under the name PoolType, which should be
%% a recognised name from type worker_pool()
start_link(WorkerMod, PoolSize, WorkerArgs, WorkerProps, PoolType) ->
	{ok, Pid} =
		riak_core_worker_pool:start_link([WorkerMod,
												PoolSize,
												WorkerArgs,
												WorkerProps],
											?MODULE),
	register(PoolType, Pid),
	lager:info("Registered worker pool of type ~w and size ~w", 
				[PoolType, PoolSize]),
	{ok, Pid}.

do_init([WorkerMod, PoolSize, WorkerArgs, WorkerProps]) ->
	process_flag(trap_exit, true),
    poolboy:start_link([{worker_module, riak_core_vnode_worker},
						{worker_args,
								[node, WorkerArgs, WorkerProps, self()]},
						{worker_callback_mod, WorkerMod},
						{size, PoolSize}, {max_overflow, 0}]).

handle_work(Pid, Work, From) ->
	riak_core_worker_pool:handle_work(Pid, Work, From).

stop(Pid, Reason) ->
    riak_core_worker_pool:stop(Pid, Reason).

%% wait for all the workers to finish any current work
shutdown_pool(Pid, Wait) ->
	riak_core_worker_pool:shutdown_pool(Pid, Wait).

reply(From, Msg) ->
	riak_core_vnode:reply(From, Msg).

do_work(Pid, Work, From) ->
	riak_core_vnode_worker:handle_work(Pid, Work, From).


