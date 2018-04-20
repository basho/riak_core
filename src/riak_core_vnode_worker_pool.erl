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

%% @doc This is a wrapper around a poolboy pool, that implements a
%% queue. That is, this process maintains a queue of work, and checks
%% workers out of a poolboy pool to consume it.
%%
%% The workers it uses send two messages to this process.
%%
%% The first message is at startup, the bare atom
%% `worker_started'. This is a clue to this process that a worker was
%% just added to the poolboy pool, so a checkout request has a chance
%% of succeeding. This is most useful after an old worker has exited -
%% `worker_started' is a trigger guaranteed to arrive at a time that
%% will mean an immediate poolboy:checkout will not beat the worker
%% into the pool.
%%
%% The second message is when the worker finishes work it has been
%% handed, the two-tuple, `{checkin, WorkerPid}'. This message gives
%% this process the choice of whether to give the worker more work or
%% check it back into poolboy's pool at this point. Note: the worker
%% should *never* call poolboy:checkin itself, because that will
%% confuse (or cause a race) with this module's checkout management.
-module(riak_core_vnode_worker_pool).

-behaviour(riak_core_worker_pool).

-export([do_init/1, reply/2, do_work/3]).

%% API
-export([start_link/5, stop/2, shutdown_pool/2, handle_work/3]).


start_link(WorkerMod,
			PoolSize, VNodeIndex,
			WorkerArgs, WorkerProps) ->
	riak_core_worker_pool:start_link([WorkerMod,
										PoolSize,
										VNodeIndex,
										WorkerArgs,
										WorkerProps],
										?MODULE).
	
handle_work(Pid, Work, From) ->
	riak_core_worker_pool:handle_work(Pid, Work, From).

stop(Pid, Reason) ->
    riak_core_worker_pool:stop(Pid, Reason).

%% wait for all the workers to finish any current work
shutdown_pool(Pid, Wait) ->
	riak_core_worker_pool:shutdown_pool(Pid, Wait).

reply(From, Msg) ->
	riak_core_vnode:reply(From, Msg).

do_init([WorkerMod, PoolSize, VNodeIndex, WorkerArgs, WorkerProps]) ->
    poolboy:start_link([{worker_module, riak_core_vnode_worker},
							{worker_args,
								[VNodeIndex, WorkerArgs, WorkerProps, self()]},
							{worker_callback_mod, WorkerMod},
							{size, PoolSize}, {max_overflow, 0}]).

do_work(Pid, Work, From) ->
	riak_core_vnode_worker:handle_work(Pid, Work, From).


