%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.
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
-module(worker_pool_test).

-behaviour(riak_core_vnode_worker).
-include_lib("eunit/include/eunit.hrl").

-export([init_worker/3, handle_work/3]).

-define(P1_CONCURRENCY,  3).
-define(P1_PARTITION,   13).

-define(P2_CONCURRENCY,  7).
-define(P2_PARTITION,   17).

init_worker(_VnodeIndex, Noreply, _WorkerProps) ->
    {'ok', Noreply}.

handle_work(Work, From, 'true' = State) ->
    Work(),
    riak_core_vnode:reply(From, 'ok'),
    {'noreply', State};

handle_work(Work, _From, 'false' = State) ->
    Work(),
    {'reply', 'ok', State}.

-ifdef(TEST).

receive_result(N) ->
    receive
        {N, 'ok'} when (N rem 2) /= 0 ->
            'true';
        {N, {'error', {'worker_crash', _, _}}} when (N rem 2) == 0 ->
            'true';
        {N, _} = Msg ->
            ?debugFmt("received ~p~n", [Msg]),
            'false'
    end.

test_worker_pool(Partition, PoolSize, Arg) ->
    {'ok', Pool} = riak_core_vnode_worker_pool:start_link(
                        ?MODULE, PoolSize, Partition, Arg, []),

    Seq = lists:seq(1, 10),
    lists:foldl(fun submit_work/2, Pool, Seq),
    [?assertEqual('true', receive_result(N)) || N <- Seq],

    erlang:unlink(Pool),
    riak_core_vnode_worker_pool:shutdown_pool(Pool, 1000),
    'ok'.

submit_work(N, Pool) ->
    Work = fun() ->
        timer:sleep(99),
        1/(N rem 2)
    end,
    From = {raw, N, erlang:self()},
    riak_core_vnode_worker_pool:handle_work(Pool, Work, From),
    Pool.

simple_worker_pool() ->
    test_worker_pool(?P1_PARTITION, ?P1_CONCURRENCY, 'false').

simple_noreply_worker_pool() ->
    test_worker_pool(?P2_PARTITION, ?P2_CONCURRENCY, 'true').

pool_test_() ->
    {setup,
        fun() ->
            error_logger:tty(false),
            {ok, Sup} = riak_core_job_sup:start_link(),
            Sup
        end,
        fun(Sup) ->
            erlang:unlink(Sup),
            erlang:exit(Sup, 'shutdown'),
            error_logger:tty(true)
        end,
        [
            fun simple_worker_pool/0,
            fun simple_noreply_worker_pool/0
        ]
    }.

-endif.
