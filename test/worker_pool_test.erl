%% -------------------------------------------------------------------
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
-module(worker_pool_test).

-behaviour(riak_core_vnode_worker).
-include_lib("eunit/include/eunit.hrl").

-export([init_worker/3, handle_work/3]).

init_worker(_VnodeIndex, Noreply, _WorkerProps) ->
    {ok, Noreply}.

handle_work(Work, From, true = State) ->
    Work(),
    riak_core_vnode:reply(From, ok),
    {noreply, State};
handle_work(Work, _From, false = State) ->
    Work(),
    {reply, ok, State}.

-ifdef(TEST).

receive_result(N) ->
    receive
        {N, ok} when N rem 2 /= 0 ->
            true;
        {N, {error, {worker_crash, _, _}}} when N rem 2 == 0 ->
            true
    after
        0 ->
            timeout
    end.

simple_worker_pool() ->
    {ok, Pool} = riak_core_vnode_worker_pool:start_link(?MODULE, 3, 10, false, []),
    [ riak_core_vnode_worker_pool:handle_work(Pool, fun() ->
                        timer:sleep(100),
                        1/(N rem 2)
                end,
                {raw, N, self()})
            || N <- lists:seq(1, 10)],

    timer:sleep(1200),

    %% make sure we got all the expected responses

    [ ?assertEqual(true, receive_result(N)) || N <- lists:seq(1, 10)].

simple_noreply_worker_pool() ->
    {ok, Pool} = riak_core_vnode_worker_pool:start_link(?MODULE, 3, 10, true, []),
    [ riak_core_vnode_worker_pool:handle_work(Pool, fun() ->
                        timer:sleep(100),
                        1/(N rem 2)
                end,
                {raw, N, self()})
            || N <- lists:seq(1, 10)],

    timer:sleep(1200),

    %% make sure we got all the expected responses

    [ ?assertEqual(true, receive_result(N)) || N <- lists:seq(1, 10)].


pool_test_() ->
    {setup,
        fun() ->
                error_logger:tty(false)
        end,
        fun(_) ->
                error_logger:tty(true)
        end,
        [
            fun simple_worker_pool/0,
            fun simple_noreply_worker_pool/0
        ]
    }.

-endif.
