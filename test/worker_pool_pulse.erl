%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Test riak_core_vnode_worker_pool's interaction with poolboy
%% under PULSE. This requires that riak_core, poolboy, and this module
%% be compiled with the 'PULSE' macro defined.
-module(worker_pool_pulse).

-behaviour(riak_core_vnode_worker).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").

%% riak_core_vnode_worker behavior
-export([init_worker/3, handle_work/3]).
%% console debugging convenience
-compile(export_all).

-ifdef(PULSE).
-include_lib("pulse/include/pulse.hrl").
%% have to transform the 'receive' of the work results
-compile({parse_transform, pulse_instrument}).
%% don't trasnform toplevel test functions
-compile({pulse_skip,[{prop_any_pool,0},
                      {prop_small_pool,0},
                      {pool_test_,0}]}).
-endif.

%%% Worker Definition - does nothing but reply with what it is given

init_worker(_VnodeIndex, Noreply, _WorkerProps) ->
    {ok, Noreply}.

handle_work(die, _From, _State) ->
    exit(test_die);
handle_work(Work, _From, State) ->
    {reply, Work, State}.

%% none of these tests make sense if PULSE is not used
-ifdef(PULSE).

%% @doc Any amount of work should complete through any size pool.
prop_any_pool() ->
    ?FORALL({Seed, ExtraWork, WorkList},
            {pulse:seed(),
             frequency([{10,true},{1,false}]),
             list(frequency([{10,nat()}, {1,die}]))},
            aggregate([{{extra_work,
                         pool_size(ExtraWork, WorkList) < length(WorkList) },
                        {deaths, lists:member(die, WorkList)}}],
            ?WHENFAIL(
               io:format(user,
                         "PoolSize: ~b~n"
                         "WorkList: ~p~n"
                         "Schedule: ~p~n",
                         [pool_size(ExtraWork, WorkList),
                          WorkList,
                          pulse:get_schedule()]),
               begin
                   PoolSize = pool_size(ExtraWork, WorkList),
                   true == all_work_gets_done(Seed, PoolSize, WorkList)
               end))).

pool_size(false, WorkList) ->
    length(WorkList);
pool_size(true, WorkList) ->
    case length(WorkList) div 2 of
        0 ->
            1;
        Size ->
            Size
    end.

%% @doc Minimal case for the issue this test was created to probe: one
%% worker, two inputs.
prop_small_pool() ->
    ?PULSE(Result, all_work_gets_done(1, [1,2]), true == Result).

all_work_gets_done(Seed, PoolSize, WorkList) ->
    pulse:run_with_seed(
      fun() -> all_work_gets_done(PoolSize, WorkList) end,
      Seed).

all_work_gets_done(PoolSize, WorkList) ->
    %% get the pool up
    {ok, Pool} = riak_core_vnode_worker_pool:start_link(
                   ?MODULE, PoolSize, 10, false, []),

    %% send all the work
    [ riak_core_vnode_worker_pool:handle_work(
        Pool, W, {raw, N, self()})
      || {N, W} <- lists:zip(lists:seq(1, length(WorkList)), WorkList) ],

    %% wait for all the work
    Results = [ receive {N, _} -> ok end
                || N <- lists:seq(1, length(WorkList)) ],
    riak_core_vnode_worker_pool:stop(Pool, normal),

    %% check that we got a response for every piece of work
    %% TODO: actually not needed, since the bug is deadlock, and will
    %% thus never get here
    length(Results) == length(WorkList).

pool_test_() ->
    {setup,
        fun() ->
                error_logger:tty(false),
                pulse:start()
        end,
        fun(_) ->
                pulse:stop(),
                error_logger:tty(true)
        end,
        [
         %% not necessary to run both tests here, but why not anyway?
         ?_assert(eqc:quickcheck(prop_small_pool())),
         ?_assert(eqc:quickcheck(prop_any_pool()))
        ]
    }.

-endif.
