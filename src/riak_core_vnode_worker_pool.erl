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
-module(riak_core_vnode_worker_pool).

-behaviour(gen_fsm).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
        terminate/3, code_change/4]).

%% gen_fsm states
-export([ready/2, queueing/2, ready/3, queueing/3]).

%% API
-export([start_link/5]).

-record(state, {
        queue = queue:new(),
        pool :: pid()
        %remaining = 0 :: non_neg_integer()
    }).

start_link(WorkerMod, PoolSize, VNodeIndex, WorkerArgs, WorkerProps) ->
    gen_fsm:start_link(?MODULE, [WorkerMod, PoolSize,  VNodeIndex, WorkerArgs, WorkerProps], []).

init([WorkerMod, PoolSize, VNodeIndex, WorkerArgs, WorkerProps]) ->
    %io:format("~p starting worker pool with module ~p size ~p~n", [self(), WorkerMod,
            PoolSize]),
    {ok, Pid} = poolboy:start_link([{worker_module, riak_core_vnode_worker},
            {worker_args, [VNodeIndex, WorkerArgs, WorkerProps]},
            {worker_callback_mod, WorkerMod},
            {size, PoolSize}, {max_overflow, 0},
            {checkout_blocks, false}]),
    {ok, ready, #state{pool=Pid}}.

ready(_Event, _From, State) ->
    {reply, ok, ready, State}.

ready({work, Work, From} = Msg, #state{pool=Pool, queue=Q} = State) ->
    case poolboy:checkout(Pool) of
        full ->
            %io:format("queued work~n"),
            {next_state, queueing, State#state{queue=queue:in(Msg, Q)}};
        Pid when is_pid(Pid) ->
            %io:format("offloading work to worker ~p~n", [Pid]),
            ok = riak_core_vnode_worker:handle_work(Pid, Pool, Work, From),
            {next_state, ready, State}
    end;
ready(_Event, State) ->
    {next_state, ready, State}.

queueing(_Event, _From, State) ->
    {reply, ok, queueing, State}.

queueing({work, _Work, _From} = Msg, #state{queue=Q} = State) ->
    %io:format("queued work~n"),
    {next_state, queueing, State#state{queue=queue:in(Msg, Q)}};
queueing(_Event, State) ->
    {next_state, queueing, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, {error, unknown_message}, StateName, State}.

handle_info(checkin, _, #state{pool = Pool, queue=Q} = State) ->
    %io:format("checkin -- switching back to ready~n"),
    case queue:out(Q) of
        {{value, {work, Work, From}}, Rem} ->
            case poolboy:checkout(Pool) of
                full ->
                    {next_state, queueing, State#state{queue=Q}};
                Pid when is_pid(Pid) ->
                    %io:format("offloading work to worker ~p~n", [Pid]),
                    ok = riak_core_vnode_worker:handle_work(Pid, Pool, Work, From),
                    {next_state, queueing, State#state{queue=Rem}}
            end;
        {empty, Empty} ->
            {next_state, ready, State#state{queue=Empty}}
    end;
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.
