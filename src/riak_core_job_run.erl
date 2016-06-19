%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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

-module(riak_core_job_run).

% Public API
-export([start_link/1, run/4]).

% Spawned Function
-export([runner/1]).

-include("riak_core_job_internal.hrl").

%% ===================================================================
%% API functions
%% ===================================================================

-spec start_link(node_id()) -> {ok, pid()}.
%% @doc Start a new process linked to the calling supervisor.
start_link(VNodeID) ->
    {ok, erlang:spawn_link(?MODULE, runner, [VNodeID])}.

-spec run(pid(), pid(), reference(), riak_core_job:work()) -> ok.
%% @doc Tell the specified runner to start the unit of work.
%% The specified reference is passed in callbacks to the manager.
run(Runner, Mgr, Ref, Work) ->
    Runner ! {start, Mgr, Ref, Work, erlang:self()},
    receive
        {started, Ref} ->
            ok
    end.

%% ===================================================================
%% Process
%% ===================================================================

-spec runner(node_id()) -> ok | no_return().
%% @doc Process entry point.
runner(VNodeID) ->
    {Mgr, Ref, Work} = receive
        {start, M, R, W, Starter} ->
            Starter ! {started, R},
            {M, R, W}
    end,
    riak_core_job_mgr:starting(Mgr, Ref),
    Ctx1 = invoke(VNodeID, riak_core_job:get(init, Work)),
    riak_core_job_mgr:running(Mgr, Ref),
    Ctx2 = invoke(Ctx1, riak_core_job:get(run, Work)),
    riak_core_job_mgr:cleanup(Mgr, Ref),
    Ret = invoke(Ctx2, riak_core_job:get(fini, Work)),
    riak_core_job_mgr:done(Mgr, Ref, Ret),
    ok.

-spec invoke(term(), {module(), atom(), [term()]} | {fun(), [term()]})
        -> term() | no_return().
invoke(First, {Mod, Func, Args}) ->
    erlang:apply(Mod, Func, [First | Args]);
invoke(First, {Func, Args}) ->
    erlang:apply(Func, [First | Args]).
