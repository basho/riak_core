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

-module(riak_core_job_runner).

% Public API
-export([start_link/1, run/4]).

% Spawned Function
-export([runner/1]).

-include("riak_core_job_internal.hrl").

%% ===================================================================
%% API functions
%% ===================================================================

-spec start_link(scope_id()) -> {'ok', pid()}.
%% @doc Start a new process linked to the calling supervisor.
start_link(ScopeID) ->
    {ok, erlang:spawn_link(?MODULE, 'runner', [ScopeID])}.

-spec run(pid(), pid(), reference(), riak_core_job:work()) -> 'ok'.
%% @doc Tell the specified runner to start the unit of work.
%% The specified reference is passed in callbacks to the scope service.
run(Runner, Svc, Ref, Work) ->
    Runner ! {'start', Svc, Ref, Work, erlang:self()},
    receive
        {'started', Ref} ->
            'ok'
    end.

%% ===================================================================
%% Process
%% ===================================================================

-spec runner(scope_id()) -> 'ok' | no_return().
%% @doc Process entry point.
runner(ScopeID) ->
    {Svc, Ref, Work} = receive
        {'start', S, R, W, Starter} ->
            Starter ! {'started', R},
            {S, R, W}
    end,
    riak_core_job_service:starting(Svc, Ref),
    Ctx1 = invoke({ScopeID, Svc}, riak_core_job:get('init', Work)),
    riak_core_job_service:running(Svc, Ref),
    Ctx2 = invoke(Ctx1, riak_core_job:get('run', Work)),
    riak_core_job_service:cleanup(Svc, Ref),
    Ret = invoke(Ctx2, riak_core_job:get('fini', Work)),
    riak_core_job_service:done(Svc, Ref, Ret),
    ok.

-spec invoke(term(), {module(), atom(), [term()]} | {fun(), [term()]})
        -> term() | no_return().
invoke(First, {Mod, Func, Args}) ->
    erlang:apply(Mod, Func, [First | Args]);
invoke(First, {Func, Args}) ->
    erlang:apply(Func, [First | Args]).
