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
-export([start_link/0, run/4]).

% Spawned Function
-export([handle_event/0]).

-include("riak_core_job_internal.hrl").

%% ===================================================================
%% API functions
%% ===================================================================

-spec start_link() -> {'ok', pid()}.
%%
%% @doc Start a new process linked to the calling supervisor.
%%
start_link() ->
    {'ok', proc_lib:spawn_link(?MODULE, 'handle_event', [])}.

-spec run(
    Runner  :: pid(),
    Manager :: atom() | pid(),
    Ref     :: reference(),
    Job     :: riak_core_job:job())
        -> 'ok' | {'error', term()}.
%%
%% @doc Tell the specified runner to start the unit of work.
%%
%% The specified reference is passed in callbacks to the jobs service.
%%
run(Runner, Manager, Ref, Job) ->
    Msg = {'start', erlang:self(), Manager, Ref, Job},
    case erlang:is_process_alive(Runner) of
        'true' ->
            _ = erlang:send(Runner, Msg),
            receive {'started', Ref} ->
                'ok'
            after 9999 ->
                {'error', 'timeout'}
            end;
        _ ->
            {'error', 'noproc'}
    end.

%% ===================================================================
%% Process
%% ===================================================================

-spec handle_event() -> no_return().
%
% Check pending events and report whether to continue running or exit.
%
handle_event() ->
    _ = erlang:erase(),
    _ = erlang:process_flag('trap_exit', 'true'),
    receive
        {'EXIT', _, Reason} ->
            erlang:exit(Reason);
        {'start', Starter, Manager, Ref, Job} ->
            _ = erlang:send(Starter, {'started', Ref}),
            run(Manager, Ref, Job)
    end,
    handle_event().

%% ===================================================================
%% Internal
%% ===================================================================

-spec run(
    Manager :: atom() | pid(),
    Ref     :: reference(),
    Job     :: riak_core_job:job())
        -> 'ok'.
%%
%% @doc Run one unit of work.
%%
run(Manager, Ref, Job) ->
    Work = riak_core_job:work(Job),
    riak_core_job_manager:starting(Manager, Ref),
    Ret1 = riak_core_job:invoke(riak_core_job:setup(Work), Manager),
    riak_core_job_manager:running(Manager, Ref),
    Ret2 = riak_core_job:invoke(riak_core_job:main(Work), Ret1),
    riak_core_job_manager:cleanup(Manager, Ref),
    Ret3 = riak_core_job:invoke(riak_core_job:cleanup(Work), Ret2),
    riak_core_job_manager:finished(Manager, Ref, Ret3).
