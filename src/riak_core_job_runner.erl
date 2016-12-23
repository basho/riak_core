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

%% @private
%% @doc Internal Job Runner Process.
%%
%% These processes are started and owned ONLY by the riak_core_job_sup
%% supervisor.
%%
-module(riak_core_job_runner).

% Private API
-export([start_link/0, run/3]).

% Spawned Function
-export([handle_event/0]).

-include("riak_core_job_internal.hrl").

%% ===================================================================
%% Private API
%% ===================================================================

-spec start_link() -> {'ok', pid()}.
%% @private
%% @doc Start a new process linked to the calling supervisor.
%%
start_link() ->
    {'ok', proc_lib:spawn_link(?MODULE, 'handle_event', [])}.

-spec run(
    Runner  :: pid(),
    MgrKey  :: riak_core_job_manager:mgr_key(),
    Job     :: riak_core_job:job())
        -> 'ok' | {'error', term()}.
%% @private
%% @doc Tell the specified runner to start the unit of work.
%%
%% The specified reference is passed in callbacks to the jobs service.
%%
run(Runner, MgrKey, Job) ->
    Msg = {'start', erlang:self(), MgrKey, Job},
    case erlang:is_process_alive(Runner) of
        'true' ->
            _ = erlang:send(Runner, Msg),
            receive {'started', MgrKey} ->
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
%% @private
%% @doc Check pending events and continue running or exit.
%%
handle_event() ->
    _ = erlang:erase(),
    _ = erlang:process_flag('trap_exit', 'true'),
    receive
        {'EXIT', _, Reason} ->
            erlang:exit(Reason);
        {'start', Starter, MgrKey, Job} ->
            _ = erlang:send(Starter, {'started', MgrKey}),
            run(MgrKey, Job)
    end,
    handle_event().

%% ===================================================================
%% Internal
%% ===================================================================

-spec run(
    MgrKey  :: riak_core_job_manager:mgr_key(),
    Job     :: riak_core_job:job())
        -> 'ok'.
%
% Run one unit of work.
%
run(MgrKey, Job) ->
    Work = riak_core_job:work(Job),
    riak_core_job_manager:starting(MgrKey),
    Ret1 = riak_core_job:invoke(riak_core_job:setup(Work), MgrKey),
    riak_core_job_manager:running(MgrKey),
    Ret2 = riak_core_job:invoke(riak_core_job:main(Work), Ret1),
    riak_core_job_manager:cleanup(MgrKey),
    Ret3 = riak_core_job:invoke(riak_core_job:cleanup(Work), Ret2),
    riak_core_job_manager:finished(MgrKey, Ret3).
