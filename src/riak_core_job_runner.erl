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
-export([start_link/1, run/4]).

% Spawned Function
-export([runner/2]).

-include("riak_core_job_internal.hrl").

%% ===================================================================
%% Internal State
%% ===================================================================

-define(MAX_SPURIOUS,   3).

% Support the runner's token being distinct from the globally-defined constant.
-record(state, {
    token                   ::  term(),
    recycled    = false     ::  boolean(),
    spurious    = 0         ::  non_neg_integer()
}).
-type state()   ::  #state{}.

%% ===================================================================
%% Private API
%% ===================================================================

-spec start_link(Token :: term()) -> {'ok', pid()}.
%% @private
%% @doc Start a new Runner linked to the calling process.
%%
start_link(?job_run_ctl_token = Token) ->
    {'ok', proc_lib:spawn_link(
        ?MODULE, runner, [?job_run_ctl_token, #state{token = Token}])}.

-spec run(
    Runner  :: pid(),
    Token   :: term(),
    MgrKey  :: riak_core_job_manager:mgr_key(),
    Job     :: riak_core_job:job())
        -> 'ok' | {'error', term()}.
%% @private
%% @doc Tell the specified Runner to start the Job's Unit of Work.
%%
%% The specified MgrKey is passed in callbacks to the Jobs Manager.
%%
run(Runner, ?job_run_ctl_token = Token, MgrKey, Job) ->
    Msg = {start, erlang:self(), Token, MgrKey, Job},
    case erlang:is_process_alive(Runner) of
        true ->
            _ = erlang:send(Runner, Msg),
            receive {started, Token, MgrKey} ->
                ok
            after 9999 ->
                {error, timeout}
            end;
        _ ->
            {error, noproc}
    end.

%% ===================================================================
%% Process
%% ===================================================================

-spec runner(Token :: term(), State :: state()) -> no_return().
%% @private
%% @doc Process entry.
%%
runner(?job_run_ctl_token, State) ->
    run_loop(State).

%% ===================================================================
%% Internal
%% ===================================================================

-spec run_loop(State :: state()) -> no_return().
%
% Main loop, once per Job.
%
run_loop(#state{recycled = true} = State) ->
    _ = erlang:process_flag(trap_exit, true),
    _ = erlang:erase(),
    handle_event(State);

run_loop(State) ->
    _ = erlang:process_flag(trap_exit, true),
    handle_event(State).

-spec handle_event(State :: state()) -> no_return().
%
% Check pending events and continue running or exit.
%
handle_event(State) when State#state.spurious > ?MAX_SPURIOUS ->
    erlang:exit(spurious);

handle_event(#state{token = Token} = State) ->
    receive
        {'EXIT', _, Reason} ->
            erlang:exit(Reason);

        {start, Starter, Token, MgrKey, Job} ->
            _ = erlang:send(Starter, {started, Token, MgrKey}),
            run(MgrKey, Job),
            run_loop(State#state{recycled = true});

        {confirm, Service, Nonce} ->
            % Allow closely-coupled processes that know the secret handshake
            % to confirm that this is a runner.
            Hash = erlang:phash2({Nonce, Token, erlang:self()}),
            _ = erlang:send(Service, {confirm, Nonce, Hash}),
            handle_event(State);

        Other ->
            handle_event(Other, State)
    end.

-spec handle_event(Message :: term(), State :: state()) -> no_return().
%
% This is almost certainly a straggler directed at a previous UoW running
% on this process, which is, of course, the downside of recycling Runners.
% We wouldn't be recycling, though, if load wasn't an issue, so discard
% it and continue ... but keep a count.
%
handle_event(Spurious, #state{recycled = true} = State) ->
    _ = lager:warning(
        "Runner ~p received spurious message ~p",
        [erlang:self(), Spurious]),
    handle_event(State#state{spurious = (State#state.spurious + 1)});
%
% This is a freshly started process, so it's not just some spurious noise.
%
handle_event(Unknown, _State) ->
    _ = lager:error(
        "Runner ~p exiting due to unrecognized message ~p",
        [erlang:self(), Unknown]),
    erlang:exit(shutdown).

-spec run(
    MgrKey  :: riak_core_job_manager:mgr_key(),
    Job     :: riak_core_job:job())
        -> 'ok'.
%
% Run one Unit of Work.
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
