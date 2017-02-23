%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016-2017 Basho Technologies, Inc.
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
%% ANY unexpected message causes the process to terminate.
%%
-module(riak_core_job_runner).
-behaviour(gen_server).

%% Private API
-export([
    run/4
]).

%% Gen_server API
-export([
    code_change/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1,
    start_link/1,
    terminate/2
]).

-include("riak_core_job_internal.hrl").

%% ===================================================================
%% Internal State
%% ===================================================================

-define(MAX_SPURIOUS,   3).

%% Support the runner's token being distinct from the globally-defined constant.
-record(state, {
    token                   ::  term(),
    recycled    = false     ::  boolean()
}).
-type state()   ::  #state{}.

%% ===================================================================
%% Private API
%% ===================================================================

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
run(Runner, Token, MgrKey, Job) ->
    Request = {start, Token, MgrKey, Job},
    case catch gen_server:call(Runner, Request) of
        {'EXIT', {Reason, _Stack}} ->
            {error, Reason};
        {'EXIT', Why} ->
            {error, Why};
        Result ->
            Result
    end.

%% ===================================================================
%% Process
%% ===================================================================

-spec start_link(Token :: term()) -> {ok, pid()}.
%% @private
%% @doc Start a new Runner linked to the calling process.
%%
start_link(Token) ->
    gen_server:start_link(?MODULE, Token, []).

-spec init(Token :: term()) -> {ok, state()}.
%% @private
%% @doc gen_server initialization.
%%
init(Token) ->
    {ok, #state{token = Token}}.

-spec handle_call(Request :: term(), From :: {pid(), term()}, State :: state())
        -> {reply, term(), state()} | {stop, term(), term(), state()}.
%% @private
%% @doc Handle a job start request.
%%
handle_call({start, Token, MgrKey, Job}, _From, #state{token = Token} = State) ->
    _ = gen_server:cast(erlang:self(), {run, Token, MgrKey, Job}),
    {reply, ok, State};

handle_call(Request, From, State) ->
    _ = lager:error("Unrecognized call ~p from ~p, exiting.", [Request, From]),
    {stop, badarg, {error, badarg}, State}.

-spec handle_cast(Request :: term(), State :: state())
        -> {noreply, state()} | {stop, term(), state()}.
%% @private
%% @doc Handle a job run request.
%%
handle_cast({run, Token, MgrKey, Job}, #state{token = Token, recycled = false} = State) ->
    _ = run(MgrKey, Job),
    {noreply, State#state{recycled = true}};

handle_cast({run, Token, MgrKey, Job}, #state{token = Token} = State) ->
    _ = erlang:erase(),
    _ = run(MgrKey, Job),
    {noreply, State};

handle_cast(Request, State) ->
    _ = lager:error("Unrecognized cast ~p, exiting.", [Request]),
    {stop, badarg, State}.

-spec handle_info(Message :: term(), State :: state())
        -> {stop, term(), state()}.
%% @private
%% @doc Anything that shows up here is spurious by definition.
%%
handle_info(Message, State) ->
    _ = lager:error("Spurious message ~p, exiting.", [Message]),
    {stop, spurious, State}.

-spec code_change(OldVsn :: term(), State :: state(), Extra :: term())
        -> {ok, state()}.
%% @private
%% @doc Don't care, carry on.
%%
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec terminate(Why :: term(), State :: state()) -> ok.
%% @private
%% @doc Don't care, the supervisor will see it and clean up.
%%
terminate(_Why, _State) ->
    ok.

%% ===================================================================
%% Internal
%% ===================================================================

-spec run(
    MgrKey  :: riak_core_job_manager:mgr_key(),
    Job     :: riak_core_job:job())
        -> 'ok'.
%%
%% Run one Unit of Work.
%%
run(MgrKey, Job) ->
    Work = riak_core_job:work(Job),
    riak_core_job_manager:starting(MgrKey),
    Ret1 = riak_core_job:invoke(riak_core_job:setup(Work), MgrKey),
    riak_core_job_manager:running(MgrKey),
    Ret2 = riak_core_job:invoke(riak_core_job:main(Work), Ret1),
    riak_core_job_manager:cleanup(MgrKey),
    Ret3 = riak_core_job:invoke(riak_core_job:cleanup(Work), Ret2),
    riak_core_job_manager:finished(MgrKey, Ret3).
