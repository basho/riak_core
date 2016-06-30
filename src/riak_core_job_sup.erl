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

-module(riak_core_job_sup).
-behaviour(supervisor).

%% @doc Processes in the job management supervision tree.
%%
%% Each of this module's init/1 functions represents a supervisor level in
%% the supervision tree. The top level is a singleton, and the rest are
%% per-scope.
%%
%% Top level supervisor
%%  Start:  riak_core_job_sup:start_link()
%%  Child:  Request manager
%%      Start:  riak_core_job_manager:start_link(Parent)
%%  Child:  Per scope supervisor
%%      Start:  riak_core_job_sup:start_link(?SCOPE_SUP_ID(ScopeID),...)
%%      Child:  Per-scope job service
%%          Start:  riak_core_job_service:start_link(?SCOPE_SVC_ID(ScopeID),...)
%%      Child:  Work supervisor
%%          Start:  riak_core_job_sup:start_link(?WORK_SUP_ID(ScopeID))
%%          Child:  Work runner
%%              Start:  riak_core_job_runner:start_link(ScopeID, ...)
%%

% Public API
-export([start_link/0]).

% Private API
-export([start_link/1, start_link/3, start_scope/4, stop_scope/2]).

% supervisor callbacks
-export([init/1]).

-include("riak_core_job_internal.hrl").

% type shorthand
-type config()  ::  riak_core_job_service:config().
-type dummy()   ::  riak_core_job:job().

%% ===================================================================
%% Public API
%% ===================================================================

-spec start_link() -> {ok, pid()}.
%%
%% @doc Creates the singleton top-level supervisor.
%%
start_link() ->
    supervisor:start_link(?MODULE, ?MODULE).

%% ===================================================================
%% Private API
%% ===================================================================

-spec start_link(work_sup_id()) -> {ok, pid()}.
%%
%% @doc Start a per-scope work supervisor.
%%
start_link(WorkSupId) ->
    supervisor:start_link(?MODULE, WorkSupId).

-spec start_link(scope_sup_id(), config(), dummy())
        -> {ok, pid()} | {error, term()}.
%%
%% @doc Start a per-scope service supervisor.
%%
start_link(ScopeSupId, SvcConfig, DummyJob) ->
    supervisor:start_link(?MODULE, {ScopeSupId, SvcConfig, DummyJob}).

-spec start_scope(pid(), scope_id(), config(), dummy())
        -> {ok, pid()} | {error, term()}.
%%
%% @doc Start a per-scope supervision tree.
%%
%% Returns {ok, pid()} if either started successfully or already running.
%%
start_scope(JobsSup, ScopeID, SvcConfig, DummyJob) ->
    ScopeSupId = ?SCOPE_SUP_ID(ScopeID),
    case supervisor:start_child(JobsSup, {ScopeSupId,
            {?MODULE, start_link, [ScopeSupId, SvcConfig, DummyJob]},
            permanent, ?SCOPE_SUP_SHUTDOWN_TIMEOUT, supervisor, [?MODULE]}) of
        {ok, _} = Ret ->
            Ret;
        {error, {already_started, ScopeSupPid}} ->
            {ok, ScopeSupPid};
        {error, _} = Error ->
            Error
    end.

-spec stop_scope(pid(), scope_id()) -> ok | {error, term()}.
%%
%% @doc Stop a per-scope supervision tree.
%%
%% Returns {ok, pid()} if either started successfully or already running.
%%
stop_scope(JobsSup, ScopeID) ->
    ScopeSupId = ?SCOPE_SUP_ID(ScopeID),
    case supervisor:terminate_child(JobsSup, ScopeSupId) of
        ok ->
            % If this doesn't return 'ok', something's badly hosed somewhere.
            supervisor:delete_child(JobsSup, ScopeSupId);
        {error, not_found} ->
            ok;
        {error, _} = Error ->
            Error
    end.

%% ===================================================================
%% Callbacks
%% ===================================================================

-spec init(?MODULE | {scope_sup_id(), config(), dummy()} | work_sup_id())
        -> 'ignore' | {'ok', {
                {supervisor:strategy(), pos_integer(), pos_integer()},
                [supervisor:child_spec()] }}.
%
% The restart frequency is set to handle the case of an evil job being
% submitted to all scopes that somehow causes each service that receives it
% to crash. That REALLY shouldn't happen, but since we don't know here how
% many scopes could end up running on this node we kinda punt for now.
% TODO: should we try to do something intelligent based on ring size?
%
init({?SCOPE_SUP_ID(ScopeID) = Id, SvcConfig, DummyJob}) ->
    riak_core_job_manager:register(Id, erlang:self()),
    ScopeSvcId = ?SCOPE_SVC_ID(ScopeID),
    WorkSupId = ?WORK_SUP_ID(ScopeID),
    {ok, {{rest_for_one, 30, 60}, [
        {ScopeSvcId,
            {riak_core_job_service, start_link,
                [ScopeSvcId, SvcConfig, DummyJob]},
            permanent, ?SCOPE_SVC_SHUTDOWN_TIMEOUT,
            worker, [riak_core_job_service]},
        {WorkSupId,
            {?MODULE, start_link, [WorkSupId]},
            permanent, ?SCOPE_SUP_SHUTDOWN_TIMEOUT,
            supervisor, [?MODULE]}
    ]}};

%% Per-scope worker supervisor
init(?WORK_SUP_ID(ScopeID) = Id) ->
    riak_core_job_manager:register(Id, erlang:self()),
    {ok, {{simple_one_for_one, 30, 60}, [
        {vnode_job, {riak_core_job_runner, start_link, [ScopeID]},
            temporary, ?WORK_RUN_SHUTDOWN_TIMEOUT,
            worker, [riak_core_job_runner]}
    ]}};

%% Singleton top-level supervisor
init(?MODULE) ->
    JobsSup = erlang:self(),
    {ok, {{one_for_one, 30, 60}, [
        {riak_core_job_manager,
            {riak_core_job_manager, start_link, [JobsSup]},
            permanent, ?JOBS_MGR_SHUTDOWN_TIMEOUT,
            worker, [riak_core_job_manager]}
    ]}}.
