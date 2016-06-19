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
%% the supervision tree. The top level is singleton, and the rest two are
%% per-vnode.
%%
%% Top level supervisor
%%  Start:  ?MODULE:start_link()
%%  Child:  Request service
%%      Start:  riak_core_job_svc:start_link(Parent)
%%  Child:  Per vnode supervisor
%%      Start:  ?MODULE:start_link(?NODE_SUP_ID(VNodeID), ...)
%%      Child:  Job manager
%%          Start:  riak_core_job_mgr:start_link(?NODE_MGR_ID(VNodeID), ...)
%%      Child:  Work supervisor
%%          Start:  ?MODULE:start_link(?WORK_SUP_ID(VNodeID))
%%          Child:  Work runner
%%              Start:  riak_core_job_run:start_link(VNodeID, ...)
%%

% Public API
-export([start_link/0]).

% Private API
-export([start_link/1, start_link/3, start_node/4, stop_node/2]).

% supervisor callbacks
-export([init/1]).

-include("riak_core_job_internal.hrl").

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
%% @doc Start a per-node work supervisor.
%%
start_link(WorkSupId) ->
    supervisor:start_link(?MODULE, WorkSupId).

-spec start_link(node_sup_id(), riak_core_job_mgr:config(), riak_core_job:job())
        -> {ok, pid()} | {error, term()}.
%%
%% @doc Start a per-node service supervisor.
%%
start_link(NodeSupId, MgrConfig, DummyJob) ->
    supervisor:start_link(?MODULE, {NodeSupId, MgrConfig, DummyJob}).

-spec start_node(pid(), node_id(),
        riak_core_job_mgr:config(), riak_core_job:job())
        -> {ok, pid()} | {error, term()}.
%%
%% @doc Start a per-node service supervisor.
%%
%% Returns {ok, pid()} if either started successfully or already running.
%%
start_node(JobsSup, VNodeID, MgrConfig, DummyJob) ->
    NodeSupId = ?NODE_SUP_ID(VNodeID),
    case supervisor:start_child(JobsSup, {NodeSupId,
            {?MODULE, start_link, [NodeSupId, MgrConfig, DummyJob]},
            permanent, ?NODE_SUP_SHUTDOWN_TIMEOUT, supervisor, [?MODULE]}) of
        {ok, _} = Ret ->
            Ret;
        {error, {already_started, NodeSupPid}} ->
            {ok, NodeSupPid};
        {error, _} = Error ->
            Error
    end.

-spec stop_node(pid(), node_id()) -> ok | {error, term()}.
%%
%% @doc Start a per-node service supervisor.
%%
%% Returns {ok, pid()} if either started successfully or already running.
%%
stop_node(JobsSup, VNodeID) ->
    NodeSupId = ?NODE_SUP_ID(VNodeID),
    case supervisor:terminate_child(JobsSup, NodeSupId) of
        ok ->
            % If this doesn't return 'ok', something's badly FU'd somewhere.
            supervisor:delete_child(JobsSup, NodeSupId);
        {error, not_found} ->
            ok;
        {error, _} = Error ->
            Error
    end.

%% ===================================================================
%% Callbacks
%% ===================================================================

% The restart frequency is set to handle a job being submitted to all vnodes
% that crashes each manager process that receives it. That REALLY shouldn't
% happen, but since we don't know here how many vnodes could end up running
% on this node we kinda punt for now.
% TODO: should we try to do something intelligent based on ring size?

init({?NODE_SUP_ID(VNodeID) = Id, MgrConfig, DummyJob}) ->
    riak_core_job_svc:register(Id, erlang:self()),
    NodeMgrId = ?NODE_MGR_ID(VNodeID),
    WorkSupId = ?WORK_SUP_ID(VNodeID),
    {ok, {{rest_for_one, 30, 60}, [
        {NodeMgrId,
            {riak_core_job_mgr, start_link, [NodeMgrId, MgrConfig, DummyJob]},
            permanent, ?NODE_MGR_SHUTDOWN_TIMEOUT,
            worker, [riak_core_job_mgr]},
        {WorkSupId,
            {?MODULE, start_link, [WorkSupId]},
            permanent, ?NODE_SUP_SHUTDOWN_TIMEOUT,
            supervisor, [?MODULE]}
    ]}};

%% Per-node worker supervisor
init(?WORK_SUP_ID(VNodeID) = Id) ->
    riak_core_job_svc:register(Id, erlang:self()),
    {ok, {{simple_one_for_one, 30, 60}, [
        {vnode_job, {riak_core_job_run, start_link, [VNodeID]},
            temporary, ?NODE_RUN_SHUTDOWN_TIMEOUT,
            worker, [riak_core_job_run]}
    ]}};

%% Singleton top-level supervisor
init(?MODULE) ->
    JobsSup = erlang:self(),
    {ok, {{one_for_one, 30, 60}, [
        {riak_core_job_svc, {riak_core_job_svc, start_link, [JobsSup]},
            permanent, ?JOBS_SVC_SHUTDOWN_TIMEOUT,
            worker, [riak_core_job_svc]}
    ]}}.
