%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.
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

%%
%% @doc This module is deprecated and will be removed in v3!
%%
%% @deprecated Use module {@link riak_core_job_manager}.
%%
-module(riak_core_vnode_worker).
-deprecated(module).

-include("riak_core_vnode.hrl").

%% init_worker(VNodeIndex, WorkerArgs, WorkerProps) -> {ok, WorkerState}.
-callback init_worker(partition(), [term()], [atom() | {atom(), term()}])
        -> {ok, term()}.

%% handle_work(WorkSpec, Sender, WorkerState) -> Response.
-callback handle_work(tuple(), sender(), term())
        -> {reply, term(), term()} | {noreply, term()}.
