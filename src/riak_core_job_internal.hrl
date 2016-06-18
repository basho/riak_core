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

%% Macros shared among the processes in the job supervision tree.
%% There is NOTHING in this file that should be used outside those processes!

-ifdef(PULSE).
-compile([
    export_all,
    {parse_transform,   pulse_instrument},
    {pulse_replace_module, [
        {gen_fsm,       pulse_gen_fsm},
        {gen_server,    pulse_gen_server},
        {supervisor,    pulse_supervisor}
    ]}
]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("riak_core_vnode.hrl").

%% These types are exported by one or more modules.
-type node_type()   ::  atom().
-type node_id()     ::  {node_type(), partition()}.

-ifdef(namespaced_types).
-define(DictT,      dict:dict()).
-define(OrdDictT,   orddict:orddict()).
-else.
-define(DictT,      dict()).
-define(OrdDictT,   orddict()).
-endif.
-define(ProcIdPat,  {_, {_, _}}).

-define(NODE_JOB_MGR_STARTUP_TIMEOUT,    8000).

%% These shutdown timeouts should always be in descending order.
-define(CORE_SVC_SHUTDOWN_TIMEOUT,      28000).
-define(NODE_JOB_SUP_SHUTDOWN_TIMEOUT,  26000).
-define(NODE_JOB_MGR_SHUTDOWN_TIMEOUT,  24000).
-define(NODE_JOB_RUN_SHUTDOWN_TIMEOUT,  22000).

-define(NODE_SUP_ID(VNodeID),   {node_sup, VNodeID}).
-define(NODE_MGR_ID(VNodeID),   {node_mgr, VNodeID}).
-define(WORK_SUP_ID(VNodeID),   {work_sup, VNodeID}).

-type node_sup_id() ::  ?NODE_SUP_ID(node_id()).
-type node_mgr_id() ::  ?NODE_MGR_ID(node_id()).
-type work_sup_id() ::  ?WORK_SUP_ID(node_id()).

-type job_proc_id() ::  node_sup_id() | node_mgr_id() | work_sup_id().
