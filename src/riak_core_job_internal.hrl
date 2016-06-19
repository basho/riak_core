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

%% These types are used internally between cooperating modules.
-define(NODE_SUP_TAG,   'node_sup').
-define(NODE_MGR_TAG,   'node_mgr').
-define(WORK_SUP_TAG,   'work_sup').

-type proc_type()   ::  ?NODE_SUP_TAG | ?NODE_MGR_TAG | ?WORK_SUP_TAG .
-type proc_id()     ::  {proc_type(), node_id()}.

-type node_sup_id() ::  {?NODE_SUP_TAG, node_id()}.
-type node_mgr_id() ::  {?NODE_MGR_TAG, node_id()}.
-type work_sup_id() ::  {?WORK_SUP_TAG, node_id()}.

%% Allow a pretty long time for a node to start. It shouldn't take anywhere
%% near this long unless the system is really bogged down.
-define(NODE_STARTUP_TIMEOUT,       12000).

%% Shutdown timeouts should always be in descending order as listed here.
-define(JOBS_SVC_SHUTDOWN_TIMEOUT,  28000).
-define(NODE_SUP_SHUTDOWN_TIMEOUT,  26000).
-define(NODE_MGR_SHUTDOWN_TIMEOUT,  24000).
-define(NODE_RUN_SHUTDOWN_TIMEOUT,  22000).

-define(NODE_SUP_ID(VNodeID),   {?NODE_SUP_TAG, VNodeID}).
-define(NODE_MGR_ID(VNodeID),   {?NODE_MGR_TAG, VNodeID}).
-define(WORK_SUP_ID(VNodeID),   {?WORK_SUP_TAG, VNodeID}).

