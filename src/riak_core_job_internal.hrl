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
-type scope_type()  ::  atom().
-type scope_index() ::  partition().
-type scope_id()    ::  {scope_type(), scope_index()}.

%% These types are used internally between cooperating modules.
-define(SCOPE_SUP_TAG,  'scope_sup').
-define(SCOPE_SVC_TAG,  'scope_svc').
-define(WORK_SUP_TAG,   'work_sup').

-type proc_type()   ::  ?SCOPE_SUP_TAG | ?SCOPE_SVC_TAG | ?WORK_SUP_TAG .
-type proc_id()     ::  {proc_type(), scope_id()}.

-type scope_sup_id()::  {?SCOPE_SUP_TAG,  scope_id()}.
-type scope_svc_id()::  {?SCOPE_SVC_TAG,  scope_id()}.
-type work_sup_id() ::  {?WORK_SUP_TAG,   scope_id()}.

%% Allow a pretty long time for a scope to start. It shouldn't take anywhere
%% near this long unless the system is really bogged down.
-define(SCOPE_SVC_STARTUP_TIMEOUT,  12000).

%% Shutdown timeouts should always be in descending order as listed here.
-define(JOBS_MGR_SHUTDOWN_TIMEOUT,  28000).
-define(SCOPE_SUP_SHUTDOWN_TIMEOUT, 26000).
-define(SCOPE_SVC_SHUTDOWN_TIMEOUT, 24000).
-define(WORK_RUN_SHUTDOWN_TIMEOUT,  22000).

%% How long stop_scope/1 waits for the scope to shut down.
-define(STOP_SCOPE_TIMEOUT,     (?SCOPE_SUP_SHUTDOWN_TIMEOUT + 1000)).

-define(SCOPE_SUP_ID(ScopeID),  {?SCOPE_SUP_TAG,  ScopeID}).
-define(SCOPE_SVC_ID(ScopeID),  {?SCOPE_SVC_TAG,  ScopeID}).
-define(WORK_SUP_ID(ScopeID),   {?WORK_SUP_TAG,   ScopeID}).

%% I don't dare expose this globally, but in the job management stuff allow
%% something resembling sane if/else syntax.
-define(else,   'true').

%% The servers including this file all implement gen_server, so this is a
%% shortcut to stuff an asynchronous message into their own message queue.
-define(cast(Msg),  gen_server:cast(erlang:self(), Msg)).

%% Specific terms that get mapped between the worker pool facade and the new
%% API services.
%% When we're sure they're stable, it may be worth moving (some of) these to
%% riak_core_vnode.hrl.
-define(JOB_ERR_CANCELED,       'canceled').
-define(JOB_ERR_CRASHED,        'crashed').
-define(JOB_ERR_KILLED,         'killed').
-define(JOB_ERR_REJECTED,       'job_rejected').
-define(JOB_ERR_QUEUE_OVERFLOW, 'job_queue_full').
-define(JOB_ERR_SHUTTING_DOWN,  'scope_shutdown').
