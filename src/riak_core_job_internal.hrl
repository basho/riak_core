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

-ifndef(riak_core_job_internal_included).
-define(riak_core_job_internal_included, true).

%% Macros shared among the processes in the async job management modules.
%% There is little, if anything, here that should be used outside those modules!

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
-include("riak_core_job.hrl").

-ifdef(namespaced_types).
-define(dict_t(K,V),    dict:dict(K, V)).
-define(orddict_t(K,V), orddict:orddict(K, V)).
-define(queue_t(T),     queue:queue(T)).
-else.
-define(dict_t(K,V),    dict()).
-define(orddict_t(K,V), orddict:orddict()).
-define(queue_t(T),     queue()).
-endif.

-ifdef(edoc).
-define(opaque, -opaque).
-else.
-define(opaque, -type).
-endif.

-define(JOBS_MGR_NAME,  riak_core_job_manager).
-define(JOBS_SVC_NAME,  riak_core_job_service).
-define(WORK_SUP_NAME,  riak_core_job_sup).

%% Shutdown timeouts should always be in ascending order as listed here.
-define(WORK_RUN_SHUTDOWN_TIMEOUT,  15000).
-define(JOBS_SUP_SHUTDOWN_TIMEOUT,  (?WORK_RUN_SHUTDOWN_TIMEOUT + 1500)).
-define(JOBS_SVC_SHUTDOWN_TIMEOUT,  (?JOBS_SUP_SHUTDOWN_TIMEOUT + 1500)).
-define(JOBS_MGR_SHUTDOWN_TIMEOUT,  (?JOBS_SVC_SHUTDOWN_TIMEOUT + 1500)).

%% I don't dare expose this globally, but in the job management stuff allow
%% something resembling sane if/else syntax.
-define(else,   'true').

%% This is just handy to have around because it's used a lot.
-define(is_non_neg_int(Term),   erlang:is_integer(Term) andalso Term >= 0).

%% The servers including this file implement gen_server, so this is a shortcut
%% to stuff an asynchronous message into their own message queue.
-define(cast(Msg),  gen_server:cast(erlang:self(), Msg)).

%% Specific terms that get mapped between the worker pool facade and the new
%% API services.
%% When we're sure they're stable, it may be worth moving (some of) these to
%% riak_core_job.hrl.
-define(JOB_ERR_CANCELED,       canceled).
-define(JOB_ERR_CRASHED,        crashed).
-define(JOB_ERR_KILLED,         killed).
-define(JOB_ERR_REJECTED,       job_rejected).
-define(JOB_ERR_QUEUE_OVERFLOW, job_queue_full).
-define(JOB_ERR_SHUTTING_DOWN,  service_shutdown).

-define(UNMATCHED_ARGS(Args),
    erlang:error({unmatched, {?MODULE, ?LINE}, Args})).

% Internal magic tokens - stay away!
% These aren't intended to offer any security, they're mainly to avoid
% mistakes by ensuring the processes we're talking to implement the behavior
% we expect. In some cases we're only dealing with a pid, so these give us the
% equivalent of a way to ask "are you who I think you are?"
-define(job_svc_cfg_token,      '$config$611007$').
-define(job_run_ctl_token,      '$control$19276$').

%% How long to wait for a 'confirm' response.
%% Note: DON'T encapsulate the 'confirm' exchange in a function - that would
%% kinda defeat its purpose.
-define(CONFIRM_MSG_TIMEOUT,    666).

-endif. % riak_core_job_internal_included
