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

-ifndef(riak_core_job_included).
-define(riak_core_job_included, true).

%% Jobs API Configuration keys:
%%
%%  {?JOB_SVC_ACCEPT_CLASS, [riak_core_job:class()]}
%%      Whitelist of job classes to accept.
%%      Overridden by ?JOB_SVC_ACCEPT_FUNC.
%%      Default: all classes are accepted.
%%
%%  {?JOB_SVC_ACCEPT_FUNC,  riak_core_job_service:cbfunc()}
%%      Callback invoked as if by:
%%          accept(Arg1 ... ArgN, scope_id(), job()) -> boolean()
%%      The arity of the supplied function must be 2 + length(Args).
%%      Default: as determined by ?JOB_SVC_ACCEPT_CLASS.
%%
%%  {?JOB_SVC_CONCUR_LIMIT, pos_integer()}
%%      Maximum number of jobs to execute concurrently per scope.
%%      Default: ?JOB_SVC_DEFAULT_CONCUR.
%%
%%  {?JOB_SVC_QUEUE_LIMIT,  non_neg_integer()}
%%      Maximum number of jobs to queue for future execution per scope.
%%      Default: ?JOB_SVC_CONCUR_LIMIT * ?JOB_SVC_DEFAULT_QUEMULT.
%%
-define(JOB_SVC_ACCEPT_CLASS,   'job_accept_class').
-define(JOB_SVC_ACCEPT_FUNC,    'job_accept_func').
-define(JOB_SVC_CONCUR_LIMIT,   'job_concurrency_limit').
-define(JOB_SVC_QUEUE_LIMIT,    'job_queue_limit').

%% Default per-scope job concurrency.
-define(JOB_SVC_DEFAULT_CONCUR, 10).

%% If per-scope maximum queue length is not configured, it defaults to the
%% concurrency value multiplied by this number.
-define(JOB_SVC_DEFAULT_QUEMULT, 3).

%% Values for riak_core_job:priority(), whose type is defined as
%%  ?JOB_PRIO_MIN .. ?JOB_PRIO_MAX
-define(JOB_PRIO_MIN,            0).
-define(JOB_PRIO_MAX,           99).
-define(JOB_PRIO_DEFAULT,       50).
-define(JOB_PRIO_LOW,           25).
-define(JOB_PRIO_HIGH,          75).

-endif. % riak_core_job_included
