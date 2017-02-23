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

-ifndef(riak_core_job_included).
-define(riak_core_job_included, true).

%%
%% Jobs API Configuration keys.
%%
%% Refer to riak_core_job_manager module documentation for usage.
%% Keep these in sync with the above module documentation, where they're
%% hard-coded for EDoc.
%%
-define(JOB_SVC_CONCUR_LIMIT,   job_concurrency_limit).
-define(JOB_SVC_HIST_LIMIT,     job_history_limit).
-define(JOB_SVC_IDLE_MAX,       job_idle_max_limit).
-define(JOB_SVC_IDLE_MIN,       job_idle_min_limit).
-define(JOB_SVC_QUEUE_LIMIT,    job_queue_limit).
-define(JOB_SVC_RECYCLE,        job_idle_recycle).

%%
%% Default configuration values.
%%
%% Notes:
%%
%%  It would be a mistake to set JOB_SVC_DEFAULT_CONCUR to a 'concur'
%%  multiplier. Doing so *will* resolve to a usable value, but it's unlikely
%%  to be what you want.
%%
%%  The _IDLE_ configuration keys that don't have defaults defined here do, in
%%  fact, have defaults, but they're calculated in a manner that can't be
%%  defined using the configuration syntax.
%%
%% Refer to the riak_core_job_manager module documentation for details.
%% Keep these in sync with the above module documentation, where they're
%% hard-coded for EDoc.
%%
-define(JOB_SVC_DEFAULT_CONCUR,     {scheds, 6}).
-define(JOB_SVC_DEFAULT_QUEUE,      {concur, 3}).
-define(JOB_SVC_DEFAULT_HIST,       {concur, 1}).
-define(JOB_SVC_DEFAULT_RECYCLE,    false).

%%
%% Values for riak_core_job:priority(), whose type is defined as
%%  ?JOB_PRIO_MIN .. ?JOB_PRIO_MAX
%%
-define(JOB_PRIO_MIN,            0).
-define(JOB_PRIO_MAX,           99).
-define(JOB_PRIO_DEFAULT,       50).
-define(JOB_PRIO_LOW,           25).
-define(JOB_PRIO_HIGH,          75).

%%
%% A reasonable guard for Job Global IDs.
%% Obviously, this is tightly coupled to the implementation, but there aren't
%% many options - erlang:is_record/2 needs to see the full record definition,
%% which we're not going to put out in public, but erlang:is_record/3 can get
%% by with the name and size.
%%
-define(is_job_gid(Term),   erlang:is_record(Term, rcj_gid_v1, 4)).

-endif. % riak_core_job_included
