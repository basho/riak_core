%% -------------------------------------------------------------------
%%
%% Copyright (c) 2010-2016 Basho Technologies, Inc.
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

-type sender_type() :: fsm | server | raw.
-type sender() :: {sender_type(), reference() | tuple(), pid()} |
                  %% TODO: Double-check that these special cases are kosher
                  {server, undefined, undefined} | % special case in
                                                   % riak_core_vnode_master.erl
                  {fsm, undefined, pid()} |        % special case in
                                                   % riak_kv_util:make_request/2.erl
                  ignore.
-type partition() :: chash:index_as_int().
-type vnode_req() :: term().
-type keyspaces() :: [{partition(), [partition()]}].

-record(riak_vnode_req_v1, {
          index :: partition(),
          sender=ignore :: sender(),
          request :: vnode_req()}).

-record(riak_coverage_req_v1, {
          index :: partition(),
          keyspaces :: keyspaces(),
          sender=ignore :: sender(),
          request :: vnode_req()}).

-record(riak_core_fold_req_v1, {
          foldfun :: fun(),
          acc0 :: term()}).
-record(riak_core_fold_req_v2, {
          foldfun :: fun(),
          acc0 :: term(),
          forwardable :: boolean(),
          opts = [] :: list()}).

-define(VNODE_REQ, #riak_vnode_req_v1).
-define(COVERAGE_REQ, #riak_coverage_req_v1).
-define(FOLD_REQ, #riak_core_fold_req_v2).

-type handoff_dest() :: {riak_core_handoff_manager:ho_type(), {partition(), node()}}.

%% Refer to the description in riak_core_job
-record(riak_core_work_v1, {
    init    :: riak_core_job:init_rec(),
    run     :: riak_core_job:run_rec(),
    fini    :: riak_core_job:fini_rec()
}).

-define(VNODE_JOB_PRIO_MIN,      0).
-define(VNODE_JOB_PRIO_MAX,     99).
-define(VNODE_JOB_PRIO_DEFAULT, 50).
-define(VNODE_JOB_PRIO_LOW,     25).
-define(VNODE_JOB_PRIO_HIGH,    75).

-define(VNODE_JOB,              #riak_core_job_v1).

%% Refer to the description in riak_core_job.
%% Fields without defaults are REQUIRED, even though the language can't
%% enforce that. A record where required fields are 'undefined' is malformed.
-record(riak_core_job_v1, {
    %% What type or class of work this job performs.
    type        = 'anon'        :: riak_core_job:unspec() | riak_core_job:what(),

    from        = 'ignore'      :: sender(),

    %% Currently unimplemented, but here so we don't need to define a new
    %% version of the record when (or if) it is ...
    priority    = ?VNODE_JOB_PRIO_DEFAULT   :: riak_core_job:priority(),

    %% If supplied, the 'client_id' is presumed to mean something to the
    %% originator of the work, and is included as an element in the 'global_id'
    global_id                   :: riak_core_job:id(),
    client_id   = 'anon'        :: riak_core_job:unspec() | riak_core_job:id(),
    internal_id = 'anon'        :: riak_core_job:unspec() | riak_core_job:id(),

    %% Timestamps when relevant state transitions occur
    %% Maybe these should be stats?
    init_time   = 0             :: riak_core_job:time(),
    queue_time  = 0             :: riak_core_job:time(),
    start_time  = 0             :: riak_core_job:time(),

    %% The work to perform. Note that the type riak_core_job:work() type exactly
    %% matches the type here, but can't be used yet when compiling that module.
    work                        :: #riak_core_work_v1{} | riak_core_job:legacy(),

    %% Optional callbacks to be invoked when the job is killed or finished,
    %% respectively.
    kill_cb     = 'undefined'   :: riak_core_job:kill_cb(),
    done_cb     = 'undefined'   :: riak_core_job:done_cb(),

    %% Maybe keep some statistics around?
    stats       = []            :: [riak_core_job:stat()]
}).

