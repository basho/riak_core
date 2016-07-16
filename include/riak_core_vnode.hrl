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
