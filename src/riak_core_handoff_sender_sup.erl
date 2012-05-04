%% -------------------------------------------------------------------
%%
%% Copyright (c) 2011-2012 Basho Technologies, Inc.
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

-module(riak_core_handoff_sender_sup).
-behaviour(supervisor).

%% callbacks
-export([start_link/0,
         init/1
        ]).

%% API
-export([start_handoff/5,
         start_repair/6
        ]).

-include("riak_core_handoff.hrl").
-define(CHILD(I,Type), {I,{I,start_link,[]},temporary,brutal_kill,Type,[I]}).

%%%===================================================================
%%% API
%%%===================================================================

start_link () ->
    supervisor:start_link({local,?MODULE},?MODULE,[]).

%% @doc Start the handoff process for the module (`Module'), partition
%%      (`Partition'), and vnode (`VNode') from the local node to the
%%      target node (`TargetNode').
-spec start_handoff(ho_type(), module(), any(), pid(), node()) -> {ok, pid()}.
start_handoff(Type, Module, Partition, VNode, TargetNode) ->
    Opts = [{src_partition, Partition},
            {target_partition, Partition},
            {filter, none}],
    supervisor:start_child(?MODULE, [TargetNode, Module, {Type, Opts}, VNode]).

%% @doc Start the repair process for the given module (`Module') from
%%      the local partition (`SrcPartition') to the remote partition
%%      (`TargetNode' and `TargetPartition') using the filter
%%      (`Filter').
-spec start_repair(module(), any(), any(), pid(), node(), predicate() | none) ->
                          {ok, pid()}.
start_repair(Module, SrcPartition, TargetPartition, VNode, TargetNode, Filter) ->
    Type = repair,
    Opts = [{src_partition, SrcPartition},
            {target_partition, TargetPartition},
            {filter, Filter}],
    supervisor:start_child(?MODULE, [TargetNode, Module, {Type, Opts}, VNode]).


%%%===================================================================
%%% Callbacks
%%%===================================================================

%% @private
init ([]) ->
    {ok,{{simple_one_for_one,10,10},
         [?CHILD(riak_core_handoff_sender,worker)
         ]}}.
