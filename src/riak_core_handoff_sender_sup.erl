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
-export([start_sender/5]).

-include("riak_core_handoff.hrl").
-define(CHILD(I,Type), {I,{I,start_link,[]},temporary,brutal_kill,Type,[I]}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    supervisor:start_link({local,?MODULE},?MODULE,[]).

%% @doc Start the handoff process for the module (`Module'), partition
%%      (`Partition'), and vnode (`VNode') from the local node to the
%%      target node (`TargetNode') with options `Opts'.
%%
%%      Options:
%%        * src_partition - required. the integer index of the source vnode
%%        * target_partition - required. the integer index of the target vnode
%%        * filter - optional. an arity one function that takes the key and returns
%%                   a boolean. If false, the key is not sent
%%        * unsent_fun - optional. an arity 2 function that takes a key that was not sent
%%                       (based on filter) and an accumulator. This function is called
%%                       for each unsent key.
%%        * unsent_acc0 - optional. The intial accumulator value passed to unsent_fun
%%                        for the first unsent key
-spec start_sender(ho_type(), atom(), term(), pid(), [{atom(), term()}]) -> {ok, pid()}.
start_sender(Type, Module, TargetNode, VNode, Opts) ->
    supervisor:start_child(?MODULE, [TargetNode, Module, {Type, Opts}, VNode]).

%%%===================================================================
%%% Callbacks
%%%===================================================================

%% @private
init ([]) ->
    {ok,{{simple_one_for_one,10,10},
         [?CHILD(riak_core_handoff_sender,worker)
         ]}}.
