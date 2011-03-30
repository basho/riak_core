%% -------------------------------------------------------------------
%%
%% riak_core_eventhandler_sup: supervise minder processes for gen_event handlers
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc supervise riak_core_eventhandler_guard processes
-module(riak_core_eventhandler_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).
-export([start_handler_guard/3]).

start_handler_guard(HandlerMod, Handler, Args) ->
    case supervisor:start_child(?MODULE, handler_spec(HandlerMod, Handler, Args)) of
        {ok, _Pid} -> ok;
        Other -> Other
    end.

handler_spec(HandlerMod, Handler, Args) ->
    {{HandlerMod, Handler},
     {riak_core_eventhandler_guard, start_link, [HandlerMod, Handler, Args]},
     permanent, 5000, worker, [riak_core_eventhandler_guard]}.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @private
init([]) ->
    {ok, {{one_for_one, 10, 10}, []}}.

