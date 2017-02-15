%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Basho Technologies, Inc.  All Rights Reserved.
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

%% @see riak_core_info_service
-module(riak_core_info_service_sup).

-behaviour(supervisor).

-export([start_link/0,
         start_service/4]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_service(Registration, Shutdown, Source, Handler) ->
    supervisor:start_child(?SERVER, [Registration, Shutdown, Source, Handler]).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([]) ->
    ChildSpec = child_spec(),
    SupFlags = {simple_one_for_one, 5, 1000},
    {ok, {SupFlags, [ChildSpec]}}.

child_spec() ->
    {na,
     {riak_core_info_service_process, start_link, []},
     permanent, 2000, worker, [riak_core_info_service_process]}.
