%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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

-module(riak_core_bg_manager_sup).
-behaviour(supervisor).

%% beahvior functions
-export([start_link/0,
         init/1
        ]).

-define(CHILD(I,Type), {I,{I,start_link,[]},permanent,5000,Type,[I]}).

%% begins the supervisor, init/1 will be called
start_link () ->
    supervisor:start_link({local,?MODULE},?MODULE,[]).

%% Supervisor of all background manager processes
%%
%% Lock manager (currently in bg_manager) and Token manager both use ETS
%% tables to manage persistent state. The table_manager is started from
%% our parent supervisor (riak_core_sup) and creates the tables used by
%% background managers. So, riak_core_table_manager needs to be started
%% before this supervisor.

%% @private
init ([]) ->
    {ok,{{one_for_all,10,10},
         [?CHILD(riak_core_bg_manager, worker),
          ?CHILD(riak_core_token_manager, worker)]}}.
