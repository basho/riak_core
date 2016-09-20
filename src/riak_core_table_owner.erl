%% -------------------------------------------------------------------
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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
%%
%% @doc <p>ETS tables, when created with the `ets:new/2' function, have a single
%% owning process. If the owning process exits, then any ETS tables associated
%% with that process are deleted. The purpose of the `riak_core_table_owner'
%% module (which is a gen_server process) is to serve as the owning process for
%% ETS tables that do not otherwise have an obvious owning process. For example,
%% the `riak_core_throttle' module uses an ETS table for maintaining its state,
%% but it is not itself a process and therefore the owning process for it ETS
%% table is not clear. In this case, `riak_core_table_owner' can be used to
%% create and own the ETS table on its behalf.</p>
%%
%% <p>It is important that this process never crashes, as that would lead to
%% loss of data. Therefore, a defensive approach is taken and any calls to
%% external modules are protected with the try/catch mechanism.</p>
%%
%% <p>Note that this first iteration does not provide any API functions for
%% reading or writing data in ETS tables and therefore is appropriate only for
%% named <em>public</em> ETS tables. In order to be more broadly useful, future
%% enhancements to this module should include API functions for efficiently
%% reading and writing ETS data, preferably without going through a gen_server
%% call.</p>
-module(riak_core_table_owner).

-behaviour(gen_server).

%% API
-export([start_link/0,
         create_table/2,
         maybe_create_table/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% Unfortunately the `ets' module does not define a type for options, but we
%% can at least insist on a list.
-type ets_options() :: list().
-type ets_table() :: ets:tab().
-type create_table_result() :: {ok, ets_table()} | {error, Reason::term()}.

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, Reason::term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, dict:new(), []).

%% Creates a new ETS table with the given `Name' and `Options'.
%% Since the table will be owned by the `riak_core_table_owner' process, it
%% should be created with the `public' option so that other processes can read
%% and write data in the table.
-spec create_table(Name::atom(), Options::ets_options()) -> create_table_result().
create_table(Name, Options) ->
    gen_server:call(?MODULE, {create_table, Name, Options}).

%% Creates a new ETS table with the given `Name' and `Options', if and only if
%% it was not already created previously.
%% Since the table will be owned by the `riak_core_table_owner' process, it
%% should be created with the `public' option so that other processes can read
%% and write data in the table.
-spec maybe_create_table(Name::atom(), Options::ets_options()) -> create_table_result().
maybe_create_table(Name, Options) ->
    gen_server:call(?MODULE, {maybe_create_table, Name, Options}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(State) ->
    {ok, State}.

handle_call({create_table, Name, Options}, _From, State) ->
    do_create_table(Name, Options, State);
handle_call({maybe_create_table, Name, Options}, _From, State) ->
    case dict:find(Name, State) of
        {ok, Table} ->
            {reply, {ok, Table}, State};
        error ->
            do_create_table(Name, Options, State)
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_create_table(Name, Options, State) ->
    try
        Table = ets:new(Name, Options),
        {reply, {ok, Table}, dict:store(Name, Table, State)}
    catch
        Error ->
            {reply, {error, Error}, State}
    end.
