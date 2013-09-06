%% -------------------------------------------------------------------
%%
%% riak_core_table_manager: ETS table ownership and crash protection
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc A gen_server process that creates and serves as heir to a
%% ETS table; and coordinates with matching user processes. If a user
%% process exits, this server will inherit the ETS table and hand it
%% back to the user process when it restarts.
%%
%% For theory of operation, please see the web page:
%% http://steve.vinoski.net/blog/2011/03/23/dont-lose-your-ets-tables/

-module(riak_core_table_manager).

-behaviour(gen_server).

%% API
-export([start_link/1,
         claim_table/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {tables}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link([term()]) -> {ok, Pid::pid()} | ignore | {error, Error::term()}.
start_link(TableSpecs) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [TableSpecs], []).


%%--------------------------------------------------------------------
%% @doc
%% Gives the registration table away to the caller, which should be
%% the registrar process.
%%
%% @end
%%--------------------------------------------------------------------
-spec claim_table(atom()) -> ok.
claim_table(TableName) ->
    gen_server:call(?SERVER, {claim_table, TableName}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec init([term()]) -> {ok, undefined}.
%% Tables :: [{TableName, [props]}]
%% Table specs are provided by the process that creates this table manager,
%% presumably a supervisor such as riak_core_sup.
init([TableSpecs]) ->
    lager:debug("Table Manager starting up with tables: ~p", [TableSpecs]),
    Tables = lists:foldl(fun(Spec, TT) -> create_table(Spec, TT) end, [], TableSpecs),
    {ok, #state{tables=Tables}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Msg::term(), From::{pid(), term()}, State::term()) ->
                         {reply, Reply::term(), State::term()} |
                         {noreply, State::term()}.
%% TableName :: atom()
handle_call({claim_table, TableName}, {Pid, _Tag}, State) ->
    %% The user process is (re-)claiming the table. Give it away.
    %% We remain the heir in case the user process exits.
    case lookup_table(TableName, State) of
        undefined ->
            %% Table does not exist, which is madness.
            {reply, {undefined_table, TableName}, State};
        TableId ->
            lager:debug("Giving away table ~p (~p) to ~p", [TableName, TableId, Pid]),
            ets:give_away(TableId, Pid, undefined),
            Reply = ok,
            {reply, Reply, State}
    end;

handle_call(_Msg, _From, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(term(), term()) -> {noreply, State::term()}.
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({'ETS-TRANSFER', TableId, FromPid, _HeirData}, State) ->
    %% The table's user process exited and transferred the table back to us.
    lager:debug("Table user process ~p exited, ~p received table ~p", [FromPid, self(), TableId]),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @end
%%--------------------------------------------------------------------
-spec terminate(term(), term()) -> ok.
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @end
%%--------------------------------------------------------------------
-spec code_change(term(), term(), term()) -> {ok, term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Create the initial table based on a table name and properties.
%% The table will eventually be given away by claim_table, but we
%% want to remain the heir in case the claimer crashes.
create_table({TableName, TableProps}, Tables) ->
    TableId = ets:new(TableName, TableProps),
    ets:setopts(TableId, [{heir, self(), undefined}]),
    [{TableName, TableId} | Tables].

-spec lookup_table(TableName::atom(), State::term()) -> ets:tid() | undefined.
lookup_table(TableName, #state{tables=Tables}) ->
    proplists:get_value(TableName, Tables).
