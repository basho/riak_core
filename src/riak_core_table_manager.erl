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
%% ETS tables. Creates tables passed in and then gives them to the first
%% process to claim them, setting itself as the heir in case the claiming
%% process exits.
%%
%% For theory of operation, please see the web page:
%% http://steve.vinoski.net/blog/2011/03/23/dont-lose-your-ets-tables/

-module(riak_core_table_manager).

-behaviour(gen_server).

-ifdef(PULSE).
-include_lib("pulse/include/pulse.hrl").
-include_lib("pulse_otp/include/pulse_otp.hrl").
-compile(export_all).
-compile({parse_transform, pulse_instrument}).
-endif.

%% API
-export([start_link/1,
         claim_table/1,
         ensure_heired/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-type ets_option() :: term().
-type table_name() :: atom().
-type table_spec() :: {table_name(), [ets_option()]}.
-type table_data() :: {ets:tid(), table_name()}.

%-record(state, {tables}).
-record(table_state, {
    table_id,
    claimed_by :: 'undefined' | pid(),
    second_claimant :: 'undefined' | pid()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server possibly creating the given ets tables. In the case
%% where a table was unclaimed, it is lost. If a table was claimed, control
%% was given to the claiming process. On start, that process will recieve
%% a message from the table manager for each table it has claimed that
%% the table manager should be heir to:
%%
%%    {riak_core_table_manager, restarted, TableData}
%%
%% The `TableData' should be passed in to the {@link ensure_heired/1}
%% function.
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link([table_spec()]) -> {ok, Pid::pid()} | ignore | {error, Error::term()}.
start_link(TableSpecs) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [TableSpecs], []).


%%--------------------------------------------------------------------
%% @doc
%% Gives the named table away to the caller. If a second process attempts
%% to claim the table, that process will be blocked until the first process
%% dies. The ets table is then given to the second process. This is to
%% avoid a race condition when a claiming process is restarted by a
%% supervisor possibly sooner than the ets-transfer message reaches the
%% table manager.
%%
%% A third process attempting to claim is simply denied.
%%
%% @end
%%--------------------------------------------------------------------
-spec claim_table(atom()) -> {ok, {ets:tid(), atom()}} | {'error', 'already_claimed'}.
claim_table(TableName) ->
    case gen_server:call(?SERVER, {claim_table, TableName}, infinity) of
        ok ->
            {ok, {TableId, Name}} = Return = wait_for_table_transfer(TableName),
            ets:setopts(TableId, [{heir, whereis(?SERVER), Name}]),
            Return;
        Else ->
            Else
    end.

wait_for_table_transfer(Name) ->
    receive
        {'ETS-TRANSFER', TableId, _FromPid, Name} ->
            {ok, {TableId, Name}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sets the heir of the given table (referenced in the TableData) is the
%% table manager. If the table manager is restarted, the table manager
%% attempts to recreate all the tables passed into {@link start_link/1}.
%% If any of those tables already exist, the owner process of that table
%% is sent a message:
%%
%%     {riak_core_table_manager, restarted, TableData}
%%
%% The `TableData' should be passed into this function by the owner,which
%% will properly set the heir to the new table_manager process.
%%
%% @end
%%--------------------------------------------------------------------
-spec ensure_heired(table_data()) -> 'ok' | {'error', 'no_proc'}.
ensure_heired({TableId, TableName}) ->
    MgrPid = whereis(?MODULE),
    ensure_heired(TableId, TableName, MgrPid).

ensure_heired(_TableId, _TableName, undefined) ->
    {error, no_proc};

ensure_heired(TableId, TableName, MgrPid) ->
    case ets:info(TableId, heir) of
        MgrPid ->
            ok;
        _NotPid ->
            ets:setopts(TableId, [{heir, MgrPid, TableName}]),
            ok
    end.

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
    Self = self(),
    Tables = lists:foldl(fun({Name, Opts}, Acc) ->
        Tid = maybe_create_table(Name, Opts),
        Claimed = case ets:info(Tid, owner) of
            Self -> undefined;
            Owner ->
                alert_to_restart(Owner, Tid, Name),
                Owner
        end,
        dict:store(Name, #table_state{table_id = Tid, claimed_by = Claimed}, Acc)
    end, dict:new(), TableSpecs),
    {ok, Tables}.

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
handle_call({claim_table, TableName}, From, State) ->
    %% The user process is (re-)claiming the table. Give it away.
    %% We remain the heir in case the user process exits.
    case maybe_claim_table(TableName, From, State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        Else ->
            {reply, Else, State}
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
handle_info({'ETS-TRANSFER', _TableId, FromPid, TableName}, State) ->
    %% The table's user process exited and transferred the table back to us.
    lager:debug("Table user process ~p exited, ~p received table ~p", [FromPid, self(), TableName]),
    State2 = maybe_fallback_claim(TableName, State),
    {noreply, State2};

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

maybe_create_table(TableName, TableProps) ->
    try create_table(TableName, TableProps) of
        TableId ->
            TableId
    catch
        error:badarg ->
            recover_table_id(TableName)
    end.

recover_table_id(TableName) ->
    AllTables = ets:all(),
    case lists:member(TableName, AllTables) of
        false ->
            IdsOnly = lists:filter(fun(E) -> not is_atom(E) end, AllTables),
            recover_table_id_deep(TableName, IdsOnly);
        true ->
            TableName
    end.

recover_table_id_deep(TableName, []) ->
    throw({no_existant_table_found, TableName});

recover_table_id_deep(TableName, [Id | Rest]) ->
    case ets:info(Id, name) of
        TableName ->
            Id;
        _ ->
            recover_table_id_deep(TableName, Rest)
    end.

%% Create the initial table based on a table name and properties.
%% The table will eventually be given away by claim_table, but we
%% want to remain the heir in case the claimer crashes.
create_table(TableName, TableProps) ->
    TableId = ets:new(TableName, TableProps),
    ets:setopts(TableId, [{heir, self(), undefined}]),
    TableId.

maybe_claim_table(TableName, {Pid, _Tag} = TryingToClaim, ClaimedDict) ->
    case dict:find(TableName, ClaimedDict) of
        error ->
            {error, no_such_table};
        {ok, #table_state{claimed_by = undefined} = TState} ->
            lager:debug("Giving away table ~p (~p) to ~p", [TableName, TState, TryingToClaim]),
            ets:give_away(TState#table_state.table_id, Pid, TableName),
            TState2 = TState#table_state{claimed_by = Pid},
            {ok, dict:store(TableName, TState2, ClaimedDict)};
        {ok, #table_state{second_claimant = undefined} = TState} ->
            lager:debug("Setting up secondary claimant for ~p", [TableName]),
            TState2 = TState#table_state{second_claimant = TryingToClaim},
            {ok, dict:store(TableName, TState2, ClaimedDict)};
        {ok, _} ->
            lager:debug("Table ~p already has 2 claimants", [TableName]),
            {error, already_claimed}
    end.

maybe_fallback_claim(TableName, ClaimedDict) ->
    case dict:find(TableName, ClaimedDict) of
        error ->
            ClaimedDict;
        {ok, #table_state{second_claimant = undefined} = TState} ->
            TState2 = TState#table_state{claimed_by = undefined},
            dict:store(TableName, TState2, ClaimedDict);
        {ok, TState} ->
            ReplyTo = TState#table_state.second_claimant,
            TState2 = TState#table_state{claimed_by = undefined, second_claimant = undefined},
            ClaimedDict1 = dict:store(TableName, TState2, ClaimedDict),
            case maybe_claim_table(TableName, ReplyTo, ClaimedDict1) of
                {ok, OutDict} ->
                    gen_server:reply(ReplyTo, ok),
                    OutDict;
                Else ->
                    lager:warning("could not update claim to second claimantfor tstate ~p due to ~p", [TState, Else]),
                    ClaimedDict1
            end
    end.

alert_to_restart(Owner, TableId, TableName) ->
    Owner ! {?MODULE, restarted, {TableId, TableName}}.

