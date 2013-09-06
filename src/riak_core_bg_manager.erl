%% -------------------------------------------------------------------
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
-module(riak_core_bg_manager).

-behaviour(gen_server).

%% API
-export([
         %% Universal
         start_link/0,
         enable/0,
         disable/0,
         %% Locks
         %% TODO: refactor the lock implementation to another module ala tokens
         get_lock/1,
         get_lock/2,
         get_lock/3,
         lock_count/0,
         lock_count/1,
         lock_types/0,
         all_locks/0,
         query_locks/1,
         enable_locks/0,
         enable_locks/1,
         disable_locks/0,
         disable_locks/1,
         disable_locks/2,
         concurrency_limit/1,
         set_concurrency_limit/2,
         set_concurrency_limit/3,
         concurrency_limit_reached/1,
         %% Tokens, all proxied to riak_core_token_manager
         set_token_rate/2,
         token_rate/1,
         enable_tokens/0,
         enable_tokens/1,
         disable_tokens/0,
         disable_tokens/1,
         get_token/1,
         get_token/2,
         get_token/3,
         get_token_sync/1,
         get_token_sync/2,
         get_token_sync/3,
         token_types/0,
         tokens_given/0,
         tokens_given/1,
         tokens_waiting/0,
         tokens_waiting/1
        ]).

-include("riak_core_token_manager.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {table_id:: ets:tid(),            %% TableID of ?LM_ETS_TABLE
                enabled :: boolean()}).

-record(lock_info, {concurrency_limit :: non_neg_integer(),
                    enabled           :: boolean()}).

-define(SERVER, ?MODULE).
-define(DEFAULT_CONCURRENCY, 0). %% DO NOT CHANGE. DEFAULT SET TO 0 TO ENFORCE "REGISTRATION"
-define(limit(X), (X)#lock_info.concurrency_limit).
-define(enabled(X), (X)#lock_info.enabled).
-define(DEFAULT_LOCK_INFO, #lock_info{enabled=true, concurrency_limit=?DEFAULT_CONCURRENCY}).
-define(LM_ETS_TABLE, lock_mgr_table).   %% name of private lock manager ETS table
-define(LM_ETS_OPTS, [private, bag]).    %% creation time properties of lock manager ETS table

-type concurrency_limit() :: non_neg_integer() | infinity.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Starts the server
-spec start_link() -> {ok, pid()} | ignore | {error, term}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Enable handing out of all background locks and tokens
-spec enable() -> ok.
enable() ->
    enable_tokens(),
    enable_locks().

%% @doc Disable handing out of all background locks and tokens
-spec disable() -> ok.
disable() ->
    disable_tokens(),
    disable_locks().

%%% Token API proxies to the token manager

%% @doc Set the refill rate of tokens.
-spec set_token_rate(any(), riak_core_token_manager:tm_rate()) -> riak_core_token_manager:tm_rate().
set_token_rate(Type, {Period, Count}) ->
    riak_core_token_manager:set_token_rate(Type, {Period, Count}).

-spec token_rate(any()) -> riak_core_token_manager:tm_rate().
token_rate(Type) ->
    riak_core_token_manager:token_rate(Type).

%% @doc Asynchronously get a token of kind Type. returns "max_tokens" if empty
-spec get_token(any()) -> ok | max_tokens.
get_token(Type) ->
    get_token(Type, self()).

%% @doc Asynchronously get a token of kind Type.
%%      Associate token with provided pid or metadata. If metadata
%%      is provided the lock is associated with the calling process.
%%      Returns "max_tokens" if empty.
-spec get_token(any(), pid() | [{atom(), any()}]) -> ok | max_tokens.
get_token(Type, Pid) when is_pid(Pid) ->
    get_token(Type, Pid, []);
get_token(Type, Meta) ->
    get_token(Type, self(), Meta).

-spec get_token(any(), pid(), [{atom(), any()}]) -> ok | max_concurrency.
get_token(Type, Pid, Meta) ->
    riak_core_token_manager:get_token_async(Type, Pid, Meta).

%% @doc Synchronously get a token of type Type. returns "max_tokens" if empty
-spec get_token_sync(any()) -> ok | max_tokens.
get_token_sync(Type) ->
    get_token_sync(Type, self()).

%% @doc Synchronously get a token of kind Type.
%%      Associate token with provided pid or metadata. If metadata
%%      is provided the lock is associated with the calling process.
%%      Returns "max_tokens" if empty.
-spec get_token_sync(any(), pid() | [{atom(), any()}]) -> ok | max_tokens.
get_token_sync(Type, Pid) when is_pid(Pid) ->
    get_token_sync(Type, Pid, []);
get_token_sync(Type, Meta) ->
    get_token(Type, self(), Meta).

-spec get_token_sync(any(), pid(), [{atom(), any()}]) -> ok | max_concurrency.
get_token_sync(Type, Pid, Meta) ->
    riak_core_token_manager:get_token_sync(Type, Pid, Meta).

token_types() ->
    riak_core_token_manager:token_types().

tokens_given() ->
    riak_core_token_manager:tokens_given().

tokens_given(Type) ->
    riak_core_token_manager:tokens_given(Type).

tokens_waiting() ->
    riak_core_token_manager:tokens_waiting().

tokens_waiting(Type) ->
    riak_core_token_manager:tokens_waiting(Type).

%% @doc Enable handing out of any tokens
-spec enable_tokens() -> ok.
enable_tokens() ->
    riak_core_token_manager:enable().

%% @doc Disable handing out of any tokens
-spec disable_tokens() -> ok.
disable_tokens() ->
    riak_core_token_manager:disable().

%% @doc Enable handing out of tokens of the given type.
-spec enable_tokens(any()) -> ok.
enable_tokens(Type) ->
    riak_core_token_manager:enable(Type).


%% @doc same as `disable(Type, false)'
-spec disable_tokens(any()) -> ok.
disable_tokens(Type) ->
    riak_core_token_manager:enable(Type).

%%% Locks

%% @doc Acquire a concurrency lock of the given type, if available,
%%      and associate the lock with the calling process.
-spec get_lock(any()) -> ok | max_concurrency.
get_lock(Type) ->
    get_lock(Type, self()).

%% @doc Acquire a concurrency lock of the given type, if available,
%%      and associate the lock with the provided pid or metadata. If metadata
%%      is provided the lock is associated with the calling process
-spec get_lock(any(), pid() | [{atom(), any()}]) -> ok | max_concurrency.
get_lock(Type, Pid) when is_pid(Pid) ->
    get_lock(Type, Pid, []);
get_lock(Type, Opts) when is_list(Opts)->
    get_lock(Type, self(), Opts).

%% @doc Acquire a concurrency lock of the given type, if available,
%%      and associate the lock with the provided pid and metadata.
-spec get_lock(any(), pid(), [{atom(), any()}]) -> ok | max_concurrency.
get_lock(Type, Pid, Info) ->
    gen_server:call(?MODULE, {get_lock, Type, Pid, Info}, infinity).

%% @doc Return the current concurrency count for all lock types
-spec lock_count() -> integer().
lock_count() ->
    gen_server:call(?MODULE, lock_count, infinity).

%% @doc Return the current concurrency count of the given lock type.
-spec lock_count(any()) -> integer().
lock_count(Type) ->
    gen_server:call(?MODULE, {lock_count, Type}, infinity).

%% @doc Return list of lock types and associated info. To be returned in this list
%%      a lock type must have had its concurrency set or have been enabled/disabled.
-spec lock_types() -> [{any(), boolean(), concurrency_limit()}].
lock_types() ->
    gen_server:call(?MODULE, lock_types, infinity).

%% @doc Returns all currently held locks
-spec all_locks() -> [{any(), pid(), reference(), [{atom(), any()}]}].
all_locks() ->
    query_locks([]).

%% @doc Queries the currently held locks returning any locks that match the given criteria.
%%      If no criteria is present then all held locks are returned. The query is a proplist of
%%      2-tuples. The keys 'pid' and 'type', have special meaning. If they are keys in the
%%      query proplists, only locks matching the corresponding pid or lock type are
%%      returned. All other pairs are compared for equality against the proplist passed as the third
%%      argument to get_lock/3. The returned value is a list of 4-tuples. The first element
%%      is the lock type; the second, the pid holding the lock; the third, the lock refernce,
%%      and the fourth is the metadata passed to get_lock/3.
-spec query_locks([{atom(), any()}]) -> [{any(), pid(), reference(), [{atom(), any()}]}].
query_locks(Query) ->
    gen_server:call(?MODULE, {query_locks, Query}, infinity).

%% @doc Enable handing out of any locks
-spec enable_locks() -> ok.
enable_locks() ->
    gen_server:cast(?MODULE, enable).

%% @doc Disable handing out of any locks
-spec disable_locks() -> ok.
disable_locks() ->
    gen_server:cast(?MODULE, disable).

%% @doc Enable handing out of locks of the given type.
-spec enable_locks(any()) -> ok.
enable_locks(Type) ->
    gen_server:cast(?MODULE, {enable, Type}).

%% @doc same as `disable_locks(Type, false)'
-spec disable_locks(any()) -> ok.
disable_locks(Type) ->
    disable_locks(Type, false).

%% @doc Disable handing out of locks of the given type. If `Kill' is `true' any processes
%%      holding locks for the given type will be killed with reaseon `max_concurrency'
-spec disable_locks(any(), boolean()) -> ok.
disable_locks(Type, Kill) ->
    gen_server:cast(?MODULE, {disable, Type, Kill}).

%% @doc Get the current maximum concurrency for the given lock type.
-spec concurrency_limit(any()) -> concurrency_limit().
concurrency_limit(Type) ->
    gen_server:call(?MODULE, {concurrency_limit, Type}, infinity).

%% @doc same as `set_concurrency_limit(Type, Limit, false)'
-spec set_concurrency_limit(any(), concurrency_limit()) -> concurrency_limit().
set_concurrency_limit(Type, Limit) ->
    set_concurrency_limit(Type, Limit, false).

%% @doc Set a new maximum concurrency for the given lock type and return
%%      the previous maximum or default. If more locks are held than the new
%%      limit how they are handled depends on the value of `Kill'. If `true',
%%      then the extra locks are released by killing processes with reason `max_concurrency'.
%%      If `false', then the processes holding the extra locks are aloud to do so until they
%%      are released.
-spec set_concurrency_limit(any(), concurrency_limit(), boolean()) -> concurrency_limit().
set_concurrency_limit(Type, Limit, Kill) ->
    gen_server:call(?MODULE, {set_concurrency_limit, Type, Limit, Kill}, infinity).

%% @doc Returns true if the number of held locks is at the limit for the given lock type
-spec concurrency_limit_reached(any()) -> boolean().
concurrency_limit_reached(Type) ->
    gen_server:call(?MODULE, {lock_limit_reached, Type}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
%% @doc Initializes the server
-spec init([]) -> {ok, #state{}} |
                  {ok, #state{}, non_neg_integer() | infinity} |
                  ignore |
                  {stop, term()}.
init([]) ->
    lager:debug("Background Manager starting up."),
    %% claiming the table will result in a handle_info('ETS-TRANSFER', ...) message.
    ok = riak_core_table_manager:claim_table(?LM_ETS_TABLE),
    {ok, #state{table_id=undefined,
                enabled=true}}.

%% @private
%% @doc Handling call messages
-spec handle_call(term(), {pid(), term()}, #state{}) ->
                         {reply, term(), #state{}} |
                         {reply, term(), #state{}, non_neg_integer()} |
                         {noreply, #state{}} |
                         {noreply, #state{}, non_neg_integer()} |
                         {stop, term(), term(), #state{}} |
                         {stop, term(), #state{}}.
handle_call({get_lock, LockType, Pid, Info}, _From, State) ->
    {Reply, State2} = try_lock(LockType, Pid, Info, State),
    {reply, Reply, State2};
handle_call({lock_count, LockType}, _From, State) ->
    {reply, held_count(LockType, State), State};
handle_call(lock_count, _From, State) ->
    Count = length(held_locks(State)),
    {reply, Count, State};
handle_call({lock_limit_reached, LockType}, _From, State) ->
    HeldCount = held_count(LockType, State),
    Limit = ?limit(lock_info(LockType, State)),
    {reply, HeldCount >= Limit, State};
handle_call(lock_types, _From, State=#state{table_id=TableId}) ->
    Infos = [{Type,Info} || {{info, Type},Info} <- ets:match_object(TableId, {{info, '_'},'_'})],
    Types = [{Type, ?enabled(LI), ?limit(LI)} || {Type, LI} <- Infos],
    {reply, Types, State};
handle_call({query_locks, Query}, _From, State) ->
    Results = query_locks(Query, State),
    {reply, Results, State};
handle_call({concurrency_limit, LockType}, _From, State) ->
    Limit = ?limit(lock_info(LockType, State)),
    {reply, Limit, State};
handle_call({set_concurrency_limit, LockType, Limit, Kill}, _From, State) ->
    OldLimit = ?limit(lock_info(LockType, State)),
    State2 = update_concurrency_limit(LockType, Limit, State),
    maybe_honor_limit(Kill, LockType, Limit, State2),
    {reply, OldLimit, State2}.


%% @private
%% @doc Handling cast messages
-spec handle_cast(term(), #state{}) -> {noreply, #state{}} |
                                       {noreply, #state{}, non_neg_integer()} |
                                       {stop, term(), #state{}}.
handle_cast({enable, LockType}, State) ->
    State2 = enable_lock(LockType, State),
    {noreply, State2};
handle_cast({disable, LockType, Kill}, State) ->
    State2 = disable_lock(LockType, State),
    maybe_honor_limit(Kill, LockType, 0, State),
    {noreply, State2};
handle_cast(enable, State) ->
    State2 = State#state{enabled=true},
    {noreply, State2};
handle_cast(disable, State) ->
    State2 = State#state{enabled=false},
    {noreply, State2}.

%% @private
%% @doc Handling all non call/cast messages
-spec handle_info(term(), #state{}) -> {noreply, #state{}} |
                                       {noreply, #state{}, non_neg_integer()} |
                                       {stop, term(), #state{}}.
%% Handle transfer of ETS table from table manager
handle_info({'ETS-TRANSFER', TableId, Pid, _Data}, State) ->
    lager:debug("table_mgr (~p) -> bg_mgr (~p) receiving ownership of TableId: ~p", [Pid, self(), TableId]),
    State2 = State#state{table_id=TableId},
    {noreply, State2};
handle_info({'DOWN', Ref, _, _, _}, State) ->
    State2 = release_lock(Ref, State),
    {noreply, State2};
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
%% @doc Convert process state when code is changed
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
query_locks(FullQuery, State) ->
    Locks = held_locks(State),
    Base = case proplists:get_value(type, FullQuery) of
               undefined -> Locks;
               LockType -> orddict:from_list([{LockType, held_locks(LockType, State)}])
           end,
    Query = proplists:delete(type, FullQuery),
    Matching = orddict:fold(fun(Type, Held, Matching) ->
                                    [matching_locks(Type, Held, Query) | Matching]
                            end,
                            [], Base),
    lists:flatten(Matching).

matching_locks(Type, Held, FullQuery) ->
    QueryPid = proplists:get_value(pid, FullQuery),
    Query = proplists:delete(pid, FullQuery),
    [{Type, Pid, Ref, Info} || {Pid, Ref, Info} <- Held,
                               matches_pid(QueryPid, Pid),
                               matches_query(Info, Query)].

matches_pid(undefined, _) ->
    true;
matches_pid(QueryPid, QueryPid) ->
    true;
matches_pid(_, _) ->
    false.

matches_query(Info, Query) ->
    SortedInfo = lists:ukeysort(1, Info),
    SortedQuery = lists:ukeysort(1, Query),
    (SortedQuery -- SortedInfo) =:= [].

try_lock(LockType, Pid, Info, State=#state{enabled=GlobalEnabled}) ->
    LockInfo = lock_info(LockType, State),
    Enabled = GlobalEnabled andalso ?enabled(LockInfo),
    Limit = ?limit(LockInfo),
    Held  = held_count(LockType, State),
    try_lock(Enabled andalso not (Held >= Limit), LockType, Pid, Info, State).

try_lock(false, _LockType, _Pid, _Info, State) ->
    {max_concurrency, State};
try_lock(true, LockType, Pid, Info, State) ->
    Ref = monitor(process, Pid),
    State2 = add_lock(LockType, {Pid, Ref, Info}, State),
    {ok, State2}.

add_lock(LockType, Lock, State=#state{table_id=TableId}) ->
    Key = {held, LockType},
    ets:insert(TableId, {Key, Lock}),
    State.

release_lock(Ref, State=#state{table_id=TableId}) ->
    %% There should only be one instance of the object, but we'll zap all that match.
    Pattern = {{held, '_'}, {'_', Ref, '_'}},
    Matches = [Lock || {{held, _Type},Lock} <- ets:match_object(TableId, Pattern)],
    [ets:delete_object(TableId, Obj) || Obj <- Matches],
    State.

maybe_honor_limit(true, LockType, Limit, State) ->
    Held = held_locks(LockType, State),
    case Limit < length(Held) of
        true ->
            {_Keep, Discard} = lists:split(Limit, Held),
            %% killing of processes will generate down messages and release the locks
            [erlang:exit(Pid, max_concurrency) || {Pid, _, _} <- Discard],
            ok;
        false ->
            ok
    end;
maybe_honor_limit(false, _LockType, _Limit, _State) ->
    ok.

held_count(LockType, State) ->
    length(held_locks(LockType, State)).

held_locks(#state{table_id=TableId}) ->
    [Lock || {{held, _Type},Lock} <- ets:match_object(TableId, {{held, '_'},'_'})].

held_locks(LockType, #state{table_id=TableId}) ->
    [Lock || {{held, _Type},Lock} <- ets:match_object(TableId, {{held, LockType},'_'})].

enable_lock(LockType, State) ->
    update_lock_enabled(LockType, true, State).

disable_lock(LockType, State) ->
    update_lock_enabled(LockType, false, State).

update_lock_enabled(LockType, Value, State) ->
    update_lock_info(LockType,
                     fun(LockInfo) -> LockInfo#lock_info{enabled=Value} end,
                     ?DEFAULT_LOCK_INFO#lock_info{enabled=Value},
                     State).

update_concurrency_limit(LockType, Limit, State) ->
    update_lock_info(LockType,
                     fun(LockInfo) -> LockInfo#lock_info{concurrency_limit=Limit} end,
                     ?DEFAULT_LOCK_INFO#lock_info{concurrency_limit=Limit},
                     State).

update_lock_info(LockType, Fun, Default, State=#state{table_id=TableId}) ->
    Key = {info, LockType},
    NewInfo = case ets:lookup(TableId, Key) of
                  [] -> Default;
                  [{_Key,LockInfo}] -> Fun(LockInfo)
              end,
    ets:insert(TableId, {Key, NewInfo}),
    State.

lock_info(LockType, #state{table_id=TableId}) ->
    Key = {info,LockType},
    case ets:lookup(TableId, Key) of
        [] -> ?DEFAULT_LOCK_INFO;
        [{_Key,LockInfo}] -> LockInfo;
        [First | _Rest] ->
            lager:error("Unexpected multiple instances of key ~p in table", [{info, LockType}]),
            First %% try to keep going
    end.
