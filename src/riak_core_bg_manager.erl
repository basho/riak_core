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
-export([start_link/0,
         get_lock/1,
         get_lock/2,
         get_lock/3,
         lock_count/0,
         lock_count/1,
         enable/1,
         disable/1,
         concurrency_limit/1,
         set_concurrency_limit/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {held  :: ordict:orddict(),
                info  :: orddict:orddict()}).

-record(lock_info, {concurrency_limit :: non_neg_integer(),
                    enabled           :: boolean()}).

-define(SERVER, ?MODULE).
-define(DEFAULT_CONCURRENCY, 2).
-define(limit(X), (X)#lock_info.concurrency_limit).
-define(enabled(X), (X)#lock_info.enabled).
-define(DEFAULT_LOCK_INFO, #lock_info{enabled=true, concurrency_limit=?DEFAULT_CONCURRENCY}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Starts the server
-spec start_link() -> {ok, pid()} | ignore | {error, term}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Acquire a concurrency lock of the given type, if available,
%%      and associate the lock with the calling process.
-spec get_lock(any()) -> ok | max_concurrency.
get_lock(Type) ->
    get_lock(Type, self()).

%% @doc Acquire a concurrency lock of the given type, if available,
%%      and associate the lock with the provided pid.
-spec get_lock(any(), pid()) -> ok | max_concurrency.
get_lock(Type, Pid) ->
    get_lock(Type, Pid, []).

%% @doc Acquire a concurrency lock of the given type, if available,
%%      and associate the lock with the provided pid.
%%      TODO: info on options, but there are none right now
-spec get_lock(any(), pid(), [{atom(), any()}]) -> ok | max_concurrency.
get_lock(Type, Pid, Opts) ->
    gen_server:call(?MODULE, {get_lock, Type, Pid, Opts}, infinity).

%% @doc Return the current concurrency count for all lock types
-spec lock_count() -> integer().
lock_count() ->
    gen_server:call(?MODULE, lock_count, infinity).

%% @doc Return the current concurrency count of the given lock type.
-spec lock_count(any()) -> integer().
lock_count(Type) ->
    gen_server:call(?MODULE, {lock_count, Type}, infinity).

%% @doc Enable handing out of locks of the given type.
-spec enable(any()) -> ok.
enable(Type) ->
    %% TODO: should this be a cast?
    gen_server:call(?MODULE, {enable, Type}, infinity).

%% @doc Disable handing out of locks of the given type.
-spec disable(any()) -> ok.
disable(Type) ->
    %% TODO: should this be a cast?
    gen_server:call(?MODULE, {disable, Type}, infinity).

%% @doc Get the current maximum concurrency for the given lock type.
-spec concurrency_limit(any()) -> non_neg_integer().
concurrency_limit(Type) ->
    gen_server:call(?MODULE, {concurrency_limit, Type}, infinity).

%% @doc Set a new maximum concurrency for the given lock type;
%%      and return the previous maximum or default.
-spec set_concurrency_limit(any(), non_neg_integer()) -> non_neg_integer().
set_concurrency_limit(Type, Limit) ->
    gen_server:call(?MODULE, {set_concurrency_limit, Type, Limit}, infinity).


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
    {ok, #state{info=orddict:new(), held=orddict:new()}}.

%% @private
%% @doc Handling call messages
-spec handle_call(term(), {pid(), term()}, #state{}) ->
                         {reply, term(), #state{}} |
                         {reply, term(), #state{}, non_neg_integer()} |
                         {noreply, #state{}} |
                         {noreply, #state{}, non_neg_integer()} |
                         {stop, term(), term(), #state{}} |
                         {stop, term(), #state{}}.
handle_call({get_lock, LockType, Pid, Opts}, _From, State) ->
    {Reply, State2} = try_lock(LockType, Pid, Opts, State),
    {reply, Reply, State2};
handle_call({lock_count, LockType}, _From, State) ->
    {reply, held_count(LockType, State), State};
handle_call(lock_count, _From, State=#state{held=Locks}) ->
    Count = orddict:fold(fun(_, Held, Total) -> Total + length(Held) end,
                         0, Locks),
    {reply, Count, State};
handle_call({enable, LockType}, _From, State) ->
    State2 = enable_lock(LockType, State),
    {reply, ok, State2};
handle_call({disable, LockType}, _From, State) ->
    State2 = disable_lock(LockType, State),
    {reply, ok, State2};
handle_call({concurrency_limit, LockType}, _From, State) ->
    Limit = ?limit(lock_info(LockType, State)),
    {reply, Limit, State};
handle_call({set_concurrency_limit, LockType, Limit}, _From, State) ->
    OldLimit = ?limit(lock_info(LockType, State)),
    NewState = update_concurrency_limit(LockType, Limit, State),
    {reply, OldLimit, NewState}.

%% @private
%% @doc Handling cast messages
-spec handle_cast(term(), #state{}) -> {noreply, #state{}} |
                                       {noreply, #state{}, non_neg_integer()} |
                                       {stop, term(), #state{}}.
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
%% @doc Handling all non call/cast messages
-spec handle_info(term(), #state{}) -> {noreply, #state{}} |
                                       {noreply, #state{}, non_neg_integer()} |
                                       {stop, term(), #state{}}.
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
try_lock(LockType, Pid, Opts, State) ->
    LockInfo = lock_info(LockType, State),
    Enabled = ?enabled(LockInfo),
    Limit = ?limit(LockInfo),
    Held  = held_count(LockType, State),
    try_lock(Enabled andalso not (Held >= Limit), LockType, Pid, Opts, State).

try_lock(false, _LockType, _Pid, _Opts, State) ->
    {max_concurrency, State};
try_lock(true, LockType, Pid, _Opts, State=#state{held=Locks}) ->
    Ref = monitor(process, Pid),
    NewLocks = orddict:append(LockType, {Pid, Ref}, Locks),
    {ok, State#state{held=NewLocks}}.

release_lock(Ref, State=#state{held=Locks}) ->
    %% TODO: this makes me (jordan) :(
    Released = orddict:map(fun(Type, Held) -> release_lock(Ref, Type, Held) end,
                           Locks),
    State#state{held=Released}.

release_lock(Ref, _LockType, Held) ->
    lists:keydelete(Ref, 2, Held).
    

held_count(LockType, #state{held=Locks}) ->
    case orddict:find(LockType, Locks) of
        error -> 0;             
        {ok, Held} -> length(Held)
    end.

enable_lock(LockType, State) ->
    update_lock_enabled(LockType, true, State).

disable_lock(LockType, State) ->
    %% TODO: should we also kill all processes that hold the lock/release all locks?
    update_lock_enabled(LockType, false, State).

update_lock_enabled(LockType, Value, State) ->
    update_lock_info(LockType,
                     fun(LockInfo) -> LockInfo#lock_info{enabled=Value} end,
                     ?DEFAULT_LOCK_INFO#lock_info{enabled=Value},
                     State).

update_concurrency_limit(LockType, Limit, State) ->
    %% TODO: if Limit < Number of Currently held locks, should we kill # Held - Limit
    %%       processes and release their locks
    update_lock_info(LockType,
                     fun(LockInfo) -> LockInfo#lock_info{concurrency_limit=Limit} end,
                     ?DEFAULT_LOCK_INFO#lock_info{concurrency_limit=Limit},
                     State).

update_lock_info(LockType, Fun, Default, State=#state{info=Info}) ->
    NewInfo = orddict:update(LockType, Fun, Default, Info),
    State#state{info=NewInfo}.


lock_info(LockType, #state{info=Info}) ->
    case orddict:find(LockType, Info) of
        error -> ?DEFAULT_LOCK_INFO;
        {ok, LockInfo} -> LockInfo
    end.
