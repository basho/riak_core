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
         concurrency/1,
         set_concurrency/2
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(DEFAULT_CONCURRENCY, 2).

-define(SERVER, ?MODULE). 

-record(state, { locks  :: ordict:orddict(),
                 limits :: orddict:orddict() }).

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
    gen_server:call(?MODULE, {enable, Type}, infinity).

%% @doc Disable handing out of locks of the given type.
-spec disable(any()) -> ok.
disable(Type) ->
    gen_server:call(?MODULE, {disable, Type}, infinity).

%% @doc Get the current maximum concurrency for the given lock type.
-spec concurrency(any()) -> integer().
concurrency(Type) ->
    gen_server:call(?MODULE, {get_max_concurrency, Type}, infinity).

%% @doc Set a new maximum concurrency for the given lock type;
%%      and return the previous maximum or default.
-spec set_concurrency(any(), integer()) -> integer().
set_concurrency(Type, Max) ->
    gen_server:call(?MODULE, {set_max_concurrency, Type, Max}, infinity).


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
    {ok, #state{limits=orddict:new(), locks=orddict:new()}}.

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
handle_call(lock_count, _From, State=#state{locks=Locks}) ->
    Count = orddict:fold(fun(_, Held, Total) -> Total + length(Held) end,
                         0, Locks),
    {reply, Count, State}.

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
    Limit = lock_limit(LockType, State),
    Held  = held_count(LockType, State),
    try_lock(Held >= Limit, LockType, Pid, Opts, State).

try_lock(true, _LockType, _Pid, _Opts, State) ->
    {max_concurrency, State};
try_lock(false, LockType, Pid, _Opts, State=#state{locks=Locks}) ->
    Ref = monitor(process, Pid),
    NewLocks = orddict:append(LockType, {Pid, Ref}, Locks),
    {ok, State#state{locks=NewLocks}}.

release_lock(Ref, State=#state{locks=Locks}) ->
    %% TODO: this makes me (jordan) :(
    Released = orddict:map(fun(Type, Held) -> release_lock(Ref, Type, Held) end,
                           Locks),
    State#state{locks=Released}.

release_lock(Ref, _LockType, Held) ->
    lists:keydelete(Ref, 2, Held).
    

held_count(LockType, #state{locks=Locks}) ->
    case orddict:find(LockType, Locks) of
        error -> 0;             
        {ok, Held} -> length(Held)
    end.

lock_limit(LockType, #state{limits=Limits}) ->
    case orddict:find(LockType, Limits) of
        error -> ?DEFAULT_CONCURRENCY;
        {ok, Limit} -> Limit
    end.
