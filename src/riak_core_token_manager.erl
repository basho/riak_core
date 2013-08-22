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
-module(riak_core_token_manager).

-behaviour(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([start_link/0,
         enable/0,
         enable/1,
         disable/0,
         disable/1,
         token_rate/1,
         set_token_rate/2,
         get_token_async/3,
         get_token_sync/3,
         token_types/0,
         tokens_given/0,
         tokens_given/1,
         tokens_waiting/0,
         tokens_waiting/1
        ]).

-record(state, {info    :: ordict:orddict(),
                waiting :: queue(),
                given   :: orddict:orddict(),
                enabled :: boolean()}).

-record(token_info, {rate      :: rate(),
                     enabled   :: boolean()}).

-type token() :: any().
-type meta()  :: {atom(), any()}.                %% meta data to associate with a token
-type period() :: pos_integer().                 %% refill period in milliseconds
-type count() :: pos_integer().                  %% refill tokens to count at each refill period
-type rate() :: {period(), count(), boolean()}.  %% token refresh rate and if starts full

-define(rate(X), (X)#token_info.rate).
-define(enabled(X), (X)#token_info.enabled).
-define(DEFAULT_RATE, {0,0}).                    %% DO NOT CHANGE. DEFAULT SET TO ENFORCE "REGISTRATION"
-define(DEFAULT_TOKEN_INFO, #token_info{enabled=true, rate=?DEFAULT_RATE}).

-define(SERVER, ?MODULE).

%% @doc Set the refill rate of tokens.
-spec set_token_rate(token(), riak_core_token_manager:rate()) -> ok.
set_token_rate(Type, {Period, Count, StartFull}) ->
    riak_core_token_manager:set_token_rate(Type, {Period, Count}, StartFull).

-spec token_rate(token()) -> riak_core_token_manager:rate().
token_rate(Type) ->
    gen_server:call(?SERVER, {token_rate, Type}, infinity).

token_types() ->
    gen_server:call(?SERVER, token_types, infinity).

tokens_given() ->
    gen_server:call(?SERVER, tokens_given, infinity).

tokens_given(Type) ->
    gen_server:call(?SERVER, {tokens_given, Type}, infinity).

tokens_waiting() ->
    gen_server:call(?SERVER, tokens_waiting, infinity).

tokens_waiting(Type) ->
    gen_server:call(?SERVER, {tokens_waiting, Type}, infinity).

%% @doc Aynchronously get a token of kind Type.
%%      Associate token with provided pid and metadata.
%%      Returns "max_tokens" if empty.
-spec get_token_async(token(), pid(), [meta()]) -> ok | max_concurrency.
get_token_async(Type, Pid, Meta) ->
    gen_server:call(?SERVER, {get_token_async, Type, Pid, Meta}, infinity).

%% @doc Synchronously get a token of kind Type.
%%      Associate token with provided pid and metadata.
%%      Returns "max_tokens" if empty.
-spec get_token_sync(token(), pid(), [meta()]) -> ok | max_concurrency.
get_token_sync(Type, Pid, Meta) ->
    gen_server:call(?SERVER, {get_token_sync, Type, Pid, Meta}, infinity).

%% @doc Enable handing out of any tokens
-spec enable() -> ok.
enable() ->
    gen_server:cast(?SERVER, enable).

%% @doc Disable handing out of any tokens
-spec disable() -> ok.
disable() ->
    gen_server:cast(?SERVER, disable).

%% @doc Enable handing out of tokens of the given type.
-spec enable(token()) -> ok.
enable(Type) ->
    gen_server:cast(?SERVER, {enable, Type}).


%% @doc Siable handing out any tokens of the given type.
-spec disable(token()) -> ok.
disable(Type) ->
    gen_server:cast(?SERVER, {disable, Type}).

%% @doc Starts the server
-spec start_link() -> {ok, pid()} | ignore | {error, term}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%% Gen Server %%%%%%%

%% @private
%% @doc Initializes the server
-spec init([]) -> {ok, #state{}} |
                  {ok, #state{}, non_neg_integer() | infinity} |
                  ignore |
                  {stop, term()}.
init([]) ->
    {ok, #state{info=orddict:new(),
                given=orddict:new(),
                waiting=queue:new(),
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
handle_call({token_rate, TokenType}, _From, State) ->
    Rate = ?rate(token_info(TokenType, State)),
    {reply, Rate, State};
handle_call({set_token_rate, TokenType, Rate}, _From, State) ->
    OldRate = ?rate(token_info(TokenType, State)),
    State2 = update_token_rate(TokenType, Rate, State),
    %% TODO: reschedule waiting callers
    {reply, OldRate, State2};
handle_call(Call, _From, State) ->
    lager:warning("Unhandled call: ~p", [Call]),
    Reply = {unhanded_call, Call},
    {reply, Reply, State}.

handle_cast({enable, Type}, State) ->
    State2 = enable_token(Type, State),
    {noreply, State2};
handle_cast({disable, Type}, State) ->
    State2 = disable_token(Type, State),
    {noreply, State2};
handle_cast(enable, State) ->
    State2 = State#state{enabled=true},
    {noreply, State2};
handle_cast(disable, State) ->
    State2 = State#state{enabled=false},
    {noreply, State2};
handle_cast(Cast, State) ->
    lager:warning("Unhandled cast: ~p", [Cast]),
    Reply = {unhandled_cast, Cast},
    {reply, Reply, State}.

handle_info({'DOWN', Ref, _, _, _}, State) ->
    lager:info("Linked process died with ref ~p: ", [Ref]),
    {noreply, State};
handle_info(Info, State) ->
    lager:warning("Unhandled info: ~p", [Info]),
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


token_info(TokenType, #state{info=Info}) ->
    case orddict:find(TokenType, Info) of
        error -> ?DEFAULT_TOKEN_INFO;
        {ok, TokenInfo} -> TokenInfo
    end.

enable_token(TokenType, State) ->
    update_token_enabled(TokenType, true, State).

disable_token(TokenType, State) ->
    update_token_enabled(TokenType, false, State).

update_token_enabled(TokenType, Value, State) ->
    update_token_info(TokenType,
                     fun(TokenInfo) -> TokenInfo#token_info{enabled=Value} end,
                     ?DEFAULT_TOKEN_INFO#token_info{enabled=Value},
                     State).

update_token_rate(TokenType, Rate, State) ->
    update_token_info(TokenType,
                     fun(TokenInfo) -> TokenInfo#token_info{rate=Rate} end,
                     ?DEFAULT_TOKEN_INFO#token_info{rate=Rate},
                     State).

update_token_info(TokenType, Fun, Default, State=#state{info=Info}) ->
    NewInfo = orddict:update(TokenType, Fun, Default, Info),
    State#state{info=NewInfo}.
