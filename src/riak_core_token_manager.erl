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
%% ./rebar skip_deps=true eunit suite=token_manager
%%
%% -------------------------------------------------------------------
-module(riak_core_token_manager).

-include("riak_core_token_manager.hrl").

-behaviour(gen_server).


%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0,
         start_link/1,
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
         tokens_blocked/0,
         tokens_blocked/1
        ]).

%% reporting
-export([clear_history/0,
         head/0,
         head/1,
         head/2,
         head/3,
         tail/0,
         tail/1,
         tail/2,
         tail/3,
         ps/0,
         ps/1]).

-define(SERVER, ?MODULE).

-record(state, {info    :: orddict:orddict(),    %% tm_token() -> token_info()
                blocked :: orddict:orddict(),    %% tm_token() -> queue of token_entry()
                given   :: orddict:orddict(),    %% tm_token() -> [token_entry()]
                enabled :: boolean(),            %% Global enable/disable switch
                %% stats
                window  :: orddict:orddict(),    %% tm_token() -> tm_stat_hist()
                history :: queue(),              %% tm_token() -> queue of tm_stat_hist()
                window_interval :: tm_period(),  %% history window size in seconds
                window_tref :: reference()       %% reference to history window sampler timer
               }).

%% General settings of a token type.
-record(token_info, {rate      :: tm_rate(),
                     enabled   :: boolean()}).

-define(rate(X), (X)#token_info.rate).
-define(enabled(X), (X)#token_info.enabled).
-define(DEFAULT_RATE, {0,0}).                    %% DO NOT CHANGE. DEFAULT SET TO ENFORCE "REGISTRATION"
-define(DEFAULT_TOKEN_INFO, #token_info{enabled=true, rate=?DEFAULT_RATE}).

%% An instance of a token entry in "given" or "blocked"
-record(token_entry, {token     :: tm_token(),
                      pid       :: pid(),
                      meta      :: tm_meta(),
                      from      :: {pid(), term()},
                      state     :: given | blocked
                     }). %% undefined unless on queue

-define(TOKEN_ENTRY(Type, Pid, Meta, From, Status),
        #token_entry{token=Type, pid=Pid, meta=Meta, from=From, state=Status}).
-define(token(X), (X)#token_entry.token).
-define(pid(X), (X)#token_entry.pid).
-define(meta(X), (X)#token_entry.meta).
-define(from(X), (X)#token_entry.from).

%% Stats

clear_history() ->
    gen_server:cast(?SERVER, clear_history).

%% List history of token manager
%% @doc show history of token request/grants over default and custom intervals.
%%      offset is forwards-relative to the oldest sample interval
-spec head() -> [[tm_stat_hist()]].
head() ->
        head(all).
-spec head(tm_token()) -> [[tm_stat_hist()]].
head(Token) ->
        head(Token, ?DEFAULT_TM_OUTPUT_SAMPLES).
-spec head(tm_token(), tm_count()) -> [[tm_stat_hist()]].
head(Token, NumSamples) ->
    head(Token, 0, NumSamples).
-spec head(tm_token(), tm_count(), tm_count()) -> [[tm_stat_hist()]].
head(Token, Offset, NumSamples) ->
    gen_server:call(?SERVER, {head, Token, Offset, NumSamples}, infinity).

%% @doc return history of token request/grants over default and custom intervals.
%%      offset is backwards-relative to the newest sample interval
-spec tail() -> [[tm_stat_hist()]].
tail() ->
    tail(all).
-spec tail(tm_token()) -> [[tm_stat_hist()]].
tail(Token) ->
    tail(Token, ?DEFAULT_TM_OUTPUT_SAMPLES).
-spec tail(tm_token(), tm_count()) -> [[tm_stat_hist()]].
tail(Token, NumSamples) ->
    tail(Token, NumSamples, NumSamples).
-spec tail(tm_token(), tm_count(), tm_count()) -> [[tm_stat_hist()]].
tail(Token, Offset, NumSamples) ->
    gen_server:call(?SERVER, {tail, Token, Offset, NumSamples}, infinity).

%% @doc List most recent requests/grants for tokens of all token types
-spec ps() -> [tm_stat_live()].
ps() ->
    ps(all).
%% @doc List most recent requests/grants for tokens for given token type
-spec ps(tm_token()) -> [tm_stat_live()].
ps(Token) ->
    gen_server:call(?SERVER, {ps, Token}, infinity).

%% @doc Set the refill rate of tokens.
-spec set_token_rate(tm_token(), tm_rate()) -> ok.
set_token_rate(Type, Rate) ->
    gen_server:call(?SERVER, {set_token_rate, Type, Rate}, infinity).

-spec token_rate(tm_token()) -> tm_rate().
token_rate(Type) ->
    gen_server:call(?SERVER, {token_rate, Type}, infinity).

token_types() ->
    gen_server:call(?SERVER, token_types, infinity).

%% @doc Return a list of all tokens in current given set
tokens_given() ->
    gen_server:call(?SERVER, tokens_given, infinity).

%% @doc Return a list of all tokens of type Type in current given set
tokens_given(Type) ->
    gen_server:call(?SERVER, {tokens_given, Type}, infinity).

%% @doc Return a list of all blocked tokens
tokens_blocked() ->
    gen_server:call(?SERVER, tokens_blocked, infinity).

%% @doc Return a list of all blocked tokens of type Type
tokens_blocked(Type) ->
    gen_server:call(?SERVER, {tokens_blocked, Type}, infinity).

%% @doc Asynchronously get a token of kind Type.
%%      Associate token with provided pid and metadata.
%%      Returns "max_tokens" if empty.
-spec get_token_async(tm_token(), pid(), [tm_meta()]) -> ok | max_tokens.
get_token_async(Type, Pid, Meta) ->
    gen_server:call(?SERVER, {get_token_async, Type, Pid, Meta}, infinity).

%% @doc Synchronously get a token of kind Type.
%%      Associate token with provided pid and metadata.
%%      Returns "max_tokens" if empty.
-spec get_token_sync(tm_token(), pid(), [tm_meta()]) -> ok | max_tokens.
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
-spec enable(tm_token()) -> ok.
enable(Type) ->
    gen_server:cast(?SERVER, {enable, Type}).


%% @doc Siable handing out any tokens of the given type.
-spec disable(tm_token()) -> ok.
disable(Type) ->
    gen_server:cast(?SERVER, {disable, Type}).

%% @doc Starts the server
-spec start_link() -> {ok, pid()} | ignore | {error, term}.
start_link() ->
    start_link(?DEFAULT_TM_SAMPLE_WINDOW).

-spec start_link(tm_period()) -> {ok, pid()} | ignore | {error, term}.
start_link(Interval) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Interval], []).

%%%% Gen Server %%%%%%%

%% @private
%% @doc Initializes the server with a history window interval in seconds
-spec init([tm_period()]) -> {ok, #state{}} |
                             {ok, #state{}, non_neg_integer() | infinity} |
                             ignore |
                             {stop, term()}.
init([Interval]) ->
    State = #state{info=orddict:new(),
                   given=orddict:new(),
                   blocked=orddict:new(),
                   window=orddict:new(),
                   history=queue:new(),
                   enabled=true,
                   window_interval=Interval},
    State2 = schedule_sample_history(State),
    {ok, State2}.

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
    {OldRate, State2} = do_set_token_rate(TokenType, Rate, State),
    {reply, OldRate, State2};
handle_call(token_types, _From, State) ->
    Result = do_token_types(State),
    {reply, Result, State};
handle_call(tokens_given, _From, State) ->
    Result = do_ps(all, [given], State),
    {reply, Result, State};
handle_call({tokens_given, Token}, _From, State) ->
    Result = do_ps(Token, [given], State),
    {reply, Result, State};
handle_call(tokens_blocked, _From, State) ->
    Result = do_ps(all, [blocked], State),
    {reply, Result, State};
handle_call({tokens_blocked, Token}, _From, State) ->
    Result = do_ps(Token, [blocked], State),
    {reply, Result, State};
handle_call({get_token_async, Type, Pid, Meta}, _From, State) ->
    do_get_token_async(Type, Pid, Meta, State);
handle_call({get_token_sync, Type, Pid, Meta}, From, State) ->
    do_get_token_sync(Type, Pid, Meta, From, State);
handle_call({head, Token, Offset, Count}, _From, State) ->
    Result = do_hist(head, Token, Offset, Count, State),
    {reply, Result, State};
handle_call({tail, Token, Offset, Count}, _From, State) ->
    Result = do_hist(tail, Token, Offset, Count, State),
    {reply, Result, State};
handle_call({ps, Token}, _From, State) ->
    Result = do_ps(Token, [given, blocked], State),
    {reply, Result, State};
handle_call(Call, _From, State) ->
    lager:warning("Unhandled call: ~p", [Call]),
    Reply = {unhanded_call, Call},
    {reply, Reply, State}.

handle_cast(clear_history, State) ->
    State2 = do_clear_history(State),
    {noreply, State2};
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

handle_info(sample_history, State) ->
    State2 = schedule_sample_history(State),
    State3 = do_sample_history(State2),
    {noreply, State3};
handle_info({refill_tokens, Type}, State) ->
    State2 = do_refill_tokens(Type, State),
    schedule_refill_tokens(Type, State2),
    {noreply, State2};
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Get stat history for given token type from sample set
-spec stat_window(tm_token(), orddict:orddict()) -> tm_stat_hist().
stat_window(TokenType, Window) ->
    case orddict:find(TokenType, Window) of
        error -> ?DEFAULT_TM_STAT_HIST;
        {ok, StatHist} -> StatHist
    end.

do_token_types(#state{info=Tokens}) ->
    orddict:fetch_keys(Tokens).

do_set_token_rate(TokenType, Rate, State) ->
    OldRate = ?rate(token_info(TokenType, State)),
    State2 = update_token_rate(TokenType, Rate, State),
    schedule_refill_tokens(TokenType, State2),
    %% maybe reschedule blocked callers
    State3 = maybe_unblock_blocked(TokenType, State2),
    {OldRate, State3}.

%% erase saved history
do_clear_history(State=#state{window_tref=TRef}) ->
    erlang:cancel_timer(TRef),
    State2 = State#state{history=queue:new()},
    schedule_sample_history(State2).

%% Return stats history from head or tail of stats history queue
do_hist(End, TokenType, Offset, Count, State) when Offset =< 0 ->
    do_hist(End, TokenType, 1, Count, State);
do_hist(End, TokenType, Offset, Count, State) when Count =< 0 ->
    do_hist(End, TokenType, Offset, ?DEFAULT_TM_OUTPUT_SAMPLES, State);
do_hist(End, TokenType, Offset, Count, #state{history=HistQueue}) ->
    QLen = queue:len(HistQueue),
    First = max(1, case End of
                       head -> Offset;
                       tail -> QLen - Offset + 1
                   end),
    Last = min(QLen, max(First + Count - 1, 1)),
    case segment_queue(First, Last, HistQueue) of
        empty -> [];
        {ok, Hist } -> 
            case TokenType of
                all ->
                    StatsDictList = queue:to_list(Hist),
                    [orddict:to_list(Stat) || Stat <- StatsDictList];
                _T  ->
                    [[{TokenType, stat_window(TokenType, StatsDict)}] || StatsDict <- queue:to_list(Hist)]
            end
    end.

segment_queue(First, Last, Queue) ->
    QLen = queue:len(Queue),
%%    ?debugFmt("First: ~p, Last: ~p, QLen: ~p", [First, Last, QLen]),
    case QLen >= Last andalso QLen > 0 of
        true ->
            %% trim off extra tail, then trim head
            Front = case QLen == Last of
                        true -> Queue;
                        false ->
                            {QFirst, _QRest} = queue:split(Last, Queue),
                            QFirst
                    end,
            case First == 1 of
                true -> {ok, Front};
                false ->
                    {_Skip, Back} = queue:split(First-1, Front),
                    {ok, Back}
            end;
        false ->
            %% empty
            empty
    end.
    

format_entry(Entry) ->
%%  ?debugFmt("Entry: ~p~n", [Entry]),
    #tm_stat_live
        {
          token = Entry#token_entry.token,
          consumer = Entry#token_entry.pid,
          meta = Entry#token_entry.meta,
          state = Entry#token_entry.state
        }.

fmt_live_tokens(Entries) ->
    [format_entry(Entry) || Entry <- Entries].

do_ps(all, Status, #state{given=Given, blocked=Blocked}) ->
    E1 = case lists:member(given, Status) of
             true -> 
                 lists:flatten([Entries || {_T, Entries} <- orddict:to_list(Given)]);
             false ->
                 []
         end,
    E2 = case lists:member(blocked, Status) of
             true ->
                 E1 ++ lists:flatten(
                         [[Entry || Entry <- queue:to_list(Q)] || {_T, Q} <- orddict:to_list(Blocked)]);
             false ->
                 E1
         end,
    fmt_live_tokens(E2);
do_ps(TokenType, Status, State) ->
    E1 = case lists:member(given, Status) of
             true ->
                 tokens_given(TokenType, State);
             false ->
                 []
         end,
    E2 = case lists:member(blocked, Status) of
             true ->
                 E1 ++ queue:to_list(token_queue(TokenType, State));
             false ->
                 E1
         end,
    fmt_live_tokens(E2).

%% Possibly send replies to processes blocked on tokens of Type.
%% Returns new State.
give_available_tokens(Type, 0, TokenQueue, State) ->
    %% no more available tokens to give out
    update_token_queue(Type, TokenQueue, State);
give_available_tokens(Type, NumAvailable, TokenQueue, State) ->
    case queue:out(TokenQueue) of
        {empty, _Q} ->
            %% no more blocked entries
            State;
        {{value, Entry}, TokenQueue2} ->
%%            ?debugFmt("Entry: ~p", [Entry]),
            %% queue entry to unblock
            Pid = ?pid(Entry),
            Meta = ?meta(Entry),
            From = ?from(Entry),
            %% account for given token
            State2 = give_token(Type, Pid, Meta, State),
            %% send reply to blocked caller, unblocking them.
            gen_server:reply(From, ok),
            %% unblock next blocked in queue
            give_available_tokens(Type, NumAvailable-1,TokenQueue2,State2)
    end.

%% For the given type, check the current given count and if less
%% than the rate limit, give out as many tokens as are available
%% to callers on the blocked list. They need a reply because they
%% made a gen_server:call() that we have not replied to yet.
maybe_unblock_blocked(Type, State) ->
    Entries = tokens_given(Type, State),
    {_Period, MaxCount} = ?rate(token_info(Type, State)),
    PosNumAvailable = erlang:max(MaxCount - length(Entries), 0),
    Queue = token_queue(Type, State),
    give_available_tokens(Type, PosNumAvailable, Queue, State).

%% Schedule a timer event to refill tokens of given type
schedule_refill_tokens(Type, State) ->
    {Period, _Count} = ?rate(token_info(Type, State)),
    erlang:send_after(Period*1000, self(), {refill_tokens, Type}).

%% Schedule a timer event to snapshot the current history
schedule_sample_history(State=#state{window_interval=Interval}) ->
    TRef = erlang:send_after(Interval*1000, self(), sample_history),
    State#state{window_tref=TRef}.

do_sample_history(State=#state{window=Window, history=Histories}) ->
    %% Move the current window of measurements onto the history queues.
    %% Trim queue down to DEFAULT_TM_KEPT_SAMPLES if too big now.
    Queue2 = queue:in(Window, Histories),
    Trimmed = case queue:len(Queue2) > ?DEFAULT_TM_KEPT_SAMPLES of
                  true ->
                      {_Discarded, Rest} = queue:out(Queue2),
                      Rest;
                  false ->
                      Queue2
              end,
%%    ?debugFmt("Storing a sample history, Queue[~p]: ~p", [queue:len(Trimmed), Trimmed]),
    EmptyWindow = orddict:new(),
    State#state{window=EmptyWindow, history=Trimmed}.

update_stat_window(TokenType, Fun, Default, State=#state{window=Window}) ->
    NewWindow = orddict:update(TokenType, Fun, Default, Window),
    State#state{window=NewWindow}.

default_refill(Token, State) ->
%%    ?debugFmt("default_refill: ~p", [Token]),
    {_Rate, Limit} = ?rate(token_info(Token, State)),
    ?DEFAULT_TM_STAT_HIST#tm_stat_hist{refills=1, limit=Limit}.

default_given(Token, State) ->
%%    ?debugFmt("default_given: ~p", [Token]),
    {_Rate, Limit} = ?rate(token_info(Token, State)),
    ?DEFAULT_TM_STAT_HIST#tm_stat_hist{given=1, limit=Limit}.

increment_stat_refills(Token, State) ->
    update_stat_window(Token,
                       fun(Stat) -> Stat#tm_stat_hist{refills=1+Stat#tm_stat_hist.refills} end,
                       default_refill(Token, State),
                       State).

increment_stat_given(Token, State) ->
    update_stat_window(Token,
                       fun(Stat) -> Stat#tm_stat_hist{given=1+Stat#tm_stat_hist.given} end,
                       default_given(Token, State),
                       State).

increment_stat_blocked(Token, State) ->
    {_Rate, Limit} = ?rate(token_info(Token, State)),
    update_stat_window(Token,
                       fun(Stat) -> Stat#tm_stat_hist{blocked=1+Stat#tm_stat_hist.blocked} end,
                       ?DEFAULT_TM_STAT_HIST#tm_stat_hist{blocked=1, limit=Limit},
                       State).

%% Token refill timer event handler.
%%   Capture stats of what was given in the previous period,
%%   Clear all tokens of this type from the given set,
%%   Unblock blocked processes if possible.
do_refill_tokens(Type, State=#state{given=Given}) ->
%%    ?debugFmt("refilling tokens for type: ~p", [Type]),
    State2 = increment_stat_refills(Type, State),
    NewGiven = orddict:erase(Type, Given),
    maybe_unblock_blocked(Type, State2#state{given=NewGiven}).

token_info(TokenType, #state{info=Info}) ->
    case orddict:find(TokenType, Info) of
        error -> ?DEFAULT_TOKEN_INFO;
        {ok, TokenInfo} -> TokenInfo
    end.

update_token_queue(TokenType, TokenQueue, State=#state{blocked=Orddict1}) ->
    Fun = fun(_Val) -> TokenQueue end,
    State#state{blocked=orddict:update(TokenType, Fun, TokenQueue, Orddict1)}.

token_queue(TokenType, #state{blocked=Orddict1}) ->
    case orddict:find(TokenType, Orddict1) of
        error -> queue:new();
        {ok, TokenQueue} -> TokenQueue
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

tokens_given(Type, #state{given=AllGiven}) ->
    case orddict:find(Type, AllGiven) of
        error -> [];
        {ok, Given} -> Given
    end.

%% Add a token of type to our given set and remove it from blocked set if present
give_token(Type, Pid, Meta, State=#state{given=Given, blocked=Blocked}) ->
    Entry = ?TOKEN_ENTRY(Type, Pid, Meta, undefined, given),
    NewGiven = orddict:append(Type, Entry, Given),
    NewBlocked = orddict:erase(Type, Blocked),
    %% update given stats
    State2 = increment_stat_given(Type, State),
    State2#state{given=NewGiven, blocked=NewBlocked}.

%% Put a token request on the blocked queue. We'll reply later when a token
%% becomes available
enqueue_request(Type, Pid, Meta, From, State) ->
    OldQueue = token_queue(Type, State),
    NewQueue = queue:in(?TOKEN_ENTRY(Type, Pid, Meta, From, blocked), OldQueue),
    %% update blocked stats
    State2 = increment_stat_blocked(Type, State),
    %% Put new queue back in state
    update_token_queue(Type, NewQueue, State2).

%% reply ok now if available or max_tokens if not. Non-blocking
do_get_token_async(Type, Pid, Meta, State) ->
    Info = token_info(Type, State),
    Entries = tokens_given(Type, State),
    {_Period, MaxCount} = ?rate(Info),
    case length(Entries) < MaxCount of
        true ->
            %% tokens are available
            {reply, ok, give_token(Type, Pid, Meta, State)};
        false ->
            {reply, max_tokens, State}
    end.

%% reply now if available or reply later if en-queued. Blocking
do_get_token_sync(Type, Pid, Meta, From, State) ->
    case do_get_token_async(Type, Pid, Meta, State) of
        {reply, max_tokens, _S} ->
            {noreply, enqueue_request(Type, Pid, Meta, From, State)};
        {reply, ok, State2} ->
            {reply, ok, State2}
    end.
