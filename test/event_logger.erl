%%% File        : event_logger.erl
%%% Author      : Ulf Norell
%%% Description : 
%%% Created     : 20 Aug 2013 by Ulf Norell
-module(event_logger).

-compile(export_all).

-behaviour(gen_server).

%% API
-export([start_link/0, start/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-record(state, { events = [] }).

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() -> do_start(start_link).
start()      -> do_start(start).

do_start(Start) ->
  Ref = make_ref(),
  Res = gen_server:Start({global, ?SERVER}, ?MODULE, [self(), Ref], []),
  receive
    {started, Ref} -> Res
  after 1000 ->
    {error, timeout}
  end.
  

event(E) ->
  gen_server:call({global, ?SERVER}, {event, E}).

get_events() ->
  gen_server:call({global, ?SERVER}, get_events).

get_events(IdleTime, Timeout) ->
  %% gen_server:call({global, ?SERVER}, {get_events, IdleTime, Timeout}).
  Now = timestamp(),
  get_events(IdleTime, Timeout, Now).

get_events(IdleTime, Timeout, T0) ->
  T = timestamp(),
  case (T - T0) div 1000 >= Timeout of
    true  -> {[], timeout};
    false ->
      case gen_server:call({global, ?SERVER}, {get_events, IdleTime}) of
        {not_done, Delta} ->
          timer:sleep(Delta + 2),
          get_events(IdleTime, Timeout, T0);
        Events   -> {Events, ok}
      end
  end.

stop() ->
  gen_server:call({global, ?SERVER}, stop).

reset() ->
  gen_server:call({global, ?SERVER}, reset).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |

%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Parent, Ref]) ->
  %% schedule_tick(),
  Parent ! {started, Ref},
  {ok, #state{}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) ->
%%                {reply, Reply, State} |
%%                {reply, Reply, State, Timeout} |
%%                {noreply, State} |
%%                {noreply, State, Timeout} |
%%                {stop, Reason, Reply, State} |
%%                {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(reset, _From, _State) ->
  {reply, ok, #state{ events = [{timestamp(), init}] }};
handle_call({event, E}, _From, State) ->
  {reply, ok, State#state{ events = [{timestamp(), E} | State#state.events] }};
handle_call(get_events, _From, State) ->
  {reply, mk_trace(State), State#state{ events = [] }};
handle_call({get_events, IdleTime}, _From, State) ->
  case IdleTime - time_since_last_event(State) of
    T when T =< 0 -> {reply, mk_trace(State), State#state{ events = [] }};
    T             -> {reply, {not_done, T}, State}
  end;
handle_call({get_events, IdleTime, Timeout}, From, State) ->
  case IdleTime - time_since_last_event(State) of
    T when T =< 0 -> {reply, {mk_trace(State), ok}, State#state{ events = [] }};
    T ->
      Timer = erlang:send_after(Timeout, self(), {timeout, From}),
      erlang:send_after(T + 2, self(), {check_idle, From, IdleTime, Timer}),
      {noreply, State#state{ events = [{timestamp(), get_events}|State#state.events] }}
  end;
handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(reset, _State) ->
  {noreply, #state{ events = [{timestamp(), init}] }};
handle_cast({event, E}, State) ->
  {noreply, State#state{ events = [{timestamp(), E} | State#state.events] }};
handle_cast(_Msg, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(tick, State) ->
  %% schedule_tick(),
  {noreply, State};
handle_info({timeout, From}, State) ->
  gen_server:reply(From, {mk_trace(State), timeout}),
  {noreply, State#state{ events = [] }};
handle_info({check_idle, _, _, _}, State=#state{ events = [] }) ->
  {noreply, State};
handle_info({check_idle, From, IdleTime, TimeoutRef}, State) ->
  case IdleTime - time_since_last_event(State) of
    T when T =< 0 ->
      print_mailbox("1"),
      timer:sleep(1),
      print_mailbox("2"),
      erlang:cancel_timer(TimeoutRef),
      timer:sleep(1),
      print_mailbox("3"),
      gen_server:reply(From, {mk_trace(State), ok}),
      timer:sleep(1),
      print_mailbox("4"),
      {noreply, State#state{ events = [] }};
    T ->
      erlang:send_after(T + 2, self(), {check_idle, From, IdleTime, TimeoutRef}),
      {noreply, State#state{ events = [{timestamp(), check_idle}|State#state.events] }}
  end;
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

mk_trace(#state{ events = Es }) -> mk_trace([{timestamp(), end_of_trace}|Es]);
mk_trace([]) -> [];
mk_trace(Es) ->
  Es1=[{T0,_}|_] = lists:reverse(Es),
  lists:sort([ {T - T0 + 1, simple_names(E)} || {T, E} <- Es1 ]).

timestamp() -> from_now(os:timestamp()).

from_now({A, B, C}) ->
  C + 1000000 * (B + 1000000 * A).

time_since_last_event(S) ->
  T       = timestamp(),
  {T0, _} = last_event(S#state.events),
  (T - T0) div 1000.

last_event([{_, check_idle}|Es]) -> last_event(Es);
last_event([E|_]) -> E.

print_mailbox(S) ->
  {message_queue_len, N} = process_info(self(), message_queue_len),
  [ io:format("Mailbox ~s = ~p\n", [S, N]) || N > 2 ].

schedule_tick() ->
  erlang:send_after(5, self(), tick).

simple_names(Node) when is_atom(Node) ->
  case string:tokens(atom_to_list(Node),"@") of
    [Name, _Host] -> list_to_atom(Name);
    _             -> Node
  end;
simple_names([H|T]) ->
  [simple_names(H)|simple_names(T)];
simple_names(T) when is_tuple(T) ->
  list_to_tuple(simple_names(tuple_to_list(T)));
simple_names(X) -> X.

