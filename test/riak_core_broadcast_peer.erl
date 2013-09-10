%%% File        : riak_core_broadcast_peer.erl
%%% Author      : Ulf Norell
%%% Description : 
%%% Created     : 19 Aug 2013 by Ulf Norell
-module(riak_core_broadcast_peer).

-compile(export_all).

-behaviour(gen_server).

%% API
-export([start_link/2, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-record(state, {parent, name}).

-define(SERVER, riak_core_broadcast).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Parent, Peer) ->
  error_logger:tty(false),
  gen_server:start_link({local, ?SERVER}, ?MODULE, [Parent, Peer], []).

stop() ->
  gen_server:call(?SERVER, stop).

%%====================================================================
%% gen_server callbacks
%%====================================================================

report(State, Fun, Req) ->
  rpc:call(State#state.parent, peer, Fun, [State#state.name, Req]).

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Parent, Peer]) ->
  %% io:format("~p init(~p, ~p)\n", [node(), Parent, Peer]),
  {ok, #state{name = Peer, parent = Parent}}.

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
handle_call(stop, _, State) ->
  {stop, stopped, ok, State};
handle_call(Request, _From, State) ->
  report(State, call, Request),
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
  report(State, cast, Msg),
  {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(Info, State) ->
  report(State, info, Info),
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


