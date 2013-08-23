%%% File        : proxy_server.erl
%%% Author      : Ulf Norell
%%% Description : 
%%% Created     : 22 Aug 2013 by Ulf Norell
-module(proxy_server).

-compile(export_all).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-record(state, {config :: [{{atom(), atom()}, [{keep|drop, integer()}]}]}).

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
  gen_server:start_link({global, ?SERVER}, ?MODULE, [], []).

setup(Config) ->
  gen_server:call({global, ?SERVER}, {setup, Config}).

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
init([]) ->
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
handle_call({setup, Config}, _From, _State) ->
  {reply, ok, #state{ config = Config }};
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({From, Server, To, Msg}, State) ->
  {Keep, State1} = keep_msg(node_name(From), node_name(To), State),
  if Keep -> gen_server:cast({Server, To}, Msg);
     true -> event_logger:event({dropped, node_name(From), node_name(To), Msg}) end,
  {noreply, State1};
handle_cast(_Msg, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
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

keep_msg(From, To, State) ->
  Key = {From, To},
  Cfg = State#state.config,
  {Keep, Cfg1} =
    case proplists:get_value(Key, Cfg, []) of
      []             -> {true,  Cfg};
      [{keep, N}|Xs] -> {true,  store(Key, [{keep, N - 1} || N > 1] ++ Xs, Cfg)};
      [{drop, N}|Xs] -> {false, store(Key, [{drop, N - 1} || N > 1] ++ Xs, Cfg)}
    end,
  {Keep, State#state{ config = Cfg1 }}.

store(Key, [], Cfg) -> lists:keydelete(Key, 1, Cfg);
store(Key, Xs, Cfg) -> lists:keystore(Key, 1, Cfg, {Key, Xs}).

node_name(Node) ->
  list_to_atom(hd(string:tokens(atom_to_list(Node),"@"))).
