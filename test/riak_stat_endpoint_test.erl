%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_endpoint_test).
-include_lib("riak_core/include/riak_stat.hrl").
-behaviour(gen_server).

%% API
-export([start_link/0,
    open_socket/3,
    get_objects/0]).

%% gen_event callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
-define(ETS_TABLE, riak_stat_endpoint_test_table).

-record(state, {
    ets_table_name = ?ETS_TABLE,
    ets_table_tid,
    udp_socket,
    tcp_socket
}).

%%%===================================================================
%%% API
%%%===================================================================

open_socket(udp, Port, HostName) ->
    gen_server:call(?MODULE, {open_socket, udp, Port, HostName});
open_socket(tcp, Port, HostName) ->
    gen_server:call(?MODULE, {open_socket, tcp, Port, HostName});
open_socket(_Protocol, _Port, _HostName) ->
    io:format(user, "Wrong protocol from test~n").

get_objects() -> ok.

%%--------------------------------------------------------------------
-spec(start_link() -> {ok, pid()} | {error, {already_started, pid()}}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


%%--------------------------------------------------------------------
-spec(init(InitArgs :: term()) ->
    {ok, State :: #state{}} |
    {ok, State :: #state{}, hibernate} |
    {error, Reason :: term()}).
init([]) ->
    Tid = ets:new(?ETS_TABLE, [named_table,protected]),
    {ok, #state{ets_table_tid = Tid}}.


%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: any(), State :: #state{}) ->
    {ok, Reply :: term(), NewState :: #state{}} |
    {ok, Reply :: term(), NewState :: #state{}, hibernate} |
    {swap_handler, Reply :: term(), Args1 :: term(), NewState :: #state{},
        Handler2 :: (atom() | {atom(), Id :: term()}), Args2 :: term()} |
    {remove_handler, Reply :: term()}).
handle_call({open_socket, udp, Port, _HostName}, _From, State) ->
    {Reply, NewState} =
    case gen_udp:open(Port) of
        {ok, Socket} ->
            {Socket, State#state{udp_socket = Socket}};
        {error, _Reason} ->
            {error, State}
    end,
    {reply, Reply, NewState};
handle_call({open_socket, tcp, Port, HostName}, _From, State) ->
    {Reply, NewState} =
    case gen_tcp:connect(HostName, Port, [{active,once}]) of
        {ok, Socket} ->
            {Socket, State#state{tcp_socket = Socket}};
        {error, _Reason} ->
            {error, State}
    end,
    {reply, Reply, NewState};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {ok, Reply, State}.


%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {ok, Reply :: term(), NewState :: #state{}} |
    {ok, Reply :: term(), NewState :: #state{}, hibernate} |
    {swap_handler, Reply :: term(), Args1 :: term(), NewState :: #state{},
        Handler2 :: (atom() | {atom(), Id :: term()}), Args2 :: term()} |
    {remove_handler, Reply :: term()}).
handle_cast(_Request, State) ->
    Reply = ok,
    {ok, Reply, State}.

%% todo: handle info for messages from udp and tcp
%%--------------------------------------------------------------------
-spec(handle_info(Info :: term(), State :: #state{}) ->
    {ok, NewState :: #state{}} |
    {ok, NewState :: #state{}, hibernate} |
    {swap_handler, Args1 :: term(), NewState :: #state{},
        Handler2 :: (atom() | {atom(), Id :: term()}), Args2 :: term()} |
    remove_handler).
handle_info(_Info, State) ->
    {ok, State}.
%% todo: store the objects down to ets

%%--------------------------------------------------------------------
-spec(terminate(Args :: (term() | {stop, Reason :: term()} | stop |
remove_handler | {error, {'EXIT', Reason :: term()}} |
{error, term()}), State :: term()) -> term()).
terminate(_Arg, _State) ->
    ok.

%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
