%%%-------------------------------------------------------------------
%%% @doc
%%% Test the polling of stats and pushing to a udp amd tcp endpoint
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
    udp_serverip,
    udp_port,
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

get_objects() ->
    gen_server:call(?MODULE, get_objects).

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
handle_call(get_objects, _From, State = #state{
    udp_port =      UDPPort,
    udp_serverip =  UDPServerIP,
    udp_socket =    UDPSocket,
    tcp_socket =    TCPSocket,
    ets_table_tid = Tid
}) ->
    Objects = ets:tab2list(Tid),
    LengthUDPObjs = length(proplists:get_value(
        {udp, UDPSocket, UDPServerIP, UDPPort}, Objects,[])),

    LengthTCPObjs = length(proplists:get_value(
        {tcp, TCPSocket}, Objects,[])),

    Reply = LengthTCPObjs + LengthUDPObjs,
    {ok, Reply, State};
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

%%--------------------------------------------------------------------
-spec(handle_info(Info :: term(), State :: #state{}) ->
    {ok, NewState :: #state{}} |
    {ok, NewState :: #state{}, hibernate} |
    {swap_handler, Args1 :: term(), NewState :: #state{},
        Handler2 :: (atom() | {atom(), Id :: term()}), Args2 :: term()} |
    remove_handler).
handle_info({udp, Socket, IP, Port, Data}, State = #state{ets_table_tid = Tid}) ->
    Object = ets:lookup(Tid, {udp, Socket, IP, Port}),
    ets:insert(Tid, {{udp, Socket, IP, Port}, [Data|Object]}),
    {ok, State#state{udp_socket = Socket, udp_serverip = IP, udp_port = Port}};
handle_info({tcp, Socket, Data}, State = #state{ets_table_tid = Tid}) ->
    Object =  ets:lookup(Tid, {tcp, Socket}),
    ets:insert(Tid, {{tcp, Socket}, [Data|Object]}),
    {ok, State#state{tcp_socket = Socket}};
handle_info(_Info, State) ->
    {ok, State}.

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
