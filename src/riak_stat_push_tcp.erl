%%%-------------------------------------------------------------------
%%% @doc
%%% Data from riak_stat_push through console, passed into this
%%% gen_server to open up a tcp socket on a port, Values from exometer
%%% are retrieved and sent as a json object to a latency monitoring
%%% server
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_push_tcp).
-include_lib("riak_core/include/riak_stat_push.hrl").

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    socket        :: socket(),
    server        :: server_ip(),
    latency_port  :: push_port(),
    server_ip     :: server_ip(),
    stats_port    :: push_port(),
    hostname      :: hostname(),
    instance      :: instance(),
    stats         :: listofstats()
}).

-define(TCP_PORT,         0).
-define(TCP_ADDRESS,      inet_db:gethostname()).
-define(TCP_OPTIONS,      [?TCP_BUFFER,
                           ?TCP_SNDBUFF,
                           ?TCP_ACTIVE,
                           ?TCP_PACKET,
                           ?TCP_REUSE]).

-define(TCP_BUFFER,       {buffer, 100*1024*1024}).
-define(TCP_SNDBUFF,      {sndbuf,   5*1024*1024}).
-define(TCP_ACTIVE,       {active,          true}).
-define(TCP_PACKET,       {packet,0}).
-define(TCP_REUSE,        {reuseaddr, true}).

%%TODO copy UDP server

%%%===================================================================
%%% API
%%%===================================================================
-spec(start_link(Arg :: term()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Obj) ->
    gen_server:start_link(?MODULE, Obj, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([{{MontiorLatencyPort,Instance,[Sip]},Stats}]) ->
    MonitorStatsPort = ?MONITORSTATSPORT,
    MonitorServer    = ?MONITORSERVER,
    Hostname         = ?TCP_ADDRESS,
    State = #state{latency_port = MontiorLatencyPort,
                    server_ip = Sip,
        server = MonitorServer,
        hostname = Hostname,
        instance = Instance,
        stats_port = MonitorStatsPort,
        stats = Stats},
    case gen_tcp:connect(Sip,MontiorLatencyPort,?TCP_OPTIONS) of
        {ok, Socket} ->
            lager:info("Server Started: ~p",[Instance]),
            self() ! refresh_monitor_server_ip,
            send_after(?STATS_UPDATE_INTERVAL, {dispatch_stats, Stats}),

            {ok, State#state{socket = Socket}
            };
        Error ->
            lager:info("Gen server stopped Setting up : ~p  because : ~p",[Instance,Error]),
            {stop,Error} %% Cannot Start because Socket was not returned.
    end.
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{},timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(refresh_monitor_server_ip, State = #state{server = MonitorServer}) ->
    NewState = case inet:gethostbyname(MonitorServer) of
                   {ok,{hostent,_Hostname,_,_,_, [MonitorServerIp]}} ->
                       State#state{server_ip = MonitorServerIp};
                   Other ->
                       lager:warning(
                           "Unable to refresh ip address of monitor server due to ~p,
                            retrying in ~p ms~n",
                           [Other, ?REFRESH_INTERVAL]),
                       State
               end,
    send_after(?REFRESH_INTERVAL, refresh_monitor_server_ip),
    {noreply, NewState};
handle_info({dispatch_stats, Stats}, #state{
    socket=Socket, server_ip = undefined, hostname = Hostname,
    instance = Instance} = State) ->
    dispatch_stats(Socket, Hostname, Instance, Stats),
    {noreply, State};
handle_info({dispatch_stats, Stats}, #state{
    socket=Socket, hostname = Hostname, instance = Instance} = State) ->
    dispatch_stats(Socket, Hostname, Instance, Stats),
    {noreply, State};
handle_info(tcp_closed,State) ->
    %% todo: try to initiate connection again move the open socket to its own function
    {stop, endpoint_closed, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(endpoint_closed, #state{instance = Instance}) ->
    lager:error("Connection Closed on other end, terminating : ~p",[Instance]),
    terminate_server(Instance),
    ok;
terminate(manual, #state{instance = Instance}) ->
    terminate_server(Instance),
    ok;
terminate(restart_failed, _State) ->
    %% todo: running , restart failed.
    ok;
terminate(_Reason, _State) ->
    %% todo: close TCP connection %% do the same for udp
    ok.

%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================



-spec(send(socket(),jsonstats()) -> ok | error()).
send(Socket, Data) ->
    gen_tcp:send(Socket, Data).

send_after(Interval, Arg) ->
    erlang:send_after(Interval,self(),Arg).

%%--------------------------------------------------------------------
%% @doc
%% Retrieve the stats from exometer and convert to json object, to
%% send to the endpoint. Repeat.
%% @end
%%--------------------------------------------------------------------
-spec(dispatch_stats(socket(),hostname(),instance(),metrics()) -> ok | error()).
dispatch_stats(Socket, ComponentHostname, Instance, Stats) ->
    case riak_stat_push_util:json_stats(ComponentHostname, Instance, Stats) of
        ok ->
            send_after(?STATS_UPDATE_INTERVAL, {dispatch_stats, Stats});
        JsonStats ->
            send(Socket, JsonStats),
            send_after(?STATS_UPDATE_INTERVAL, {dispatch_stats, Stats})
    end.

set_running_false_fun() ->
    fun({ODT,_MDT,_Pid,{running,true},Node,Port,Sip,Stats}) ->
        {ODT,calendar:universal_time(),undefined,{running,false},Node,Port,Sip,Stats}
    end.

terminate_server(Instance) ->
    Protocol = tcp,
    Key = {Protocol, Instance},
    Prefix = {riak_stat_push, node()},
    Fun = set_running_false_fun(),
    riak_stat_meta:put(Prefix,Key,Fun).
