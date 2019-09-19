%%%-------------------------------------------------------------------
%%% @doc
%%% Data given from riak_stat_console through console/config, passed
%%% into this gen_server to open a udp socket on a Port. Values from
%%% Exometer are retrieved and sent as a Json object to a latency monitoring
%%% server
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_push).
-include_lib("riak_core/include/riak_stat.hrl").

-behaviour(gen_server).

-export([get_host/0]).

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
    server        :: server(),
    latency_port  :: latency_port(),
    server_ip     :: server_ip(),
    stats_port    :: stats_port(),
    hostname      :: hostname(),
    instance      :: instance()
}).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(ENVAPP, riak_core).

-define(INSTANCE,              app_helper:get_env(?ENVAPP, instance)).
-define(MONITOR_SERVER,        app_helper:get_env(?ENVAPP, monitor_server)).
-define(MONITOR_LATENCY_PORT,  app_helper:get_env(?ENVAPP, monitor_latency_port)).
-define(MONITOR_STATS_PORT,    app_helper:get_env(?ENVAPP, monitor_stats_port)).

-define(EXCLUDED_DATAPOINTS,   app_helper:get_env(?ENVAPP, endpoint_excluded_datapoints, [ms_since_reset])).
-define(STATS_LISTEN_PORT,     app_helper:get_env(?ENVAPP, stats_listen_port, 9000)).

-define(ENDPOINTTABLE,         endpoint_state).
-define(UDP_KEY,               udp_socket).
-define(WM_KEY,                http_socket).

-define(STATS_UPDATE_INTERVAL, app_helper:get_env(?ENVAPP, endpoint_stats_update_interval, 1000)).
-define(REFRESH_INTERVAL,      app_helper:get_env(?ENVAPP, endpoint_ip_refresh_interval, 30000)).

-define(SPIRAL_TIME_SPAN,      app_helper:get_env(?ENVAPP, endpoint_stats_spiral_time_span, 1000)).
-define(HISTOGRAM_TIME_SPAN,   app_helper:get_env(?ENVAPP, endpoint_stats_histogram_time_span, 1000)).

-define(UDP_OPEN_PORT,         10029).
-define(UDP_OPEN_BUFFER,       {buffer, 100*1024*1024}).
-define(UDP_OPEN_SNDBUFF,      {sndbuf,   5*1024*1024}).
-define(UDP_OPEN_ACTIVE,       {active,         false}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
-spec(start_link(Term::arg()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Obj) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Obj, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init({{MonitorLatencyPort, Instance, Sip}, Stats}) ->

    MonitorServer      = ?MONITOR_SERVER,
    MonitorStatsPort   = ?MONITOR_STATS_PORT,
    Hostname           = inet_db:gethostname(),
    Socket             = open(),
    ets:new(?ENDPOINTTABLE,[named_table]),
    ets_insert({MonitorServer, Sip, MonitorLatencyPort, Socket}),

    self() ! refresh_monitor_server_ip,
    send_after(?STATS_UPDATE_INTERVAL, {dispatch_stats, Stats}),

    {ok, #state{
        socket        = Socket,
        latency_port  = MonitorLatencyPort,
        server        = MonitorServer,
        server_ip     = Sip,
        stats_port    = MonitorStatsPort,
        hostname      = Hostname,
        instance      = Instance}
    }.

%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
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
handle_info(refresh_monitor_server_ip, State = #state{socket = Socket, latency_port = LatencyMonitorPort, server = MonitorServer}) ->
    NewState = case inet:gethostbyname(MonitorServer) of
                   {ok,{hostent,_Hostname,_,_,_, [MonitorServerIp]}} ->
                       true = ets_insert({MonitorServer, MonitorServerIp, LatencyMonitorPort, Socket}),
                       State#state{server_ip = MonitorServerIp};
                   Other ->
                       lager:warning("Unable to refresh ip address of monitor server due to ~p, retrying in ~p ms~n", [Other, ?REFRESH_INTERVAL]),
                       State
               end,
    send_after(?REFRESH_INTERVAL, refresh_monitor_server_ip),
    {noreply, NewState};
handle_info({dispatch_stats, Stats}, #state{socket=Socket, stats_port = Port, server_ip = undefined, server = Server, hostname = Hostname, instance = Instance} = State) ->
    dispatch_stats(Socket, Hostname, Instance, Server, Port, Stats),
    {noreply, State};
handle_info({dispatch_stats, Stats}, #state{socket=Socket, stats_port = Port, server_ip = ServerIp, hostname = Hostname, instance = Instance} = State) ->
    dispatch_stats(Socket, Hostname, Instance, ServerIp, Port, Stats),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
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



send(Socket, Host, Port, Data) ->
    gen_udp:send(Socket, Host, Port, Data).

send_after(Interval, Arg) ->
    erlang:send_after(Interval, self(), Arg).

open() ->
    {ok, Socket} =
        gen_udp:open(?UDP_OPEN_PORT,
            [?UDP_OPEN_BUFFER, ?UDP_OPEN_SNDBUFF, ?UDP_OPEN_ACTIVE]),
    Socket.

ets_insert(Arg) ->
    ets:insert(?ENDPOINTTABLE, {?UDP_KEY, Arg}).

dispatch_stats(Socket, ComponentHostname, Instance, MonitoringHostname, Port, Stats) ->
    Metrics = get_stats(Stats),
    case riak_stat_json:metrics_to_json(Metrics,
        [{instance, Instance},{hostname, ComponentHostname}], ?EXCLUDED_DATAPOINTS) of
        [] ->
            ok;
        JsonStats ->
            ok = send(Socket, MonitoringHostname, Port, JsonStats)
    end,

    erlang:send_after(?STATS_UPDATE_INTERVAL, self(), {dispatch_stats, Stats}).


get_stats(Stats) ->
    riak_stat_exom:get_values(Stats).



