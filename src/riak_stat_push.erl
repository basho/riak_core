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

-export([notify/1, notify/2, notify/3, get_host/0]).

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
%% @doc
%% Notify the udp latency monitor
%% @end
%%--------------------------------------------------------------------
-spec(notify(jsonprops()) -> ok).
notify(JsonProps) when is_list(JsonProps) ->
    DateTime = format_time(),
    try
        case get_host() of
            {Socket, Host, Port} ->
                Data = build_data_packet(JsonProps, DateTime),
                send(Socket, Host, Port, Data);
            {error, no_udp_socket} ->
                lager:error("No UDP socket for sending latency"),
                ok
        end
    catch Class:Reason  ->
        lager:error("Unable to log latency for json=~p, timestamp=~s, Reason={~p,~p}, Stacktrace=~p~n",
            [JsonProps, DateTime, Class, Reason, erlang:get_stacktrace()])
    end;
notify(JsonProps) ->
    lager:error("Unknown format of JsonProps=~p~n", [JsonProps]).

-spec(notify(serviceid(), correlationid()) -> ok).
notify(ServiceId, CorrelationId)
    when is_list(ServiceId) orelse is_binary(ServiceId)
    andalso is_list(CorrelationId) orelse is_binary(CorrelationId) ->
    notify([
        {service_id, ServiceId},
        {correlation_id, CorrelationId}
    ]);
notify(ServiceId, CorrelationId) ->
    lager:error("Unknown format ServiceId=~p, CorrelationId=~p~n", [ServiceId, CorrelationId]).

-spec(notify(serviceid(), correlationid(), jsonprops()) -> ok).
notify(ServiceId, CorrelationId, JsonProps)
    when is_list(ServiceId) orelse is_binary(ServiceId)
    andalso is_list(CorrelationId) orelse is_binary(CorrelationId)
    andalso is_list(JsonProps) ->
    notify([
        {service_id, ServiceId},
        {correlation_id, CorrelationId}
        | JsonProps
    ]);
notify(ServiceId, CorrelationId, JsonProps) ->
    lager:error("Unknown format ServiceId=~p, CorrelationId=~p, JsonProps=~p~n",
        [ServiceId, CorrelationId, JsonProps]).


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
format_time() ->
    format_time(os:timestamp()).

format_time(Time) ->
    riak_stat_json:format_time(Time).

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

get_host() ->
    riak_stat_latency:get_host(udp_info).

build_data_packet(Props, DateTime) ->
    [${, riak_stat_json:format_fields([{timestamp, DateTime}|Props], []), $}].

get_stats(Stats) ->
    riak_stat_exom:get_values(Stats).



-ifdef(TEST).

build_data_packet_test() ->
    DateTime = format_time({1429,703171,905719}),
    ?assertEqual("{\"timestamp\":\"2015-04-22T11:46:11.905Z\",\"service_id\":\"test\",\"correlation_id\":\"document_1\"}",
        lists:flatten(build_data_packet([{service_id, "test"}, {correlation_id, document_1}], DateTime))),

    ?assertEqual("{\"timestamp\":\"2015-04-22T11:46:11.905Z\",\"service_id\":\"test\",\"correlation_id\":\"document_1\",\"wibble\":1}",
        lists:flatten(build_data_packet([{service_id, "test"}, {correlation_id, document_1}|[{wibble, 1}]], DateTime))),

    ?assertEqual("{\"timestamp\":\"2015-04-22T11:46:11.905Z\",\"service_id\":\"test\",\"correlation_id\":\"document_1\",\"wobble\":[\"hello\",\"world\"],\"wibble\":1}",
        lists:flatten(build_data_packet([{service_id, "test"}, {correlation_id, document_1},{wobble, [<<"hello">>, <<"world">>]}|[{wibble, 1}]], DateTime))),

    ?assertEqual("{\"timestamp\":\"2015-04-22T11:46:11.905Z\",\"service_id\":\"test\",\"correlation_id\":\"document_1\"}",
        lists:flatten(build_data_packet([{service_id, "test"}, {correlation_id, "document_1"}], DateTime))),

    ?assertEqual("{\"timestamp\":\"2015-04-22T11:46:11.905Z\",\"service_id\":\"test\",\"correlation_id\":\"document_1\"}",
        lists:flatten(build_data_packet([{service_id, <<"test">>}, {correlation_id, <<"document_1">>}], DateTime))),

    ?assertEqual("{\"timestamp\":\"2015-04-22T11:46:11.905Z\",\"service_id\":1,\"correlation_id\":2}",
        lists:flatten(build_data_packet([{service_id, 1}, {correlation_id, 2}], DateTime))).

build_stats_test() ->
    JsonStats = riak_stat_json:metrics_to_json([{[wibble, wobble], [{95, 23}, {n, 290}]}], [], []),
    ?debugFmt("~s", [JsonStats]).

-endif.


-ifdef(TEST).

-endif.
