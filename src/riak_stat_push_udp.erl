%%%-------------------------------------------------------------------
%%% @doc
%%% Data given from riak_stat_push through console/config, passed
%%% into this gen_server to open a udp socket on a Port. Values from
%%% Exometer are retrieved and sent as a Json object to a latency monitoring
%%% server
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_push_udp).
-include_lib("riak_core/include/riak_stat.hrl").

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
    server        :: server(),
    latency_port  :: latency_port(),
    server_ip     :: server_ip(),
    stats_port    :: stats_port(),
    hostname      :: hostname(),
    instance      :: instance()
}).

-define(UDP_OPEN_PORT,         10029).
-define(UDP_OPTIONS,           [?UDP_OPEN_BUFFER,
                                ?UDP_OPEN_SNDBUFF,
                                ?UDP_OPEN_ACTIVE]).
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

    MonitorStatsPort   = ?MONITORSTATSPORT,
    MonitorServer      = ?MONITORSERVER,
    Hostname           = inet_db:gethostname(),
    Socket             = open(),

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
handle_info({dispatch_stats, Stats}, #state{socket=Socket, stats_port = Port,
    server_ip = undefined, server = Server, hostname = Hostname,
    instance = Instance} = State) ->
    dispatch_stats(Socket, Hostname, Instance, Server, Port, Stats),
    {noreply, State};
handle_info({dispatch_stats, Stats}, #state{socket=Socket, stats_port = Port,
    server_ip = ServerIp, hostname = Hostname, instance = Instance} = State) ->
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
    erlang:send_after(Interval,self(),Arg).

open() ->
    {ok, Socket} =
        gen_udp:open(?UDP_OPEN_PORT, ?UDP_OPTIONS),
    Socket.


dispatch_stats(Socket, ComponentHostname, Instance, MonitoringHostname, Port, Stats) ->
    case riak_stat_push_util:json_stats(ComponentHostname, Instance, Stats) of
        ok ->
            send_after(?STATS_UPDATE_INTERVAL, {dispatch_stats, Stats});
        JsonStats ->
            send(Socket, MonitoringHostname, Port, JsonStats),
            send_after(?STATS_UPDATE_INTERVAL, {dispatch_stats, Stats})
    end.
