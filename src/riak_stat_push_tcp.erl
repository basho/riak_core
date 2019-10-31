%%%-------------------------------------------------------------------
%%% @doc
%%% Data from riak_stat_push through console, passed into this
%%% gen_server to open up a tcp socket on a port, Values from exometer
%%% are retrieved and sent as a json object to a latency monitoring
%%% server of "your" choice.
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_push_tcp).
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

-define(TCP_PORT,         8090).
-define(TCP_ADDRESS,      inet_db:gethostname()).
-define(TCP_OPTIONS,      [?TCP_BUFFER,
                           ?TCP_SNDBUFF,
                           ?TCP_ACTIVE,
                           ?TCP_PACKET,
                           ?TCP_REUSE
]).

-define(TCP_BUFFER,       {buffer, 100*1024*1024}).
-define(TCP_SNDBUFF,      {sndbuf,   5*1024*1024}).
-define(TCP_ACTIVE,       {active,          true}).
-define(TCP_PACKET,       {packet,0}).
-define(TCP_REUSE,        {reuseaddr, true}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
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

    case gen_tcp:connect(Sip,?TCP_PORT,?TCP_OPTIONS) of
        {ok, Socket} ->
            self() ! refresh_monitor_server_ip,
            send_after(?STATS_UPDATE_INTERVAL, {dispatch_stats, Stats}),

            {ok, #state{
                socket       = Socket,
                latency_port = MontiorLatencyPort,
                server       = MonitorServer,
                server_ip    = Sip,
                stats_port   = MonitorStatsPort,
                hostname     = Hostname,
                instance     = Instance}
            };
        Error ->
            {stop, Error}
    end.
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
handle_info({dispatch_stats, Stats}, #state{
    socket=Socket, server_ip = undefined, hostname = Hostname,
    instance = Instance} = State) ->
    dispatch_stats(Socket, Hostname, Instance, Stats),
    {noreply, State};
handle_info({dispatch_stats, Stats}, #state{
    socket=Socket, hostname = Hostname, instance = Instance} = State) ->
    dispatch_stats(Socket, Hostname, Instance, Stats),
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

send(Socket, Data) ->
    gen_tcp:send(Socket, Data).

send_after(Interval, Arg) ->
    erlang:send_after(Interval,self(),Arg).

dispatch_stats(Socket, ComponentHostname, Instance, Stats) ->
    case riak_stat_push_util:json_stats(ComponentHostname, Instance, Stats) of
        ok ->
            send_after(?STATS_UPDATE_INTERVAL, {dispatch_stats, Stats});
        JsonStats ->
            send(Socket, JsonStats),
            send_after(?STATS_UPDATE_INTERVAL, {dispatch_stats, Stats})
    end.
