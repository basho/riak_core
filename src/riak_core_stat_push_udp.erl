%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(riak_core_stat_push_udp).
-include("riak_stat_push.hrl").

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
    hostname      :: hostname(),
    instance      :: instance(),
    stats         :: listofstats()
}).

-define(PROTOCOL, udp).

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

-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([Options]) ->
    case open(Options) of
        {ok, State} ->
            {ok, State};
        {error, Error} ->
            lager:error("Error starting ~p because: ~p", [?MODULE, Error]),
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
                       State#state{server = MonitorServerIp};
                   Other ->
                       lager:warning(
                           "Unable to refresh ip address of monitor server due to ~p,
                            retrying in ~p ms~n",
                           [Other, ?REFRESH_INTERVAL]),
                       State
               end,
    refresh_monitor_server_ip(),
    {noreply, NewState};
handle_info(push_stats, #state{
    socket       = Socket,
    latency_port = Port,
    stats        = Stats,
    hostname     = Hostname} = State) ->
    push_stats(Socket, Hostname, Port, Stats),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(shutdown, #state{instance = Instance}) ->
    lager:info("Stopping ~p",[Instance]),
    terminate_server(Instance),
    ok;
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


open({{Port, _Instance, _Sip}, _Stats}=Info) ->
    Options = ?OPTIONS,
    case gen_udp:open(Port, Options) of
        {ok, Socket} ->
            State = create_state(Socket, Info),
            refresh_monitor_server_ip(),
            push_stats(),
            {ok, State};
        Error ->
            {error, Error}
    end.

create_state(Socket, {{MonitorLatencyPort, Instance, Sip}, Stats}) ->
    Hostname          = inet_db:gethostname(),
    #state{
        socket        = Socket,
        latency_port  = MonitorLatencyPort,
        server        = Sip,
        hostname      = Hostname,
        instance      = Instance,
        stats         = Stats}.

refresh_monitor_server_ip() ->
    send_after(?REFRESH_INTERVAL, refresh_monitor_server_ip).

%%--------------------------------------------------------------------

terminate_server(Instance) ->
    Key = {?PROTOCOL, Instance},
    Prefix = {riak_stat_push, node()},
    riak_core_stat_push_sup:stop_running_server(Prefix,Key).

%%--------------------------------------------------------------------

send(Socket, Host, Port, Data) ->
    gen_udp:send(Socket, Host, Port, Data).

send_after(Interval, Arg) ->
    erlang:send_after(Interval,self(),Arg).

%%--------------------------------------------------------------------
%% @doc
%% Retrieve the stats from exometer and convert to json object, to
%% send to the endpoint. Repeat.
%% @end
%%--------------------------------------------------------------------
push_stats(Socket, ComponentHostname, Port, Stats) ->
    JsonStats = riak_core_stat_push_util:json_stats(Stats),
    send(Socket, ComponentHostname, Port, JsonStats),
    push_stats().

push_stats() ->
    send_after(?STATS_UPDATE_INTERVAL, push_stats).
