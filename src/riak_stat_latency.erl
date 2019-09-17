%%%-------------------------------------------------------------------
%%% @doc
%%% From "riak admin stat setup ___" to this module, you cna poll the stats
%%% from exometer and setup a server to push those stats to an udp endpoint.
%%% The server can be terminated to stop the pushing of stats.
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_latency).
-include_lib("riak_core/include/riak_core_stat.hrl").

%% API
-export([
    setup/1,
    setdown/1,
    setdown/0,
    get_host/1,
    get_stats/0,
    sanitise_data/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%%-------------------------------------------------------------------
%% @doc
%% the default operation of this function is to start up the pushing and
%% polling of stats from exometer to the UDP endpoint "setup".
%% The ability to pass in an argument gives the added layer of functionality to
%% choose the endpoint details quicker and easier, passing in arguments is optional.
%% sanitise the argument passed in to retrieve the data from console,
%% passing in information is optional as all the defaults are pulled out from
%% the .config.
%% similar to other stats functions the stats can be passed in to only poll
%% those stats, however, only the enabled stats will return with values
%%
%% If the server is already setup and polling stats, setting up another will
%% "restart" the gen_server.
%% @end
%%%-------------------------------------------------------------------
-spec(setup(arg()) -> ok).
setup(Arg) ->
    {{Port, Instance, Sip}, STATSorPROFILES} = sanitise_data(Arg),
    start_server(riak_stat_push, {{Port, Instance, Sip}, STATSorPROFILES}).

start_server(Child, Arg) ->
    riak_core_stats_sup:start_server(Child, Arg).


%%%-------------------------------------------------------------------
%% @doc
%% Kill the udp servers currently running and pushing stats to an endpoint.

%% Stop the pushing of stats by taking the setup of the udp gen_server
%% down
%% @end
%%%-------------------------------------------------------------------
-spec(setdown(arg()) -> ok).
setdown() ->
    setdown([]).
setdown(_Arg) ->
    terminate_server().

terminate_server() ->
    riak_core_stats_sup:stop_server(riak_stat_push).


%%--------------------------------------------------------------------

-spec(sanitise_data(arg()) -> sanitised_data()).
%% @doc
%% Sanitise the data coming in, into the necessary arguments for setting
%% up an endpoint, This is specific to the endpoint functions
%% @end
sanitise_data([]) ->
    io:fwrite("No argument given~n");
sanitise_data(Arg) ->
    [Opts | Stats] = break_up(Arg, "/"),
    List = break_up(Opts, "\\s"),
        case Stats of
            [] -> case riak_stat_console:data_sanitise(List) of
                      [riak|Rest] -> {sanitise_data_([]),[riak|Rest]};
                      _ -> {sanitise_data_(List), ['_']}
                  end;
            Data -> {sanitise_data_(List),riak_stat_console:data_sanitise(Data)}
        end.

break_up(Arg, Str) ->
    re:split(Arg, Str, []).

sanitise_data_(Arg) ->
    sanitise_data_(Arg, ?MONITOR_STATS_PORT, ?INSTANCE, ?MONITOR_SERVER).
sanitise_data_([<<"port=", Po/binary>> | Rest], Port, Instance, Sip) ->
    NewPort =
        case binary_to_integer(Po) of
            {error, _reason} -> Port;
            Int -> Int
        end,
    sanitise_data_(Rest, NewPort, Instance, Sip);
sanitise_data_([<<"instance=", I/binary>> | Rest], Port, _Instance, Sip) ->
    NewInstance = binary_to_list(I),
    sanitise_data_(Rest, Port, NewInstance, Sip);
sanitise_data_([<<"sip=", S/binary>> | Rest], Port, Instance, _Sip) ->
    NewIP = re:split(S, "\\s", [{return, list}]),
    sanitise_data_(Rest, Port, Instance, NewIP);
sanitise_data_([], Port, Instance, Sip) ->
    {Port, Instance, Sip}.



%%%-------------------------------------------------------------------

-type hostarg() :: atom().

%%%-------------------------------------------------------------------
%% @doc
%% get the host details of the udp_socket or http request details, similar to the state
%% in a gen_server but kept in an ets - table to preserve it longer that the udp
%% gen_server, information is pulled out like a last known request
%% @end
%%%-------------------------------------------------------------------
-spec(get_host(hostarg()) -> {socket(), server_ip() | server(), port()}).
get_host(Info) ->
    case ets:lookup(?ENDPOINTTABLE, Info) of
        [{_, {MonitorServer, undefined, MonitorPort, Socket}}] ->
            {Socket, MonitorServer, MonitorPort};
        [{_, {_MonitorServer, MonitorServerIp, MonitorPort, Socket}}] ->
            {Socket, MonitorServerIp, MonitorPort};
        [] ->
            {error, no_info}
    end.


%%%-------------------------------------------------------------------
%% @doc
%% retrieve all the stats out of riak_kv_status
%% todo: improve the stat collection method
%% @end
%%%-------------------------------------------------------------------
-spec(get_stats() -> stats()).
get_stats() ->
    case application:get_env(riak_core, endpoint_defaults_type, classic) of
        classic ->
            riak_kv_status:get_stats(web);
        beta ->
            riak_stat_exom:get_values(['_'])
    end.

-ifdef(TEST).

-endif.
