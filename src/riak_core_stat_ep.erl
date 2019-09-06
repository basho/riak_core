%%%-------------------------------------------------------------------
%%% @doc
%%% From "riak admin stat setup ___" to this module, you cna poll the stats
%%% from exometer and setup a server to push those stats to an udp endpoint.
%%% The server can be terminated to stop the pushing of stats.
%%% @end
%%%-------------------------------------------------------------------
-module(riak_core_stat_ep).
-include_lib("riak_core/include/riak_core_stat.hrl").

%% Exports
-export([
    setup/1,
    setdown/1,
    setdown/0,
    get_host/1,
    get_stats/0
]).
-spec(setup(arg()) -> ok).
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
setup(Arg) ->
    {{Port, Instance, Sip}, STATSorPROFILES} = sanitise_data(Arg),
    start_server(riak_core_stat_latency, {{Port, Instance, Sip}, STATSorPROFILES}).


-spec(setdown(arg()) -> ok).
%% @doc
%% Kill the udp servers currently running and pushing stats to an endpoint.

%% Stop the pushing of stats by taking the setup of the udp gen_server
%% down
%% @end
setdown() ->
    setdown([]).
setdown(_Arg) ->
    terminate_server().



sanitise_data([<<>>]) -> sanitise_data([]);
sanitise_data(Arg) -> riak_core_stat_data:sanitise_data(Arg).

start_server(Child, Arg) ->
%%  case get_child() of
%%    {error, Reason} ->
%%      lager:error("Couldn't find Children because: ~p~n", [Reason]);
%%    ChildRef ->
%%      Ids = [Id || {Id, _Child, _Type, _Mods} <- ChildRef],
%%      terminate_server(Ids),
    riak_core_stats_sup:start_server(Child, Arg).
%%  end.


%%terminate_server(Arg) ->/.
%%  riak_core_stats_sup:stop_server(Arg).
terminate_server() ->
    riak_core_stats_sup:stop_server(riak_core_stat_latency).


%%-------------------------------------------------------------------
-spec(get_host(hostarg()) -> {socket(), server_ip() | server(), port()}).
%% @doc
%% get the host details of the udp_socket or http request details, similar to the state
%% in a gen_server but kept in an ets - table to preserve it longer that the udp
%% gen_server, information is pulled out like a last known request
%% @end
get_host(Info) ->
    case ets:lookup(?ENDPOINTTABLE, Info) of
        [{_, {MonitorServer, undefined, MonitorPort, Socket}}] ->
            {Socket, MonitorServer, MonitorPort};
        [{_, {_MonitorServer, MonitorServerIp, MonitorPort, Socket}}] ->
            {Socket, MonitorServerIp, MonitorPort};
        [] ->
            {error, no_info}
    end.

-spec(get_stats() -> stats()).
%% @doc
%% retrieve all the stats out of riak_kv_status
%% todo: improve the stat collection method
%% @end
get_stats() ->
    case application:get_env(riak_core, endpoint_defaults_type, classic) of
        classic ->
            riak_kv_status:get_stats(web);
        beta ->
            riak_core_stat_coordinator:get_values(['_'])
    end.