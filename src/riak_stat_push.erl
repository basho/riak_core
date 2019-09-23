%%%-------------------------------------------------------------------
%%% @doc
%%% From "riak admin push setup ___" to this module, you cna poll the stats
%%% from exometer and setup a server to push those stats to an udp endpoint.
%%% The server can be terminated to stop the pushing of stats.
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_push).
-include_lib("riak_core/include/riak_stat.hrl").

%% API
-export([
    setup/1,
    setdown/1,
    setdown/0,
    get_stats/0,
    sanitise_data/1
]).

%% todo: make it so the sip instance and port should be added in,
%% should not be undefined in the gen_server, otherwise fail.

%% todo: add choice of protocol for udp or tcp

%%%-------------------------------------------------------------------
%% @doc
%% the default operation of this function is to start up the pushing
%% and polling of stats from exometer to the UDP endpoint "setup".
%% The ability to pass in an argument gives the added layer of
%% functionality to choose the endpoint details quicker and easier,
%% passing in arguments is optional. sanitise the argument passed in
%% to retrieve the data from console, passing in information is
%% optional as all the defaults are pulled out from the .config.
%% similar to other stats functions the stats can be passed in to only
%% poll those stats, however, only the enabled stats will return with
%% values
%%
%% If the server is already setup and polling stats, setting up
%% another will "restart" the gen_server.
%% @end
%%%-------------------------------------------------------------------
-spec(setup(arg()) -> ok).
setup(Arg) ->
    case sanitise_data(Arg) of
        ok -> ok; %% io:format output
        SanitisedData ->
            start_server(riak_stat_push_udp,SanitisedData)
    end. %% todo start a server based on the protocol, udp/tcp

%% todo: store the information about the setting up of stats, similar to
%% the profiles in the riak_core_metadata.

%% saving it as a profile is helpful for the specific stats set up of
%% that cluster of riak etc....

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
    riak_core_stats_sup:stop_server(riak_stat_push_udp).

%%--------------------------------------------------------------------

-spec(sanitise_data(arg()) -> sanitised()).
%% @doc
%% Sanitise the data coming in, into the necessary arguments for setting
%% up an endpoint, This is specific to the endpoint functions
%% @end
sanitise_data([]) ->
    io:fwrite("No argument given~n");
sanitise_data(Arg) ->
    [Opts | Stats] = break_up(Arg, "/"),
    List = break_up(Opts, ","),
    case Stats of
        [] -> case riak_stat_console:data_sanitise(List) of
                  {[riak|Rest],_,_} ->
                      %% if only the stats needed is passed in:
                      {data_sanitise_2_electric_boogaloo([]),[riak|Rest]};
                  _ ->
                      {data_sanitise_2_electric_boogaloo(List), ['_']}
              end;
        Data ->
            {data_sanitise_2_electric_boogaloo(List),
                riak_stat_console:data_sanitise(Data)}
    end. %% sanitise the data and look for the protocol, if there is no
        %% todo: if no protocol (udp/tcp) is not specified, return error.
        %% todo: in the sanitisation perform the staritng of the server?

break_up(Arg, Str) ->
    re:split(Arg, Str).

data_sanitise_2_electric_boogaloo(Arg) ->
    data_sanitise_2_electric_boogaloo(Arg,
        ?MONITOR_LATENCY_PORT, ?INSTANCE, ?MONITOR_SERVER).

data_sanitise_2_electric_boogaloo([<<"port=", Po/binary>> | Rest],
    Port, Instance, ServerIp) ->
    NewPort =
        try binary_to_integer(Po) of
            Int -> Int
        catch _e:_r -> Port
        end,
    data_sanitise_2_electric_boogaloo(Rest,
        NewPort, Instance, ServerIp);

data_sanitise_2_electric_boogaloo([<<"instance=", I/binary>> | Rest],
    Port, _Instance, ServerIp) ->
    NewInstance = binary_to_list(I),
    data_sanitise_2_electric_boogaloo(Rest, Port, NewInstance, ServerIp);

data_sanitise_2_electric_boogaloo([<<"sip=", S/binary>> | Rest],
    Port, Instance, _ServerIp) ->
    NewIP = re:split(S, "\\s", [{return, list}]),
    data_sanitise_2_electric_boogaloo(Rest, Port, Instance, NewIP);

data_sanitise_2_electric_boogaloo([], Port, Instance, ServerIp) ->
    {Port, Instance, ServerIp}.

%%%-------------------------------------------------------------------
%% @doc
%% retrieve all the stats out of riak_kv_status
%% todo: improve the stat collection method
%% @end
%%%-------------------------------------------------------------------
-spec(get_stats() -> stats()).
get_stats() ->
    case application:get_env(riak_core, push_defaults_type, classic) of
        classic ->
            riak_kv_status:get_stats(web);
        beta ->  %% get all stats and their values
            riak_stat_exom:get_values(['_'])
    end.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.