%%%-------------------------------------------------------------------
%%% @doc
%%% From "riak admin push setup ___" to this module, you can poll the stats
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

-define(PUSHPREFIX, {riak_stat_push, term_to_binary(node())}).
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
        {Protocol,SanitisedData} ->
            Pid = start_server(Protocol,SanitisedData),
            store_setup_info(Pid, Protocol,SanitisedData)
    end.

store_setup_info(Pid, Protocol, {{Port, Instance, Sip},Stats}) ->
    Prefix = ?PUSHPREFIX,
    {Date0,Time0} = calendar:local_time(),
    Date = tuple_to_list(Date0), Time = tuple_to_list(Time0),
    Key = {Date,Time,Protocol},
    Value = {Port,Sip,Instance,Pid,{running,true},Stats},
    riak_core_metadata:put(Prefix,Key,Value).

start_server(undefined, {{_P, _I, _S}, _St}) ->
    io:format("No Protocol type entered~n");
start_server(_Pr, {{undefined, _I, _S},_St}) ->
    io:format("No Port type entered~n");
start_server(_Pr, {{_P, undefined, _S},_St}) ->
    io:format("No Instance entered~n");
start_server(_Pr, {{_P, _I, undefined},_St}) ->
    io:format("Wrong Server Ip entered~n");
start_server(udp, Data) ->
    riak_core_stats_sup:start_server(riak_stat_push_udp, Data);
start_server(tcp, Arg) ->
    riak_core_stats_sup:start_server(riak_stat_push_tcp, Arg);
start_server(http, _Arg) ->
    io:format("Error Unsupported : ~p~n",[http]);
start_server(Protocol, _Arg) ->
    io:format("Error wrong type : ~p~n",[Protocol]).

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
setdown([]) ->
    io:format("No Argument entered ~n");
setdown(Arg) ->
    %%todo: put down a server depending on the information given. if instance is
    %% given then it is all that is needed to terminate, if the port is given,
    %% then the sip is needed
    %% if the sip is given then the port is needed,
    {Protocol, SanitisedData} = sanitise_data(Arg),
    terminate_server(Protocol, SanitisedData).

terminate_server(Protocol, {{Port,Instance,ServerIp},_Stats}) ->
    ok;
terminate_server(Protocol,SanitisedData) ->
    riak_core_stats_sup:stop_server(riak_stat_push_udp).

foldinate(_,{{undefined,_I,_S},_St})->
    ok;
foldinate(undefined,SanitisedData) ->
    foldinate('_',SanitisedData);
foldinate(Protocol,{{Port,Instance,ServerIp},_Stats}) ->
    riak_core_metadata:fold(
        fun
            ({MDate,MTime,MProtocol}, [{MPort,MSip,MInst,MPid,MRunTuple,MStats}],
                {APort,AInstance,AServerIP})
                when (APort == '_' orelse APort == MPort) andalso
                 (AInstance == '_' orelse AInstance == MInst) andalso
                 (AServerIP == '_' orelse AServerIP == MSip) ->
                

                                    ok
        end,
        {Port,Instance,ServerIp},?PUSHPREFIX, [{match, {'_','_',Protocol}}]
    ).

%%--------------------------------------------------------------------
%% todo: make it so the sip instance and port should be added in,
%% should not be undefined in the gen_server, otherwise fail.
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
    data_sanitise_2(List,Stats).
%%
%%    case Stats of
%%        [] -> case riak_stat_console:data_sanitise(List) of
%%                  {[riak|Rest],_,_,_} ->
%%                      %% if only the stats needed is passed in:
%%                      {data_sanitise_2_electric_boogaloo([]),[riak|Rest]};
%%                  _ ->
%%                      {data_sanitise_2_electric_boogaloo(List), ['_']}
%%              end;
%%        Data ->
%%            {data_sanitise_2_electric_boogaloo(List),
%%                riak_stat_console:data_sanitise(Data)}
%%    end. %% sanitise the data and look for the protocol, if there is no
        %% todo: if no protocol (udp/tcp) is not specified, return error.
        %% todo: in the sanitisation perform the staritng of the server?

break_up(Arg, Str) ->
    re:split(Arg, Str).

data_sanitise_2(List,[]) ->
    case data_sanitise_2_electric_boogaloo(List) of
        ok -> ok;
        {Protocol, Port, Instance, ServerIp} ->
            {Protocol, {{Port, Instance, ServerIp},['_']}}
    end;
data_sanitise_2(List,Stats) ->
    case data_sanitise_2_electric_boogaloo(List) of
        ok -> ok;
        {Protocol, Port, Instance, ServerIp} ->
            {Protocol, {{Port, Instance, ServerIp},
                riak_stat_console:data_sanitise(Stats)}}
    end.

data_sanitise_2_electric_boogaloo(Arg) ->
    data_sanitise_2_electric_boogaloo(
        Arg, undefined,undefined,undefined,undefined).
data_sanitise_2_electric_boogaloo([<<"protocol=", Pr/binary>>|Rest],
    Protocol, Port, Instance, ServerIp) ->
    NewProtocol =
    case Pr of
        <<"udp">> -> udp;
        <<"tcp">> -> tcp;
        <<"http">> -> http;
        _         -> Protocol
    end,
    data_sanitise_2_electric_boogaloo(Rest,
        NewProtocol,Port, Instance,ServerIp);
data_sanitise_2_electric_boogaloo([<<"port=", Po/binary>> | Rest],
    Protocol, Port, Instance, ServerIp) ->
    NewPort =
        try binary_to_integer(Po) of
            Int -> Int
        catch _e:_r -> Port
        end,
    data_sanitise_2_electric_boogaloo(Rest,
        Protocol, NewPort, Instance, ServerIp);

data_sanitise_2_electric_boogaloo([<<"instance=", I/binary>> | Rest],
    Protocol, Port, _Instance, ServerIp) ->
    NewInstance = binary_to_list(I),
    data_sanitise_2_electric_boogaloo(Rest, Protocol, Port, NewInstance, ServerIp);

data_sanitise_2_electric_boogaloo([<<"sip=", S/binary>> | Rest],
    Protocol, Port, Instance, _ServerIp) ->
    NewIP = re:split(S, "\\s", [{return, list}]),
    data_sanitise_2_electric_boogaloo(Rest,Protocol,Port, Instance, NewIP);
data_sanitise_2_electric_boogaloo([],Protocol, Port, Instance, ServerIp) ->
    {Protocol,Port, Instance, ServerIp}.

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