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
    sanitise_data/1,
    find_persisted_data/1
]).

-define(PUSHPREFIX, {riak_stat_push, term_to_binary(node())}).

%%%-------------------------------------------------------------------
%% @doc
%% the default operation of this function is to start up the pushing
%% / polling of stats from exometer to the UDP/TCP endpoint.
%% The ability to pass in an argument gives the added layer of
%% functionality to choose the endpoint details quicker and easier,
%%
%% If the server is already setup and polling stats, setting up
%% another will start up another gen_server.
%% @end
%%%-------------------------------------------------------------------
-spec(setup(arg()) -> ok).
setup(Arg) ->
    case sanitise_data(Arg) of
        ok -> ok; %% ok == io:format output i.e. error msg
        {Protocol,SanitisedData} ->
            case check_args(Protocol,SanitisedData) of
                ok -> ok;
                {Protocol,SanitisedData} ->
                    Pid = start_server(Protocol,SanitisedData),
                    store_setup_info(Pid, Protocol,SanitisedData)
            end
    end.

store_setup_info(Pid, Protocol, {{Port, Instance, Sip},Stats}) ->
    Prefix = ?PUSHPREFIX,
    {Date0,Time0} = calendar:local_time(),
    Date = tuple_to_list(Date0), Time = tuple_to_list(Time0),
    Key = {Date,Time,Protocol}, %% Key is tuple-object
    Value =
        #{port     => Port,
          serverip => Sip,
          instance => Instance,
          pid      => Pid,
          running  => true,
          stats    => Stats}, %% Value is a map
    riak_stat_meta:put(Prefix,Key,Value).


start_server(udp, Data) ->
    riak_core_stats_sup:start_server(riak_stat_push_udp, Data);
start_server(tcp, Arg) ->
    riak_core_stats_sup:start_server(riak_stat_push_tcp, Arg);
start_server(http, _Arg) ->
    io:format("Error Unsupported : ~p~n",[http]);
start_server(Protocol, _Arg) ->
    io:format("Error wrong type : ~p~n",[Protocol]).

check_args('_', {{_P, _I, _S}, _St}) ->
    io:format("No Protocol type entered~n");
check_args(_Pr, {{'_', _I, _S},_St}) ->
    io:format("No Port type entered~n");
check_args(_Pr, {{_P, '_', _S},_St}) ->
    io:format("No Instance entered~n");
check_args(_Pr, {{_P, _I, '_'},_St}) ->
    io:format("Wrong Server Ip entered~n");
check_args(Protocol,{{Port,Instance,ServerIp},Stats}) ->
    {Protocol,{{Port,Instance,ServerIp},Stats}}.

%%%-------------------------------------------------------------------
%% @doc
%% Kill the udp servers currently running and pushing stats to an endpoint.

%% Stop the pushing of stats by taking the setup of the udp gen_server
%% down
%% @end
%%%-------------------------------------------------------------------
-spec(setdown(arg()) -> ok).
setdown([]) ->
    io:format("No Argument entered ~n");
setdown(Arg) ->
    {Protocol, SanitisedData} = sanitise_data(Arg),
    terminate_server(Protocol, SanitisedData).

%% @doc change the undefined into
-spec(terminate_server(arg(),arguments()) -> ok | error()).
terminate_server(Protocol,SanitisedData) ->
    foldinate(Protocol, SanitisedData).

foldinate('_',SanitisedData) ->
    case check_args(fake_protocol,SanitisedData) of
        %% dont care about the protocol not being entered
        ok -> ok; %% something else isnt given
        {_FakeProtocol, SanitisedData} ->
            stop_server(fold('_',SanitisedData))
    end;
foldinate(Protocol,SanitisedData) ->
    case check_args(Protocol,SanitisedData) of
        ok -> ok;
        {Protocol,SanitisedData} ->
            stop_server(fold(Protocol,SanitisedData))
    end.

stop_server(ChildrenInfo) ->
    lists:foreach(fun
                      ({{_D,_T,_Pr},{_Po,_Sip,_I,Pid,_R,_S}}) ->
                          riak_core_stats_sup:stop_server(Pid)
                  end, ChildrenInfo).

fold(Protocol,{{Port,Instance,ServerIp},_Stats}) ->
    riak_core_metadata:fold(
        fun              %% K-V in metadata (legacy)
            ({{MDate,MTime,MProtocol}, [{MPort,MSip,MInst,MPid,MRunTuple,MStats}]},
                {Acc, APort,AInstance,AServerIP})
                when    (    APort == MPort orelse     APort == '_')
                andalso (AInstance == MInst orelse AInstance == '_')
                andalso (AServerIP == MSip  orelse AServerIP == '_') ->
                {[{{MDate,MTime,MProtocol},{MPort,MSip,MInst,MPid,MRunTuple,MStats}}|Acc],
                    APort,AInstance,AServerIP};

            ({{MDate,MTime,MProtocol},[#{
                port     := MPort,
                serverip := MSip,
                instance := MInst,
                pid      := MPid,
                runnning := MRun,
                stats    := MStats}]},
                                    {Acc,APort,AInstance,AServerIP})
                when    (    APort == MPort orelse     APort == '_')
                andalso (AInstance == MInst orelse AInstance == '_')
                andalso (AServerIP == MSip  orelse AServerIP == '_') ->
                {[{{MDate,MTime,MProtocol},{MPort,MSip,MInst,MPid,MRun,MStats}}|Acc],
                    APort,AInstance,AServerIP};

            ({_K,_V},{Acc,APort,AInstance,AServerIP}) ->
                {Acc,APort,AInstance,AServerIP}
        end,
        {[],Port,Instance,ServerIp},
            ?PUSHPREFIX, [{match, {'_','_',Protocol}}]
    ).

%%%-------------------------------------------------------------------

find_persisted_data(Arg) ->
    {Protocol, SanitisedData} = sanitise_data(Arg),
     print_info(fold(Protocol,SanitisedData)).

print_info([]) ->
    io:fwrite("Nothing found~n");
print_info(Info) ->
    lists:foreach(fun
                      ({{Date,Time,Protocol},{Port,Sip,Inst,Pid,Run,Stats}}) ->
                          io:fwrite("~p/~p/~p ",Date),
                          io:fwrite("~p:~p:~p ",Time),
                          io:fwrite("~p       ",[Protocol]),
                          io:fwrite("~p ~p ~p ~p ~p ~p~n",
                              [Port,Sip,Inst,Pid,Run,Stats]);
                      (_) -> ok
                  end, Info).



%%--------------------------------------------------------------------
%% @doc
%% Sanitise the data coming in, into the necessary arguments for setting
%% up an endpoint, This is specific to the endpoint functions
%% @end
%%--------------------------------------------------------------------
-spec(sanitise_data(arg()) -> sanitised()).
sanitise_data([]) ->
    io:fwrite("No argument given~n");
sanitise_data(Arg) ->
    [Opts | Stats] = break_up(Arg, "/"),
    List = break_up(Opts, ","),
    data_sanitise_2(List,Stats).

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
            {Names,_,_,_} =
                riak_stat_console:data_sanitise(Stats),
            {Protocol, {{Port, Instance, ServerIp},
                Names}}
    end.

data_sanitise_2_electric_boogaloo(Arg) ->
    data_sanitise_2_electric_boogaloo(
        Arg, '_','_','_','_').
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.