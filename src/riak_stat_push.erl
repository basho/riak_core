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
    find_persisted_data/1,
    find_persisted_data/2
]).

%% todo: on start up find the running server values and start up

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
    io:format("Setting up Polling on that endpoint~n"),
    case sanitise_and_check_args(Arg) of
        ok -> ok;
        {Protocol, SanitisedData} ->
            io:format("Data Sanitised~n"),
            Pid = maybe_start_server(Protocol, SanitisedData),
            io:format("Polling Initiated ~p~n",[Pid]),
            store_setup_info(Pid, Protocol, SanitisedData),
            io:format("Setup Stored~n"),
            ok
    end.
store_setup_info(ok,_,_) ->
    ok;
store_setup_info(Pid, Protocol, {{Port, Instance, Sip}, Stats}) ->
    Prefix = ?PUSHPREFIX,
    {Date0, Time0} = calendar:local_time(),
    Date = tuple_to_list(Date0), Time = tuple_to_list(Time0),
    Key = {Date, Time, Protocol}, %% Key is tuple-object
    Value = {Port, undefined, % server
        Sip, Instance, Pid, {running,true}, Stats},
    riak_stat_meta:put(Prefix, Key, Value).

maybe_start_server(Protocol, {{Port,Instance,Sip},Stats}) ->
    case find_persisted_data(Protocol,{{'_',Instance,'_'},'_'}) of
        [] -> start_server(Protocol,{{Port,Instance,Sip},Stats});
        Otherwise ->
            lists:foreach(fun
                              ({{_D,_T,_Pr},{_Po,_Se,_SIp,_In,_Pid,{running,true},_St}}) ->
                                  io:fwrite("Server of that instance is already running~n");
                              ({{_D,_T,_Pr},{_Po,_Se,_SIp,_In,_Pid,{running,false},_St}}) ->
                                  NewPid = start_server(Protocol,{{Port,Instance,Sip},Stats}),
                                  store_setup_info(NewPid,Protocol,{{Port,Instance,Sip},Stats})
                          end, Otherwise)
    end.

start_server(udp, Arg) ->
    riak_stat_push_sup:start_server(udp, Arg);
start_server(tcp, Arg) ->
    riak_stat_push_sup:start_server(tcp, Arg);
start_server(http, _Arg) ->
    io:format("Error Unsupported : ~p~n", [http]);
start_server(Protocol, _Arg) ->
    io:format("Error wrong type : ~p~n", [Protocol]).

sanitise_and_check_args(Arg) ->
    case sanitise_data(Arg) of
        ok -> ok; % ok == io:format output i.e. error msg
        {Protocol, SanitisedData} ->
            case check_args(Protocol, SanitisedData) of
                ok -> ok;
                OtherWise -> OtherWise
            end
    end.

check_args('_', {{_P, _I, _S}, _St}) ->
    io:format("No Protocol type entered~n");
check_args(_Pr, {{'_', _I, _S}, _St}) ->
    io:format("No Port type entered~n");
check_args(_Pr, {{_P, '_', _S}, _St}) ->
    io:format("No Instance entered~n");
check_args(_Pr, {{_P, _I, '_'}, _St}) ->
    io:format("Wrong Server Ip entered~n");
check_args(Protocol, {{Port, Instance, ServerIp}, Stats}) ->
    {Protocol, {{Port, Instance, ServerIp}, Stats}}.

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
-spec(terminate_server(arg(), arguments()) -> ok | error()).
terminate_server(Protocol, SanitisedData) ->
    stop_server(fold(Protocol, SanitisedData)).

stop_server([]) ->
    io:fwrite("Error, Server not found~n");
stop_server(ChildrenInfo) ->
%%    io:format("Children: ~p~n",[ChildrenInfo]),
    lists:foreach(fun
                      ({{D, T, Pr}, {Po, Se, Sip, I, Pid, {running,true}, S}}) ->
                          riak_stat_push_sup:stop_server(I),
                          riak_stat_meta:put(?PUSHPREFIX, {D,T,Pr},{Po,Se,Sip,I,Pid,{running,false},S});
                      ({{_D, _T, _Pr}, {_Po, _Se, _Sip, _I, _Pid, {running,false}, _S}}) ->
                          ok;
                      (Other) ->
                          io:format("Error, wrong return type in riak_stat_push:fold/2 = ~p~n", [Other])
                  end, ChildrenInfo).

fold(Protocol, {{Port, Instance, ServerIp}, _Stats}) ->
%%    io:format("Port:  ~p~nInstance: ~p~nServerIP: ~p~n",[Port, Instance, ServerIp]),
    {Return, Port, Instance, ServerIp} =
        riak_core_metadata:fold(
            fun                     %% original using maps
                ({{MDate, MTime, MProtocol}, [#{port     := MPort,
                                                serverip := MSip,
                                                instance := MInst,
                                                pid      := MPid,
                                                runnning := MRun,
                                                stats    := MStats}]},
                    {Acc, APort, AInstance, AServerIP})
                    when (APort == MPort orelse APort == '_')
                    andalso (AInstance == MInst orelse AInstance == '_')
                    andalso (AServerIP == MSip orelse AServerIP == '_') ->
%%                    io:format("1 MPort:  ~p~nMInstance: ~p~nMServerIP: ~p~n",[MPort, MInst, MSip]),
                    {[{{MDate, MTime, MProtocol}, {MPort, MSip, MInst, MPid, MRun, MStats}} | Acc],
                        APort, AInstance, AServerIP};

                        %% New, Maps were not working to setdown
                ({{MDate, MTime, MProtocol}, [{MPort,
                                               MServer,
                                               MSip,
                                               MInst,
                                               MPid,
                                               MRun,
                                               MStats}]},
                    {Acc, APort, AInstance, AServerIP})
                    when (APort == MPort orelse APort == '_')
                    and (AInstance == MInst orelse AInstance == '_')
                    and (AServerIP == MSip orelse AServerIP == '_') ->
%%                    io:format("2 MPort:  ~p~nMInstance: ~p~nMServerIP: ~p~n",[MPort, MInst, MSip]),
                    {[{{MDate, MTime, MProtocol}, {MPort, MServer, MSip, MInst, MPid, MRun, MStats}} | Acc],
                        APort, AInstance, AServerIP};

                ({_K, _V}, {Acc, APort, AInstance, AServerIP}) ->
                    %%                    io:format("K: ~p    ",[K]),
%%                    io:format("V: ~p~n",[V]),
                    {Acc, APort, AInstance, AServerIP}
            end,
            {[], Port, Instance, ServerIp},
            ?PUSHPREFIX, [{match, {'_', '_', Protocol}}]
        ),
    Return.

%%%-------------------------------------------------------------------

find_persisted_data(Arg) ->
    {Protocol, SanitisedData} = sanitise_data(Arg),
    print_info(find_persisted_data(Protocol, SanitisedData)).
find_persisted_data(Protocol, {{Port, Instance, ServerIp}, Stats}) ->
    fold(Protocol, {{Port, Instance, ServerIp}, Stats}).

print_info([]) ->
    io:fwrite("Nothing found~n");
print_info(Info) ->
    lists:foreach(fun
                      ({{Date, Time, Protocol}, {Port, Sip, Inst, Pid, Run, Stats}}) ->
                          io:fwrite("~p/~p/~p ", Date),
                          io:fwrite("~p:~p:~p ", Time),
                          io:fwrite("~p       ", [Protocol]),
                          io:fwrite("~p ~p ~p ~p ~p ~p~n",
                              [Port, Sip, Inst, Pid, Run, Stats]);
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
    data_sanitise_2(List, Stats).

break_up(Arg, Str) ->
    re:split(Arg, Str).

data_sanitise_2(List, []) ->
    case parse_args(List) of
        ok -> ok;
        {Protocol, Port, Instance, ServerIp} ->
            {Protocol, {{Port, Instance, ServerIp}, ['_']}}
    end;
data_sanitise_2(List, Stats) ->
    case parse_args(List) of
        ok -> ok;
        {Protocol, Port, Instance, ServerIp} ->
            {Names, _, _, _} =
                riak_stat_console:data_sanitise(Stats),
            {Protocol, {{Port, Instance, ServerIp},
                Names}}
    end.

parse_args(Arg) ->
    parse_args(
        Arg, '_', '_', '_', '_').
parse_args([<<"protocol=", Pr/binary>> | Rest],
    Protocol, Port, Instance, ServerIp) ->
    NewProtocol =
        case Pr of
            <<"udp">> -> udp;
            <<"tcp">> -> tcp;
            <<"http">> -> http;
            _ -> Protocol
        end,
    parse_args(Rest,
        NewProtocol, Port, Instance, ServerIp);
parse_args([<<"port=", Po/binary>> | Rest],
    Protocol, Port, Instance, ServerIp) ->
    NewPort =
        try binary_to_integer(Po) of
            Int -> Int
        catch _e:_r -> Port
        end,
    parse_args(Rest,
        Protocol, NewPort, Instance, ServerIp);

parse_args([<<"instance=", I/binary>> | Rest],
    Protocol, Port, _Instance, ServerIp) ->
    NewInstance = binary_to_list(I),
    parse_args(Rest, Protocol, Port, NewInstance, ServerIp);

parse_args([<<"sip=", S/binary>> | Rest],
    Protocol, Port, Instance, _ServerIp) ->
    NewIP = re:split(S, "\\s", [{return, list}]),
    parse_args(Rest, Protocol, Port, Instance, NewIP);
parse_args([], Protocol, Port, Instance, ServerIp) ->
    {Protocol, Port, Instance, ServerIp}.
