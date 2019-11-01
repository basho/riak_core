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
    find_push_stats/2,
    fold_through_meta/3
]).

-define(NODE, node()).
-define(PUSHPREFIX(Node), {riak_stat_push, Node}).

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
    case sanitise_and_check_args(Arg) of
        ok -> ok;
        {Protocol, SanitisedData} ->
            maybe_start_server(Protocol, SanitisedData)
    end.

store_setup_info(ok,_,_) ->
    ok;
store_setup_info(Pid, Protocol, {{Port, Instance, Sip}, Stats}) ->
    Prefix = ?PUSHPREFIX(?NODE),
    Key = key_maker(Protocol), %% Key is tuple-object
    %% Save the stats in the same format as the argument given,
    %% instead of the sanitised version.
    Value = {Port,
            ?NODE,     % Node
            Sip,       % Server IP address
            Instance,  % Name of instance, also name of the child gen_server
            Pid,       % Pid of the Child started
        {running,true}, % Status of the gen_server, will remain
                        % true if the node goes down and this hasn't been
                        % "setdown" - the gen_server will be reloaded on
                        % reboot of the node.
            Stats},
    riak_stat_meta:put(Prefix, Key, Value).

key_maker(Protocol) ->
    {Date0, Time0} = calendar:local_time(),
    Date = tuple_to_list(Date0), Time = tuple_to_list(Time0),
    {Date,Time,Protocol}.

maybe_start_server(Protocol, {{Port,Instance,Sip},Stats}) ->
    case fold_through_meta(Protocol,{{'_',Instance,'_'},'_'}, ?NODE) of
        [] ->
            Pid = start_server(Protocol,{{Port,Instance,Sip},Stats}),
            store_setup_info(Pid, Protocol, {{Port, Instance, Sip}, Stats});
        Otherwise ->
            lists:foreach(fun
                              ({{_D, _T, _Pr}, {_Po, _No, _SIp, _In, _Pid, {running, true}, _St}}) ->
                                  io:fwrite("Server of that instance is already running~n");
                              ({{_D, _T, _Pr}, {_Po, _No, _SIp, _In, _Pid, {running, false}, _St}}) ->
                                  NewPid = start_server(Protocol, {{Port, Instance, Sip}, Stats}),
                                  store_setup_info(NewPid, Protocol, {{Port, Instance, Sip}, Stats})
                          end, Otherwise)
    end.

start_server(udp, Arg) ->
    Pid = riak_stat_push_sup:start_server(udp, Arg),
    Pid;
start_server(tcp, Arg) ->
    Pid = riak_stat_push_sup:start_server(tcp, Arg),
    Pid;
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
    io:fwrite("No Argument entered ~n");
setdown(Arg) ->
        case sanitise_data(Arg) of
            {error, Reason} -> print_info({error,Reason});
            {Protocol, SanitisedData} -> terminate_server(Protocol, SanitisedData)
        end.

%% @doc change the undefined into
-spec(terminate_server(arg(), arguments()) -> ok | error()).
terminate_server(Protocol, SanitisedData) ->
    stop_server(fold({'_','_',Protocol}, SanitisedData, ?NODE)).

stop_server([]) ->
    io:fwrite("Error, Server not found~n");
stop_server(ChildrenInfo) ->
    lists:foreach(fun
                      ({{D, T, Pr}, {Po, Se, Sip, I, Pid, {running,true}, S}}) ->
                          riak_stat_push_sup:stop_server(I),
                          riak_stat_meta:put(?PUSHPREFIX(?NODE), {D,T,Pr},{Po,Se,Sip,I,Pid,{running,false},S});
                      ({{_D, _T, _Pr}, {_Po, _Se, _Sip, _I, _Pid, {running,false}, _S}}) ->
                          ok;
                      (Other) ->
                          io:format("Error, wrong return type in riak_stat_push:fold/2 = ~p~n", [Other])
                  end, ChildrenInfo).

fold({Date, Time, Protocol}, {{Port, Instance, ServerIp}, Stats}, Node) ->
    {Return, Port, Instance, ServerIp, Stats} =
        riak_core_metadata:fold(
            fun
                ({{MDate, MTime, MProtocol}, [{MPort,
                                               MNode,
                                               MSip,
                                               MInst,
                                               MPid,
                                               MRun,
                                               MStats}]},
                    {Acc, APort, AInstance, AServerIP, AStats})
                    when (APort     == MPort  orelse APort     == '_')
                    and  (AInstance == MInst  orelse AInstance == '_')
                    and  (AServerIP == MSip   orelse AServerIP == '_')
                    and  (AStats    == MStats orelse AStats    == '_') ->
                    {[{{MDate, MTime, MProtocol}, {MPort, MNode, MSip, MInst, MPid, MRun, MStats}} | Acc],
                        APort, AInstance, AServerIP, AStats};

                ({_K, _V}, {Acc, APort, AInstance, AServerIP,AStats}) ->
                    {Acc, APort, AInstance, AServerIP,AStats}
            end,
            {[], Port, Instance, ServerIp, Stats},
            ?PUSHPREFIX(Node), [{match, {Date, Time, Protocol}}]
        ),
    Return.

%%%-------------------------------------------------------------------

find_push_stats(_Nodes,[]) ->
    print_info({error,badarg});
find_push_stats(Nodes, ["*"]) ->
    print_info(fold_through_meta('_',{{'_','_','_'},'_'}, Nodes));
find_push_stats(Nodes,Arg) ->
    case sanitise_data(Arg) of
        {error, Reason} -> print_info({error, Reason});
        {Protocol, SanitisedData} ->
            print_info(fold_through_meta(Protocol, SanitisedData, Nodes))
    end.

%% todo: find a way to search by date. By time might be too useless. as the key is a list
%% it  can be done like [year,'_', day], to find all the times there was a stat push on like the
%% 5th of every month in 2019, or ['_',may,'_'] find all the setups in may.

fold_through_meta(Protocol, {{Port, Instance, ServerIp}, Stats}, Nodes) ->
    [fold(Protocol, {{Port, Instance, ServerIp}, Stats}, Node) || Node <- Nodes].

%%%-------------------------------------------------------------------

%% exported function to delete the persisted data in the metadata.
%% accessible through the remote_console

delete_logs(Date) ->
    NewDate = get_date(Date, {'_','_','_'}),
    SelectDate = tuple_to_list(NewDate),
    FoundList = fold_through_meta({SelectDate, '_', '_'}, {{'_','_','_'},'_'}, [node()]),
    delete_from_meta(FoundList).

%% todo: make a delete-logs-all function as well. or local/native as options
%% so delete_logs(Date, all | local) default is local.

get_date([Year|MonthDay], {TheYear,TheMonth,TheDay}) ->
    ok.

delete_from_meta(List) ->
    ok.

%%%-------------------------------------------------------------------

print_info([]) ->
    io:fwrite("Nothing found~n");
print_info({error,badarg}) ->
    io:fwrite("Invalid Argument Given~n");
print_info({error,Reason}) ->
    io:fwrite("Error: ~p~n",[Reason]);
print_info(Info) ->
    lists:foreach(fun
                      ({{Date, Time, Protocol}, {Port,Server, Sip, Inst, Pid, Run, Stats}}) ->
                          consistent_date(Date),
                          consistent_time(Time),
                          StatsString = io_lib:format("~p",[Stats]),
                          io:fwrite("~p    ", [Protocol]),
                          io:fwrite("~p ~p ~p ~p ~p ~p ~s~n",
                              [Port, Server, Sip, Inst, Pid, Run, StatsString]);
                      (_Other) -> ok
                  end, Info).

%% the dates and time when printed have a different length, due to the
%% integer being < 10, so the lack of digits makes the list of information
%% printed out have a wonky effect.
%% This is just another layer of pretty printing.

consistent_date(Date) ->
    NewDate = integers_to_strings(Date),
    io:fwrite("~s/~s/~s ",NewDate).

consistent_time(Time) ->
    NewTime = integers_to_strings(Time),
    io:fwrite("~s:~s:~s ",NewTime).

integers_to_strings(IntegerList) ->
    lists:map(fun
                  (Num) when length(Num) < 2 -> "0"++Num;
                  (Num) -> Num
              end, [integer_to_list(N) || N <-IntegerList]).

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
    try parse_args(List) of
        {Protocol, Port, Instance, ServerIp} ->
            {Protocol, {{Port, Instance, ServerIp}, ['_']}}
    catch
        error:function_clause -> {error, badarg};
        _:Reason -> {error, Reason}
    end;
data_sanitise_2(List, Stats) ->
    try parse_args(List) of
        {Protocol, Port, Instance, ServerIp} ->
            {Names, _, _, _} =
                riak_stat_console:data_sanitise(Stats),
            {Protocol, {{Port, Instance, ServerIp},
                Names}}
    catch
        error:function_clause -> {error, badarg};
        _:Reason -> {error, Reason}
    end.

parse_args(Arg) ->
    parse_args(
        Arg, '_', '_', '_', '_').
parse_args(<<>>, Protocol, Port, Instance, ServerIp) ->
    parse_args([],Protocol,Port,Instance,ServerIp);
parse_args([<<"protocol=", Pr/binary>> | Rest],
    Protocol, Port, Instance, ServerIp) ->
    NewProtocol =
        case Pr of
            <<"udp">> -> udp;
            <<"tcp">> -> tcp;
            <<"http">> -> http;
            <<"*">> -> '_';
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
