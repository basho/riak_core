%%%-------------------------------------------------------------------
%%% @doc
%%% From "riak admin push setup ___" to this module, you can poll the stats
%%% from exometer and setup a server to push those stats to an udp endpoint.
%%% The server can be terminated to stop the pushing of stats.
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_push).
-include_lib("riak_core/include/riak_stat_push.hrl").

%% API
-export([
    setup/1,
    setdown/1,
    sanitise_data/1,
    find_push_stats/2,
    fold_through_meta/3
]).

-define(NODE, [node()]).
-define(PUSHPREFIX(Node), {riak_stat_push, node()}).

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
-spec(setup(consolearg()) -> print()).
setup(Arg) ->
    case sanitise_and_check_args(Arg) of
        ok -> ok;
        {Protocol, SanitisedData} ->
            maybe_start_server(Protocol, SanitisedData)
    end.

-spec(store_setup_info(ok | pid(), protocol(), sanitised_push())
                                          -> ok | print() | error()).
store_setup_info(ok,_,_) ->
    ok;
store_setup_info(Pid, Protocol, {{Port, Instance, Sip}, Stats}) ->
    Prefix = ?PUSHPREFIX(node()),
    Key = {Protocol,Instance},
    Value = {
        calendar:universal_time(),
        calendar:universal_time(),
        Pid,            % Pid of the Child started
        {running,true}, % Status of the gen_server, will remain
        node(),         % Node
        Port,
        Sip,            % Server IP address
        Stats},
    riak_stat_meta:put(Prefix, Key, Value).

maybe_start_server(Protocol, {{Port,Instance,Sip},Stats}) ->
    case fold_through_meta(Protocol,{{'_',Instance,'_'},'_'}, ?NODE) of
        [] ->
            Pid = start_server(Protocol,{{Port,Instance,Sip},Stats}),
            store_setup_info(Pid, Protocol, {{Port, Instance, Sip}, Stats});
        Otherwise ->
            lists:foreach(fun
                              ({{_Pr,_In}, {_ODT,_MDT,_Pid,{running,true},_Node,_Port,_Sip,_St}}) ->
                                  io:fwrite("Server of that instance is already running~n");
                              ({{_Pr,_In}, {_ODT,_MDT,_Pid,{running,false},_Node,_Port,_Sip,_St}}) ->
                                  NewPid = start_server(Protocol, {{Port, Instance, Sip}, Stats}),
                                  store_setup_info(NewPid, Protocol, {{Port, Instance, Sip}, Stats})
                          end, Otherwise)
    end.

-spec(start_server(protocol(), sanitised_push())
        -> print() | error()).
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

check_args(error, Reason) ->
    io:format("Error : ~p~n",[Reason]);
check_args('_', {{_P, _I, _S}, _St}) ->
    io:format("No Protocol type entered~n");
check_args(_Pr, {{'_', _I, _S}, _St}) ->
    io:format("No Port type entered~n");
check_args(_Pr, {{_P, '_', _S}, _St}) ->
    io:format("No Instance entered~n");
check_args(_Pr, {{_P, _I, '_'}, _St}) ->
    io:format("No Server Ip entered~n");
check_args(Protocol, {{Port, Instance, ServerIp}, Stats}) ->
    {Protocol, {{Port, Instance, ServerIp}, Stats}}.

%%%-------------------------------------------------------------------
%% @doc
%% Kill the udp servers currently running and pushing stats to an endpoint.
%% Stop the pushing of stats by taking the setup of the udp gen_server
%% down
%% @end
%%%-------------------------------------------------------------------
-spec(setdown(consolearg()) -> print()).
setdown([]) ->
    io:fwrite("No Argument entered ~n");
setdown(Arg) ->
        case sanitise_data(Arg) of
            {error, Reason} -> print_info({error,Reason});
            {Protocol, SanitisedData} -> terminate_server(Protocol, SanitisedData)
        end.

%% @doc change the undefined into
-spec(terminate_server(protocol(), sanitised_push()) -> ok | error()).
terminate_server(Protocol, {{Port, Instance, Sip},Stats}) ->
    stop_server(fold(Protocol, Port, Instance, Sip, Stats, node())).

stop_server([]) ->
    io:fwrite("Error, Server not found~n");
stop_server(ChildrenInfo) ->
    lists:foreach(fun
                      ({{Protocol, Instance},{OrigDT,_DT,_Pid,{running,true},Node,Port,SIp,Stats}}) ->
                          riak_stat_push_sup:stop_server(Instance),
                          riak_stat_meta:put(?PUSHPREFIX(node()),
                              {Protocol, Instance},
                              {OrigDT, calendar:universal_time(),undefined,{running,false},Node,Port,SIp,Stats});
                      ({{_Protocol, _Instance}, {_ODT,_DT,_Pid,{running,false},_Node,_Port,_SIp,_Stats}}) ->
                          ok;
                      (Other) ->
                          io:format("Error, wrong return type in riak_stat_push:fold/2 = ~p~n", [Other])
                  end, ChildrenInfo).

-spec(fold(protocol(),port(),instance(),server_ip(),metrics(),node()) -> listofpush()).
fold(Protocol, Port, Instance, ServerIp, Stats, Node) ->
    {Return, Port, ServerIp, Stats, Node} =
        riak_core_metadata:fold(
            fun
                ({{MProtocol, MInstance},   %% KEY
                    [{OriginalDateTime,     %% VALUE
                        ModifiedDateTime,
                        Pid,
                        RunningTuple, %% {running, boolean()}
                        MNode,
                        MPort,
                        MSip,
                        MStats}]},
                    {Acc, APort, AServerIP, AStats, ANode}) %% Acc and Guard
                    when (APort     == MPort  orelse APort     == '_')
                    and  (AServerIP == MSip   orelse AServerIP == '_')
                    and  (ANode     == MNode  orelse ANode     == node()) %% default compare to own node
                    and  (AStats    == MStats orelse AStats    == '_') ->
                    %% Matches all the Guards given in Acc
                    {[{{MProtocol,MInstance}, {OriginalDateTime, %% First start
                                                ModifiedDateTime,%% date last changed.
                                                Pid,
                                                RunningTuple,
                                                MNode,
                                                MPort,
                                                MSip,
                                                MStats}} | Acc],

                                                APort, AServerIP, AStats,ANode};

                %% Doesnt Match Guards above
                ({_K, _V}, {Acc, APort, AServerIP,AStats,ANode}) ->
                    {Acc, APort, AServerIP,AStats,ANode}
            end,
            {[], Port, ServerIp, Stats, Node}, %% Accumulator
            ?PUSHPREFIX(Node), %% Prefix to Iterate over
            [{match, {Protocol, Instance}}] %% Key to Object match
        ),
    Return.

%%%-------------------------------------------------------------------

-spec(find_push_stats(node_or_nodes(), consolearg()) -> print()).
find_push_stats(_Nodes,[]) ->
    print_info({error,badarg});
find_push_stats(Nodes, ["*"]) ->
    print_info(fold_through_meta('_','_','_','_','_', Nodes));
find_push_stats(Nodes,Arg) ->
    case sanitise_data(Arg) of
        {error, Reason} -> print_info({error, Reason});
        {Protocol, SanitisedData} ->
            print_info(fold_through_meta(Protocol, SanitisedData, Nodes))
    end.

-spec(fold_through_meta(protocol(),sanitised_push(),node_or_nodes()) -> listofpush()).
fold_through_meta(Protocol, {{Port, Instance, ServerIp}, Stats}, Nodes) ->
    fold_through_meta(Protocol,Port,Instance,ServerIp,Stats,Nodes).
fold_through_meta(Protocol, Port, Instance, ServerIp, Stats, Nodes) ->
    lists:flatten([fold(Protocol, Port, Instance, ServerIp, Stats, Node) || Node <- Nodes]).

%%%-------------------------------------------------------------------

-spec(print_info(pusharg() | any()) -> print() | error()).
print_info([]) ->
    io:fwrite("Nothing found~n");
print_info({error,badarg}) ->
    io:fwrite("Invalid Argument Given~n");
print_info({error,Reason}) ->
    io:fwrite("Error: ~p~n",[Reason]);
print_info(Info) ->
    lists:foreach(fun
                      ({{Protocol,Instance},
                          {OriginalDateTime,     %% VALUE
                           ModifiedDateTime,
                           Pid,
                           RunningTuple, %% {running, boolean()}
                           Node,
                           Port,
                           Sip,
                           Stats}}) ->
                          {ODate,OTime} = OriginalDateTime,
                          {MDate,MTime} = ModifiedDateTime,
                          io:fwrite("~s ~s",[consistent_date(MDate),consistent_time(MTime)]),
                          io:fwrite("~p ~s ", [Protocol, Instance]),
                          StatsString = io_lib:format("~p",[Stats]),
                          io:fwrite("~s ~s",[consistent_date(ODate),consistent_time(OTime)]),
                          io:fwrite("~p ~p ~p ~p ~p ~s~n",
                              [Node, Sip, Port, RunningTuple, Pid, StatsString]);
                      (_Other) -> ok
                  end, Info).

%% the dates and time when printed have a different length, due to the
%% integer being < 10, so the lack of digits makes the list of information
%% printed out have a wonky effect.
%% This is just another layer of pretty printing.

consistent_date(Date) when is_tuple(Date) ->
    consistent_date(tuple_to_list(Date));
consistent_date(Date) when is_list(Date) ->
    NewDate = integers_to_strings(Date),
    io_lib:format("~s/~s/~s ",NewDate).


consistent_time(Time) when is_tuple(Time) ->
    consistent_date(tuple_to_list(Time));
consistent_time(Time) when is_list(Time)->
    NewTime = integers_to_strings(Time),
    io_lib:format("~s:~s:~s ",NewTime).

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
-spec(sanitise_data(consolearg()) -> {protocol(),sanitised_push()} | error()).
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