%%%-------------------------------------------------------------------
%%% @doc
%%% From "riak admin push setup ___" to this module, you can poll the
%%% stats from exometer and setup a server to push those stats to an
%%% udp endpoint.
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
    find_push_stats/1,
    find_push_stats/2,
    find_push_stats_all/1,
    fold_through_meta/3,
    store_setup_info/3
]).

-define(NEWMAP, #{
    original_dt => calendar:universal_time(),
    modified_dt => calendar:universal_time(),
    pid => undefined,
    running => true,
    node => node(),
    port => undefined,
    server_ip => undefined,
    stats => ['_']
    }).

-define(PUTMAP(Pid,Port,Server,Stats,Map),
    Map#{pid => Pid,
         port => Port,
         server_ip => Server,
         stats => Stats}).
-define(STARTMAP(Map),
    Map#{modified_dt => calendar:universal_time(),
         running => true}).
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
setup(ListofArgs) ->
    {Protocol, Data} = sanitise_data(ListofArgs),
    maybe_start_server(Protocol, Data).

host_port(HostPort) ->
    [Host|Port] = re:split(HostPort,"\\:",[{return,list}]),
    {Host,Port}.
hostname([Host]) ->
    hostname(Host);
hostname(Host) ->
    list_to_atom(Host).
port([Port]) ->
    port(Port);
port(Port) ->
    list_to_integer(Port).


-spec(store_setup_info(push_key(),push_value(), (new | existing))
                                          -> ok | print() | error()).
store_setup_info({_Key,_Instance},
    #{pid := NotPid}, _Type) when is_pid(NotPid) == false -> ok;
store_setup_info(Key, MapValues, new) ->
    riak_stat_meta:put(?PUSHPREFIX, Key, MapValues);
store_setup_info(Key, MapValues = #{running := _Bool}, existing) ->
    NewMap = ?STARTMAP(MapValues),
    riak_stat_meta:put(?PUSHPREFIX, Key, NewMap).

maybe_start_server(Protocol, {{Port, Instance,Sip},'_'}) ->
    maybe_start_server(Protocol, {{Port,Instance,Sip},['_']});
maybe_start_server(Protocol, {{Port,Instance,Sip},Stats}) ->
    case fold_through_meta(Protocol,{{'_',Instance,'_'},'_'}, [node()]) of
        [] ->
            Pid = start_server(Protocol,{{Port,Instance,Sip},Stats}),
            MapValue = ?PUTMAP(Pid,Port,Sip,Stats,?NEWMAP),
            store_setup_info({Protocol, Instance},MapValue,new);
        Servers ->
            maybe_start_server(Servers,Protocol,{{Port,Instance,Sip},Stats})

    end.
maybe_start_server(ServersFound,Protocol,{{Port,Instance,Sip},Stats}) ->
    lists:foreach(
        fun
            ({{_Pr,_In}, #{running := true}}) ->
                io:fwrite("Server of that instance is already running~n");
            ({{_Pr,_In}, #{running := false} = ExistingMap}) ->
                Pid =
                    start_server(Protocol, {{Port, Instance, Sip}, Stats}),
                MapValue = ?PUTMAP(Pid,Port,Sip,Stats,ExistingMap),
                store_setup_info({Protocol, Instance}, MapValue, existing)
        end, ServersFound).

-spec(start_server(protocol(), sanitised_push())
        -> print() | error()).
start_server(Protocol, Arg) ->
    riak_stat_push_sup:start_server(Protocol, Arg).

%%%-------------------------------------------------------------------
%% @doc
%% Kill the udp servers currently running and pushing stats to an
%% endpoint. Stop the pushing of stats by taking the setup of the udp
%% gen_server down
%% @end
%%%-------------------------------------------------------------------
-spec(setdown(consolearg()) -> print()).

setdown(ListofArgs) ->
    {Protocol, Data} = sanitise_data(ListofArgs),
    terminate_server(Protocol, Data).

%% @doc change the undefined into
-spec(terminate_server(protocol(), sanitised_push()) -> ok | error()).
terminate_server(Protocol, {{Port, Instance, Sip},Stats}) ->
    stop_server(fold(Protocol, Port, Instance, Sip, Stats, node())).

stop_server(ChildrenInfo) ->
    lists:foreach(
        fun
            ({{Protocol, Instance},#{running := true} = MapValue}) ->
                riak_stat_push_sup:stop_server(Instance),
                riak_stat_meta:put(?PUSHPREFIX,
                    {Protocol, Instance},
                    MapValue#{
                        modified_dt => calendar:universal_time(),
                        pid => undefined,
                        running => false});
            (_) -> ok
        end, ChildrenInfo).

-spec(fold(protocol(),port(),instance(),server_ip(),metrics(),node()) ->
    listofpush()).
fold(Protocol, Port, Instance, ServerIp, Stats, Node) ->
    {Return, Port, ServerIp, Stats, Node} =
        riak_core_metadata:fold(
            fun
                ({{MProtocol, MInstance},   %% KEY would be the same
                     [#{node := MNode,
                        port := MPort,
                        server_ip := MSip,
                        stats := MStats} = MapValue]},

                    {Acc, APort, AServerIP, AStats, ANode}) %% Acc and Guard
                    when (APort     == MPort  orelse APort     == '_')
                    and  (AServerIP == MSip   orelse AServerIP == '_')
                    and  (ANode     == MNode  orelse ANode     == node())
                    and  (AStats    == MStats orelse AStats    == '_') ->
                    %% Matches all the Guards given in Acc
                    {[{{MProtocol,MInstance}, MapValue} | Acc],
                        APort, AServerIP, AStats,ANode};

                %% Doesnt Match Guards above
                ({_K, _V}, {Acc, APort, AServerIP,AStats,ANode}) ->
                    {Acc, APort, AServerIP,AStats,ANode}
            end,
            {[], Port, ServerIp, Stats, Node}, %% Accumulator
            ?PUSHPREFIX, %% Prefix to Iterate over
            [{match, {Protocol, Instance}}] %% Key to Object match
        ),
    Return.

%%%-------------------------------------------------------------------
%%%-------------------------------------------------------------------
%% @doc
%% Get information on the stats polling, as in the date and time the
%% stats pushing began, and the port, serverip, instance etc that was
%% given at the time of setup
%% @end
%%%-------------------------------------------------------------------
-spec(find_push_stats(consolearg()) -> ok | error()).
find_push_stats(Arg) ->
    find_push_stats([node()], Arg).

-spec(find_push_stats(node_or_nodes(), consolearg()) -> print()).
find_push_stats(_Nodes,[]) ->
    print_info({error,badarg});
find_push_stats(Nodes, ["*"]) ->
    print_info(fold_through_meta('_','_','_','_','_', Nodes));
find_push_stats(Nodes,Arg) ->
    {Protocol, SanitisedData} = sanitise_data(Arg),
    print_info(fold_through_meta(Protocol, SanitisedData, Nodes)).

%%%-------------------------------------------------------------------
%% @doc
%% Get information on the stats polling, as in the date and time the
%% stats pushing began, and the port, serverip, instance etc that was
%% given at the time of setup - but for all nodes in the cluster
%% @end
%%%-------------------------------------------------------------------
-spec(find_push_stats_all(consolearg()) -> ok | error()).
find_push_stats_all(Arg) ->
    find_push_stats([node()|nodes()],Arg).

-spec(fold_through_meta(protocol(),sanitised_push(),node_or_nodes()) ->
    listofpush()).
fold_through_meta(Protocol, {{Port, Instance, ServerIp}, Stats}, Nodes) ->
    fold_through_meta(Protocol,Port,Instance,ServerIp,Stats,Nodes).
fold_through_meta(Protocol, Port, Instance, ServerIp, Stats, Nodes) ->
    lists:flatten(
        [fold(Protocol, Port, Instance, ServerIp, Stats, Node)
            || Node <- Nodes]).

%%%-------------------------------------------------------------------

-spec(print_info(pusharg() | any()) -> print() | error()).
print_info([]) ->
    io:fwrite("Nothing found~n");
print_info({error,badarg}) ->
    io:fwrite("Invalid Argument Given~n");
print_info({error,Reason}) ->
    io:fwrite("Error: ~p~n",[Reason]);
print_info(Info) ->
    String = "~10s ~8s ~-8s ~-15s ~-10s  ~-8s  ~-16s ~-16s ~-6s ~-8s ~s~n",
    ColumnArgs =
        ["LastDate","LastTime","Protocol","Name","Date","Time",
            "Node","ServerIp","Port","Running?","Stats"],
    io:format(String,ColumnArgs),
    lists:foreach(
        fun
            ({{Protocol,Instance},
                #{original_dt   := OriginalDateTime,
                    modified_dt := ModifiedDateTime,
                    running     := Running,
                    node        := Node,
                    port        := Port,
                    server_ip   := Sip,
                    stats       := Stats}
            }) ->
                {ODate,OTime} = OriginalDateTime,
                {MDate,MTime} = ModifiedDateTime,
                LastDate = consistent_date(MDate),
                LastTime = consistent_time(MTime),
                SProtocol = atom_to_list(Protocol),
                SInstance = Instance,
                Date = consistent_date(ODate),
                Time = consistent_time(OTime),
                SNode = atom_to_list(Node),
                ServerIp = Sip,
                SPort = integer_to_list(Port),
                SRunning = atom_to_list(Running),
                StatsString = io_lib:format("~p",[Stats]),
                Args = [LastDate,LastTime,SProtocol,SInstance,Date,
                    Time,SNode,ServerIp,SPort,SRunning,StatsString],
                io:format(String,Args);
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
    consistent_time(tuple_to_list(Time));
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
sanitise_data(ListofArgs) ->
    lists:foldl(
        fun
    %% Protocol specified
            ("tcp", {_ProAcc,{{PortAcc,InAcc,HostAcc},StatAcc}}) ->
                {tcp,{{PortAcc,InAcc,HostAcc},StatAcc}};
            ("udp", {_ProAcc,{{PortAcc,InAcc,HostAcc},StatAcc}}) ->
                {udp,{{PortAcc,InAcc,HostAcc},StatAcc}};
            %% Find the Host and Port
            (HostPortOrNot, {ProAcc,{{PortAcc,InAcc,HostAcc},StatAcc}}) ->
                case host_port(HostPortOrNot) of
                    {NoHost, []} ->
                        case string:find(NoHost, ".") of
                            nomatch -> %% Then its instance
                                TheInstance = NoHost,
                                {ProAcc,{{PortAcc,TheInstance,HostAcc},StatAcc}};
                            _ -> %% Its a stat request
                                AStats = NoHost,
                                {TheStats,_,_,_} = riak_stat_console:data_sanitise([AStats]),
                                {ProAcc,{{PortAcc,InAcc,HostAcc},TheStats}}
                        end;
                    {AHost,APort} ->
                        TheHost = hostname(AHost),
                        ThePort = port(APort),
                        {ProAcc,{{ThePort,InAcc,TheHost},StatAcc}}
                end
        end, {'_',{{'_','_','_'},'_'}}, ListofArgs).
