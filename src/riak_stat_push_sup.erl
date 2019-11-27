%%%-------------------------------------------------------------------
%%% @doc
%%% Superviser for the tcp and udp servers that will poll stats from
%%% exometer and push them to an endpoint given
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_push_sup).
-include_lib("riak_core/include/riak_stat_push.hrl").
-behaviour(supervisor).

%% API
-export([
    start_link/0,
    start_server/2,
    stop_server/1]).

%% Supervisor callbacks
-export([init/1]).

-define(STRATEGY,  one_for_one).
-define(INTENSITY, 1).
-define(PERIOD,    5).

-define(RESTART,  transient).
-define(SHUTDOWN, 5000).

-define(SERVER, ?MODULE).
-define(TCP_CHILD, riak_stat_push_tcp).
-define(UDP_CHILD, riak_stat_push_udp).

-define(PUSHPREFIX, {riak_stat_push, term_to_binary(node())}).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%-------------------------------------------------------------------
%% @doc
%% Start up a gen server for the pushing of stats to an endpoint.
%% @end
%%%-------------------------------------------------------------------
-spec(start_server(protocol(),sanitised_push()) -> ok | print() | error()).
start_server(Protocol, Data) ->
    CHILD = child_spec(Data,Protocol),
    start_child(CHILD).

%%%-------------------------------------------------------------------
%% @doc
%% Stop the persisting of stats by terminating and deleting the server
%% pushing the stats to an endpoint
%% @end
%%%-------------------------------------------------------------------
-spec(stop_server([pid()] | [atom()] | list()) -> ok | print() | error()).
stop_server(Child) when is_list(Child) ->
    ChildName = list_to_atom(Child),
    _Terminate = supervisor:terminate_child(?MODULE, ChildName),
    _Delete = supervisor:delete_child(?MODULE, ChildName),
    io:fwrite("Polling Stopped for: ~s~n",[Child]),
    ok.


%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
        MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
        [ChildSpec :: supervisor:child_spec()]
    }} |
    ignore |
    {error, Reason :: term()}).
init([]) ->
    STRATEGY = ?STRATEGY,
    INTENSITY = ?INTENSITY,
    PERIOD = ?PERIOD,
    SupFlags = {STRATEGY,INTENSITY,PERIOD},
    Children = get_children(),
    %% todo: create function that will start up servers. If it and it will
    %% try to restart incrementally.

    %% That way the whole system does not fall over should the genserver
    %% not start.
    {ok, {SupFlags, Children}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% Retrieve the information stored in the metadata about any gen_servers
%% that may have been running before the node was stopped.
%% @end
%%%-------------------------------------------------------------------
-spec(get_children() -> listofpush()).
get_children() ->
    ListOfKids = riak_stat_push:fold_through_meta('_', {{'_', '_', '_'}, '_'}, [node()]),
    lists:foldl(
        fun
            ({{Protocol,Instance}, {_ODate,_MDate,_Pid,{running, true},_Node,Port,Sip,Stats}}, Acc) ->
            Data = {{Port, Instance, Sip}, Stats},
            [child_spec(Data, Protocol) | Acc];

            %% MAPS
            ({{Protocol,Instance}, #{running := true, port := Port, server_ip := Sip, stats := Stats}}, Acc) ->
            Data = {{Port, Instance, Sip}, Stats},
            [child_spec(Data, Protocol) | Acc];
            (_other,Acc) -> Acc
        end, [], ListOfKids).


%%%-------------------------------------------------------------------
%% @doc
%% Create a child spec out of the information given.
%% @end
%%%-------------------------------------------------------------------
-spec(child_spec(sanitised_push(),protocol()) -> supervisor:child_spec()).
child_spec(Data,Protocol) ->
    ChildName = server_name(Data),
    Module    = mod_name(Protocol),
    Function  = start_link,
    Args      = [Data],
    Restart   = ?RESTART,
    Shutdown  = ?SHUTDOWN,
    Type      = worker,
    MFArgs    = {Module, Function, [Args]},
    {ChildName, MFArgs, Restart, Shutdown, Type, [Module]}.

server_name({{_,ServerName,_},_}) -> list_to_atom(ServerName).

mod_name(udp) -> ?UDP_CHILD;
mod_name(tcp) -> ?TCP_CHILD.

%%%-------------------------------------------------------------------
%% @doc
%% Start up the gen_server responsible for pushing stats and their values
%% to an endpoint. Passing in the Data needed.
%% @end
%%%-------------------------------------------------------------------
-spec(start_child(supervisor:child_spec()) -> ok | print() | error() | pid()).
start_child(Child) ->
    case supervisor:start_child(?MODULE, Child) of
        {ok, Pid} ->
            io:format("Polling Initiated~n",[]),
            lager:info("Child Started : ~p",[Child]),
            Pid;
        {error, Reason} ->
            case Reason of
                {already_started, Pid} -> Pid;
                econnrefused ->               io:fwrite("Error : Connection Refused~n"),
                    lager:info("Child Refused to start because Connection was refused : ~p",[Child]);
                {{error, econnrefused}, _} -> io:fwrite("Error : Connection Refused~n"),
                    lager:info("Child Refused to start because Connection was refused : ~p",[Child]);
                {error, Other} ->             io:fwrite("Error : ~p~n", [Other]),
                    lager:info("Child Refused to start because ~p : ~p",[Other,Child]);
                Other ->                      io:fwrite("Error : ~p~n", [Other]),
                    lager:info("Child Refused to start because  : ~p",[Other,Child])
            end
    end.

