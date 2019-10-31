%%%-------------------------------------------------------------------
%%% @doc
%%% Superviser for the tcp and udp servers that will poll stats from
%%% exometer and push them to an endpoint given
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_push_sup).

-behaviour(supervisor).

%% API
-export([
    start_link/0,
    start_server/2,
    stop_server/1]).

%% Supervisor callbacks
-export([init/1]).

-define(STRATEGY,  one_for_one).
-define(INTENSITY, 1000).
-define(PERIOD,    5000).

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

start_server(Protocol, Data) ->
    CHILD = child_spec(Data,Protocol),
    start_child(CHILD).

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
    {ok, {SupFlags, Children}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_children() ->
    ListOfKids = riak_stat_push:fold_through_meta('_', {{'_', '_', '_'}, '_'}, [node()]),
    lists:foldl(
        fun({{_Date, _Time, Protocol}, {Port, _Server, Sip, ServerName, _Pid, {running, true}, Stats}}, Acc) ->
            Data = {{Port, ServerName, Sip}, Stats},
            [child_spec(Data, Protocol) | Acc];
            (_other,Acc) -> Acc
        end, [], ListOfKids).


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

start_child(Child) ->
    case supervisor:start_child(?MODULE, Child) of
        {ok, Pid} ->
            io:format("Polling Initiated~n",[]),
            Pid;
        {error, {already_started, Pid}} -> Pid;
        {error, econnrefused} -> io:fwrite("Error - Connection Refused~n");
        {error, Other} -> io:fwrite("Error : ~p~n", [Other])
    end.


%% todo: every day check for data in the metadata that is older than 6months ol and delete it ,
%% it can be an appget value thing so the default is at like 5am etc....