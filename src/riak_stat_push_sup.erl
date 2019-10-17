%%%-------------------------------------------------------------------
%%% @doc
%%%
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

-define(SERVER, ?MODULE).
-define(TIMEOUT, 5000).
-define(TCP_CHILD, riak_stat_push_tcp).
-define(UDP_CHILD, riak_stat_push_udp).
-define(CHILD(Mod,Arg), {Mod,{Mod,start_link,[Arg]},transient,?TIMEOUT,worker,[Mod]}).
-define(CHILD(Name,Mod,Arg), {Name,{Mod,start_link,[Arg]},transient,?TIMEOUT,worker,[Mod]}).

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
    UDPCHILD = child_name(Data,Protocol),
    start_child(UDPCHILD).

child_name(Data,udp) ->
    {{_Port, ServerName, _ServerIP},_Stats} = Data,
    ?CHILD(ServerName,?UDP_CHILD,[Data]);
child_name(Data,tcp) ->
    {{_Port, ServerName, _ServerIP},_Stats} = Data,
    ?CHILD(ServerName,?TCP_CHILD,[Data]).

start_child(Child) ->
    case supervisor:start_child(?MODULE, Child) of
        {ok, Pid} -> Pid;
        {error, {already_started, Pid}} -> Pid;
        {error, Other} -> io:fwrite("Error : ~p~n", [Other])
    end.

stop_server(Child) ->
    io:format("Stopping Child Server: ~p~n",[Child]),
    Terminate = supervisor:terminate_child(?MODULE, Child),
    Delete = supervisor:delete_child(?MODULE, Child),
    io:format("Terminated Child : ~p~n",[Terminate]),
    io:format("Deleted Child : ~p~n",[Delete]),
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
    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    Children = restart_children(),
%%    Children =
%%        ListOfKids = riak_stat_push:find_persisted_data('_',{{'_','_','_'},'_'}),
%%    lists:foldl(fun
%%                    ({_Date,_Time,Protocol},{Port,_Server,Sip,ServerName,_Pid,{running,true},Stats},Acc) ->
%%                        [child_name({{Port,ServerName,Sip},Stats},Protocol)|Acc];
%%                    (_,Acc) -> Acc
%%                end, [], ListOfKids),

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    {ok, {SupFlags, Children}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

restart_children() ->
    ListOfKids = riak_stat_push:find_persisted_data('_',{{'_','_','_'},'_'}),
    lists:ukeysort(1,lists:foldl(fun
                      ({{_Date,_Time,Protocol},{Port,_Server,Sip,ServerName,_Pid,{running,true},Stats}},Acc) ->
                         [child_name({{Port,ServerName,Sip},Stats},Protocol)|Acc];
                      (_,Acc) -> Acc
                  end, [], ListOfKids)).

