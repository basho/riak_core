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

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_server(udp, Data) ->
    UDPCHILD = ?CHILD(?UDP_CHILD,Data),
    start_child(UDPCHILD);
start_server(tcp, Data) ->
    TCPCHILD = ?CHILD(?TCP_CHILD,Data),
    start_child(TCPCHILD).

start_child(Child) ->
    case supervisor:start_child(?MODULE, Child) of
        {ok, Pid} -> Pid;
        {error, {already_started, Pid}} -> Pid;
        {error, Other} -> io:fwrite("Error : ~p~n", [Other])
    end.

stop_server(Child) ->
    supervisor:terminate_child(?MODULE, Child),
    supervisor:delete_child(?MODULE, Child),
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

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    {ok, {SupFlags, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
