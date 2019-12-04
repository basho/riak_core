%%%-------------------------------------------------------------------
%%% @doc
%%% Supervisor for the tcp and udp servers that will poll stats from
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
    stop_server/1,
    restart_children/3]).

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

-define(PUSHPREFIX, {riak_stat_push, node()}).
-define(ATTEMPTS, app_helper:get_env(riak_stat_push, restart_attempts, 50)).
-define(TIMINGS,  app_helper:get_env(riak_stat_push, restart_timings,
                        [X*1000 || X <- timings()])).

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
    Respond = start_child(CHILD),
    log_and_respond(Respond).

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
    Strategy = ?STRATEGY,
    Intensity = ?INTENSITY,
    Period = ?PERIOD,
%%    SupFlags = {STRATEGY,INTENSITY,PERIOD},
    SupFlags = #{
        strategy => Strategy,
        intensity => Intensity,
        period => Period},
    restart_children(),

    {ok, {SupFlags, []}}.

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
    #{
        id => ChildName,
        start => MFArgs,
        restart => Restart,
        shutdown => Shutdown,
        type => Type,
        modules => [Module]
    }.
%%        {ChildName, MFArgs, Restart, Shutdown, Type, [Module]}.


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
start_child(Children) when is_list(Children) ->
    [start_child(Child)|| Child <- Children];
start_child(Child) ->
    case supervisor:start_child(?MODULE, Child) of
        {ok, Pid}                       -> {Child, Pid};
        {error, {already_started, Pid}} -> {Child, Pid};
        {error, econnrefused}           -> {Child, econnrefused};
        {error, {error, Reason}}        -> {Child, Reason};
        {error, Reason}                 -> {Child, Reason}
    end.

log_and_respond({Child, Response}) ->
    log(Child, Response),
    respond(Response).

log(Child, Pid) when is_pid(Pid)->
    lager:info("Child started : ~p with Pid : ~p",[Child,Pid]);
log(Child, econnrefused) ->
    lager:error("Child refused to start - Connection refused. Child : ~p",[Child]);
log(Child, Error) ->
    lager:error("Child refused to start - ~p. Child : ~p",[Error,Child]).

respond(Pid) when is_pid(Pid) ->
    io:fwrite("Polling Started~n");
respond(econnrefused) ->
    io:fwrite("Could not initiate Polling, Connection refused~n");
respond(Error) ->
    io:fwrite("Could not initiate Polling, ~p~n",[Error]).

restart_children() ->
    %% todo: use stop children if it is running,
    %% todo: make it start children up
    Children = get_children(),
    Attempts = ?ATTEMPTS, %% 100 Attempts at restarting the Children
    Timings  = ?TIMINGS,
    {_Pid, _Ref} =
        spawn_monitor(riak_stat_push_sup, restart_children, [Attempts, Timings, Children]),
    ok.

restart_children(_Attempts, _Timings, []) ->
    ok;
restart_children(0, _Timings, Children) ->
    lager:error("Could not restart Children : ~p, No attempts remaining",
        [[Name || {Name, _, _, _,_,_} <- Children]]),
    %% Setting running = false:
    Keys = make_key(Children),
    riak_stat_push_util:stop_running_server(Keys);
restart_children(Attempts, [Timing|Timings], Children) ->
    timer:sleep(Timing),
    ToStart = lists:foldl(fun
                              ({Child, econnrefused}, Acc) ->
                                  riak_stat_push_util:stop_running_server(make_key([Child])),
                                  Acc;
                              ({Child,Pid}, Acc) when is_pid(Pid) ->
                                  make_key(Child),
                                  %% todo: store in meta,
                                  Acc;
                              ({Child,_Other}, Acc) ->
                                  [Child|Acc]
                          end, [], start_child(Children)),
    restart_children(Attempts-1,Timings,ToStart).


make_key(Child) ->
    lists:foldl(
        fun({Name, {Mod, _Fun, _Args}, _R, _S, _T, _M}, Acc) ->
            Protocol =
                case Mod of
                    riak_stat_push_tcp -> tcp;
                    riak_stat_push_udp -> udp
                end,
            [{Protocol, atom_to_list(Name)} | Acc]
        end, [], Child).

timings() ->
    timings(1,[]).

timings([], Timings) -> lists:reverse(lists:flatten(Timings));
timings(128,Timings) ->
    N=128*2,
    Next = [N,N,N,N],
    timings([],[Next|Timings]);
timings(Time, Timings) ->
    N=Time*2,
    timings(N, [[N,N,N,N]|Timings]).

