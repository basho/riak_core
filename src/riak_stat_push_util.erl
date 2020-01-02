%%%-------------------------------------------------------------------
%%% @doc
%%% Functions used by riak_stat_push modules
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_push_util).
-include_lib("riak_core/include/riak_stat_push.hrl").

-export([
    restart_children/1,
    stop_running_server/1,
    stop_running_server/2,
    log_and_respond/1]).

-export([
    json_stats/1,
    json_stats/3,
    get_stats/1]).

%%%===================================================================
%%% gen_server's functions
%%%===================================================================
-type children()        :: [child()].
-type child()           :: supervisor:child_spec().
%%%-------------------------------------------------------------------
%% @doc
%% Called by riak_stat_push_sup to restart the children that were
%% running before the node shut down
%% @end
%%%-------------------------------------------------------------------
-spec(restart_children(children()) -> ok).
restart_children(Children) ->
    restart_children(Children, ?ATTEMPTS, 1).
restart_children([], _, _) -> ok;
restart_children(Children,0,_) -> [log(Child,attempts_failed)
    || #{id := Child} <- Children],
    Keys = make_key(Children),
    stop_running_server(Keys);
%% Every 5 attempts double the Delay between attempts
restart_children(Children, Attempts, Delay) when (Attempts rem 5) == 0 ->
    ToRestart = restarter(Children),
    restart_children(ToRestart,Attempts-1,Delay*2);
restart_children(Children,Attempts,Delay) ->
    ToRestart = restarter(Children),
    restart_children(ToRestart,Attempts-1,Delay).

%% @doc Starts the children, if connection is refused they
%% are removed from the list and marked as running => false @end
restarter(Children) ->
    lists:foldl(fun
                    ({Child,econnrefused},Acc) ->
                        Key = make_key(Child),
                        stop_running_server(Key),
                        log_and_respond({Child,econnrefused}),
                        Acc;
                    ({Child,Pid},Acc) when is_pid(Pid) ->
                        Key = make_key(Child),
                        store_in_meta(Key,existing),
                        log_and_respond({Child,Pid}),
                        Acc;
                    ({Child,_Other},Acc) ->
                        [Child|Acc]
                end,[],start_child(Children)).

make_key(#{id := Name,modules:=[riak_stat_push_tcp]}) ->
    {tcp,atom_to_list(Name)};
make_key(#{id := Name,modules:=[riak_stat_push_udp]}) ->
    {udp,atom_to_list(Name)};
make_key(Children) when is_list(Children) ->
    [make_key(Child) || Child <- Children, is_map(Child)].

start_child(Children) ->
    riak_stat_push_sup:start_child(Children).

stop_running_server(Keys) when is_list(Keys)->
    [stop_running_server(Key) || Key <- Keys];
stop_running_server(Key) when is_tuple(Key)->
    stop_running_server(?PUSHPREFIX,Key).
stop_running_server(Prefix, Key) ->
    Fun = set_running_false_fun(),
    riak_stat_meta:put(Prefix,Key,Fun).

set_running_false_fun() ->
    fun
        (#{running := true} = MapValue) ->
            MapValue#{
                running => false,
                pid => undefined,
                modified_dt => calendar:universal_time()}
    end.

store_in_meta(Key,Type) ->
    MapValues = riak_stat_meta:get(?PUSHPREFIX,Key),
    riak_stat_push:store_setup_info(Key,MapValues,Type).

%%%===================================================================
%%% Printing API
%%%===================================================================

log_and_respond({Child, Response}) ->
    log(Child, Response),
    respond(Response).

log(Child,{ok,ok}) ->
    lager:info("Child Terminated and Deleted successfully : ~p",[Child]);
log(Child,{ok,Error}) ->
    lager:info("Child Terminated successfully : ~p",[Child]),
    lager:info("Error in Deleteing ~p : ~p",[Child,Error]);
log(Child,{Error,_}) ->
    lager:info("Child ~p termination error : ~p",[Child,Error]);
log(Child, Pid) when is_pid(Pid)->
    lager:info("Child started : ~p with Pid : ~p",[Child,Pid]);
log(Child, attempts_failed) ->
    lager:info("Attempts to start ~p failed, Exceeded Attempt limit : ~p",[Child,?ATTEMPTS]);
log(Child, econnrefused) ->
    lager:error("Child refused to start - Connection refused. Child : ~p",[Child]);
log(Child, Error) ->
    lager:error("Child refused to start - ~p. Child : ~p",[Error,Child]).

respond({ok,ok}) -> ok;
respond(Pid) when is_pid(Pid) ->
    io:fwrite("Polling Started~n");
respond(econnrefused) ->
    io:fwrite("Could not initiate Polling, Connection refused~n");
respond(Error) ->
    io:fwrite("Could not initiate Polling, ~p~n",[Error]).

%%%===================================================================
%%% Data API
%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% Get the stats from exometer and convert them into json objects
%% for both riak_stat_push_tcp/udp and riak_stat_wm.
%% @end
%%%-------------------------------------------------------------------
-spec(get_stats(pusharg()) -> jsonstats()).
get_stats(Arg) ->
    Stats = riak_stat_exom:get_values(Arg),
    lists:map(fun({N,V}) ->
        NewName = parse_name(N),
        NewValues = parse_values(V),
        {NewName,NewValues}
              end, Stats).

parse_name([riak | Name]) ->
    parse_name(Name);
parse_name(Name) ->
    [App | Stat] = Name,
    S = atom_to_list(App),
    T = ": ",
    R = stringifier(Stat),
    S++T++R.

stringifier(Stat) ->
    lists:foldr(fun
                    (S,Acc) when is_list(S) ->
                        S++" "++Acc;
                    (I,Acc) when is_integer(I) ->
                        integer_to_list(I)++" "++Acc;
                    ({IpAddr,Port},Acc) when is_tuple(IpAddr)->
                        T = ip_maker(IpAddr),
                        P = integer_to_list(Port),
                        "{"++T++","++P++"}"++" "++Acc;
                    ({IpAddr,Port},Acc) when is_list(IpAddr)->
                        T = IpAddr,
                        P = integer_to_list(Port),
                        "{"++T++","++P++"}"++" "++Acc;
                    (T,Acc) when is_tuple(T) ->
                        tuple_to_list(T)++" "++Acc;
                    (L,Acc) when is_atom(L) ->
                        atom_to_list(L)++" "++Acc;
                    (_Other,Acc) ->
                        Acc
                end, " = ",Stat).

ip_maker(IPAddr) when is_tuple(IPAddr)->
    inet:ntoa(IPAddr);
ip_maker(IPAddr) when is_list(IPAddr) ->
    {ok, TupleAddress} = inet:parse_ipv4_address(IPAddr),
    TupleAddress.

parse_values(Values) ->
    lists:foldl(fun
                    ({value,{ok,Vals}},Acc) -> [parse_values(Vals)|Acc];
                    ({ok,Vals},Acc) -> [parse_values(Vals)|Acc];
                    ({ms_since_reset,_V},Acc) -> Acc;
                    (V,Acc) -> [V|Acc]
                end, [],Values).


%%%-------------------------------------------------------------------
%% @doc
%% called by push_stats/4-5 in riak_stat_push_tcp/udp to collect
%% stats from get_stats/1 and convert them to json objects
%% @end
%%%-------------------------------------------------------------------
-spec(json_stats(listofstats()) -> jsonstats()).
json_stats(Stats) ->
    json_stats([],{instance,"web"},Stats).
json_stats(ComponentHostname, Instance, Stats) ->
    Metrics = get_stats(Stats),
    AdditionalFields = flatten_components(ComponentHostname, Instance),
    JsonStats = encode(Metrics),
    DateTime = format_time(os:timestamp()),
    [${, format_fields(AdditionalFields, []), $,,
        quote("timestamp"), $:, quote(DateTime), $,, JsonStats, $}, "\n"].

encode(Metrics) ->
    New = lists:foldl(fun({Name, Value}, Acc) ->
        case proplists:get_value(value, Value) of
            {error, not_found} -> Acc;
            _Other -> [{Name, Value} | Acc]
        end
                      end, [], Metrics),
    mochijson2:encode({struct, New}).

flatten_components(Hostname, Instance) when is_tuple(Hostname) ->
    flatten_components(ip_maker(Hostname),Instance);
flatten_components(Hostname, Instance) ->
    NewHostname = lists:flatten(Hostname),
    NewInstance = lists:flatten(Instance),
    flatten_components(NewHostname, NewInstance, []).

flatten_components(Hostname, Instance, Acc) when is_tuple(Hostname) ->
    flatten_components([],Instance,[{hostname,Hostname}|Acc]);
flatten_components(Hostname, Instance, Acc) when is_tuple(Instance) ->
    flatten_components(Hostname, [], [{instance,Instance}|Acc]);
flatten_components([{HostnameAtom,Hostname}], Instance, Acc) ->
    flatten_components([], Instance, [{HostnameAtom, Hostname} | Acc]);
flatten_components(Hostname, [{InstanceAtom, Instance}], Acc) ->
    flatten_components(Hostname, [], [{InstanceAtom, Instance} | Acc]);
flatten_components([],[],Acc) -> Acc;
flatten_components(Hostname, Instance, Acc)
    when is_list(Hostname) andalso is_list(Instance) ->
    flatten_components([],[],[{hostname, Hostname},{instance,Instance}|Acc]).

format_fields([], Json) ->
    lists:reverse(Json);
format_fields([{K,V}], Json) ->
    lists:reverse([[quote(to_list(K)), $:,
        quote_value(V)]| Json]);
format_fields([{K,V}|KVs], Json) ->
    format_fields(KVs, [[quote(to_list(K)), $:,
        quote_value(V), $,]| Json]).

quote_value([X|_] = Binaries) when is_binary(X) ->
    ["[", deep_quote(Binaries, []), "]"];
quote_value(X) ->
    do_quote_value(X).

deep_quote([], Acc) ->
    lists:reverse(Acc);
deep_quote([Last], Acc) ->
    deep_quote([], [do_quote_value(Last)|Acc]);
deep_quote([Item|Items], Acc) ->
    deep_quote(Items, [$,, do_quote_value(Item)|Acc]).


do_quote_value(X) when is_atom(X) ->
    quote(atom_to_list(X));
do_quote_value(X) when is_integer(X) ->
    integer_to_list(X);
do_quote_value(Value) when is_binary(Value) ->
    quote(binary_to_list(Value));
do_quote_value(Value) when is_list(Value) ->
    quote(Value).

to_list(X) when is_atom(X) ->
    atom_to_list(X);
to_list(X) when is_binary(X) ->
    binary_to_list(X);
to_list(X) when is_list(X) ->
    X.

%% @doc surround argument with quotes @end
quote(X) ->
    [$", X, $"].

-define(MILLISECONDS_MICROSECONDS_DIVISOR,1000).
-define(SECS_MILLISECOND_DIVISOR,1000).

%%--------------------------------------------------------------------
%% @doc
%% For a given timestamp, returns the string representation in the following
%% format, YYYY.MM.ddTHH:mm:ss.SSSZ the time is in UTC.
%% @end
%%--------------------------------------------------------------------
-spec(format_time(erlang:timestamp()) -> string()).
format_time({ _, _, MicroSeconds} = Now) ->
    {{Year, Month, Day},{ Hour, Min, Sec}} = calendar:now_to_universal_time(Now),
    lists:flatten(io_lib:format("~.4.0w-~.2.0w-~.2.0wT~.2.0w:~.2.0w:~.2.0w.~.3.0wZ",
        [Year, Month, Day, Hour, Min, Sec, MicroSeconds div ?MILLISECONDS_MICROSECONDS_DIVISOR])).

