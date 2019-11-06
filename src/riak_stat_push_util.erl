%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_push_util).

%% API
-export([
    json_stats/1,
    json_stats/3,
    get_stats/1]).

-define(Excluded_Datapoints,   [ms_since_reset]).
-define(PUSHPREFIX(Node), {riak_stat_push, node()}).


json_stats(Stats) ->
    json_stats([],{instance,"web"},Stats).
json_stats(ComponentHostname, Instance, Stats) ->
    Metrics = get_stats(Stats),
    AdditionalFields = lists:flatten([ComponentHostname, Instance]),
    JsonStats = mochijson2:encode({struct, Metrics}),
    DateTime = format_time(os:timestamp()),
    [${, format_fields(AdditionalFields, []), $,,
        quote("timestamp"), $:, quote(DateTime), $,, JsonStats, $}, "\n"].

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

ip_maker(IpAddr) ->
    Ip = tuple_to_list(IpAddr),
    NewIp = [integer_to_list(N) || N <- Ip],
    lists:join(".",NewIp).

parse_values(Values) ->
    lists:foldl(fun
                    ({ms_since_reset,_V},Acc) -> Acc;
                    (V,Acc) -> [V|Acc]
                end, [],Values).
