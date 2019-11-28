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

%%%===================================================================
%%% gen_server's functions
%%%===================================================================

store_setup_info(State) ->
    ok.






%%%===================================================================
%%%===================================================================

%% todo: write a manual json object encoder for the stats http and endpoint.
%% todo: write specs and docs for the functions in this module
%% todo: add default functions for both the tcp gen server and udp gen server to use
%% as in: when the gen_server is started have it so it so the gen_server stores the
%% information into the metadata, not riak_stat_push, and the same for when the server is
%% terminated.

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
