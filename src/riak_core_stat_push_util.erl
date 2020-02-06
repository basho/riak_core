%%%-------------------------------------------------------------------
%%% @doc
%%% Functions used by riak_stat_push modules
%%% @end
%%%-------------------------------------------------------------------
-module(riak_core_stat_push_util).
-include("riak_stat_push.hrl").

-export([
    encode/1]).

-export([
    json_stats/1,
    get_stats/1]).

%
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
    Stats = riak_core_stats_mgr:get_values(Arg),
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

ip_maker([IPAddr]) ->
    ip_maker(IPAddr);
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
    Metrics = get_stats(Stats),
    JsonStats = encode(Metrics),
    DateTime = format_time(os:timestamp()),
    [${, quote("timestamp"), $:, quote(DateTime), $,, JsonStats, $}, "\n"].

encode(Metrics) ->
    New = lists:foldl(fun({Name, Value}, Acc) ->
        case proplists:get_value(value, Value) of
            {error, not_found} -> Acc;
            _Other -> [{Name, Value} | Acc]
        end
                      end, [], Metrics),
    mochijson2:encode({struct, New}).

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
format_time(Now) ->
    DateTime = calendar:now_to_universal_time(Now),
    httpd_util:rfc1123_date(DateTime).

