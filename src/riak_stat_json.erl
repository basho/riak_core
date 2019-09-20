%%%-------------------------------------------------------------------
%%% @doc
%%% Turn the metrics from exometer into json objects
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_json).
-include_lib("riak_core/include/riak_stat.hrl").

%% API
-export([
    metrics_to_json/2,
    metrics_to_json/3,
    metric_to_string/2,
    format_fields/2,
    format_time/0,
    format_time/1,
    encode/1
]).

%%--------------------------------------------------------------------
%% @doc
%% Metrics to JSON
%% @end
%%--------------------------------------------------------------------
-spec(metrics_to_json(stats(), list(), list()) -> jsonprops()).

metrics_to_json(Metrics,Fields,ExcludedDataPoints) ->
    Dict = make_dict_of_stats(Metrics),
    ok.

make_dict_of_stats(Metrics) ->
    lists:foldl(fun
                    ({Stat,DataPoints},Dict) ->
                        group_data(Stat, DataPoints, Dict)
                end, dict:new(), Metrics).

de_nest_stats([Stat],DataPoints,Dict) ->
    dict:update(Stat,fun(D) ->
    lists:append(DataPoints,D)
                     end,DataPoints,Dict)


metrics_to_json(Metrics, AdditionalFields, ExcludedDataPoints) ->
    Dict1 = lists:foldl(fun
                            ({Metric, DataPoints}, Dict) ->
        group_data(Metric, DataPoints, Dict)
                        end, dict:new(), Metrics),
    JsonStats = lists:reverse(
        to_json(
            dict:to_list(Dict1), [], 0, ExcludedDataPoints)),

    case JsonStats of
        [] ->
            [];
        _ ->
            DateTime = format_time(),
            [${, format_fields(AdditionalFields, []), $,,
                quote("timestamp"),
                $:, quote(DateTime), $,,
                JsonStats, $}, "\n"]
    end.

metrics_to_json(Metrics, ExcludedDataPoints) ->
    Dict1 = lists:foldl(fun({Metric, DataPoints}, Dict) ->
        group_data(Metric, DataPoints, Dict)
                        end, dict:new(), Metrics),
    JsonStats = lists:reverse(
        to_json(
            dict:to_list(Dict1), [], 0, ExcludedDataPoints)),

    case JsonStats of
        [] ->
            [];
        _ ->
            DateTime = format_time(), %% {timestamp,datetime,[stats]}
            [${, quote("timestamp"),
                $:, quote(DateTime), $,,
                    JsonStats, $}, "\n"]
    end.


group_data([Metric], DataPoints, Dict) ->
    dict:update(Metric, fun(A) ->
        lists:append(DataPoints, A)
                        end, DataPoints, Dict);
group_data([Metric|Metrics], DataPoints, Dict) ->
    NewDict = case dict:find(Metric, Dict) of
                  {ok, Value} ->
                      group_data(Metrics, DataPoints, Value);
                  error ->
                      group_data(Metrics, DataPoints, dict:new())
              end,
    dict:store(Metric, NewDict, Dict).


to_json([], Acc, _, _ExcludedDataPoints) ->
    Acc;
to_json([{Name, [{_DataPoint,_Value}|_] = DataPoints}|Others],
    Acc, _Count, ExcludedDataPoints) ->
    case Others of
        [] ->
            to_json(Others, [
                [$", metric_elem_to_binary(Name), $",
                    $:,
                    ${, datapoint_to_json(DataPoints, [], ExcludedDataPoints), $}]|Acc],
                0, ExcludedDataPoints);
        _ ->
            to_json(Others, [
                [$", metric_elem_to_binary(Name), $",
                    $:,
                    ${, datapoint_to_json(DataPoints, [], ExcludedDataPoints), $},$,]|Acc],
                0, ExcludedDataPoints)
    end;
to_json([{Metric, Dict}|Others], Acc, Count, ExcludedDataPoints) ->
    Acc1 = to_json(
        dict:to_list(Dict),
        [[$", metric_elem_to_binary(Metric), $", $:, ${]|Acc],
            Count + 1, ExcludedDataPoints),
    case Others of
        [] ->
            to_json(Others, [[$}]|Acc1], 0, ExcludedDataPoints);
        _ ->
            to_json(Others, [[$},$,]|Acc1], 0, ExcludedDataPoints)
    end.

format_fields([], Json) ->
    lists:reverse(Json);
format_fields([{K,V}], Json) ->
    lists:reverse([[quote(to_list(K)), $:,
        quote_value(V)]| Json]);
format_fields([{K,V}|KVs], Json) ->
    format_fields(KVs, [[quote(to_list(K)), $:,
        quote_value(V), $,]| Json]).

%% @doc surround argument with quotes @end
quote(X) ->
    [$", X, $"].


to_list(X) when is_atom(X) ->
    atom_to_list(X);
to_list(X) when is_binary(X) ->
    binary_to_list(X);
to_list(X) when is_list(X) ->
    X.

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


metric_elem_to_binary(E) when is_atom(E) ->
    atom_to_binary(E, latin1);
metric_elem_to_binary(E) when is_list(E) ->
    list_to_binary(E);
metric_elem_to_binary(E) when is_integer(E) ->
    integer_to_binary(E);
metric_elem_to_binary(E) when is_binary(E) ->
    E.


datapoint_to_json([], Acc, _ExcludedDataPoints) ->
    lists:reverse(Acc);
datapoint_to_json([{DataPoint, Value}|DataPoints], Acc, ExcludedDataPoints) ->
    case lists:member(DataPoint, ExcludedDataPoints) of
        false ->
            Line = [$", name(DataPoint), $", $:, integer_to_binary(Value)],
            case DataPoints of
                [] ->
                    datapoint_to_json(DataPoints, [Line|Acc], ExcludedDataPoints);
                _ ->
                    datapoint_to_json(DataPoints, [[Line, $,]|Acc], ExcludedDataPoints)
            end;
        true ->
            case DataPoints of
                [] ->
                    [H|T] = Acc,
                    datapoint_to_json(DataPoints, [H--[$,]|T], ExcludedDataPoints);
                _ ->
                    datapoint_to_json(DataPoints, Acc, ExcludedDataPoints)
            end
    end.

name(DataPoint) when is_atom(DataPoint) ->
    atom_to_binary(DataPoint, latin1);
name(DataPoint) when is_integer(DataPoint) ->
    integer_to_binary(DataPoint);
name(DataPoint) when is_list(DataPoint) ->
    list_to_binary(DataPoint).


%% @doc Metrics to KV String @end
metric_to_string(Metric, DataPoints) ->
    metric_to_string(Metric, DataPoints, []).

metric_to_string(_Metric, [], Acc) ->
    Acc;
metric_to_string(Metric, [{DataPoint, Value}|DataPoints], Acc) ->
    metric_to_string(Metric, DataPoints, [to_kv(Metric, DataPoint, Value)|Acc]).

to_kv(Metric, DataPoint, Value) ->
    [metric_to_binary(Metric), $., name(DataPoint), $=, integer_to_binary(Value), $\n].

metric_to_binary([Final]) ->
    metric_elem_to_binary(Final);
metric_to_binary([H | T]) ->
    <<(metric_elem_to_binary(H))/binary, $., (metric_to_binary(T))/binary >>.


%%%===================================================================
%%% helper functions
%%%===================================================================

-define(MILLISECONDS_MICROSECONDS_DIVISOR,1000).
-define(SECS_MILLISECOND_DIVISOR,1000).

format_time() ->
    format_time(os:timestamp()).

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

encode(Arg) ->
    mochijson:encode(Arg).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

funx() ->
Dict =
    lists:foldl(fun
                    ({[Metric],DataPoints},Dict) ->
                        dict:update(Metric,fun(A) ->
                            lists:append(DataPoints,A) end,DataPoints,Dict);
                    ({[Metric|Metrics],DataPoints},Dict) ->
                        NewDict = case dict:find(Metric,Dict) of
                                      {ok,Value} ->
                                          io:format("first one~n"),
                                          lists:foldl(fun({[Stat],DPs},Dic) ->
                                              dict:update(Stat,fun(A)-> lists:append(DPs,A) end,DPs,Dic);
                                              ({[Stat|Stats],DPs},Dic) -> NewDic = case dict:find(Stat,Dic) of {ok,_} -> io:format("Too Nested~n"),Dic;	error -> io:format("Im not recursing again~n"),Dic end,	dict:store(Stat,NewDic,Dic)end, Value, Metrics);error -> io:format("second one~n"),lists:foldl(fun({[Stat],DPs},Dic) ->	dict:update(Stat,fun(A)-> lists:append(DPs,A) end,DPs,Dic);({[Stat|Stats],DPs},Dic) -> NewDic = case dict:find(Stat,Dic) of {ok,_} -> io:format("Too Nested~n"),Dic; error -> io:format("Im not recursing again~n"),Dic end,dict:store(Stat,NewDic,Dic) end, dict:new(), Metrics) end, dict:store(Metric,NewDict,Dict).