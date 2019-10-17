%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_push_util).

%% API
-export([
    json_stats/3,
    get_stats/1]).

-define(EXCLUDED_DATAPOINTS,   [ms_since_reset]).


json_stats(ComponentHostname, Instance, Stats) ->
    lager:info("Dispatching Stats~n"),
    Metrics = get_stats(Stats),
    case riak_stat_json:metrics_to_json(Metrics,
        [{instance, Instance},{hostname, ComponentHostname}], ?EXCLUDED_DATAPOINTS) of
        [] -> ok;
        JsonStats -> JsonStats
    end.

get_stats(Stats) ->
    riak_stat_exom:get_values(Stats).

