%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_push_util).

%% API
-export([send_after/3, json_stats/3, get_stats/1]).

-define(EXCLUDED_DATAPOINTS,   [ms_since_reset]).


send_after(Interval, Arg, WhoTo) ->
    erlang:send_after(Interval, WhoTo, Arg).

json_stats(ComponentHostname, Instance, Stats) ->
    Metrics = get_stats(Stats),
    case riak_stat_json:metrics_to_json(Metrics, [{instance, Instance},{hostname, ComponentHostname}], ?EXCLUDED_DATAPOINTS) of

        [] -> ok;
        JsonStats -> JsonStats
    end.

get_stats(Stats) ->
    riak_stat_exom:get_values(Stats).



%% todo: create a function to save the configuration of the endpoint pushing
%% of the stats. could save it in the metadata to persist? with a profile