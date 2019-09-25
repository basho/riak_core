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
%%
%%%%%-------------------------------------------------------------------
%%%% @doc
%%%% retrieve all the stats out of riak_kv_status
%%%% todo: improve the stat collection method
%%%% @end
%%%%%-------------------------------------------------------------------
%%-spec(get_stats() -> stats()).
%%get_stats() ->
%%    case application:get_env(riak_core, push_defaults_type, classic) of
%%        classic ->
%%            riak_kv_status:get_stats(web);
%%        beta ->  %% get all stats and their values
%%            riak_stat_exom:get_values(['_'])
%%    end.
%%

%% starting up endpoint servers in tcp and udp
%% todo: create in this module

%% todo: create a function to save the configuration of the endpoint pushing
%% of the stats. could save it in the metadata to persist? with a profile