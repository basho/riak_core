%%%-------------------------------------------------------------------
%%% @author savannahallsop
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Sep 2019 10:57
%%%-------------------------------------------------------------------
-module(riak_stat_push_util).
-author("savannahallsop").

%% API
-export([send_after/3, dispatch_stats/7, get_stats/1]).

-define(EXCLUDED_DATAPOINTS,   [ms_since_reset]).


send_after(Interval, Arg, WhoTo) ->
    erlang:send_after(Interval, WhoTo, Arg).

dispatch_stats(Socket, ComponentHostname, Instance, MonitoringHostname, Port, Stats, WhoTo) ->
    Metrics = get_stats(Stats),
    case riak_stat_json:metrics_to_json(Metrics,
        [{instance, Instance},{hostname, ComponentHostname}], ?EXCLUDED_DATAPOINTS) of
        [] ->
            ok;
        JsonStats ->
            ok = send(Socket, MonitoringHostname, Port, JsonStats ,WhoTo)
    end,
    erlang:send_after(?STATS_UPDATE_INTERVAL, WhoTo, {dispatch_stats, Stats}).

get_stats(Stats) ->
    riak_stat_exom:get_values(Stats).

send(Socket,MonitoringHostName, Port,JsonStats, WhoTo) ->
    WhoTo ! {guess_whos_back,Socket,MonitoringHostName,Port,JsonStats}.