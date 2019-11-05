%%%-------------------------------------------------------------------
%%% @doc
%%% Manages all things exometer, any function calls
%%% to exometer go through here.
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_exom).
-include_lib("riak_core/include/riak_stat.hrl").

-export([
%% Registration API
    register/1,

%% Read API
    get_values/1, get_info/2, find_entries/2, find_entries/1,
    get_datapoint/2, get_value/1, select/1, sample/1,
    find_stats_info/2, aggregate/2, resolve/1,

%% Update API
    update/3, update/4, change_status/1, change_status/2,

%% Deleting/Resetting API
    reset_stat/1, unregister/1,

%% Other
    alias/1, aliases/2, find_alias/1, timestamp/0]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% NB : DP = DataPoint
%%      Alias : Name of a datapoint in a Stat.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%===================================================================
%%% Registration API
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%% Registers all stats, using  exometer:re_register/3, any stat that is
%% re_registered overwrites the previous entry, works the same as
%% exometer:new/3 except it wont return an error if the stat already
%% is registered.
%% @end
%%%-------------------------------------------------------------------
-spec(register(tuple_stat()) -> ok | error()).
register({StatName, Type, Opts, Aliases}) ->
    register(StatName, Type, Opts, Aliases).
register(StatName, Type, Opts, Aliases) ->
    exometer:re_register(StatName, Type ,Opts),
    lists:foreach(fun
                      ({DP,Alias}) ->
                          aliases(new,[Alias,StatName,DP])
                  end,Aliases).

%%%-------------------------------------------------------------------
%% @doc
%% goes to exometer_alias and performs the type of alias function specified
%% @end
%%%-------------------------------------------------------------------
-spec(aliases(aliastype(), list()) -> ok | acc() | error()).
aliases(new, [Alias,StatName,DP]) ->
    exometer_alias:new(Alias,StatName,DP);
aliases(prefix_foldl,[]) ->
    exometer_alias:prefix_foldl(<<>>,alias_fun(),orddict:new());
aliases(regexp_foldr,[N]) ->
    exometer_alias:regexp_foldr(N,alias_fun(),orddict:new()).

alias_fun() ->
    fun(Alias, Entry, DP, Acc) ->
        orddict:append(Entry, {DP, Alias}, Acc)
    end.

-spec(alias(Group :: orddict:orddict()) -> ok | acc()).
alias(Group) ->
    lists:keysort(
        1,
        lists:foldl(
            fun({K, DPs}, Acc) ->
                case get_datapoint(K, [D || {D, _} <- DPs]) of
                    {ok, Vs} when is_list(Vs) ->
                        lists:foldr(fun({D, V}, Acc1) ->
                            {_, N} = lists:keyfind(D, 1, DPs),
                            [{N, V} | Acc1]
                                    end, Acc, Vs);
                    Other ->
                        Val = case Other of
                                  {ok, disabled} -> undefined;
                                  _ -> 0
                              end,
                        lists:foldr(fun({_, N}, Acc1) ->
                            [{N, Val} | Acc1]
                                    end, Acc, DPs)
                end
            end, [], orddict:to_list(Group))).

%%%-------------------------------------------------------------------

find_alias([]) ->
    [];
find_alias({DP, Alias}) ->
    alias_dp({DP, Alias}).

alias_dp({DP, Alias}) ->
    case exometer_alias:get_value(Alias) of
        {ok, Val} -> {DP, Val};
        _ -> []
    end.

%%%===================================================================
%%% Reading Stats API
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%% find the entry and dp for an alias given
%% @end
%%%-------------------------------------------------------------------
-spec(resolve(aliases()) -> listofstats() | error()).
resolve(Alias) ->
    exometer_alias:resolve(Alias).

%%%-------------------------------------------------------------------
%% @doc
%% The Path is the start or full name of the stat(s) you wish to find,
%% i.e. [riak,riak_kv|'_'] as a path will return stats with those to
%% elements in their path. and uses exometer:find_entries
%% @end
%%%-------------------------------------------------------------------
-spec(get_values(metricname()) -> n_v_stats() | error()).
get_values(Path) ->
    exometer:get_values(Path).

%%%-------------------------------------------------------------------
%% @doc
%% find information about a stat on a specific item
%% @end
%%%-------------------------------------------------------------------
-spec(get_info(metricname(), (info() | attributes())) -> stat_value()).
get_info(Stat, Info) ->
    exometer:info(Stat, Info).

%%%-------------------------------------------------------------------
%% @doc
%% Use @see exometer:find_entries to get the name, type and status of
%% a stat given, fo all the stats that match the Status given put into
%% a list to be returned
%% @end
%%%-------------------------------------------------------------------
-spec(find_entries(metricname(), status()) -> listofstats()).
find_entries(Stats, Status) ->
    lists:foldl(
        fun(Stat, Found) ->
            case find_entries(Stat) of
                [{Name, Type, EStatus}]
                    when EStatus == Status orelse Status == '_' ->
                    [{Name, Type, Status} | Found];
                [{_Name, _Type, _EStatus}] -> % Different status
                    Found;
                [] ->
                    Found
            end
        end, [], Stats).

find_entries(Stat) ->
    exometer:find_entries(Stat).

%%%-------------------------------------------------------------------
%% @doc
%% Retrieves the datapoint value from exometer
%% @end
%%%-------------------------------------------------------------------
-spec(get_datapoint(metricname(), datapoints()) -> stat_value() | error()).
get_datapoint(Name, Datapoint) ->
    exometer:get_value(Name, Datapoint).

get_value(Name) ->
    exometer:get_value(Name).

%%%-------------------------------------------------------------------
%% @doc
%% Find the stat in exometer using pattern :: ets:match_spec()
%% @end
%%%-------------------------------------------------------------------
-spec(select(pattern()) -> stat_value()).
select(Pattern) ->
    exometer:select(Pattern).

%%%-------------------------------------------------------------------
-spec(sample(metricname()) -> ok | error()).
sample(Stat) ->
    exometer:sample(Stat).

%%%-------------------------------------------------------------------
%% @doc
%% Find the stats and the info for that stat
%% @end
%%%-------------------------------------------------------------------
-spec(find_stats_info(metricname(), datapoints()) -> listofstats()).
find_stats_info(Stats, Info) when is_atom(Info) ->
    find_stats_info(Stats, [Info]);
find_stats_info(Stat, Info) when is_list(Info) ->
    lists:foldl(fun(DP, Acc) ->
        case get_datapoint(Stat, DP) of
            {ok, [{DP, _Error}]} ->
                Acc;
            {ok, Value} ->
                [{DP, Value} | Acc];
            {error, _R} ->
                Acc;
            {DP, undefined} ->
                Acc
        end
                end, [], Info).


%%%-------------------------------------------------------------------
%% @doc
%% "Aggregate data points of matching entries"
%% for example: in riak_kv_stat:stats() ->
%%
%% aggregate({{['_',actor_count], '_', '_'},[],[true]}], [max])
%%
%% aggregates the max of the:
%% [counter,actor_count],
%% [set,actor_count] and
%% [map,actor_count]
%% By adding them together.
%% .
%% @end
%%%-------------------------------------------------------------------
-spec(aggregate(pattern(), datapoints()) -> listofstats()).
aggregate(Pattern, Datapoints) ->
    Entries = metric_names(Pattern),
    Num = length(Entries),
    {AvgDP, OtherDP} = aggregate_average(Datapoints),
    AggrAvgs = do_aggregate(Pattern, AvgDP),
    OtherAggs = do_aggregate(Pattern, OtherDP),
    Averaged = do_average(Num, AggrAvgs),
    io:fwrite("Aggregation of : ~n"),
    [io:fwrite("~p  ", [Name]) || Name <- Entries],
    io:fwrite("~n~p~n~p~n", [Averaged, OtherAggs]).

do_aggregate(_Pattern, []) ->
    [];
do_aggregate(Pattern, DataPoints) ->
    lists:map(fun(DP) ->
        {DP, exometer:aggregate(Pattern, DP)}
              end, DataPoints).

%%%-------------------------------------------------------------------
%% @doc
%% In case the aggregation is for the average of certain values
%% @end
%%%-------------------------------------------------------------------
aggregate_average(DataPoints) ->
    lists:foldl(fun(DP, {Avg, Other}) ->
        {agg_avg(DP, Other, Avg), lists:delete(DP, Other)}
                end, {[], DataPoints}, [one, mean, median, 95, 99, 100, max]).

agg_avg(DP, DataPoints, AvgAcc) ->
    case lists:member(DP, DataPoints) of
        true ->
            [DP | AvgAcc];
        false ->
            AvgAcc
    end.

do_average(Num, DataValues) ->
    lists:map(fun({DP, Values}) ->
        {DP, {aggregated, Values}, {average, Values / Num}}
              end, DataValues).

metric_names(Pattern) ->
    [Name || {Name, _Type, _Status} <- select(Pattern)].


%%%===================================================================
%%% Updating Stats API
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%% Updates the stat, if the stat does not exist it will create a
%% crude version of the metric
%% @end
%%%-------------------------------------------------------------------
-spec(update(metricname(),incrvalue(),type(),options()) -> ok).
update(Name, Val, Type) ->
    update(Name, Val, Type, []).
update(Name, Val, Type, Opts) ->
    exometer:update_or_create(Name, Val,Type, Opts).

%%%-------------------------------------------------------------------
%% @doc
%% enable or disable the stats in the list
%% @end
%%%-------------------------------------------------------------------
-spec(change_status(n_s_stats()) -> print()).
change_status(Stats) when is_list(Stats) ->
    lists:map(fun
                  ({Stat,{status,Status}}) -> change_status(Stat,Status);
                  ({Stat, Status}) -> change_status(Stat, Status)
              end, Stats);
change_status({Stat, Status}) ->
    change_status(Stat, Status).
change_status(Stat, Status) ->
    set_opts(Stat, [{status, Status}]),
    io:fwrite("~p : ~p~n",[Stat,Status]).

%%%-------------------------------------------------------------------
%% @doc
%% Set the options for a stat in exometer, setting the status as either enabled or
%% disabled in it's options in exometer will change its status in the entry
%% @end
%%%-------------------------------------------------------------------
-spec(set_opts(metricname(), options()) -> ok | error()).
set_opts(StatName, Opts) ->
    exometer:setopts(StatName, Opts).


%%%===================================================================
%%% Deleting/Resetting Stats API
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%% resets the stat in exometer
%% @end
%%%-------------------------------------------------------------------
-spec(reset_stat(metricname()) -> ok | error()).
reset_stat(StatName) ->
    exometer:reset(StatName).

%%%-------------------------------------------------------------------
%% @doc
%% deletes the stat entry from exometer
%% @end
%%%-------------------------------------------------------------------
-spec(unregister(metricname()) -> ok | error()).
unregister(StatName) ->
    exometer:delete(StatName).

%%%===================================================================
%%% Deleting/Resetting Stats API
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%% Returns the timestamp to put in the stat entry
%% @end
%%%-------------------------------------------------------------------
-spec(timestamp() -> timestamp()).
timestamp() ->
    exometer_util:timestamp().
