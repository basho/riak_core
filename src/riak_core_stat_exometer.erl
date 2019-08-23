%%%-------------------------------------------------------------------
%%% @doc
%%% Exometer Man, the Manager for all things exometer, any function calls
%%% to exometer go through here.
%%% @end
%%%-------------------------------------------------------------------
-module(riak_core_stat_exometer).
-include_lib("riak_core/include/riak_core_stat.hrl").

%% API
-export([
  find_entries/2,
  find_alias/1,
  find_static_stats/1,
  find_stats_info/2,
  register_stat/4,
  alias/1,
  aliases/2,
  re_register/2,
  read_stats/1,
  get_datapoint/2,
  get_value/1,
  get_values/1,
  select_stat/1,
  info/2,
  aggregate/2,
  update_or_create/3,
  update_or_create/4,
  change_status/1,
  set_opts/2,
  unregister_stat/1,
  reset_stat/1
]).

%% Secondary API
-export([
  timestamp/0
]).

%% additional API
-export([
  start/0,
  stop/0
]).


%%%===================================================================
%%% API
%%%===================================================================

-spec(find_entries(stats(), status()) -> stats()).
%% @doc
%% Use @see exometer:find_entries to get the name, type and status of
%% a stat given, fo all the stats that match the Status given put into
%% a list to be returned
%% @end
find_entries(Stats, Status) ->
  lists:foldl(fun(Stat, Found) ->
    case find_entries(Stat) of
      [{Name, _Type, EStatus}] when EStatus == Status; Status == '_' ->
        [{Name, Status} | Found];
      [{_Name, _Type, _EStatus}] -> % Different status
        Found;
      [] ->
        Found
    end
            end, [], Stats).

-spec(find_static_stats(stats()) -> stats()).
%% @doc
%% Find all the enabled stats in exometer with the value 0 or [] and
%% put into a list
%% @end
find_static_stats(Stats) when is_list(Stats) ->
  lists:map(fun(Stat) ->
    case get_values(Stat) of
      [] ->
        [];
      List ->
        lists:foldl(fun
                      ({Name, 0}, Acc) ->
                        [{Name, 0} | Acc];
                      ({Name, []}, Acc) ->
                        [{Name, 0} | Acc];
                      ({_Name, _V}, Acc) ->
                        Acc
                    end, [], List)
    end
            end, Stats).

-spec(find_stats_info(stats(), datapoint()) -> stats()).
%% @doc
%% Find the stats and the info for that stat
%% @end
find_stats_info(Stats, Info) when is_atom(Info) ->
  find_stats_info(Stats, [Info]);
find_stats_info(Stat, Info) when is_list(Info) ->
  lists:foldl(fun(DP,Acc) ->
    case get_datapoint(Stat, DP) of
      {ok, [{DP,Error}]} ->
        io:format("Error: ~p~n", [Error]),
        Acc;
      {ok,Value} ->
        io:format("Value: ~p~n", [Value]),
        [{DP,Value}|Acc];
      {error, R} ->
        io:format("error: ~p~n", [R]),
        Acc;
      {DP,undefined} ->
        io:format("undefined: ~p~n", [undefined]),
        Acc
    end
    end, [],Info).
%%  lists:map(fun(Stat) ->
%%  lists:foldl(fun(Stat, Acc) ->
%%    [case get_datapoint(Stat,DP) of

%%     end || DP <- Info].

%%    [{Stats, [{DP, case get_datapoint(Stats, DP) of
%%                     {ok, Value} -> Value;
%%                     {error, _R} ->
%%                   end} || DP <- Info]}].
%%            end, Stats).


%%%%%%%%%%%%%% CREATING %%%%%%%%%%%%%%

-spec(register_stat(statname(), type(), options(), aliases()) ->
  ok | error()).
%% @doc
%% Registers all stats, using  exometer:re_register/3, any stat that is
%% re_registered overwrites the previous entry, works the same as
%% exometer:new/3 except it wont return an error if the stat already
%% is registered.
%% @end
register_stat(StatName, Type, Opts, Aliases) ->
%%  io:format("riak_stat_exometer:register_stat(~p)~n", [StatName]),
  re_register(StatName, Type, Opts),
%%  lager:info("re_register_stat: ~p ~p ~p ~p~n",[StatName,Type, Opts, Aliases]),
%%  io:format("riak_stat_exometer:re_register(Stat) = ~p~n", [Registerd]),
  lists:foreach(
    fun({DP, Alias}) ->
      aliases(new, [Alias, StatName, DP])
    end, Aliases).

re_register(StatName, Type) ->
  re_register(StatName, Type, []).
re_register(StatName, Type, Opts) ->
%%  io:format("Name: ~p, Type: ~p, Opts: ~p~n", [StatName, Type, Opts]),
  exometer:re_register(StatName, Type, Opts).

-spec(alias(Group :: term()) -> ok | acc()).
alias(Group) ->
  lists:keysort(
    1,
    lists:foldl(
      fun({K, DPs}, Acc) ->
        case get_datapoint(K, [D || {D,_} <- DPs]) of
          {ok, Vs} when is_list(Vs) ->
            lists:foldr(fun({D,V}, Acc1) ->
              {_,N} = lists:keyfind(D,1,DPs),
              [{N,V}|Acc1]
                        end, Acc, Vs);
          Other ->
            Val = case Other of
                    {ok, disabled} -> undefined;
                    _ -> 0
                  end,
            lists:foldr(fun({_,N}, Acc1) ->
              [{N,Val}|Acc1]
                        end, Acc, DPs)
        end
      end, [], orddict:to_list(Group))).

-spec(aliases(type(), list()) -> ok | acc() | error()).
%% @doc
%% goes to exometer_alias and performs the type of alias function specified
%% @end
aliases(new, [Alias, StatName, DP]) ->
%%  io:format("riak_stat_exometer:aliases(~p, ~p, ~p)~n", [Alias, StatName, DP]),
  exometer_alias:new(Alias, StatName, DP);
%%  io:format("riak_stat_exometer:aliases(etc) = ~p~n", Answer);
aliases(prefix_foldl, []) ->
  exometer_alias:prefix_foldl(<<>>, alias_fun(), orddict:new());
aliases(regexp_foldr, [N]) ->
  exometer_alias:regexp_foldr(N, alias_fun(), orddict:new()).

alias_fun() ->
  fun(Alias, Entry, DP, Acc) ->
    orddict:append(Entry, {DP, Alias}, Acc)
  end.

%%%%%%%%%%%%%% READING %%%%%%%%%%%%%%

-spec(read_stats(App :: atom()) -> value() | error()).
%% @doc
%% read the stats from exometer and print them out in the format needed, uses
%% exometer functions.
%% @end
read_stats(App) ->
  Values = get_values([?PFX, App]),
  [Name  || {Name, _V} <- Values].

-spec(get_datapoint(statname(), datapoint()) -> exo_value() | error()).
%% @doc
%% Retrieves the datapoint value from exometer
%% @end
get_datapoint(Name, Datapoint) ->
  exometer:get_value(Name, Datapoint).

-spec(get_value(statname()) -> exo_value() | error()).
%% @doc
%% Same as the function above, except in exometer the Datapoint:
%% 'default' is inputted, however it is used by some modules
%% @end
get_value(S) ->
  exometer:get_value(S).

-spec(get_values(any()) -> exo_value() | error()).
%% @doc
%% The Path is the start or full name of the stat(s) you wish to find,
%% i.e. [riak,riak_kv] as a path will return stats with those to elements
%% in their path. and uses exometer:find_entries and above function
%% @end
get_values(Path) ->
  exometer:get_values(Path).

-spec(select_stat(pattern()) -> value()).
%% @doc
%% Find the stat in exometer using this pattern
%% @end
select_stat(Pattern) ->
%%  io:format("23 riak_core_stat_exometer:select_stat(~p)~n", [Pattern]),
  exometer:select(Pattern).

-spec(find_entries(stats()) -> stats()).
%% @doc
%% @see exometer:find_entries
%% @end
find_entries(Stat) ->
  exometer:find_entries(Stat).

find_alias([]) ->
  [];
find_alias({DP,Alias}) -> %% which is better alias or get_value
%%  {T1,Val1} = timer:tc(fun dp_dp/2, [Name, DP]),
%%  {T2,_Val2} = timer:tc(fun alias_dp/2, [Alias, DP]),
%%  io:format("Time1 : ~p,~nTime2 : ~p~n",[T1,T2]),
%%  Diff = timer:now_diff(T2,T1),
%%  io:format("Diff : ~p~n",[T1 - T2]),
%%  Val1.
  alias_dp({DP,Alias}).


%%dp_dp(Name, DP) ->
%%  case get_datapoint(Name, DP) of
%%    {ok, Value} -> Value;
%%    _Error -> []
%%  end.

alias_dp({DP,Alias}) ->
  case exometer_alias:get_value(Alias) of
    {ok, Val} -> {DP,Val};
    _ -> []
  end.

-spec(info(statname(), info()) -> value()).
%% @doc
%% find information about a stat on a specific item
%% @end
info(Name, Type) ->
  exometer:info(Name, Type).

-spec(aggregate(pattern(), datapoint()) -> stats()).
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

%% @doc In case the aggregation is for the average of certain values @end
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

do_average(Num, DataValues)  ->
  lists:map(fun({DP, Values}) ->
    {DP, {aggregated, Values}, {average, Values/Num}}
            end, DataValues).

metric_names(Pattern) ->
  [Name || {Name, _Type, _Status} <- select_stat(Pattern)].

%%%%%%%%%%%%%% UPDATING %%%%%%%%%%%%%%

-spec(update_or_create(statname(), value(), type()) ->
  ok | error()).
%% @doc
%% Sends the stat to exometer to get updated, unless it is not already a stat then it
%% will be created. First it is checked in meta_mgr and registered there.
%% @end
update_or_create(Name, UpdateVal, Type) ->
  update_or_create(Name, UpdateVal, Type, []).
-spec(update_or_create(Name :: list() | atom(), UpdateVal :: any(), Type :: atom() | term(), Opts :: list()) ->
  ok | term()).
update_or_create(Name, UpdateVal, Type, Opts) ->
  exometer:update_or_create(Name, UpdateVal, Type, Opts).

-spec(change_status(Stats :: list() | term()) ->
  ok | term()).
%% @doc
%% enable or disable the stats in the list
%% @end
change_status(Stats) when is_list(Stats) ->
  lists:map(fun
              ({Stat, {status, Status}}) -> change_status(Stat, Status);
              ({Stat, Status}) ->           change_status(Stat, Status)
            end, Stats);
change_status({Stat, Status}) ->
  change_status(Stat, Status).
change_status(Stat, Status) ->
  set_opts(Stat, [{status, Status}]).


-spec(set_opts(statname(), options()) -> ok | error()).
%% @doc
%% Set the options for a stat in exometer, setting the status as either enabled or
%% disabled in it's options in exometer will change its status in the entry
%% @end
set_opts(StatName, Opts) ->
  exometer:setopts(StatName, Opts).

%%%%%%%%%%%%% UNREGISTER / RESET %%%%%%%%%%%%%%

-spec(unregister_stat(statname()) -> ok | error()).
%% @doc
%% deletes the stat entry from exometer
%% @end
unregister_stat(StatName) ->
  exometer:delete(StatName).

-spec(reset_stat(statname()) -> ok | error()).
%% @doc
%% resets the stat in exometer
%% @end
reset_stat(StatName) ->
  exometer:reset(StatName).


%%%%%%%%%%%% Helper Functions %%%%%%%%%%%

-spec(timestamp() -> timestamp()).
%% @doc
%% Returns the timestamp to put in the stat entry
%% @end
timestamp() ->
  exometer_util:timestamp().

%%%%%%%%%%%% Extras %%%%%%%%%%%%%%%

%% @doc
%% Used in testing in certain modules
%% @end
start() ->
  exometer:start().

stop() ->
  exometer:stop().

