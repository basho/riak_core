%%%-------------------------------------------------------------------
%%% @doc
%%% Register, Read, Update or Unregister stats from exometer or metadata
%%% through this module, calls from the _stat modules call through this module,
%%% whereas console "riak-admin stat ---" commands call through riak_core
%%% _stat_console.
%%% .
%%% @end
%%%-------------------------------------------------------------------
-module(riak_core_stat_admin).

-include_lib("riak_core/include/riak_core_stat.hrl").

%% API
-export([
    get_stats/0,
    get_stats/1,
    get_stat/1,
    get_value/1,
    get_info/1,
    aggregate/2,
    register/2,
    update/3,
    unregister/1,
    unregister/4,
    prefix/0
  ]).

%%%===================================================================
%%% API
%%%===================================================================

-spec(get_stats() -> stats()).
%% @doc
%% Get all the stats from metadata (if enabled) or exometer, similar to
%% "riak-admin stat show riak.**" but from inside the shell
%% @end
get_stats() ->
    get_stat(['_']).

get_stats(App) ->
%%  io:format("PFX :~p~n", [?PFX]),
    get_stat([?PFX, App]).

%%% ------------------------------------------------------------------

-spec(get_stat(stats()) -> stats()).
%% @doc
%% Give a path to a particular stat such as : [riak,riak_kv,node,gets,time]
%% to retrieve the stat's as [{Name,Type,Status}]
%% @end
get_stat(Path) ->
    print(find_entries(Path, '_')).

-spec(get_value(data()) -> value()).
%% @doc
%% Get the stat(s) value from exometer, only returns enabled values
%% @end
get_value(Arg) ->
    Stats = get_values(Arg),
    print(Stats).

%%% ------------------------------------------------------------------

-spec(get_info(app()) -> stats()).
%% @doc
%% get the info for the apps stats from exometer
%% @end
get_info(App) ->
    print([find_stat_info(Stat) || {Stat, _Type, _Status} <- get_stats(App)]).


%%% ------------------------------------------------------------------

-spec(aggregate(pattern(), datapoint()) -> stats()).
%% @doc
%% @see exometer:aggregate
%% Does the average of stats averages
%% @end
aggregate(Stats, DPs) ->
    Pattern = {{Stats, '_', '_'}, [], [Stats]},
    AggStats = aggregate_(Pattern, DPs),
    print(AggStats).

%%% ------------------------------------------------------------------

-spec(register(app(), statname()) -> ok | error()).
%% @doc
%% register apps stats into both meta and exometer adding the prefix
%% @end
register(App, Stats) ->
    P = prefix(),
        lists:foreach(fun(Stat) ->
            register_stat(P, App, Stat)
                      end, Stats).


-spec(update(statname(), incrvalue(), type()) -> ok | error()).
%% @doc
%% update a stat in exometer, if the stat doesn't exist it will
%% re_register it. When the stat is deleted in exometer the status is
%% changed to unregistered in metadata, it will check the metadata
%% first, if unregistered then ok is returned by default and no stat
%% is created.
%% @end
update(Name, Inc, Type) ->
    case update_(Name, Inc, Type) of
        {error, Reason} ->
            lager:error("Metric not found : ~p~n", [Reason]);
        _ -> ok
    end.


-spec(unregister(app(), Mod :: data(), Idx :: data(), type()) ->
  ok | error()).
%% @doc
%% unregister a stat from the metadata leaves it's status as
%% {status, unregistered}, and deletes the metric from exometer
%% @end
unregister({Mod, Idx, Type, App}) ->
    unregister(Mod, Idx, Type, App);
unregister(StatName) ->
    unreg_stats(StatName).

unregister(App, Mod, Idx, Type) ->
    P = prefix(),
    unreg_stats(P, App, Type, Mod, Idx).


%%%===================================================================
%%% Other API
%%%==================================================================

-spec(prefix() -> value()).
%% @doc
%% standard prefix for all stats inside riak, all modules call
%% into this function for the prefix.
%% @end
prefix() ->
    app_helper:get_env(riak_core, stat_prefix, riak).

%%%-------------------------------------------------------------------

-spec(print(data(), attr()) -> value()).
print(Arg) ->
    print(Arg, []).
print(Entries, Attr) when is_list(Entries) ->
    lists:map(fun(E) -> print(E, Attr) end, Entries);
print(Entries, Attr) ->
    riak_core_stat_data:print(Entries, Attr).

%%%===================================================================
%%% Coordinator API
%%%===================================================================

find_entries(Stats, Status) ->
    riak_core_stat_coordinator:find_entries(Stats, Status).

find_stat_info(Stat) ->
    Info = [name, type, module, value, cache, status, timestamp, options],
    riak_core_stat_coordinator:find_stats_info(Stat, Info).

get_values(Arg) ->
    riak_core_stat_coordinator:get_values(Arg).

aggregate_(Stats, DPs) ->
    riak_core_stat_coordinator:aggregate(Stats, DPs).

update_(Name, Inc, Type) ->
    riak_core_stat_coordinator:update(Name, Inc, Type).

unreg_stats_(StatName) ->
  riak_core_stat_coordinator:unregister(StatName).

reg_stats_(StatInfo) ->
  riak_core_stat_coordinator:register(StatInfo).

%%%===================================================================
%%% Helper functions
%%%===================================================================

register_stat(P, App, Stat) ->
    {Name, Type, Opts, Aliases} =
        case Stat of
            {N, T} -> {N, T, [], []};
            {N, T, Os} -> {N, T, Os, []};
            {N, T, Os, As} -> {N, T, Os, As}
        end,
    StatName = stat_name(P, App, Name),
    NewOpts = add_cache(Opts),
    reg_stats_({StatName, Type, NewOpts, Aliases}).

add_cache(Opts) ->
    case lists:keyfind(cache, 1, Opts) of
        false ->
            lists:keystore(cache, 1, Opts, ?CACHE);
        _ ->
            lists:keyreplace(cache, 1, Opts, ?CACHE)
    end.

% Put the prefix and App name in front of every stat name.
stat_name(P, App, N) when is_atom(N) ->
    stat_name_([P, App, N]);
stat_name(P, App, N) when is_list(N) ->
    stat_name_([P, App | N]).

stat_name_([P, [] | Rest]) -> [P | Rest];
stat_name_(N) -> N.

unreg_stats(StatName) ->
    unreg_stats_(StatName).
unreg_stats(P, App, Type, [Op, time], Index) ->
    unreg_stats_([P, App, Type, Op, time, Index]);
unreg_stats(P, App, Type, Mod, Index) ->
    unreg_stats_([P, App, Type, Mod, Index]).

