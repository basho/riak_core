%%%-------------------------------------------------------------------
%%% @doc
%%% Functions from _stat modules or from to register, update, read
%%% or unregister a stat.
%%% @end
%%%-------------------------------------------------------------------
-module(riak_core_stat_admin).

-include_lib("riak_core/include/riak_core_stat.hrl").

%% API
-export([
    get_stats/0,
    get_stat/1,
    get_stat_value/1,
    get_app_stats/1,
    get_stats_values/1,
    get_stats_info/1,
    aggregate/2,
    register/2,
    update/3,
    unregister/1,
    unregister/4
]).

%% Other API
-export([
    data_sanitise/1,
    print/1,
    print/2
]).


%%%===================================================================
%%% API
%%%===================================================================

-spec(get_stats() -> stats()).
%% @doc
%% get all the stats from metadata (if enabled) or exometer, like
%% "riak-admin stat show riak.**"
%% @end
get_stats() ->
    [{_N, MatchSpec, _DPs}] = data_sanitise([<<"riak.**">>], '_', '_'),
    Stats = select_entries(MatchSpec),
    print(Stats).

%%% ------------------------------------------------------------------

-spec(get_stat(stats()) -> stats()).
%% @doc
%% give A Path to a particular stat such as : [riak,riak_kv,node,gets,time]
%% to retrieve the stat
%% @end
get_stat(Path) ->
    print(find_entries([Path], '_')).

-spec(get_stat_value(data()) -> value()).
%% @doc
%% Get the stat(s) value from exometer, only returns enabled values
%% @end
get_stat_value(Arg) ->
    [{Names, _MatchSpec, _DP}] = data_sanitise(Arg),
    Entries = find_entries(Names, '_'),
    print([find_stat_value(Entry) || {Entry, _} <- Entries]).

%%% ------------------------------------------------------------------

-spec(get_app_stats(app()) -> stats()).
%% @doc
%% similar to get_stats but specific to the app provided in the module
%% "riak-admin stat show riak.<app>.**"
%% @end
get_app_stats(App) ->
    [{_N, MatchSpec, _DPs}] = data_sanitise([prefix(), App], '_', '_'),
    print(select_entries(MatchSpec)).

-spec(get_stats_values(app()) -> stats()).
%% @doc
%% Get the stats for the app and all their values
%% @end
get_stats_values(App) ->
    AppStats = get_app_stats(App),
    print([find_stat_value(Stat) || {Stat, _Status} <- AppStats]).

-spec(get_stats_info(app()) -> stats()).
%% @doc
%% get the info for the apps stats from exometer
%% @end
get_stats_info(App) ->
    AppStats = get_app_stats(App),
    print([find_stat_info(Stat) || {Stat, _Status} <- AppStats]).

%%% ------------------------------------------------------------------

-spec(aggregate(pattern(), datapoint()) -> stats()).
%% @doc
%% @see exometer:aggregate
%% Does the average of stats averages
%% @end
aggregate(Stats, DPs) ->
    Pattern = {{Stats, '_', '_'}, [], [Stats]},
    AggStats = riak_core_stat_coordinator:aggregate(Pattern, DPs),
    print(AggStats).

%%% ------------------------------------------------------------------

-spec(register(app(), statname()) -> ok | error()).
%% @doc
%% register apps stats into both meta and exometer
%% @end
register(App, Stats) ->
    P = prefix(),
    io:format("riak_stat_admin:register(~p)~n", [App]),
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
    riak_core_stat_coordinator:update(Name, Inc, Type).


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


%% @doc
%% sanitise the data to a form useful for finding the stats in metadata
%% and in exometer
%% @end
data_sanitise(Arg) ->
    riak_core_stat_data:data_sanitise(Arg).
data_sanitise(Data, Type, Status) ->
    riak_core_stat_data:data_sanitise(Data, Type, Status).


-spec(print(data(), attr()) -> value()).
print(Arg) ->
    print(Arg, []).
print(Entries, Attr) when is_list(Entries) ->
    lists:map(fun(E) -> print(E, Attr) end, Entries);
print(Entries, Attr) ->
    riak_stat_data:print(Entries, Attr).

%%%===================================================================
%%% Coordinator API
%%%===================================================================

find_entries(Stats, Status) ->
    riak_core_stat_coordinator:find_entries(Stats, Status).

select_entries(MatchSpec) ->
    riak_core_stat_coordinator:select(MatchSpec).

find_stat_value(Path) ->
    riak_core_stat_coordinator:find_stats_info(Path, [value]).

find_stat_info(Stat) ->
    Info = [name, type, module, value, cache, status, timestamp, options],
    riak_core_stat_coordinator:find_stats_info(Stat, Info).


%%%===================================================================
%%% Internal functions
%%%===================================================================

register_stat(P, App, Stat) ->
    io:format("riak_stat_admin:register_stat(~p)~n", [Stat]),

    {Name, Type, Opts, Aliases} =
        case Stat of
            {N, T} -> {N, T, [], []};
            {N, T, Os} -> {N, T, Os, []};
            {N, T, Os, As} -> {N, T, Os, As}
        end,
    StatName = stat_name(P, App, Name),
    io:format("riak_stat_admin:stat_name(~p)~n", [StatName]),
    NewOpts = add_cache(Opts),
    io:format("riak_stat_admin:add_cache(~p)~n", [NewOpts]),
  % Pull out the status of the stat from MetaData
    riak_core_stat_coordinator:register({StatName, Type, NewOpts, Aliases}).

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

unreg_stats_(StatName) ->
    riak_core_stat_coordinator:unregister(StatName).
