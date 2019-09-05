%%%-------------------------------------------------------------------
%%% @doc
%%% This module is for the administration of the stats within riak_core
%%% and other stats, in other riak apps. Specifically:
%%% @see :
%%%
%%% riak_api_stat
%%% riak_core_stat
%%% riak_kv_stat
%%% riak_pipe_stat
%%% riak_repl_stats
%%% yz_stat
%%% riak_core_connection_mgr_stats (riak_repl)
%%% riak_core_exo_monitor (riak_core)
%%%
%%% These _stat modules call into this module to REGISTER, READ, UPDATE,
%%% DELETE or RESET the stats.
%%%
%%% For registration the stats call into this module to become a uniform
%%% format in a list to be "re-registered" in the metadata and in exometer
%%% Unless the metadata is disabled (it is enabled by default), the stats
%%% only get sent to exometer to be registered. The purpose of the
%%% metadata is to allow persistence of the stats registration and
%%% configuration - meaning whether the stat is enabled/disabled/unregistered,
%%% Exometer does not persist the stats values or information
%%%
%%% For reading the stats to the shell (riak attach) there are functions in
%%% each module to return all/some/specific stats and their statuses or info
%%% that is stored in either the metadata or exometer, these functions work
%%% the same way as the "riak admin stat show ..." in the command line.
%%%
%%% For updating the stats, each of the stats module will call into this module
%%% to update the stat in exometer by IncrVal given, all update function calls
%%% call into the update_or_create/3 function in exometer. Meaning if the
%%% stat is unregistered but is still hitting a function that updates it, it will
%%% create a basic (unspecific) metric for that stat so the update values are
%%% stored somewhere. This means if the stat is to be unregistered/deleted,
%%% its existence should be removed from the stats code, otherwise it is to
%%% be disabled.
%%%
%%% Resetting the stats change the values back to 0, it's reset counter in the
%%% metadata and in the exometer are updated +1
%%%
%%% As of AUGUST 2019 -> the aggregate function is a test function and
%%% works in a very basic way, i.e. it will produce the sum of any counter
%%% datapoints for any stats given, or the average of any "average" datapoints
%%% such as mean/median/one/value etc... This only works on a single nodes
%%% stats list, there will be updates in future to aggregate the stats
%%% for all the nodes.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat).
-include_lib("riak_core/include/riak_stat.hrl").

%% Registration API
-export([
    register/2]).

%% Read Stats API
-export([
    find_entries/2,
    stats/0,
    app_stats/1,
    get_stats/1,
    get_value/1,
    get_info/1,
    aggregate/2]).

%% Update API
-export([update/3]).

%% Delete/Reset API
-export([
    unregister/1,
    unregister/4]).

%% Additional API
-export([prefix/0]).

-define(STATCACHE,          app_helper:get_env(riak_core,exometer_cache,{cache,5000})).
-define(INFOSTAT,           [name,type,module,value,cache,status,timestamp,options]).

%%%===================================================================
%%% Registration API
%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% register an apps stats in both metadata and exometer adding the stat
%% prefix
%% @end
%%%-------------------------------------------------------------------
-spec(register(app(), statslist()) -> ok | error()).
register(App, Stats) ->
    Prefix = prefix(),
    lists:foreach(fun(Stat) ->
        register_(Prefix, App, Stat)
                  end, Stats).

register_(Prefix, App, Stat) ->
    {Name, Type, Options, Aliases} =
        case Stat of
            {N, T}       -> {N, T,[],[]};
            {N, T, O}    -> {N, T, O,[]};
            {N, T, O, A} -> {N, T, O, A}
        end,
    StatName = stat_name(Prefix, App, Name),
    OptsWithCache = add_cache(Options),
    register_stats({StatName, Type, OptsWithCache, Aliases}).

stat_name(P, App, N) when is_atom(N) ->
    stat_name_([P, App, N]);
stat_name(P, App, N) when is_list(N) ->
    stat_name_([P, App | N]).

stat_name_([P, [] | Rest]) -> [P | Rest];
stat_name_(N) -> N.

add_cache(Options) ->
    %% look for cache value in stat, if undefined - add cache option
    case proplists:get_value(cache, Options) of
        undefined -> [?STATCACHE | Options];
        _ -> Options
    end.

register_stats(StatInfo) -> riak_stat_mgr:register(StatInfo).

%%%===================================================================
%%% Reading Stats API
%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% get all the stats for riak stored in exometer/metadata
%% @end
%%%-------------------------------------------------------------------
-spec(stats() -> statslist()).
stats() ->
    print(get_stats([prefix()|'_'])).

%%%-------------------------------------------------------------------
%% @doc
%% get all the stats for an app stored in exometer/metadata
%% @end
%%%-------------------------------------------------------------------
-spec(app_stats(app()) -> statslist()).
app_stats(App) ->
    print(get_stats([prefix(),App|'_'])).

%%%-------------------------------------------------------------------
%% @doc
%% Give a path to a particular stat such as : [riak,riak_kv,node,gets,time]
%% to retrieve the stat's as [{Name,Type,Status}]
%% @end
%%%-------------------------------------------------------------------
-spec(get_stats(statslist()) -> arg()).
get_stats(Path) ->
    find_entries(Path, '_').

find_entries(Stats, Status) ->
    MS = [{{Stats, '_', Status}, [], ['$_']}],
    riak_stat_mgr:find_entries(MS,Stats,Status).

%%%-------------------------------------------------------------------
%% @doc
%% get the value of the stat from exometer (enabled metrics only)
%% @end
%%%-------------------------------------------------------------------
-spec(get_value(statslist()) -> arg()).
get_value(Arg) ->
    print(riak_stat_exom:get_values(Arg)).

%%%-------------------------------------------------------------------
%% @doc
%% get the info of the stat/app-stats from exometer (enabled metrics only)
%% @end
%%%-------------------------------------------------------------------
-spec(get_info(app() | statslist()) -> stats()).
get_info(Arg) when is_atom(Arg) ->
    get_info([prefix(),Arg |'_']); %% assumed arg is the app
get_info(Arg) ->
    print([{Stat, stat_info(Stat)} || {Stat, _Status} <- get_stats(Arg)]).

stat_info(Stat) ->
    riak_stat_exom:get_info(Stat, ?INFOSTAT).

%%%-------------------------------------------------------------------
%% @doc
%% @see exometer:aggregate
%% Does the average of stats averages
%% @end
%%%-------------------------------------------------------------------
-spec(aggregate(pattern(), datapoint()) -> stats()).
aggregate(Stats, DPs) ->
    Pattern = [{{Stats, '_', '_'}, [], ['$_']}],
    print(aggregate_(Pattern, DPs)).

aggregate_(Pattern, DPs) ->
    riak_stat_mgr:aggregate(Pattern, DPs).

%%%===================================================================
%%% Updating Stats API
%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% update a stat in exometer, if the stat doesn't exist it will
%% re_register it. When the stat is deleted in exometer the status is
%% changed to unregistered in metadata, it will check the metadata
%% first, if unregistered then ok is returned by default and no stat
%% is created.
%% @end
%%%-------------------------------------------------------------------
-spec(update(statname(), incrvalue(), type()) -> ok | error()).
update(Name, Inc, Type) ->
    ok = riak_stat_exom:update(Name, Inc, Type).


%%%===================================================================
%%% Deleting/Resetting Stats API
%%%===================================================================

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

unreg_stats(StatName) ->
    unreg_stats_(StatName).
unreg_stats(P, App, Type, [Op, time], Index) ->
    unreg_stats_([P, App, Type, Op, time, Index]);
unreg_stats(P, App, Type, Mod, Index) ->
    unreg_stats_([P, App, Type, Mod, Index]).

unreg_stats_(StatName) ->
    riak_stat_mgr:unregister(StatName).

%%%===================================================================
%%% Additional API
%%%==================================================================

%%%-------------------------------------------------------------------
%% @doc
%% standard prefix for all stats inside riak, all modules call
%% into this function for the prefix.
%% @end
%%%-------------------------------------------------------------------
-spec(prefix() -> value()).
prefix() ->
    app_helper:get_env(riak_core, stat_prefix, riak).

%%%-------------------------------------------------------------------

-spec(print(data(), attr()) -> value()).
print(Arg) ->
    print(Arg, []).
print(Entries, Attr) when is_list(Entries) ->
    lists:map(fun(E) -> print(E, Attr) end, Entries);
print(Entries, Attr) ->
    riak_stat_data:print(Entries, Attr).

