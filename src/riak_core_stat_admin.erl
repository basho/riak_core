%%%-------------------------------------------------------------------
%%% @doc
%%% Despite the name of this module, the "riak-admin ..." commands from
%%% the console do not follow through to this module, they instead are
%%% directed to the @see riak_core_stat_console module.
%%%
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
%% "riak-admin stat show riak.**" but from inside the shell or riak attach
%% @end
get_stats() ->
    get_stat([prefix() | '_']).

get_stats(App) ->
    get_stat([prefix(), App | '_']).

%%% ------------------------------------------------------------------

-spec(get_stat(list()) -> stats()).
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
    print(get_values(Arg)).

%%% ------------------------------------------------------------------

-spec(get_info(app()) -> stats()).
%% @doc
%% get the info for the apps stats from exometer
%% @end
get_info(App) ->
    print([{Stat, find_stat_info(Stat)} || {Stat, _Status} <- get_stats(App)]).

%%% ------------------------------------------------------------------

-spec(aggregate(pattern(), datapoint()) -> stats()).
%% @doc
%% @see exometer:aggregate
%% Does the average of stats averages
%% @end
aggregate(Stats, DPs) ->
    Pattern = [{{Stats, '_', '_'}, [], ['$_']}],
    print(aggregate_(Pattern, DPs)).

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

%%% ------------------------------------------------------------------

-spec(update(statname(), incrvalue(), type()) -> ok | error()).
%% @doc
%% update a stat in exometer, if the stat doesn't exist it will
%% re_register it. When the stat is deleted in exometer the status is
%% changed to unregistered in metadata, it will check the metadata
%% first, if unregistered then ok is returned by default and no stat
%% is created.
%% @end
update(Name, Inc, Type) ->
    ok = update_(Name, Inc, Type).

%%% ------------------------------------------------------------------

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
    MS = [{{Stats, '_', Status}, [], ['$_']}],
    riak_core_stat_coordinator:find_entries(MS, Stats, []).

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