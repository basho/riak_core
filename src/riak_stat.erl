%%%-------------------------------------------------------------------
%%% @doc
%%% This module is for the administration of the stats within
%%% riak_core and other stats, in other riak apps. Specifically:
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
%%% These _stat modules call into this module to REGISTER, READ,
%%% UPDATE, DELETE or RESET the stats.
%%%
%%% For registration the stats call into this module to become a
%%% uniform format in a list to be "re-registered" in the metadata
%%% and in exometer. Unless the metadata is disabled (it is enabled
%%% by default), the stats are sent to both to be registered. The
%%% purpose of the metadata is to allow persistence of the stats
%%% registration and configuration - whether the stat is
%%% enabled/disabled/unregistered;
%%% Exometer does not persist the stats values or information.
%%%
%%% For updating the stats, each of the stats module will call into
%%% this module to update the stat in exometer by IncrVal given, all
%%% update function calls call into the update_or_create/3 function
%%% in exometer. Meaning if the stat has been deleted but is still
%%% hitting a function that updates it, it will create a basic
%%% (unspecific) metric for that stat so the update values are stored
%%% somewhere. This means if the stat is to be unregistered/deleted,
%%% its existence should be removed from the stats code, otherwise it
%%% is to be disabled.
%%%
%%% Resetting the stats change the values back to 0, and it's reset
%%% counter in the metadata and in the exometer are updated +1
%%% (enabled only)
%%%
%%% As of AUGUST 2019 -> the aggregate function is a test function and
%%% works in a very basic way, i.e. it will produce the sum of any
%%% counter datapoints for any stats given, or the average of any
%%% "average" datapoints such as mean/median/one/value etc...
%%% This only works on a single nodes stats list, there will be
%%% updates in future to aggregate the stats for all the nodes.
%%% @end
%%%-------------------------------------------------------------------

-module(riak_stat).
-include_lib("riak_core/include/riak_stat.hrl").

-export([
    register/2,
    register_stats/1,

    find_entries/2,
    stats/0,
    app_stats/1,
    get_stats/1,
    get_value/1,
    get_info/1,
    aggregate/2,
    sample/1,

    update/3,

    unregister/1,
    unregister_vnode/4,
    reset/1,

    prefix/0
]).

-define(STATCACHE, app_helper:get_env(riak_core,exometer_cache,
                                                    {cache,5000})).
                  %% default can be changed in config this way.

%%%===================================================================
%%% Registration API
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%% register apps stats in both metadata and exometer adding the stat
%% prefix
%% @end
%%%-------------------------------------------------------------------
-spec(register(app(), statslist()) -> ok | error()).
register(App, Stats) ->
    lists:foreach(fun
                      ({N,T})     -> register_(App,N,T,[],[]);
                      ({N,T,O})   -> register_(App,N,T,O ,[]);
                      ({N,T,O,A}) -> register_(App,N,T,O , A)
                  end, Stats).

%%          NB : {N,T,O,A} == {Name,Type,Options,Aliases}

register_(App, N,Type,O,Aliases) ->
    StatName = stat_name(prefix(),App,N),
    Opts     = add_cache(O),
    register_stats({StatName,Type,Opts,Aliases}).

%% All stats of App are appended onto the prefix
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
        _ -> Options %% Otherwise return the Options as is
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
%% Give a path to a stat such as : [riak,riak_kv,node,gets,time]
%% to retrieve the stat' as [{Name,Type,Status}]
%% @end
%%%-------------------------------------------------------------------
-spec(get_stats(statslist()) -> arg()).
get_stats(Path) ->
    find_entries(Path, '_').

find_entries(Stats, Status) ->
    find_entries(Stats, Status, '_', []).

find_entries(Stats, Status, Type, DPs) ->
    riak_stat_mgr:find_entries(Stats, Status, Type, DPs).

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
%% get the info of the stat/app-stats from exometer
%% (enabled metrics only)
%% @end
%%%-------------------------------------------------------------------
-spec(get_info(app() | statslist()) -> stats()).
get_info(Arg) when is_atom(Arg) ->
    get_info([prefix(),Arg |'_']); %% assumed arg is the app (atom)
get_info(Arg) ->
    print([{Stat, stat_info(Stat)} ||
        {Stat, _Status} <- get_stats(Arg)]).

stat_info(Stat) ->
    riak_stat_exom:get_info(Stat, ?INFOSTAT).

%%%-------------------------------------------------------------------
%% @doc
%% @see exometer:aggregate/2
%% Does the average of stats averages
%% @end
%%%-------------------------------------------------------------------
-spec(aggregate(metricname(), datapoint()) -> stats()).
aggregate(Stats, DPs) ->
    Pattern = [{{Stats, '_', '_'}, [], ['$_']}], %% Match_spec
    print(aggregate_(Pattern, DPs)).

aggregate_(Pattern, DPs) ->
    riak_stat_mgr:aggregate(Pattern, DPs).

%%%-------------------------------------------------------------------
%% @doc
%% @see exometer:sample/1
%% @end
%%%-------------------------------------------------------------------
-spec(sample(metricname()) -> ok | error() | value()).
sample(StatName) ->
    riak_stat_exom:sample(StatName).

%%%===================================================================
%%% Updating Stats API
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%% update a stat in exometer, if the stat doesn't exist it will
%% re_register it. When the stat is deleted in exometer the status is
%% changed to unregistered in metadata, it will check the metadata
%% first, if unregistered then 'ok' is returned by default and no stat
%% is created.
%% @end
%%%-------------------------------------------------------------------
-spec(update(statname(), incrvalue(), type()) -> ok | error()).
update(Name, Inc, Type) ->
    ok = riak_stat_exom:update(Name, Inc, Type).

%%%===================================================================
%%% Deleting/Resetting Stats API
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%% unregister a stat from the metadata leaves it's status as
%% {status, unregistered}, and deletes the metric from exometer
%% @end
%%%-------------------------------------------------------------------
-spec(unregister({app(),Idx :: term(),type(),app()} | metricname()) ->
    ok | error()).
unregister({Mod, Idx, Type, App}) -> %% specific to riak_core vnode
    unregister_vnode(Mod, Idx, Type, App);
unregister(StatName) -> %% generic
    unregister_stat(StatName).

unregister_vnode(App, Mod, Idx, Type) ->
    P = prefix(),
    unregister_stat(P, App, Type, Mod, Idx).

unregister_stat(StatName) ->
    unreg_stats(StatName).
unregister_stat(P, App, Type, [Op, time], Index) ->
    unreg_stats([P, App, Type, Op, time, Index]);
unregister_stat(P, App, Type, Mod, Index) ->
    unreg_stats([P, App, Type, Mod, Index]).

unreg_stats(StatName) ->
    riak_stat_mgr:unregister(StatName).

%%%-------------------------------------------------------------------
%% @doc
%% reset the stat in exometer and in metadata (enabled metrics only)
%% @end
%%%-------------------------------------------------------------------
-spec(reset(metricname()) -> ok | error()).
reset(StatName) ->
    riak_stat_mgr:reset_stat(StatName).

%%%===================================================================
%%% Additional API
%%%===================================================================
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

-spec(print(arg()) -> ok).
-spec(print(data(), attr()) -> ok).
print(Arg) ->
    print(Arg, []).
print(Entries, Attr) when is_list(Entries) ->
    lists:map(fun(E) -> print(E, Attr) end, Entries);
print(Entries, Attr) ->
    riak_stat_console:print(Entries, Attr).

%%%===================================================================
%%% EUNIT Testing
%%%===================================================================

%%-ifdef(TEST).
%%-include_lib("eunit/include/eunit.hrl").
%%%% todo: testing
%%-endif.
