%%%-------------------------------------------------------------------
%%% @doc
%%% This module is for the administration of the stats within
%%% riak_core and other stats, in other riak apps.
%%%
%%% These _stat modules call into this module to REGISTER, READ,
%%% UPDATE, DELETE or RESET the stats.
%%%
%%%
%%% As of 2019 -> the aggregate function is a test function and
%%% works in a very basic way, i.e. it will produce the sum of any
%%% counter datapoints for any stats given, or the average of any
%%% "average" datapoints such as mean/median/one/value etc...
%%% This only works on a single nodes stats list, there will be
%%% updates in future to aggregate the stats for all the nodes.
%%% @end
%%%-------------------------------------------------------------------

-module(riak_stat).
-include_lib("riak_core/include/riak_stat.hrl").

%% Internally Used
-export([
    register/2,
    register_stats/1,
    get_stats/1,
    sample/1,
    update/3,
    unregister/1,
    unregister_vnode/4,
    reset/1,
    prefix/0
]).

%% Externally Used
-export([
    find_entries/2,
    stats/0,
    app_stats/1,
    get_value/1,
    get_info/1,
    aggregate/2
]).

-define(STATCACHE, app_helper:get_env(riak_core,exometer_cache,
                                                    {cache,5000})).

%%%===================================================================
%%% Registration API
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%%% For registration the stats call into this module to become a
%%% uniform format in a list to be "re-registered" in the metadata
%%% and in exometer. Unless the metadata is disabled (it is enabled
%%% by default), the stats are sent to both to be registered. The
%%% purpose of the metadata is to allow persistence of the stats
%%% registration and configuration - whether the stat is
%%% enabled/disabled/unregistered;
%%% Exometer does not persist the stats values or information.
%%% Metadata does not persist the stats values.
%% @end
%%%-------------------------------------------------------------------
-spec(register(app(), listofstats()) -> ok | error()).
register(App, Stats) ->
    lists:foreach(fun
      ({Name,Type})                -> register_(App,Name,Type,[],[]);
      ({Name,Type,Option})         -> register_(App,Name,Type,Option,[]);
      ({Name,Type,Option,Aliases}) -> register_(App,Name,Type,Option,Aliases)
                  end, Stats).


register_(App, Name, Type, Options, Aliases) ->
    StatName   = stat_name(prefix(),App,Name),
    NewOptions = add_cache(Options),
    register_stats({StatName, Type, NewOptions, Aliases}).

%% All stats in App are appended onto the prefix
stat_name(Prefix, App, Name) when is_atom(Name) ->
    stat_name_([Prefix, App, Name]);
stat_name(Prefix, App, Name) when is_list(Name) ->
    stat_name_([Prefix, App | Name]);
stat_name(_,_,Name) ->
    lager:error("Stat Name illegal ~p ",[Name]).

stat_name_([P, [] | Rest]) -> [P | Rest];
stat_name_(N) -> N.

add_cache(Options) ->
    %% look for cache value in stat, if undefined - add cache option
    case proplists:get_value(cache, Options) of
        undefined -> [?STATCACHE | Options];
        _ -> Options %% Otherwise return the Options as is
    end.

-spec(register_stats(StatInfo :: tuple_stat()) -> ok | error()).
register_stats(StatInfo) -> riak_stat_mgr:register(StatInfo).


%%%===================================================================
%%% Reading Stats API - Internal
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%% Give a path to a stat such as : [riak,riak_kv,node,gets,time]
%% to retrieve the stats' as [{Name,Type,Status}]
%% @end
%%%-------------------------------------------------------------------
-spec(get_stats(metricname()) -> nts_stats()).
get_stats(Path) ->
    find_entries(Path, '_').

find_entries(Stats, Status) ->
    find_entries(Stats, Status, '_', []).

find_entries(Stats, Status, Type, DPs) ->
    riak_stat_mgr:find_entries(Stats, Status, Type, DPs).

%%%-------------------------------------------------------------------
%% @doc
%% @see exometer:sample/1
%% @end
%%%-------------------------------------------------------------------
-spec(sample(metricname()) -> ok | error() | exo_value()).
sample(StatName) ->
    riak_stat_exom:sample(StatName).


%%%===================================================================
%%% Reading Stats API - External
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc (Shell function - not used internally).
%% Print all the stats for riak stored in exometer/metadata
%% Usage : riak_stat:stats().
%% @end
%%%-------------------------------------------------------------------
-spec(stats() -> print()).
stats() ->
    print(get_stats([[prefix()|'_']])).

%%%-------------------------------------------------------------------
%% @doc (Shell function - not used internally).
%% Print all the stats for an app stored in exometer/metadata
%% Usage: riak_stat:app_stats(riak_api).
%% @end
%%%-------------------------------------------------------------------
-spec(app_stats(app()) -> print()).
app_stats(App) ->
    print(get_stats([[prefix(),App|'_']])).

%%%-------------------------------------------------------------------
%% @doc (Shell function - not used internally)
%% get the value of the stat from exometer (returns enabled metrics)
%% @end
%%%-------------------------------------------------------------------
-spec(get_value(listofstats()) -> print()).
get_value(Arg) ->
    print(riak_stat_exom:get_values(Arg)).

%%%-------------------------------------------------------------------
%% @doc (Shell function - not used internally)
%% get the info of the stat/app-stats from exometer
%% (enabled metrics only)
%% @end
%%%-------------------------------------------------------------------
-spec(get_info(app() | metrics()) -> print()).
get_info(Arg) when is_atom(Arg) ->
    get_info([prefix(),Arg |'_']); %% assumed arg is the app (atom)
get_info(Arg) ->
    {Stats, _DPs} = get_stats([Arg]),
    print([{Stat, stat_info(Stat)} || {Stat,_T,_S} <- Stats]).

stat_info(Stat) ->
    riak_stat_exom:get_info(Stat, ?INFOSTAT).

%%%-------------------------------------------------------------------
%% @doc (Shell function - not used internally)
%% @see exometer:aggregate/2
%% Does the average of stats averages, called manually, not used
%% internally or automatically.
%% @end
%%%-------------------------------------------------------------------
-spec(aggregate(metricname(), datapoints()) -> print()).
aggregate(Stats, DPs) ->
    Pattern = [{{Stats, '_', '_'}, [], ['$_']}], %% Match_spec
    print(aggregate_(Pattern, DPs)).

aggregate_(Pattern, DPs) ->
    riak_stat_mgr:aggregate(Pattern, DPs).


%%%===================================================================
%%% Updating Stats API
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%% For updating the stats, each of the stats module will call into
%% this module to update the stat in exometer by IncrVal given, all
%% update function calls call into the update_or_create/3 function
%% in exometer. Meaning if the stat has been deleted but is still
%% hitting a function that updates it, it will create a basic
%% (unspecific) metric for that stat so the update values are stored
%% somewhere. This means if the stat is to be unregistered/deleted,
%% its existence should be removed from the stats code, otherwise it
%% is to be disabled.
%% @end
%%%-------------------------------------------------------------------
-spec(update(metricname(), incrvalue(), type()) -> ok | error()).
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
    unregister_vnode(App, Mod, Idx, Type);
unregister(StatName) -> %% generic
    unregister_stat(StatName).

unregister_vnode(App, Mod, Idx, Type) ->
    Prefix = prefix(),
    unregister_stat(Prefix, App, Type, Mod, Idx).

unregister_stat(StatName) ->
    unreg_stats(StatName).
unregister_stat(Prefix, App, Type, [Op, time], Index) ->
    unreg_stats([Prefix, App, Type, Op, time, Index]);
unregister_stat(Prefix, App, Type, Mod, Index) ->
    unreg_stats([Prefix, App, Type, Mod, Index]).

unreg_stats(StatName) ->
    riak_stat_mgr:unregister(StatName).

%%%-------------------------------------------------------------------
%% @doc
%% Resetting the stats change the values back to 0, and it's reset
%% counter in the metadata and in the exometer are updated +1
%% (enabled only)
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
-spec(prefix() -> prefix()).
prefix() -> app_helper:get_env(riak_core, stat_prefix, riak).

%%%-------------------------------------------------------------------

-spec(print(Entries ::
           (nts_stats() %% [{Name,Type,Status}]
          | n_v_stats() %% [{Name,Value/Values}]
          | n_i_stats() %% [{Name, Information}]
          | stat_value())) -> ok).
print(Entries) -> riak_stat_console:print(Entries).
