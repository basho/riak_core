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
    register/2,
    register_stats/1]).

%% Read Stats API
-export([
    find_entries/2,
    stats/0,
    app_stats/1,
    get_stats/1,
    get_value/1,
    get_info/1,
    aggregate/2,
    sample/1]).

%% Update API
-export([update/3]).

%% Delete/Reset API
-export([
    unregister/1,
    unregister/4,
    reset/1]).


%% Additional API
-export([prefix/0]).


%%-ifdef(TEST).
%%-export([register_stats/1]).
%%-endif.


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

%%%-------------------------------------------------------------------
%% @doc
%% unregister a stat from the metadata leaves it's status as
%% {status, unregistered}, and deletes the metric from exometer
%% @end
%%%-------------------------------------------------------------------
-spec(unregister(app(), Mod :: data(), Idx :: data(), type()) ->
    ok | error()).
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

%%%-------------------------------------------------------------------
%% @doc
%% reset the stat in exometer and in metadata
%% @end
%%%-------------------------------------------------------------------
-spec(reset(metricname()) -> ok | error()).
reset(StatName) ->
    riak_stat_mgr:reset_stat(StatName).


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

%%%===================================================================
%%% EUNIT Testing
%%%==================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(new(Mod),                   meck:new(Mod)).
-define(expect(Mod,Fun,Func),       meck:expect(Mod,Fun,Func)).
-define(expect(Mod,Fun,Val,Func),   meck:expect(Mod,Fun,Val,Func)).
-define(unload(Mod),                meck:unload(Mod)).

-define(setup(Fun),        {setup,    fun setup/0,          fun cleanup/1, Fun}).
-define(foreach(Funs),     {foreach,  fun setup/0,          fun cleanup/1, Funs}).

-define(spawn(Test),       {spawn,        Test}).
-define(timeout(Test),     {timeout, 120, Test}).
-define(inorder(Test),     {inorder,      Test}).
-define(inparallel(Test),  {inparallel,   Test}).

-define(setuptest(Desc, Test), {Desc, ?setup(fun(_) -> Test end)}).

setup() ->
    catch(?unload(riak_stat)),
    catch(?unload(riak_stat_data)),
    catch(?unload(riak_stat_mgr)),
    catch(?unload(riak_stat_exom)),
    catch(?unload(riak_stat_meta)),
    catch(?unload(riak_stat_console)),
    catch(?unload(riak_stat_profiles)),
    ?new(riak_stat),
    ?new(riak_stat_data),
    ?new(riak_stat_mgr),
    ?new(riak_stat_exom),
    ?new(riak_stat_meta),
    ?new(riak_stat_console),
    ?new(riak_stat_profiles),
%%    ?expect(riak_stat,register_stats,ok,fun() -> ok end),
%%    ?expect(riak_stat,register_stats,fun() -> ok end),
%%    {ok, Pid} = riak_stat_profiles:start_link(),
%%    [Pid].
    ok.

cleanup(Pids) ->
    process_flag(trap_exit, true),
    catch(?unload(riak_stat)),
    catch(?unload(riak_stat_data)),
    catch(?unload(riak_stat_mgr)),
    catch(?unload(riak_stat_exom)),
    catch(?unload(riak_stat_meta)),
    catch(?unload(riak_stat_console)),
    catch(?unload(riak_stat_profiles)),
    process_flag(trap_exit, false),
    Children = [supervisor:which_children(Pid) || Pid <- Pids],
    lists:foreach(fun({Child, _n, _o, _b}) ->
        [supervisor:terminate_child(Pid, Child) || Pid <- Pids] end, Children).

-define(TestApps,     [riak_stat,riak_test,riak_core,riak_kv,riak_repl,riak_pipe]).
-define(TestCaches,   [{cache,6000},{cache,7000},{cache,8000},{cache,9000},{cache,0}]).
-define(TestStatuses, [{status,disabled},{status,enabled}]).
-define(TestName,     [stat,counter,active,list,pb,node,metadata,exometer]).
-define(TestTypes,    [histogram, gauge, spiral, counter, duration]).

-define(HistoAlias,   ['mean','max','99','95','median']).
-define(SpiralAlias,  ['one','count']).
-define(DuratAlias,   ['mean','max','last','min']).

types() ->
    pick_rand_(?TestTypes).

options() ->
    Cache  = pick_rand_(?TestCaches),
    Status = pick_rand_(?TestStatuses),
    [Cache|Status].

aliases(Stat,Type) ->
    case Type of
        histogram ->
            [alias(Stat,Alias) || Alias <- ?HistoAlias];
        gauge ->
            [];
        spiral ->
            [alias(Stat,Alias) || Alias <- ?SpiralAlias];
        counter ->
            [alias(Stat,value)];
        duration ->
            [alias(Stat,Alias) || Alias <- ?DuratAlias]
    end.

alias(Stat,Alias) ->
    Pre = lists:map(fun(S) -> atom_to_list(S) end, Stat),
    Pref = lists:join("_", Pre),
    Prefi = lists:concat(Pref++"_"++atom_to_list(Alias)),
    Prefix = list_to_atom(Prefi),
    {Alias, Prefix}.


pick_rand_([]) ->
    pick_rand_([error]);
pick_rand_(List) ->
    Num  = length(List),
    Elem = rand:uniform(Num),
    element(Elem, List).


stat_generator() ->
    Prefix = prefix(),
    RandomApp = pick_rand_(?TestApps),
    RandomName = [pick_rand_(?TestName) || _ <- lists:seq(1,rand:uniform(length(?TestName)))],
    Stat = [Prefix, RandomApp | RandomName],
    RandomType = types(),
    RandomOptions = options(),
    RandomAliases = aliases(Stat,RandomType),
    {Stat, RandomType, RandomOptions, RandomAliases}.

-define(TestStatNum, 1000).


%%%-------------------------------------------------------------------

register_stat_test() ->
    ?setuptest("riak_stat:register(Stat) test",
        [
            {"Register stats",                fun test_register_stats/0},
            {"Register an unregistered stat", fun test_register_unreg_stats/0},
            {"Register stat twice",           fun test_re_register/0},
            {"Register a stat many times",    fun test_stat_register_many/0},
            {"Register many stats many times",fun test_stats_register_many/0}
        ]).


test_register_stats() ->
    ?expect(riak_stat, register_stats, fun() -> ok end),
        lists:foreach(fun(_) ->
            Stat = stat_generator(),
            ?assert(register_stats(Stat))
                      end,
        lists:seq(1, rand:uniform(?TestStatNum))).


test_register_unreg_stats() ->
    {CStat, Ctype, COpts, CAlias} = stat_generator(),
    riak_stat:unregister(CStat),
    register_stats({CStat, Ctype, COpts, CAlias}).

test_re_register() ->
    Stat = stat_generator(),
    register_stats(Stat),
    register_stats(Stat).

test_stat_register_many() ->
    Stat = stat_generator(),
    [register_stats(Stat) || _ <- lists:seq(1, ?TestStatNum)].

test_stats_register_many() ->
    Stats = [stat_generator() || _ <- lists:seq(1, ?TestStatNum)],
    lists:foreach(fun(Stat) ->
        [register_stats(Stat) || _ <- lists:seq(1, ?TestStatNum)]
                  end, Stats).

%% todo: output checks: makesure what goes in is what is actually registered
%% todo: register without metadata, then reload metadata and register again
%% todo: check if registered with cache it is available to get in that time
%% from the cache
%% todo: check if it is registered without cache it is not available in that
%% time.

%% todo:what registers the quickest? exometer or metadata.

%%%-------------------------------------------------------------------
%%
reading_stats_test() ->
    ?setuptest("riak_stat:stats()/app_stats(App)/get_stats(Arg)/etc...",
        [
            {"stats()",                     fun test_stats_0/0},
            {"app_stats(App)",              fun test_app_stats_1/0},
            {"get_stats(Path)",             fun test_get_stats/0},
            {"find_entries/2",              fun test_find_entries_1/0},
            {"find_entries/4",              fun test_find_entries_2/0},
            {"get_values",                  fun test_get_values/0},
            {"get_info",                    fun test_get_info/0},
            {"aggregate stats",             fun test_aggregate/0},
            {"sample Stats",                fun test_sample_stat/0}
        ]).
%%
test_stats_0() ->
    Stats = get_stats([prefix()|'_']),
    ?assertEqual(true,
    print(Stats) == stats()). %% consistent.

test_app_stats_1() ->
    Stats = get_stats([prefix(),riak_stat|'_']),
    ?assertEqual(true,
        print(Stats) == app_stats(riak_stat)), %% existing app
    ?assertEqual(true,
    print([]) == app_stats(riak_fake)).

test_get_stats() ->
    Stat = stat_generator(),
    register_stats(Stat), %% ensure registered,
    Stat = get_stats(Stat),

    Path1 = [riak,riak_stat|'_'],
    PathGet1 = get_stats(Path1),
    ?assertEqual(true,
        print(PathGet1) == app_stats(riak_stat)).

test_find_entries_1() ->
    Path = [riak,riak_core|'_'],
    P1 = find_entries(Path,'_'), %% any status.
    P2 = find_entries(Path,enabled),
    P3 = find_entries(Path,disabled),
    %% make sure non are the same.
    ?assertEqual(true, P1 =/= P2),
    ?assertEqual(true, P2 =/= P3),
    ?assertEqual(true, P3 =/= P1),
    %% cause if they are theres other problems.
    P2List = lists_member(P1,P2),
    P3List = lists_member(P1,P3),
    P2List = [], %% should both be in the P1 list so both
    P3List = []. %% should be empty lists


lists_member(List1, List2) ->
    lists:concat(
        lists:map(fun(L) ->
            case lists:member(L, List1) of
                true -> [];
                false -> error
            end
                  end, List2)).

test_find_entries_2() ->
    Path = [riak,riak_kv|'_'],
    P1 = find_entries(Path,'_',histogram,[]),
    P2 = find_entries(Path,enabled,histogram,[]),
    P3 = find_entries(Path,disabled,histogram,[]),
    P4 = find_entries(Path,enabled,histogram,[max]),
    P5 = find_entries(Path,enabled,'_',[mean,max]),

    [] = lists_member(P1, P2),
    [] = lists_member(P1, P3),
    [] = lists_member(P4, P5).

test_get_values() ->
    ok = get_value(stat_generator()).

test_get_info() ->
    ok = get_info(riak_stat), %% for app when is_atom
    ok = get_info([riak,riak_stat|'_']).

test_aggregate() ->
    ok.

test_sample_stat() ->
    ok.


%%%-------------------------------------------------------------------

update_stats_test() ->
    ?setuptest("Update Stats Test",
        [{"Update a stat, check Vals are different",    fun test_update/0}]).

test_update() ->
    Name = [riak,riak_stat,control,stat],
    Type = histogram,
    Options = [{status,enabled},{cache,5000}],
    Aliases = [{max,control_stat_max},
    {min,control_stat_min},
    {99,control_stat_99},
    {95,control_stat_95},
    {mean,contol_stat_mean}],
    ControlsStat = {Name,Type, Options,Aliases},
    register_stats(ControlsStat),
    %% made sure the stat is registered and its type
    update(Name, rand:uniform(), Type),
    [{Name, Vals1}] = riak_stat_exom:get_values(Name),
    update(Name, rand:uniform(), Type),
    [{Name, Vals2}] = riak_stat_exom:get_values(Name),
    ?assertEqual(Vals1 =/= Vals2, true).

%%%-------------------------------------------------------------------

unregister_stats_test() ->
    ?setuptest("unregister and delete stats",
        [
            {"unregister stat",             fun test_unregister_stat/0},
            {"unregister stat twice",       fun test_unregister_again/0},
            {"unregister non-existing stat",fun test_unregister_unstat/0}
        ]).

test_unregister_stat() ->
    {Stat,T,O,A} = stat_generator(),
    register_stats({Stat,T,O,A}),
    riak_stat:unregister(Stat).

test_unregister_again() ->
    {Stat,T,O,A} = stat_generator(),
    register_stats({Stat,T,O,A}),
    riak_stat:unregister(Stat),
    riak_stat:unregister(Stat).

test_unregister_unstat() ->
    {Stat,_T,_O,_A} = stat_generator(),
    riak_stat:unregister(Stat).

%%%-------------------------------------------------------------------

reset_stats_test() ->
    ?setuptest("Reset a stat",
        [
            {"reset a stat",                fun test_reset_stat/0},
            {"reset a non-existing stat",   fun test_reset_unstat/0}
        ]).

test_reset_stat() ->
    {Stat,T,O,A} = stat_generator(),
    register_stats({Stat,T,O,A}),
    update(Stat,rand:uniform(),T),
    update(Stat,rand:uniform(),T),
    update(Stat,rand:uniform(),T),
    reset(Stat).

test_reset_unstat() ->
    {Stat,_T,_O,_A} = stat_generator(),
    reset(Stat).

-endif.
