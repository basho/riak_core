%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_test).
-include_lib("eunit/include/eunit.hrl").
-compile([export_all]).

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
%%%---------------------------------------------------------------------------------
-define(PREFIX,       riak).

-define(TestApps,     [riak_stat,riak_test,riak_core,riak_kv,riak_repl,riak_pipe]).
-define(TestCaches,   [{cache,6000},{cache,7000},{cache,8000},{cache,9000},{cache,0}]).
-define(TestStatuses, [{status,disabled},{status,enabled}]).
-define(TestName,     [stat,counter,active,list,pb,node,metadata,exometer]).
-define(TestTypes,    [histogram, gauge, spiral, counter, duration]).

-define(HistoAlias,   ['mean','max','99','95','median']).
-define(SpiralAlias,  ['one','count']).
-define(DuratAlias,   ['mean','max','last','min']).

-define(TestStatNum, 1000).

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
    Prefix = ?PREFIX,
    RandomApp = pick_rand_(?TestApps),
    RandomName = [pick_rand_(?TestName) || _ <- lists:seq(1,rand:uniform(length(?TestName)))],
    Stat = [Prefix, RandomApp | RandomName],
    RandomType = types(),
    RandomOptions = options(),
    RandomAliases = aliases(Stat,RandomType),
    {Stat, RandomType, RandomOptions, RandomAliases}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

data_sanitisation_tests_() ->
    ?setuptest("data_sanitise(Arg) when Arg == from Console",
        [
            {"Sanitise Data from the console for riak admin stat", fun test_data_sanitise_console/0},
            {"Sanitise Data from the console for riak admin endpoint", fun test_data_sanitise_endpoint/0}
        ]).

test_data_sanitise_console() -> ok.
test_data_sanitise_endpoint() -> ok.

profile_test_() ->
    ?setuptest("Test Profile functionality",
        [
            {"Save a profile", fun test_save_profile/0},
            {"Save a profile, same name, different nodes", fun test_save_profile_for_two/0},
            {"Save a profile, load on different nodes", fun test_save_profile_for_them/0},

            {"Load a profile", fun test_load_profile/0},
            {"Load a profile, on all nodes", fun test_load_all_profiles/0},
            {"Load a profile, already loaded", fun test_load_profile_again/0},

            {"Delete a profile", fun test_delete_profile/0},
            {"Delete a non-existent profile", fun test_delete_un_profile/0},
            {"Delete a profile thats loaded", fun test_delete_loaded_profile/0},
            {"Delete profile loaded on other nodes", fun test_unknown_delete_profile/0},
            {"Delete profile then load on other node", fun test_delete_then_they_load/0},

            {"Reset a profile", fun test_reset_profile/0}
        ]).

test_save_profile() -> ok.
test_save_profile_for_two() -> ok.
test_save_profile_for_them() -> ok.
test_load_profile() -> ok.
test_load_all_profiles() -> ok.
test_load_profile_again() -> ok.
test_delete_profile() -> ok.
test_delete_un_profile() -> ok.
test_delete_loaded_profile() -> ok.
test_unknown_delete_profile() -> ok.
test_delete_then_they_load() -> ok.
test_reset_profile() -> ok.

stat_admin_test_() ->
    ?setuptest("Stat administration functions",
        [
            {"Register a stat", fun test_register_stat/0},
            {"Register a stat again", fun test_register_stat_again/0},
            {"Register a basic stat", fun test_register_raw_stat/0},

            {"Read a stat by app", fun test_read_app_stats/0},
            {"Read a stat by path", fun test_read_path_stats/0},
            {"Read a stats value", fun test_read_stats_val/0},

            {"Update a stat", fun test_update_stat/0},
            {"Update a stat many times", fun test_update_stat_many/0},
            {"Update many stats at once", fun test_update_many_stat/0},
            {"Update many stats many times", fun test_update_many_stat_many/0},

            {"Unregister a stat", fun test_unregister_stat/0},
            {"Unregister unregistered stat", fun test_unregister_stat_again/0},
            {"Unregister a stat that doesnt exist", fun test_unregister_unstat/0},

            {"Reset a stat", fun test_reset_stat/0},
            {"Reset a stat thats disabled", fun test_reset_dis_stat/0},
            {"Reset a stat that doesnt exist", fun test_reset_non_stat/0}
        ]).

test_register_stat() -> ok.
test_register_stat_again() -> ok.
test_register_raw_stat() -> ok.
test_read_app_stats() -> ok.
test_read_path_stats() -> ok.
test_read_stats_val() -> ok.
test_update_stat() -> ok.
test_update_stat_many() -> ok.
test_update_many_stat() -> ok.
test_update_many_stat_many() -> ok.
test_unregister_stat() -> ok.
test_unregister_stat_again() -> ok.
test_unregister_unstat() -> ok.
test_reset_stat() -> ok.
test_reset_dis_stat() -> ok.
test_reset_non_stat() -> ok.

legacy_search_test_() ->
    ?setuptest("Testing the use of legacy_Search",
        [
            {"Test for legacy stat name", fun test_legacy_search_stat/0},
            {"Test for non legacy stat name", fun test_non_legacy_search_stat/0}
        ]).

test_legacy_search_stat() -> ok.
test_non_legacy_search_stat() -> ok.

find_entries_test_() ->
    ?setuptest("Test the efficiency and use of find_entries",
        [
            {"Find_entries in metadata",fun test_find_entries_metadata/0},
            {"Find_entries in exometer",fun test_find_entries_exometer/0},
            {"Find_entries fun in exometer",fun test_find_entries_exometer_fun/0}
        ]).

test_find_entries_metadata() -> ok.
test_find_entries_exometer() -> ok.
test_find_entries_exometer_fun() -> ok.

exometer_test_() ->
    ?setuptest("Testing functions specific to the exometer side of riak_stat",
        [
            {"Aggregation of stats", fun test_stat_aggregation/0},
            {"Sample for a probe in exometer", fun test_sample_exometer/0},
            {"aliases tests", fun test_aliases/0}
        ]).

test_stat_aggregation() -> ok.
test_sample_exometer() -> ok.
test_aliases() -> ok.

metadata_test_() ->
    ?setuptest("Test functions specific to the riak_core_metadata",
        [
            {""}
        ])