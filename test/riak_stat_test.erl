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

-define(TestPort,       8189).
-define(TestSip,        "127.0.0.1").
-define(TestInstance,   "testinstance").


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%% HELPER FUNS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
%%%%%%%%%%%%%%%%%%%%%%%%%%% SETUP TEST %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

setup() ->
    exometer:start(),
    lists:foreach(fun(_) ->
        riak_stat:register_stats(stat_generator())
                  end, lists:seq(1, rand:uniform(?TestStatNum))).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%% CLEANUP TEST %%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

cleanup(_) ->
    exometer:stop().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%% TEST FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

data_sanitisation_tests_() ->
    ?setuptest("data_sanitise(Arg) when Arg == from Console",
        [
            {"Sanitise Data from the console for riak admin stat",     fun test_data_sanitise_console/0},
            {"Sanitise Data from the console for riak admin endpoint", fun test_data_sanitise_endpoint/0}
        ]).

%%% --------------------------------------------------------------

profile_test_() ->
    ?setuptest("Test Profile functionality",
        [
            {"Save a profile",                              fun test_save_profile/0},
            {"Save a profile, same name, different nodes",  fun test_save_profile_for_two/0},
            {"Save a profile, load on different nodes",     fun test_save_profile_for_them/0},

            {"Load a profile",                              fun test_load_profile/0},
            {"Load a profile, on all nodes",                fun test_load_all_profiles/0},
            {"Load a profile, already loaded",              fun test_load_profile_again/0},

            {"Delete a profile",                            fun test_delete_profile/0},
            {"Delete a non-existent profile",               fun test_delete_un_profile/0},
            {"Delete a profile thats loaded",               fun test_delete_loaded_profile/0},
            {"Delete profile loaded on other nodes",        fun test_unknown_delete_profile/0},
            {"Delete profile then load on other node",      fun test_delete_then_they_load/0},

            {"Reset a profile",                             fun test_reset_profile/0}
        ]).

%%% --------------------------------------------------------------

stat_admin_test_() ->
    ?setuptest("Stat administration functions",
        [
            {"Register a stat",                     fun test_register_stat/0},
            {"Register a stat again",               fun test_register_stat_again/0},
            {"Register a basic stat",               fun test_register_raw_stat/0},

            {"Read a stat by app",                  fun test_read_app_stats/0},
            {"Read a stat by path",                 fun test_read_path_stats/0},
            {"Read a stats value",                  fun test_read_stats_val/0},

            {"Update a stat",                       fun test_update_stat/0},
            {"Update a stat many times",            fun test_update_stat_many/0},
            {"Update many stats at once",           fun test_update_many_stat/0},
            {"Update many stats many times",        fun test_update_many_stat_many/0},

            {"Unregister a stat",                   fun test_unregister_stat/0},
            {"Unregister unregistered stat",        fun test_unregister_stat_again/0},
            {"Unregister a stat that doesnt exist", fun test_unregister_unstat/0},

            {"Reset a stat",                        fun test_reset_stat/0},
            {"Reset a stat thats disabled",         fun test_reset_dis_stat/0},
            {"Reset a stat that doesnt exist",      fun test_reset_non_stat/0}
        ]).

%%% --------------------------------------------------------------

legacy_search_test_() ->
    ?setuptest("Testing the use of legacy_Search",
        [
            {"Test for legacy stat name",     fun test_legacy_search_stat/0},
            {"Test for non legacy stat name", fun test_non_legacy_search_stat/0}
        ]).

%%% --------------------------------------------------------------

find_entries_test_() ->
    ?setuptest("Test the efficiency and use of find_entries",
        [
            {"Find_entries in metadata",    fun test_find_entries_metadata/0},
            {"Find_entries in exometer",    fun test_find_entries_exometer/0},
            {"Find_entries fun in exometer",fun test_find_entries_exometer_fun/0}
        ]).

%%% --------------------------------------------------------------

exometer_test_() ->
    ?setuptest("Testing functions specific to the exometer side of riak_stat",
        [
            {"Aggregation of stats",           fun test_stat_aggregation/0},
            {"Sample for a probe in exometer", fun test_sample_exometer/0},
            {"aliases tests",                  fun test_aliases/0}
        ]).

%%% --------------------------------------------------------------

metadata_test_() ->
    ?setuptest("Test functions specific to the riak_core_metadata",
        [
            {"Test stat configuration persistence", fun test_stat_persistence/0},
            {"Test profile persistence",            fun test_profile_persistence/0},
            {"Test fold in riak_core_meta for profiles/stats", fun test_fold_meta/0}
        ]).

%%% --------------------------------------------------------------

endpoint_test_() ->
    ?setuptest("Test stuff specific to pushing stats to an endpoint",
        [
            {"Set up stat polling to an udp endpoint",   fun test_stat_polling/0},
            {"Set down stat polling to an udp endpoint", fun test_stop_stat_polling/0},
            {"Test Json objects from metrics",           fun test_udp_json/0},
            {"Sanitise data from console for endpoint",  fun test_sanitise_data_endpoint/0}
        ]).

%%% --------------------------------------------------------------

wm_test_() ->
    ?setuptest("Testing the HTTP output of stats via Webmachine",
        [
            {"Stats and aggregation in HTTP", fun test_stats_http/0},
            {"Json objects from metrics wm",  fun test_json_objects_wm/0}
        ]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%% HELPER TEST FUNS %%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @see data_sanitisation_tests_/0

test_data_sanitise_console() ->
    %% Sanitise data from the console specific for riak admin stat ---
    A1 = ["riak.**"], %% the format the data comes in from riak_core_console,
    A2 = ["riak.riak_kv.**"],
    A3 = ["riak.riak_kv.node.gets.**"],
    A4 = ["riak.riak_kv.node.gets"],
    A5 = ["node_gets"], %% legacy search for same as above
    A6 = ["riak.**/type=duration"],
    A7 = ["riak.**/mean,max"],
    A8 = ["riak.riak_kv.**/status=*/type=spiral/one"],
    A9 = ["riak.**.time"], %% riak at beginning and time at end
    {N1,_T1,_S1,_B1} = riak_stat_console:data_sanitise(A1),
    {N2,_T2,_S2,_B2} = riak_stat_console:data_sanitise(A2),
    {N3,_T3,_S3,_B3} = riak_stat_console:data_sanitise(A3),
    {N4,_T4,_S4,_B4} = riak_stat_console:data_sanitise(A4),
    {N5,_T5,_S5,_B5} = riak_stat_console:data_sanitise(A5),
    {N6, T6,_S6,_B6} = riak_stat_console:data_sanitise(A6),
    {N7,_T7,_S7, B7} = riak_stat_console:data_sanitise(A7),
    {N8, T8, S8, B8} = riak_stat_console:data_sanitise(A8),
    {N9,_T9,_S9,_B9} = riak_stat_console:data_sanitise(A9),
    ?_assertEqual([riak|'_'],N1),
    ?_assertEqual([riak,riak_kv|'_'],N2),
    ?_assertEqual([riak,riak_kv,node,gets|'_'],N3),
    ?_assertEqual([riak,riak_kv,node,gets],N4),
    ?_assertEqual([node_gets],N5),
    ?_assertEqual([riak|'_'],N6), ?_assertEqual(duration,T6),
    ?_assertEqual([riak|'_'],N7), ?_assertEqual([mean,max],B7),
    ?_assertEqual([riak,riak_kv|'_'],N8),?_assertEqual(spiral,T8),
        ?_assertEqual('_',S8),?_assertEqual([one],B8),
    ?_assertEqual([[riak,'_',time],
                    [riak,'_','_',time],
                    [riak,'_','_','_',time],
                    [riak,'_','_','_','_',time],
                    [riak,'_','_','_','_','_',time],
                    [riak,'_','_','_','_','_','_',time],
                    [riak,'_','_','_','_','_','_','_',time],
                    [riak,'_','_','_','_','_','_','_','_',time],
                    [riak,'_','_','_','_','_','_','_','_','_',time],
                    [riak,'_','_','_','_','_','_','_','_','_','_',time]],N9).
                        %% stop judging the pyramid

test_data_sanitise_endpoint() ->
    %% Sanitise data from the console specific to riak admin stat setup/
    %% setdown ---
    A1 = ["port=8080"],
    A2 = ["port=8080 sip=127.0.0.1"],
    A3 = ["port=8080 sip=127.0.0.1 instance=test"],
    A4 = ["port=8080/riak.riak_kv.**"],
    A5 = ["port=8080 sip=127.0.0.1/riak.riak_kv.**"],
    A6 = ["sip=127.0.0.1/riak.riak_kv.**"],
    A7 = ["riak.riak_kv.**"],
    {{ P1,_I1,_S1},_Stats1} = riak_stat_latency:sanitise_data(A1),
    {{ P2,_I2, S2},_Stats2} = riak_stat_latency:sanitise_data(A2),
    {{ P3, I3, S3},_Stats3} = riak_stat_latency:sanitise_data(A3),
    {{ P4,_I4,_S4}, Stats4} = riak_stat_latency:sanitise_data(A4),
    {{ P5,_I5, S5}, Stats5} = riak_stat_latency:sanitise_data(A5),
    {{_P6,_I6, S6}, Stats6} = riak_stat_latency:sanitise_data(A6),
    {{_P7,_I7,_S7}, Stats7} = riak_stat_latency:sanitise_data(A7),
    ?_assertEqual(8080,P1),
    ?_assertEqual(8080,P2),?_assertEqual(?TestSip,S2),
    ?_assertEqual(8080,P3),?_assertEqual(?TestSip,S3),?_assertEqual("test",I3),
    ?_assertEqual(8080,P4),?_assertEqual([riak,riak_kv|'_'],Stats4),
    ?_assertEqual(8080,P5),?_assertEqual(?TestSip,S5),?_assertEqual([riak,riak_kv|'_'],Stats5),
    ?_assertEqual(?TestSip,S6),?_assertEqual([riak,riak_kv|'_'],Stats6),
    ?_assertEqual([riak,riak_kv|'_'],Stats7).


%%% --------------------------------------------------------------

%% @see profile_test_/0

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

%%% --------------------------------------------------------------

%% @see stat_admin_test_/0

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

%%% --------------------------------------------------------------

%% @see legacy_search_test_/0

test_legacy_search_stat() -> ok.
test_non_legacy_search_stat() -> ok.

%%% --------------------------------------------------------------

%% @see find_entries_test_/0

test_find_entries_metadata() -> ok.
test_find_entries_exometer() -> ok.
test_find_entries_exometer_fun() -> ok.

%%% --------------------------------------------------------------

%% @see exometer_test_/0

test_stat_aggregation() -> ok.
test_sample_exometer() -> ok.
test_aliases() -> ok.

%%% --------------------------------------------------------------

%% @see metadata_test_/0

test_stat_persistence() -> ok.
test_profile_persistence() -> ok.
test_fold_meta() -> ok.

%%% --------------------------------------------------------------

%% @see endpoint_test_/0

test_stat_polling() -> ok.
test_stop_stat_polling() -> ok.
test_udp_json() -> ok.
test_sanitise_data_endpoint() -> ok.

%%% --------------------------------------------------------------

%% @see wm_test_/0

test_stats_hhtp() -> ok.
test_json_objects_wm() -> ok.

