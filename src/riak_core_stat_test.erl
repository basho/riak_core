%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(riak_core_stat_test).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_core/include/riak_core_stat.hrl").

%%
%%-define(setup(Fun),        {setup,    fun setup/0,          fun cleanup/1, Fun}).
%%-define(foreach(Funs),     {foreach,  fun setup/0,          fun cleanup/1, Funs}). %% ist of Funs
%%-define(endpointsetup(Fun),{setup,    fun setup_endpoint/0, fun cleanup_endpoint/1, Fun}).
%%
%%-define(spawn(Test),       {spawn,        Test}).
%%-define(timeout(Test),     {timeout, 120, Test}).
%%-define(inorder(Test),     {inorder,      Test}).
%%-define(inparallel(Test),  {inparallel,   Test}).
%%
%%-define(unload(Module), meck:unload(Module)).
%%-define(new(Module),    meck:new(Module, [passthrough])).
%%-define(expect(Module, Fun, Val, Funfun), meck:expect(Module, Fun, Val, Funfun)).
%%-define(expect(Module, Fun, Funfun),      meck:expect(Module, Fun, Funfun)).
%%
%%-define(consoletest(Desc, Test), {?spawn([{Desc, ?setupconsole(fun(_) -> Test end)}])}).
%%-define(profiletest(Desc, Test), {?spawn([{Desc, ?setupprofile(fun(_) -> Test end)}])}).
%%-define(setuptest(Desc, Test), {Desc, ?setup(fun(_) -> Test end)}).
%%
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%% SETUP FUNCTIONS %%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%setup() ->
%%    catch(?unload(riak_stat)),
%%    catch(?unload(riak_stat_admin)),
%%    catch(?unload(riak_stat_data)),
%%    catch(?unload(riak_stat_info)),
%%    catch(?unload(riak_stat_coordinator)),
%%    catch(?unload(riak_stat_exometer)),
%%    catch(?unload(riak_stat_metdata)),
%%    catch(?unload(riak_stat_console)),
%%    catch (?unload(riak_stat_profiles)),
%%    ?new(riak_stat),
%%    ?new(riak_stat_admin),
%%    ?new(riak_stat_data),
%%    ?new(riak_stat_info),
%%    ?new(riak_stat_coordinator),
%%    ?new(riak_stat_exometer),
%%    ?new(riak_stat_metadata),
%%    ?new(riak_stat_console),
%%    ?new(riak_stat_profiles),
%%    {ok, Pid} = riak_core_stat_profiles:start_link(),
%%    [Pid].
%%
%%setup_endpoint() ->
%%    catch (?unload(exoskeleskin)),
%%    catch (?unload(exoskeles_console)),
%%    catch (?unload(exoskele_data)),
%%    catch (?unload(exoskele_json)),
%%    catch (?unload(exoskele_sup)),
%%    catch (?unload(exoskele_wm)),
%%    ?new(exoskeleskin),
%%    ?new(exoskeles_console),
%%    ?new(exoskele_data),
%%    ?new(exoskele_json),
%%    ?new(exoskele_sup),
%%    ?new(exoskele_wm),
%%    Arg = {{?MONITOR_LATENCY_PORT, ?INSTANCE, ?MONITOR_SERVER}, ['_']},
%%    {ok, Pid} = riak_core_stat_latency:start_link(Arg),
%%    [Pid | setup()]. %% endpoint depends on riak_stat
%%
%%%%%%%% CLEANUP %%%%%%
%%
%%cleanup(Pids) ->
%%    process_flag(trap_exit, true),
%%    catch(?unload(riak_stat)),
%%    catch(?unload(riak_stat_admin)),
%%    catch(?unload(riak_stat_data)),
%%    catch(?unload(riak_stat_info)),
%%    catch(?unload(riak_stat_coordinator)),
%%    catch(?unload(riak_stat_exometer)),
%%    catch(?unload(riak_stat_metdata)),
%%    catch(?unload(riak_stat_console)),
%%    catch(?unload(riak_stat_profiles)),
%%    process_flag(trap_exit, false),
%%    Children = [supervisor:which_children(Pid) || Pid <- Pids],
%%    lists:foreach(fun({Child, _n, _o, _b}) ->
%%        [supervisor:terminate_child(Pid, Child) || Pid <- Pids] end, Children).
%%
%%cleanup_endpoint(Pids) ->
%%    process_flag(trap_exit, true),
%%    catch(?unload(exoskele_console)),
%%    catch(?unload(exoskele_data)),
%%    catch(?unload(exoskele_udp)),
%%    catch(?unload(exoskele_wm)),
%%    catch(?unload(exoskele_json)),
%%    catch(?unload(exoskeleskin)),
%%    process_flag(trap_exit, false),
%%    cleanup(Pids).
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%% STATS  FUNCTIONS %%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%
%%
%%
%%% % % % % % % % % % % % % % % % % % % % % % %
%%%%%%%%%%%%%%%%%%%% console %%%%%%%%%%%%%%%%%%%
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%% TESTS DESCRIPTIONS %%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%%% riak-admin stat show <entry>
%%stat_show_test_() ->
%%    ?consoletest("riak-admin stat show <entry>",
%%        [
%%            {"stat show *.**",                        fun test_stat_show_star/0},
%%            {"stat show riak.**",                     fun test_stat_show_riak_star/0},
%%            {"stat show riak.*.node.**",              fun test_stat_show_riak_node_stats/0},
%%            {"stat show riak.**/status=disabled",     fun test_stat_show_disabled/0},
%%            {"stat show node_gets",                   fun test_stat_show_legacy_search/0},
%%            {"stat show riak.riak_kv.node.gets",      fun test_stat_show_node_gets/0},
%%            {"stat show *.**/type=duration/mean,max", fun test_stat_show_type_dps/0},
%%            {"stat show not_stat",                    fun test_stat_show_not_stat/0}
%%        ]).
%%
%%stat_show_0_test_() ->
%%    ?consoletest("riak-admin stat show-0 <entry>",
%%        [
%%            {"stat show-0 *.**",                      fun test_stat_show0_star/0},
%%            {"stat show-0 riak.**",                   fun test_stat_show0_riak_star/0},
%%            {"stat show-0 riak.*.node.**",            fun test_stat_show0_riak_node_stats/0},
%%            {"stat show-0 node_gets",                 fun test_stat_show0_legacy_search/0},
%%            {"stat show-0 not_stat",                  fun test_stat_show0_not_stat/0}
%%        ]).
%%
%%stat_disable_0_test_() ->
%%    ?consoletest("riak-admin stat disable-0 <entry>",
%%        [
%%            {"stat disable-0 riak.**",                fun test_stat_disable0_riak_star/0},
%%            {"stat disable-0 riak.*.node.**",         fun test_stat_disable0_riak_node_stats/0},
%%            {"stat disable-0 node_gets",              fun test_stat_disable0_legacy_search/0},
%%            {"stat disable-0 riak.riak_kv.node.gets", fun test_stat_disable0_node_gets/0},
%%            {"stat disable-0 not_stat",               fun test_stat_disable0_not_stat/0}
%%        ]).
%%
%%stat_info_test_() ->
%%    ?consoletest("riak-admin stat info <entry>",
%%        [
%%            {"stat show -module *.**",                fun test_stat_info_star_module/0},
%%            {"stat show -options riak.**",            fun test_stat_info_options_riak_star/0},
%%            {"stat show riak.*.node.**",              fun test_stat_info_riak_node_stats/0},
%%            {"stat show -status riak.**",             fun test_stat_info_status/0},
%%            {"stat show -datapoints node_gets",       fun test_stat_info_legacy_search_dps/0},
%%            {"stat show riak.riak_kv.node.gets",      fun test_stat_info_node_gets/0},
%%            {"stat show -type -value *.**",           fun test_stat_info_type_value/0},
%%            {"stat show not_stat",                    fun test_stat_info_not_stat/0}
%%        ]).
%%
%%stat_enable_test_() ->
%%    ?consoletest("riak-admin stat enable <entry>",
%%        [
%%            {"stat enable riak.**",                   fun test_stat_enable_riak_star/0},
%%            {"stat enable riak.*.node.**",            fun test_stat_enable_riak_node_stats/0},
%%            {"stat enable node_gets",                 fun test_stat_enable_legacy_search/0},
%%            {"stat enable riak.riak_kv.node.gets",    fun test_stat_enable_node_gets/0},
%%            {"stat enable not_stat",                  fun test_stat_enable_not_stat/0}
%%        ]).
%%
%%stat_disable_test_() ->
%%    ?consoletest("riak-admin stat disable <entry>",
%%        [
%%            {"stat disable riak.**",                  fun test_stat_disable_riak_star/0},
%%            {"stat disable riak.*.node.**",           fun test_stat_disable_riak_node_stats/0},
%%            {"stat disable node_gets",                fun test_stat_disable_legacy_search/0},
%%            {"stat disable riak.riak_kv.node.gets",   fun test_stat_disable_node_gets/0},
%%            {"stat disable not_stat",                 fun test_stat_disable_not_stat/0}
%%        ]).
%%
%%reset_stats_test_() ->
%%    ?consoletest("riak-admin stat reset <entry>",
%%        [
%%            {"stat reset riak.**",                    fun test_stat_reset_riak_star/0},
%%            {"stat reset riak.*.node.**",             fun test_stat_reset_riak_node_stats/0},
%%            {"stat reset node_gets",                  fun test_stat_reset_legacy_search/0},
%%            {"stat reset riak.riak_kv.node.gets",     fun test_stat_reset_node_gets/0},
%%            {"stat reset not_stat",                   fun test_stat_reset_not_stat/0}
%%        ]).
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%% ACTUAL TESTS %%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%%% riak-admin stat show
%%
%%test_stat_show_star() ->
%%    ok.
%%
%%test_stat_show_riak_star() ->
%%    ok.
%%
%%test_stat_show_riak_node_stats() ->
%%    ok.
%%
%%test_stat_show_disabled() ->
%%    ok.
%%
%%test_stat_show_legacy_search() ->
%%    ok.
%%
%%test_stat_show_node_gets() ->
%%    ok.
%%
%%test_stat_show_type_dps() ->
%%    ok.
%%
%%test_stat_show_not_stat() ->
%%    ok.
%%
%%
%%%% riak-admin stat show-0
%%
%%test_stat_show0_star() ->
%%    ok.
%%
%%test_stat_show0_riak_star() ->
%%    ok.
%%
%%test_stat_show0_riak_node_stats() ->
%%    ok.
%%
%%test_stat_show0_legacy_search() ->
%%    ok.
%%
%%test_stat_show0_not_stat() ->
%%    ok.
%%
%%
%%%% riak-admin stat disable-0
%%
%%test_stat_disable0_riak_star() ->
%%    ok.
%%
%%test_stat_disable0_riak_node_stats() ->
%%    ok.
%%
%%test_stat_disable0_legacy_search() ->
%%    ok.
%%
%%test_stat_disable0_node_gets() ->
%%    ok.
%%
%%test_stat_disable0_not_stat() ->
%%    ok.
%%
%%
%%%% riak-admin stat info
%%
%%test_stat_info_star_module() ->
%%    ok.
%%
%%test_stat_info_options_riak_star() ->
%%    ok.
%%
%%test_stat_info_riak_node_stats() ->
%%    ok.
%%
%%test_stat_info_status() ->
%%    ok.
%%
%%test_stat_info_legacy_search_dps() ->
%%    ok.
%%
%%test_stat_info_node_gets() ->
%%    ok.
%%
%%test_stat_info_type_value() ->
%%    ok.
%%
%%test_stat_info_not_stat() ->
%%    ok.
%%
%%
%%%% riak-admin stat enable
%%
%%test_stat_enable_riak_star() ->
%%    ok.
%%
%%test_stat_enable_riak_node_stats() ->
%%    ok.
%%
%%test_stat_enable_legacy_search() ->
%%    ok.
%%
%%test_stat_enable_node_gets() ->
%%    ok.
%%
%%test_stat_enable_not_stat() ->
%%    ok.
%%
%%
%%%% riak-admin stat disable
%%
%%test_stat_disable_riak_star() ->
%%    ok.
%%
%%test_stat_disable_riak_node_stats() ->
%%    ok.
%%
%%test_stat_disable_legacy_search() ->
%%    ok.
%%
%%test_stat_disable_node_gets() ->
%%    ok.
%%
%%test_stat_disable_not_stat() ->
%%    ok.
%%
%%
%%%% riak-admin stat reset
%%
%%test_stat_reset_riak_star() ->
%%    ok.
%%
%%test_stat_reset_riak_node_stats() ->
%%    ok.
%%
%%test_stat_reset_legacy_search() ->
%%    ok.
%%
%%test_stat_reset_node_gets() ->
%%    ok.
%%
%%test_stat_reset_not_stat() ->
%%    ok.
%%
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%% HELPER FUNCTIONS %%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%
%%
%%% % % % % % % % % % % % % % % % % % % % % % %
%%%%%%%%%%%%%%%%%%%% profiles %%%%%%%%%%%%%%%%%%
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%% TESTS DESCRIPTIONS %%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%save_profile_test_() ->
%%    ?profiletest("riak-admin stat save-profile <entry>",
%%        [
%%            {"Saving a profile",                  fun test_save_profile/0},
%%            {"Saving a profile twice",            fun test_save_profile_again/0}
%%        ]).
%%
%%load_profile_test_() ->
%%    ?profiletest("riak-admin stat load-profile <entry>",
%%        [
%%            {"Loading a profile",                 fun test_load_profile/0},
%%            {"Loading an already loaded profile", fun test_load_profile_again/0},
%%            {"Loading a non-existent profile",    fun test_load_fake_profile/0}
%%        ]).
%%
%%remove_profile_test_() ->
%%    ?profiletest("riak-admin stat remove-profile <entry>",
%%        [
%%            {"Delete a profile",                  fun test_delete_profile/0},
%%            {"Delete a non-existent profile",     fun test_delete_fake_profile/0}
%%        ]).
%%
%%reset_profiles_test_() ->
%%    ?profiletest("riak-admin stat reset-profiles <entry>",
%%        [
%%            {"Reset a profile that is loaded",    fun test_reset_profiles/0},
%%            {"Reset without a profile loaded",    fun test_reset_without/0}
%%        ]).
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%% ACTUAL TESTS %%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%%% riak-admin stat save-profile
%%
%%test_save_profile() ->
%%    ok.
%%
%%test_save_profile_again() ->
%%    ok.
%%
%%%% riak-admin stat load-profile
%%
%%test_load_profile() ->
%%    ok.
%%
%%test_load_profile_again() ->
%%    ok.
%%
%%test_load_fake_profile() ->
%%    ok.
%%
%%%% riak-admin stat delete-profile
%%
%%test_delete_profile() ->
%%    ok.
%%
%%test_delete_fake_profile() ->
%%    ok.
%%
%%%% riak-admin stat reset-profiles
%%
%%test_reset_profiles() ->
%%    ok.
%%
%%test_reset_without() ->
%%    ok.
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%% HELPER FUNCTIONS %%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%
%%% % % % % % % % % % % % % % % % % % % % % % %
%%%%%%%%%%%%%%%%%%%%% admin %%%%%%%%%%%%%%%%%%%%
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%% TESTS DESCRIPTIONS %%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%%%% API
%%
%%read_admin_api_test_() ->
%%    ?setuptest("riak_stat admin api read functions",
%%        [
%%            {"riak_stat:get_stats()",                   fun test_get_stats/0},
%%            {"riak_stat:get_stat(Path)",                fun test_get_stat/0},
%%            {"riak_stat:get_value(Arg)",                fun test_get_value/0},
%%            {"riak_stat:get_stats(App)",                fun test_get_app_stats/0},
%%            {"riak_stat:get_stats_info(App)",           fun test_get_stats_info/0},
%%            {"riak_stat:aggregate(Stats, Dps)",         fun test_aggregate_stats/0}
%%        ]).
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%create_admin_api_test_() ->
%%    ?setuptest("riak_stat admin api create functions",
%%        [
%%            {"register a stat that already exists",     fun test_re_register/0},
%%            {"register an unregistered stat",           fun test_stat_unregister/0}
%%        ]).
%%
%%update_admin_api_test_() ->
%%    ?setuptest("riak_stat admin api update functions",
%%        [
%%            {"update a non-existent stat",              fun test_update_non_stat/0},
%%            {"updata a stat twice at the same time",    fun test_multi_update/0},
%%            {"update a stat that is unregistered",      fun test_update_unregistered/0}
%%        ]).
%%
%%delete_admin_api_test_() ->
%%    ?setuptest("riak_stat admin api delete functions",
%%        [
%%            {"unregister a stat",                       fun test_unregister_stat/0},
%%            {"unregister a non-existent stat",          fun test_unregister_non_stat/0},
%%            {"unregister, then enabled metadata again", fun test_unregister_no_meta/0},
%%            {"unregister a stat twice at the same time",fun test_unregister_multi_stat/0}
%%        ]).
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%% ACTUAL TESTS %%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%%% Read Functions
%%
%%test_get_stats() ->
%%    {T, V} = timer:tc(fun riak_core_stat_admin:get_stats/0),
%%    io:format("riak_core_stat_admin:get_stats() -> ~p, in ~p milliseconds~n",
%%        [ok==V, T]).
%%
%%test_get_stat() ->
%%    {T, V} = timer:tc(fun riak_core_stat_admin:get_stat/1, [riak,common|'_']),
%%    io:format("riak_core_stat_admin:get_stat(riak.common.**) -> ~p, in ~p milliseconds~n",
%%        [ok==V, T]).
%%
%%test_get_value() ->
%%    {T, V} = timer:tc(fun riak_core_stat_admin:get_value/1, [riak,riak_kv,node|'_']),
%%    io:format("riak_core_stat_admin:get_value(riak.riak_kv.node.**) -> ~p in ~p milliseconds~n",
%%        [ok==V, T]).
%%
%%test_get_app_stats() ->
%%    {T, V} = timer:tc(fun riak_core_stat_admin:get_stats/1, riak_core),
%%    io:format("riak_core_stat_admin:get_stats(riak_core) -> ~p in ~p milliseconds~n",
%%        [ok==V, T]).
%%
%%test_get_stats_info() ->
%%    {T, V} = timer:tc(fun riak_core_stat_admin:get_info/1, riak_api),
%%    io:format("riak_core_stat_admin:get_info(riak_api) -> ~p in ~p milliseconds",
%%        [ok==V, T]).
%%
%%test_aggregate_stats() ->
%%    {T, V} = timer:tc(fun riak_core_stat_admin:aggregate/2, [[riak,riak_kv,'_',actor_count],[max]]),
%%    io:format("riak_core_stat_admin:aggregate([riak,riak_kv,'_',actor_count],[max]) -> ~p, in ~p ms~n",
%%        [ok==V, T]).
%%
%%%% Create Functions
%%
%%test_re_register() ->
%%    ok.
%%
%%test_stat_unregister() ->
%%    ok.
%%
%%%% Update Functions
%%
%%test_update_non_stat() ->
%%    ok.
%%
%%test_multi_update() ->
%%    ok.
%%
%%test_update_unregistered() ->
%%    ok.
%%
%%%% Delete Functions
%%
%%test_unregister_stat() ->
%%    ok.
%%
%%test_unregister_non_stat() ->
%%    ok.
%%
%%test_unregister_no_meta() ->
%%    ok.
%%
%%test_unregister_multi_stat() ->
%%    ok.
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%% ACTUAL TESTS %%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%
%%
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%% HELPER FUNCTIONS %%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
