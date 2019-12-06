%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_test).
-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).
-compile([export_all]).
-endif.

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

-define(TestUPort,       8080).
-define(TestTPort,       8082).
-define(TestSip,        "127.0.0.1").
-define(TestUInstance,   "test-udp-instance").
-define(TestTInstance,   "test-tcp-instance").


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

console_test_() ->
    ?setuptest("Testing all the riak-admin console commands",
        [
            {"riak admin stat ____",        fun test_riak_admin_stat/0},
            {"riak admin push ____",        fun test_riak_admin_push/0},
            {"riak admin profiles ____",    fun test_riak_admin_profiles/0},
            {"riak admin <enter>  ____",    fun test_riak_admin_extra/0}
        ]).

test_riak_admin_stat() ->
    Names = namesarg(),
    %% All the functions use Names

    Types = typesarg(),
    %% Enable/Disable/Show/Show-0/Disable-0/Reset

    Statuses = statusarg(),
    %% Show/Show-0

    DPs = dparg(),
    %% Show/Show-0

    Infos = infoarg(),
    %% Info

    En = "enable",
    Dis = "disable",

    ShowArgs     = make_args({Names,Types,Statuses,DPs},15,[]),
    Show0Args    = make_args({Names,Types,Statuses,DPs},15,[]),
    InfoArgs     = make_args({Names,Infos},15,[]),
    EnableArgs   = make_args({Names,Types},15,[]),
    DisableArgs  = make_args({Names,Types},15,[]),
    ResetArgs    = make_args({Names,Types},15,[]),
    MetadataArgs = [En,Dis,En,En,Dis,Dis,En,En,Dis,En,Dis,En],

    ?assert(test_random(show,ShowArgs)),
    ?assert(test_random(show0,Show0Args)),
    ?assert(test_random(info,InfoArgs)),
    ?assert(test_random(enable,EnableArgs)),
    ?assert(test_random(disable,DisableArgs)),
    ?assert(test_random(reset,ResetArgs)),
    ?assert(test_random(metadata,MetadataArgs)),
    ok.

namesarg() ->
    ["riak.**",
        "riak.riak_kv.**",
        "riak.**.time",
        "riak.*",
        "riak.*.*.*",
        "node_gets",
        "riak_repl.**.sent"].

typesarg() ->
    ["/type=spiral",
        "/type=gauge",
        "/type=duration",
        "/type=histogram",
        "/type=counter",
        "/type=fast_counter",
        "/type=probe",
        ""].

statusarg() ->
    ["/status=enabled",
        "/status=disabled",
        "/status=*",
        "/status=unregistered",
        ""].

dparg() ->
    ["/mean",
        "/min,max",
        "/value,type",
        "/resets,status",
        "/median,mode",
        "/name,module",
        "/ ",
        ""].

infoarg() ->
    ["-type-status",
        "-type",
        "-value",
        "-cache",
        "-name-timestamp",
        "-options",
        ""].

test_riak_admin_push() ->
    Ports     = portsarg(),
    Protocols = protocolarg(),
    Instances = instancearg(),
    ServerIps = serveriparg(),

    SetupArgs   = make_args({Protocols,Ports,Instances,ServerIps},10,[]),
    SetdownArgs = make_args({[""|Protocols],[""|Ports],[""|Instances],[""|ServerIps]},20,[]),
    InfoArgs    = make_args({[""|Protocols],[""|Ports],[""|Instances],[""|ServerIps]},10,[]),

    ?assert(test_random(setup,SetupArgs)),
    ?assert(test_random(setdown,SetdownArgs)),
    ?assert(test_random(infopush,InfoArgs)),
    ?assert(test_random(infopushall,InfoArgs)),
    ok.

portsarg() ->
    [",port=" ++ Str || Str <-
        ["8080", "9090", "9090", "0", "1234"
            "24999","57863", "30030303"]].

protocolarg() ->
    ["protocol=tcp","protocol=udp"].

instancearg() ->
    [",instance=" ++ Str || Str <-
        ["help","help","test","isthisglutenfree",
            "tunasandwich","cheese","test","dairy"]].

serveriparg() ->
    [",sip=" ++ Str || Str <-
        ["127.0.0.1","0.0.0.0","localhost","chicken"]].

test_riak_admin_profiles() ->
    Profiles = profilearg(),
    ?assert(test_random(save,Profiles)),
    ?assert(test_random(load,Profiles)),
    ?assert(test_random(delete,Profiles)),
    ?assert(test_random(profreset,Profiles)),
    ok.

profilearg() ->
    ["profilename","testprofile","profiletest",
        "testagain","name","abcde",
        "riak_stat","help","useful",
        "stats","pushprofile","testing"].

test_riak_admin_extra() ->
    %% test all the riak stat stuff together

    ok.

make_args(_Lists,0,Acc) -> Acc;
make_args({Name,Info},Times,Acc) ->
    make_args({Name,Info},Times-1,
        [(r(Name)++r(Info))|Acc]);
make_args({Name,Type,Statuses,DPs},Times,Acc) ->
    make_args({Name,Type,Statuses,DPs},Times-1,
        [(r(Name)++r(Type)++r(Statuses)++r(DPs))|Acc]).

r(N) ->
    lists:nth(rand:uniform(length(N)),N).

test_random(Type,Args) ->
    lists:foreach(fun(Arg) ->
        ?debugFmt("Arg: ~p",[Arg]),
        test_fun(Type,Arg)
                  end, Args).

test_fun(show,Arg) ->
    ok == riak_core_console:stat_show(Arg);
test_fun(show0,Arg) ->
    ok == riak_core_console:stat_0(Arg);
test_fun(info,Arg) ->
    ok == riak_core_console:stat_info(Arg);
test_fun(enable,Arg) ->
    ok == riak_core_console:stat_enable(Arg);
test_fun(disable,Arg) ->
    ok == riak_core_console:stat_disable(Arg);
test_fun(reset,Arg) ->
    ok == riak_core_console:stat_reset(Arg);
test_fun(metadata,Arg) ->
    ok == riak_core_console:stat_metadata(Arg);

test_fun(setup,Arg) ->
    ok == riak_core_console:setup_endpoint(Arg);
test_fun(setdown,Arg) ->
    ok == riak_core_console:setdown_endpoint(Arg);
test_fun(infopush,Arg) ->
    ok == riak_core_console:find_push_stats(Arg);
test_fun(infopushall,Arg) ->
    ok == riak_core_console:find_push_stats_all(Arg);

test_fun(save,Arg) ->
    ok == riak_core_console:add_profile(Arg);
test_fun(load,Arg) ->
    ok == riak_core_console:load_profile(Arg);
test_fun(delete,Arg) ->
    ok == riak_core_console:remove_profile(Arg);
test_fun(profreset,Arg) ->
    ok == riak_core_console:reset_profile(Arg).



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
            {"Delete profile loaded on other nodes",        fun test_unknown_delete_profile/0},
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
            {"Unregister a stat",                   fun test_unregister_stat/0},
            {"Unregister unregistered stat",        fun test_unregister_stat_again/0},
            {"Reset a stat",                        fun test_reset_stat/0},
            {"Reset a stat thats disabled",         fun test_reset_dis_stat/0}
        ]).


%%% --------------------------------------------------------------



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
    ?assertEqual([[riak|'_']],N1),
    ?assertEqual([[riak,riak_kv|'_']],N2),
    ?assertEqual([[riak,riak_kv,node,gets|'_']],N3),
    ?assertEqual([[riak,riak_kv,node,gets]],N4),
    ?assertEqual([[node_gets]],N5),
    ?assertEqual([[riak|'_']],N6), ?_assertEqual(duration,T6),
    ?assertEqual([[riak|'_']],N7), ?_assertEqual([mean,max],B7),
    ?assertEqual([[riak,riak_kv|'_']],N8),?_assertEqual(spiral,T8),
    ?assertEqual('_',S8),?_assertEqual([one],B8),
    ?assertEqual([[riak,'_',time],
                    [riak,'_','_',time],
                    [riak,'_','_','_',time],
                    [riak,'_','_','_','_',time],
                    [riak,'_','_','_','_','_',time],
                    [riak,'_','_','_','_','_','_',time],
                    [riak,'_','_','_','_','_','_','_',time],
                    [riak,'_','_','_','_','_','_','_','_',time],
                    [riak,'_','_','_','_','_','_','_','_','_',time],
                    [riak,'_','_','_','_','_','_','_','_','_','_',time]],N9).
                        %% stop judging the pyramid                                                                             Its meant to look like that

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
    {{ P1,_I1,_S1},_Stats1} = riak_stat_push:sanitise_data(A1),
    {{ P2,_I2, S2},_Stats2} = riak_stat_push:sanitise_data(A2),
    {{ P3, I3, S3},_Stats3} = riak_stat_push:sanitise_data(A3),
    {{ P4,_I4,_S4}, Stats4} = riak_stat_push:sanitise_data(A4),
    {{ P5,_I5, S5}, Stats5} = riak_stat_push:sanitise_data(A5),
    {{_P6,_I6, S6}, Stats6} = riak_stat_push:sanitise_data(A6),
    {{_P7,_I7,_S7}, Stats7} = riak_stat_push:sanitise_data(A7),
    ?_assertEqual(8080,P1),
    ?_assertEqual(8080,P2),?_assertEqual(?TestSip,S2),
    ?_assertEqual(8080,P3),?_assertEqual(?TestSip,S3),?_assertEqual("test",I3),
    ?_assertEqual(8080,P4),?_assertEqual([riak,riak_kv|'_'],Stats4),
    ?_assertEqual(8080,P5),?_assertEqual(?TestSip,S5),?_assertEqual([riak,riak_kv|'_'],Stats5),
    ?_assertEqual(?TestSip,S6),?_assertEqual([riak,riak_kv|'_'],Stats6),
    ?_assertEqual([riak,riak_kv|'_'],Stats7).

%%% --------------------------------------------------------------

%% @see profile_test_/0

test_save_profile() ->
    %% Save a profile
    ProfileName = ["test-profile"], %% input type from console
    ?_assert(ok == riak_stat_profiles:save_profile(ProfileName)).

test_save_profile_for_two() ->
    %% Save a profile with different stats configuration on two separate
    %% nodes at the same time, see which becomes the alpha
    %%
    %% This test was done manually, the profile is consistent with only one
    %% node always, therefore that node is the alpha.
    %% However the node that "wins" the main profile stat configuration
    %% is random.
    ok.

test_save_profile_for_them() ->
    %% Save a profile, open on a different node
    %%
    %% Tested manually, the profile only changes the stats that need changing
    %% on that node.
    ok.

test_load_profile() ->
    %% load a profile
    ProfileName = ["hellothere"],
    riak_stat_profiles:save_profile(ProfileName),
    ?_assert(ok == riak_stat_profiles:load_profile(ProfileName)).

test_load_all_profiles() ->
    %% load a profile, onto multiple nodes
    %%
    %% Tested manually, can be done, each node changes the stats based on
    %% the stat configuration on that node,
    ok.

test_load_profile_again() ->
    %% load a profile that is already loaded,
    %% enabled all the stats and load that profile again
    %%
    %% First time testing:
    %%
    %% RPC to 'dev1@127.0.0.1' failed: {'EXIT',
%%    {{case_clause,{status,enabled}},
%%        [{exometer,setopts,2,
%%            [{file,
%%                "/home/savannahallsop/github/riak/_checkouts/riak_core/_build/default/lib/exometer_core/src/exometer.erl"},
%%                {line,535}]},
%%            {lists,map,2,
%%                [{file,"lists.erl"},{line,1239}]},
%%            {lists,map,2,
%%                [{file,"lists.erl"},{line,1239}]},
%%            {riak_stat_mgr,change_status,1,
%%                [{file,
%%                    "/home/savannahallsop/github/riak/_checkouts/riak_core/src/riak_stat_mgr.erl"},
%%                    {line,290}]},
%%            {rpc,'-handle_call_call/6-fun-0-',5,
%%                [{file,"rpc.erl"},{line,197}]}]}}
%%
%%  Testing it again: by trying to recreate the error
%%
%%  Works perfectly fine.
    ok.

test_delete_profile() ->
    %% Delete a profile
    ProfileName = ["obiwankenobi"],
    riak_stat_profiles:save_profile(ProfileName),
    ?_assert(ok == riak_stat_profiles:delete_profile(ProfileName)).

test_delete_un_profile() ->
    %% Delete a profile that has never existed,
    %% then delete a profile that used to exist
    ProfileName = ["Idonotexist"],
    ?_assert(no_profile == riak_stat_profiles:delete_profile(ProfileName)),
    ?_assert(no_profile == riak_stat_profiles:delete_profile(["anotheronebitesthedust"])).


test_unknown_delete_profile() ->
    %% Load a profile on another node, then delete it on a different node
    %%
    %% Loaded "timothy" one dev1, deleted on dev2. Loaded timothy again on
    %% dev1 and it loaded again.
    %% Reset profile and loaded timothy with return Error : no profile exists.
    ok.

test_reset_profile() ->
    %% Reset profile . i.e. set all stats to enabled and unload a profile
    ProfileName = ["you-were-my-brother-anakin"],
    riak_stat_profiles:save_profile(ProfileName),
    ?_assert(ok == riak_stat_profiles:reset_profile()),
    ?_assert([] == riak_stat_exom:select([{{[riak|'_'],'_',disabled},[],['$_']}])).

%%% --------------------------------------------------------------

%% @see stat_admin_test_/0

test_register_stat() ->
    %% Register a stat in the metadata and in exometer
    Stat = stat_generator(),
    ?_assert(ok == riak_stat:register_stats(Stat)).

test_register_stat_again() ->
    %% register a stat then register it again
    Stat = stat_generator(),
    ok = riak_stat:register_stats(Stat),
    ok = riak_stat:register_stats(Stat),
    ?_assert(ok == riak_stat:register_stats(Stat)).

test_register_raw_stat() ->
    %% register a stat with just its name and type
    Stat = {[riak,stat,test,name],counter,[],[]},
    ?_assert(ok == riak_stat:register_stats(Stat)).

test_read_app_stats() ->
    %% get stats using riak_stat:get_stats(App)
    App = riak_api,
    StatName = [?PREFIX,App|'_'],
    riak_stat_console:status_change(StatName, enabled), %% ensure enabled
    Stats = riak_stat:get_stats(StatName), %% as used in riak_stat
    ?_assertEqual(Stats, [{pbc_connects,spiral,enabled},
                            {[pbc_connects,active],{function,App,active_pbc_connects},enabled}]).

test_read_path_stats() ->
    %% get a stats by passing in the path of the stat
    Path = [?PREFIX,riak_kv,node,gets,time],
    riak_stat_console:status_change(Path, enabled), %% ensure enabled
    Stat = riak_stat:get_stats(Path),
    ?_assertEqual([{[riak,riak_kv,node,gets,time],histogram,enabled}],Stat).

test_read_stats_val() ->
    %% Get a stats value from exometer
    Stat = [?PREFIX,riak_stat,test,counter],
    riak_stat:register_stats({Stat,spiral,[],[]}),
    ok = update_again(Stat,1.0,spiral,100),
    {ok, Value} = riak_stat_exom:get_value(Stat),
    Count = proplists:get_value(count, Value),
    ?_assertEqual(Count, 100.0).

update_again(_,_,_,0) ->
    ok;
update_again(Stat,Incr,Type,Num) ->
    riak_stat:update(Stat,Incr,Type),
    update_again(Stat,Incr,Type,Num-1).

test_update_stat() ->
    %% update a stat
    Stat = [?PREFIX,riak_stat,test,updates],
    riak_stat:register_stats({Stat,counter,[],[]}),
    update_again(Stat,1,counter,1).

test_unregister_stat() ->
    %% unregister a stat
    Stat = [?PREFIX,riak_stat,unregister,test],
    riak_stat:register_stats({Stat,counter,[],[]}),
    ?_assertEqual(ok,riak_stat:unregister(Stat)).

test_unregister_stat_again() ->
    %% unregister a stat that has just been unregistered
    Stat = [?PREFIX,riak_stat,unregister,test],
    riak_stat:register_stats({Stat,counter,[],[]}),
    ?_assertEqual(ok,riak_stat:unregister(Stat)),
    ?_assertEqual(ok,riak_stat:unregister(Stat)).

test_reset_stat() ->
    %% reset a stat (known to be enabled)
    Stat = [?PREFIX,riak_stat,reset,test],
    riak_stat:register_stats({Stat,counter,[],[]}),
    update_again(Stat,1,counter,24601),
    ?_assertEqual(ok,riak_stat:reset(Stat)).

test_reset_dis_stat() ->
    %% try to reset a disabled stat (error)
    Stat = [?PREFIX,riak_stat,reset,test],
    riak_stat:register_stats({Stat,counter,[],[]}),
    update_again(Stat,1,counter,24601),
    riak_stat_console:status_change(Stat,disabled),
    ?_assertEqual(ok,riak_stat:reset(Stat)).
