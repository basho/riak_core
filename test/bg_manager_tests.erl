-module(bg_manager_tests).
-compile(export_all).

-include_lib("riak_core_bg_manager.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(BG_MGR, riak_core_bg_manager).
-define(TIMEOUT, 3000). %% blocking resource timeout

bg_mgr_test_() ->
    {timeout, 60000,  %% Seconds to finish all of the tests
     {setup, fun() ->
                     start_bg_mgr()      %% uses non-linking start.
             end, 
      fun(_) -> ok end,                           %% cleanup
      fun(_) ->
              [ %% Tests
                { "set/get token rates + verify rates",
                  fun() ->
                          setup_token_rates(),
                          verify_token_rates()
                  end},

                { "crash token manager + verify rates persist",
                  fun() ->
                          crash_and_restart_token_manager(),
                          verify_token_rates()
                  end},

                {"lock/token separation",
                 fun() ->
                         %% Trying to set the rate on a token of the wrong type looks
                         %% like an unregistered token.
                         ?assertEqual({unregistered, token_a}, riak_core_bg_manager:get_token(token_a)),
                         ?assertEqual(undefined, riak_core_bg_manager:set_token_rate(token_a, {1,5})),
                         ?assertEqual({badtype, token_a},
                                      riak_core_bg_manager:set_concurrency_limit(token_a, 42)),

                         %% Same for locks.
                         ?assertEqual({unregistered, lock_a}, riak_core_bg_manager:get_lock(lock_a)),
                         ?assertEqual(undefined, riak_core_bg_manager:set_concurrency_limit(lock_a, 52)),
                         ?assertEqual({badtype, lock_a},
                                      riak_core_bg_manager:set_token_rate(lock_a, {1, 5})),

                         %% Don't allow get_token(Lock)
                         ?assertEqual({badtype, lock_a}, riak_core_bg_manager:get_token(lock_a)),

                         %% Don't allow get_lock(Token)
                         ?assertEqual({badtype, token_a}, riak_core_bg_manager:get_lock(token_a))

                 end},

                {"failing crash/revive EQC test case",
                 %% bg_manager_eqc:set_token_rate('B', 2) -> 0
                 %% bg_manager_eqc:get_token('B') -> ok
                 %% bg_manager_eqc:crash() -> ok
                 %% bg_manager_eqc:revive() -> true
                 %% bg_manager_eqc:get_token('B') -> ok
                 %% bg_manager_eqc:get_token('B') -> ok
                 fun() ->
                         T = token_b,
                         Max = 2,
                         Period = 5000000,
                         ?BG_MGR:set_token_rate(T, {Period, Max}),
                         ?assertEqual(ok, ?BG_MGR:get_token(T)),
                         Locker = spawn(fun() -> receive _ -> ok end end),
                         Locker2 = spawn(fun() -> receive _ -> ok end end),
                         ?assertMatch({ok, _}, ?BG_MGR:get_lock(lock_a, Locker)),
                         ?assertMatch({ok, _}, ?BG_MGR:get_lock(lock_a, Locker2)),
                         ?assertEqual(1, length(?BG_MGR:all_tokens(T))),
                         kill_bg_mgr(),
                         Locker2 ! ok,
                         timer:sleep(100),
                         start_bg_mgr(),
                         %crash_and_restart_token_manager(),
                         ?assertEqual(ok, ?BG_MGR:get_token(T)),
                         ?assertEqual(2, length(?BG_MGR:all_tokens(T))),
                         ?assertEqual(max_concurrency, ?BG_MGR:get_token(T)),
                         ?assertEqual(1, length(?BG_MGR:all_locks(lock_a))),
                        Locker ! ok
                 end},

                {"lock api", [

                    {"get limit for unregistered lock", ?_assertEqual(unregistered, riak_core_bg_manager:concurrency_limit(noexists))},

                    {"set concurrency limit for new lock", ?_assertEqual(undefined, riak_core_bg_manager:set_concurrency_limit(lock_api, 2))},

                    {"get concurrency limit", ?_assertEqual(2, riak_core_bg_manager:concurrency_limit(lock_api))},

                    {"concurrency limit reached", ?_assertNot(riak_core_bg_manager:concurrency_limit_reached(lock_api))},

                    {"get lock with options", spawn, ?_assertMatch({ok, _}, riak_core_bg_manager:get_lock(lock_api, []))},

                    {"current count", ?_assertEqual(0, riak_core_bg_manager:lock_count(lock_api))},

                    {"lock info", fun() ->
                        Expected = lists:sort([
                            {lock_a, true, 52},
                            {lock_api, true, 2}
                        ]),
                        Info = lists:sort(riak_core_bg_manager:lock_info()),
                        ?assertEqual(Expected, Info)
                    end},

                    {"lock info for a specific lock", fun() ->
                        Expected = {true, 2},
                        ?assertEqual(Expected, riak_core_bg_manager:lock_info(lock_api))
                    end}

                ]},

                {"token api", [

                    {"set token rate", ?_assertEqual(undefined, riak_core_bg_manager:set_token_rate(token_api, {1000, 2}))},

                    {"get token sans pid", ?_assertEqual(ok, riak_core_bg_manager:get_token(token_api, []))},

                    {"all token info", fun() ->
                        % sleep to let tokens refill
                        UnsortedGot = riak_core_bg_manager:token_info(),
                        Got = lists:sort(UnsortedGot),
                        Expected = lists:sort(
                            [{TName, true, Rate} || {TName, Rate} <- some_token_rates()] ++ [
                            {token_api, true, {1000, 2}},
                            {token_a, true, {1,5}},
                            {token_b, true, {5000000,2}}
                        ]),
                        ?assertEqual(Expected, Got)
                    end},

                    {"a specific token's info", ?_assertEqual({true, {1000, 2}}, riak_core_bg_manager:token_info(token_api))}

                ]},

                {"reporting api", [

                    {"all resources", fun() ->
                        Self = self(),
                        Rec = {bg_stat_live, token_b, token, {Self, []}},
                        Expected = [Rec, Rec],
                        % wait for most tokens
                        timer:sleep(1100),
                        Got = riak_core_bg_manager:all_resources(),
                        ?assertEqual(Expected, Got)
                    end},

                    {"all resources of a given name", ?_assertEqual([], riak_core_bg_manager:all_resources(noexists))},

                    {"all locks", fun() ->
                        Self = self(),
                        Pids = lists:map(fun(_) ->
                            spawn(fun() ->
                                _ = riak_core_bg_manager:get_lock(lock_api),
                                Self ! continue,
                                receive _ -> ok end
                            end)
                        end, lists:seq(1,2)),
                        Expected = lists:map(fun(P) ->
                            {bg_stat_live, lock_api, lock, {P, []}}
                        end, Pids),
                        lists:foreach(fun(_) -> receive continue -> ok end end, Pids),
                        Got = riak_core_bg_manager:all_locks(),
                        ?assertEqual(Expected, Got)
                    end},

                    {"locks of a given name", ?_assertEqual([], riak_core_bg_manager:all_locks(noexists))},

                    {"all tokens", fun() ->
                        Self = self(),
                        Rec = {bg_stat_live, token_b, token, {Self, []}},
                        Expected = [Rec, Rec],
                        Got = riak_core_bg_manager:all_tokens(),
                        ?assertEqual(Expected, Got)
                    end}

                ]},

                {"enable/disable API", [
                    {"get current global enabled", ?_assertEqual(enabled, riak_core_bg_manager:enabled())},

                    {"global diable returns disabled", ?_assertEqual(disabled, riak_core_bg_manager:disable())},

                    {"specific resource during global disable returns disabled", ?_assertEqual(disabled, riak_core_bg_manager:enabled(token1))},

                    {"getting a token indicates max concurrency", ?_assertEqual(max_concurrency, riak_core_bg_manager:get_token(token1))},

                    {"global enable returns enabled", ?_assertEqual(enabled, riak_core_bg_manager:enable())},

                    {"global enable query", ?_assertEqual(enabled, riak_core_bg_manager:enabled())},

                    {"no explosion diabling an unregistered token", ?_assertEqual(unregistered, riak_core_bg_manager:disable(noexists))},

                    {"no explosion enabling an unregistered token", ?_assertEqual(unregistered, riak_core_bg_manager:enable(noexists))},

                    {"no explosion checking status of unregistered token", ?_assertEqual(unregistered, riak_core_bg_manager:enabled(noexists))},

                    {"no explosion disabling an unregistred lock with kill", ?_assertEqual(unregistered, riak_core_bg_manager:disable(noexists, true))},

                    {"disable a specific token", ?_assertEqual(disabled, riak_core_bg_manager:disable(token1))},

                    {"reports correct status for disabled token", ?_assertEqual(disabled, riak_core_bg_manager:enabled(token1))},

                    {"enable a specific token", ?_assertEqual(enabled, riak_core_bg_manager:enable(token1))},

                    {"disable with true kill exits a locking process", fun() ->
                        Self = self(),
                        {Locker, Mon} = spawn_monitor(fun() ->
                            {ok, _} = riak_core_bg_manager:get_lock(lock_a),
                            Self ! continue,
                            receive _ -> ok end
                        end),
                        receive continue -> ok end,
                        disabled = riak_core_bg_manager:disable(lock_a, true),
                        Got = receive
                            {'DOWN', Mon, process, Locker, max_concurrency} ->
                                true
                        after 3000 ->
                            false
                        end,
                        ?assert(Got)
                    end}

                ]},

                {"bypass API", [
                    {"set bypass to true", fun() ->
                        ok = riak_core_bg_manager:bypass(true),
                        ?assert(riak_core_bg_manager:bypassed())
                    end},

                    {"enabled returns bypassed", ?_assertEqual(bypassed, riak_core_bg_manager:enabled())},

                    {"reduce token rate to zero and still get a token", fun() ->
                        riak_core_bg_manager:set_token_rate(token_a, {1,0}),
                        ?assertEqual(ok, riak_core_bg_manager:get_token(token_a))
                    end},

                    {"reduce lock limiit to zero and still get one", fun() ->
                         riak_core_bg_manager:set_concurrency_limit(lock_a, 0),
                         ?assertMatch({ok, _Ref1}, riak_core_bg_manager:get_lock(lock_a))
                    end},

                    {"bypass over-rides resource specific disable", fun() ->
                        ?assertEqual(bypassed, riak_core_bg_manager:disable(lock_a))
                    end},

                    {"bypass over-rides global disable", fun() ->
                        ?assertEqual(bypassed, riak_core_bg_manager:disable()),
                        ?assertMatch({ok, _Ref2}, riak_core_bg_manager:get_lock(lock_a))
                    end},

                    {"max_concurrenty once bypass set to false", fun() ->
                        ?BG_MGR:bypass(false),
                        ?BG_MGR:enable(),
                        ?assertEqual(max_concurrency, ?BG_MGR:get_lock(lock_a))
                    end}
                 ]}

              ] end}
    }.

registered_token_names() ->
    [Token || {Token, _Enabled, _Rate} <- ?BG_MGR:token_info()].

check_head_token(Token, StatsList) ->
    F1 = fun(Stats) -> filter_stat(Token, Stats) end,
    ?assertEqual(?BG_MGR:head(Token), lists:map(F1, StatsList)).

filter_stat(Token, Stats) ->
    lists:filter(fun({T,_Stat}) -> Token==T end, Stats).

spawn_sync_request(Token, Pid, Meta) ->
    spawn(fun() -> ok = ?BG_MGR:get_token_blocking(Token, Pid, Meta, ?TIMEOUT) end).

-spec some_token_rates() -> [{bg_token(), {bg_period(), bg_count()}}].
some_token_rates() ->
    [ {token1, {1*1000, 5}},
      {token2, {1*1000, 4}},
      {{token3,stuff3}, {1*1000, 3}}
    ].

expected_token_names() ->
    [ Type || {Type, _Rate} <- some_token_rates()].

max_token_period() ->
    lists:foldl(fun({_T,{Period,_Limit}},Max) -> erlang:max(Period,Max) end, 0, some_token_rates()).

expected_token_limit(Token) ->
    {_Period, Limit} = proplists:get_value(Token, some_token_rates()),
    Limit.

setup_token_rates() ->
    [?BG_MGR:set_token_rate(Type, Rate) || {Type, Rate} <- some_token_rates()].

verify_token_rate(Type, ExpectedRate) ->
    Rate = ?BG_MGR:token_rate(Type),
    ?assertEqual(ExpectedRate, Rate).

verify_token_rates() ->
    [verify_token_rate(Type, Rate) || {Type, Rate} <- some_token_rates()],
    %% check un-registered token is not setup
    Rate = ?BG_MGR:token_rate(bogusToken),
    DefaultRate = {unregistered, bogusToken},
    ?assertEqual(DefaultRate, Rate).

%% start a stand-alone server, not linked, so that when we crash it, it
%% doesn't take down our test too.
start_bg_mgr() ->
    %% setup with history window to 1 seconds
    {ok, Pid} = ?BG_MGR:start_link(),
    unlink(Pid),
    timer:sleep(100).

kill_bg_mgr() ->
    Pid = erlang:whereis(?BG_MGR),
    ?assertNot(Pid == undefined),
    unlink(Pid),
    erlang:exit(Pid, kill).

crash_and_restart_token_manager() ->
    kill_bg_mgr(),
    timer:sleep(100),
    start_bg_mgr(),
    timer:sleep(100).

-endif.
