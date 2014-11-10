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
      fun(_) ->
	      kill_bg_mgr(),
	      ok end,                           %% cleanup
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
                         ?assertEqual(1, length(?BG_MGR:all_tokens(T))),
                         crash_and_restart_token_manager(),
                         ?assertEqual(ok, ?BG_MGR:get_token(T)),
                         ?assertEqual(2, length(?BG_MGR:all_tokens(T))),
                         ?assertEqual(max_concurrency, ?BG_MGR:get_token(T))
                 end},

                {"bypass API",
                 fun() ->
                         %% bypass API
                         riak_core_bg_manager:bypass(true),

                         %% reduce token rate to zero and ensure we still get one
                         riak_core_bg_manager:set_token_rate(token_a, {1,0}),
                         ok = riak_core_bg_manager:get_token(token_a),

                         %% reduce lock limit to zero and ensure we still get one
                         riak_core_bg_manager:set_concurrency_limit(lock_a, 0),
                         {ok, _Ref1} = riak_core_bg_manager:get_lock(lock_a),

                         %% even if globally disabled, we get a lock
                         riak_core_bg_manager:disable(),
                         {ok, _Ref2} = riak_core_bg_manager:get_lock(lock_a),

                         %% turn it back on
                         ?BG_MGR:bypass(false),
                         ?BG_MGR:enable(),
                         ?assertEqual(max_concurrency, ?BG_MGR:get_lock(lock_a))
                 end}

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
    ?BG_MGR:start(),
    timer:sleep(100).

kill_bg_mgr() ->
    Pid = erlang:whereis(?BG_MGR),
    ?assertNot(Pid == undefined),
    erlang:exit(Pid, kill).

crash_and_restart_token_manager() ->
    kill_bg_mgr(),
    timer:sleep(100),
    start_bg_mgr(),
    timer:sleep(100).

-endif.
