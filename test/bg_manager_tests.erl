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
                     riak_core_table_manager:start_link([{?BG_ETS_TABLE, ?BG_ETS_OPTS}]),
                     start_bg_mgr()
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

                { "head - empty",
                  fun() ->
                          %% we haven't taken a sample yet, so history is empty
                          Hist = ?BG_MGR:head(),
                          ?assertEqual([], Hist)
                  end},

                { "no locks given",
                  fun() ->
                          Given = ?BG_MGR:locks_held(),
                          ?assertEqual([], Given)
                  end},

                { "verify registered tokens",
                  fun() ->
                          Tokens = registered_token_names(),
                          ?assertEqual(lists:sort(expected_token_names()), lists:sort(Tokens))
                  end},
                
                { "lock registration",
                  fun() ->
                          ?assertEqual({unregistered, a}, riak_core_bg_manager:get_lock(a)),
                          ?assertEqual(0, riak_core_bg_manager:set_concurrency_limit(a, 2)),
                          ?assertEqual(2, riak_core_bg_manager:concurrency_limit(a)),
                          ?assertEqual(false, riak_core_bg_manager:concurrency_limit_reached(a)),
                          ?assertEqual(2, riak_core_bg_manager:set_concurrency_limit(a, 3)),
                          ?assertEqual(3, riak_core_bg_manager:concurrency_limit(a)),
                          ?assertEqual(false, riak_core_bg_manager:concurrency_limit_reached(a)),
                          ?assertEqual(3, riak_core_bg_manager:set_concurrency_limit(a, 4))
                  end},

                { "global enable/disable",
                  fun() ->
                          ?BG_MGR:disable(),
                          ?assertEqual(max_concurrency, ?BG_MGR:get_lock(a)),
                          ?BG_MGR:enable(),
                          ?assertEqual(ok, ?BG_MGR:get_lock(a)),
                          ?assertEqual(ok, ?BG_MGR:get_lock(a, self()))
                  end},

                { "locks_held multiple times for same resource ",
                  fun() ->
                          %% lock a has been given twice now
                          ?assertEqual(2, length(?BG_MGR:locks_held(a)))
                  end},

                { "lock set/get concurrency",
                  fun() ->
                          ?assertEqual(0, riak_core_bg_manager:set_concurrency_limit(b, 1)),
                          ?assertEqual(false, riak_core_bg_manager:concurrency_limit_reached(b)),
                          riak_core_bg_manager:get_lock(b),
                          ?assertEqual(true, riak_core_bg_manager:concurrency_limit_reached(b)),
                          ?assertEqual(max_concurrency, riak_core_bg_manager:get_lock(b))
                  end},

                { "lock_info",
                  fun() ->
                          Locks = riak_core_bg_manager:lock_info(),
                          ?assertEqual(2, length(Locks)), %% a and b are registered
                          ?assertEqual({true, 1}, ?BG_MGR:lock_info(b))
                  end},

                { "enable/disable resource",
                  fun() ->
                          ?assertEqual(1, ?BG_MGR:set_concurrency_limit(b, 2)),
                          ?BG_MGR:disable(a),
                          %% make sure b is still enabled
                          ?assertEqual(ok, ?BG_MGR:get_lock(b, self(), [meta_b])),
                          %% a should be disabled now
                          ?assertEqual(max_concurrency, ?BG_MGR:get_lock(a, self(), [meta_a])),
                          ?BG_MGR:enable(a),
                          %% a should be re-enabled now
                          ?assertEqual(ok, ?BG_MGR:get_lock(a, self(), [meta_a]))
                  end},

                { "locks held",
                  fun() ->
                          Held = ?BG_MGR:locks_held(a),
                          ?assertEqual(length(Held), ?BG_MGR:lock_count(a))
                  end},

                { "no tokens given",
                  fun() ->
                          Given = ?BG_MGR:tokens_given(),
                          ?assertEqual([], Given)
                  end},
                
                { "get_token + tokens_given",
                  fun() ->
                          Meta1 = [{foo,bar}],
                          Meta2 = [{moo,tar,blar}],
                          Pid = self(),
                          ok = ?BG_MGR:get_token(token1, Pid, Meta1),
                          ok = ?BG_MGR:get_token(token2, Pid, Meta2),
                          AllGiven = ?BG_MGR:tokens_given(),
                          ?assertEqual(lists:sort([make_live_stat(token1,token,Pid,Meta1,given),
                                                   make_live_stat(token2,token,Pid,Meta2,given)]),
                                       lists:sort(AllGiven)),
                          Given = ?BG_MGR:tokens_given(token1),
                          ?assertEqual([make_live_stat(token1,token,Pid,Meta1,given)], Given)
                  end},

                { "get_token + tokens_given + max_concurrency",
                  fun() ->
                          ?assertNot([] == ?BG_MGR:tokens_given()),
                          %% let tokens refill and confirm available
                          MaxPeriod = (max_token_period() * 1000) + 100,
                          timer:sleep(MaxPeriod),
                          ?assertEqual([], ?BG_MGR:tokens_given()),

                          Pid = self(),
                          DoOne =
                              fun(Token) ->
                                      %% Max out the tokens for this period
                                      N = expected_token_limit(Token),
                                      [ok = ?BG_MGR:get_token(Token,Pid,[{meta,M}])
                                       || M <- lists:seq(1,N)],
                                      Expected = [make_live_stat(Token,token,Pid,[{meta,M}],given)
                                                  || M <- lists:seq(1,N)],
                                      %% try to get another, but it should fail
                                      Result = ?BG_MGR:get_token(Token,Pid,{meta,N+1}),
                                      ?assertEqual(max_concurrency, Result),
                                      Given = ?BG_MGR:tokens_given(Token),
                                      ?assertEqual(Expected, Given)
                              end,
                          %% all of our test token types
                          Tokens = expected_token_names(),
                          %% can get requested and nothing should balk
                          [DoOne(Token) || Token <- Tokens],
                          %% Does the total number of given tokens add up the expected limit?
                          TotalLimits = lists:foldl(fun(Limit,Sum) -> Limit+Sum end, 0,
                                                    [expected_token_limit(Token) || Token <- Tokens]),
                          AllGiven = ?BG_MGR:tokens_given(),
                          ?assertEqual(TotalLimits, erlang:length(AllGiven))
                  end},

                { "get_token_blocking",
                  fun() ->
                          %% all the tokens have been max'd out now.
                          %% start sub-process that will block on a token
                          Meta = [{i,am,blocked}],
                          TestPid = self(),
                          Pid = spawn(fun() ->
                                              TestPid ! {blocking, self()},
                                              ok = ?BG_MGR:get_token_blocking(token1, self(), Meta, ?TIMEOUT),
                                              TestPid ! {unblocked, self()}
                                      end),
                          receive
                              {blocking, Pid} ->
                                  timer:sleep(10),
                                  %% check that we have one on the waiting list
                                  Blocked1 = ?BG_MGR:tokens_blocked(),
                                  ?assertEqual([make_live_stat(token1,token,Pid,Meta,blocked)], Blocked1)
                          end,
                          %% let tokens refill and confirm available and blocked is now given
                          MaxPeriod = (max_token_period() * 1000) + 100,
                          receive
                              {unblocked,Pid} ->
                                  %% check that our token was given and the blocked queue is empty
                                  Blocked2 = ?BG_MGR:tokens_blocked(),
                                  Given2 = ?BG_MGR:tokens_given(token1),
                                  ?assertEqual([make_live_stat(token1,token,Pid,Meta,given)], Given2),
                                  ?assertEqual([],Blocked2)
                          after MaxPeriod ->
                                  ?assert(false)
                          end
                  end},

                { "head - with samples",
                  fun() ->
                          %% 
                          Hist = ?BG_MGR:head(),
                          ?assertNot([] == Hist)
                  end},

                { "clear history",
                  fun() ->
                          ?BG_MGR:clear_history(),
                          Hist = ?BG_MGR:head(),
                          ?assertEqual([], Hist)
                  end},

                { "clear all tokens given and confirm none given",
                  fun() ->
                          %% wait a little longer than longest refill time.
                          MaxPeriod = (max_token_period() * 1000) + 100,
                          timer:sleep(MaxPeriod),
                          %% confirm none given now
                          ?assertEqual([], ?BG_MGR:tokens_given())
                  end},
                
                { "synchronous gets and ps/history",
                  fun() ->
                          Tokens = expected_token_names(),
                          %% clear history and let 1 sample be taken with empty stats
                          %% Sample1: empty
                          ?BG_MGR:clear_history(),
                          E1 = [{Token, make_hist_stat(token,expected_token_limit(Token),1,0,0)}
                                || Token <- Tokens],
                          timer:sleep(1000),
                          %% Sample2: 1 given for each token
                          E2 = [get_n_tokens(Token, self(), 1) || Token <- Tokens],
                          timer:sleep(1000),
                          %% Sample3: max given for each token, plus 2 blocked
                          E3 = [max_out_plus(Token, self(), 2) || Token <- Tokens],

                          %% let all spawned procs run
                          timer:sleep(100),

                          PS = ?BG_MGR:query_resource(all, [given, blocked], [token]),
                          Given = ?BG_MGR:tokens_given(),
                          Blocked = ?BG_MGR:tokens_blocked(),
                          ?assertNot(PS == []),
                          ?assertEqual(PS, Given++Blocked),

                          timer:sleep(1000-100),
                          %% verify history
                          %% TODO: refills is hard to calculate and needs a better solution
                          ?assertEqual([E1,E2,E3], ?BG_MGR:head()),
                          ?assertEqual([E1], ?BG_MGR:head(all,1)),
                          ?assertEqual([E2], ?BG_MGR:head(all,2,1)),  %% head(offset, count)
                          ?assertEqual([E3], ?BG_MGR:head(all,3,1)),
                          ?assertEqual([E1,E2], ?BG_MGR:head(all,2)),
                          ?assertEqual([E2,E3], ?BG_MGR:head(all,2,2)),
                          ?assertEqual([E1,E2,E3], ?BG_MGR:head(all,3)),
                          ?assertEqual([E3], ?BG_MGR:tail(all,1)),
                          ?assertEqual([E2,E3], ?BG_MGR:tail(all,2)),  %% tail(offset)
                          ?assertEqual([E1,E2,E3], ?BG_MGR:tail(all,3)),
                          ?assertEqual([E2,E3], ?BG_MGR:tail(all,2,2)),  %% tail(offset,count)
                          ?assertEqual([E2], ?BG_MGR:tail(all,2,1)),
                          %% test range guards on head/tail
                          ?assertEqual([E1,E2,E3], ?BG_MGR:head(all,0)),
                          ?assertEqual([E1,E2,E3], ?BG_MGR:head(all,-1)),
                          ?assertEqual([E3], ?BG_MGR:tail(all,0)),
                          ?assertEqual([E3], ?BG_MGR:tail(all,-1)),

                          %% calling head on a specific token yields a list of lists,
                          %% but each "of lists" has only one thing in it.
                          [check_head_token(Token, [E1,E2,E3]) || Token <- Tokens]

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

get_n_tokens(Token, Pid, N) ->
    Limit = expected_token_limit(Token),
    [spawn_sync_request(Token,Pid,[{meta,M}]) || M <- lists:seq(1,N)],
    {Token, make_hist_stat(token,Limit, 1, N, 0)}.

max_out_plus(Token, Pid, X) ->
    %% Max out the tokens for this period and request X more than Max
    N = expected_token_limit(Token),
    get_n_tokens(Token, Pid, N+X),
    %% Hack alert: given is N + X becuase we "know" we got a refill in this period.
    {Token, make_hist_stat(token,N, 1, N+X, X)}.

make_hist_stat(Type, Limit, Refills, Given, Blocked) ->
     #bg_stat_hist
        {
          type=Type,
          limit=Limit,
          refills=Refills,
          given=Given,
          blocked=Blocked
        }.
          
make_live_stat(Resource, Type, Pid, Meta, Status) ->
    #bg_stat_live
        {
          resource=Resource,
          type=Type,
          consumer=Pid,
          meta=Meta,
          state=Status
        }.

-spec some_token_rates() -> [{bg_token(), {bg_period(), bg_count()}}].
some_token_rates() ->
    [ {token1, {1, 5}},
      {token2, {1, 4}},
      {{token3,stuff3}, {1, 3}}
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
    ?BG_MGR:start(1).

crash_and_restart_token_manager() ->
    Pid = erlang:whereis(?BG_MGR),
    ?assertNot(Pid == undefined),
    erlang:exit(Pid, kill),
    timer:sleep(100),
    start_bg_mgr(),
    timer:sleep(100).

-endif.
