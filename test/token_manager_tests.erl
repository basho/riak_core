-module(token_manager_tests).
-compile(export_all).

-include_lib("riak_core_token_manager.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(TOK_MGR, riak_core_token_manager).

token_mgr_test_() ->
    {timeout, 60000,  %% Seconds to finish all of the tests
     {setup, fun() ->
                     riak_core_table_manager:start_link([{?TM_ETS_TABLE, ?TM_ETS_OPTS}]),
                     start_token_mgr()
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
                          Hist = ?TOK_MGR:head(),
                          ?assertEqual([], Hist)
                  end},

                { "token types",
                  fun() ->
                          Types = ?TOK_MGR:token_types(),
                          ?assertEqual(lists:sort(expected_token_types()), lists:sort(Types))
                  end},

                { "no tokens given",
                  fun() ->
                          Given = ?TOK_MGR:tokens_given(),
                          ?assertEqual([], Given)
                  end},
                
                { "get_token_sync + tokens_given",
                  fun() ->
                          Meta1 = [{foo,bar}],
                          Meta2 = [{moo,tar,blar}],
                          Pid = self(),
                          ok = ?TOK_MGR:get_token_async(token1, Pid, Meta1),
                          ok = ?TOK_MGR:get_token_async(token2, Pid, Meta2),
                          AllGiven = ?TOK_MGR:tokens_given(),
                          ?assertEqual(lists:sort([make_live_stat(token1, Pid, Meta1, given),
                                                   make_live_stat(token2, Pid, Meta2, given)]),
                                       lists:sort(AllGiven)),
                          Given = ?TOK_MGR:tokens_given(token1),
                          ?assertEqual([make_live_stat(token1, Pid, Meta1, given)], Given)
                  end},

                { "get_token_sync + tokens_given + max_concurrency",
                  fun() ->
                          ?assertNot([] == ?TOK_MGR:tokens_given()),
                          %% let tokens refill and confirm available
                          MaxPeriod = (max_token_period() * 1000) + 100,
                          timer:sleep(MaxPeriod),
                          ?assertEqual([], ?TOK_MGR:tokens_given()),

                          Pid = self(),
                          DoOne =
                              fun(Token) ->
                                      %% Max out the tokens for this period
                                      N = expected_token_limit(Token),
                                      [ok = ?TOK_MGR:get_token_async(Token,Pid,[{meta,M}])
                                       || M <- lists:seq(1,N)],
                                      Expected = [make_live_stat(Token,Pid,[{meta,M}],given)
                                                  || M <- lists:seq(1,N)],
                                      %% try to get another, but it should fail
                                      ?assertEqual(max_tokens, ?TOK_MGR:get_token_async(Token,Pid,{meta,N+1})),
                                      Given = ?TOK_MGR:tokens_given(Token),
                                      ?assertEqual(Expected, Given)
                              end,
                          %% all of our test token types
                          Tokens = expected_token_types(),
                          %% can get requested and nothing should balk
                          [DoOne(Token) || Token <- Tokens],
                          %% Does the total number of given tokens add up the expected limit?
                          TotalLimits = lists:foldl(fun(Limit,Sum) -> Limit+Sum end, 0,
                                                    [expected_token_limit(Token) || Token <- Tokens]),
                          AllGiven = ?TOK_MGR:tokens_given(),
                          ?assertEqual(TotalLimits, erlang:length(AllGiven))
                  end},

                { "get_token_sync blocking",
                  fun() ->
                          %% all the tokens have been max'd out now.
                          %% start sub-process that will block on a token
                          Meta = [{i,am,blocked}],
                          TestPid = self(),
                          Pid = spawn(fun() ->
                                              TestPid ! {blocking, self()},
                                              ok = ?TOK_MGR:get_token_sync(token1, self(), Meta),
                                              TestPid ! {unblocked, self()}
                                      end),
                          receive
                              {blocking, Pid} ->
                                  timer:sleep(10),
                                  %% check that we have one on the waiting list
                                  Blocked1 = ?TOK_MGR:tokens_blocked(),
                                  ?assertEqual([make_live_stat(token1, Pid, Meta, blocked)], Blocked1)
                          end,
                          %% let tokens refill and confirm available and blocked is now given
                          MaxPeriod = (max_token_period() * 1000) + 100,
                          receive
                              {unblocked,Pid} ->
                                  %% check that our token was given and the blocked queue is empty
                                  Blocked2 = ?TOK_MGR:tokens_blocked(),
                                  Given2 = ?TOK_MGR:tokens_given(token1),
                                  ?assertEqual([make_live_stat(token1, Pid, Meta, given)], Given2),
                                  ?assertEqual([],Blocked2)
                          after MaxPeriod ->
                                  ?assert(false)
                          end
                  end},

                { "head - with samples",
                  fun() ->
                          %% 
                          Hist = ?TOK_MGR:head(),
                          ?assertNot([] == Hist)
                  end},

                { "clear history",
                  fun() ->
                          ?TOK_MGR:clear_history(),
                          Hist = ?TOK_MGR:head(),
                          ?assertEqual([], Hist)
                  end},

                { "clear all tokens given and confirm none given",
                  fun() ->
                          %% wait a little longer than longest refill time.
                          MaxPeriod = (max_token_period() * 1000) + 100,
                          timer:sleep(MaxPeriod),
                          %% confirm none given now
                          ?assertEqual([], ?TOK_MGR:tokens_given())
                  end},
                
                { "synchronous gets and ps/history",
                  fun() ->
                          Tokens = expected_token_types(),
                          %% clear history and let 1 sample be taken with empty stats
                          %% Sample1: empty
                          ?TOK_MGR:clear_history(),
                          E1 = [{Token, make_hist_stat(expected_token_limit(Token),1,0,0)} || Token <- Tokens],
                          timer:sleep(1000),
                          %% Sample2: 1 given for each token
                          E2 = [get_n_tokens(Token, self(), 1) || Token <- Tokens],
                          timer:sleep(1000),
                          %% Sample3: max given for each token, plus 2 blocked
                          E3 = [max_out_plus(Token, self(), 2) || Token <- Tokens],

                          %% let all spawned procs run
                          timer:sleep(100),

                          PS = ?TOK_MGR:ps(),
                          Given = ?TOK_MGR:tokens_given(),
                          Blocked = ?TOK_MGR:tokens_blocked(),
                          ?assertNot(PS == []),
                          ?assertEqual(PS, Given++Blocked),

                          timer:sleep(1000-100),
                          %% verify history
                          %% TODO: refills is hard to calculate and needs a better solution
                          ?assertEqual([E1,E2,E3], ?TOK_MGR:head()),
                          ?assertEqual([E1], ?TOK_MGR:head(all,1)),
                          ?assertEqual([E2], ?TOK_MGR:head(all,2,1)),  %% head(offset, count)
                          ?assertEqual([E3], ?TOK_MGR:head(all,3,1)),
                          ?assertEqual([E1,E2], ?TOK_MGR:head(all,2)),
                          ?assertEqual([E2,E3], ?TOK_MGR:head(all,2,2)),
                          ?assertEqual([E1,E2,E3], ?TOK_MGR:head(all,3)),
                          ?assertEqual([E3], ?TOK_MGR:tail(all,1)),
                          ?assertEqual([E2,E3], ?TOK_MGR:tail(all,2)),  %% tail(offset)
                          ?assertEqual([E1,E2,E3], ?TOK_MGR:tail(all,3)),
                          ?assertEqual([E2,E3], ?TOK_MGR:tail(all,2,2)),  %% tail(offset,count)
                          ?assertEqual([E2], ?TOK_MGR:tail(all,2,1)),
                          %% test range guards on head/tail
                          ?assertEqual([E1,E2,E3], ?TOK_MGR:head(all,0)),
                          ?assertEqual([E1,E2,E3], ?TOK_MGR:head(all,-1)),
                          ?assertEqual([E3], ?TOK_MGR:tail(all,0)),
                          ?assertEqual([E3], ?TOK_MGR:tail(all,-1)),

                          %% calling head on a specific token yields a list of lists,
                          %% but each "of lists" has only one thing in it.
                          [check_head_token(Token, [E1,E2,E3]) || Token <- Tokens]

                  end}

              ] end}
    }.

check_head_token(Token, StatsList) ->
    F1 = fun(Stats) -> filter_stat(Token, Stats) end,
    ?assertEqual(?TOK_MGR:head(Token), lists:map(F1, StatsList)).

filter_stat(Token, Stats) ->
    lists:filter(fun({T,_Stat}) -> Token==T end, Stats).

spawn_sync_request(Token, Pid, Meta) ->
    spawn(fun() -> ok = ?TOK_MGR:get_token_sync(Token, Pid, Meta) end).

get_n_tokens(Token, Pid, N) ->
    Limit = expected_token_limit(Token),
    [spawn_sync_request(Token,Pid,[{meta,M}]) || M <- lists:seq(1,N)],
    {Token, make_hist_stat(Limit, 1, N, 0)}.

max_out_plus(Token, Pid, X) ->
    %% Max out the tokens for this period and request X more than Max
    N = expected_token_limit(Token),
    get_n_tokens(Token, Pid, N+X),
    %% Hack alert: given is N + X becuase we "know" we got a refill in this period.
    {Token, make_hist_stat(N, 1, N+X, X)}.

make_hist_stat(Limit, Refills, Given, Blocked) ->
     #tm_stat_hist
        {
          limit=Limit,
          refills=Refills,
          given=Given,
          blocked=Blocked
        }.
          
make_live_stat(Token, Pid, Meta, Status) ->
    #tm_stat_live
        {
          token=Token,
          consumer=Pid,
          meta=Meta,
          state=Status
        }.

-spec some_token_rates() -> [{tm_token(), {tm_period(), tm_count()}}].
some_token_rates() ->
    [ {token1, {1, 5}},
      {token2, {1, 4}},
      {{token3,stuff3}, {1, 3}}
    ].

expected_token_types() ->
    [ Type || {Type, _Rate} <- some_token_rates()].

max_token_period() ->
    lists:foldl(fun({_T,{Period,_Limit}},Max) -> erlang:max(Period,Max) end, 0, some_token_rates()).

expected_token_limit(Token) ->
    {_Period, Limit} = proplists:get_value(Token, some_token_rates()),
    Limit.

setup_token_rates() ->
    [?TOK_MGR:set_token_rate(Type, Rate) || {Type, Rate} <- some_token_rates()].

verify_token_rate(Type, ExpectedRate) ->
    Rate = ?TOK_MGR:token_rate(Type),
    ?assertEqual(ExpectedRate, Rate).

verify_token_rates() ->
    [verify_token_rate(Type, Rate) || {Type, Rate} <- some_token_rates()],
    %% check un-registered token is not setup
    Rate = ?TOK_MGR:token_rate(bogusToken),
    DefaultRate = {0,0},
    ?assertEqual(DefaultRate, Rate).

%% start a stand-alone server, not linked, so that when we crash it, it
%% doesn't take down our test too.
start_token_mgr() ->
    %% setup with history window to 1 seconds
    ?TOK_MGR:start(1).

crash_and_restart_token_manager() ->
    Pid = erlang:whereis(?TOK_MGR),
    ?assertNot(Pid == undefined),
    erlang:exit(Pid, kill),
    timer:sleep(100),
    start_token_mgr(),
    timer:sleep(100).

-endif.
