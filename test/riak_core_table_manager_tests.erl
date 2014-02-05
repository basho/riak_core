-module(riak_core_table_manager_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

start_test() ->
    Got = riak_core_table_manager:start_link([]),
    ?assertMatch({ok, _}, Got),
    {ok, Pid} = Got,
    unlink(Pid),
    exit(Pid, kill).

table_claim_test_() ->
    {setup, fun() ->
        {ok, Pid} = riak_core_table_manager:start_link([
            {test_table, [named_table, protected]}
        ]),
        Pid
    end,
    fun(Pid) ->
        unlink(Pid),
        exit(Pid, kill)
    end,
    fun(_Pid) -> [

        {spawn, [

            {"basic claim", fun() ->
                Got = riak_core_table_manager:claim_table(test_table),
                ?assertEqual({ok, {test_table, test_table}}, Got)
            end},

            {"owner is caller of claim", fun() ->
                Owner = ets:info(test_table, owner),
                ?assertEqual(self(), Owner)
            end},

            {"heir is correct", fun() ->
                Heir = ets:info(test_table, heir),
                ?assertEqual(whereis(riak_core_table_manager), Heir)
            end}

        ]},

        {spawn, [

            {"basic re-claim", fun() ->
                timer:sleep(500),
                Got = riak_core_table_manager:claim_table(test_table),
                ?assertEqual({ok, {test_table, test_table}}, Got)
            end},

            {"owner is new caller of claim", fun() ->
                Info = ets:info(test_table),
                Owner = proplists:get_value(owner, Info),
                ?assertEqual(self(), Owner)
            end}
        ]}

    ] end}.

-endif.
