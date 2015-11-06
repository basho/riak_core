-module(riak_core_ring_cli).

-behavior(clique_handler).
-export([
         register_cli/0,
         member_status/3
        ]).

-spec register_cli() -> ok.
register_cli() ->
    register_cli_usage(),
    register_cli_cmds(),
    ok.

register_cli_cmds() ->
    lists:foreach(fun(Args) -> apply(clique, register_command, Args) end,
                  [ member_status_register() ]).

register_cli_usage() ->
    clique:register_usage(["riak-admin", "member-status"], member_status_usage()).

%%%
%% Ring status
%%%

member_status_register() ->
    [["riak-admin", "member-status"], % Cmd
     [],                              % KeySpecs
     [],                              % FlagSpecs
     fun member_status/3].            % Implementation callback.

member_status_usage() ->
    [
     "riak-admin member-status\n",
     "    Display the cluster status and member counts by node status.\n"
    ].

member_status(_Cmd, [], []) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    AllStatus = lists:keysort(2, riak_core_ring:all_member_status(Ring)),
    IsPending = ([] /= riak_core_ring:pending_changes(Ring)),

    {Joining, Valid, Down, Leaving, Exiting, Rows} =
        lists:foldl(fun({Node, Status},
                        {Joining0, Valid0, Down0, Leaving0, Exiting0, Rows0}) ->
                            {RingPercent, NextPercent} =
                                pending_claim_percentage(Ring, Node),
                            Pending =
                            case IsPending of
                                true ->
                                    {pending, NextPercent};
                                false ->
                                    {pending, "--"}
                            end,
                            Row =
                                [{status, Status},
                                 {node, Node},
                                 {ring, RingPercent},
                                 Pending],
                            case Status of
                                joining ->
                                    {Joining0 + 1, Valid0, Down0, Leaving0, Exiting0, Rows0 ++ [Row]};
                                valid ->
                                    {Joining0, Valid0 + 1, Down0, Leaving0, Exiting0, Rows0 ++ [Row]};
                                down ->
                                    {Joining0, Valid0, Down0 + 1, Leaving0, Exiting0, Rows0 ++ [Row]};
                                leaving ->
                                    {Joining0, Valid0, Down0, Leaving0 + 1, Exiting0, Rows0 ++ [Row]};
                                exiting ->
                                    {Joining0, Valid0, Down0, Leaving0, Exiting0 + 1, Rows0 ++ [Row]}
                            end
                    end, { 0,0,0,0,0, []}, AllStatus),

    Header = clique_status:text("---- Ring Status ----"),
    CountsRows = [[{valid, Valid}, {leaving, Leaving}, {exiting, Exiting},
                  {joining, Joining}, {down, Down}]],
    CountsTable = clique_status:table(CountsRows),
    NodeTable = clique_status:table(Rows),
    [Header, NodeTable, CountsTable].

%% @doc Return for a given ring and node, percentage currently owned and
%% anticipated after the transitions have been completed.
pending_claim_percentage(Ring, Node) ->
    RingSize = riak_core_ring:num_partitions(Ring),
    FutureRingSize = riak_core_ring:future_num_partitions(Ring),
    Indices = riak_core_ring:indices(Ring, Node),
    NextIndices = riak_core_ring:future_indices(Ring, Node),
    RingPercent = length(Indices) * 100 / RingSize,
    NextPercent = length(NextIndices) * 100 / FutureRingSize,
    {RingPercent, NextPercent}.
