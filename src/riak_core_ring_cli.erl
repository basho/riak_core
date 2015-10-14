-module(riak_core_ring_cli).
-compile(export_all).

-behavior(clique_handler).
-export([register_cli/0]).

-define(CMD, "riak-admin").

-spec register_cli() -> ok.
register_cli() ->
    register_cli_usage(),
    register_cli_cmds(),
    ok.

register_cli_cmds() ->
    [ apply(clique, register_command, Args)
      || Args <-
         [
          member_status_register()          
         ]], ok.

register_cli_usage() ->
    clique:register_usage([?CMD], base_usage()),
    clique:register_usage([?CMD, "member-status"], member_status_usage()),
    clique:register_usage([?CMD, "service-nodes"], service_nodes_usage()).

%%%
%% Ring status
%%%

status_register() ->
    [["riak-admin", "member-status"], % Cmd
     [],                              % KeySpecs
     [],                              % FlagSpecs
     fun member_status/3].                   % Implementation callback.

base_usage() ->
    [
     "data-platform-admin <command>\n\n",
     "  Commands:\n",
     "    member-status             Check the cluster\n\n",
     "    service-nodes             Display all instances running the designated service\n\n",
     "Use --help after a sub-command for more details.\n"
    ].

member_status_usage() ->
    [
     "riak-admin member-status\n",
     " TODO\n"
    ].

member_status([], [], []) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    print_member_status(Ring).

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

print_member_status(Ring) ->
    io:format("~33..=s Membership ~34..=s~n", ["", ""]),
    io:format("Status     Ring    Pending    Node~n"),
    io:format("~79..-s~n", [""]),
    AllStatus = lists:keysort(2, riak_core_ring:all_member_status(Ring)),
    IsPending = ([] /= riak_core_ring:pending_changes(Ring)),

    {Joining, Valid, Down, Leaving, Exiting} =
        lists:foldl(fun({Node, Status},
                        {Joining0, Valid0, Down0, Leaving0, Exiting0}) ->
                            {RingPercent, NextPercent} =
                                pending_claim_percentage(Ring, Node),

                            case IsPending of
                                true ->
                                    io:format("~-8s  ~5.1f%    ~5.1f%    ~p~n",
                                              [Status, RingPercent,
                                               NextPercent, Node]);
                                false ->
                                    io:format("~-8s  ~5.1f%      --      ~p~n",
                                              [Status, RingPercent, Node])
                            end,
                            case Status of
                                joining ->
                                    {Joining0 + 1, Valid0, Down0, Leaving0, Exiting0};
                                valid ->
                                    {Joining0, Valid0 + 1, Down0, Leaving0, Exiting0};
                                down ->
                                    {Joining0, Valid0, Down0 + 1, Leaving0, Exiting0};
                                leaving ->
                                    {Joining0, Valid0, Down0, Leaving0 + 1, Exiting0};
                                exiting ->
                                    {Joining0, Valid0, Down0, Leaving0, Exiting0 + 1}
                            end
                    end, {0,0,0,0,0}, AllStatus),
    io:format("~79..-s~n", [""]),
    io:format("Valid:~b / Leaving:~b / Exiting:~b / Joining:~b / Down:~b~n",
              [Valid, Leaving, Exiting, Joining, Down]),
    ok.
