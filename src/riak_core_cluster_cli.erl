%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%% @doc This module encapsulates the command line interface for the
%% "new" cluster commands:
%%
%% status, partition-count, partitions, partition-id, partition-index
%%
%% The "old" command implementations for join, leave, plan, commit,
%% etc are in `riak_core_console.erl'

-module(riak_core_cluster_cli).

-behaviour(clique_handler).

-export([
    register_cli/0,
    status/3,
    partition_count/3,
    partitions/3,
    partition/3
]).

register_cli() ->
    register_all_usage(),
    register_all_commands().

register_all_usage() ->
    clique:register_usage(["riak-admin", "cluster"], cluster_usage()),
    clique:register_usage(["riak-admin", "cluster", "status"], status_usage()),
    clique:register_usage(["riak-admin", "cluster", "partition"], partition_usage()),
    clique:register_usage(["riak-admin", "cluster", "partitions"], partitions_usage()),
    clique:register_usage(["riak-admin", "cluster", "partition_count"], partition_count_usage()).

register_all_commands() ->
    lists:foreach(fun(Args) -> apply(clique, register_command, Args) end,
                  [status_register(), partition_count_register(),
                   partitions_register(), partition_register()]).

%%%
%% Cluster status
%%%

status_register() ->
    [["riak-admin", "cluster", "status"], % Cmd
     [],                                  % KeySpecs
     [],                                  % FlagSpecs
     fun status/3].                       % Implementation callback.

cluster_usage() ->
    [
     "riak-admin cluster <sub-command>\n\n",
     "  Display cluster-related status and settings.\n\n",
     "  Sub-commands:\n",
     "    status           Display a summary of cluster status\n",
     "    partition        Map partition IDs to indexes\n",
     "    partitions       Display partitions on a node\n",
     "    partition-count  Display ring size or node partition count\n\n",
     "  Use --help after a sub-command for more details.\n"
    ].

status_usage() ->
    ["riak-admin cluster status\n\n",
     "  Display a summary of cluster status information.\n"].

future_claim_percentage([], _Ring, _Node) ->
    "--";
future_claim_percentage(_Changes, Ring, Node) ->
    FutureRingSize = riak_core_ring:future_num_partitions(Ring),
    NextIndices = riak_core_ring:future_indices(Ring, Node),
    io_lib:format("~5.1f", [length(NextIndices) * 100 / FutureRingSize]).

claim_percent(Ring, Node) ->
    RingSize = riak_core_ring:num_partitions(Ring),
    Indices = riak_core_ring:indices(Ring, Node),
    io_lib:format("~5.1f", [length(Indices) * 100 / RingSize]).

status(_CmdBase, [], []) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    RingStatus = riak_core_status:ring_status(),
    %% {Claimant, RingReady, Down, MarkedDown, Changes} = RingStatus
    %%
    %% Group like statuses together
    AllStatus = lists:keysort(2, riak_core_ring:all_member_status(Ring)),

    Rows = [ format_status(Node, Status, Ring, RingStatus) ||
      {Node, Status} <- AllStatus ],

    Table = clique_status:table(Rows),

    T0 = clique_status:text("---- Cluster Status ----"),
    T1 = clique_status:text(io_lib:format("Ring ready: ~p~n", [element(2, RingStatus)])),
    T2 = clique_status:text(
           "Key: (C) = Claimant; availability marked with '!' is unexpected"),
    [T0,T1,Table,T2].

format_status(Node, Status, Ring, RingStatus) ->
    {Claimant, _RingReady, Down, MarkedDown, Changes} = RingStatus,
    [{node, is_claimant(Node, Claimant)},
     {status, Status},
     {avail, node_availability(Node, Down, MarkedDown)},
     {ring, claim_percent(Ring, Node)},
     {pending, future_claim_percentage(Changes, Ring, Node)}].

is_claimant(Node, Node) ->
    " (C) " ++ atom_to_list(Node) ++ " ";
is_claimant(Node, _Other) ->
    "     " ++ atom_to_list(Node) ++ " ".

node_availability(Node, Down, MarkedDown) ->
    case {lists:member(Node, Down), lists:member(Node, MarkedDown)} of
         {false, false} -> "  up   ";
         {true,  true } -> " down  ";
         {true,  false} -> " down! ";
         {false, true } -> "  up!  "
    end.

%%%
%% cluster partition-count
%%%

partition_count_register() ->
    [["riak-admin", "cluster", "partition-count"], % Cmd
     [],                                           % KeySpecs
     [{node, [{shortname, "n"}, {longname, "node"},
              {typecast,
               fun clique_typecast:to_node/1}]}],% FlagSpecs
     fun partition_count/3].                       % Implementation callback

partition_count_usage() ->
    ["riak-admin cluster partition-count [--node node]\n\n",
     "  Display the number of partitions (ring-size) for the entire\n",
     "  cluster or the number of partitions on a specific node.\n\n",
     "Options\n",
     "  -n <node>, --node <node>\n",
     "      Display the handoffs on the specified node.\n",
     "      This flag can currently take only one node and be used once\n"
    ].

partition_count(_CmdBase, [], [{node, Node}]) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices = riak_core_ring:indices(Ring, Node),
    Row = [[{node, Node}, {partitions, length(Indices)}, {pct, claim_percent(Ring, Node)}]],
    [clique_status:table(Row)];
partition_count(_CmdBase, [], []) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    [clique_status:text(
         io_lib:format("Cluster-wide partition-count: ~p",
                       [riak_core_ring:num_partitions(Ring)]))].

%%%
%% cluster partitions
%%%

partitions_register() ->
    [["riak-admin", "cluster", "partitions"],      % Cmd
     [],                                           % KeySpecs
     [{node, [{shortname, "n"}, {longname, "node"},
              {typecast,
               fun clique_typecast:to_node/1}]}],% FlagSpecs
     fun partitions/3].                            % Implementation callback


partitions_usage() ->
    ["riak-admin cluster partitions [--node node]\n\n",
     "  Display the partitions on a node. Defaults to local node.\n\n",
     "Options\n",
     "  -n <node>, --node <node>\n",
     "      Display the handoffs on the specified node.\n",
     "      This flag can currently take only one node and be used once\n"
    ].

partitions(_CmdBase, [], [{node, Node}]) ->
    partitions_output(Node);
partitions(_CmdBase, [], []) ->
    partitions_output(node()).

partitions_output(Node) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    RingSize = riak_core_ring:num_partitions(Ring),
    {Primary, Secondary, Stopped} = riak_core_status:partitions(Node, Ring),
    T0 = clique_status:text(io_lib:format("Partitions owned by ~p:", [Node])),
    Rows = generate_rows(RingSize, primary, Primary)
           ++ generate_rows(RingSize, secondary, Secondary)
           ++ generate_rows(RingSize, stopped, Stopped),
    Table = clique_status:table(Rows),
    [T0, Table].

generate_rows(_RingSize, Type, []) ->
    [[{type, Type}, {index, "--"}, {id, "--"}]];
generate_rows(RingSize, Type, Ids) ->
    %% Build a list of proplists, one for each partition id
    [
      [ {type, Type}, {index, I},
        {id, hash_to_partition_id(I, RingSize)} ]
    || I <- Ids ].

%%%
%% cluster partition id=0
%% cluster partition index=576460752303423500
%%%

partition_register() ->
    [["riak-admin", "cluster", "partition"],         % Cmd
     [{id,    [{typecast, fun list_to_integer/1}]},
      {index, [{typecast, fun list_to_integer/1}]}], % KeySpecs
     [],                                             % FlagSpecs
     fun partition/3].                               % Implementation callback

partition_usage() ->
    ["riak-admin cluster partition id=0\n",
     "riak-admin cluster partition index=228359630832953580969325755111919221821\n\n",
     "  Display the id for the provided index, or index for the ",
     "specified id.\n"].

partition(_CmdBase, [{index, Index}], []) when Index >= 0 ->
    id_out(index, Index);
partition(_CmdBase, [{id, Id}], []) when Id >= 0 ->
    id_out(id, Id);
partition(_CmdBase, [{Op, Value}], []) ->
    [make_alert(["ERROR: The given value ", integer_to_list(Value),
                " for ", atom_to_list(Op), " is invalid."])];
partition(_CmdBase, [], []) ->
    clique_status:usage().

id_out(InputType, Number) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    RingSize = riak_core_ring:num_partitions(Ring),
    [id_out1(InputType, Number, Ring, RingSize)].

id_out1(index, Index, Ring, RingSize) ->
    case riak_core_ring_util:hash_is_partition_boundary(Index, RingSize) of
        true ->
            Owner = riak_core_ring:index_owner(Ring, Index),
            clique_status:table([
                [{index, Index}, 
                 {id, hash_to_partition_id(Index, RingSize)},
                 {node, Owner}]]);
        false ->
            make_alert(["ERROR: Index ", integer_to_list(Index),
                        " isn't a partition boundary value."])
    end;
id_out1(id, Id, Ring, RingSize) when Id < RingSize ->
    Idx = partition_id_to_hash(Id, RingSize),
    Owner = riak_core_ring:index_owner(Ring, Idx),
    clique_status:table([[{index, Idx}, {id, Id}, {node, Owner}]]);
id_out1(id, Id, _Ring, _RingSize) ->
    make_alert(["ERROR: Id ", integer_to_list(Id), " is invalid."]).

%%%
%% Internal
%%%

make_alert(Iolist) ->
    Text = [clique_status:text(Iolist)],
    clique_status:alert(Text).

hash_to_partition_id(Hash, RingSize) ->
    riak_core_ring_util:hash_to_partition_id(Hash, RingSize).

partition_id_to_hash(Id, RingSize) ->
    riak_core_ring_util:partition_id_to_hash(Id, RingSize).
    
