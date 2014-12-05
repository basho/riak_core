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
%% members
%%
%% The "old" command implementations for join, leave, plan, commit, 
%% etc are in `riak_core_console.erl'
%%
%% @TODO: Move implementation of old commands here?

-module(riak_core_cluster_cli).

-behaviour(riak_cli_handler).

-export([
    register_cli/0,
    status/2,
    partition_count/2,
    partitions/2,
    partition/2,
    members/2
]).

register_cli() ->
    register_all_usage(),
    register_all_commands().

register_all_commands() ->
    lists:foreach(fun(Args) -> apply(riak_cli, register_command, Args) end,
                  [status_register(), partition_count_register(),
                   partitions_register(), partition_register(),
                   members_register()
                  ]).

register_all_usage() ->
    lists:foreach(fun(Args) -> apply(riak_cli, register_usage, Args) end,
                  [status_usage(), partition_count_usage(),
                   partitions_usage(), partition_usage(),
                   members_usage()
                  ]).

%%%
%% Cluster status
%%%

status_register() ->
    [
     ["riak-admin", "cluster", "status"], % Cmd
     [],                                  % KeySpecs
     [],                                  % FlagSpecs
     fun status/2                         % Implementation callback
    ].

status_usage() ->
    Text = [
            "riak-admin cluster status\n\n",
            "Present a summary of cluster status information."
    ],
    [
     ["riak-admin", "cluster", "status"],
     Text
    ].

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

status([], []) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    RingStatus = riak_core_status:ring_status(),
    %% {Claimant, RingReady, Down, MarkedDown, Changes} = RingStatus
    %%
    %% Group like statuses together
    AllStatus = lists:keysort(2, riak_core_ring:all_member_status(Ring)),

    Rows = [ format_status(Node, Status, Ring, RingStatus) ||
      {Node, Status} <- AllStatus ],

    Table = riak_cli_status:table(Rows),

    T0 = riak_cli_status:text("---- Cluster Status ----"),
    T1 = riak_cli_status:text(io_lib:format("Ring ready: ~p~n", [element(2, RingStatus)])),
    T2 = riak_cli_status:text(
           "Key: (C) = Claimant; availability marked with '!' is unexpected"),
    Out = riak_cli_status:alert([T0,T1,Table,T2]),
    riak_cli:print([Out]).

format_status(Node, Status, Ring, RingStatus) ->
    {Claimant, _RingReady, Down, MarkedDown, Changes} = RingStatus,
    [{node, is_claimant(Node, Claimant)},
     {status, Status},
     {avail, node_availability(Node, Down, MarkedDown)},
     {ring, claim_percent(Ring, Node)},
     {pending, future_claim_percentage(Changes, Ring, Node)}
    ].

is_claimant(Node, Node) ->
    " (C) " ++ atom_to_list(Node) ++ " ";
is_claimant(Node, _Other) ->
    " " ++ atom_to_list(Node) ++ " ".

node_availability(Node, Down, MarkedDown) ->
    case {lists:member(Node, Down), lists:member(Node, MarkedDown)} of
        {false, false} -> "  up   "; % put common case at the top
          {true, true} -> " down  ";
         {true, false} -> " down! ";
         {false, true} -> "  up!  "
    end.

%%%
%% cluster partition-count
%%%

partition_count_register() ->
    [
     ["riak-admin", "cluster", "partition-count"], % Cmd
     [],                                           % KeySpecs
     [{node, [{shortname, "n"}, {longname, "node"}, 
              {typecast, fun list_to_atom/1}]}],   % FlagSpecs
     fun partition_count/2                         % Implementation callback
    ].

partition_count_usage() ->
   Text = [
            "riak-admin cluster partition-count [--node node]\n\n",
            "Returns the number of partitions (ring-size) for the entire\n",
            "cluster or the number of partitions on a specific node."
    ],
    [
     ["riak-admin", "cluster", "partition-count"],
     Text
    ].

partition_count([], [{node, Node}]) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices = riak_core_ring:indices(Ring, Node),
    Row = [[{node, Node}, {partitions, length(Indices)}, {pct, claim_percent(Ring, Node)}]],
    Table = [riak_cli_status:table(Row)],
    riak_cli:print(Table);
partition_count([], []) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    T = riak_cli_status:text(
         io_lib:format("Cluster-wide partition-count: ~p", 
                       [riak_core_ring:num_partitions(Ring)])),
    riak_cli:print([T]).

%%%
%% cluster partitions
%%%

partitions_register() ->
    [
     ["riak-admin", "cluster", "partitions"],      % Cmd
     [],                                           % KeySpecs
     [{node, [{shortname, "n"}, {longname, "node"},
              {typecast, fun list_to_atom/1}]}],   % FlagSpecs
     fun partitions/2                              % Implementation callback
    ].


partitions_usage() ->
   Text = [
            "riak-admin cluster partitions [--node node]\n\n",
            "Returns the partitions which belong to the current node,\n",
            "or to a specific node."
    ],
    [
     ["riak-admin", "cluster", "partitions"],
     Text
    ].

partitions([], [{node, Node}]) ->
    partitions_output(Node);
partitions([], []) ->
    partitions_output(node()).

partitions_output(Node) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    RingSize = riak_core_ring:num_partitions(Ring),
    {Primary, Secondary, Stopped} = riak_core_status:partitions(Node, Ring),
    T0 = riak_cli_status:text(io_lib:format("Partitions owned by ~p:", [Node])),
    Rows = generate_rows(RingSize, primary, Primary) 
           ++ generate_rows(RingSize, secondary, Secondary)
           ++ generate_rows(RingSize, stopped, Stopped),
    Table = riak_cli_status:table(Rows),
    Alert = riak_cli_status:alert([T0, Table]),
    riak_cli:print([Alert]).

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
    [
     ["riak-admin", "cluster", "partition"],        % Cmd
     [
      {id,    [{typecast, fun list_to_integer/1}]},
      {index, [{typecast, fun list_to_integer/1}]}
     ],                                             % KeySpecs
     [],                                            % FlagSpecs
     fun partition/2                                % Implementation callback
    ].

partition_usage() ->
    Text = [
            "riak-admin cluster partition id=0\n",
            "riak-admin cluster partition index=22835963083295358096932575511191922182123945984\n\n",
            "Returns the id or index for the specified index or id.\n"
    ],
    [
     ["riak-admin", "cluster", "partition"],
     Text
    ].

partition([{index, Index}], []) when Index >= 0 ->
    id_out(index, Index);
partition([{id, Id}], []) when Id >= 0 ->
    id_out(id, Id);
partition([{Op, Value}], []) ->
    {error, ["The given value ", integer_to_list(Value), 
             " for ", atom_to_list(Op), " is invalid\n"]}.

id_out(InputType, Number) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    RingSize = riak_core_ring:num_partitions(Ring),
    id_out1(InputType, Number, Ring, RingSize).

%% XXX FIXME: Validation of id - test for boundary condition and that it's lower
%% than the ringsize - 1 partition id
id_out1(index, Index, Ring, RingSize) ->
    Owner = riak_core_ring:index_owner(Ring, Index),
    T = riak_cli_status:text(
          io_lib:format("Partition index: ~p -> id: ~p~n\t(owner: ~p)", 
              [Index, hash_to_partition_id(Index, RingSize), Owner])),
    riak_cli:print([T]);

id_out1(id, Id, Ring, RingSize) when Id < RingSize ->
    Idx = partition_id_to_hash(Id, RingSize),
    Owner = riak_core_ring:index_owner(Ring, Idx),
    T = riak_cli_status:text(
          io_lib:format("Partition id: ~p -> index: ~p~n\t(owner: ~p)~n", 
              [Id, partition_id_to_hash(Id, RingSize), Owner])),
    riak_cli:print([T]);
id_out1(id, Id, _Ring, _RingSize) ->
    {error, ["Id ", Id, " is invalid."]}.

%%%
%% cluster members
%%%

members_register() ->
    [
     ["riak-admin", "cluster", "members"],       % Cmd
     [],                                         % KeySpecs
     [{all, [{shortname, "a"}, 
             {longname, "all"}]}],               % FlagSpecs
     fun members/2                               % Implementation callback
    ].

members_usage() ->
    Text = [
            "riak-admin cluster members [--all]\n\n",
            "Returns node names for all valid members in the cluster.\n",
            "If you want *all* members regardless of status, give the\n",
            "'--all' flag.\n"
    ],
    [
     ["riak-admin", "cluster", "members"],
     Text
    ].

members([], [{all, _Value}]) ->
    member_output(get_status());
members([], []) ->
    member_output(
      [ {Node, Status} || {Node, Status} <- get_status(), Status =:= valid ]
    ).

member_output(L) ->
    O = [ riak_cli_status:text(atom_to_list(N)) || {N, _S} <- L ],
    riak_cli:print(O).

get_status() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    lists:keysort(2, riak_core_ring:all_member_status(Ring)).

%%% FIXME! -> REMOVE AFTER MERGE
%%% Code depends on commit 0b8a86 for riak_core_ring_util.erl
hash_to_partition_id(CHashKey, RingSize) when is_binary(CHashKey) ->
    <<CHashInt:160/integer>> = CHashKey,
    hash_to_partition_id(CHashInt, RingSize);
hash_to_partition_id(CHashInt, RingSize) ->
    CHashInt div chash:ring_increment(RingSize).

partition_id_to_hash(Id, RingSize) ->
    Id * chash:ring_increment(RingSize).
