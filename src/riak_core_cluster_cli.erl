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

-export([
    register_all_commands/0,
    register_all_usage/0,
    status/2%,
%    partition_count/2,
%    partitions/2,
%    partition_id/2,
%    partition_index/2,
%    members/2
]).

register_all_commands() ->
    lists:foreach(fun(Args) -> apply(riak_cli, register_command, Args) end,
                  [status_register()%, partition_count_register(),
%                   partitions_register(), partition_id_register(),
%                   partition_index_register(), members_register()
                  ]).

register_all_usage() ->
    lists:foreach(fun(Args) -> apply(riak_cli, register_usage, Args) end,
                  [status_usage()%, partition_count_usage(),
                   %partitions_usage(), partition_id_usage(),
                   %partition_index_usage(), members_usage()
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
            "Some more text here"
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

    Table = [riak_cli_status:table(Rows)],

    io:format("---- Cluster Status ----~n~n", []),
    io:format("Ring ready: ~p", [element(2, RingStatus)]),
    riak_cli:print(Table),
    io:format("~nKey: (C) = Claimant; availability marked with '!' is unexpected~n~n", []).

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

