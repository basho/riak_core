%% -------------------------------------------------------------------
%%
%% Copyright (c) 2011 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_core_console).
-export([member_status/1, ring_status/1]).

member_status([]) ->
    io:format("~33..=s Membership ~34..=s~n", ["", ""]),
    io:format("Status     Ring    Pending    Node~n"),
    io:format("~79..-s~n", [""]),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    AllStatus = lists:keysort(2, riak_core_ring:all_member_status(Ring)),
    RingSize = riak_core_ring:num_partitions(Ring),
    IsPending = ([] /= riak_core_ring:pending_changes(Ring)),

    {Joining, Valid, Down, Leaving, Exiting} =
        lists:foldl(fun({Node, Status},
                        {Joining0, Valid0, Down0, Leaving0, Exiting0}) ->
                            Indices = riak_core_ring:indices(Ring, Node),
                            NextIndices =
                                riak_core_ring:future_indices(Ring, Node),
                            RingPercent = length(Indices) * 100 / RingSize,
                            NextPercent = length(NextIndices) * 100 / RingSize,

                            StatusOut =
                                case riak_core_gossip:legacy_gossip(Node) of
                                    true -> "(legacy)";
                                    false -> Status
                                end,
                            
                            case IsPending of
                                true ->
                                    io:format("~-8s  ~5.1f%    ~5.1f%    ~p~n",
                                              [StatusOut, RingPercent,
                                               NextPercent, Node]);
                                false ->
                                    io:format("~-8s  ~5.1f%      --      ~p~n",
                                              [StatusOut, RingPercent, Node])
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

ring_status([]) ->
    case riak_core_gossip:legacy_gossip() of
        true ->
            io:format("Currently in legacy gossip mode.~n"),
            ok;
        false ->
            {Claimant, RingReady, Down, MarkedDown, Changes} =
                riak_core_status:ring_status(),
            claimant_status(Claimant, RingReady),
            ownership_status(Down, Changes),
            unreachable_status(Down -- MarkedDown),
            ok
    end.

claimant_status(Claimant, RingReady) ->
    io:format("~34..=s Claimant ~35..=s~n", ["", ""]),
    io:format("Claimant:  ~p~n", [Claimant]),
    case RingReady of
        undefined ->
            io:format("Status:     down~n"
                      "Ring Ready: unknown~n", []);
        _ ->
            io:format("Status:     up~n"
                      "Ring Ready: ~p~n", [RingReady])
    end,
    io:format("~n", []).

ownership_status(Down, Changes) ->
    io:format("~30..=s Ownership Handoff ~30..=s~n", ["", ""]),
    case Changes of
        [] ->
            io:format("No pending changes.~n");
        _ ->
            orddict:fold(fun print_ownership_status/3, Down, Changes)
    end,
    io:format("~n", []).

print_ownership_status({Owner, NextOwner}, Transfers, Down) ->
    io:format("Owner:      ~s~n"
              "Next Owner: ~s~n", [Owner, NextOwner]),
    case {lists:member(Owner, Down),
          lists:member(NextOwner, Down)} of
        {true, true} ->
            io:format("~n"),
            io:format("!!! ~s is DOWN !!!~n", [Owner]),
            io:format("!!! ~s is DOWN !!!~n~n", [NextOwner]),
            lists:foreach(fun print_index/1, Transfers);
        {true, _} ->
            io:format("~n"),
            io:format("!!! ~s is DOWN !!!~n~n", [Owner]),
            lists:foreach(fun print_index/1, Transfers);
        {_, true} ->
            io:format("~n"),
            io:format("!!! ~s is DOWN !!!~n~n", [NextOwner]),
            lists:foreach(fun print_index/1, Transfers);
        _ ->
            lists:foreach(fun print_transfer_status/1, Transfers)
    end,
    io:format("~n"),
    io:format("~79..-s~n", [""]),
    Down.

print_index({Idx, _Waiting, _Complete, _Status}) ->
    io:format("Index: ~b~n", [Idx]).

print_transfer_status({Idx, Waiting, Complete, Status}) ->
    io:format("~nIndex: ~b~n", [Idx]),
    case Status of
        complete ->
            io:format("  All transfers complete. Waiting for "
                      "claimant to change ownership.~n");
        awaiting ->
            io:format("  Waiting on: ~p~n", [Waiting]),
            case Complete of
                [] ->
                    ok;
                _ ->
                    io:format("  Complete:   ~p~n", [Complete])
            end
    end.

unreachable_status([]) ->
    io:format("~30..=s Unreachable Nodes ~30..=s~n", ["", ""]),
    io:format("All nodes are up and reachable~n", []),
    io:format("~n", []);
unreachable_status(Down) ->
    io:format("~30..=s Unreachable Nodes ~30..=s~n", ["", ""]),
    io:format("The following nodes are unreachable: ~p~n", [Down]),
    io:format("~n", []),
    io:format("WARNING: The cluster state will not converge until all nodes~n"
              "are up. Once the above nodes come back online, convergence~n"
              "will continue. If the outages are long-term or permanent, you~n"
              "can either mark the nodes as down (riak-admin down NODE) or~n"
              "forcibly remove the nodes from the cluster (riak-admin~n"
              "force-remove NODE) to allow the remaining nodes to settle.~n"),
    ok.
