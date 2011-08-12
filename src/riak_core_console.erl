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
-compile(export_all).

member_status() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    AllStatus = riak_core_ring:all_member_status(Ring),
    {Active, Leaving, Exiting} =
        lists:foldl(fun({Node, Status}, {Active0, Leaving0, Exiting0}) ->
                            case Status of
                                valid ->
                                    io:format("~p is ACTIVE~n", [Node]),
                                    {Active0 + 1, Leaving0, Exiting0};
                                leaving ->
                                    io:format("~p is LEAVING~n", [Node]),
                                    {Active0, Leaving0 + 1, Exiting0};
                                exiting ->
                                    io:format("~p is EXITING~n", [Node]),
                                    {Active0, Leaving0, Exiting0 + 1}
                            end
                    end, {0,0,0}, AllStatus),
    io:format("~79..-s~n", [""]),
    io:format("Active:   ~p~n"
              "Leaving:  ~p~n"
              "Exiting:  ~p~n", [Active, Leaving, Exiting]),
    ok.

ring_status() ->
    %% TODO: Change this for cases where not all nodes in the
    %%       cluster are running the same vnode modules.
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Changes = riak_core_ring:pending_changes(Ring),
    AllMods = [Mod || {_App, Mod} <- riak_core:vnode_modules()],
    io:format("~34..=s Claimant ~35..=s~n", ["", ""]),
    Claimant = riak_core_ring:claimant(Ring),
    io:format("Claimant:  ~p~n", [Claimant]),
    case rpc:call(Claimant, riak_core_ring, ring_ready, [], 5000) of
        {badrpc, _} ->
            io:format("Status:     down~n"
                      "Ring Ready: unknown~n", []);
        RingReady ->
            io:format("Status:     up~n"
                      "Ring Ready: ~p~n", [RingReady])
    end,
    io:format("~n", []),
    io:format("~30..=s Ownership Handoff ~30..=s~n", ["", ""]),
    ring_status(Changes, AllMods).

ring_status([], _) ->
    io:format("No pending changes.~n"),
    ok;
ring_status(Changes, AllMods) ->
    io:format("~-49s ~-14s ~-14s~n", ["Index", "Owner", "NextOwner"]),
    io:format("~79..-s~n", [""]),
    lists:foldl(
      fun({Idx, Owner, NextOwner, Mods, Status}, Acc) ->
              case Status of
                  complete ->
                      io:format("~-49b ~-14s ~-14s~n"
                                "  All transfers complete. Waiting for "
                                "claimant to change ownership.~n",
                                [Idx, Owner, NextOwner]),
                      Acc;
                  awaiting ->
                      Waiting = AllMods -- Mods,
                      io:format("~-49b ~-14s ~-14s~n",
                                [Idx, Owner, NextOwner]),
                      io:format("  Waiting on: ~p~n", [Waiting]),
                      case Mods of
                          [] ->
                              ok;
                                    _ ->
                              io:format("  Complete:   ~p~n", [Mods])
                      end,
                                io:format("~79..-s~n", [""]),
                      Acc
              end
      end, [], Changes),
    ok.

