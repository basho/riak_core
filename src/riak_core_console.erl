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
-export([member_status/1, ring_status/1, print_member_status/2,
         stage_leave/1, stage_remove/1, stage_replace/1, stage_resize_ring/1,
         stage_force_replace/1, print_staged/1, commit_staged/1,
         clear_staged/1, transfer_limit/1, pending_claim_percentage/2,
         transfers/1, add_user/1, add_source/1, grant/1, revoke/1,
         print_users/1, print_user/1, print_sources/1, ciphers/1]).

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

member_status([]) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    print_member_status(Ring, legacy_gossip(Ring)).

legacy_gossip(Ring) ->
    Members = riak_core_ring:all_members(Ring),
    LegacyGossip =
        [{Node, riak_core_gossip:legacy_gossip(Node)} || Node <- Members],
    orddict:from_list(LegacyGossip).

print_member_status(Ring, LegacyGossip) ->
    io:format("~33..=s Membership ~34..=s~n", ["", ""]),
    io:format("Status     Ring    Pending    Node~n"),
    io:format("~79..-s~n", [""]),
    AllStatus = lists:keysort(2, riak_core_ring:all_member_status(Ring)),
    IsPending = ([] /= riak_core_ring:pending_changes(Ring)),

    {Joining, Valid, Down, Leaving, Exiting} =
        lists:foldl(fun({Node, Status},
                        {Joining0, Valid0, Down0, Leaving0, Exiting0}) ->
                            StatusOut =
                                case orddict:fetch(Node, LegacyGossip) of
                                    true -> "(legacy)";
                                    false -> Status
                                end,

                            {RingPercent, NextPercent} =
                                pending_claim_percentage(Ring, Node),

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

%% Provide a list of nodes with pending partition transfers (i.e. any secondary vnodes)
%% and list any owned vnodes that are *not* running
-spec(transfers([string()]) -> ok).
transfers([]) ->
    try
        {DownNodes, Pending} = riak_core_status:transfers(),
        case DownNodes of
            [] -> ok;
            _  -> io:format("Nodes ~p are currently down.\n", [DownNodes])
        end,
        F = fun({waiting_to_handoff, Node, Count}, Acc) ->
                io:format("~p waiting to handoff ~p partitions\n", [Node, Count]),
                Acc + 1;
            ({stopped, Node, Count}, Acc) ->
                io:format("~p does not have ~p primary partitions running\n", [Node, Count]),
                Acc + 1
        end,
        case lists:foldl(F, 0, Pending) of
            0 ->
                io:format("No transfers active\n"),
                ok;
            _ ->
                error
        end
    catch
        Exception:Reason ->
            lager:error("Transfers failed ~p:~p", [Exception,
                    Reason]),
            io:format("Transfers failed, see log for details~n"),
            error
    end,

    %% Now display active transfers
    {Xfers, Down} = riak_core_status:all_active_transfers(),

    DisplayXfer =
        fun({{Mod, Partition}, Node, outbound, active, _Status}) ->
                print_v1_status(Mod, Partition, Node);

           ({status_v2, Status}) ->
                %% Display base status
                Type = proplists:get_value(type, Status),
                Mod = proplists:get_value(mod, Status),
                SrcPartition = proplists:get_value(src_partition, Status),
                TargetPartition = proplists:get_value(target_partition, Status),
                StartTS = proplists:get_value(start_ts, Status),
                SrcNode = proplists:get_value(src_node, Status),
                TargetNode = proplists:get_value(target_node, Status),

                print_v2_status(Type, Mod, {SrcPartition, TargetPartition}, StartTS),

                %% Get info about stats if there is any yet
                Stats = proplists:get_value(stats, Status),

                print_stats(SrcNode, TargetNode, Stats),
                io:format("~n");

           (_) ->
                ignore
        end,
    DisplayDown =
        fun(Node) ->
                io:format("Node ~p could not be contacted~n", [Node])
        end,

    io:format("~nActive Transfers:~n~n", []),
    [DisplayXfer(Xfer) || Xfer <- lists:flatten(Xfers)],

    io:format("~n"),
    [DisplayDown(Node) || Node <- Down],
    ok.

print_v2_status(Type, Mod, {SrcPartition, TargetPartition}, StartTS) ->
    StartTSStr = datetime_str(StartTS),
    Running = timer:now_diff(os:timestamp(), StartTS),
    RunningStr = riak_core_format:human_time_fmt("~.2f", Running),

    io:format("transfer type: ~s~n", [Type]),
    io:format("vnode type: ~p~n", [Mod]),
    case Type of
        repair ->
            io:format("source partition: ~p~n", [SrcPartition]),
            io:format("target partition: ~p~n", [TargetPartition]);
        _ ->
            io:format("partition: ~p~n", [TargetPartition])
    end,
    io:format("started: ~s [~s ago]~n", [StartTSStr, RunningStr]).

print_v1_status(Mod, Partition, Node) ->
    io:format("vnode type: ~p~n", [Mod]),
    io:format("partition: ~p~n", [Partition]),
    io:format("target node: ~p~n~n", [Node]).

print_stats(SrcNode, TargetNode, no_stats) ->
    io:format("last update: no updates seen~n"),
    print_size(undefined),
    io:format("objects transferred: unknown~n~n"),
    print_arrowbox(SrcNode, TargetNode, "unknown", "unknown", 0.0);
print_stats(SrcNode, TargetNode, Stats) ->
    ObjsS = proplists:get_value(objs_per_s, Stats),
    BytesS = proplists:get_value(bytes_per_s, Stats),
    LastUpdate = proplists:get_value(last_update, Stats),
    Diff = timer:now_diff(os:timestamp(), LastUpdate),
    DiffStr = riak_core_format:human_time_fmt("~.2f", Diff),
    Objs = proplists:get_value(objs_total, Stats),
    ObjsSStr = riak_core_format:fmt("~p Objs/s", [ObjsS]),
    ByteStr = riak_core_format:human_size_fmt("~.2f", BytesS) ++ "/s",
    TS = datetime_str(LastUpdate),
    Size = proplists:get_value(size, Stats),
    DonePctDecimal = proplists:get_value(pct_done_decimal, Stats),
    io:format("last update: ~s [~s ago]~n", [TS, DiffStr]),
    print_size(Size),
    io:format("objects transferred: ~p~n~n", [Objs]),
    print_arrowbox(SrcNode, TargetNode, ObjsSStr, ByteStr, DonePctDecimal).

print_size(undefined) ->
    io:format("total size: unknown~n");
print_size({Objs, objects}) ->
    io:format("total size: ~p objects~n", [Objs]);
print_size({Bytes, bytes}) ->
    io:format("total size: ~p bytes~n", [Bytes]).

-define(ARROW, "=======> ").

print_arrowbox(SrcAtom, TargetAtom, Objs, Bytes, Progress) ->
    Src0 = atom_to_list(SrcAtom),
    Target0 = atom_to_list(TargetAtom),
    {SCont1, SCont2} = wrap(Src0),
    {TCont1, TCont2} = wrap(Target0),

    Src = case SCont1 of
              "" -> string:centre(Src0, 25);
              _ -> Src0
          end,
    Target = case TCont1 of
                 "" -> string:centre(Target0, 25);
                 _ -> Target0
             end,
    
    ToFrom = riak_core_format:fmt("~25s ~10s ~25s",
                                  [Src, ?ARROW, Target]),
    Width = length(ToFrom),

    Prog = progress(Progress, 50),

    io:format("~s~n", [string:centre(Objs, Width)]),
    io:format("~s~n", [ToFrom]),
    Fmt = "~-25s ~10s ~-25s~n",
    case SCont1 /= "" orelse TCont1 /= "" of
        true -> 
            io:format(Fmt, [SCont1, "", TCont1]);
        _ -> ok
    end, 
    case SCont2 /= "" orelse TCont2 /= "" of
        true -> 
            io:format(Fmt, [SCont2, "", TCont2]);
        _ -> ok
    end, 
    io:format("~s~n", [string:centre("     "++Prog, Width)]),
    io:format("~s~n", [string:centre(Bytes, Width)]).
    

wrap(String) ->
    Len = length(String),
    case Len of
        N when N > 50 ->
            One = lists:sublist(String, 26, 25),
            Two = lists:sublist(String, 51, 25),
            {One, Two};
        N when N >= 25,
               N =< 50->
            One = lists:sublist(String, 26, 25),
            {One, ""};
        _ -> 
            {"", ""}
    end.


progress(undefined, MaxSize) ->
    FormatStr = progress_fmt(progress_size(MaxSize), 0), %% this is wrong, need - 6 refactor
    riak_core_format:fmt(FormatStr, ["", "", "N/A"]);
progress(PctDecimal, MaxSize) ->
    ProgressTotalSize = progress_size(MaxSize),
    ProgressSize = trunc(PctDecimal * ProgressTotalSize),
    PadSize = ProgressTotalSize - ProgressSize,
    FormatStr = progress_fmt(ProgressSize, PadSize),
    riak_core_format:fmt(FormatStr, ["", "", integer_to_list(trunc(PctDecimal * 100))]).

progress_size(MaxSize) ->
    MaxSize - 7. %% 7 fixed characters in progress bar

progress_fmt(ArrowSize, PadSize) ->
    riak_core_format:fmt("|~~~p..=s~~~p.. s| ~~3.. s%", [ArrowSize, PadSize]).

datetime_str({_Mega, _Secs, _Micro}=Now) ->
    datetime_str(calendar:now_to_datetime(Now));
datetime_str({{Year, Month, Day}, {Hour, Min, Sec}}) ->
    riak_core_format:fmt("~4..0B-~2..0B-~2..0B ~2..0B:~2..0B:~2..0B",
                         [Year,Month,Day,Hour,Min,Sec]).

stage_leave([]) ->
    stage_leave(node());
stage_leave([NodeStr]) ->
    stage_leave(list_to_atom(NodeStr));
stage_leave(Node) ->
    try
        case riak_core_claimant:leave_member(Node) of
            ok ->
                io:format("Success: staged leave request for ~p~n", [Node]),
                ok;
            {error, already_leaving} ->
                io:format("~p is already in the process of leaving the "
                          "cluster.~n", [Node]),
                ok;
            {error, not_member} ->
                io:format("Failed: ~p is not a member of the cluster.~n",
                          [Node]),
                error;
            {error, only_member} ->
                io:format("Failed: ~p is the only member.~n", [Node]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Leave failed ~p:~p", [Exception, Reason]),
            io:format("Leave failed, see log for details~n"),
            error
    end.

stage_remove([NodeStr]) ->
    stage_remove(list_to_atom(NodeStr));
stage_remove(Node) ->
    try
        case riak_core_claimant:remove_member(Node) of
            ok ->
                io:format("Success: staged remove request for ~p~n", [Node]),
                ok;
            {error, not_member} ->
                io:format("Failed: ~p is not a member of the cluster.~n",
                          [Node]),
                error;
            {error, is_claimant} ->
                is_claimant_error(Node, "remove"),
                error;
            {error, only_member} ->
                io:format("Failed: ~p is the only member.~n", [Node]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Remove failed ~p:~p", [Exception, Reason]),
            io:format("Remove failed, see log for details~n"),
            error
    end.

stage_replace([NodeStr1, NodeStr2]) ->
    stage_replace(list_to_atom(NodeStr1), list_to_atom(NodeStr2)).
stage_replace(Node1, Node2) ->
    try
        case riak_core_claimant:replace(Node1, Node2) of
            ok ->
                io:format("Success: staged replacement of ~p with ~p~n",
                          [Node1, Node2]),
                ok;
            {error, already_leaving} ->
                io:format("~p is already in the process of leaving the "
                          "cluster.~n", [Node1]),
                ok;
            {error, not_member} ->
                io:format("Failed: ~p is not a member of the cluster.~n",
                          [Node1]),
                error;
            {error, invalid_replacement} ->
                io:format("Failed: ~p is not a valid replacement candiate.~n"
                          "Only newly joining nodes can be used for "
                          "replacement.~n", [Node2]),
                error;
            {error, already_replacement} ->
                io:format("Failed: ~p is already staged to replace another "
                          "node.~n", [Node2]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Node replacement failed ~p:~p", [Exception, Reason]),
            io:format("Node replacement failed, see log for details~n"),
            error
    end.

stage_force_replace([NodeStr1, NodeStr2]) ->
    stage_force_replace(list_to_atom(NodeStr1), list_to_atom(NodeStr2)).
stage_force_replace(Node1, Node2) ->
    try
        case riak_core_claimant:force_replace(Node1, Node2) of
            ok ->
                io:format("Success: staged forced replacement of ~p with ~p~n",
                          [Node1, Node2]),
                ok;
            {error, not_member} ->
                io:format("Failed: ~p is not a member of the cluster.~n",
                          [Node1]),
                error;
            {error, is_claimant} ->
                is_claimant_error(Node1, "replace"),
                error;
            {error, invalid_replacement} ->
                io:format("Failed: ~p is not a valid replacement candiate.~n"
                          "Only newly joining nodes can be used for "
                          "replacement.~n", [Node2]),
                error;
            {error, already_replacement} ->
                io:format("Failed: ~p is already staged to replace another "
                          "node.~n", [Node2]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Forced node replacement failed ~p:~p",
                        [Exception, Reason]),
            io:format("Forced node replacement failed, see log for details~n"),
            error
    end.

stage_resize_ring(["abort"]) ->
    try
        case riak_core_claimant:abort_resize() of
            ok ->
                io:format("Success: staged abort resize ring request~n"),
                ok;
            {error, not_resizing} ->
                io:format("Failed: ring is not resizing or resize has completed"),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Abort resize ring request failed ~p:~p",
                        [Exception, Reason]),
            io:format("Abort resize ring request failed, see log for details~n"),
            error
    end;
stage_resize_ring([SizeStr]) ->
    try list_to_integer(SizeStr) of
        Size -> stage_resize_ring(Size)
    catch
        error:badarg ->
            io:format("Failed: Ring size must be an integer.")
    end;
stage_resize_ring(NewRingSize) ->
    try
        case riak_core_claimant:resize_ring(NewRingSize) of
            ok ->
                io:format("Success: staged resize ring request with new size: ~p~n",
                          [NewRingSize]),
                ok;
            {error, not_capable} ->
                io:format("Failed: at least one node is not capable of performing the operation~n"),
                error;
            {error, same_size} ->
                io:format("Failed: current ring size is already ~p~n",
                          [NewRingSize]),
                error;
            {error, control_running} ->
                io:format("Failed: cannot resize ring with Riak Control running~n"),
                error;
            {error, search_running} ->
                io:format("Failed: cannot resize ring with Riak Search~n"),
                error;
            {error, single_node} ->
                io:format("Failed: cannot resize single node~n"),
                error;
            {error, pending_changes} ->
                io:format("Failed: there are pending changes. Try again once completed~n"),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Resize ring request failed ~p:~p",
                        [Exception, Reason]),
            io:format("Resize ring request failed, see log for details~n"),
            error
    end.

clear_staged([]) ->
    try
        case riak_core_claimant:clear() of
            ok ->
                io:format("Cleared staged cluster changes~n"),
                ok
        end
    catch
        Exception:Reason ->
            lager:error("Failed to clear staged cluster changes ~p:~p",
                        [Exception, Reason]),
            io:format("Failed to clear staged cluster changes, see log "
                      "for details~n"),
            error
    end.

is_claimant_error(Node, Action) ->
    io:format("Failed: ~p is the claimant (see: riak-admin ring_status).~n",
              [Node]),
    io:format(
      "The claimant is the node responsible for initiating cluster changes,~n"
      "and cannot forcefully ~s itself. You can use 'riak-admin down' to~n"
      "mark the node as offline, which will trigger a new claimant to take~n"
      "over.  However, this will clear any staged changes.~n", [Action]).

print_staged([]) ->
    case riak_core_claimant:plan() of
        {error, legacy} ->
            io:format("The cluster is running in legacy mode and does not "
                      "support plan/commit.~n");
        {error, ring_not_ready} ->
            io:format("Cannot plan until cluster state has converged.~n"
                      "Check 'Ring Ready' in 'riak-admin ring_status'~n");
        {error, invalid_resize_claim} ->
            io:format("Unable to claim some partitions in resized ring.~n"
                      "Check that there are no pending changes in 'riak-admin ring-status'~n"
                      "If there are, try again once they are completed~n"
                      "Otherwise try again shortly.~n");
        {ok, Changes, NextRings} ->
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            %% The last next ring is always the final ring after all changes,
            %% which is uninteresting to show. Only print N-1 rings.
            NextRings2 = lists:sublist(NextRings,
                                       erlang:max(0, length(NextRings)-1)),
            print_plan(Changes, Ring, NextRings2),
            ok
    end.

print_plan([], _, _) ->
    io:format("There are no staged changes~n");
print_plan(Changes, Ring, NextRings) ->
    io:format("~31..=s Staged Changes ~32..=s~n", ["", ""]),
    io:format("Action         Details(s)~n"),
    io:format("~79..-s~n", [""]),

    lists:map(fun({Node, join}) ->
                      io:format("join           ~p~n", [Node]);
                 ({Node, leave}) ->
                      io:format("leave          ~p~n", [Node]);
                 ({Node, remove}) ->
                      io:format("force-remove   ~p~n", [Node]);
                 ({Node, {replace, NewNode}}) ->
                      io:format("replace        ~p with ~p~n", [Node, NewNode]);
                 ({Node, {force_replace, NewNode}}) ->
                      io:format("force-replace  ~p with ~p~n", [Node, NewNode]);
                 ({_, {resize, NewRingSize}}) ->
                      CurrentSize = riak_core_ring:num_partitions(Ring),
                      io:format("resize-ring    ~p to ~p partitions~n",[CurrentSize,NewRingSize]);
                 ({_, abort_resize}) ->
                      CurrentSize = riak_core_ring:num_partitions(Ring),
                      io:format("resize-ring    abort. current size: ~p~n", [CurrentSize])
              end, Changes),
    io:format("~79..-s~n", [""]),
    io:format("~n"),

    lists:map(fun({Node, remove}) ->
                      io:format("WARNING: All of ~p replicas will be lost~n", [Node]);
                 ({Node, {force_replace, _}}) ->
                      io:format("WARNING: All of ~p replicas will be lost~n", [Node]);
                 (_) ->
                      ok
              end, Changes),
    io:format("~n"),

    Transitions = length(NextRings),
    case Transitions of
        1 ->
            io:format("NOTE: Applying these changes will result in 1 "
                      "cluster transition~n~n");
        _ ->
            io:format("NOTE: Applying these changes will result in ~b "
                      "cluster transitions~n~n", [Transitions])
    end,

    lists:mapfoldl(fun({Ring1, Ring2}, I) ->
                           io:format("~79..#s~n", [""]),
                           io:format("~24.. s After cluster transition ~b/~b~n",
                                     ["", I, Transitions]),
                           io:format("~79..#s~n~n", [""]),
                           output(Ring1, Ring2),
                           {ok, I+1}
                   end, 1, NextRings),
    ok.

output(Ring, NextRing) ->
    Members = riak_core_ring:all_members(NextRing),
    LegacyGossip = orddict:from_list([{Node, false} || Node <- Members]),
    riak_core_console:print_member_status(NextRing, LegacyGossip),
    io:format("~n"),

    FutureRing = riak_core_ring:future_ring(NextRing),
    case riak_core_ring_util:check_ring(FutureRing) of
        [] ->
            ok;
        _ ->
            io:format("WARNING: Not all replicas will be on distinct nodes~n~n")
    end,

    Owners1 = riak_core_ring:all_owners(Ring),
    Owners2 = riak_core_ring:all_owners(NextRing),
    Owners3 = lists:zip(Owners1, Owners2),
    Reassigned = [{Idx, PrevOwner, NewOwner}
                  || {{Idx, PrevOwner}, {Idx, NewOwner}} <- Owners3,
                     PrevOwner /= NewOwner],
    ReassignedTally = tally(Reassigned),

    Pending = riak_core_ring:pending_changes(NextRing),
    Next = [{Idx, PrevOwner, NewOwner} || {Idx, PrevOwner, NewOwner, _, _} <- Pending],
    NextTally = tally(Next),
    Resizing = riak_core_ring:is_resizing(NextRing),

    case Reassigned of
        [] ->
            ok;
        _ ->
            io:format("Partitions reassigned from cluster changes: ~p~n",
                      [length(Reassigned)]),
            [io:format("  ~b reassigned from ~p to ~p~n", [Count, PrevOwner, NewOwner])
             || {{PrevOwner, NewOwner}, Count} <- ReassignedTally],
            io:format("~n"),
            ok
    end,

    case {Resizing, Next} of
        {_, []} ->
            ok;
        {true, _} ->
            io:format("Ring is resizing. see riak-admin ring-status for transfer details.~n");
        _ ->
            io:format("Transfers resulting from cluster changes: ~p~n",
                      [length(Next)]),
            [io:format("  ~b transfers from ~p to ~p~n", [Count, PrevOwner, NewOwner])
             || {{PrevOwner, NewOwner}, Count} <- NextTally],
            ok,
            io:format("~n")
    end,
    ok.

tally(Changes) ->    
    Tally =
        lists:foldl(fun({_, PrevOwner, NewOwner}, Tally) ->
                            dict:update_counter({PrevOwner, NewOwner}, 1, Tally)
                    end, dict:new(), Changes),
    dict:to_list(Tally).

commit_staged([]) ->
    case riak_core_claimant:commit() of
        ok ->
            io:format("Cluster changes committed~n");
        {error, legacy} ->
            io:format("The cluster is running in legacy mode and does not "
                      "support plan/commit.~n");
        {error, nothing_planned} ->
            io:format("You must verify the plan with "
                      "'riak-admin cluster plan' before committing~n");
        {error, ring_not_ready} ->
            io:format("Cannot commit until cluster state has converged.~n"
                      "Check 'Ring Ready' in 'riak-admin ring_status'~n");
        {error, plan_changed} ->
            io:format("The plan has changed. Verify with "
                      "'riak-admin cluster plan' before committing~n");
        _ ->
            io:format("Unable to commit cluster changes. Plan "
                      "may have changed, please verify the~n"
                      "plan and try to commit again~n")
    end.

transfer_limit([]) ->
    {Limits, Down} =
        riak_core_util:rpc_every_member_ann(riak_core_handoff_manager,
                                            get_concurrency, [], 5000),
    io:format("~s~n", [string:centre(" Transfer Limit ", 79, $=)]),
    io:format("Limit        Node~n"),
    io:format("~79..-s~n", [""]),
    lists:foreach(fun({Node, Limit}) ->
                          io:format("~5b        ~p~n", [Limit, Node])
                  end, Limits),
    lists:foreach(fun(Node) ->
                          io:format("(offline)    ~p~n", [Node])
                  end, Down),
    io:format("~79..-s~n", [""]),
    io:format("Note: You can change transfer limits with "
              "'riak-admin transfer_limit <limit>'~n"
              "      and 'riak-admin transfer_limit <node> <limit>'~n"),
    ok;
transfer_limit([LimitStr]) ->
    {Valid, Limit} = check_limit(LimitStr),
    case Valid of
        false ->
            io:format("Invalid limit: ~s~n", [LimitStr]),
            error;
        true ->
            io:format("Setting transfer limit to ~b across the cluster~n",
                      [Limit]),
            {_, Down} =
                riak_core_util:rpc_every_member_ann(riak_core_handoff_manager,
                                                    set_concurrency,
                                                    [Limit], 5000),
            (Down == []) orelse
                io:format("Failed to set limit for: ~p~n", [Down]),
            ok
    end;
transfer_limit([NodeStr, LimitStr]) ->
    Node = list_to_atom(NodeStr),
    {Valid, Limit} = check_limit(LimitStr),
    case Valid of
        false ->
            io:format("Invalid limit: ~s~n", [LimitStr]),
            error;
        true ->
            case rpc:call(Node, riak_core_handoff_manager,
                          set_concurrency, [Limit]) of
                {badrpc, _} ->
                    io:format("Failed to set transfer limit for ~p~n", [Node]);
                _ ->
                    io:format("Set transfer limit for ~p to ~b~n",
                              [Node, Limit])
            end,
            ok
    end.

check_limit(Str) ->
    try
        Int = list_to_integer(Str),
        {Int >= 0, Int}
    catch
        _:_ ->
            {false, 0}
    end.

add_user([Username|Options]) ->
    case riak_core_security:add_user(list_to_binary(Username),
                                     parse_options(Options)) of
        ok -> ok;
        Error ->
            io:format("~p~n", [Error]),
            Error
    end.

add_source([Users, CIDR, Source | Options]) ->
    Unames = case string:tokens(Users, ",") of
        ["all"] ->
            all;
        Other ->
            [list_to_binary(O) || O <- Other]
    end,
    case riak_core_security:add_source(Unames, parse_cidr(CIDR),
                                  list_to_atom(Source),
                                  parse_options(Options)) of
        ok ->
            ok;
        Error ->
            io:format("~p~n", [Error]),
            Error
    end.

grant([Grants, "ON", "ANY", "TO", Users]) ->
    Unames = case string:tokens(Users, ",") of
        ["all"] ->
            all;
        Other ->
            [list_to_binary(O) || O <- Other]
    end,
    Permissions = case string:tokens(Grants, ",") of
        ["all"] ->
            all;
        Other2 ->
            Other2
    end,
    case riak_core_security:add_grant(Unames, any, Permissions) of
        ok -> ok;
        Error ->
            io:format("~p~n", [Error]),
            Error
    end;
grant([Grants, "ON", Type, Bucket, "TO", Users]) ->
    grant([Grants, "ON", {list_to_binary(Type), list_to_binary(Bucket)}, "TO",
           Users]);
grant([Grants, "ON", Type, "TO", Users]) when is_list(Type) ->
    grant([Grants, "ON", list_to_binary(Type), "TO", Users]);
grant([Grants, "ON", Bucket, "TO", Users]) ->
    Unames = case string:tokens(Users, ",") of
        ["all"] ->
            all;
        Other ->
            [list_to_binary(O) || O <- Other]
    end,
    Permissions = case string:tokens(Grants, ",") of
        ["all"] ->
            all;
        Other2 ->
            Other2
    end,
    case riak_core_security:add_grant(Unames, Bucket, Permissions) of
        ok -> ok;
        Error ->
            io:format("~p~n", [Error]),
            Error
    end;
grant(_) ->
    io:format("Usage: grant <permissions> ON (<type> [bucket]|ANY) TO <users>"),
    error.

revoke([Grants, "ON", "ANY", "FROM", Users]) ->
    Unames = case string:tokens(Users, ",") of
        ["all"] ->
            all;
        Other ->
            [list_to_binary(O) || O <- Other]
    end,
    Permissions = case string:tokens(Grants, ",") of
        ["all"] ->
            all;
        Other2 ->
            Other2
    end,
    riak_core_security:add_revoke(Unames, any, Permissions);
revoke([Grants, "ON", Type, Bucket, "FROM", Users]) ->
    revoke([Grants, "ON", {list_to_binary(Type), list_to_binary(Bucket)},
            "FROM",
           Users]);
revoke([Grants, "ON", Type, "FROM", Users]) when is_list(Type) ->
    revoke([Grants, "ON", list_to_binary(Type), "FROM", Users]);
revoke([Grants, "ON", Bucket, "FROM", Users]) ->
    Unames = case string:tokens(Users, ",") of
        ["all"] ->
            all;
        Other ->
            [list_to_binary(O) || O <- Other]
    end,
    Permissions = case string:tokens(Grants, ",") of
        ["all"] ->
            all;
        Other2 ->
            Other2
    end,
    riak_core_security:add_revoke(Unames, Bucket, Permissions);
revoke(_) ->
    io:format("Usage: revoke <permissions> ON <type> [bucket] FROM <users>"),
    error.


print_users([]) ->
    riak_core_security:print_users().

print_user([User]) ->
    riak_core_security:print_user(list_to_binary(User)).

print_sources([]) ->
    riak_core_security:print_sources().

ciphers([]) ->
    riak_core_security:print_ciphers();

ciphers([CipherList]) ->
    case riak_core_security:set_ciphers(CipherList) of
        ok ->
            riak_core_security:print_ciphers(),
            ok;
        error ->
            error
    end.

parse_options(Options) ->
    parse_options(Options, []).

parse_options([], Acc) ->
    Acc;
parse_options([H|T], Acc) ->
    [Key, Value] = string:tokens(H, "="),
    parse_options(T, [{Key, Value}|Acc]).

parse_cidr(CIDR) ->
    [IP, Mask] = string:tokens(CIDR, "/"),
    {ok, Addr} = inet_parse:address(IP),
    {Addr, list_to_integer(Mask)}.
