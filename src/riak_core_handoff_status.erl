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
-module(riak_core_handoff_status).

-include("riak_core_handoff.hrl").

-export([handoff_summary/3,
         handoff_details/3,
         collect_node_transfer_data/0]).

-define(ORDERED_TRANSFERS_FOR_DISPLAY,
        [{ownership, "Ownership"}, {resize, "Resize"}, {hinted, "Hinted"}, {repair, "Repair"}]).

%% This module's CLI callbacks and usage are registered with riak_core_handoff_cli
%%%%%%
%% clique callbacks

-spec handoff_summary([string()], [tuple()], [tuple()]) -> clique_status:status().
handoff_summary(_CmdBase, [], []) ->
    node_summary().

-spec handoff_details([string()], [tuple()], [tuple()]) -> clique_status:status().
handoff_details(_CmdBase, [], []) ->
    build_handoff_details(node());
handoff_details(_CmdBase, [], [{node, Node}]) ->
    build_handoff_details(Node);
handoff_details(_CmdBase, [], [{all, _Value}]) ->
    build_handoff_details(all);
handoff_details(_CmdBase, [], _) ->
    [clique_status:alert([clique_status:text("Cannot use both --all and --node flags at the same time.")])].

%% end of clique callbacks
%%%%%%

%%%%%%
%% RPC-exposed functions
collect_node_transfer_data() ->
    ActiveTransfers = riak_core_handoff_manager:status({direction, outbound}),
    KnownTransfers = riak_core_vnode_manager:all_handoffs(),
    {ActiveTransfers, KnownTransfers}.

%%%%%%
%% Private

%%%%%%
%% Details View
-spec build_handoff_details(node()|all) -> clique_status:status().
build_handoff_details(NodeOrAll) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {Active0, DownNodes} = collect_from_all({riak_core_handoff_manager, status, [{direction, outbound}]}, 5000),
    Active = flatten_transfer_proplist(Active0),
    AllNodes = riak_core_ring:all_members(Ring),
    Nodes = case NodeOrAll of
                all -> AllNodes -- DownNodes;
                Node -> [Node] -- DownNodes %% Don't display row in table if requested node is down
            end,
    FilteredTransfers =
        lists:filter(fun(T) ->
                             lists:member(transfer_info(T, src_node), Nodes) orelse
                                 lists:member(transfer_info(T, target_node), Nodes)
                     end,
                     Active),

    Details = case length(FilteredTransfers) of
        0 -> [clique_status:text("No ongoing transfers.")];
        _ ->
            Header = clique_status:text("Type Key: O = Ownership, H = Hinted, Rz = Resize, Rp = Repair"),
            RingSize = riak_core_ring:num_partitions(Ring),
            Table = clique_status:table(
                      [ row_details(Transfer, RingSize) || Transfer <- FilteredTransfers]),
            [Header, Table]
    end,
    maybe_add_down_nodes(DownNodes, Details).

row_details(T, RingSize) ->
    SourceNode = transfer_info(T, src_node),
    TargetNode = transfer_info(T, target_node),
    SourceIdx = transfer_info(T, src_partition),
    TargetIdx = transfer_info(T, target_partition),
    Source = io_lib:format("~ts:~B", [SourceNode, riak_core_ring_util:hash_to_partition_id(SourceIdx, RingSize)]),
    Target = io_lib:format("~ts:~B", [TargetNode, riak_core_ring_util:hash_to_partition_id(TargetIdx, RingSize)]),

    [
     {'Type', format_transfer_type(transfer_info(T, type))},
     {'Source:ID', Source},
     {'Target:ID', Target},
     {'Size', format_transfer_size(transfer_stat(T, size))},
     {'Rate (KB/s)', format_rate(transfer_stat(T, bytes_per_s))},
     {'%', format_percent(transfer_stat(T, pct_done_decimal))},
     {'Sender', transfer_info(T, sender_pid)}
    ].

transfer_info({_Node, {status_v2, Status}}, Key) ->
    case lists:keyfind(Key, 1, Status) of
        {Key, Value} -> Value;
        _ -> " "
    end.

transfer_stat(Transfer, Key) ->
    case transfer_info(Transfer, stats) of
        no_stats -> unknown;
        Stats ->
            {Key, Value} = lists:keyfind(Key, 1, Stats),
            Value
    end.

format_transfer_type(ownership) ->
    "O";
format_transfer_type(hinted) ->
    "H";
format_transfer_type(resize) ->
    "Rz";
format_transfer_type(repair) ->
    "Rp".

format_transfer_size({Num, objects}) ->
    io_lib:format("~B Objs", [Num]);
format_transfer_size({Num, bytes}) ->
    riak_core_format:human_size_fmt("~B", Num);
format_transfer_size(_) ->
    "--".

format_rate(Rate) when is_number(Rate) ->
    io_lib:format("~.2f", [Rate / 1024]);
format_rate(_) ->
    "--".

format_percent(PercentAsDecimal) when is_number(PercentAsDecimal) ->
    io_lib:format("~6.2f", [PercentAsDecimal*100]);
format_percent(_) ->
    "--".

%%%%%%
%% Summary View
-spec node_summary() -> clique_status:status().
node_summary() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),

    %% Known and Active will be proplists with each node name showing
    %% up 0 or more times; the value will be a `status_v2' structure
    %% (see `riak_core_handoff_manager') for active transfers or a
    %% custom handoff shorthand (see the spec for
    %% `collect_known_transfers') for known transfers.
    {ActiveAndKnown, DownNodes} = collect_transfer_data(),
    {Active0, Known0} = split_active_and_known(ActiveAndKnown),
    Known = correlate_known_transfers(Ring, Known0),
    Active = correlate_active_transfers(Active0),

    KnownStats = count_known_transfers(Known),
    ActiveStats = count_active_transfers(Active),

    %% Each stats structure is an orddict with
    %% "Node:TransferType:Direction" and "TransferType" keys, values
    %% are integers. Also ActiveStats will have a `nodes' key with a
    %% ordset of nodes.

    Header = clique_status:text(["Each cell indicates active transfers and, in parenthesis, the number of all known transfers.\n",
                                   "The 'Total' column is the sum of the active transfers."]),
    AllNodes = riak_core_ring:all_members(Ring),
    Nodes = AllNodes -- DownNodes,
    Table = clique_status:table(
              [ [ format_node_name(Node) | row_summary(Node, KnownStats, ActiveStats) ] ||
                  Node <- Nodes]),
    Summary = [Header, Table],
    maybe_add_down_nodes(DownNodes, Summary).

collect_transfer_data() ->
    collect_from_all({riak_core_handoff_status, collect_node_transfer_data, []}, 5000).

%% See riak_core_handoff_manager:build_status/1 for details on this structure
-spec correlate_active_transfers([{node(),[{status_v2, list(tuple())}]}]) ->
                                        [{node(),{status_v2, list(tuple())}}].
correlate_active_transfers(Outbound) ->
    Inbound = reverse_active_transfers(Outbound),
    Outbound ++ Inbound.

reverse_active_transfers(Outbound) ->
    [
     begin
         T0 = replace_transfer_info(T, direction, inbound),
         Target = transfer_info(T, target_node),
         {_OtherNode, TransferInfo} = T0,
         {Target, TransferInfo}
     end
     || T <- Outbound, transfer_info(T, target_node) /= '$delete' ].

replace_transfer_info({Node, {status_v2, Status}}, Key, Value) ->
    {Node, {status_v2, lists:keyreplace(Key, 1, Status, {Key, Value})}}.

%%%%%%
replace_known_with_ring_transfers(Known, RingTransfers) ->
    Known1 = lists:filter(fun({_Node, {{_Module, _Index},
                                       {ownership, _Dir, _Data}}}) ->
                                  false;
                             ({_Node, {{_Module, _Index},
                                       {resize, _Dir, _Data}}}) ->
                                  false;
                             (_) ->
                                  true
                          end,
                          Known),
    coalesce_known_transfers(Known1, RingTransfers).

parse_ring_into_known(Ring) ->
    Transitions = riak_core_ring:pending_changes(Ring),
    [
     transform_ring_transition(T) ||
        {_,_,_,_,State } = T <- Transitions, (State =:= awaiting)
    ].

%% Transform a single tuple with a list of modules (from ring.next) to
%% a list of tuples with one module each.
%% Special case to also transform '$resize' to resize
%% {_Idx, _Source,_Destination,_Modules, awaiting}
transform_ring_transition({_Index, _Source, '$resize', _Modules, _state} = T) ->
    explode_ring_modules(resize, T);
transform_ring_transition({_Index, _Source, _Dest, _Modules, _state} = T) ->
    explode_ring_modules(ownership, T).

explode_ring_modules(Type, {Index, Source, Dest, Modules, _state}) ->
    [{Source, {{M, Index}, {Type, outbound, Dest}}} || M <- Modules].

%% The ring has global visibility into incomplete ownership/resize
%% transfers, so we'll filter those out of what
%% `riak_core_vnode_manager' gives us and replace with the ring data
-spec correlate_known_transfers(riak_core_ring:riak_core_ring(),
                                [{node(), known_handoff()}]) ->
                                       [{node(), known_handoff()}].
correlate_known_transfers(Ring, Known) ->
    RingKnown = parse_ring_into_known(Ring),
    TransfersUpdatedWithRing =
        replace_known_with_ring_transfers(Known, RingKnown),
    Unidirectional =
        lists:flatten(TransfersUpdatedWithRing),
    ReversedKnown = reverse_known_transfers(Ring, Unidirectional),
    coalesce_known_transfers(ReversedKnown, Unidirectional).

find_resize_target(Ring, Idx) ->
    riak_core_ring:index_owner(Ring, Idx).

flatten_transfer_proplist(List) ->
    lists:flatten(
      lists:map(fun({Node, NodeTransfers}) ->
                        lists:map(fun(T) -> {Node, T} end, NodeTransfers)
                end, List)).

coalesce_known_transfers(Proplist1, Proplist2) ->
    lists:flatten(Proplist1, Proplist2).

reverse_direction(inbound) ->
    outbound;
reverse_direction(outbound) ->
    inbound.

reverse_known_transfers(Ring, Known) ->
    lists:foldl(fun({_Node1, {{_Module, _Index},
                              {delete, local, '$delete'}}}, Acc) ->
                        %% Don't reverse delete operations
                        Acc;
                   ({Node1, {{Module, Index},
                             {resize =Type,
                              outbound=Dir,
                              '$resize'}}}, Acc) ->
                        Node2 = find_resize_target(Ring, Index),
                        [{Node2, {{Module, Index},
                                  {Type, reverse_direction(Dir), Node1}}}
                         | Acc];
                   ({Node1, {{Module, Index}, {Type, Dir, Node2}}}, Acc) ->
                        [{Node2, {{Module, Index},
                                  {Type, reverse_direction(Dir), Node1}}}
                         | Acc]
                end,
                [],
                Known).

add_node_name(Node, Dict) ->
    orddict:update(nodes, fun(Set) -> ordsets:add_element(Node, Set) end,
                   ordsets:from_list([Node]), Dict).

count_active_transfers(Transfers) ->
    lists:foldl(fun({Node, {status_v2, Props}}, Dict) ->
                        Type = proplists:get_value(type, Props),
                        Direction = proplists:get_value(direction, Props),
                        Dict1 = increment_counter(io_lib:format("~ts:~s:~s",
                                                                [Node, Type,
                                                                 Direction]),
                                                  Dict),
                        Dict2 = increment_counter(Type, Dict1),
                        add_node_name(Node, Dict2)
                end,
                orddict:new(),
                Transfers).

%%
%% The ring has a complete view of all known ownership/resize
%% transfers, so rely on that for a tally instead of the data
%% collected from riak_core_vnode_manager.
count_known_transfers(Transfers) ->
    lists:foldl(fun({Node, {{_Module, _Index},
                            {Type, Direction, _Node2}}}, Dict) ->
                        Dict1 = increment_counter(io_lib:format("~ts:~s:~s",
                                                                [Node, Type,
                                                                 Direction]),
                                                  Dict),
                        increment_counter(Type, Dict1)
                end,
                orddict:new(),
                Transfers).

increment_counter(Key, Dict) ->
    orddict:update_counter(Key, 1, Dict).

split_active_and_known(ActiveAndKnown) ->
    ActiveList = lists:map(fun({Node, {NodeActive, _}}) ->
                                   {Node, NodeActive} end, ActiveAndKnown),
    KnownList = lists:map(fun({Node, {_, NodeKnown}}) ->
                                  {Node, NodeKnown} end, ActiveAndKnown),
    {flatten_transfer_proplist(ActiveList), flatten_transfer_proplist(KnownList)}.

row_summary(Node, Known, Active) ->
    {Cells, TotalActive} =
        row_summary(Node, Known, Active, ?ORDERED_TRANSFERS_FOR_DISPLAY, [], {"Total", 0}),
    [TotalActive | Cells].

row_summary(_Node, _Known, _Active, [], Accum, Total) ->
    {lists:reverse(Accum), Total};

row_summary(Node, Known, Active, [{Type, Title}|Tail], Accum, {"Total", Total}) ->
    KeyIn = io_lib:format("~ts:~s:~s", [Node, Type, inbound]),
    KeyOut = io_lib:format("~ts:~s:~s", [Node, Type, outbound]),
    ActiveCount = dict_count_helper(orddict:find(KeyIn, Active)) +
        dict_count_helper(orddict:find(KeyOut, Active)),
    KnownCount= dict_count_helper(orddict:find(KeyIn, Known)) +
        dict_count_helper(orddict:find(KeyOut, Known)),
    CellContents = format_cell_contents(Title, KnownCount, ActiveCount),
    row_summary(Node, Known, Active, Tail,
                [CellContents | Accum], {"Total", Total + ActiveCount}).

format_cell_contents(Type, KnownCount, ActiveCount) ->
    Content = case {KnownCount, ActiveCount} of
                  {0, 0} -> " ";
                  {0, _} -> io_lib:format("~B (Unknown)", [ActiveCount]);
                  _ -> io_lib:format("~B (~B)", [ActiveCount, KnownCount])
              end,
    {Type, Content}.

dict_count_helper({ok, Count}) ->
    Count;
dict_count_helper(error) ->
    0.

-spec format_node_name(atom()) -> {nonempty_string(), nonempty_string()}.
format_node_name(Node) when is_atom(Node) ->
    {"Node", "  " ++ atom_to_list(Node) ++ "  "}.


%% RPC Helper to make sure we don't get {node(), {badrpc, _}} in our results, which happens
%% occasionally when you call to a node that is up, but the particular gen_server/etc.
%% isn't loaded yet.
collect_from_all({M, F, A}, Timeout) ->
    {Results, DownNodes} = riak_core_util:rpc_every_member_ann(M, F, A, Timeout),
    {Good, Bad} = lists:partition(fun({_Node, {badrpc, _}}) -> false;
                                     (_) -> true end, Results),
    BadNodes = lists:map(fun({Node, _}) -> Node end, Bad),
    {Good, DownNodes ++ BadNodes}.

maybe_add_down_nodes(DownNodes, Output) ->
    case DownNodes of
        [] ->
            Output;
        _ ->
            NodesDown = clique_status:alert([clique_status:list("(unreachable)", DownNodes)]),
            Output ++ [NodesDown]
    end.
