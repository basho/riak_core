%% -------------------------------------------------------------------
%%
%% Riak: A lightweight, decentralized key-value store.
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_core_status).
-export([ringready/0,
         all_active_transfers/0,
         transfers/0,
         transfer_limit/0,
         transfer_limit/1,
         rpc_transfer_limit/0,
         rpc_transfer_limit/1,
         ring_status/0]).

%% Status writer API
-export([parse/3]).

-include("riak_core_status_types.hrl").

-spec parse(status(), fun(), Acc0 :: term()) -> term().
parse([], Fun, Acc) ->
    Fun(done, Acc);
%% Alert is currently the only non-leaf element
parse([{alert, Elem} | T], Fun, Acc) ->
    Acc1 = Fun(alert, Acc),
    Acc2 = parse(Elem, Fun, Acc1),
    Acc3 = Fun(alert_done, Acc2),
    parse(T, Fun, Acc3);
%% Leaf elements
parse([Elem | T], Fun, Acc) ->
    Acc1 = Fun(Elem, Acc),
    parse(T, Fun, Acc1).

-spec(ringready() -> {ok, [atom()]} | {error, any()}).
ringready() ->
    case get_rings() of
        {[], Rings} ->
            {N1, R1} = hd(Rings),
            case rings_match(hash_ring(R1), tl(Rings)) of
                true ->
                    Nodes = [N || {N, _} <- Rings],
                    {ok, Nodes};
                {false, N2} ->
                    {error, {different_owners, N1, N2}}
            end;
        {Down, _Rings} ->
            {error, {nodes_down, Down}}
    end.

-spec transfer_limit() -> status().
transfer_limit() ->
    Limits0 = riak_core_handoff_manager:get_all_concurrency(),
    Limits = [{Node, Limit} || {Node, [Limit]} <- Limits0],
    transfer_limit_status(Limits).

-spec transfer_limit(node()) -> status().
transfer_limit(Node) ->
    case riak_core_handoff_manager:get_concurrency(Node) of
        undefined ->
            [{alert, [{text, io_lib:format("Invalid/unknown node '~s'.", [Node])}]}];
        Limit ->
            transfer_limit_status([{Node, Limit}])
    end.

-spec transfer_limit_status([{node(), non_neg_integer()}]) -> status().
transfer_limit_status(Limits0) ->
    Schema = [node, limit],
    Limits = lists:keysort(1, Limits0),
    Rows = [[Node, Limit] || {Node, Limit} <- Limits],
    [{table, Schema, Rows}].

-spec rpc_transfer_limit() -> status().
rpc_transfer_limit() ->
    {Limits, Down} =
        riak_core_util:rpc_every_member_ann(riak_core_handoff_manager,
                                            get_concurrency, [], 5000),
    rpc_transfer_limit_status(Limits, Down).

-spec rpc_transfer_limit(node()) -> status().
rpc_transfer_limit(Node) ->
    case riak_core_util:safe_rpc(Node, riak_core_handoff_manager,
                                 get_concurrency, [], 5000) of
        {badrpc, _} ->
            rpc_transfer_limit_status([], [Node]);
        Limit ->
            rpc_transfer_limit_status([{Node, Limit}], [])
    end.

-spec rpc_transfer_limit_status([{node(), non_neg_integer()}], [node()]) ->
                                       status().
rpc_transfer_limit_status(Limits, Down) ->
    Schema = [node, limit],
    Rows = [[Node, Limit] || {Node, Limit} <- Limits],
    Table = {table, Schema, Rows},
    case Down of
        [] ->
            [Table];
        _ ->
            NodesDown = {alert, [{column, "(offline)", Down}]},
            [Table, NodesDown]
    end.

-spec(transfers() -> {[atom()], [{waiting_to_handoff, atom(), integer()} |
                                 {stopped, atom(), integer()}]}).
transfers() ->
    {Down, Rings} = get_rings(),

    %% Work out which vnodes are running and which partitions they claim
    F = fun({N, R}, Acc) ->
                {_Pri, Sec, Stopped} = partitions(N, R),
                Acc1 = case Sec of
                           [] ->
                               [];
                           _ ->
                               [{waiting_to_handoff, N, length(Sec)}]
                       end,
                case Stopped of
                    [] ->
                        Acc1 ++ Acc;
                    _ ->
                        Acc1 ++ [{stopped, N, length(Stopped)} | Acc]
                end
        end,
    {Down, lists:foldl(F, [], Rings)}.

%% @doc Produce status for all active transfers in the cluster.
-spec all_active_transfers() -> {Xfers :: list(tuple()), Down :: list(node())}.
all_active_transfers() ->
    {Xfers, Down} =
        riak_core_util:rpc_every_member(riak_core_handoff_manager,
                                        status,
                                        [{direction, outbound}],
                                        5000),
    {Xfers, Down}.

ring_status() ->
    %% Determine which nodes are reachable as well as what vnode modules
    %% are running on each node.
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    {AllMods, Down} =
        riak_core_util:rpc_every_member_ann(riak_core, vnode_modules, [], 1000),

    %% Check if the claimant is running and if it believes the ring is ready
    Claimant = riak_core_ring:claimant(Ring),
    case riak_core_util:safe_rpc(Claimant, riak_core_ring, ring_ready, [], 5000) of
        {badrpc, _} ->
            Down2 = lists:usort([Claimant | Down]),
            RingReady = undefined;
        RingReady ->
            Down2 = Down,
            RingReady = RingReady
    end,

    %% Get the list of pending ownership changes
    Changes = riak_core_ring:pending_changes(Ring),
    %% Group pending changes by (Owner, NextOwner)
    Merged = lists:foldl(
               fun({Idx, Owner, NextOwner, Mods, Status}, Acc) ->
                       orddict:append({Owner, NextOwner},
                                      {Idx, Mods, Status},
                                      Acc)
               end, [], Changes),

    %% For each pending transfer, determine which vnode modules have completed
    %% handoff and which we are still waiting on.
    %% Final result is of the form:
    %%   [{Owner, NextOwner}, [{Index, WaitingMods, CompletedMods, Status}]]
    TransferStatus = orddict:map(
                       fun({Owner, _}, Transfers) ->
                               case orddict:find(Owner, AllMods) of
                                   error ->
                                       [{Idx, down, Mods, Status}
                                        || {Idx, Mods, Status} <- Transfers];
                                   {ok, OwnerMods} ->
                                       NodeMods = [Mod || {_App, Mod} <- OwnerMods],
                                       [{Idx, NodeMods -- Mods, Mods, Status}
                                        || {Idx, Mods, Status} <- Transfers]
                               end
                       end, Merged),

    MarkedDown = riak_core_ring:down_members(Ring),
    {Claimant, RingReady, Down2, MarkedDown, TransferStatus}.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% Retrieve the rings for all other nodes by RPC
get_rings() ->
    {RawRings, Down} = riak_core_util:rpc_every_member(
                         riak_core_ring_manager, get_my_ring, [], 30000),
    RawRings2 = [riak_core_ring:upgrade(R) || {ok, R} <- RawRings],
    Rings = orddict:from_list([{riak_core_ring:owner_node(R), R} || R <- RawRings2]),
    {lists:sort(Down), Rings}.

%% Produce a hash of the 'chash' portion of the ring
hash_ring(R) ->
    erlang:phash2(riak_core_ring:all_owners(R)).

%% Check if all rings match given a hash and a list of [{N,P}] to check
rings_match(_, []) ->
    true;
rings_match(R1hash, [{N2, R2} | Rest]) ->
    case hash_ring(R2) of
        R1hash ->
            rings_match(R1hash, Rest);
        _ ->
            {false, N2}
    end.

%% Get a list of active partition numbers - regardless of vnode type
active_partitions(Node) ->
    VNodes = gen_server:call({riak_core_vnode_manager, Node}, all_vnodes, 30000),
    lists:foldl(fun({_, P, _}, Ps) ->
                        ordsets:add_element(P, Ps)
                end, [], VNodes).

%% Return a list of active primary partitions, active secondary partitions (to be handed off)
%% and stopped partitions that should be started
partitions(Node, Ring) ->
    Owners = riak_core_ring:all_owners(Ring),
    Owned = ordsets:from_list(owned_partitions(Owners, Node)),
    Active = ordsets:from_list(active_partitions(Node)),
    Stopped = ordsets:subtract(Owned, Active),
    Secondary = ordsets:subtract(Active, Owned),
    Primary = ordsets:subtract(Active, Secondary),
    {Primary, Secondary, Stopped}.

%% Return the list of partitions owned by a node
owned_partitions(Owners, Node) ->
    [P || {P, Owner} <- Owners, Owner =:= Node].
