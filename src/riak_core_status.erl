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
-export([ringready/0, transfers/0, ring_status/0]).

-spec(ringready() -> {ok, [atom()]} | {error, any()}).
ringready() ->
    case get_rings() of
        {[], Rings} ->
            {N1,R1}=hd(Rings),
            case rings_match(hash_ring(R1), tl(Rings)) of
                true ->
                    Nodes = [N || {N,_} <- Rings],
                    {ok, Nodes};

                {false, N2} ->
                    {error, {different_owners, N1, N2}}
            end;

        {Down, _Rings} ->
            {error, {nodes_down, Down}}
    end.


-spec(transfers() -> {[atom()], [{waiting_to_handoff, atom(), integer()} |
                                 {stopped, atom(), integer()}]}).
transfers() ->
    {Down, Rings} = get_rings(),

    %% Work out which vnodes are running and which partitions they claim
    F = fun({N,R}, Acc) ->
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

ring_status() ->
    %% Determine which nodes are reachable as well as what vnode modules
    %% are running on each node.
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    {AllMods, Down} =
        riak_core_util:rpc_every_member_ann(riak_core, vnode_modules, [], 1000),

    %% Check if the claimant is running and if it believes the ring is ready
    Claimant = riak_core_ring:claimant(Ring),
    case rpc:call(Claimant, riak_core_ring, ring_ready, [], 5000) of
        {badrpc, _} ->
            Down2 = lists:usort([Claimant|Down]),
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
    TransferStatus =
        orddict:map(
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
    lists:foldl(fun({_,P}, Ps) ->
                        ordsets:add_element(P, Ps)
                end, [], running_vnodes(Node)).

%% Get a list of running vnodes for a node
running_vnodes(Node) ->
    Pids = vnode_pids(Node),
    [rpc:call(Node, riak_core_vnode, get_mod_index, [Pid], 30000) || Pid <- Pids].

%% Get a list of vnode pids for a node
vnode_pids(Node) ->
    [Pid || {_,Pid,_,_} <- supervisor:which_children({riak_core_vnode_sup, Node})].

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
