%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc riak_core_ring manages a riak node's local view of partition ownership.
%%      The functions in this module revolve around use of the chstate record,
%%      which should be treated as opaque by other modules.  Riak nodes exchange
%%      instances of these records via gossip in order to converge on a common
%%      view of node/partition ownership.

-module(riak_core_ring).

-export([all_members/1,
         all_owners/1,
         all_preflists/2,
         diff_nodes/2,
         equal_rings/2,
         fresh/0,
         fresh/1,
         fresh/2,
         get_meta/2, 
         index_owner/2,
         my_indices/1,
         num_partitions/1,
         owner_node/1,
         preflist/2,
         random_node/1,
         random_other_index/1,
         random_other_index/2,
         random_other_node/1,
         reconcile/2,
         rename_node/3,
         responsible_index/2,
         transfer_node/3,
         update_meta/3]).

-export([cluster_name/1,
         claimant/1,
         member_status/2,
         all_member_status/1,
         add_member/3,
         remove_member/3,
         leave_member/3,
         exit_member/3,
         claiming_members/1,
         set_owner/2,
         indices/2,
         disowning_indices/2,
         pending_changes/1,
         next_owner/2,
         next_owner/3,
         handoff_complete/3,
         ring_ready/0,
         ring_ready/1,
         ring_ready_info/1,
         ring_changed/2,
         reconcile_members/2]).

-export_type([riak_core_ring/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(ROUT(S,A),ok).
%%-define(ROUT(S,A),?debugFmt(S,A)).
%%-define(ROUT(S,A),io:format(S,A)).

-record(chstate, {
    nodename :: node(),          % the Node responsible for this chstate
    vclock   :: vclock:vclock(), % for this chstate object, entries are
                                 % {Node, Ctr}
    chring   :: chash:chash(),   % chash ring of {IndexAsInt, Node} mappings
    meta     :: dict(),          % dict of cluster-wide other data (primarily
                                 % bucket N-value, etc)

    clustername :: {node(), term()}, 
    next     :: [{integer(), node(), node(), [module()], awaiting | complete}],
    members  :: [{node(), {member_status(), vclock:vclock()}}],
    claimant :: node(),
    seen     :: [{node(), vclock:vclock()}],
    rvsn     :: vclock:vclock()
}). 

-type member_status() :: valid | invalid | leaving | exiting.

%% type meta_entry(). Record for each entry in #chstate.meta
-record(meta_entry, {
    value,    % The value stored under this entry
    lastmod   % The last modified time of this entry, 
              %  from calendar:datetime_to_gregorian_seconds(
              %                             calendar:universal_time()), 
}).

%% riak_core_ring() is the opaque data type used for partition ownership
-type riak_core_ring() :: #chstate{}.
-type chstate() :: riak_core_ring().

-type pending_change() :: {Owner :: node(),
                           NextOwner :: node(),
                           awaiting | complete}
                        | {undefined, undefined, undefined}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Produce a list of all nodes that are members of the cluster
-spec all_members(State :: chstate()) -> [Node :: term()].
all_members(#chstate{members=Members}) ->
    get_members(Members).

%% @doc Provide all ownership information in the form of {Index,Node} pairs.
-spec all_owners(State :: chstate()) -> [{Index :: integer(), Node :: term()}].
all_owners(State) ->
    chash:nodes(State#chstate.chring).

%% @doc Provide every preflist in the ring, truncated at N.
-spec all_preflists(State :: chstate(), N :: integer()) ->
                               [[{Index :: integer(), Node :: term()}]].
all_preflists(State, N) ->
    [lists:sublist(preflist(Key, State),N) ||
        Key <- [<<(I+1):160/integer>> ||
                   {I,_Owner} <- ?MODULE:all_owners(State)]].

%% @doc For two rings, return the list of owners that have differing ownership.
-spec diff_nodes(chstate(), chstate()) -> [node()].
diff_nodes(State1,State2) ->
    AO = lists:zip(all_owners(State1),all_owners(State2)),
    AllDiff = [[N1,N2] || {{I,N1},{I,N2}} <- AO, N1 =/= N2],
    lists:usort(lists:flatten(AllDiff)).

-spec equal_rings(chstate(), chstate()) -> boolean().
equal_rings(_A=#chstate{chring=RA,meta=MA},_B=#chstate{chring=RB,meta=MB}) ->
    MDA = lists:sort(dict:to_list(MA)),
    MDB = lists:sort(dict:to_list(MB)),
    case MDA =:= MDB of
        false -> false;
        true -> RA =:= RB
    end.

%% @doc This is used only when this node is creating a brand new cluster.
-spec fresh() -> chstate().
fresh() ->
    % use this when starting a new cluster via this node
    fresh(node()).

%% @doc Equivalent to fresh/0 but allows specification of the local node name.
%%      Called by fresh/0, and otherwise only intended for testing purposes.
-spec fresh(NodeName :: term()) -> chstate().
fresh(NodeName) ->
    fresh(app_helper:get_env(riak_core, ring_creation_size), NodeName).

%% @doc Equivalent to fresh/1 but allows specification of the ring size.
%%      Called by fresh/1, and otherwise only intended for testing purposes.
-spec fresh(RingSize :: integer(), NodeName :: term()) -> chstate().
fresh(RingSize, NodeName) ->
    VClock=vclock:increment(NodeName, vclock:fresh()),
    #chstate{nodename=NodeName,
             clustername={NodeName, erlang:now()},
             members=[{NodeName, {valid, VClock}}],
             chring=chash:fresh(RingSize, NodeName),
             next=[],
             claimant=NodeName,
             seen=[{NodeName, VClock}],
             rvsn=VClock,
             vclock=VClock,
             meta=dict:new()}.

% @doc Return a value from the cluster metadata dict
-spec get_meta(Key :: term(), State :: chstate()) -> 
    {ok, term()} | undefined.
get_meta(Key, State) -> 
    case dict:find(Key, State#chstate.meta) of
        error -> undefined;
        {ok, M} -> {ok, M#meta_entry.value}
    end.

%% @doc Return the node that owns the given index.
-spec index_owner(State :: chstate(), Idx :: integer()) -> Node :: term().
index_owner(State, Idx) ->
    hd([Owner || {I, Owner} <- ?MODULE:all_owners(State), I =:= Idx]).

%% @doc Return all partition indices owned by the node executing this function.
-spec my_indices(State :: chstate()) -> [integer()].
my_indices(State) ->
    [I || {I,Owner} <- ?MODULE:all_owners(State), Owner =:= node()].

%% @doc Return the number of partitions in this Riak ring.
-spec num_partitions(State :: chstate()) -> integer().
num_partitions(State) ->
    chash:size(State#chstate.chring).

%% @doc Return the node that is responsible for a given chstate.
-spec owner_node(State :: chstate()) -> Node :: term().
owner_node(State) ->
    State#chstate.nodename.

%% @doc For a given object key, produce the ordered list of
%%      {partition,node} pairs that could be responsible for that object.
-spec preflist(Key :: binary(), State :: chstate()) ->
                               [{Index :: integer(), Node :: term()}].
preflist(Key, State) -> chash:successors(Key, State#chstate.chring).

%% @doc Return a randomly-chosen node from amongst the owners.
-spec random_node(State :: chstate()) -> Node :: term().
random_node(State) ->
    L = all_members(State),
    lists:nth(random:uniform(length(L)), L).

%% @doc Return a partition index not owned by the node executing this function.
%%      If this node owns all partitions, return any index.
-spec random_other_index(State :: chstate()) -> integer().
random_other_index(State) ->
    L = [I || {I,Owner} <- ?MODULE:all_owners(State), Owner =/= node()],
    case L of
        [] -> hd(my_indices(State));
        _ -> lists:nth(random:uniform(length(L)), L)
    end.

-spec random_other_index(State :: chstate(), Exclude :: [term()]) -> integer() | no_indices.
random_other_index(State, Exclude) when is_list(Exclude) ->
    L = [I || {I, Owner} <- ?MODULE:all_owners(State),
              Owner =/= node(),
              not lists:member(I, Exclude)],
    case L of
        [] -> no_indices;
        _ -> lists:nth(random:uniform(length(L)), L)
    end.

%% @doc Return a randomly-chosen node from amongst the owners other than this one.
-spec random_other_node(State :: chstate()) -> Node :: term() | no_node.
random_other_node(State) ->
    case lists:delete(node(), all_members(State)) of
        [] ->
            no_node;
        L ->
            lists:nth(random:uniform(length(L)), L)
    end.

%% @doc Incorporate another node's state into our view of the Riak world.
-spec reconcile(ExternState :: chstate(), MyState :: chstate()) ->
        {no_change, chstate()} | {new_ring, chstate()}.
reconcile(ExternState, MyState) ->
    case internal_reconcile(MyState, ExternState) of
        {false, State} ->
            {no_change, State};
        {true, State} ->
            {new_ring, State}
    end.

%% @doc  Rename OldNode to NewNode in a Riak ring.
-spec rename_node(State :: chstate(), OldNode :: atom(), NewNode :: atom()) ->
            chstate().
rename_node(State=#chstate{chring=Ring, nodename=ThisNode}, OldNode, NewNode) 
  when is_atom(OldNode), is_atom(NewNode)  ->
    State#chstate{
      chring=lists:foldl(
               fun({Idx, Owner}, AccIn) ->
                       case Owner of
                           OldNode -> 
                               chash:update(Idx, NewNode, AccIn);
                           _ -> AccIn
                       end
               end, Ring, riak_core_ring:all_owners(State)),
      nodename=case ThisNode of OldNode -> NewNode; _ -> ThisNode end,
      vclock=vclock:increment(NewNode, State#chstate.vclock)}.

%% @doc Determine the integer ring index responsible
%%      for a chash key.
-spec responsible_index(chash:index(), chstate()) -> integer().
responsible_index(ChashKey, #chstate{chring=Ring}) ->
    <<IndexAsInt:160/integer>> = ChashKey,
    chash:next_index(IndexAsInt, Ring).

-spec transfer_node(Idx :: integer(), Node :: term(), MyState :: chstate()) ->
           chstate().
transfer_node(Idx, Node, MyState) ->
    case chash:lookup(Idx, MyState#chstate.chring) of
        Node ->
            MyState;
        _ ->
            Me = MyState#chstate.nodename,
            VClock = vclock:increment(Me, MyState#chstate.vclock),
            CHRing = chash:update(Idx, Node, MyState#chstate.chring),
            MyState#chstate{vclock=VClock,chring=CHRing}
    end.

% @doc Set a key in the cluster metadata dict
-spec update_meta(Key :: term(), Val :: term(), State :: chstate()) -> chstate().
update_meta(Key, Val, State) ->
    Change = case dict:find(Key, State#chstate.meta) of
                 {ok, OldM} ->
                     Val /= OldM#meta_entry.value;
                 error ->
                     true
             end,
    if Change ->
            M = #meta_entry { 
              lastmod = calendar:datetime_to_gregorian_seconds(
                          calendar:universal_time()),
              value = Val
             },
            VClock = vclock:increment(State#chstate.nodename,
                                      State#chstate.vclock),
            State#chstate{vclock=VClock,
                          meta=dict:store(Key, M, State#chstate.meta)};
       true ->
            State
    end.

%% @doc Return the current claimant.
-spec claimant(State :: chstate()) -> node().
claimant(#chstate{claimant=Claimant}) ->
    Claimant.

%% @doc Returns the unique identifer for this cluster.
-spec cluster_name(State :: chstate()) -> term().
cluster_name(State) ->
    State#chstate.clustername.

%% @doc Returns the current membership status for a node in the cluster.
-spec member_status(State :: chstate(), Node :: node()) -> member_status().
member_status(#chstate{members=Members}, Node) ->
    member_status(Members, Node);
member_status(Members, Node) ->
    case orddict:find(Node, Members) of
        {ok, {Status, _}} ->
            Status;
        _ ->
            invalid
    end.

%% @doc Returns the current membership status for all nodes in the cluster.
-spec all_member_status(State :: chstate()) -> [{node(), member_status()}].
all_member_status(#chstate{members=Members}) ->
    [{Node, Status} || {Node, {Status, _VC}} <- Members, Status /= invalid].
    
add_member(PNode, State, Node) ->
    set_member(PNode, State, Node, valid).

remove_member(PNode, State, Node) ->
    set_member(PNode, State, Node, invalid).

leave_member(PNode, State, Node) ->
    set_member(PNode, State, Node, leaving).

exit_member(PNode, State, Node) ->
    set_member(PNode, State, Node, exiting).

%% @doc Return a list of all members of the cluster that are eligible to
%%      claim partitions.
-spec claiming_members(State :: chstate()) -> [Node :: node()].
claiming_members(#chstate{members=Members}) ->
    get_members(Members, [valid]).

%% @doc Set the node that is responsible for a given chstate.
-spec set_owner(State :: chstate(), Node :: node()) -> chstate().
set_owner(State, Node) ->
    State#chstate{nodename=Node}.

%% @doc Return all partition indices owned by a node.
-spec indices(State :: chstate(), Node :: node()) -> [integer()].
indices(State, Node) ->
    AllOwners = all_owners(State),
    [Idx || {Idx, Owner} <- AllOwners, Owner =:= Node].

%% @doc Return all indices that a node is scheduled to give to another.
disowning_indices(State, Node) ->
    [Idx || {Idx, Owner, _NextOwner, _Mods, _Status} <- State#chstate.next,
            Owner =:= Node].

%% @doc Returns a list of all pending ownership transfers.
pending_changes(State) ->
    %% For now, just return next directly.
    State#chstate.next.

%% @doc Return details for a pending partition ownership change.
-spec next_owner(State :: chstate(), Idx :: integer()) -> pending_change().
next_owner(State, Idx) ->
    case lists:keyfind(Idx, 1, State#chstate.next) of
        false ->
            {undefined, undefined, undefined};
        NInfo ->
            next_owner(NInfo)
    end.

%% @doc Return details for a pending partition ownership change.
-spec next_owner(State :: chstate(), Idx :: integer(),
                 Mod :: module()) -> pending_change().
next_owner(State, Idx, Mod) ->
    case lists:keyfind(Idx, 1, State#chstate.next) of
        false ->
            {undefined, undefined, undefined};
        {_, Owner, NextOwner, _Transfers, complete} ->
            {Owner, NextOwner, complete};
        {_, Owner, NextOwner, Transfers, _Status} ->
            case ordsets:is_element(Mod, Transfers) of
                true ->
                    {Owner, NextOwner, complete};
                false ->
                    {Owner, NextOwner, awaiting}
            end
    end.

%% @doc Returns true if all cluster members have seen the current ring.
-spec ring_ready(State :: chstate()) -> boolean().
ring_ready(State0) ->
    Owner = owner_node(State0),
    State = update_seen(Owner, State0),
    Seen = State#chstate.seen,
    Members = get_members(State#chstate.members),
    VClock = State#chstate.vclock,
    R = [begin
             case orddict:find(Node, Seen) of
                 error ->
                     false;
                 {ok, VC} ->
                     vclock:equal(VClock, VC)
             end
         end || Node <- Members],
    Ready = lists:all(fun(X) -> X =:= true end, R),
    Ready.

ring_ready() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ring_ready(Ring).

ring_ready_info(State0) ->
    Owner = owner_node(State0),
    State = update_seen(Owner, State0),
    Seen = State#chstate.seen,
    Members = get_members(State#chstate.members),
    RecentVC =
        orddict:fold(fun(_, VC, Recent) ->
                             case vclock:descends(VC, Recent) of
                                 true ->
                                     VC;
                                 false ->
                                     Recent
                             end
                     end, State#chstate.vclock, Seen),
    Outdated =
        orddict:filter(fun(_, VC) ->
                               not vclock:equal(VC, RecentVC)
                       end, Seen),
    Outdated.
    
%% @doc Marks a pending transfer as completed.
-spec handoff_complete(State :: chstate(), Idx :: integer(),
                       Mod :: module()) -> chstate().
handoff_complete(State, Idx, Mod) ->
    transfer_complete(State, Idx, Mod).

ring_changed(Node, State) ->
    internal_ring_changed(Node, State).

%% =========================================================================
%% Claimant rebalance/reassign logic
%% (TODO: Consider refactoring into riak_core_gossip or riak_core_claimant)
%% =========================================================================

%% @private
internal_ring_changed(Node, CState0) ->
    CState = update_seen(Node, CState0),
    case ring_ready(CState) of
        false ->
            CState;
        true ->
            {C1, CState2} = maybe_update_claimant(Node, CState),
            {C2, CState3} = maybe_update_ring(Node, CState2),
            {C3, CState4} = maybe_remove_exiting(Node, CState3),

            %% Start/stop converge and rebalance delay timers
            %% (converge delay)
            %%   -- Starts when claimant changes the ring
            %%   -- Stops when the ring converges (ring_ready)
            %% (rebalance delay)
            %%   -- Starts when next changes from empty to non-empty
            %%   -- Stops when next changes from non-empty to empty
            %%
            Changed    = (C1 or C2 or C3),
            IsClaimant = (CState4#chstate.claimant =:= Node),
            WasPending = ([] /= pending_changes(CState)),
            IsPending  = ([] /= pending_changes(CState4)),

            %% Outer case statement already checks for ring_ready
            case {IsClaimant, Changed} of
                {true, true} ->
                    riak_core_stat:update(converge_timer_end),
                    riak_core_stat:update(converge_timer_begin);
                {true, false} ->
                    riak_core_stat:update(converge_timer_end);
                _ ->
                    ok
            end,

            case {IsClaimant, WasPending, IsPending} of
                {true, false, true} ->
                    riak_core_stat:update(rebalance_timer_begin);
                {true, true, false} ->
                    riak_core_stat:update(rebalance_timer_end);
                _ ->
                    ok
            end,

            case Changed of
                true ->
                    VClock = vclock:increment(Node, CState4#chstate.vclock),
                    CState4#chstate{vclock=VClock};
                false ->
                    CState4
            end
    end.

%% @private
maybe_update_claimant(Node, CState) ->
    Members = get_members(CState#chstate.members, [valid, leaving]),
    Claimant = CState#chstate.claimant,
    RVsn = CState#chstate.rvsn,
    NextClaimant = hd(Members ++ [undefined]),
    ClaimantMissing = not lists:member(Claimant, Members),

    case {ClaimantMissing, NextClaimant} of
        {true, Node} ->
            %% Become claimant
            %%?assert(Node /= Claimant),
            RVsn2 = vclock:increment(Claimant, RVsn),
            CState2 = CState#chstate{claimant=Node, rvsn=RVsn2},
            {true, CState2};
        _ ->
            {false, CState}
    end.

%% @private
maybe_update_ring(Node, CState) ->
    Claimant = CState#chstate.claimant,
    case Claimant of
        Node ->
            case claiming_members(CState) of
                [] ->
                    %% Consider logging an error/warning here or even
                    %% intentionally crashing. This state makes no logical
                    %% sense given that it represents a cluster without any
                    %% active nodes.
                    {false, CState};
                _ ->
                    {Changed, CState2} = update_ring(Node, CState),
                    {Changed, CState2}
            end;
        _ ->
            {false, CState}
    end.

%% @private
maybe_remove_exiting(Node, CState) ->
    Claimant = CState#chstate.claimant,
    case Claimant of
        Node ->
            Exiting = get_members(CState#chstate.members, [exiting]),
            Changed = (Exiting /= []),
            CState2 =
                lists:foldl(fun(ENode, CState0) ->
                                    %% Tell exiting node to shutdown.
                                    riak_core_ring_manager:refresh_ring(ENode),
                                    set_member(Node, CState0, ENode,
                                               invalid, same_vclock)
                            end, CState, Exiting),
            {Changed, CState2};
        _ ->
            {false, CState}
    end.

%% @private
update_ring(CNode, CState) ->
    Next0 = CState#chstate.next,

    ?ROUT("Members: ~p~n", [CState#chstate.members]),
    ?ROUT("Updating ring :: next0 : ~p~n", [Next0]),

    %% Remove tuples from next for removed nodes
    InvalidMembers = get_members(CState#chstate.members, [invalid]),
    Next2 = lists:filter(fun(NInfo) ->
                                 {_, NextOwner, Status} = next_owner(NInfo),
                                 (Status =:= complete) or
                                     not lists:member(NextOwner, InvalidMembers)
                         end, Next0),
    CState2 = CState#chstate{next=Next2},

    %% Transfer ownership after completed handoff
    {RingChanged1, CState3} = transfer_ownership(CState2),
    ?ROUT("Updating ring :: next1 : ~p~n", [CState3#chstate.next]),

    %% Ressign leaving/inactive indices
    {RingChanged2, CState4} = reassign_indices(CState3),
    ?ROUT("Updating ring :: next2 : ~p~n", [CState4#chstate.next]),

    Next3 = rebalance_ring(CNode, CState4),
    NextChanged = (Next0 /= Next3),
    Changed = (NextChanged or RingChanged1 or RingChanged2),
    case Changed of
        true ->
            OldS = ordsets:from_list([{Idx,O,NO} || {Idx,O,NO,_,_} <- Next0]),
            NewS = ordsets:from_list([{Idx,O,NO} || {Idx,O,NO,_,_} <- Next3]),
            Diff = ordsets:subtract(NewS, OldS),
            [log(next, NChange) || NChange <- Diff],
            RVsn2 = vclock:increment(CNode, CState4#chstate.rvsn),
            ?ROUT("Updating ring :: next3 : ~p~n", [Next3]),
            {true, CState4#chstate{next=Next3, rvsn=RVsn2}};
        false ->
            {false, CState}
    end.

%% @private
transfer_ownership(CState=#chstate{next=Next}) ->
    %% Remove already completed and transfered changes
    Next2 = lists:filter(fun(NInfo={Idx, _, _, _, _}) ->
                                 {_, NewOwner, S} = next_owner(NInfo),
                                 not ((S == complete) and
                                      (index_owner(CState, Idx) =:= NewOwner))
                         end, Next),

    CState2 = lists:foldl(
                fun(NInfo={Idx, _, _, _, _}, CState0) ->
                        case next_owner(NInfo) of
                            {_, Node, complete} ->
                                log(ownership, {Idx, Node, CState0}),
                                riak_core_ring:transfer_node(Idx, Node, CState0);
                            _ ->
                                CState0
                        end
                end, CState, Next2),

    NextChanged = (Next2 /= Next),
    RingChanged = (all_owners(CState) /= all_owners(CState2)),
    Changed = (NextChanged or RingChanged),
    {Changed, CState2#chstate{next=Next2}}.

%% @private
reassign_indices(CState=#chstate{next=Next}) ->
    Invalid = get_members(CState#chstate.members, [invalid]),
    CState2 =
        lists:foldl(fun(Node, CState0) ->
                            remove_node(CState0, Node, invalid)
                    end, CState, Invalid),
    CState3 = case Next of
                  [] ->
                      Leaving = get_members(CState#chstate.members, [leaving]),
                      lists:foldl(fun(Node, CState0) ->
                                          remove_node(CState0, Node, leaving)
                                  end, CState2, Leaving);
                  _ ->
                      CState2
              end,
    Owners1 = all_owners(CState),
    Owners2 = all_owners(CState3),
    RingChanged = (Owners1 /= Owners2),
    NextChanged = (Next /= CState3#chstate.next),
    {RingChanged or NextChanged, CState3}.

%% @private
rebalance_ring(_CNode, CState=#chstate{next=[]}) ->
    Members = claiming_members(CState),
    CState2 = lists:foldl(fun(Node, Ring0) ->
                                  riak_core_gossip:claim_until_balanced(Ring0, Node)
                          end, CState, Members),
    Owners1 = all_owners(CState),
    Owners2 = all_owners(CState2),
    Owners3 = lists:zip(Owners1, Owners2),
    Next = [{Idx, PrevOwner, NewOwner, [], awaiting}
            || {{Idx, PrevOwner}, {Idx, NewOwner}} <- Owners3,
               PrevOwner /= NewOwner],
    Next;
rebalance_ring(_CNode, _CState=#chstate{next=Next}) ->
    Next.

%% @private
all_next_owners(#chstate{next=Next}) ->
    [{Idx, NextOwner} || {Idx, _, NextOwner, _, _} <- Next].

%% @private
change_owners(CState, Reassign) ->
    lists:foldl(fun({Idx, NewOwner}, CState0) ->
                        riak_core_ring:transfer_node(Idx, NewOwner, CState0)
                end, CState, Reassign).

%% @private
remove_node(CState, Node, Status) ->
    Indices = indices(CState, Node),
    remove_node(CState, Node, Status, Indices).

%% @private
remove_node(CState, _Node, _Status, []) ->
    CState;
remove_node(CState, Node, Status, Indices) ->
    %% ?debugFmt("Reassigning from ~p: ~p~n", [Node, indices(CState, Node)]),
    CStateT1 = change_owners(CState, all_next_owners(CState)),
    CStateT2 = riak_core_gossip:remove_from_cluster(CStateT1, Node),
    Owners1 = all_owners(CState),
    Owners2 = all_owners(CStateT2),
    Owners3 = lists:zip(Owners1, Owners2),
    RemovedIndices = case Status of
                         invalid ->
                             Indices;
                         leaving ->
                             []
                     end,
    Reassign = [{Idx, NewOwner} || {Idx, NewOwner} <- Owners2,
                                   lists:member(Idx, RemovedIndices)],
    Next = [{Idx, PrevOwner, NewOwner, [], awaiting}
            || {{Idx, PrevOwner}, {Idx, NewOwner}} <- Owners3,
               PrevOwner /= NewOwner,
               not lists:member(Idx, RemovedIndices)],

    [log(reassign, {Idx, NewOwner, CState}) || {Idx, NewOwner} <- Reassign],

    %% Unlike rebalance_ring, remove_node can be called when Next is non-empty,
    %% therefore we need to merge the values. Original Next has priority.
    Next2 = substitute(1, Next, CState#chstate.next),
    CState2 = change_owners(CState, Reassign),
    CState2#chstate{next=Next2}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @private
merge_meta(M1,M2) ->
    dict:merge(fun(_,D1,D2) -> pick_val(D1,D2) end, M1, M2).

%% @private
pick_val(M1,M2) ->
    case M1#meta_entry.lastmod > M2#meta_entry.lastmod of
        true -> M1;
        false -> M2
    end.                   

%% @private
internal_reconcile(State, OtherState) ->
    VNode = owner_node(State),
    State2 = update_seen(VNode, State),
    OtherState2 = update_seen(VNode, OtherState),
    Seen = reconcile_seen(State2, OtherState2),
    State3 = State2#chstate{seen=Seen},
    OtherState3 = OtherState2#chstate{seen=Seen},
    SeenChanged = not equal_seen(State, State3),

    VC1 = State3#chstate.vclock,
    VC2 = OtherState3#chstate.vclock,
    VC3 = vclock:merge([VC1, VC2]),
    %%io:format("V1: ~p~nV2: ~p~n", [VC1, VC2]),
    Newer = vclock:descends(VC1, VC2),
    Older = vclock:descends(VC2, VC1),
    Equal = equal_cstate(State3, OtherState3),
    case {Equal, Newer, Older} of
        {_, true, false} ->
            {SeenChanged, State3#chstate{vclock=VC3}};
        {_, false, true} ->
            {true, OtherState3#chstate{nodename=VNode, vclock=VC3}};
        {true, _, _} ->
            {SeenChanged, State3#chstate{vclock=VC3}};
        {_, true, true} ->
            throw("Equal vclocks, but cstate unequal");
        {_, false, false} ->
            State4 = reconcile_divergent(VNode, State3, OtherState3),
            {true, State4#chstate{nodename=VNode}}
    end.

%% @private
reconcile_divergent(VNode, StateA, StateB) ->
    VClock = vclock:increment(VNode, vclock:merge([StateA#chstate.vclock,
                                                   StateB#chstate.vclock])),
    Members = reconcile_members(StateA, StateB),
    Meta = merge_meta(StateA#chstate.meta, StateB#chstate.meta),
    NewState = reconcile_ring(StateA, StateB, get_members(Members)),
    NewState#chstate{vclock=VClock, members=Members, meta=Meta}.

%% @private
reconcile_members(StateA, StateB) ->
    orddict:merge(
      fun(_K, {Valid1, VC1}, {Valid2, VC2}) ->
              New1 = vclock:descends(VC1, VC2),
              New2 = vclock:descends(VC2, VC1),
              MergeVC = vclock:merge([VC1, VC2]),
              case {New1, New2} of
                  {true, false} ->
                      {Valid1, MergeVC};
                  {false, true} ->
                      {Valid2, MergeVC};
                  {_, _} ->
                      {merge_status(Valid1, Valid2), MergeVC}
              end
      end,
      StateA#chstate.members,
      StateB#chstate.members).

%% @private
reconcile_seen(StateA, StateB) ->
    orddict:merge(fun(_, VC1, VC2) ->
                          vclock:merge([VC1, VC2])
                  end, StateA#chstate.seen, StateB#chstate.seen).

%% @private
merge_next_status(complete, _) ->
    complete;
merge_next_status(_, complete) ->
    complete;
merge_next_status(awaiting, awaiting) ->
    awaiting.

%% @private
reconcile_next(Next1, Next2) ->
    lists:zipwith(fun({Idx, Owner, Node, Transfers1, Status1},
                      {Idx, Owner, Node, Transfers2, Status2}) ->
                          {Idx, Owner, Node,
                           ordsets:union(Transfers1, Transfers2),
                           merge_next_status(Status1, Status2)}
                  end, Next1, Next2).

%% @private
reconcile_divergent_next(BaseNext, OtherNext) ->
    MergedNext = substitute(1, BaseNext, OtherNext),
    lists:zipwith(fun({Idx, Owner1, Node1, Transfers1, Status1},
                      {Idx, Owner2, Node2, Transfers2, Status2}) ->
                          Same = ({Owner1, Node1} =:= {Owner2, Node2}),
                          case {Same, Status1, Status2} of
                              {false, _, _} ->
                                  {Idx, Owner1, Node1, Transfers1, Status1};
                              _ ->
                                  {Idx, Owner1, Node1,
                                   ordsets:union(Transfers1, Transfers2),
                                   merge_next_status(Status1, Status2)}
                          end
                  end, BaseNext, MergedNext).

%% @private
substitute(Idx, TL1, TL2) ->
    lists:map(fun(T) ->
                      Key = element(Idx, T),
                      case lists:keyfind(Key, Idx, TL2) of
                          false ->
                              T;
                          T2 ->
                              T2
                      end
              end, TL1).

%% @private
reconcile_ring(StateA=#chstate{claimant=Claimant1, rvsn=VC1, next=Next1},
               StateB=#chstate{claimant=Claimant2, rvsn=VC2, next=Next2},
               Members) ->
    V1Newer = vclock:descends(VC1, VC2),
    V2Newer = vclock:descends(VC2, VC1),
    EqualVC = (vclock:equal(VC1, VC2) and (Claimant1 =:= Claimant2)),
    case {EqualVC, V1Newer, V2Newer} of
        {true, _, _} ->
            %%?assertEqual(StateA#chstate.chring, StateB#chstate.chring),
            Next = reconcile_next(Next1, Next2),
            StateA#chstate{next=Next};
        {_, true, false} ->
            Next = reconcile_divergent_next(Next1, Next2),
            StateA#chstate{next=Next};
        {_, false, true} ->
            Next = reconcile_divergent_next(Next2, Next1),
            StateB#chstate{next=Next};
        {_, _, _} ->
            CValid1 = lists:member(Claimant1, Members),
            CValid2 = lists:member(Claimant2, Members),
            case {CValid1, CValid2} of
                {true, false} ->
                    Next = reconcile_divergent_next(Next1, Next2),
                    StateA#chstate{next=Next};
                {false, true} ->
                    Next = reconcile_divergent_next(Next2, Next1),
                    StateB#chstate{next=Next};
                {false, false} ->
                    throw("Neither claimant valid");
                {true, true} ->
                    %% This should never happen in normal practice.
                    %% But, we need to handle it for exceptional cases.
                    case Claimant1 < Claimant2 of
                        true ->
                            Next = reconcile_divergent_next(Next1, Next2),
                            StateA#chstate{next=Next};
                        false ->
                            Next = reconcile_divergent_next(Next2, Next1),
                            StateB#chstate{next=Next}
                    end
            end
    end.

%% @private
merge_status(invalid, _) ->
    invalid;
merge_status(_, invalid) ->
    invalid;
merge_status(valid, _) ->
    valid;
merge_status(_, valid) ->
    valid;
merge_status(exiting, _) ->
    exiting;
merge_status(_, exiting) ->
    exiting;
merge_status(leaving, _) ->
    leaving;
merge_status(_, leaving) ->
    leaving;
merge_status(_, _) ->
    invalid.

%% @private
transfer_complete(CState=#chstate{next=Next, vclock=VClock}, Idx, Mod) ->
    {Idx, Owner, NextOwner, Transfers, Status} = lists:keyfind(Idx, 1, Next),
    Transfers2 = ordsets:add_element(Mod, Transfers),
    VNodeMods =
        ordsets:from_list([VMod || {_, VMod} <- riak_core:vnode_modules()]),
    Status2 = case {Status, Transfers2} of
                  {complete, _} ->
                      complete;
                  {awaiting, VNodeMods} ->
                      complete;
                  _ ->
                      awaiting
              end,
    Next2 = lists:keyreplace(Idx, 1, Next,
                             {Idx, Owner, NextOwner, Transfers2, Status2}),
    VClock2 = vclock:increment(Owner, VClock),
    CState#chstate{next=Next2, vclock=VClock2}.

%% @private
next_owner({_, Owner, NextOwner, _Transfers, Status}) ->
    {Owner, NextOwner, Status}.

%% @private
get_members(Members) ->
    get_members(Members, [valid, leaving, exiting]).

%% @private
get_members(Members, Types) ->
    [Node || {Node, {V, _}} <- Members, lists:member(V, Types)].

%% @private
set_member(Node, CState, Member, Status) ->
    VClock = vclock:increment(Node, CState#chstate.vclock),
    CState2 = set_member(Node, CState, Member, Status, same_vclock),
    CState2#chstate{vclock=VClock}.

%% @private
set_member(Node, CState, Member, Status, same_vclock) ->
    Members2 = orddict:update(Member,
                              fun({_, VC}) ->
                                      {Status, vclock:increment(Node, VC)}
                              end,
                              {Status, vclock:increment(Node,
                                                        vclock:fresh())},
                              CState#chstate.members),
    CState#chstate{members=Members2}.

%% @private
update_seen(Node, CState=#chstate{vclock=VClock, seen=Seen}) ->
    Seen2 = orddict:update(Node,
                           fun(SeenVC) ->
                                   vclock:merge([SeenVC, VClock])
                           end,
                           VClock, Seen),
    CState#chstate{seen=Seen2}.

%% @private
equal_cstate(StateA, StateB) ->
    T1 = equal_members(StateA#chstate.members, StateB#chstate.members),
    T2 = vclock:equal(StateA#chstate.rvsn, StateB#chstate.rvsn),
    T3 = equal_seen(StateA, StateB),
    T4 = equal_rings(StateA, StateB),

    %% Clear fields checked manually and test remaining through equality.
    StateA2=StateA#chstate{nodename=ok, members=ok, vclock=ok, rvsn=ok,
                           seen=ok, chring=ok, meta=ok},
    StateB2=StateB#chstate{nodename=ok, members=ok, vclock=ok, rvsn=ok,
                           seen=ok, chring=ok, meta=ok},
    T5 = (StateA2 =:= StateB2),
    T1 and T2 and T3 and T4 and T5.

%% @private
equal_members(M1, M2) ->
    L = orddict:merge(fun(_, {Status1, VC1}, {Status2, VC2}) ->
                              (Status1 =:= Status2) andalso
                                  vclock:equal(VC1, VC2)
                      end, M1, M2),
    {_, R} = lists:unzip(L),
    lists:all(fun(X) -> X =:= true end, R).

%% @private
equal_seen(StateA, StateB) ->
    Seen1 = filtered_seen(StateA),
    Seen2 = filtered_seen(StateB),
    L = orddict:merge(fun(_, VC1, VC2) ->
                              vclock:equal(VC1, VC2)
                      end, Seen1, Seen2),
    {_, R} = lists:unzip(L),
    lists:all(fun(X) -> X =:= true end, R).

%% @private
filtered_seen(State=#chstate{seen=Seen}) ->
    case get_members(State#chstate.members) of
        [] ->
            Seen;
        Members ->
            orddict:filter(fun(N, _) -> lists:member(N, Members) end, Seen)
    end.

log(ownership, {Idx, NewOwner, CState}) ->
    Owner = index_owner(CState, Idx),
    lager:debug("(new-owner) ~b :: ~p -> ~p~n", [Idx, Owner, NewOwner]);
log(reassign, {Idx, NewOwner, CState}) ->
    Owner = index_owner(CState, Idx),
    lager:debug("(reassign) ~b :: ~p -> ~p~n", [Idx, Owner, NewOwner]);
log(next, {Idx, Owner, NewOwner}) ->
    lager:debug("(pending) ~b :: ~p -> ~p~n", [Idx, Owner, NewOwner]);
log(_, _) ->
    ok.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

sequence_test() ->
    I1 = 365375409332725729550921208179070754913983135744,
    I2 = 730750818665451459101842416358141509827966271488,
    A = fresh(4,a),
    B1 = A#chstate{nodename=b},
    B2 = transfer_node(I1, b, B1),
    ?assertEqual(B2, transfer_node(I1, b, B2)),
    {new_ring, A1} = reconcile(B1,A),
    C1 = A#chstate{nodename=c},
    C2 = transfer_node(I1, c, C1),
    {new_ring, A2} = reconcile(C2,A1),
    {new_ring, A3} = reconcile(B2,A2),
    C3 = transfer_node(I2,c,C2),
    {new_ring, C4} = reconcile(A3,C3),
    {new_ring, A4} = reconcile(C4,A3),
    {new_ring, B3} = reconcile(A4,B2),
    ?assertEqual(A4#chstate.chring, B3#chstate.chring),
    ?assertEqual(B3#chstate.chring, C4#chstate.chring).

param_fresh_test() ->
    application:set_env(riak_core,ring_creation_size,4),
    ?assertEqual(fresh(), fresh(4,node())),
    ?assertEqual(owner_node(fresh()),node()).

index_test() ->
    Ring0 = fresh(2,node()),
    Ring1 = transfer_node(0,x,Ring0),
    ?assertEqual(0,random_other_index(Ring0)),
    ?assertEqual(0,random_other_index(Ring1)),
    ?assertEqual(node(),index_owner(Ring0,0)),
    ?assertEqual(x,index_owner(Ring1,0)),
    ?assertEqual(lists:sort([x,node()]),lists:sort(diff_nodes(Ring0,Ring1))).

reconcile_test() ->
    Ring0 = fresh(2,node()),
    Ring1 = transfer_node(0,x,Ring0),
    ?assertEqual({no_change,Ring1},reconcile(fresh(2,someone_else),Ring1)),
    RingB0 = fresh(2,node()),
    RingB1 = transfer_node(0,x,RingB0),
    RingB2 = RingB1#chstate{nodename=b},
    ?assertEqual({no_change,RingB2},reconcile(Ring1,RingB2)).

metadata_inequality_test() ->
    Ring0 = fresh(2,node()),
    Ring1 = update_meta(key,val,Ring0),
    ?assertNot(equal_rings(Ring0,Ring1)),
    ?assertEqual(Ring1#chstate.meta,
                 merge_meta(Ring0#chstate.meta,Ring1#chstate.meta)),
    timer:sleep(1001), % ensure that lastmod is at least a second later
    Ring2 = update_meta(key,val2,Ring1),
    ?assertEqual(get_meta(key,Ring2),
                 get_meta(key,#chstate{meta=
                            merge_meta(Ring1#chstate.meta,
                                       Ring2#chstate.meta)})),
    ?assertEqual(get_meta(key,Ring2),
                 get_meta(key,#chstate{meta=
                            merge_meta(Ring2#chstate.meta,
                                       Ring1#chstate.meta)})).
rename_test() ->
    Ring0 = fresh(2, node()),
    Ring = rename_node(Ring0, node(), 'new@new'),
    ?assertEqual('new@new', owner_node(Ring)),
    ?assertEqual(['new@new'], all_members(Ring)).

exclusion_test() ->    
    Ring0 = fresh(2, node()),
    Ring1 = transfer_node(0,x,Ring0),
    ?assertEqual(0, random_other_index(Ring1,[730750818665451459101842416358141509827966271488])),
    ?assertEqual(no_indices, random_other_index(Ring1, [0])),
    ?assertEqual([{730750818665451459101842416358141509827966271488,node()},{0,x}],
                 preflist(<<1:160/integer>>, Ring1)).

random_other_node_test() ->
    Ring0 = fresh(2, node()),
    ?assertEqual(no_node, random_other_node(Ring0)),
    Ring1 = transfer_node(0, 'new@new', Ring0),
    ?assertEqual('new@new', random_other_node(Ring1)).

-endif.
