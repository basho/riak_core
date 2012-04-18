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
         get_buckets/1,
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
         legacy_ring/1,
         legacy_reconcile/2,
         upgrade/1,
         downgrade/2,
         set_tainted/1,
         check_tainted/2,
         nearly_equal/2,
         claimant/1,
         member_status/2,
         pretty_print/2,
         all_member_status/1,
         update_member_meta/5,
         get_member_meta/3,
         add_member/3,
         remove_member/3,
         leave_member/3,
         exit_member/3,
         down_member/3,
         active_members/1,
         claiming_members/1,
         ready_members/1,
         random_other_active_node/1,
         down_members/1,
         set_owner/2,
         indices/2,
         future_indices/2,
         disowning_indices/2,
         pending_changes/1,
         next_owner/2,
         next_owner/3,
         handoff_complete/3,
         ring_ready/0,
         ring_ready/1,
         ring_ready_info/1,
         ring_changed/2,
         set_cluster_name/2,
         reconcile_names/2,
         reconcile_members/2,
         is_primary/2]).

-export_type([riak_core_ring/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(ROUT(S,A),ok).
%%-define(ROUT(S,A),?debugFmt(S,A)).
%%-define(ROUT(S,A),io:format(S,A)).

-define(CHSTATE, #chstate_v2).
-record(chstate_v2, {
    nodename :: node(),          % the Node responsible for this chstate
    vclock   :: vclock:vclock(), % for this chstate object, entries are
                                 % {Node, Ctr}
    chring   :: chash:chash(),   % chash ring of {IndexAsInt, Node} mappings
    meta     :: dict(),          % dict of cluster-wide other data (primarily
                                 % bucket N-value, etc)

    clustername :: {node(), term()}, 
    next     :: [{integer(), node(), node(), [module()], awaiting | complete}],
    members  :: [{node(), {member_status(), vclock:vclock(), []}}],
    claimant :: node(),
    seen     :: [{node(), vclock:vclock()}],
    rvsn     :: vclock:vclock()
}). 

%% Legacy chstate
-record(chstate, {
    nodename :: node(), % the Node responsible for this chstate
    vclock,   % for this chstate object, entries are {Node, Ctr}
    chring :: chash:chash(),   % chash ring of {IndexAsInt, Node} mappings
    meta      % dict of cluster-wide other data (primarily bucket N-value, etc)
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
-type riak_core_ring() :: ?CHSTATE{}.
-type chstate() :: riak_core_ring().

-type pending_change() :: {Owner :: node(),
                           NextOwner :: node(),
                           awaiting | complete}
                        | {undefined, undefined, undefined}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Returns true if the given ring is a legacy ring.
legacy_ring(#chstate{}) ->
    true;
legacy_ring(_) ->
    false.

%% @doc Upgrade old ring structures to the latest format.
upgrade(Old=?CHSTATE{}) ->
    Old;
upgrade(Old=#chstate{}) ->
    #chstate{nodename=Node,
             vclock=VC,
             chring=Ring,
             meta=Meta} = Old,
    New1 = ?CHSTATE{nodename=Node,
                    vclock=VC,
                    chring=Ring,
                    meta=Meta,
                    clustername=undefined,
                    next=[],
                    members=[],
                    claimant=undefined,
                    seen=[],
                    rvsn=VC},
    MemberVC = vclock:increment(Node, vclock:fresh()),
    Members = [{Member, {valid, MemberVC, []}}
               || Member <- chash:members(Ring)],
    New2 = New1?CHSTATE{members=Members},
    case node() of
        Node ->
            GVsn = riak_core_gossip:gossip_version(),
            update_member_meta(Node, New2, Node,
                               gossip_vsn, GVsn, same_vclock);
        _ ->
            New2
    end.

%% @doc Downgrade the latest ring structure to a specified version.
downgrade(1,?CHSTATE{nodename=Node,
                     vclock=VC,
                     chring=Ring,
                     meta=Meta}) ->
    #chstate{nodename=Node,
             vclock=VC,
             chring=Ring,
             meta=Meta};
downgrade(2,State=?CHSTATE{}) ->
    State.

set_tainted(Ring) ->
    update_meta(riak_core_ring_tainted, true, Ring).

check_tainted(#chstate{}, _Msg) ->
    %% Legacy ring is never tainted
    ok;
check_tainted(Ring=?CHSTATE{}, Msg) ->
    Exit = app_helper:get_env(riak_core, exit_when_tainted, false),
    case {get_meta(riak_core_ring_tainted, Ring), Exit} of
        {{ok, true}, true} ->
            riak_core:stop(Msg),
            ok;
        {{ok, true}, false} ->
            lager:error(Msg),
            ok;
        _ ->
            ok
    end.

%% @doc Verify that the two rings are identical expect that metadata can
%%      differ and RingB's vclock is allowed to be equal or a direct
%%      descendant of RingA's vclock. This matches the changes that the
%%      fix-up logic may make to a ring.
nearly_equal(RingA, RingB) ->
    TestVC = vclock:descends(RingB?CHSTATE.vclock, RingA?CHSTATE.vclock),
    RingA2 = RingA?CHSTATE{vclock=[], meta=[]},
    RingB2 = RingB?CHSTATE{vclock=[], meta=[]},
    TestRing = (RingA2 =:= RingB2),
    TestVC and TestRing.

%% @doc Determine if a given Index/Node `IdxNode' combination is a
%%      primary.
-spec is_primary(chstate(), {integer(), node()}) -> boolean().
is_primary(Ring, IdxNode) ->
    Owners = all_owners(Ring),
    lists:member(IdxNode, Owners).

%% @doc Produce a list of all nodes that are members of the cluster
-spec all_members(State :: chstate()) -> [Node :: term()].
all_members(?CHSTATE{members=Members}) ->
    get_members(Members).

%% @doc Produce a list of all active (not marked as down) cluster members
active_members(?CHSTATE{members=Members}) ->
    get_members(Members, [joining, valid, leaving, exiting]).

%% @doc Returns a list of members guaranteed safe for requests
ready_members(?CHSTATE{members=Members}) ->
    get_members(Members, [valid, leaving]).

%% @doc Provide all ownership information in the form of {Index,Node} pairs.
-spec all_owners(State :: chstate()) -> [{Index :: integer(), Node :: term()}].
all_owners(State) ->
    chash:nodes(State?CHSTATE.chring).

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
equal_rings(_A=?CHSTATE{chring=RA,meta=MA},_B=?CHSTATE{chring=RB,meta=MB}) ->
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
    GossipVsn = riak_core_gossip:gossip_version(),
    ?CHSTATE{nodename=NodeName,
             clustername={NodeName, erlang:now()},
             members=[{NodeName, {valid, VClock, [{gossip_vsn, GossipVsn}]}}],
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
    case dict:find(Key, State?CHSTATE.meta) of
        error -> undefined;
        {ok, M} -> {ok, M#meta_entry.value}
    end.

%% @doc return the names of all the custom buckets stored in the ring.
-spec get_buckets(State :: chstate()) -> [term()].
get_buckets(State) ->
    Keys = dict:fetch_keys(State?CHSTATE.meta),
    lists:foldl(
        fun({bucket, Bucket}, Acc) ->
                [Bucket|Acc];
            (_, Acc) ->
                Acc
        end, [], Keys).

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
    chash:size(State?CHSTATE.chring).

%% @doc Return the node that is responsible for a given chstate.
-spec owner_node(State :: chstate()) -> Node :: term().
owner_node(State) ->
    State?CHSTATE.nodename.

%% @doc For a given object key, produce the ordered list of
%%      {partition,node} pairs that could be responsible for that object.
-spec preflist(Key :: binary(), State :: chstate()) ->
                               [{Index :: integer(), Node :: term()}].
preflist(Key, State) -> chash:successors(Key, State?CHSTATE.chring).

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

%% @doc Return a randomly-chosen active node other than this one.
-spec random_other_active_node(State :: chstate()) -> Node :: term() | no_node.
random_other_active_node(State) ->
    case lists:delete(node(), active_members(State)) of
        [] ->
            no_node;
        L ->
            lists:nth(random:uniform(length(L)), L)
    end.

%% @doc Incorporate another node's state into our view of the Riak world.
-spec reconcile(ExternState :: chstate(), MyState :: chstate()) ->
        {no_change, chstate()} | {new_ring, chstate()}.
reconcile(ExternState, MyState) ->
    check_tainted(ExternState,
                  "Error: riak_core_ring/reconcile :: "
                  "reconciling tainted external ring"),
    check_tainted(MyState, 
                  "Error: riak_core_ring/reconcile :: "
                  "reconciling tainted internal ring"),
    case internal_reconcile(MyState, ExternState) of
        {false, State} ->
            {no_change, State};
        {true, State} ->
            {new_ring, State}
    end.

%% @doc  Rename OldNode to NewNode in a Riak ring.
-spec rename_node(State :: chstate(), OldNode :: atom(), NewNode :: atom()) ->
            chstate().
rename_node(State=?CHSTATE{chring=Ring, nodename=ThisNode, members=Members,
                           seen=Seen}, OldNode, NewNode) 
  when is_atom(OldNode), is_atom(NewNode)  ->
    State?CHSTATE{
      chring=lists:foldl(
               fun({Idx, Owner}, AccIn) ->
                       case Owner of
                           OldNode -> 
                               chash:update(Idx, NewNode, AccIn);
                           _ -> AccIn
                       end
               end, Ring, riak_core_ring:all_owners(State)),
      members=proplists:substitute_aliases([{OldNode, NewNode}], Members),
      seen=proplists:substitute_aliases([{OldNode, NewNode}], Seen),
      nodename=case ThisNode of OldNode -> NewNode; _ -> ThisNode end,
      vclock=vclock:increment(NewNode, State?CHSTATE.vclock)}.

%% @doc Determine the integer ring index responsible
%%      for a chash key.
-spec responsible_index(chash:index(), chstate()) -> integer().
responsible_index(ChashKey, ?CHSTATE{chring=Ring}) ->
    <<IndexAsInt:160/integer>> = ChashKey,
    chash:next_index(IndexAsInt, Ring).

-spec transfer_node(Idx :: integer(), Node :: term(), MyState :: chstate()) ->
           chstate().
transfer_node(Idx, Node, MyState) ->
    case chash:lookup(Idx, MyState?CHSTATE.chring) of
        Node ->
            MyState;
        _ ->
            Me = MyState?CHSTATE.nodename,
            VClock = vclock:increment(Me, MyState?CHSTATE.vclock),
            CHRing = chash:update(Idx, Node, MyState?CHSTATE.chring),
            MyState?CHSTATE{vclock=VClock,chring=CHRing}
    end.

% @doc Set a key in the cluster metadata dict
-spec update_meta(Key :: term(), Val :: term(), State :: chstate()) -> chstate().
update_meta(Key, Val, State) ->
    Change = case dict:find(Key, State?CHSTATE.meta) of
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
            VClock = vclock:increment(State?CHSTATE.nodename,
                                      State?CHSTATE.vclock),
            State?CHSTATE{vclock=VClock,
                          meta=dict:store(Key, M, State?CHSTATE.meta)};
       true ->
            State
    end.

%% @doc Return the current claimant.
-spec claimant(State :: chstate()) -> node().
claimant(?CHSTATE{claimant=Claimant}) ->
    Claimant.

%% @doc Returns the unique identifer for this cluster.
-spec cluster_name(State :: chstate()) -> term().
cluster_name(State) ->
    State?CHSTATE.clustername.

%% @doc Sets the unique identifer for this cluster.
set_cluster_name(State, Name) ->
    State?CHSTATE{clustername=Name}.

reconcile_names(RingA=?CHSTATE{clustername=NameA},
                RingB=?CHSTATE{clustername=NameB}) ->
    case (NameA =:= undefined) or (NameB =:= undefined) of
        true ->
            {RingA?CHSTATE{clustername=undefined},
             RingB?CHSTATE{clustername=undefined}};
        false ->
            {RingA, RingB}
    end.
    
%% @doc Returns the current membership status for a node in the cluster.
-spec member_status(State :: chstate(), Node :: node()) -> member_status().
member_status(?CHSTATE{members=Members}, Node) ->
    member_status(Members, Node);
member_status(Members, Node) ->
    case orddict:find(Node, Members) of
        {ok, {Status, _, _}} ->
            Status;
        _ ->
            invalid
    end.

%% @doc Returns the current membership status for all nodes in the cluster.
-spec all_member_status(State :: chstate()) -> [{node(), member_status()}].
all_member_status(?CHSTATE{members=Members}) ->
    [{Node, Status} || {Node, {Status, _VC, _}} <- Members, Status /= invalid].

get_member_meta(State, Member, Key) ->
    case orddict:find(Member, State?CHSTATE.members) of
        error -> undefined;
        {ok, {_, _, Meta}} ->
            case orddict:find(Key, Meta) of
                error ->
                    undefined;
                {ok, Value} ->
                    Value
            end
    end.
    
%% @doc Set a key in the member metadata orddict
update_member_meta(Node, State, Member, Key, Val) ->
    VClock = vclock:increment(Node, State?CHSTATE.vclock),
    State2 = update_member_meta(Node, State, Member, Key, Val, same_vclock),
    State2?CHSTATE{vclock=VClock}.

update_member_meta(Node, State, Member, Key, Val, same_vclock) ->
    Members = State?CHSTATE.members,
    case orddict:is_key(Member, Members) of
        true ->
            Members2 = orddict:update(Member,
                                      fun({Status, VC, MD}) ->
                                              {Status,
                                               vclock:increment(Node, VC),
                                               orddict:store(Key, Val, MD)}
                                      end,
                                      Members),
            State?CHSTATE{members=Members2};
        false ->
            State
    end.

add_member(PNode, State, Node) ->
    set_member(PNode, State, Node, joining).

remove_member(PNode, State, Node) ->
    set_member(PNode, State, Node, invalid).

leave_member(PNode, State, Node) ->
    set_member(PNode, State, Node, leaving).

exit_member(PNode, State, Node) ->
    set_member(PNode, State, Node, exiting).

down_member(PNode, State, Node) ->
    set_member(PNode, State, Node, down).

%% @doc Return a list of all members of the cluster that are eligible to
%%      claim partitions.
-spec claiming_members(State :: chstate()) -> [Node :: node()].
claiming_members(?CHSTATE{members=Members}) ->
    get_members(Members, [joining, valid, down]).

%% @doc Return a list of all members of the cluster that are marked as down.
-spec down_members(State :: chstate()) -> [Node :: node()].
down_members(?CHSTATE{members=Members}) ->
    get_members(Members, [down]).

%% @doc Set the node that is responsible for a given chstate.
-spec set_owner(State :: chstate(), Node :: node()) -> chstate().
set_owner(State, Node) ->
    State?CHSTATE{nodename=Node}.

%% @doc Return all partition indices owned by a node.
-spec indices(State :: chstate(), Node :: node()) -> [integer()].
indices(State, Node) ->
    AllOwners = all_owners(State),
    [Idx || {Idx, Owner} <- AllOwners, Owner =:= Node].

%% @doc Return all partition indices that will be owned by a node after all
%%      pending ownership transfers have completed.
-spec future_indices(State :: chstate(), Node :: node()) -> [integer()].
future_indices(State, Node) ->
    FutureState = change_owners(State, all_next_owners(State)),
    indices(FutureState, Node).

%% @doc Return all indices that a node is scheduled to give to another.
disowning_indices(State, Node) ->
    [Idx || {Idx, Owner, _NextOwner, _Mods, _Status} <- State?CHSTATE.next,
            Owner =:= Node].

%% @doc Returns a list of all pending ownership transfers.
pending_changes(State) ->
    %% For now, just return next directly.
    State?CHSTATE.next.

%% @doc Return details for a pending partition ownership change.
-spec next_owner(State :: chstate(), Idx :: integer()) -> pending_change().
next_owner(State, Idx) ->
    case lists:keyfind(Idx, 1, State?CHSTATE.next) of
        false ->
            {undefined, undefined, undefined};
        NInfo ->
            next_owner(NInfo)
    end.

%% @doc Return details for a pending partition ownership change.
-spec next_owner(State :: chstate(), Idx :: integer(),
                 Mod :: module()) -> pending_change().
next_owner(State, Idx, Mod) ->
    case lists:keyfind(Idx, 1, State?CHSTATE.next) of
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
    check_tainted(State0,
                  "Error: riak_core_ring/ring_ready called on tainted ring"),
    Owner = owner_node(State0),
    State = update_seen(Owner, State0),
    Seen = State?CHSTATE.seen,
    Members = get_members(State?CHSTATE.members, [valid, leaving, exiting]),
    VClock = State?CHSTATE.vclock,
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
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    ring_ready(Ring).

ring_ready_info(State0) ->
    Owner = owner_node(State0),
    State = update_seen(Owner, State0),
    Seen = State?CHSTATE.seen,
    Members = get_members(State?CHSTATE.members, [valid, leaving, exiting]),
    RecentVC =
        orddict:fold(fun(_, VC, Recent) ->
                             case vclock:descends(VC, Recent) of
                                 true ->
                                     VC;
                                 false ->
                                     Recent
                             end
                     end, State?CHSTATE.vclock, Seen),
    Outdated =
        orddict:filter(fun(Node, VC) ->
                               (not vclock:equal(VC, RecentVC))
                                   and lists:member(Node, Members)
                       end, Seen),
    Outdated.
    
%% @doc Marks a pending transfer as completed.
-spec handoff_complete(State :: chstate(), Idx :: integer(),
                       Mod :: module()) -> chstate().
handoff_complete(State, Idx, Mod) ->
    transfer_complete(State, Idx, Mod).

ring_changed(Node, State) ->
    check_tainted(State,
                  "Error: riak_core_ring/ring_changed called on tainted ring"),
    internal_ring_changed(Node, State).

pretty_print(Ring, Opts) ->
    OptNumeric = lists:member(numeric, Opts),
    OptLegend = lists:member(legend, Opts),
    Out = proplists:get_value(out, Opts, standard_io),
    TargetN = proplists:get_value(target_n, Opts,
                                  app_helper:get_env(riak_core, target_n_val)),

    Owners = riak_core_ring:all_members(Ring),
    Indices = riak_core_ring:all_owners(Ring),
    RingSize = length(Indices),
    Numeric = OptNumeric or (length(Owners) > 26),
    case Numeric of
        true ->
            Ids = [integer_to_list(N) || N <- lists:seq(1, length(Owners))];
        false ->
            Ids = [[Letter] || Letter <- lists:seq(97, 96+length(Owners))]
    end,
    Names = lists:zip(Owners, Ids),
    case OptLegend of
        true ->
            io:format(Out, "~36..=s Nodes ~36..=s~n", ["", ""]),
            [begin
                 NodeIndices = [Idx || {Idx,Owner} <- Indices,
                                       Owner =:= Node],
                 RingPercent = length(NodeIndices) * 100 / RingSize,
                 io:format(Out, "Node ~s: ~5.1f% ~s~n",
                           [Name, RingPercent, Node])
             end || {Node, Name} <- Names],
            io:format(Out, "~36..=s Ring ~37..=s~n", ["", ""]);
        false ->
            ok
    end,

    case Numeric of
        true ->
            Ownership =
                [orddict:fetch(Owner, Names) || {_Idx, Owner} <- Indices],
            io:format(Out, "~p~n", [Ownership]);
        false ->
            lists:foldl(fun({_, Owner}, N) ->
                                Name = orddict:fetch(Owner, Names),
                                case N rem TargetN of
                                    0 ->
                                        io:format(Out, "~s|", [[Name]]);
                                    _ ->
                                        io:format(Out, "~s", [[Name]])
                                end,
                                N+1
                        end, 1, Indices),
            io:format(Out, "~n", [])
    end.

%% ===================================================================
%% Legacy reconciliation
%% ===================================================================

%% @doc Incorporate another node's state into our view of the Riak world.
legacy_reconcile(ExternState, MyState) ->
    case vclock:equal(MyState#chstate.vclock, vclock:fresh()) of
        true -> 
            {new_ring, #chstate{nodename=MyState#chstate.nodename,
                                vclock=ExternState#chstate.vclock,
                                chring=ExternState#chstate.chring,
                                meta=ExternState#chstate.meta}};
        false ->
            case ancestors([ExternState, MyState]) of
                [OlderState] ->
                    case vclock:equal(OlderState#chstate.vclock,
                                      MyState#chstate.vclock) of
                        true ->
                            {new_ring,
                             #chstate{nodename=MyState#chstate.nodename,
                                      vclock=ExternState#chstate.vclock,
                                      chring=ExternState#chstate.chring,
                                      meta=ExternState#chstate.meta}};
                        false -> {no_change, MyState}
                    end;
                [] -> 
                    case legacy_equal_rings(ExternState,MyState) of
                        true -> {no_change, MyState};
                        false -> {new_ring,
                                  legacy_reconcile(MyState#chstate.nodename,
                                                   ExternState, MyState)}
                    end
            end
    end.

%% @private
ancestors(RingStates) ->
    Ancest = [[O2 || O2 <- RingStates,
     vclock:descends(O1#chstate.vclock,O2#chstate.vclock),
     (vclock:descends(O2#chstate.vclock,O1#chstate.vclock) == false)]
 || O1 <- RingStates],
    lists:flatten(Ancest).

%% @private
legacy_equal_rings(_A=#chstate{chring=RA,meta=MA},
                   _B=#chstate{chring=RB,meta=MB}) ->
    MDA = lists:sort(dict:to_list(MA)),
    MDB = lists:sort(dict:to_list(MB)),
    case MDA =:= MDB of
        false -> false;
        true -> RA =:= RB
    end.

%% @private
% @doc If two states are mutually non-descendant, merge them anyway.
%      This can cause a bit of churn, but should converge.
% @spec legacy_reconcile(MyNodeName :: term(),
%                 StateA :: chstate(), StateB :: chstate())
%              -> chstate()
legacy_reconcile(MyNodeName, StateA, StateB) ->
    % take two states (non-descendant) and merge them
    VClock = vclock:increment(MyNodeName,
        vclock:merge([StateA#chstate.vclock,
                StateB#chstate.vclock])),
    CHRing = chash:merge_rings(StateA#chstate.chring,StateB#chstate.chring),
    Meta = merge_meta(StateA#chstate.meta, StateB#chstate.meta),
    #chstate{nodename=MyNodeName,
             vclock=VClock,
             chring=CHRing,
             meta=Meta}.

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
            {C2, CState3} = maybe_handle_joining(Node, CState2),
            case C2 of
                true ->
                    Changed = true,
                    CState5 = CState3;
                false ->
                    {C3, CState4} = maybe_update_ring(Node, CState3),
                    {C4, CState5} = maybe_remove_exiting(Node, CState4),
                    Changed = (C1 or C2 or C3 or C4)
            end,

            %% Start/stop converge and rebalance delay timers
            %% (converge delay)
            %%   -- Starts when claimant changes the ring
            %%   -- Stops when the ring converges (ring_ready)
            %% (rebalance delay)
            %%   -- Starts when next changes from empty to non-empty
            %%   -- Stops when next changes from non-empty to empty
            %%
            IsClaimant = (CState5?CHSTATE.claimant =:= Node),
            WasPending = ([] /= pending_changes(CState)),
            IsPending  = ([] /= pending_changes(CState5)),

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

            %% Set cluster name if it is undefined
            case {IsClaimant, cluster_name(CState5)} of
                {true, undefined} ->
                    ClusterName = {Node, erlang:now()},
                    riak_core_util:rpc_every_member(riak_core_ring_manager,
                                                    set_cluster_name,
                                                    [ClusterName],
                                                    1000),
                    ok;
                _ ->
                    ClusterName = cluster_name(CState5),
                    ok
            end,

            case Changed of
                true ->
                    VClock = vclock:increment(Node, CState5?CHSTATE.vclock),
                    CState5?CHSTATE{vclock=VClock, clustername=ClusterName};
                false ->
                    CState5
            end
    end.

%% @private
maybe_update_claimant(Node, CState) ->
    Members = get_members(CState?CHSTATE.members, [valid, leaving]),
    Claimant = CState?CHSTATE.claimant,
    RVsn = CState?CHSTATE.rvsn,
    NextClaimant = hd(Members ++ [undefined]),
    ClaimantMissing = not lists:member(Claimant, Members),

    case {ClaimantMissing, NextClaimant} of
        {true, Node} ->
            %% Become claimant
            RVsn2 = vclock:increment(Claimant, RVsn),
            CState2 = CState?CHSTATE{claimant=Node, rvsn=RVsn2},
            {true, CState2};
        _ ->
            {false, CState}
    end.

%% @private
maybe_update_ring(Node, CState) ->
    Claimant = CState?CHSTATE.claimant,
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
    Claimant = CState?CHSTATE.claimant,
    case Claimant of
        Node ->
            %% Change exiting nodes to invalid, skipping this node.
            Exiting = get_members(CState?CHSTATE.members, [exiting]) -- [Node],
            Changed = (Exiting /= []),
            CState2 =
                lists:foldl(fun(ENode, CState0) ->
                                    %% Tell exiting node to shutdown.
                                    CName = cluster_name(CState),
                                    riak_core_ring_manager:refresh_ring(ENode,
                                                                        CName),
                                    set_member(Node, CState0, ENode,
                                               invalid, same_vclock)
                            end, CState, Exiting),
            {Changed, CState2};
        _ ->
            {false, CState}
    end.

%% @private
maybe_handle_joining(Node, CState) ->
    Claimant = CState?CHSTATE.claimant,
    case Claimant of
        Node ->
            Joining = get_members(CState?CHSTATE.members, [joining]),
            Changed = (Joining /= []),
            CState2 =
                lists:foldl(fun(JNode, CState0) ->
                                    set_member(Node, CState0, JNode,
                                               valid, same_vclock)
                            end, CState, Joining),
            {Changed, CState2};
        _ ->
            {false, CState}
    end.

%% @private
update_ring(CNode, CState) ->
    Next0 = CState?CHSTATE.next,

    ?ROUT("Members: ~p~n", [CState?CHSTATE.members]),
    ?ROUT("Updating ring :: next0 : ~p~n", [Next0]),

    %% Remove tuples from next for removed nodes
    InvalidMembers = get_members(CState?CHSTATE.members, [invalid]),
    Next2 = lists:filter(fun(NInfo) ->
                                 {Owner, NextOwner, _} = next_owner(NInfo),
                                 not lists:member(Owner, InvalidMembers) and
                                 not lists:member(NextOwner, InvalidMembers)
                         end, Next0),
    CState2 = CState?CHSTATE{next=Next2},

    %% Transfer ownership after completed handoff
    {RingChanged1, CState3} = transfer_ownership(CState2),
    ?ROUT("Updating ring :: next1 : ~p~n", [CState3?CHSTATE.next]),

    %% Ressign leaving/inactive indices
    {RingChanged2, CState4} = reassign_indices(CState3),
    ?ROUT("Updating ring :: next2 : ~p~n", [CState4?CHSTATE.next]),

    %% Rebalance the ring as necessary
    Next3 = rebalance_ring(CNode, CState4),
    lager:debug("Pending ownership transfers: ~b~n",
                [length(pending_changes(CState4))]),
    
    %% Remove transfers to/from down nodes
    Next4 = handle_down_nodes(CState4, Next3),

    NextChanged = (Next0 /= Next4),
    Changed = (NextChanged or RingChanged1 or RingChanged2),
    case Changed of
        true ->
            OldS = ordsets:from_list([{Idx,O,NO} || {Idx,O,NO,_,_} <- Next0]),
            NewS = ordsets:from_list([{Idx,O,NO} || {Idx,O,NO,_,_} <- Next4]),
            Diff = ordsets:subtract(NewS, OldS),
            [log(next, NChange) || NChange <- Diff],
            RVsn2 = vclock:increment(CNode, CState4?CHSTATE.rvsn),
            ?ROUT("Updating ring :: next3 : ~p~n", [Next4]),
            {true, CState4?CHSTATE{next=Next4, rvsn=RVsn2}};
        false ->
            {false, CState}
    end.

%% @private
transfer_ownership(CState=?CHSTATE{next=Next}) ->
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
                                riak_core_ring:transfer_node(Idx, Node,
                                                             CState0);
                            _ ->
                                CState0
                        end
                end, CState, Next2),

    NextChanged = (Next2 /= Next),
    RingChanged = (all_owners(CState) /= all_owners(CState2)),
    Changed = (NextChanged or RingChanged),
    {Changed, CState2?CHSTATE{next=Next2}}.

%% @private
reassign_indices(CState=?CHSTATE{next=Next}) ->
    Invalid = get_members(CState?CHSTATE.members, [invalid]),
    CState2 =
        lists:foldl(fun(Node, CState0) ->
                            remove_node(CState0, Node, invalid)
                    end, CState, Invalid),
    CState3 = case Next of
                  [] ->
                      Leaving = get_members(CState?CHSTATE.members, [leaving]),
                      lists:foldl(fun(Node, CState0) ->
                                          remove_node(CState0, Node, leaving)
                                  end, CState2, Leaving);
                  _ ->
                      CState2
              end,
    Owners1 = all_owners(CState),
    Owners2 = all_owners(CState3),
    RingChanged = (Owners1 /= Owners2),
    NextChanged = (Next /= CState3?CHSTATE.next),
    {RingChanged or NextChanged, CState3}.

%% @private
rebalance_ring(_CNode, CState=?CHSTATE{next=[]}) ->
    Members = claiming_members(CState),
    CState2 = lists:foldl(fun(Node, Ring0) ->
                                  riak_core_gossip:claim_until_balanced(Ring0,
                                                                        Node)
                          end, CState, Members),
    Owners1 = all_owners(CState),
    Owners2 = all_owners(CState2),
    Owners3 = lists:zip(Owners1, Owners2),
    Next = [{Idx, PrevOwner, NewOwner, [], awaiting}
            || {{Idx, PrevOwner}, {Idx, NewOwner}} <- Owners3,
               PrevOwner /= NewOwner],
    Next;
rebalance_ring(_CNode, _CState=?CHSTATE{next=Next}) ->
    Next.

%% @private
handle_down_nodes(CState, Next) ->
    LeavingMembers = get_members(CState?CHSTATE.members, [leaving, invalid]),
    DownMembers = get_members(CState?CHSTATE.members, [down]),
    Next2 = [begin
                 OwnerLeaving = lists:member(O, LeavingMembers),
                 NextDown = lists:member(NO, DownMembers),
                 case (OwnerLeaving and NextDown) of
                     true ->
                         Active = riak_core_ring:active_members(CState) -- [O],
                         RNode = lists:nth(random:uniform(length(Active)),
                                           Active),
                         {Idx, O, RNode, Mods, Status};
                     _ ->
                         T
                 end
             end || T={Idx, O, NO, Mods, Status} <- Next],
    Next3 = [T || T={_, O, NO, _, _} <- Next2,
                  not lists:member(O, DownMembers),
                  not lists:member(NO, DownMembers)],
    Next3.

%% @private
all_next_owners(?CHSTATE{next=Next}) ->
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
    Next2 = lists:ukeysort(1, CState?CHSTATE.next ++ Next),
    CState2 = change_owners(CState, Reassign),
    CState2?CHSTATE{next=Next2}.

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
    State3 = State2?CHSTATE{seen=Seen},
    OtherState3 = OtherState2?CHSTATE{seen=Seen},
    SeenChanged = not equal_seen(State, State3),

    %% Try to reconcile based on vector clock, chosing the most recent state.
    VC1 = State3?CHSTATE.vclock,
    VC2 = OtherState3?CHSTATE.vclock,
    %% vclock:merge has different results depending on order of input vclocks
    %% when input vclocks have same counter but different timestamps. We need
    %% merge to be deterministic here, hence the additional logic.
    VMerge1 = vclock:merge([VC1, VC2]),
    VMerge2 = vclock:merge([VC2, VC1]),
    case {vclock:equal(VMerge1, VMerge2), VMerge1 < VMerge2} of
        {true, _} ->
            VC3 = VMerge1;
        {_, true} ->
            VC3 = VMerge1;
        {_, false} ->
            VC3 = VMerge2
    end,

    Newer = vclock:descends(VC1, VC2),
    Older = vclock:descends(VC2, VC1),
    Equal = equal_cstate(State3, OtherState3),
    case {Equal, Newer, Older} of
        {_, true, false} ->
            {SeenChanged, State3?CHSTATE{vclock=VC3}};
        {_, false, true} ->
            {true, OtherState3?CHSTATE{nodename=VNode, vclock=VC3}};
        {true, _, _} ->
            {SeenChanged, State3?CHSTATE{vclock=VC3}};
        {_, true, true} ->
            %% Exceptional condition that should only occur during
            %% rolling upgrades and manual setting of the ring.
            %% Merge as a divergent case.
            State4 = reconcile_divergent(VNode, State3, OtherState3),
            {true, State4?CHSTATE{nodename=VNode}};
        {_, false, false} ->
            %% Unable to reconcile based on vector clock, merge rings.
            State4 = reconcile_divergent(VNode, State3, OtherState3),
            {true, State4?CHSTATE{nodename=VNode}}
    end.

%% @private
reconcile_divergent(VNode, StateA, StateB) ->
    VClock = vclock:increment(VNode, vclock:merge([StateA?CHSTATE.vclock,
                                                   StateB?CHSTATE.vclock])),
    Members = reconcile_members(StateA, StateB),
    Meta = merge_meta(StateA?CHSTATE.meta, StateB?CHSTATE.meta),
    NewState = reconcile_ring(StateA, StateB, get_members(Members)),
    NewState?CHSTATE{vclock=VClock, members=Members, meta=Meta}.

%% @private
%% @doc Merge two members list using status vector clocks when possible,
%%      and falling back to manual merge for divergent cases.
reconcile_members(StateA, StateB) ->
    orddict:merge(
      fun(_K, {Valid1, VC1, Meta1}, {Valid2, VC2, Meta2}) ->
              New1 = vclock:descends(VC1, VC2),
              New2 = vclock:descends(VC2, VC1),
              MergeVC = vclock:merge([VC1, VC2]),
              case {New1, New2} of
                  {true, false} ->
                      MergeMeta = lists:ukeysort(1, Meta1 ++ Meta2),
                      {Valid1, MergeVC, MergeMeta};
                  {false, true} ->
                      MergeMeta = lists:ukeysort(1, Meta2 ++ Meta1),
                      {Valid2, MergeVC, MergeMeta};
                  {_, _} ->
                      MergeMeta = lists:ukeysort(1, Meta1 ++ Meta2),
                      {merge_status(Valid1, Valid2), MergeVC, MergeMeta}
              end
      end,
      StateA?CHSTATE.members,
      StateB?CHSTATE.members).

%% @private
reconcile_seen(StateA, StateB) ->
    orddict:merge(fun(_, VC1, VC2) ->
                          vclock:merge([VC1, VC2])
                  end, StateA?CHSTATE.seen, StateB?CHSTATE.seen).

%% @private
merge_next_status(complete, _) ->
    complete;
merge_next_status(_, complete) ->
    complete;
merge_next_status(awaiting, awaiting) ->
    awaiting.

%% @private
%% @doc Merge two next lists that must be of the same size and have
%%      the same Idx/Owner pair.
reconcile_next(Next1, Next2) ->
    lists:zipwith(fun({Idx, Owner, Node, Transfers1, Status1},
                      {Idx, Owner, Node, Transfers2, Status2}) ->
                          {Idx, Owner, Node,
                           ordsets:union(Transfers1, Transfers2),
                           merge_next_status(Status1, Status2)}
                  end, Next1, Next2).

%% @private
%% @doc Merge two next lists that may be of different sizes and
%%      may have different Idx/Owner pairs. When different, the
%%      pair associated with BaseNext is chosen. When equal,
%%      the merge is the same as in reconcile_next/2.
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
reconcile_ring(StateA=?CHSTATE{claimant=Claimant1, rvsn=VC1, next=Next1},
               StateB=?CHSTATE{claimant=Claimant2, rvsn=VC2, next=Next2},
               Members) ->
    %% Try to reconcile based on the ring version (rvsn) vector clock.
    V1Newer = vclock:descends(VC1, VC2),
    V2Newer = vclock:descends(VC2, VC1),
    EqualVC = (vclock:equal(VC1, VC2) and (Claimant1 =:= Claimant2)),
    case {EqualVC, V1Newer, V2Newer} of
        {true, _, _} ->
            Next = reconcile_next(Next1, Next2),
            StateA?CHSTATE{next=Next};
        {_, true, false} ->
            Next = reconcile_divergent_next(Next1, Next2),
            StateA?CHSTATE{next=Next};
        {_, false, true} ->
            Next = reconcile_divergent_next(Next2, Next1),
            StateB?CHSTATE{next=Next};
        {_, _, _} ->
            %% Ring versions were divergent, so fall back to reconciling based
            %% on claimant. Under normal operation, divergent ring versions
            %% should only occur if there are two different claimants, and one
            %% claimant is invalid. For example, when a claimant is removed and
            %% a new claimant has just taken over. We therefore chose the ring
            %% with the valid claimant.
            CValid1 = lists:member(Claimant1, Members),
            CValid2 = lists:member(Claimant2, Members),
            case {CValid1, CValid2} of
                {true, false} ->
                    Next = reconcile_divergent_next(Next1, Next2),
                    StateA?CHSTATE{next=Next};
                {false, true} ->
                    Next = reconcile_divergent_next(Next2, Next1),
                    StateB?CHSTATE{next=Next};
                {false, false} ->
                    %% This can occur when removed/down nodes are still
                    %% up and gossip to each other. We need to pick a
                    %% claimant to handle this case, although the choice
                    %% is irrelevant as a correct valid claimant will
                    %% eventually emerge when the ring converges.
                    case Claimant1 < Claimant2 of
                        true ->
                            Next = reconcile_divergent_next(Next1, Next2),
                            StateA?CHSTATE{next=Next};
                        false ->
                            Next = reconcile_divergent_next(Next2, Next1),
                            StateB?CHSTATE{next=Next}
                    end;
                {true, true} ->
                    %% This should never happen in normal practice.
                    %% But, we need to handle it for exceptional cases.
                    case Claimant1 < Claimant2 of
                        true ->
                            Next = reconcile_divergent_next(Next1, Next2),
                            StateA?CHSTATE{next=Next};
                        false ->
                            Next = reconcile_divergent_next(Next2, Next1),
                            StateB?CHSTATE{next=Next}
                    end
            end
    end.

%% @private
merge_status(invalid, _) ->
    invalid;
merge_status(_, invalid) ->
    invalid;
merge_status(down, _) ->
    down;
merge_status(_, down) ->
    down;
merge_status(joining, _) ->
    joining;
merge_status(_, joining) ->
    joining;
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
transfer_complete(CState=?CHSTATE{next=Next, vclock=VClock}, Idx, Mod) ->
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
    CState?CHSTATE{next=Next2, vclock=VClock2}.

%% @private
next_owner({_, Owner, NextOwner, _Transfers, Status}) ->
    {Owner, NextOwner, Status}.

%% @private
get_members(Members) ->
    get_members(Members, [joining, valid, leaving, exiting, down]).

%% @private
get_members(Members, Types) ->
    [Node || {Node, {V, _, _}} <- Members, lists:member(V, Types)].

%% @private
set_member(Node, CState, Member, Status) ->
    VClock = vclock:increment(Node, CState?CHSTATE.vclock),
    CState2 = set_member(Node, CState, Member, Status, same_vclock),
    CState2?CHSTATE{vclock=VClock}.

%% @private
set_member(Node, CState, Member, Status, same_vclock) ->
    Members2 = orddict:update(Member,
                              fun({_, VC, MD}) ->
                                      {Status, vclock:increment(Node, VC), MD}
                              end,
                              {Status, vclock:increment(Node,
                                                        vclock:fresh()), []},
                              CState?CHSTATE.members),
    CState?CHSTATE{members=Members2}.

%% @private
update_seen(Node, CState=?CHSTATE{vclock=VClock, seen=Seen}) ->
    Seen2 = orddict:update(Node,
                           fun(SeenVC) ->
                                   vclock:merge([SeenVC, VClock])
                           end,
                           VClock, Seen),
    CState?CHSTATE{seen=Seen2}.

%% @private
equal_cstate(StateA, StateB) ->
    equal_cstate(StateA, StateB, false).

equal_cstate(StateA, StateB, Verbose) ->
    T1 = equal_members(StateA?CHSTATE.members, StateB?CHSTATE.members),
    T2 = vclock:equal(StateA?CHSTATE.rvsn, StateB?CHSTATE.rvsn),
    T3 = equal_seen(StateA, StateB),
    T4 = equal_rings(StateA, StateB),

    %% Clear fields checked manually and test remaining through equality.
    %% Note: We do not consider cluster name in equality.
    StateA2=StateA?CHSTATE{nodename=ok, members=ok, vclock=ok, rvsn=ok,
                           seen=ok, chring=ok, meta=ok, clustername=ok},
    StateB2=StateB?CHSTATE{nodename=ok, members=ok, vclock=ok, rvsn=ok,
                           seen=ok, chring=ok, meta=ok, clustername=ok},
    T5 = (StateA2 =:= StateB2),

    case Verbose of
        false ->
            T1 and T2 and T3 and T4 and T5;
        true ->
            Failed =
                lists:filter(fun({Test,_}) -> Test =:= false end,
                             [{T1, members},
                              {T2, rvsn},
                              {T3, seen},
                              {T4, ring},
                              {T5, other}]),
            Failed
    end.

%% @private
equal_members(M1, M2) ->
    L = orddict:merge(fun(_, {Status1, VC1, Meta1}, {Status2, VC2, Meta2}) ->
                              (Status1 =:= Status2) andalso
                                  vclock:equal(VC1, VC2) andalso
                                  (Meta1 =:= Meta2)
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
filtered_seen(State=?CHSTATE{seen=Seen}) ->
    case get_members(State?CHSTATE.members) of
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
    B1 = A?CHSTATE{nodename=b},
    B2 = transfer_node(I1, b, B1),
    ?assertEqual(B2, transfer_node(I1, b, B2)),
    {no_change, A1} = reconcile(B1,A),
    C1 = A?CHSTATE{nodename=c},
    C2 = transfer_node(I1, c, C1),
    {new_ring, A2} = reconcile(C2,A1),
    {new_ring, A3} = reconcile(B2,A2),
    C3 = transfer_node(I2,c,C2),
    {new_ring, C4} = reconcile(A3,C3),
    {new_ring, A4} = reconcile(C4,A3),
    {new_ring, B3} = reconcile(A4,B2),
    ?assertEqual(A4?CHSTATE.chring, B3?CHSTATE.chring),
    ?assertEqual(B3?CHSTATE.chring, C4?CHSTATE.chring).

param_fresh_test() ->
    application:set_env(riak_core,ring_creation_size,4),
    ?assert(equal_cstate(fresh(), fresh(4, node()))),
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
    %% Only members and seen should have changed
    {new_ring, Ring2} = reconcile(fresh(2,someone_else),Ring1),
    ?assertMatch([{false, members}, {false, seen}],
                 equal_cstate(Ring1, Ring2, true)),
    RingB0 = fresh(2,node()),
    RingB1 = transfer_node(0,x,RingB0),
    RingB2 = RingB1?CHSTATE{nodename=b},
    ?assertMatch({no_change,_},reconcile(Ring1,RingB2)),
    {no_change, RingB3} = reconcile(Ring1,RingB2),
    ?assert(equal_cstate(RingB2, RingB3)).

metadata_inequality_test() ->
    Ring0 = fresh(2,node()),
    Ring1 = update_meta(key,val,Ring0),
    ?assertNot(equal_rings(Ring0,Ring1)),
    ?assertEqual(Ring1?CHSTATE.meta,
                 merge_meta(Ring0?CHSTATE.meta,Ring1?CHSTATE.meta)),
    timer:sleep(1001), % ensure that lastmod is at least a second later
    Ring2 = update_meta(key,val2,Ring1),
    ?assertEqual(get_meta(key,Ring2),
                 get_meta(key,?CHSTATE{meta=
                            merge_meta(Ring1?CHSTATE.meta,
                                       Ring2?CHSTATE.meta)})),
    ?assertEqual(get_meta(key,Ring2),
                 get_meta(key,?CHSTATE{meta=
                            merge_meta(Ring2?CHSTATE.meta,
                                       Ring1?CHSTATE.meta)})).
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
    Ring1 = add_member(node(), Ring0, 'new@new'),
    Ring2 = transfer_node(0, 'new@new', Ring1),
    ?assertEqual('new@new', random_other_node(Ring2)).

membership_test() ->
    RingA1 = fresh(nodeA),
    ?assertEqual([nodeA], all_members(RingA1)),

    RingA2 = add_member(nodeA, RingA1, nodeB),
    RingA3 = add_member(nodeA, RingA2, nodeC),
    ?assertEqual([nodeA, nodeB, nodeC], all_members(RingA3)),

    RingA4 = remove_member(nodeA, RingA3, nodeC),
    ?assertEqual([nodeA, nodeB], all_members(RingA4)),
    
    %% Node should stay removed
    {_, RingA5} = reconcile(RingA3, RingA4),
    ?assertEqual([nodeA, nodeB], all_members(RingA5)),

    %% Add node in parallel, check node stays removed
    RingB1 = add_member(nodeB, RingA3, nodeC),
    {_, RingA6} = reconcile(RingB1, RingA5),
    ?assertEqual([nodeA, nodeB], all_members(RingA6)),

    %% Add node as parallel descendent, check node is added
    RingB2 = add_member(nodeB, RingA6, nodeC),
    {_, RingA7} = reconcile(RingB2, RingA6),
    ?assertEqual([nodeA, nodeB, nodeC], all_members(RingA7)),

    Priority = [{invalid,1}, {down,2}, {joining,3}, {valid,4}, {exiting,5},
                {leaving,6}],
    RingX1 = fresh(nodeA),
    RingX2 = add_member(nodeA, RingX1, nodeB),
    RingX3 = add_member(nodeA, RingX2, nodeC),
    ?assertEqual(joining, member_status(RingX3, nodeC)),

    %% Parallel/sibling status changes merge based on priority
    [begin
         RingT1 = set_member(nodeA, RingX3, nodeC, StatusA),
         ?assertEqual(StatusA, member_status(RingT1, nodeC)),
         RingT2 = set_member(nodeB, RingX3, nodeC, StatusB),
         ?assertEqual(StatusB, member_status(RingT2, nodeC)),
         StatusC = case PriorityA < PriorityB of
                       true -> StatusA;
                       false -> StatusB
                   end,
         {_, RingT3} = reconcile(RingT2, RingT1),
         ?assertEqual(StatusC, member_status(RingT3, nodeC))
     end || {StatusA, PriorityA} <- Priority,
            {StatusB, PriorityB} <- Priority],

    %% Related status changes merge to descendant
    [begin
         RingT1 = set_member(nodeA, RingX3, nodeC, StatusA),
         ?assertEqual(StatusA, member_status(RingT1, nodeC)),
         RingT2 = set_member(nodeB, RingT1, nodeC, StatusB),
         ?assertEqual(StatusB, member_status(RingT2, nodeC)),
         RingT3 = set_member(nodeA, RingT1, nodeA, valid),
         {_, RingT4} = reconcile(RingT2, RingT3),
         ?assertEqual(StatusB, member_status(RingT4, nodeC))
     end || {StatusA, _} <- Priority,
            {StatusB, _} <- Priority],
    ok.
    
ring_version_test() ->
    Ring1 = fresh(nodeA),
    Ring2 = add_member(node(), Ring1, nodeA),
    Ring3 = add_member(node(), Ring2, nodeB),
    ?assertEqual(nodeA, claimant(Ring3)),
    ?CHSTATE{rvsn=RVsn, vclock=VClock} = Ring3,

    RingA1 = transfer_node(0, nodeA, Ring3),
    RingA2 = RingA1?CHSTATE{vclock=vclock:increment(nodeA, VClock)},
    RingB1 = transfer_node(0, nodeB, Ring3),
    RingB2 = RingB1?CHSTATE{vclock=vclock:increment(nodeB, VClock)},

    %% RingA1 has most recent ring version
    {_, RingT1} = reconcile(RingA2?CHSTATE{rvsn=vclock:increment(nodeA, RVsn)},
                            RingB2),
    ?assertEqual(nodeA, index_owner(RingT1,0)),

    %% RingB1 has most recent ring version
    {_, RingT2} = reconcile(RingA2,
                            RingB2?CHSTATE{rvsn=vclock:increment(nodeB, RVsn)}),
    ?assertEqual(nodeB, index_owner(RingT2,0)),

    %% Divergent ring versions, merge based on claimant
    {_, RingT3} = reconcile(RingA2?CHSTATE{rvsn=vclock:increment(nodeA, RVsn)},
                            RingB2?CHSTATE{rvsn=vclock:increment(nodeB, RVsn)}),
    ?assertEqual(nodeA, index_owner(RingT3,0)),

    %% Divergent ring versions, one valid claimant. Merge on claimant.
    RingA3 = RingA2?CHSTATE{claimant=nodeA},
    RingA4 = remove_member(nodeA, RingA3, nodeB),
    RingB3 = RingB2?CHSTATE{claimant=nodeB},
    RingB4 = remove_member(nodeB, RingB3, nodeA),
    {_, RingT4} = reconcile(RingA4?CHSTATE{rvsn=vclock:increment(nodeA, RVsn)},
                            RingB3?CHSTATE{rvsn=vclock:increment(nodeB, RVsn)}),
    ?assertEqual(nodeA, index_owner(RingT4,0)),
    {_, RingT5} = reconcile(RingA3?CHSTATE{rvsn=vclock:increment(nodeA, RVsn)},
                            RingB4?CHSTATE{rvsn=vclock:increment(nodeB, RVsn)}),
    ?assertEqual(nodeB, index_owner(RingT5,0)).

reconcile_next_test() ->
    Next1 = [{0, nodeA, nodeB, [riak_pipe_vnode], awaiting},
             {1, nodeA, nodeB, [riak_pipe_vnode], awaiting},
             {2, nodeA, nodeB, [riak_pipe_vnode], complete}],
    Next2 = [{0, nodeA, nodeB, [riak_kv_vnode], complete},
             {1, nodeA, nodeB, [], awaiting},
             {2, nodeA, nodeB, [], awaiting}],
    Next3 = [{0, nodeA, nodeB, [riak_kv_vnode, riak_pipe_vnode], complete},
             {1, nodeA, nodeB, [riak_pipe_vnode], awaiting},
             {2, nodeA, nodeB, [riak_pipe_vnode], complete}],
    ?assertEqual(Next3, reconcile_next(Next1, Next2)),

    Next4 = [{0, nodeA, nodeB, [riak_pipe_vnode], awaiting},
             {1, nodeA, nodeB, [], awaiting},
             {2, nodeA, nodeB, [riak_pipe_vnode], awaiting}],
    Next5 = [{0, nodeA, nodeC, [riak_kv_vnode], complete},
             {2, nodeA, nodeB, [riak_kv_vnode], complete}],
    Next6 = [{0, nodeA, nodeB, [riak_pipe_vnode], awaiting},
             {1, nodeA, nodeB, [], awaiting},
             {2, nodeA, nodeB, [riak_kv_vnode, riak_pipe_vnode], complete}],
    ?assertEqual(Next6, reconcile_divergent_next(Next4, Next5)).

-endif.
