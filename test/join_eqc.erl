%% Implements model of proposed join/claim changes.
%%
%% The model is implemented predominately through next_state transitions
%% within an eqc_statem test harness. The model includes a basic
%% implementation of gossip behaivor.
%%
%% There are currently two main changes/additions with respect to Justin's
%% proposal:
%%
%% 1. When a node joins a cluster, it first retrieves the ring from the
%%    node that it is joining and updates it's ring and claimant fields to
%%    match the retrieved ring before sending the join/gossip request.
%%    This makes it so a node "gives up" all partition ownership as well
%%    as yields it's claimant before joining a cluster.
%%
%% 2. In the original proposal, the claimant was responsible for deciding
%%    claims and setting NextOwner. However, it was the previous owner
%%    that would move a NextOwner to Owner after successful handoff. This
%%    leads to problems with gossip where divergent rings were merged through
%%    random selection, thus potentially overiding the new owner with the old
%%    owner upon random reconciliation. 
%%      This is addressed by adding a "ring version" to the cluster state that
%%    is only ever incremented by the current claimant. When a node decides to
%%    yields its ownership to another node, the node does a sync call to the
%%    claimant which updates the ring, increments the ring version, and sends
%%    the ring back to the previous owner that gossips it out after deleting
%%    its local vnode state. Likewise, gossip was changed so that a ring with
%%    a higher ring version is chosen during reconciliation. Random selection
%%    is only ever used if the ring verions compare equal.
%%
%% Gossip model. Gossip is modeled through three different commands: gossip,
%% any_gossip, and random_gossip. Gossip corresponds to requested gossip within
%% the model targeted to a specific node. Ie. gossip(A,B) -> node A explicitly
%% enqueues a gossip to node B. any_gossip is a requested gossip to an
%% unspecified node. random_gossip is background gossip that occur randomly
%% without being requested within the model.
%%
%% In addition to modeling gossiped state per node, there is also a global
%% gossip state that corresponds to what all nodes should eventually converge
%% to.
%%
%% Current properties tested:
%% -- reads against the global gossip state do not give not_founds.
%%    (there is a commented out read to local gossip states that sill
%%     gives not_founds due to gossip delay after ownership change)
%%
%% -- there is only one node that believes it is the claimant
%%
%% More properties need to be tested/added.
%%

-module(join_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-define(TEST_ITERATIONS, 100).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-define(INITIAL_CLUSTER_SIZE, 3).
-define(MAX_NODES, 6).
-define(RING, 16).
-define(N, 3).

%% Cluster state record propagated over "gossip"
-record(cstate, {
          ring = [] :: [{integer(), integer()}],
          next = [] :: [{integer(), integer()}],
          members = [] :: [integer()],
          claimant :: undefined | integer(),
          rvsn :: integer(),
          vclock :: vclock:vclock()
         }).

%% Node state
-record(nstate, {
          cstate = #cstate{} :: #cstate{},
          partitions = [] :: [integer()],
          has_data = [] :: [integer()]
         }).

%% Global test state
-record(state,
        { cstate :: #cstate{},
          nstates :: dict(),
          primary :: [integer()],
          others :: [integer()],
          random_ring :: [integer()],
          active_handoffs :: [{integer(), integer(), integer()}],
          gossip :: [{integer(), integer(), #cstate{}}],
          any_gossip :: [{integer(), #cstate{}}],
          split :: dict()
        }).

eqc_test_() ->
    {spawn,
     [{setup,
       fun setup/0,
       fun cleanup/1,
       [%% Run the quickcheck tests
        {timeout, 60000, % timeout is in msec
         ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_join()))))}
       ]
      }
     ]
    }.

setup() ->
    ok.

cleanup(_) ->
    ok.

prop_join() ->
%%    ?FORALL(Cmds, commands(?MODULE),
    ?FORALL(Cmds, more_commands(100, commands(?MODULE)),
            %%?TRAPEXIT(
            (
           begin
               {_History, _State, Result} = run_commands(?MODULE, Cmds),
               %%?WHENFAIL(?debugFmt("~p~n", [Result]), Result == ok),
               %%aggregate(command_names(Cmds), Result == ok)
               aggregate(command_names(Cmds),
                         ?WHENFAIL(?debugFmt("~n~p~n", [Result]), Result == ok))
           end
          )).

%% Make a ring with partitions equally distributed across the nodes
make_default_ring(Size, Nodes) ->
    N = erlang:length(Nodes),
    [{Idx, lists:nth((Idx rem N)+1, Nodes)} || Idx <- lists:seq(0, Size-1)].

indices(Ring, Node) ->
    [Idx || {Idx, Owner} <- Ring, Owner == Node].

all_partitions(State) ->
    dict:fold(fun(Node, NS, L) ->
                      [{Node, Idx} || Idx <- NS#nstate.partitions] ++ L
              end,
              [], State#state.nstates).

owner(Ring, Idx) ->
    proplists:get_value(Idx, Ring).

init_node_state(Node) ->
    Indices = lists:seq(0, ?RING-1),
    Ring = [{Idx, Node} || Idx <- Indices],
    CState = #cstate{members=[Node],
                     ring=Ring,
                     next=[],
                     claimant=Node,
                     rvsn=0,
                     vclock=vclock:increment(Node, vclock:fresh())},
    #nstate{cstate=CState,
            partitions=Indices,
            has_data=[]}.

init_node_state(Node, CState=#cstate{ring=Ring}) ->
    Indices = indices(Ring, Node),
    #nstate{cstate=CState,
            partitions = Indices,
            has_data = Indices}.

%% EQC state_m callbacks
initial_state() ->
    #state{primary=[],
           others=[],
           gossip=[],
           any_gossip=[],
           active_handoffs=[],
           split=dict:new()}.

g_initial_nodes() ->
    Nodes = lists:seq(0, ?MAX_NODES-1),
    ?LET(L, shuffle(Nodes), lists:split(?INITIAL_CLUSTER_SIZE, L)).

g_idx() ->
    choose(0, ?RING-1).

%% Generates {Node, NodeState} from list of nodes
g_node_state(State, Nodes) ->
    ?LET(Node, elements(Nodes), {Node, get_nstate(State, Node)}).

g_gossip(State, Gossip) ->
    {OtherNode, Node, OtherCS} = hd(Gossip),
    [{Node, get_nstate(State, Node)}, OtherNode, OtherCS].

g_gossip(Gossip) ->
    {OtherNode, Node, OtherCS} = hd(Gossip),
    [Node, OtherNode, OtherCS].

g_any_gossip(State, Nodes, AnyGossip) ->
    {OtherNode, OtherCS} = hd(AnyGossip),
    ?LET(Node, elements(Nodes),
         [{Node, get_nstate(State, Node)}, OtherNode, OtherCS]).

g_any_gossip(Nodes, AnyGossip) ->
    {OtherNode, OtherCS} = hd(AnyGossip),
%%    ?LET(Node, elements(Nodes),
    ?LET(Node, elements(Nodes -- [OtherNode]),
         [Node, OtherNode, OtherCS]).

g_random_ring() ->
    shuffle(lists:seq(0, ?RING-1)).

command(#state{primary=[]}) ->
    {call, ?MODULE, initial_cluster, [g_initial_nodes(), g_random_ring()]};
command(#state{cstate=_CState, primary=Primary, others=Others,
               active_handoffs=Active, gossip=Gossip,
               any_gossip=AnyGossip}) ->
    oneof([{call, ?MODULE, read, [g_idx()]},
           %%{call, ?MODULE, read, [elements(Primary), g_idx()]},
           {call, ?MODULE, random_gossip, [elements(Primary), elements(Primary)]},
           {call, ?MODULE, maybe_handoff, [elements(Primary), g_idx()]},
           {call, ?MODULE, comm_split, [elements(Primary), elements(Primary)]},
           {call, ?MODULE, comm_join, [elements(Primary), elements(Primary)]}]
          ++ [{call, ?MODULE, join, [elements(Others), elements(Primary)]} || Others /= []]
          ++ [{call, ?MODULE, gossip, g_gossip(Gossip)} || Gossip /= []]
          ++ [{call, ?MODULE, any_gossip, g_any_gossip(Primary, AnyGossip)} || AnyGossip /= []]
          ++ [{call, ?MODULE, finish_handoff, [elements(Active)]} || Active /= []]
         ).

precondition(_State, {call, _, initial_cluster, _}) ->
    true;

precondition(State, {call, _, join, [{Node, _NState}, _]}) ->
    lists:member(Node, State#state.others);

precondition(State, {call, _, gossip, [Node, OtherNode, OtherCS]}) ->
    (State#state.gossip /= []) andalso
        can_communicate(State, OtherNode, Node) and 
        equal_cstate(get_cstate(State, OtherNode), OtherCS);
        
precondition(State, {call, _, any_gossip, [Node, OtherNode, OtherCS]}) ->
    (State#state.any_gossip /= []) andalso
        can_communicate(State, OtherNode, Node) and 
        equal_cstate(get_cstate(State, OtherNode), OtherCS);

precondition(State, {call, _, random_gossip, [Node1, Node2]}) ->
    lists:member(Node1, State#state.primary) andalso
        lists:member(Node2, State#state.primary) andalso
        can_communicate(State, Node1, Node2);

precondition(State, {call, _, comm_split, [Node1, Node2]}) ->
    Node1 < Node2 andalso
        lists:member(Node1, State#state.primary) andalso
        lists:member(Node2, State#state.primary) andalso
        can_communicate(State, Node1, Node2);

precondition(State, {call, _, comm_join, [Node1, Node2]}) ->
    Node1 < Node2 andalso
        lists:member(Node1, State#state.primary) andalso
        lists:member(Node2, State#state.primary) andalso
        not can_communicate(State, Node1, Node2);

precondition(State, {call, _, maybe_handoff, [Node, Idx]}) ->
    (State#state.primary /= []) andalso
        begin
            NState = get_nstate(State, Node),
            lists:member(Idx, NState#nstate.partitions)
        end;

precondition(State, {call, _, finish_handoff, [AH={_, Prev, New}]}) ->
    lists:member(AH, State#state.active_handoffs) andalso
        can_communicate(State, Prev, New);

precondition(State, {call, _, read, [Node, _Idx]}) ->
    lists:member(Node, State#state.primary);

%% All commands other than initial_cluster run after initial_cluster
precondition(State,_) ->
    State#state.primary /= [].

next_state(State, _, {call, _, read, _}) ->
    State;

next_state(State, _Result, {call, _, initial_cluster, [{Members, Others}, RRing]}) ->
    s_initial_cluster(State, Members, Others, RRing);

next_state(State, _Result, {call, _, gossip, [Node, OtherNode, OtherCS]}) ->
    NState = get_nstate(State, Node),
    s_normal_gossip(State, Node, NState, OtherNode, OtherCS);

next_state(State, _Result, {call, _, any_gossip, [Node, OtherNode, OtherCS]}) ->
    NState = get_nstate(State, Node),
    s_any_gossip(State, Node, NState, OtherNode, OtherCS);

next_state(State, _Result, {call, _, random_gossip, [Node, OtherNode]}) ->
    NState = get_nstate(State, Node),
    OtherCS = get_cstate(State, OtherNode),
    s_random_gossip(State, Node, NState, OtherNode, OtherCS);

next_state(State, _Result, {call, _, comm_split, [Node1, Node2]}) ->
    s_comm_split(State, Node1, Node2);

next_state(State, _Result, {call, _, comm_join, [Node1, Node2]}) ->
    s_comm_join(State, Node1, Node2);

next_state(State, _Result, {call, _, join, [Node, PNode]}) ->
    NState = get_nstate(State, Node),
    s_join(State, Node, NState, PNode);

next_state(State, _Result, {call, _, maybe_handoff, [Node, Idx]}) ->
    NState = get_nstate(State, Node),
    s_maybe_handoff(State, Node, NState, Idx);

next_state(State, _Result, {call, _, finish_handoff, [AH]}) ->
    s_finish_handoff(State, AH).

%% next_state(State, _Result, {call,_,_,_}) ->
%%     State.

postcondition(State, {call, _, read, [Node, Idx]}, _Result) ->
    CState = get_cstate(State, Node),
    valid_read(State, CState, Idx);
postcondition(State, {call, _, read, [Idx]}, _Result) ->
    CState = get_global_cstate(State),
    valid_read(State, CState, Idx);

postcondition(State, Cmd, ok) ->
    State2 = next_state(State, ok, Cmd),
    lists:all(fun(X) -> X end,
              [%%check_one_claimant(State2),
               check_one_active_claimant(State2),
               check_sorted_members(State2)]);

postcondition(_,_,fail) ->
    false;

postcondition(_,_,_) ->
    true.

%% Checks that there is at most one claimant across all node's cstate.
%% This is untrue and will fail if tested due to gossip delay. This is
%% acceptable behavior. The stronger claim check_one_active_claimant
%% tested below is the correct property to test.
check_one_claimant(State) ->
    L1 = [NS#nstate.cstate#cstate.claimant
          || {_, NS} <- dict:to_list(State#state.nstates)],
    L2 = lists:usort(L1) -- [undefined],
    Sz = erlang:length(L2),
    T = (Sz == 0) or (Sz == 1),
    case T of
        false ->
            io:format("More than one claimant: ~p~n", [L2]),
            false;
        _ ->
            true
    end.

%% Checks that there is only one node that believes it is the claimant.
check_one_active_claimant(State) ->
    Primary = State#state.primary,
    L1 = [NS#nstate.cstate#cstate.claimant
          || {Node, NS} <- dict:to_list(State#state.nstates),
             Node =:= NS#nstate.cstate#cstate.claimant,
             lists:member(Node, Primary)],
    L2 = lists:usort(L1) -- [undefined],
    Sz = erlang:length(L2),
    T = (Sz == 0) or (Sz == 1),
    case T of
        false ->
            io:format("More than one claimant: ~p~n", [L2]),
            false;
        _ ->
            true
    end.

check_sorted_members(State) ->
    L1 = [begin
              M = NS#nstate.cstate#cstate.members,
              M == lists:usort(M)
          end || {_, NS} <- dict:to_list(State#state.nstates)],
    T = lists:all(fun(X) -> X end, L1),
    case T of
        false ->
            io:format("Members not sorted~n", []),
            false;
        _ ->
            true
    end.

%% Dummy commands
initial_cluster(_, _) ->
    ok.

read(_) -> 
    ok.
read(_,_) ->
    ok.

maybe_handoff(_, _) ->
    ok.

finish_handoff({_Idx, _Prev, _New}) ->
    ok.

join(_, _) ->
    ok.

gossip(_, _, _) ->
    ok.

any_gossip(_, _, _) ->
    ok.

random_gossip(_, _) ->
    ok.

comm_split(_, _) ->
    ok.

comm_join(_, _) ->
    ok.

%% The actual join/claim model, invoked by next_state transitions.
s_initial_cluster(State, Members, Others, RandomRing) ->
    Members2 = lists:sort(Members),
    Ring = make_default_ring(?RING, Members2),
    CState = #cstate{members=Members2,
                     ring=Ring,
                     next=[],
                     claimant=hd(Members2),
                     rvsn=0,
                     vclock=vclock:increment(global, vclock:fresh())},
    SMembers = [{Node, init_node_state(Node, CState)} || Node <- Members2],
    SOthers = [{Node, init_node_state(Node)} || Node <- Others],
    NStates = dict:from_list(SMembers ++ SOthers),
    State#state{cstate=CState,
                primary=Members2,
                others=Others,
                random_ring=RandomRing,
                nstates=NStates}.

s_normal_gossip(State, Node, NState, OtherNode, OtherCS) ->
    State2 = s_gossip(State, {Node, NState}, OtherNode, OtherCS),
    Gossip2 = tl(State#state.gossip),
    State2#state{gossip=Gossip2}.

s_any_gossip(State, Node, NState, OtherNode, OtherCS) ->
    State2 = s_gossip(State, {Node, NState}, OtherNode, OtherCS),
    AnyGossip2 = tl(State#state.any_gossip),
    State2#state{any_gossip=AnyGossip2}.

s_random_gossip(State, Node, NState, OtherNode, OtherCS) ->
    State2 = s_gossip(State, {Node, NState}, OtherNode, OtherCS),
    State2.

s_gossip(State, {Node, NState}, _OtherNode, OtherCS) ->
    CState = NState#nstate.cstate,
    %%?debugFmt("Gossip: ~p -> ~p~n~p~n~p", [OtherNode, Node, CState, OtherCS]),
    {Changed, CState2} = merge_cstate(Node, CState, OtherCS),
    case Changed of
        true ->
            RRing = State#state.random_ring,
            NState2 = ring_changed(RRing, {Node, NState}, CState2),
            State2 = update_nstate(State, Node, NState2),
            State3 = enqueue_gossip(State2, Node),
            State3;
        false ->
            State
    end.

s_comm_split(State, Node1, Node2) ->
    S1 = State#state.split,
    S2 = dict:store({Node1, Node2}, true, S1),
    S3 = dict:store({Node2, Node1}, true, S2),
    State#state{split=S3}.

s_comm_join(State, Node1, Node2) ->
    S1 = State#state.split,
    S2 = dict:erase({Node1, Node2}, S1),
    S3 = dict:erase({Node2, Node1}, S2),
    State#state{split=S3}.

s_join(State, Node, NState, PNode) ->
    CState=NState#nstate.cstate,

    %% Renounce all ownership before joining by setting our ring to the ring
    %% from the node we're joining.
    JCState = get_cstate(State, PNode),
    Ring2 = JCState#cstate.ring,
    %% Also, set claimant to the claimant of the cluster we're joining.
    Claimant2 = JCState#cstate.claimant,
    %%Claimant2 = CState#cstate.claimant,
    CState2 = CState#cstate{ring=Ring2, claimant=Claimant2},

    State2 = update_nstate(State, Node, NState#nstate{partitions=[],
                                                      has_data=[]}),
    State3 = update_cstate(State2, Node, CState2),
    State4 = enqueue_gossip(State3, Node, PNode),
    Others2 = State#state.others -- [Node],
    Primary2 = [Node | State#state.primary],
    State4#state{primary=Primary2, others=Others2}.

s_maybe_handoff(State, Node, NState, Idx) ->
    CState = NState#nstate.cstate,
    Active0 = State#state.active_handoffs,
    NextOwner = proplists:get_value(Idx, CState#cstate.next),
    Owner = owner(CState#cstate.ring, Idx),
    Active = case {Owner, NextOwner} of
                 {_, Node} ->
                     Active0;
                 {Node, undefined} ->
                     Active0;
                 {Node, _} ->
                     [{Idx, Node, NextOwner} | Active0];
                 {_, undefined} ->
                     [{Idx, Node, Owner} | Active0]
             end,
    State#state{active_handoffs=Active}.

s_finish_handoff(State, AH={Idx, Prev, New}) ->
    %% Previous owner deletes data for the index, transfers ownership to the
    %% new owner and gossips
    PrevNS = get_nstate(State, Prev),
    PrevCS = PrevNS#nstate.cstate,

    Claimant = PrevCS#cstate.claimant,
    case Claimant of
        Prev ->
            PrevCS2 = change_ownership(PrevCS, Idx, New),
            s_finish_handoff_2(State, AH, PrevNS, PrevCS2);
        _ ->
            %% Call to claimant to update ring ownership
            case call_change_ownership(State, Prev, Claimant, {Idx, New}) of
                {fail, State} ->
                    State;
                {ok, State2} ->
                    ClaimantCS = get_cstate(State2, Claimant),
                    {_, PrevCS2} = merge_cstate(Prev, PrevCS, ClaimantCS),
                    s_finish_handoff_2(State2, AH, PrevNS, PrevCS2)
            end
    end.

s_finish_handoff_2(State2, AH={Idx, Prev, New}, PrevNS, PrevCS2) ->
    PrevNS2 = update_partition_status(PrevNS, Idx, false, false),
    State3 = update_nstate(State2, Prev, PrevNS2),
    State4 = update_cstate(State3, Prev, PrevCS2),
    %%io:format("Finished handoff ~p :: ~p -> ~p~n", [Idx, Prev, New]),
    State5 = enqueue_gossip(State4, Prev),

    %% New owner is considered to now have data for test purposes
    NewNS = get_nstate(State5, New),
    NewNS2 = update_partition_status(NewNS, Idx, true, true),
    State6 = update_nstate(State5, New, NewNS2),

    Active = State6#state.active_handoffs -- [AH],
    State6#state{active_handoffs=Active}.

merge_cstate(VNode, CS1, CS2) ->
    VC1 = CS1#cstate.vclock,
    VC2 = CS2#cstate.vclock,
    Newer = vclock:descends(VC1, VC2),
    Older = vclock:descends(VC2, VC1),
    case {Newer, Older} of
        {true, false} ->
            {false, CS1};
        {false, true} ->
            {true, CS2};
        {false, false} ->
            CS3 = reconcile_cstate(VNode, CS1, CS2, VC1, VC2),
            {true, CS3};
        {true, true} ->
            case equal_cstate(CS1, CS2) of
                true ->
                    {false, CS1}
            end
    end.

reconcile_cstate(_VNode, CS1, CS2, VC1, VC2) ->
    %%VClock2 = vclock:increment(VNode, vclock:merge([VC1, VC2])),
    VClock2 = vclock:merge([VC1, VC2]),
    Members = lists:usort(CS1#cstate.members ++ CS2#cstate.members),
    {Ring, Next, RVsn} = reconcile_rvsn(CS1, CS2),
    %% Ring = merge_rings(CS1#cstate.ring, CS2#cstate.ring),
    %% Next = lists:usort(CS1#cstate.next ++ CS2#cstate.next),
    Claimant = merge_claimant(CS1#cstate.claimant, CS2#cstate.claimant),
    CS1#cstate{vclock=VClock2, members=Members, ring=Ring, next=Next, claimant=Claimant, rvsn=RVsn}.

reconcile_rvsn(CS1=#cstate{rvsn=V1}, _CS2=#cstate{rvsn=V2})  when V1 > V2 ->
    {CS1#cstate.ring, CS1#cstate.next, V1};
reconcile_rvsn(_CS1=#cstate{rvsn=V1}, CS2=#cstate{rvsn=V2})  when V2 > V1 ->
    {CS2#cstate.ring, CS2#cstate.next, V2};
reconcile_rvsn(CS1, CS2) ->
    Ring = merge_rings(CS1#cstate.ring, CS2#cstate.ring),
    Next = lists:usort(CS1#cstate.next ++ CS2#cstate.next),
    {Ring, Next, CS1#cstate.rvsn}.

merge_rings(NodesA,NodesB) ->
    [{I,randomnode(A,B)} || {{I,A},{I,B}} <- lists:zip(NodesA,NodesB)].

randomnode(NodeA,NodeA) -> NodeA;
randomnode(NodeA,NodeB) -> lists:nth(random:uniform(2),[NodeA,NodeB]).

merge_claimant(A, undefined) ->
    A;
merge_claimant(undefined, B) ->
    B;
merge_claimant(A, B) ->
    erlang:min(A, B).

update_partition_status(NState, Idx, Active, HasData) ->
    P = ordsets:from_list(NState#nstate.partitions),
    D = ordsets:from_list(NState#nstate.has_data),
    case {Active, HasData} of
        {false, _} ->
            NState#nstate{partitions=ordsets:del_element(Idx, P),
                          has_data=ordsets:del_element(Idx, D)};
        {true, false} ->
            NState#nstate{partitions=ordsets:add_element(Idx, P),
                          has_data=ordsets:del_element(Idx, D)};
        {true, true} ->
            NState#nstate{partitions=ordsets:add_element(Idx, P),
                          has_data=ordsets:add_element(Idx, D)}
    end.

get_nstate(State, Node) ->
    dict:fetch(Node, State#state.nstates).

update_nstate(State, Node, NState) ->
    NS = dict:store(Node, NState, State#state.nstates),
    State#state{nstates=NS}.

get_cstate(State, Node) ->
    (get_nstate(State, Node))#nstate.cstate.

update_cstate(NState=#nstate{}, Node, CState) ->
    CState2 = increment_cstate_vclock(Node, CState),
    NState#nstate{cstate=CState2};
update_cstate(State=#state{}, Node, CState) ->
    CState2 = increment_cstate_vclock(Node, CState),
    NS = dict:update(Node,
                     fun(NState) ->
                             NState#nstate{cstate=CState2}
                     end,
                     #nstate{cstate=CState2},
                     State#state.nstates),
    State#state{nstates=NS}.

pending_ownership_changes(CState) ->
    lists:any(fun({N1, N2}) -> N1 /= N2 end, CState#cstate.next).

ring_changed(RRing, {Node, NState}, CState) ->
    %% join membership (already handled by modelled join command for now)
    %% claimant update
    CState2 = maybe_update_claimant(CState, Node),
    CState3 = maybe_rebalance(CState2, Node, RRing),
    update_cstate(NState, Node, CState3).

maybe_update_claimant(CState=#cstate{claimant=Claimant}, Node)
  when (Claimant =:= undefined) or (Claimant =:= Node) ->
    case pending_ownership_changes(CState) of
        true ->
            CState;
        false ->
            Claimant2 = hd(CState#cstate.members),
            CState#cstate{claimant=Claimant2}
    end;
maybe_update_claimant(CState, _) ->
    CState.

simple_shuffle(L, N) ->
    lists:sublist(simple_shuffle(L), 1, N).
simple_shuffle(L) ->
    L2 = [{random:uniform(10000), E} || E <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.

maybe_rebalance(CState=#cstate{claimant=Node, ring=Ring, members=Members, next=Next},
                Node, RRing) ->
    %% Simple claim function: give member without partition a partition
    {Indices, Owners} = lists:unzip(Ring),
    {PendIndices, PendOwners} = lists:unzip([{I,O} || {I,O} <- Next]),
    NonOwners = (Members -- Owners) -- PendOwners,
    %% Only claim indices without existing next owners.
    ValidIndices = Indices -- PendIndices,
    %% Leave non-owners without claims if not enough indices available
    %% Shuffle = simple_shuffle(ValidIndices),
    Shuffle = lists:dropwhile(fun(E) -> not lists:member(E, ValidIndices) end,
                              RRing),
    ClaimSize = erlang:min(erlang:length(NonOwners),
                           erlang:length(Shuffle)),
    Claim = lists:zip(lists:sublist(Shuffle, 1, ClaimSize),
                      lists:sublist(NonOwners, 1, ClaimSize)),

    Next2 = lists:ukeysort(1, Claim ++ Next),
    CState#cstate{next=Next2};
maybe_rebalance(CState, _, _) ->
    CState.

%% Enqueue gossip of cstate from N1 to N2
enqueue_gossip(State=#state{gossip=G}, N1, N2) ->
    CState = get_cstate(State, N1),
    State2 = update_global_cstate(State, CState),
    State2#state{gossip=[{N1, N2, CState} | G]}.

%% Enqueue gossip of cstate from N1 to random primary node
enqueue_gossip(State=#state{any_gossip=G}, N1) ->
    CState = get_cstate(State, N1),
    State2 = update_global_cstate(State, CState),
    State2#state{any_gossip=[{N1, CState} | G]}.

             
%% Maintain a global cstate that all nodes should eventually converge to.
update_global_cstate(State=#state{cstate=CS1}, CS2) ->
    {_, CS3} = merge_cstate(global, CS1, CS2),
    State#state{cstate=CS3}.

get_global_cstate(State) ->
    State#state.cstate.

increment_cstate_vclock(Node, CState) ->
    VClock = vclock:increment(Node, CState#cstate.vclock),
    CState#cstate{vclock=VClock}.

%% Returns true if the nodes can communicate (ie. not split)
can_communicate(State, N1, N2) ->
    not dict:is_key({N1, N2}, State#state.split).

get_next_owner(Idx, CState) ->
    case lists:keyfind(Idx, 1, CState#cstate.next) of
        false ->
            undefined;
        {_, NextOwner, _} ->
            NextOwner
    end.


call_change_ownership(State, _Node, undefined, {_Idx, _New}) ->
    {fail, State};
call_change_ownership(State, Node, Claimant, {Idx, New}) ->
    Dropped = not can_communicate(State, Node, Claimant),
    CState = get_cstate(State, Claimant),
    WrongClaimant = (CState#cstate.claimant /= Claimant),
    case {Dropped, WrongClaimant} of
        {true, _} ->
            {fail, State};
        {_, true} ->
            {fail, State};
        {_, _} ->
            CState2 = change_ownership(CState, Idx, New),
            State2 = update_cstate(State, Claimant, CState2),
            {ok, State2}
    end.

change_ownership(CState, Idx, New) ->
    Next2 = CState#cstate.next -- [{Idx, New}],
    Ring2 = orddict:store(Idx, New, CState#cstate.ring),
    RVsn2 = CState#cstate.rvsn + 1,
    CState#cstate{ring=Ring2, next=Next2, rvsn=RVsn2}.

equal_cstate(CS1, CS2) ->
    CS3=CS1#cstate{vclock=undefined},
    CS4=CS2#cstate{vclock=undefined},
    CS3 =:= CS4.

valid_read(State, CState, Idx) ->
    Owner = proplists:get_value(Idx, CState#cstate.ring),
    NState = get_nstate(State, Owner),
    lists:member(Idx, NState#nstate.has_data).
