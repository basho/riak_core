-module(new_cluster_membership_model_eqc).

-ifdef(MODEL).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-define(TEST_ITERATIONS, 3000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-define(OUT(S,A),ok).
%%-define(OUT(S,A),io:format(S,A)).
-define(ROUT(S,A),ok).
%%-define(ROUT(S,A),?debugFmt(S,A)).

-define(INITIAL_CLUSTER_SIZE, 3).
-define(MAX_NODES, 6).
-define(RING, 8). %% Must be a power of two.
-define(N, 3).

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

-type member_status() :: valid | invalid | leaving | exiting.

%% Node state
-record(nstate, {
          chstate = ?CHSTATE{} :: ?CHSTATE{},
          partitions = [] :: orddict:orddict(atom(), ordsets:ordsets(integer())),
          has_data = [] :: orddict:orddict(atom(), ordsets:ordsets(integer())),
          joined_to :: integer()
         }).

%% Global test state
-record(state, {
          nstates :: dict(),
          ring_size :: integer(),
          members  :: [{node(), {member_status(), vclock:vclock()}}],
          primary :: [integer()],
          others :: [integer()],
          rejoin :: [{integer(), boolean()}],
          removed :: [integer()],
          down :: [integer()],
          allowed :: [integer()],
          random_ring :: [integer()],
          active_handoffs :: [{integer(), integer(), integer()}],
          seed :: {integer(), integer(), integer()},
          old_seed :: {integer(), integer(), integer()},
          split :: dict()
        }).

eqc_test_() ->
    {spawn,
     [{setup,
       fun setup/0,
       fun cleanup/1,
       [{inorder,
         [manual_test_list(),
          %% Run the quickcheck tests
          {timeout, 60000, % timeout is in msec
           ?_assertEqual(true, catch quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_join()))))}
         ]}
       ]
      }
     ]
    }.

eqc() ->
    quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_join()))),
    ok.

setup() ->
    ok.

cleanup(_) ->
    ok.

prop_join() ->
    ?FORALL(Cmds, more_commands(100, commands(?MODULE)),
           ?TRAPEXIT(
           (
           begin
               {_History, State, Result} = run_commands(?MODULE, Cmds),

               test_ring_convergence(State),

               aggregate(command_names(Cmds),
                         ?WHENFAIL(?debugFmt("~n~p~n", [Result]), Result == ok))
           end
          ))).

handoff_all_partitions(Node, State) ->
    case dict:find(Node, State#state.nstates) of
        error ->
            State;
        {ok, NState} ->
            VMods = vnode_modules(State, Node),
            AllPartitions = NState#nstate.partitions,
            lists:foldl(
              fun(Mod, State0) ->
                      Partitions = orddict:fetch(Mod, AllPartitions),
                      lists:foldl(
                        fun(Idx, State00) ->
                                do_maybe(State00, maybe_handoff, [Mod, Node, Idx])
                        end, State0, Partitions)
              end, State, VMods)
    end.

handoff_all(State) ->
    %%Members = get_members(State#state.members, [valid, leaving]),
    Members = State#state.primary,
    State2 = lists:foldl(fun handoff_all_partitions/2, State, Members),
    Active = State2#state.active_handoffs,
    State3 = lists:foldl(fun(AH, State0) ->
                                 do_maybe(State0, finish_handoff, [AH])
                         end, State2, Active),
    State3.

do_maybe(State, Cmd, Args) ->
    case precondition(State, {call,?MODULE,Cmd,Args}) of
        true ->
            run(State, {call,?MODULE,Cmd,Args});
        false ->
            State
    end.

test_ring_convergence(State) ->
    %%Nodes = get_members(State#state.members, [valid, leaving, exiting]),
    Nodes = State#state.primary,
    case Nodes of
        [] ->
            State;
        _ ->
            State2 = State#state{split=dict:new()},
            {Result, State3} = do_converge_ring(State2, {1000, true}),
            ?assert(Result),
            State3
    end.

do_gossip(State, N2, N1) ->
    case precondition(State, {call,?MODULE,random_gossip,[N2,N1]}) of
        true ->
            {true, run(State, {call,?MODULE,random_gossip,[N2,N1]})};
        false ->
            {false, State}
    end.

do_converge_ring(State, {_, false}) ->
    {true, State};
do_converge_ring(State, {0, _}) ->
    {false, State};
do_converge_ring(State, {RC, true}) ->
    %%Nodes = get_members(State#state.members, [valid, leaving]),
    Nodes = State#state.primary,
    {_, Changed1, State2} =
        lists:foldl(
          fun(N2, {N1, Changed0, State0}) ->
                  case do_gossip(State0, N2, N1) of
                      {true, S} ->
                          Changed = not equal_cstate(get_cstate(State0, N2),
                                                     get_cstate(S, N2)),
                          {N2, Changed0 or Changed, S};
                      _ ->
                          {N2, Changed0, State0}
                  end
          end,
          {hd(Nodes), false, State},
          tl(Nodes)),
    %% Nodes may shutdown due to gossip
    %%Nodes2 = get_members(State2#state.members, [valid, leaving]),
    Nodes2 = State2#state.primary,
    N1 = hd(lists:reverse(Nodes2)),
    {Changed2, State3} =
        lists:foldl(
          fun(N2, {Changed0, State0}) ->
                  case do_gossip(State0, N2, N1) of
                      {true, S} ->
                          Changed = not equal_cstate(get_cstate(State0, N2),
                                                     get_cstate(S, N2)),
                          {Changed0 or Changed, S};
                      _  ->
                          {Changed0, State0}
                  end
          end,
          {Changed1, State2},
          Nodes2 -- [N1]),
    do_converge_ring(State3, {RC - 1, Changed2}).

%% Make a ring with partitions equally distributed across the nodes
make_default_ring(Size, Nodes) ->
    Indices = [Idx || {Idx, _} <- chash:nodes(chash:fresh(Size, undefined))],
    N = erlang:length(Nodes),
    [{lists:nth(Idx+1, Indices), lists:nth((Idx rem N)+1, Nodes)}
     || Idx <- lists:seq(0, Size-1)].

vnode_modules(_State, _Node) ->
    [riak_kv, riak_pipe].

all_owners(?CHSTATE{chring=Ring}) ->
    chash:nodes(Ring).

all_members(?CHSTATE{members=Members}) ->
    get_members(Members).

claiming_members(?CHSTATE{members=Members}) ->
    get_members(Members, [joining, valid, down]).

gossip_members(?CHSTATE{members=Members}) ->
    get_members(Members, [joining, valid, leaving, exiting]).

indices(CState, Node) ->
    AllOwners = all_owners(CState),
    [Idx || {Idx, Owner} <- AllOwners, Owner =:= Node].

owner(?CHSTATE{chring=Ring}, Idx) ->
    chash:lookup(Idx, Ring).

owner_node(CState) ->
    CState?CHSTATE.nodename.

init_node_state(State, Node) ->
    Ring = chash:fresh(State#state.ring_size, Node),
    Indices = indices(?CHSTATE{chring=Ring}, Node),
    RVsn=vclock:increment(Node, vclock:fresh()),
    VClock=vclock:increment(Node, vclock:fresh()),
    CState = ?CHSTATE{nodename=Node,
                      clustername={Node, erlang:now()},
                      members=[{Node, {valid, VClock, []}}],
                      chring=Ring,
                      next=[],
                      claimant=Node,
                      seen=[{Node, VClock}],
                      rvsn=RVsn,
                      vclock=VClock},
    #nstate{chstate=CState,
            partitions=start_vnodes(State, Node, Indices),
            has_data=[]}.

init_node_state(State, Node, CState) ->
    Indices = indices(CState, Node),
    VNodes = start_vnodes(State, Node, Indices),
    #nstate{chstate=CState?CHSTATE{nodename=Node},
            partitions=VNodes,
            has_data=VNodes}.

%% EQC state_m callbacks
initial_state() ->
    #state{primary=[],
           others=[],
           rejoin=orddict:new(),
           removed=[],
           down=[],
           ring_size=?RING,
           members=[],
           allowed=[],
           active_handoffs=[],
           seed={3172,9814,20125},
           split=dict:new()}.

g_initial_nodes() ->
    Nodes = lists:seq(0, ?MAX_NODES-1),
    ?LET(L, shuffle(Nodes), lists:split(?INITIAL_CLUSTER_SIZE, L)).

g_idx(State) ->
    Indices = [Idx || {Idx, _} <- chash:nodes(chash:fresh(State#state.ring_size, undefined))],
    elements(Indices).

g_gossip(State, Gossip) ->
    {OtherNode, Node, OtherCS} = hd(Gossip),
    [{Node, get_nstate(State, Node)}, OtherNode, OtherCS].

g_random_ring(State) ->
    shuffle(lists:seq(0, State#state.ring_size-1)).

g_posint() ->
    ?SUCHTHAT(X, largeint(), X > 0).

g_seed() ->
    {g_posint(), g_posint(), g_posint()}.

g_handoff(State, Nodes) ->
    ?LET(Node, elements(Nodes),
         [elements(vnode_modules(State, Node)), Node, g_idx(State)]).

command(State=#state{primary=[]}) ->
    {call, ?MODULE, initial_cluster, [g_initial_nodes(), g_random_ring(State), g_seed()]};
command(State=#state{primary=Primary, others=Others, active_handoffs=Active}) ->
    oneof([{call, ?MODULE, read, [elements(Primary), g_idx(State)]},
           {call, ?MODULE, random_gossip, [elements(Primary), elements(Primary)]},
           {call, ?MODULE, maybe_handoff, g_handoff(State, Primary)},
           {call, ?MODULE, comm_split, [elements(Primary), elements(Primary)]},
           {call, ?MODULE, comm_join, [elements(Primary), elements(Primary)]},
           {call, ?MODULE, down, [elements(Primary), elements(Primary)]},
           {call, ?MODULE, remove, [elements(Primary), elements(Primary),
                                    frequency([{20,false},{80,true}])]},
           {call, ?MODULE, leave, [elements(Primary)]}]
          ++ [{call, ?MODULE, join, [elements(Others), elements(Primary)]} || Others /= []]
          ++ [{call, ?MODULE, finish_handoff, [elements(Active)]} || Active /= []]
         ).

%% If the model breaks during test case generation, Quickcheck is unable
%% provide a command sequence that led to that error. Therefore, we ignore
%% all errors during generation and then re-trigger then during dynamic
%% execution (by having the postcondition re-invoke the precondition).
precondition(State, Call) ->
    try precondition2(State, Call) of
        Result ->
            Result
    catch
        _:_ ->
            %% ?debugFmt("Precondition failed~n", []),
            true
    end.

precondition_test(State, Call) ->
    try precondition2(State, Call) of
        _Result ->
            true
    catch
        _:_ ->
            false
    end.

precondition2(_State, {call, _, initial_cluster, _}) ->
    true;

precondition2(State, {call, _, join, [Node, PNode]}) ->
    %%(not lists:member(PNode, State#state.removed)) andalso
    lists:member(PNode, State#state.primary) andalso
    lists:member(Node, State#state.others) andalso
    (not is_joining(PNode, State));

precondition2(State, {call, _, leave, [Node]}) ->
    M = get_members(State#state.members, [valid]),
    (erlang:length(State#state.primary) > 1) andalso
        (erlang:length(M) > 1) andalso
        lists:member(Node, State#state.primary) andalso
        dict:is_key(Node, State#state.nstates);

precondition2(State, {call, _, down, [Node, PNode]}) ->
    M = get_members(State#state.members, [valid]),
    (Node /= PNode) andalso
        (erlang:length(State#state.primary) > 1) andalso
        (erlang:length(M) > 1) andalso
        lists:member(Node, State#state.primary) andalso
        lists:member(PNode, State#state.primary) andalso
        dict:is_key(Node, State#state.nstates) andalso
        (not is_joining(PNode, State));

precondition2(State, {call, _, remove, [Node, PNode, _]}) ->
    M = get_members(State#state.members, [valid]),
    (Node /= PNode) andalso
        (erlang:length(State#state.primary) > 1) andalso
        (erlang:length(M) > 1) andalso
        lists:member(Node, State#state.primary) andalso
        lists:member(PNode, State#state.primary) andalso
        dict:is_key(Node, State#state.nstates) andalso
        (not is_joining(PNode, State));

precondition2(State, {call, _, random_gossip, [Node1, Node2]}) ->
    P1 = lists:member(Node1, State#state.primary),
    P2 = lists:member(Node2, State#state.primary),
    case (P1 and P2) of
        true ->
            CState2 = get_cstate(State, Node2),
            Members2 = gossip_members(CState2),
            (Node1 /= Node2) andalso
                lists:member(Node1, Members2) andalso
                can_communicate(State, Node1, Node2);
        false ->
            false
    end;

precondition2(State, {call, _, comm_split, [Node1, Node2]}) ->
    Node1 < Node2 andalso
        lists:member(Node1, State#state.primary) andalso
        lists:member(Node2, State#state.primary) andalso
        can_communicate(State, Node1, Node2);

precondition2(State, {call, _, comm_join, [Node1, Node2]}) ->
    Node1 < Node2 andalso
        lists:member(Node1, State#state.primary) andalso
        lists:member(Node2, State#state.primary) andalso
        not can_communicate(State, Node1, Node2);

precondition2(State, {call, _, maybe_handoff, [Mod, Node, Idx]}) ->
    (State#state.primary /= []) andalso
        lists:member(Mod, vnode_modules(State, Node)) andalso
        dict:is_key(Node, State#state.nstates) andalso
        vnode_running(State, Node, Mod, Idx);

precondition2(State, {call, _, finish_handoff, [AH={_, _, Prev, New}]}) ->
    %%io:format("AH: ~p / ~p~n", [AH, State#state.active_handoffs]),
    lists:member(AH, State#state.active_handoffs) andalso
        can_communicate(State, Prev, New);

precondition2(State, {call, _, read, [Node, _Idx]}) ->
    %% Reads sent to remove nodes are undefined in this model
    (not lists:member(Node, State#state.removed)) andalso
    (not lists:member(Node, State#state.down)) andalso
        lists:member(Node, State#state.primary) andalso
        (not is_joining(Node, State));

%% All commands other than initial_cluster run after initial_cluster
precondition2(State,_) ->
    State#state.primary /= [].

is_joining(Node, State) ->
    CState = get_cstate(State, Node),
    member_status(CState, Node) =:= joining.
            
maybe_inconsistent(State, Node) ->
    NState = get_nstate(State, Node),
    CState = get_cstate(State, Node),
    JoinedTo = NState#nstate.joined_to,
    JTStatus = (catch orddict:fetch(JoinedTo, State#state.members)),
    case {JoinedTo, JTStatus} of
        {undefined, _} ->
            false;
        {_, {invalid, _, _}} ->
            ClaimantCS = get_cstate(State, CState?CHSTATE.claimant),
            not lists:member(Node, get_members(ClaimantCS?CHSTATE.members));
        _ ->
            JoinCS = get_cstate(State, JoinedTo),
            not lists:member(Node, get_members(JoinCS?CHSTATE.members))
    end.

%% If the model breaks during test case generation, Quickcheck is unable
%% provide a command sequence that led to that error. Therefore, we ignore
%% all errors during generation and then re-trigger then during dynamic
%% execution (by having the next_state recomputed by the postcondition).
next_state(State, Result, Call) ->
    OldSeed = save_random(),
    try r_next_state2(State, Result, Call) of
        State2 ->
            State2
    catch
        _:_ -> 
            save_random(State#state{old_seed=OldSeed})
    end.

next_state_test(State, Result, Call) ->
    try r_next_state2(State, Result, Call) of
        State2 ->
            {ok, State2}
    catch
        _:_ ->
            fail
    end.

r_next_state2(State, Result, Call) ->
    State2 = seed_random(State),
    State3 = next_state2(State2, Result, Call),
    State4 = save_random(State3),
    State4.

next_state2(State, _, {call, _, read, _}) ->
    State;

next_state2(State, _Result, {call, _, initial_cluster, [{Members, Others}, RRing, Seed]}) ->
    s_initial_cluster(State#state{seed=Seed}, Members, Others, RRing);

next_state2(State, _Result, {call, _, random_gossip, [Node, OtherNode]}) ->
    NState = get_nstate(State, Node),
    OtherCS = get_cstate(State, OtherNode),
    s_random_gossip(State, Node, NState, OtherNode, OtherCS);

next_state2(State, _Result, {call, _, comm_split, [Node1, Node2]}) ->
    s_comm_split(State, Node1, Node2);

next_state2(State, _Result, {call, _, comm_join, [Node1, Node2]}) ->
    s_comm_join(State, Node1, Node2);

next_state2(State, _Result, {call, _, join, [Node, PNode]}) ->
    NState = get_nstate(State, Node),
    s_join(State, Node, NState, PNode);

next_state2(State, _Result, {call, _, leave, [Node]}) ->
    s_leave(State, Node);

next_state2(State, _Result, {call, _, down, [Node, PNode]}) ->
    s_down(State, Node, PNode);

next_state2(State, _Result, {call, _, remove, [Node, PNode, Shutdown]}) ->
    s_remove(State, Node, PNode, Shutdown);

next_state2(State, _Result, {call, _, maybe_handoff, [Mod, Node, Idx]}) ->
    NState = get_nstate(State, Node),
    s_maybe_handoff(State, Mod, Node, NState, Idx);

next_state2(State, _Result, {call, _, finish_handoff, [AH]}) ->
    s_finish_handoff(State, AH).

postcondition(State, {call, _, read, [Node, Idx]}, _Result) ->
    CState = get_cstate(State, Node),
    Owner = owner(CState, Idx),
    Comm = can_communicate(State, Node, Owner),
    Status = member_status(CState, Node),
    case {Comm, Status} of
        {_, exiting} ->
            %% Exiting nodes don't respond to read requests. We consider
            %% this okay in the model (eg. timeout)
            true;
        {false, _} ->
            %% We cannot communicate, so timeout.
            true;
        _ ->
            check_read(State, Owner, Idx)
    end;

postcondition(State, Cmd, _) ->
    case precondition_test(State, Cmd) of
        false ->
            io:format("Precondition failure: ~p~n", [Cmd]),
            false;
        true ->
            case next_state_test(State, ok, Cmd) of
                fail ->
                    ?debugFmt("Failed~n", []),
                    false;
                {ok, State2} ->
                    T = [%%check_members(State2),
                         %% TODO: Update check_states to work with rejoining
                         %%check_states(State2),
                         check_transfers(State2),
                         check_rvsn(State2),
                         check_sorted_members(State2)],
                    %% ?debugFmt("T: ~p~n", [T]),
                    lists:all(fun(X) -> X =:= true end, T)
            end
    end.

%% postcondition(_,_,fail) ->
%%     false;

%% postcondition(_,_,_) ->
%%     true.

%% Ensure there are no transfer loops scheduled
check_transfers(#state{nstates=NStates}) ->
    dict:fold(fun(_, NState, Acc) ->
                      Acc andalso check_transfers(NState)
              end, true, NStates);
check_transfers(#nstate{chstate=CState}) ->
    L = [Idx || {Idx, Owner, NextOwner, _, _} <- CState?CHSTATE.next,
                Owner =:= NextOwner],
    L =:= [].

check_rvsn(State) ->
    dict:map(fun(_, NState) ->
                     RVsn = NState#nstate.chstate?CHSTATE.rvsn,
                     (RVsn /= undefined) orelse throw(rvsn),
                     NState
             end,
             State#state.nstates),
    true.

check_states(State) ->
    %%?debugFmt("P: ~p~n", [State#state.primary]),
    T1 = check_states(State, State#state.primary),
    Members = get_members(State#state.members),
    %%?debugFmt("M: ~p~n", [State#state.members]),
    T2 = check_states(State, Members),
    R = T1 and T2,
    case R of
        true ->
            true;
        false ->
            false
    end.

check_states(State, Nodes) ->
    NStates = State#state.nstates,
    R = [dict:is_key(N, NStates) || N <- Nodes],
    [] == (lists:usort(R) -- [true]).

check_members(State) ->
    %% Ensure all primary test nodes exist in eventual members cstate
    P = lists:sort(State#state.primary),
    %%io:format("RM: ~p~n", [State#state.members]),
    M = get_members(State#state.members),
    %%T = ([] == (P -- M)),
    T = (P =:= M),
    case T of
        true ->
            true;
        false ->
            io:format("Membership failed: ~p :: ~p~n", [P, M]),
            false
    end.

check_sorted_members(State) ->
    L1 = [begin
              M = NS#nstate.chstate?CHSTATE.members,
              (M /= []) andalso
                  (M == lists:ukeysort(1, M))
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
initial_cluster(_, _, _) ->
    ok.

read(_, _) ->
    ok.

maybe_handoff(_, _, _) ->
    ok.

finish_handoff({_Mod, _Idx, _Prev, _New}) ->
    ok.

join(_, _) ->
    ok.

leave(_) ->
    ok.

down(_, _) ->
    ok.

remove(_, _, _) ->
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
    Ring = lists:foldl(fun({Idx, Node}, Ring0) ->
                               chash:update(Idx, Node, Ring0)
                       end,
                       chash:fresh(State#state.ring_size, undefined),
                       make_default_ring(State#state.ring_size, Members2)),
    Claimant = hd(Members2),
    Members3 = [{M, {valid, vclock:increment(M, vclock:fresh()), []}}
                || M <- Members2],
    RVsn=vclock:increment(Claimant, vclock:fresh()),
    VClock = vclock:increment(Claimant, vclock:fresh()),
    Seen = [{M, VClock} || M <- Members2],
    CState = ?CHSTATE{clustername={Claimant, erlang:now()},
                      members=Members3,
                      chring=Ring,
                      next=[],
                      claimant=Claimant,
                      seen=Seen,
                      rvsn=RVsn,
                      vclock=VClock},
    SMembers = [{Node, init_node_state(State, Node, CState)} || Node <- Members2],
    SOthers = [{Node, init_node_state(State, Node)} || Node <- Others],
    NStates = dict:from_list(SMembers ++ SOthers),
    State#state{members=Members3,
                primary=Members2,
                others=Others,
                allowed=[],
                random_ring=RandomRing,
                nstates=NStates}.

s_random_gossip(State, Node, NState, OtherNode, OtherCS) ->
    State2 = s_gossip(State, {Node, NState}, OtherNode, OtherCS),
    State2.

s_gossip(State, {Node, NState}, OtherNode, OtherCS) ->
    CState = NState#nstate.chstate,
    Members = reconcile_members(CState, OtherCS),
    WrongCluster = (CState?CHSTATE.clustername /= OtherCS?CHSTATE.clustername),
    {PreStatus, _, _} = orddict:fetch(OtherNode, Members),
    IgnoreGossip = (WrongCluster or
                    (PreStatus =:= invalid) or
                    (PreStatus =:= down)),
    case IgnoreGossip of
        true ->
            %% ?debugFmt("Ignoring: ~p~n", [OtherNode]),
            CState2 = CState,
            Changed = false;
        false ->
            {Changed, CState2} = reconcile(State, CState, OtherCS)
    end,
    OtherStatus = member_status(CState2, OtherNode),
    case {WrongCluster, OtherStatus, Changed} of
        {true, _, _} ->
            %% Tell other node to stop gossiping to this node.
            {_, State2} = cast(State, Node, OtherNode, {remove_member, Node}),
            State3 = maybe_shutdown(State2, Node),
            State3;
        {_, down, _} ->
            %% Tell other node to rejoin the cluster.
            {_, State2} = cast(State, Node, OtherNode, {rejoin, CState2}),
            State3 = maybe_shutdown(State2, Node),
            State3;
        {_, invalid, _} ->
            OtherSelfStatus = member_status(OtherCS, OtherNode),
            OtherRemoved = lists:member(OtherNode, State#state.removed),
            case {OtherRemoved, OtherSelfStatus} of
                {_, exiting} ->
                    %% Exiting node never saw shutdown cast, re-send.
                    {_, State2} = cast(State, Node, OtherNode, shutdown),
                    State3 = maybe_shutdown(State2, Node),
                    State3;
                {true, _} ->
                    %% Still alive removed node that should be shutdown
                    {_, State2} = cast(State, Node, OtherNode, shutdown),
                    State3 = maybe_shutdown(State2, Node),
                    State3;
                _ ->
                    case orddict:find(OtherNode, State#state.rejoin) of
                        {ok, true} ->
                            %% This can occur in cases where a node rejoins
                            %% a cluster before the cluster converges on its
                            %% original departure. There is nothing we can do
                            %% so we just ignore the node and hope the user
                            %% notices that the node never rejoined and tries
                            %% again. In practice, we could log a warning here.
                            State2 = maybe_shutdown(State, Node),
                            State2;
                        _ ->
                            %% No legitimate reason this should occur in the model.
                            ?debugFmt("S(~p): ~p~n", [OtherNode, member_status(OtherCS, OtherNode)]),
                            throw(invalid_node_still_alive),
                            State2 = maybe_shutdown(State, Node),
                            State2
                    end
            end;
        {_, _, true} ->
            RRing = State#state.random_ring,
            State2 = ring_changed(State, RRing, {Node, NState}, CState2),
            State3 = maybe_shutdown(State2, Node),
            State3;
        {_, _, _} ->
            State2 = maybe_shutdown(State, Node),
            State2
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
    JoinCS = get_cstate(State, PNode),
    case orddict:find(Node, JoinCS?CHSTATE.members) of
        %% Don't allow a node to rejoin while it's still exiting.
        {ok, {exiting, _, _}} ->
            State;
        _ ->
            Primary2 = lists:usort([Node | State#state.primary]),
            Others2 = State#state.others -- [Node],
            case orddict:is_key(Node, State#state.rejoin) of
                true ->
                    Rejoin2 = orddict:store(Node, true, State#state.rejoin);
                false ->
                    Rejoin2 = State#state.rejoin
            end,
            JoinCS2 = add_member(Node, JoinCS, Node),
            %% ?debugFmt("J: ~p~n", [JoinCS2?CHSTATE.members]),
            JoinCS3 = JoinCS2?CHSTATE{nodename=Node},
            State2 = update_cstate(State, PNode, JoinCS2),
            Partitions = start_vnodes(State, Node, []),
            State3 = update_nstate(State2, Node,
                                   NState#nstate{chstate=JoinCS3,
                                                 partitions=Partitions,
                                                 has_data=[],
                                                 joined_to=PNode}),
            State3#state{primary=Primary2, others=Others2, rejoin=Rejoin2}
    end.

add_member(PNode, CState, Node) ->
    set_member(PNode, CState, Node, joining).

s_leave(State, Node) ->
    CState = get_cstate(State, Node),
    case member_status(CState, Node) of
        valid ->
            CState2 = leave_member(Node, CState, Node),
            State2 = update_cstate(State, Node, CState2),
            State2;
        _ ->
            State
    end.

s_down(State, Node, PNode) ->
    DownCS = get_cstate(State, PNode),
    case orddict:find(Node, DownCS?CHSTATE.members) of
        {ok, {valid, _, _}} ->
            Down2 = [Node|State#state.down],
            DownCS2 = set_member(PNode, DownCS, Node, down),
            State2 = update_cstate(State, PNode, DownCS2),
            State2#state{down=Down2};
        _ ->
            State
    end.

leave_member(PNode, CState, Node) ->
    set_member(PNode, CState, Node, leaving).

exit_member(PNode, CState, Node) ->
    set_member(PNode, CState, Node, exiting).

all_pending(State, Node) ->
    Pending = dict:fold(fun(_, NState, Pending0) ->
                                CState = NState#nstate.chstate,
                                PendingIdx =
                                    [Idx || {Idx, _, NextOwner, _, _} <- CState?CHSTATE.next,
                                            NextOwner =:= Node],
                                Pending0 ++ PendingIdx
                        end, [], State#state.nstates),
    lists:usort(Pending).

s_remove(State, Node, PNode, Shutdown) ->
    PNodeCS = get_cstate(State, PNode),
    NodeCS = get_cstate(State, Node),
    case orddict:find(Node, PNodeCS?CHSTATE.members) of
        {ok, {invalid, _, _}} ->
            State;
        {ok, {_, _, _}} ->
            Removed2 = [Node|State#state.removed],
            PNodeCS2 = remove_member(PNode, PNodeCS, Node),
            State2 = update_cstate(State, PNode, PNodeCS2),

            %% For testing purposes, partitions owned by removed nodes should
            %% be marked as acceptable read failures.
            PendingIdx = all_pending(State, Node),
            Allowed2 = State#state.allowed ++ indices(NodeCS, Node) ++
                PendingIdx,

            case Shutdown of
                true ->
                    Primary2 = State2#state.primary -- [Node],
                    NStates2 = dict:erase(Node, State2#state.nstates);
                false ->
                    Primary2 = State2#state.primary,
                    NStates2 = State2#state.nstates
            end,

            State2#state{primary=Primary2, allowed=Allowed2, nstates=NStates2, removed=Removed2};
        _ ->
            State
    end.

remove_member(PNode, CState, Node) ->
    set_member(PNode, CState, Node, invalid).

set_member(Node, CState, Member, Status) ->
    VClock = vclock:increment(Node, CState?CHSTATE.vclock),
    CState2 = set_member(Node, CState, Member, Status, same_vclock),
    CState2?CHSTATE{vclock=VClock}.

set_member(Node, CState, Member, Status, same_vclock) ->
    Members2 = orddict:update(Member,
                              fun({_, VC, _}) ->
                                      {Status, vclock:increment(Node, VC), []}
                              end,
                              {Status, vclock:increment(Node,
                                                        vclock:fresh()), []},
                              CState?CHSTATE.members),
    CState?CHSTATE{members=Members2}.

handle_cast(State=#state{random_ring=RRing, others=Others, nstates=NStates}, _,
            Node, shutdown) ->
    %% Occasionally allow nodes to re-join
    RandomList = lists:nthtail(erlang:length(RRing) div 2, RRing),
    case lists:member(Node, RandomList) of
        true ->
            %% ?debugFmt("Complete shutdown~n", []),
            Others2 = Others,
            Rejoin2 = State#state.rejoin,
            NStates2 = dict:erase(Node, State#state.nstates);
        false ->
            %% ?debugFmt("Allowing rejoin~n", []),
            Others2 = lists:usort([Node | Others]),
            Rejoin2 = orddict:store(Node, false, State#state.rejoin),
            NStates2 = NStates
    end,
    Primary2 = State#state.primary -- [Node],
    State#state{nstates=NStates2, primary=Primary2, others=Others2,
                rejoin=Rejoin2};

handle_cast(State, _, Node, {rejoin, OtherCS}) ->
    CState = get_cstate(State, Node),
    SameCluster = (CState?CHSTATE.clustername =:= OtherCS?CHSTATE.clustername),
    case SameCluster of
        true ->
            PNode = owner_node(OtherCS),
            NState = get_nstate(State, Node),
            s_join(State, Node, NState, PNode);
        false ->
            State
    end;

handle_cast(State, _, Node, {remove_member, OtherNode}) ->
    CState = get_cstate(State, Node),
    CState2 = remove_member(Node, CState, OtherNode),
    update_cstate(State, Node, CState2);

handle_cast(State, OtherNode, Node, {gossip, OtherCS}) ->
    NState = get_nstate(State, Node),
    State2 = s_gossip(State, {Node, NState}, OtherNode, OtherCS),
    State2;

handle_cast(State, _, _, _) ->
    State.

handle_call(State, _Sender, _Receiver, _Msg) ->
    {ok, State}.

call(State, undefined, _, _) ->
    {fail, State};
call(State, _Node, undefined, _) ->
    {fail, State};
call(State, Sender, Receiver, Msg) ->
    NotDropped = can_communicate(State, Sender, Receiver),
    ValidSender = dict:is_key(Sender, State#state.nstates),
    ValidReceiver = dict:is_key(Receiver, State#state.nstates),
    case (NotDropped and ValidSender and ValidReceiver) of
        false ->
            {fail, State};
        true ->
            handle_call(State, Sender, Receiver, Msg)
    end.

cast(State, undefined, _, _) ->
    {ok, State};
cast(State, _Node, undefined, _) ->
    {ok, State};
cast(State, Sender, Receiver, Msg) ->
    NotDropped = can_communicate(State, Sender, Receiver),
    ValidSender = dict:is_key(Sender, State#state.nstates),
    ValidReceiver = dict:is_key(Receiver, State#state.nstates),
    case (NotDropped and ValidSender and ValidReceiver) of
        false ->
            {ok, State};
        true ->
            {ok, handle_cast(State, Sender, Receiver, Msg)}
    end.

s_maybe_handoff(State, Mod, Node, NState, Idx) ->
    CState = NState#nstate.chstate,
    Active0 = State#state.active_handoffs,
    {_, NextOwner, _} = next_owner(State, CState, Idx, Mod),
    Owner = owner(CState, Idx),
    %%io:format("Owner/Next: ~p / ~p~n", [Owner, NextOwner]),
    Ready = ring_ready(CState),
    Active = case {Ready, Owner, NextOwner} of
                 {_, _, Node} ->
                     Active0;
                 {_, Node, undefined} ->
                     Active0;
                 {true, Node, _} ->
                     lists:usort([{Mod, Idx, Node, NextOwner} | Active0]);
                 {_, _, undefined} ->
                     lists:usort([{Mod, Idx, Node, Owner} | Active0]);
                 {_, _, _} ->
                     Active0
             end,
    State#state{active_handoffs=Active}.

s_finish_handoff(State, AH={Mod, Idx, Prev, New}) ->
    PrevCS1 = get_cstate(State, Prev),
    Owner = owner(PrevCS1, Idx),
    {_, NextOwner, Status} = next_owner(State, PrevCS1, Idx, Mod),
    NewStatus = member_status(PrevCS1, New),

    Active = State#state.active_handoffs -- [AH],
    State2 = State#state{active_handoffs=Active},

    case {Owner, NextOwner, NewStatus, Status} of
        {_, _, invalid, _} ->
            %% Handing off to invalid node, don't give-up data.
            State2;
        {Prev, New, _, awaiting} ->
            PrevNS1 = get_nstate(State2, Prev),
            PrevNS2 = update_partition_status(Mod, PrevNS1, Idx, true, false),
            PrevCS2 = mark_transfer_complete(State, PrevCS1, Idx, Mod),
            PrevNS3 = PrevNS2#nstate{chstate=PrevCS2},
            State3 = update_nstate(State2, Prev, PrevNS3),

            %% New owner is considered to now have data for test purposes
            NewNS = get_nstate(State3, New),
            NewNS2 = update_partition_status(Mod, NewNS, Idx, true, true),
            State4 = update_nstate(State3, New, NewNS2),
            State4;
        {Prev, New, _, complete} ->
            %% Do nothing
            State2;
        {Prev, _, _, _} ->
            %% This may occur when handing off to a removed node, which we
            %% consider valid in this model.
            case lists:member(New, State#state.removed) of
                true ->
                    ok;
                false ->
                    throw("Why am I handing off?")
            end,
            %% Handoff wasn't to node that is scheduled in next, so no change.
            State2;
        {_, _, _, _} ->
            PrevCS1 = get_cstate(State2, Prev),
            PrevNS1 = get_nstate(State2, Prev),
            Ready = ring_ready(PrevCS1),
            case Ready of
                true ->
                    %% Delete data, shutdown vnode, and maybe shutdown node
                    %%io:format("Shutting down vnode ~p/~p~n", [Prev, Idx]),
                    PrevNS2 = update_partition_status(Mod, PrevNS1, Idx, false, false),
                    State3 = update_nstate(State2, Prev, PrevNS2),
                    State3;
                false ->
                    %% Delete data
                    PrevNS2 = update_partition_status(Mod, PrevNS1, Idx, true, false),
                    State3 = update_nstate(State2, Prev, PrevNS2),
                    State3
            end
    end.

maybe_shutdown(State, Node) ->
    CState = get_cstate(State, Node),
    Ready = ring_ready(CState),
    Next = CState?CHSTATE.next,
    NoIndices = (indices(CState, Node) =:= []),
    NoPartitions = (count_vnodes(State, Node) =:= 0),
    NoPendingIndices = ([] =:= ([Idx || {Idx, _, NextOwner, _, _} <- Next,
                                        NextOwner =:= Node])),
    Shutdown = (NoIndices and NoPartitions and NoPendingIndices),
    Status = member_status(CState, Node),
    case {Ready, Shutdown, Status} of
        {true, true, leaving} ->
            ?OUT("Exiting node ~p~n", [Node]),
            CState2 = exit_member(Node, CState, Node),
            update_cstate(State, Node, CState2);
        _ ->
            State
    end.
            
reconcile(State, CS01, CS02) ->
    VNode = owner_node(CS01),
    CS03 = update_seen(VNode, CS01),
    CS04 = update_seen(VNode, CS02),
    Seen = reconcile_seen(CS03, CS04),
    CS1 = CS03?CHSTATE{seen=Seen},
    CS2 = CS04?CHSTATE{seen=Seen},
    SeenChanged = not equal_seen(CS01, CS1),

    VC1 = CS1?CHSTATE.vclock,
    VC2 = CS2?CHSTATE.vclock,
    VC3 = vclock:merge([VC1, VC2]),
    %%io:format("V1: ~p~nV2: ~p~n", [VC1, VC2]),
    Newer = vclock:descends(VC1, VC2),
    Older = vclock:descends(VC2, VC1),
    Equal = equal_cstate(CS1, CS2),
    case {Equal, Newer, Older} of
        {_, true, false} ->
            %%io:format("CS1: ~p~n", [CS1]),
            {SeenChanged, CS1?CHSTATE{vclock=VC3}};
        {_, false, true} ->
            %%io:format("CS2: ~p~n", [CS2]),
            {true, CS2?CHSTATE{nodename=VNode, vclock=VC3}};
        {true, _, _} ->
            {SeenChanged, CS1?CHSTATE{vclock=VC3}};
        {_, true, true} ->
            io:format("C1: ~p~nC2: ~p~n", [CS1, CS2]),
            throw("Equal vclocks, but cstate unequal");
        {_, false, false} ->
            CS3 = reconcile_divergent(State, VNode, CS1, CS2),
            {true, CS3?CHSTATE{nodename=VNode}}
    end.

reconcile_divergent(State, VNode, CS1, CS2) ->
    VClock = vclock:increment(VNode, vclock:merge([CS1?CHSTATE.vclock,
                                                   CS2?CHSTATE.vclock])),
    Members = reconcile_members(CS1, CS2),
    CS3 = reconcile_ring(State, CS1, CS2, get_members(Members)),
    CS3?CHSTATE{vclock=VClock, members=Members}.

reconcile_members(CS1, CS2) ->
    %%?debugFmt("M1: ~p~nM2: ~p~n", [CS1?CHSTATE.members, CS2?CHSTATE.members]),
    orddict:merge(
      fun(_K, {Valid1, VC1, _}, {Valid2, VC2, _}) ->
              New1 = vclock:descends(VC1, VC2),
              New2 = vclock:descends(VC2, VC1),
              MergeVC = vclock:merge([VC1, VC2]),
              case {New1, New2} of
                  {true, false} ->
                      {Valid1, MergeVC, []};
                  {false, true} ->
                      {Valid2, MergeVC, []};
                  {_, _} ->
                      {merge_status(Valid1, Valid2), MergeVC, []}
              end
      end,
      CS1?CHSTATE.members,
      CS2?CHSTATE.members).

reconcile_seen(CS1, CS2) ->
    orddict:merge(fun(_, VC1, VC2) ->
                          vclock:merge([VC1, VC2])
                  end, CS1?CHSTATE.seen, CS2?CHSTATE.seen).

merge_next_status(complete, _) ->
    complete;
merge_next_status(_, complete) ->
    complete;
merge_next_status(awaiting, awaiting) ->
    awaiting.

reconcile_next(Next1, Next2) ->
    lists:zipwith(fun({Idx, Owner, Node, Transfers1, Status1},
                      {Idx, Owner, Node, Transfers2, Status2}) ->
                          {Idx, Owner, Node,
                           ordsets:union(Transfers1, Transfers2),
                           merge_next_status(Status1, Status2)}
                  end, Next1, Next2).

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

reconcile_ring(_State,
               CS1=?CHSTATE{claimant=Claimant1, rvsn=VC1, next=Next1},
               CS2=?CHSTATE{claimant=Claimant2, rvsn=VC2, next=Next2},
               Members) ->
    V1Newer = vclock:descends(VC1, VC2),
    V2Newer = vclock:descends(VC2, VC1),
    EqualVC = (vclock:equal(VC1, VC2) and (Claimant1 =:= Claimant2)),
    %%io:format("Next1: ~p~nNext2: ~p~n", [Next1, Next2]),
    case {EqualVC, V1Newer, V2Newer} of
        {true, _, _} ->
            ?assertEqual(CS1?CHSTATE.chring, CS2?CHSTATE.chring),
            Next = reconcile_next(Next1, Next2),
            CS1?CHSTATE{next=Next};
        {_, true, false} ->
            Next = reconcile_divergent_next(Next1, Next2),
            CS1?CHSTATE{next=Next};
        {_, false, true} ->
            Next = reconcile_divergent_next(Next2, Next1),
            CS2?CHSTATE{next=Next};
        {_, _, _} ->
            CValid1 = lists:member(Claimant1, Members),
            CValid2 = lists:member(Claimant2, Members),
            case {CValid1, CValid2} of
                {true, false} ->
                    Next = reconcile_divergent_next(Next1, Next2),
                    CS1?CHSTATE{next=Next};
                {false, true} ->
                    Next = reconcile_divergent_next(Next2, Next1),
                    CS2?CHSTATE{next=Next};
                {false, false} ->
                    %% This can occur when removed/down nodes are still
                    %% up and gossip to each other. We need to pick a
                    %% claimant to handle this case, although the choice
                    %% is irrelevant as a correct valid claimant will
                    %% eventually emerge when the ring converges.
                    case Claimant1 < Claimant2 of
                        true ->
                            Next = reconcile_divergent_next(Next1, Next2),
                            CS1?CHSTATE{next=Next};
                        false ->
                            Next = reconcile_divergent_next(Next2, Next1),
                            CS2?CHSTATE{next=Next}
                    end;
                {true, true} ->
                    %% This should never happen in normal practice.
                    %% But, we need to handle it in case of user error.
                    case Claimant1 < Claimant2 of
                        true ->
                            Next = reconcile_divergent_next(Next1, Next2),
                            CS1?CHSTATE{next=Next};
                        false ->
                            Next = reconcile_divergent_next(Next2, Next1),
                            CS2?CHSTATE{next=Next}
                    end
            end
    end.

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

update_partition_status(Mod, NState, Idx, Active, HasData) ->
    Parts = NState#nstate.partitions,
    Data = NState#nstate.has_data,
    P = case orddict:find(Mod, Parts) of error -> []; {ok, P0} -> P0 end,
    D = case orddict:find(Mod, Data) of error -> []; {ok, D0} -> D0 end,
    {P2, D2} = case {Active, HasData} of
                   {false, _} ->
                       {ordsets:del_element(Idx, P),
                        ordsets:del_element(Idx, D)};
                   {true, false} ->
                       {ordsets:add_element(Idx, P),
                        ordsets:del_element(Idx, D)};
                   {true, true} ->
                       {ordsets:add_element(Idx, P),
                        ordsets:add_element(Idx, D)}
               end,
    NState#nstate{partitions=orddict:store(Mod, P2, Parts),
                  has_data=orddict:store(Mod, D2, Data)}.

start_vnodes(State, Node, Indices) ->
    VMods = lists:sort(vnode_modules(State, Node)),
    IndicesSet = ordsets:from_list(Indices),
    [{Mod, IndicesSet} || Mod <- VMods].

count_vnodes(State, Node) ->
    NState = get_nstate(State, Node),
    length(lists:flatmap(fun({_,Vs}) -> Vs end, NState#nstate.partitions)).

vnode_running(State, Node, Mod, Idx) ->
    NState = get_nstate(State, Node),
    case orddict:find(Mod, NState#nstate.partitions) of
        error ->
            false;
        {ok, VNodes} ->
            lists:member(Idx, VNodes)
    end.

get_nstate(State, Node) ->
    dict:fetch(Node, State#state.nstates).

update_nstate(State, Node, NState) ->
    CState = NState#nstate.chstate,
    State2 = update_members(State, CState),
    NS = dict:store(Node, NState, State2#state.nstates),
    State2#state{nstates=NS}.

get_cstate(State, Node) ->
    (get_nstate(State, Node))#nstate.chstate.

member_status(?CHSTATE{members=Members}, Node) ->
    member_status(Members, Node);
member_status(Members, Node) ->
    case orddict:find(Node, Members) of
        {ok, {Status, _, _}} ->
            Status;
        _ ->
            invalid
    end.

get_members(Members) ->
    get_members(Members, [joining, valid, leaving, exiting, down]).

get_members(Members, Types) ->
    [Node || {Node, {V, _, _}} <- Members, lists:member(V, Types)].

update_seen(Node, CState=?CHSTATE{vclock=VClock, seen=Seen}) ->
    Seen2 = orddict:update(Node,
                           fun(SeenVC) ->
                                   vclock:merge([SeenVC, VClock])
                           end,
                           VClock, Seen),
    CState?CHSTATE{seen=Seen2}.

update_seen(State, Node, CState) ->
    CState2 = update_seen(Node, CState),
    State2 = save_cstate(State, Node, CState2),
    Changed = (CState2?CHSTATE.seen /= CState?CHSTATE.seen),
    {Changed, State2, CState2}.

update_cstate(NState=#nstate{}, Node, CState) ->
    CState2 = update_seen(Node, CState),
    %%CState2 = increment_cstate_vclock(Node, CState1),
    %%io:format("Update ~p :: ~p~n", [Node, CState2]),
    NState#nstate{chstate=CState2};
update_cstate(State=#state{}, Node, CState) ->
    CState2 = update_seen(Node, CState),
    %%io:format("Update ~p :: ~p~n", [Node, CState2]),
    %%CState2 = increment_cstate_vclock(Node, CState1),
    State2 = save_cstate(State, Node, CState2),
    State3 = update_members(State2, CState2),
    State3.


save_cstate(State, Node, CState) ->
    NS = dict:update(Node,
                     fun(NState) ->
                             NState#nstate{chstate=CState}
                     end,
                     #nstate{chstate=CState},
                     State#state.nstates),
    State#state{nstates=NS}.

ring_ready(CState0) ->
    Owner = owner_node(CState0),
    CState = update_seen(Owner, CState0),
    Seen = CState?CHSTATE.seen,
    %% TODO: Should we add joining here?
    %%Members = get_members(CState?CHSTATE.members, [joining, valid, leaving, exiting]),
    Members = get_members(CState?CHSTATE.members, [valid, leaving, exiting]),
    VClock = CState?CHSTATE.vclock,
    R = [begin
             case orddict:find(Node, Seen) of
                 error ->
                     false;
                 {ok, VC} ->
                     vclock:equal(VClock, VC)
             end
         end || Node <- Members],
    ?OUT("R: ~p~n", [R]),
    case lists:all(fun(X) -> X =:= true end, R) of
        true ->
            %%throw("Ring ready"),
            true;
        false ->
            false
    end.

seed_random(State) ->
    OldSeed = random:seed(State#state.seed),
    State#state{old_seed=OldSeed}.

save_random(State=#state{old_seed=undefined}) ->
    Seed = random:seed(),
    State#state{seed=Seed};
save_random(State=#state{old_seed=OldSeed}) ->
    Seed = random:seed(OldSeed),
    State#state{seed=Seed}.

save_random() ->
    Seed = random:seed(),
    random:seed(Seed),
    Seed.

ring_changed(State, _RRing, {Node, _NState}, CState0) ->
    CState = update_seen(Node, CState0),
    case ring_ready(CState) of
        false ->
            update_cstate(State, Node, CState);
        true ->
            {C1, State2, CState2} = maybe_update_claimant(State, Node, CState),
            {C2, State3, CState3} = maybe_handle_joining(State2, Node, CState2),
            case C2 of
                true ->
                    CState4 = increment_cstate_vclock(Node, CState3),
                    update_cstate(State3, Node, CState4);
                false ->
                    {C3, State4, CState4} = maybe_update_ring(State3, Node, CState3),
                    {C4, State5, CState5} = maybe_remove_exiting(State4, Node, CState4),

                    case (C1 or C2 or C3 or C4) of
                        true ->
                            CState6 = increment_cstate_vclock(Node, CState5),
                            update_cstate(State5, Node, CState6);
                        false ->
                            update_cstate(State5, Node, CState5)
                    end
            end
    end.

maybe_update_claimant(State, Node, CState) ->
    Members = get_members(CState?CHSTATE.members, [valid, leaving]),
    Claimant = CState?CHSTATE.claimant,
    RVsn = CState?CHSTATE.rvsn,
    NextClaimant = hd(Members ++ [undefined]),
    ClaimantMissing = not lists:member(Claimant, Members),

    case {ClaimantMissing, NextClaimant} of
        {true, Node} ->
            %% Become claimant
            ?assert(Node /= Claimant),
            RVsn2 = vclock:increment(Claimant, RVsn),
            CState2 = CState?CHSTATE{claimant=Node, rvsn=RVsn2},
            {true, State, CState2};
        _ ->
            {false, State, CState}
    end.

maybe_update_ring(State, Node, CState) ->
    Claimant = CState?CHSTATE.claimant,
    case Claimant of
        Node ->
            case claiming_members(CState) of
                [] ->
                    {false, State, CState};
                _ ->
                    RRing = State#state.random_ring,
                    {Changed, State2, CState2} = update_ring(State, RRing, Node, CState),
                    {Changed, State2, CState2}
            end;
        _ ->
            {false, State, CState}
    end.

maybe_remove_exiting(State, Node, CState) ->
    Claimant = CState?CHSTATE.claimant,
    case Claimant of
        Node ->
            Exiting = get_members(CState?CHSTATE.members, [exiting]) -- [Node],
            %%io:format("Claimant ~p removing exiting ~p~n", [Node, Exiting]),
            Changed = (Exiting /= []),
            {State2, CState2} =
                lists:foldl(fun(ENode, {State0, CState0}) ->
                                    {_, State02} = cast(State0, Node, ENode, shutdown),
                                    CState02 = set_member(Node, CState0, ENode, invalid, same_vclock),
                                    {State02, CState02}
                            end, {State, CState}, Exiting),
            {Changed, State2, CState2};
        _ ->
            {false, State, CState}
    end.

maybe_handle_joining(State, Node, CState) ->
    Claimant = CState?CHSTATE.claimant,
    case Claimant of
        Node ->
            Joining = get_members(CState?CHSTATE.members, [joining]),
            %% ?debugFmt("Claimant ~p joining ~p~n", [Node, Joining]),
            Changed = (Joining /= []),
            {State2, CState2} =
                lists:foldl(fun(JNode, {State0, CState0}) ->
                                    State02 = State0,
                                    CState02 = set_member(Node, CState0, JNode, valid, same_vclock),
                                    {State02, CState02}
                            end, {State, CState}, Joining),
            {Changed, State2, CState2};
        _ ->
            {false, State, CState}
    end.

get_counts(Nodes, Ring) ->
    Empty = [{Node, 0} || Node <- Nodes],
    Counts = lists:foldl(fun({_Idx, Node}, Counts) ->
                                 case lists:member(Node, Nodes) of
                                     true ->
                                         dict:update_counter(Node, 1, Counts);
                                     false ->
                                         Counts
                                 end
                         end, dict:from_list(Empty), Ring),
    dict:to_list(Counts).

update_ring(State, _RRing, CNode, CState) ->
    Next0 = CState?CHSTATE.next,

    ?ROUT("Members: ~p~n", [CState?CHSTATE.members]),
    ?ROUT("Updating ring :: next0 : ~p~n", [Next0]),

    %% Remove tuples from next for removed nodes
    InvalidMembers = get_members(CState?CHSTATE.members, [invalid]),
    Next2 = lists:filter(fun(NInfo) ->
                                 {Owner, NextOwner, _} = next_owner(State, NInfo),
                                 not lists:member(Owner, InvalidMembers) and
                                 not lists:member(NextOwner, InvalidMembers)
                         end, Next0),
    CState2 = CState?CHSTATE{next=Next2},

    %% Transfer ownership after completed handoff
    {RingChanged1, CState3} = transfer_ownership(State, CState2),
    ?ROUT("Updating ring :: next1 : ~p~n", [CState3?CHSTATE.next]),

    %% Ressign leaving/inactive indices
    {RingChanged2, State2, CState4} = reassign_indices(State, CState3),
    ?ROUT("Updating ring :: next2 : ~p~n", [CState4?CHSTATE.next]),

    %% Rebalance the ring as necessary
    Next3 = rebalance_ring(CNode, CState4),

    %% Remove transfers to/from down nodes
    Next4 = handle_down_nodes(CState4, Next3),

    NextChanged = (Next0 /= Next4),
    Changed = (NextChanged or RingChanged1 or RingChanged2),
    case Changed of
        true ->
            RVsn2 = vclock:increment(CNode, CState4?CHSTATE.rvsn),
            ?ROUT("Updating ring :: next3 : ~p~n", [Next4]),
            {true, State2, CState4?CHSTATE{next=Next4, rvsn=RVsn2}};
        false ->
            {false, State, CState}
    end.

transfer_ownership(State, CState=?CHSTATE{next=Next}) ->
    %% Remove already completed and transfered changes
    Next2 = lists:filter(fun(NInfo={Idx, _, _, _, _}) ->
                                 {_, NewOwner, S} = next_owner(State, NInfo),
                                 not ((S == complete) and
                                      (owner(CState, Idx) =:= NewOwner))
                         end, Next),

    CState2 = lists:foldl(fun(NInfo={Idx, _, _, _, _}, CState0) ->
                                  case next_owner(State, NInfo) of
                                      {_, Node, complete} ->
                                          riak_core_ring:transfer_node(Idx, Node, CState0);
                                      _ ->
                                          CState0
                                  end
                          end, CState, Next2),

    NextChanged = (Next2 /= Next),
    RingChanged = (all_owners(CState) /= all_owners(CState2)),
    Changed = (NextChanged or RingChanged),
    {Changed, CState2?CHSTATE{next=Next2}}.

reassign_indices(State, CState=?CHSTATE{next=Next}) ->
    Invalid = get_members(CState?CHSTATE.members, [invalid]),
    {State2, CState2} =
        lists:foldl(fun(Node, {State0, Ring0}) ->
                            Allowed2 = State0#state.allowed ++ indices(Ring0, Node),
                            Ring2 = remove_node(Ring0, Node, invalid),
                            {State0#state{allowed=Allowed2}, Ring2}
                    end, {State, CState}, Invalid),
    CState3 = case Next of
                  [] ->
                      Leaving = get_members(CState?CHSTATE.members, [leaving]),
                      lists:foldl(fun(Node, Ring0) ->
                                          %%remove_from_cluster(Ring0, Node)
                                          remove_node(Ring0, Node, leaving)
                                  end, CState2, Leaving);
                  _ ->
                      CState2
              end,
    Owners1 = all_owners(CState),
    Owners2 = all_owners(CState3),
    RingChanged = (Owners1 /= Owners2),
    NextChanged = (Next /= CState3?CHSTATE.next),
    {RingChanged or NextChanged, State2, CState3}.

rebalance_ring(_CNode, CState=?CHSTATE{next=[]}) ->
    Members = claiming_members(CState),
    CState2 = lists:foldl(fun(Node, Ring0) ->
                                  claim_until_balanced(Ring0, Node)
                          end, CState, Members),
    Owners1 = all_owners(CState),
    Owners2 = all_owners(CState2),
    Owners3 = lists:zip(Owners1, Owners2),
    Next = [{Idx, PrevOwner, NewOwner, [], awaiting}
            || {{Idx, PrevOwner}, {Idx, NewOwner}} <- Owners3,
               PrevOwner /= NewOwner],
    %% ?debugFmt("Next: ~p~n", [Next]),
    Next;
rebalance_ring(_CNode, _CState=?CHSTATE{next=Next}) ->
    Next.

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

claim_until_balanced(Ring, Node) ->
    %%{WMod, WFun} = app_helper:get_env(riak_core, wants_claim_fun),
    {WMod, WFun} = {riak_core_claim, default_wants_claim},
    NeedsIndexes = apply(WMod, WFun, [Ring, Node]),
    case NeedsIndexes of
        no ->
            Ring;
        {yes, _NumToClaim} ->
            %%{CMod, CFun} = app_helper:get_env(riak_core, choose_claim_fun),
            {CMod, CFun} = {riak_core_claim, default_choose_claim},
            NewRing = CMod:CFun(Ring, Node),
            claim_until_balanced(NewRing, Node)
    end.

all_next_owners(?CHSTATE{next=Next}) ->
    [{Idx, NextOwner} || {Idx, _, NextOwner, _, _} <- Next].

change_owners(CState, Reassign) ->
    lists:foldl(fun({Idx, NewOwner}, CState0) ->
                        riak_core_ring:transfer_node(Idx, NewOwner, CState0)
                end, CState, Reassign).
    
remove_node(CState, Node, Status) ->
    Indices = indices(CState, Node),
    remove_node(CState, Node, Status, Indices).

remove_node(CState, _Node, _Status, []) ->
    CState;
remove_node(CState, Node, Status, Indices) ->
    %% ?debugFmt("Reassigning from ~p: ~p~n", [Node, indices(CState, Node)]),
    CStateT1 = change_owners(CState, all_next_owners(CState)),
    CStateT2 = remove_from_cluster(CStateT1, Node),
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

    %% Unlike rebalance_ring, remove_node can be called when Next is non-empty,
    %% therefore we need to merge the values. Original Next has priority.
    Next2 = lists:ukeysort(1, CState?CHSTATE.next ++ Next),
    CState2 = change_owners(CState, Reassign),
    CState2?CHSTATE{next=Next2}.

remove_from_cluster(Ring, ExitingNode) ->
    % Get a list of indices owned by the ExitingNode...
    AllOwners = riak_core_ring:all_owners(Ring),

    % Transfer indexes to other nodes...
    ExitRing =
        case attempt_simple_transfer(Ring, AllOwners, ExitingNode) of
            {ok, NR} ->
                NR;
            target_n_fail ->
                %% re-diagonalize
                %% first hand off all claims to *any* one else,
                %% just so rebalance doesn't include exiting node
                Members = claiming_members(Ring),
                Other = hd(lists:delete(ExitingNode, Members)),
                TempRing = lists:foldl(
                             fun({I,N}, R) when N == ExitingNode ->
                                     riak_core_ring:transfer_node(I, Other, R);
                                (_, R) -> R
                             end,
                             Ring,
                             AllOwners),
                riak_core_claim:claim_rebalance_n(TempRing, Other)
        end,

    ExitRing.

attempt_simple_transfer(Ring, Owners, ExitingNode) ->
    %%TargetN = app_helper:get_env(riak_core, target_n_val),
    TargetN = 3,
    attempt_simple_transfer(Ring, Owners,
                            TargetN,
                            ExitingNode, 0,
                            [{O,-TargetN} || O <- claiming_members(Ring),
                                             O /= ExitingNode]).
attempt_simple_transfer(Ring, [{P, Exit}|Rest], TargetN, Exit, Idx, Last) ->
    %% handoff
    case [ N || {N, I} <- Last, Idx-I >= TargetN ] of
        [] ->
            target_n_fail;
        Candidates ->
            %% these nodes don't violate target_n in the reverse direction
            StepsToNext = fun(Node) ->
                                  length(lists:takewhile(
                                           fun({_, Owner}) -> Node /= Owner end,
                                           Rest))
                          end,
            case lists:filter(fun(N) -> 
                                 Next = StepsToNext(N),
                                 (Next+1 >= TargetN)
                                          orelse (Next == length(Rest))
                              end,
                              Candidates) of
                [] ->
                    target_n_fail;
                Qualifiers ->
                    %% these nodes don't violate target_n forward
                    Chosen = lists:nth(random:uniform(length(Qualifiers)),
                                       Qualifiers),
                    %% choose one, and do the rest of the ring
                    attempt_simple_transfer(
                      riak_core_ring:transfer_node(P, Chosen, Ring),
                      Rest, TargetN, Exit, Idx+1,
                      lists:keyreplace(Chosen, 1, Last, {Chosen, Idx}))
            end
    end;
attempt_simple_transfer(Ring, [{_, N}|Rest], TargetN, Exit, Idx, Last) ->
    %% just keep track of seeing this node
    attempt_simple_transfer(Ring, Rest, TargetN, Exit, Idx+1,
                            lists:keyreplace(N, 1, Last, {N, Idx}));
attempt_simple_transfer(Ring, [], _, _, _, _) ->
    {ok, Ring}.

%% Update global members set used for test case generation.
update_members(State=#state{members=Members}, CS2) ->
    CS1 = ?CHSTATE{members=Members},
    Members2 = reconcile_members(CS1, CS2),
    State#state{members=Members2}.

increment_cstate_vclock(Node, CState) ->
    VClock = vclock:increment(Node, CState?CHSTATE.vclock),
    CState?CHSTATE{vclock=VClock}.

%% Returns true if the nodes can communicate (ie. not split)
can_communicate(_State, N1, N2) when N1 =:= N2 ->
    true;
can_communicate(State, N1, N2) ->
    %% Ensure both nodes are not shutdown and are still running.
    dict:is_key(N1, State#state.nstates) and
    dict:is_key(N2, State#state.nstates) and
    (not dict:is_key({N1, N2}, State#state.split)).

next_owner(State, CState, Idx) ->
    case lists:keyfind(Idx, 1, CState?CHSTATE.next) of
        false ->
            {undefined, undefined, undefined};
        NInfo ->
            next_owner(State, NInfo)
    end.

next_owner(_State, CState, Idx, Mod) ->
    case lists:keyfind(Idx, 1, CState?CHSTATE.next) of
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

next_owner(_State, {_, Owner, NextOwner, _Transfers, Status}) ->
    {Owner, NextOwner, Status}.

mark_transfer_complete(State, CState=?CHSTATE{next=Next, vclock=VClock}, Idx, Mod) ->
    {Idx, Owner, NextOwner, Transfers, Status} = lists:keyfind(Idx, 1, Next),
    Transfers2 = ordsets:add_element(Mod, Transfers),
    VNodeMods = vnode_modules(State, Owner),
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
   
%% VClock timestamps may be different for test generation versus
%% shrinking/checking phases. Normalize to test for equality.
equal_cstate(CS1, CS2) ->
    T1 = equal_members(CS1?CHSTATE.members, CS2?CHSTATE.members),
    T2 = equal_vclock(CS1?CHSTATE.rvsn, CS2?CHSTATE.rvsn),
    T3 = equal_seen(CS1, CS2),

    %% Clear fields checked manually and test remaining through equality.
    CS3=CS1?CHSTATE{nodename=ok, members=ok, vclock=ok, rvsn=ok, seen=ok},
    CS4=CS2?CHSTATE{nodename=ok, members=ok, vclock=ok, rvsn=ok, seen=ok},
    T4 = (CS3 =:= CS4),
    T1 and T2 and T3 and T4.

equal_members(M1, M2) ->
    L = orddict:merge(fun(_, {Status1, VC1, _}, {Status2, VC2, _}) ->
                              (Status1 =:= Status2) andalso
                                  equal_vclock(VC1, VC2)
                      end, M1, M2),
    {_, R} = lists:unzip(L),
    lists:all(fun(X) -> X =:= true end, R).

equal_seen(CS1, CS2) ->
    Seen1 = filtered_seen(CS1),
    Seen2 = filtered_seen(CS2),
    L = orddict:merge(fun(_, VC1, VC2) ->
                              equal_vclock(VC1, VC2)
                      end, Seen1, Seen2),
    {_, R} = lists:unzip(L),
    lists:all(fun(X) -> X =:= true end, R).

filtered_seen(CS=?CHSTATE{seen=Seen}) ->
    case get_members(CS?CHSTATE.members) of
        [] ->
            Seen;
        Members ->
            orddict:filter(fun(N, _) -> lists:member(N, Members) end, Seen)
    end.
  
equal_vclock(VC1, VC2) ->
    VC3 = [{Node, {Count, 1}} || {Node, {Count, _TS}} <- VC1],
    VC4 = [{Node, {Count, 1}} || {Node, {Count, _TS}} <- VC2],
    vclock:equal(VC3, VC4).

check_read(State, Owner, Idx) ->
    OwnerDown = lists:member(Owner, State#state.down),
    MayFail = lists:member(Idx, State#state.allowed),
    case (MayFail or OwnerDown) of
        true ->
            true;
        false ->
            check_read2(State, Owner, Idx)
    end.

check_read2(State, Owner, Idx) -> 
    %%io:format("Checking if ~p has data for ~p~n", [Owner, Idx]),
    %% ?debugFmt("Checking if ~p has data for ~p~n", [Owner, Idx]),
    NState = get_nstate(State, Owner),
    Data = orddict:fetch(riak_kv, NState#nstate.has_data),
    Found = lists:member(Idx, Data),
    case Found of
        true ->
            true;
        false ->
            %%io:format("Checking forward~n", []),
            %% Check if we should forward
            OwnerCS = get_cstate(State, Owner),
            case next_owner(State, OwnerCS, Idx, riak_kv) of
                {_, Node, complete} ->
                    ?assert(Node /= Owner),
                    case can_communicate(State, Owner, Node) of
                        true ->
                            check_read(State, Node, Idx);
                        false ->
                            %% Timeouts are considered safe in the test/model,
                            %% it's just not_founds that we don't want.
                            %%throw(read_timeout)
                            true
                    end;
                _ ->
                    %%io:format("Invalid read of ~p / ~p~n", [Idx, Owner]),
                    false
            end
    end.

invoke({call, M, F, A}) ->
    apply(M, F, A).

run(S, Cmd) ->
    %% ?debugFmt("R: ~p~n", [Cmd]),
    ?assertEqual(true, precondition(S, Cmd)),
    R = invoke(Cmd),
    S2 = next_state2(S, R, Cmd),
    ?assertEqual(true, postcondition(S, Cmd, R)),
    S2.

run_cmd(S, {_,_,Cmd}) ->
    run(S,Cmd).

run_cmds(Cmds) ->
    run_cmds(?RING, Cmds).

run_cmds(RingSize, Cmds) ->
    S0 = (initial_state())#state{ring_size=RingSize},
    State = lists:foldl(fun({_,_,Cmd}, S) -> run(S, Cmd) end, S0, Cmds),
    ?debugFmt("------------------------------~n", []),
    %% Most update to data active member
    {_, Member0} = lists:foldl(fun(Node, {VClock0, Result}) ->
                                       CS = get_cstate(State, Node),
                                       VClock1 = CS?CHSTATE.vclock,
                                       case vclock:descends(VClock1, VClock0) of
                                           true ->
                                               {VClock1, Node};
                                           false ->
                                               {VClock0, Result}
                                       end
                               end, {vclock:fresh(), undefined}, State#state.primary),
    ?debugFmt("Member0: ~p~n", [Member0]),
    CState = get_cstate(State, Member0),
    Owners1 = all_owners(CState),
    State2 = test_ring_convergence(State),
    State3 = handoff_all(State2),
    State4 = test_ring_convergence(State3),
    %%State4 = State,
    Owners2 = all_owners(get_cstate(State4, Member0)),
    CState2 = get_cstate(State4, Member0),
    ?debugFmt("Owners1: ~p~nOwners2: ~p~n", [Owners1, Owners2]),
    ?debugFmt("Next0: ~p~n", [CState?CHSTATE.next]),
    ?debugFmt("Next1: ~p~n", [CState2?CHSTATE.next]),
    State4.

%% TODO: Re-add manual tests.
manual_test_list() ->
    [fun test_down_reassign/0].

test_down_reassign() ->    
    run_cmds([{set,{var,1},
               {call,new_cluster_membership_model_eqc,initial_cluster,
                [{[2,3,5],[0,1,4]},[0,1,2,3,4,5,6,7],{1,1,1}]}},
              {set,{var,2},{call,new_cluster_membership_model_eqc,down,[2,3]}},
              {set,{var,4},{call,new_cluster_membership_model_eqc,leave,[3]}},
              {set,{var,5},{call,new_cluster_membership_model_eqc,join,[1,5]}},
              {set,{var,6},{call,new_cluster_membership_model_eqc,join,[0,3]}},
              {set,{var,9},{call,new_cluster_membership_model_eqc,random_gossip,[3,2]}},
              {set,{var,20},{call,new_cluster_membership_model_eqc,random_gossip,[5,2]}},
              {set,{var,30},{call,new_cluster_membership_model_eqc,random_gossip,[1,5]}},
              {set,{var,56},{call,new_cluster_membership_model_eqc,random_gossip,[3,1]}},
              {set,{var,69},{call,new_cluster_membership_model_eqc,down,[0,3]}}]),
    ok.

-endif.
-endif.
