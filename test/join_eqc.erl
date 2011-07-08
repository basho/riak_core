-module(join_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-define(TEST_ITERATIONS, 3000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-define(OUT(S,A),ok).
%%-define(OUT(S,A),io:format(S,A)).

-define(INITIAL_CLUSTER_SIZE, 3).
-define(MAX_NODES, 6).
-define(RING, 10).
-define(N, 3).

-type status() :: valid | invalid | leaving | exiting.

%% Cluster state record propagated over "gossip"
-record(cstate, {
          ring = [] :: [{integer(), integer()}],
          next = [] :: [{integer(), integer(), integer(), awaiting | complete}],
          members = [] :: [{integer(), {status(), vclock:vclock()}}],
          claimant :: integer(),
          seen :: [{integer(), vclock:vclock()}],
          rvsn :: vclock:vclock(),
          vclock :: vclock:vclock()
         }).

%% Node state
-record(nstate, {
          cstate = #cstate{} :: #cstate{},
          partitions = [] :: [integer()],
          has_data = [] :: [integer()]
         }).

%% Global test state
-record(state, {
          nstates :: dict(),
          members :: [{integer(), {status(), vclock:vclock()}}],
          primary :: [integer()],
          others :: [integer()],
          allowed :: [integer()],
          random_ring :: [integer()],
          active_handoffs :: [{integer(), integer(), integer()}],
          split :: dict()
        }).

eqc_test_() ->
    {spawn,
     [{setup,
       fun setup/0,
       fun cleanup/1,
       [{inorder, [manual_test_list(),
        %% Run the quickcheck tests
        {timeout, 60000, % timeout is in msec
         ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_join()))))}]}
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

test_ring_convergence(State) ->
    Nodes = get_members(State#state.members, [valid, leaving]),
    case Nodes of
        [] ->
            {true, State};
        _ ->
            State2 = State#state{split=dict:new()},
            {Result, State3} = do_converge_ring(State2, {1000, true}),
            ?assert(Result),
            State3
    end.

do_gossip(State, N2, N1) ->
    case precondition(State, {call,join_eqc,random_gossip,[N2,N1]}) of
        true ->
            run(State, {call,join_eqc,random_gossip,[N2,N1]});
        false ->
            State
    end.

do_converge_ring(State, {_, false}) ->
    {true, State};
do_converge_ring(State, {0, _}) ->
    {false, State};
do_converge_ring(State, {RC, true}) ->
    Nodes = get_members(State#state.members, [valid, leaving]),
    {_, Changed1, State2} =
        lists:foldl(
          fun(N2, {N1, Changed0, State0}) ->
                  S = do_gossip(State0, N2, N1),
                  Changed = not equal_cstate(get_cstate(State0, N2),
                                             get_cstate(S, N2)),
                  {N2, Changed0 or Changed, S}
          end,
          {hd(Nodes), false, State},
          tl(Nodes)),
    %% Nodes may shutdown due to gossip
    Nodes2 = get_members(State2#state.members, [valid, leaving]),
    N1 = hd(lists:reverse(Nodes2)),
    {Changed2, State3} =
        lists:foldl(
          fun(N2, {Changed0, State0}) ->
                  S = do_gossip(State0, N2, N1),
                  Changed = not equal_cstate(get_cstate(State0, N2),
                                             get_cstate(S, N2)),
                  {Changed0 or Changed, S}
          end,
          {Changed1, State2},
          Nodes2 -- [N1]),
    do_converge_ring(State3, {RC - 1, Changed2}).

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
    RVsn=vclock:increment(Node, vclock:fresh()),
    VClock=vclock:increment(Node, vclock:fresh()),
    CState = #cstate{members=[{Node, {valid, vclock:fresh()}}],
                     ring=Ring,
                     next=[],
                     claimant=Node,
                     seen=[{Node, VClock}],
                     rvsn=RVsn,
                     vclock=VClock},
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
           members=[],
           allowed=[],
           active_handoffs=[],
           split=dict:new()}.

g_initial_nodes() ->
    Nodes = lists:seq(0, ?MAX_NODES-1),
    ?LET(L, shuffle(Nodes), lists:split(?INITIAL_CLUSTER_SIZE, L)).

g_idx() ->
    choose(0, ?RING-1).

g_gossip(State, Gossip) ->
    {OtherNode, Node, OtherCS} = hd(Gossip),
    [{Node, get_nstate(State, Node)}, OtherNode, OtherCS].

g_random_ring() ->
    shuffle(lists:seq(0, ?RING-1)).

command(#state{primary=[]}) ->
    {call, ?MODULE, initial_cluster, [g_initial_nodes(), g_random_ring()]};
command(#state{primary=Primary, others=Others, active_handoffs=Active}) ->
    oneof([{call, ?MODULE, read, [elements(Primary), g_idx()]},
           {call, ?MODULE, random_gossip, [elements(Primary), elements(Primary)]},
           {call, ?MODULE, maybe_handoff, [elements(Primary), g_idx()]},
           {call, ?MODULE, comm_split, [elements(Primary), elements(Primary)]},
           {call, ?MODULE, comm_join, [elements(Primary), elements(Primary)]},
           {call, ?MODULE, remove, [elements(Primary), elements(Primary)]},
           {call, ?MODULE, leave, [elements(Primary), elements(Primary)]}]
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
            ?debugFmt("Precondition failed~n", []),
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
    lists:member(PNode, State#state.primary) andalso
    lists:member(Node, State#state.others);

precondition2(State, {call, _, leave, [Node, PNode]}) ->
    M = get_members(State#state.members, [valid]),
    (Node /= PNode) andalso
        (erlang:length(State#state.primary) > 1) andalso
        (erlang:length(M) > 1) andalso
        lists:member(Node, State#state.primary) andalso
        lists:member(PNode, State#state.primary) andalso
        dict:is_key(Node, State#state.nstates);

precondition2(State, {call, _, remove, [Node, PNode]}) ->
    M = get_members(State#state.members, [valid]),
    (Node /= PNode) andalso
        (erlang:length(State#state.primary) > 1) andalso
        (erlang:length(M) > 1) andalso
        lists:member(Node, State#state.primary) andalso
        lists:member(PNode, State#state.primary) andalso
        dict:is_key(Node, State#state.nstates);
                    
precondition2(State, {call, _, random_gossip, [Node1, Node2]}) ->
    (Node1 /= Node2) andalso
    lists:member(Node1, State#state.primary) andalso
        lists:member(Node2, State#state.primary) andalso
        can_communicate(State, Node1, Node2);

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

precondition2(State, {call, _, maybe_handoff, [Node, Idx]}) ->
    (State#state.primary /= []) andalso
        dict:is_key(Node, State#state.nstates) andalso
        begin
            NState = get_nstate(State, Node),
            lists:member(Idx, NState#nstate.partitions)
        end;

precondition2(State, {call, _, finish_handoff, [AH={_, Prev, New}]}) ->
    %%io:format("AH: ~p / ~p~n", [AH, State#state.active_handoffs]),
    lists:member(AH, State#state.active_handoffs) andalso
        can_communicate(State, Prev, New);

precondition2(State, {call, _, read, [Node, _Idx]}) ->
    lists:member(Node, State#state.primary);

%% All commands other than initial_cluster run after initial_cluster
precondition2(State,_) ->
    State#state.primary /= [].

%% If the model breaks during test case generation, Quickcheck is unable
%% provide a command sequence that led to that error. Therefore, we ignore
%% all errors during generation and then re-trigger then during dynamic
%% execution (by having the next_state recomputed by the postcondition).
next_state(State, Result, Call) ->
    %%?debugFmt("~p~n", [{set,{var,1},Call}]),
    try next_state2(State, Result, Call) of
        State2 ->
            State2
    catch
        _:_ ->
            State
    end.

next_state_test(State, Result, Call) ->
    %%?debugFmt("~p~n", [{set,{var,1},Call}]),
    try next_state2(State, Result, Call) of
        State2 ->
            {ok, State2}
    catch
        _:_ ->
            fail
    end.

next_state2(State, _, {call, _, read, _}) ->
    State;

next_state2(State, _Result, {call, _, initial_cluster, [{Members, Others}, RRing]}) ->
    s_initial_cluster(State, Members, Others, RRing);

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

next_state2(State, _Result, {call, _, leave, [Node, PNode]}) ->
    s_leave(State, Node, PNode);

next_state2(State, _Result, {call, _, remove, [Node, PNode]}) ->
    s_remove(State, Node, PNode);

next_state2(State, _Result, {call, _, maybe_handoff, [Node, Idx]}) ->
    NState = get_nstate(State, Node),
    s_maybe_handoff(State, Node, NState, Idx);

next_state2(State, _Result, {call, _, finish_handoff, [AH]}) ->
    s_finish_handoff(State, AH).

%% next_state(State, _Result, {call,_,_,_}) ->
%%     State.

postcondition(State, {call, _, read, [Node, Idx]}, _Result) ->
    CState = get_cstate(State, Node),
    valid_read(State, CState, Idx);

postcondition(State, Cmd, _) ->
    case precondition_test(State, Cmd) of
        false ->
            io:format("Precondition failure: ~p~n", [Cmd]),
            false;
        true ->
            case next_state_test(State, ok, Cmd) of
                fail ->
                    false;
                {ok, State2} ->
                    lists:all(fun(X) -> X end,
                              [check_members(State2),
                               check_states(State2),
                               check_sorted_members(State2)])
            end
    end.

%% postcondition(_,_,fail) ->
%%     false;

%% postcondition(_,_,_) ->
%%     true.

check_states(State) ->
    T1 = check_states(State, State#state.primary),
    Members = get_members(State#state.members),
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
              M = NS#nstate.cstate#cstate.members,
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
initial_cluster(_, _) ->
    ok.

read(_, _) ->
    ok.

maybe_handoff(_, _) ->
    ok.

finish_handoff({_Idx, _Prev, _New}) ->
    ok.

join(_, _) ->
    ok.

leave(_, _) ->
    ok.

remove(_, _) ->
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
    Claimant = hd(Members2),
    MemberVC = vclock:increment(Claimant, vclock:fresh()),
    Members3 = [{M, {valid, MemberVC}} || M <- Members2],
    RVsn=vclock:increment(Claimant, vclock:fresh()),
    VClock = vclock:increment(Claimant, vclock:fresh()),
    Seen = [{M, VClock} || M <- Members2],
    CState = #cstate{members=Members3,
                     ring=Ring,
                     next=[],
                     claimant=Claimant,
                     seen=Seen,
                     rvsn=RVsn,
                     vclock=VClock},
    SMembers = [{Node, init_node_state(Node, CState)} || Node <- Members2],
    SOthers = [{Node, init_node_state(Node)} || Node <- Others],
    NStates = dict:from_list(SMembers ++ SOthers),
    State#state{members=Members3,
                primary=Members2,
                others=Others,
                allowed=[],
                random_ring=RandomRing,
                nstates=NStates}.

s_random_gossip(State, Node, NState, OtherNode, OtherCS) ->
    %%?OUT("S1: ~p~nS2: ~p~nGS: ~p~n", [NState, OtherCS, State#state.cstate]),
    State2 = s_gossip(State, {Node, NState}, OtherNode, OtherCS),
    State2.

s_gossip(State, {Node, NState}, _OtherNode, OtherCS) ->
    CState = NState#nstate.cstate,
    {Changed, CState2} = merge_cstate(Node, CState, OtherCS),
    case Changed of
        true ->
            RRing = State#state.random_ring,
            State2 = ring_changed(State, RRing, {Node, NState}, CState2),
            State3 = maybe_shutdown(State2, Node),
            State3;
        false ->
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
    Primary2 = lists:usort([Node | State#state.primary]),
    Others2 = State#state.others -- [Node],
    JoinCS = get_cstate(State, PNode),
    JoinCS2 = add_member(Node, JoinCS, Node),
    State2 = update_nstate(State, Node, NState#nstate{cstate=JoinCS2,
                                                      partitions=[],
                                                      has_data=[]}),
    State2#state{primary=Primary2, others=Others2}.

add_member(PNode, CState, Node) ->
    Members2 = orddict:update(Node,
                              fun({_, VC}) ->
                                      {valid, vclock:increment(PNode, VC)}
                              end,
                              {valid, vclock:increment(PNode,
                                                       vclock:fresh())},
                              CState#cstate.members),
    VClock2 = vclock:increment(PNode, CState#cstate.vclock),
    CState#cstate{members=Members2, vclock=VClock2}.

s_leave(State, Node, PNode) ->
    LeaveCS = get_cstate(State, PNode),
    %%Members = get_members(LeaveCS#cstate.members),
    case orddict:find(Node, LeaveCS#cstate.members) of
        {ok, {valid, _}} ->
            %%Primary2 = State#state.primary -- [Node],
            Primary2 = State#state.primary,
            LeaveCS2 = leave_member(PNode, LeaveCS, Node),
            State2 = update_cstate(State, PNode, LeaveCS2),

            CState = get_cstate(State2, Node),
            CState2 = leave_member(Node, CState, Node),
            State3 = update_cstate(State2, Node, CState2),

            State3#state{primary=Primary2};
        _ ->
            State
    end.

leave_member(PNode, CState, Node) ->
    Members2 = orddict:update(Node,
                              fun({_, VC}) ->
                                      {leaving, vclock:increment(PNode, VC)}
                              end,
                              {leaving, vclock:increment(PNode,
                                                         vclock:fresh())},
                              CState#cstate.members),
    VClock2 = vclock:increment(PNode, CState#cstate.vclock),
    CState#cstate{members=Members2, vclock=VClock2}.

s_remove(State, Node, PNode) ->
    PNodeCS = get_cstate(State, PNode),
    NodeCS = get_cstate(State, Node),
    case orddict:find(Node, PNodeCS#cstate.members) of
        {ok, {invalid, _}} ->
            State;
        {ok, {_, _}} ->
            Primary2 = State#state.primary -- [Node],
            %%Primary2 = State#state.primary,
            PNodeCS2 = remove_member(PNode, PNodeCS, Node),
            State2 = update_cstate(State, PNode, PNodeCS2),

            %% For testing purposes, partitions owned by removed nodes should
            %% be marked as acceptable read failures.
            PendingIdx = [Idx || {Idx, _, NextOwner, _} <- NodeCS#cstate.next,
                                 NextOwner =:= Node],
            Allowed2 = State#state.allowed ++ indices(NodeCS#cstate.ring, Node) ++
                PendingIdx,

            NStates2 = dict:erase(Node, State2#state.nstates),
            
            State2#state{primary=Primary2, allowed=Allowed2, nstates=NStates2};
        _ ->
            State
    end.

remove_member(PNode, CState, Node) ->
    Members2 = orddict:update(Node,
                              fun({_, VC}) ->
                                      {invalid, vclock:increment(PNode, VC)}
                              end,
                              {invalid, vclock:increment(PNode,
                                                         vclock:fresh())},
                              CState#cstate.members),
    VClock2 = vclock:increment(PNode, CState#cstate.vclock),
    CState#cstate{members=Members2, vclock=VClock2}.

set_member(Node, CState, Member, Status) ->
    Members2 = orddict:update(Member,
                              fun({_, VC}) ->
                                      {Status, vclock:increment(Node, VC)}
                              end,
                              {Status, vclock:increment(Node,
                                                        vclock:fresh())},
                              CState#cstate.members),
    %% VClock2 = vclock:increment(Node, CState#cstate.vclock),
    %% CState#cstate{members=Members2, vclock=VClock2}.
    CState#cstate{members=Members2}.

handle_cast(State, _, Node, shutdown) ->
    io:format("Shutting down node ~p~n", [Node]),
    NStates2 = dict:erase(Node, State#state.nstates),
    State#state{nstates=NStates2};

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

s_maybe_handoff(State, Node, NState, Idx) ->
    CState = NState#nstate.cstate,
    Active0 = State#state.active_handoffs,
    {NextOwner, _} = get_next_owner(Idx, CState),
    Owner = owner(CState#cstate.ring, Idx),
    %%io:format("Owner/Next: ~p / ~p~n", [Owner, NextOwner]),
    Ready = ring_ready(CState),
    Active = case {Ready, Owner, NextOwner} of
                 {_, _, Node} ->
                     Active0;
                 {_, Node, undefined} ->
                     Active0;
                 {true, Node, _} ->
                     lists:usort([{Idx, Node, NextOwner} | Active0]);
                 {_, _, undefined} ->
                     lists:usort([{Idx, Node, Owner} | Active0]);
                 {_, _, _} ->
                     Active0
             end,
    State#state{active_handoffs=Active}.

s_finish_handoff(State, AH={Idx, Prev, New}) ->
    PrevCS1 = get_cstate(State, Prev),
    Owner = owner(PrevCS1#cstate.ring, Idx),
    {NextOwner, Status} = get_next_owner(Idx, PrevCS1),

    Active = State#state.active_handoffs -- [AH],
    State2 = State#state{active_handoffs=Active},

    case {Owner, NextOwner, Status} of
        {Prev, New, awaiting} ->
            PrevNS1 = get_nstate(State2, Prev),
            PrevNS2 = update_partition_status(PrevNS1, Idx, true, false),
            Next = PrevCS1#cstate.next,
            Next2 = lists:keyreplace(Idx, 1, Next, {Idx, Owner, NextOwner, complete}),
            VClock2 = vclock:increment(Prev, PrevCS1#cstate.vclock),
            PrevCS2 = PrevCS1#cstate{vclock=VClock2, next=Next2},
            PrevNS3 = PrevNS2#nstate{cstate=PrevCS2},
            State3 = update_nstate(State2, Prev, PrevNS3),

            %% New owner is considered to now have data for test purposes
            NewNS = get_nstate(State3, New),
            NewNS2 = update_partition_status(NewNS, Idx, true, true),
            State4 = update_nstate(State3, New, NewNS2),
            State4;
        {Prev, New, complete} ->
            %% Do nothing
            State2;
        {Prev, _, _} ->
            throw("Why am I handing off?");
        {_, _, _} ->
            PrevCS1 = get_cstate(State2, Prev),
            PrevNS1 = get_nstate(State2, Prev),
            Ready = ring_ready(PrevCS1),
            case Ready of
                true ->
                    %% Delete data, shutdown vnode, and maybe shutdown node
                    io:format("Shutting down vnode ~p/~p~n", [Prev, Idx]),
                    PrevNS2 = update_partition_status(PrevNS1, Idx, false, false),
                    State3 = update_nstate(State2, Prev, PrevNS2),
                    %%State4 = maybe_shutdown(State3, Prev),
                    State3;
                false ->
                    %% Delete data
                    PrevNS2 = update_partition_status(PrevNS1, Idx, true, false),
                    State3 = update_nstate(State2, Prev, PrevNS2),
                    State3
            end
    end.

maybe_shutdown(State, Node) ->
    NState = get_nstate(State, Node),
    CState = get_cstate(State, Node),
    Ready = ring_ready(CState),
    Ring = get_ring(State, Node),
    Next = CState#cstate.next,
    NoIndices = (indices(Ring, Node) =:= []),
    NoPartitions = (NState#nstate.partitions =:= []),
    NoPendingIndices = ([] =:= ([Idx || {Idx, _, NextOwner, _} <- Next,
                                        NextOwner =:= Node])),
    Shutdown = (NoIndices and NoPartitions and NoPendingIndices),
    Status = get_status(CState, Node),
    case {Ready, Shutdown, Status} of
        {Ready, true, leaving} ->
            ?OUT("Exiting node ~p~n", [Node]),
            CState2 = set_member(Node, CState, Node, exiting),
            CState3 = increment_cstate_vclock(Node, CState2),
            update_cstate(State, Node, CState3);
        _ ->
            State
    end.
            
merge_cstate(VNode, CS01, CS02) ->
    CS03 = update_seen(VNode, CS01),
    CS04 = update_seen(VNode, CS02),
    Seen = reconcile_seen(CS03, CS04),
    CS1 = CS03#cstate{seen=Seen},
    CS2 = CS04#cstate{seen=Seen},
    SeenChanged = not equal_seen(CS01, CS1),

    VC1 = CS1#cstate.vclock,
    VC2 = CS2#cstate.vclock,
    %%io:format("V1: ~p~nV2: ~p~n", [VC1, VC2]),
    Newer = vclock:descends(VC1, VC2),
    Older = vclock:descends(VC2, VC1),
    Equal = equal_cstate(CS1, CS2),
    case {Equal, Newer, Older} of
        {_, true, false} ->
            %%io:format("CS1: ~p~n", [CS1]),
            {SeenChanged, CS1};
        {_, false, true} ->
            %%io:format("CS2: ~p~n", [CS2]),
            {true, CS2};
        {true, _, _} ->
            {SeenChanged, CS1};
        {_, true, true} ->
            io:format("C1: ~p~nC2: ~p~n", [CS1, CS2]),
            throw("Equal vclocks, but cstate unequal");
        {_, false, false} ->
            CS3 = reconcile_cstate(VNode, CS1, CS2, VC1, VC2),
            {true, CS3}
    end.

reconcile_cstate(VNode, CS1, CS2, VC1, VC2) ->
    VClock2 = case VNode of
                  global ->
                      vclock:merge([VC1, VC2]);
                  _ ->
                      vclock:increment(VNode, vclock:merge([VC1, VC2]))
              end,
    %%VClock2 = vclock:merge([VC1, VC2]),

    Members = reconcile_members(CS1, CS2),
    CS3 = reconcile_ring(CS1, CS2, get_members(Members)),
    CS3#cstate{vclock=VClock2, members=Members}.

reconcile_members(CS1, CS2) ->
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
      CS1#cstate.members,
      CS2#cstate.members).

reconcile_seen(CS1, CS2) ->
    orddict:merge(fun(_, VC1, VC2) ->
                          vclock:merge([VC1, VC2])
                  end, CS1#cstate.seen, CS2#cstate.seen).

reconcile_next(Next1, Next2) ->
    lists:zipwith(fun({Idx, Owner, Node, Status1}, {Idx, Owner, Node, Status2}) ->
                          case {Status1, Status2} of
                              {complete, _} ->
                                  {Idx, Owner, Node, complete};
                              {_, complete} ->
                                  {Idx, Owner, Node, complete};
                              {awaiting, awaiting} ->
                                  {Idx, Owner, Node, awaiting}
                          end
                  %%end, CS1#cstate.next, CS2#cstate.next).
                  end, Next1, Next2).

reconcile_next2(Next1, Next2) ->
    lists:zipwith(fun({Idx, Owner1, Node1, Status1}, {Idx, Owner2, Node2, Status2}) ->
                          Same = ({Owner1, Node1} =:= {Owner2, Node2}),
                          case {Same, Status1, Status2} of
                              {false, _, _} ->
                                  {Idx, Owner1, Node1, Status1};
                              {_, complete, _} ->
                                  {Idx, Owner1, Node1, complete};
                              {_, _, complete} ->
                                  {Idx, Owner1, Node1, complete};
                              {_, awaiting, awaiting} ->
                                  {Idx, Owner1, Node1, awaiting}
                          end
                  %%end, CS1#cstate.next, CS2#cstate.next).
                  end, Next1, Next2).

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

reconcile_ring(CS1=#cstate{claimant=Claimant1, rvsn=VC1},
               CS2=#cstate{claimant=Claimant2, rvsn=VC2}, Members) ->
    V1Newer = vclock:descends(VC1, VC2),
    V2Newer = vclock:descends(VC2, VC1),
    EqualVC = vclock:equal(VC1, VC2),
    Next1 = CS1#cstate.next,
    Next2 = CS2#cstate.next,
    %%io:format("Next1: ~p~nNext2: ~p~n", [Next1, Next2]),
    CS3 = case {EqualVC, V1Newer, V2Newer} of
              {true, _, _} ->
                  ?assertEqual(Claimant1, Claimant2),
                  ?assertEqual(CS1#cstate.ring, CS2#cstate.ring),
                  Next = reconcile_next(Next1, Next2),
                  CS1#cstate{next=Next};
              {_, true, false} ->
                  MNext = substitute(1, Next1, Next2),
                  Next = reconcile_next2(Next1, MNext),
                  CS1#cstate{next=Next};
              {_, false, true} ->
                  MNext = substitute(1, Next2, Next1),
                  Next = reconcile_next2(Next2, MNext),
                  CS2#cstate{next=Next};
              {_, _, _} ->
                  CValid1 = lists:member(Claimant1, Members),
                  CValid2 = lists:member(Claimant2, Members),
                  case {CValid1, CValid2} of
                      {true, false} ->
                          MNext = substitute(1, Next1, Next2),
                          Next = reconcile_next2(Next1, MNext),
                          CS1#cstate{next=Next};
                      {false, true} ->
                          MNext = substitute(1, Next2, Next1),
                          Next = reconcile_next2(Next2, MNext),
                          CS2#cstate{next=Next};
                      {false, false} ->
                          throw("Neither claimant valid");
                      {true, true} ->
                          throw("Both claimants valid")
                  end
          end,
    CS3.

merge_status(valid, _) ->
    valid;
merge_status(_, valid) ->
    valid;
merge_status(invalid, _) ->
    invalid;
merge_status(_, invalid) ->
    invalid;
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
    CState = NState#nstate.cstate,
    State2 = update_members(State, CState),
    NS = dict:store(Node, NState, State2#state.nstates),
    State2#state{nstates=NS}.

get_cstate(State, Node) ->
    (get_nstate(State, Node))#nstate.cstate.

get_ring(State, Node) ->
    (get_cstate(State, Node))#cstate.ring.

get_status(CState, Node) ->
    {Status, _} = orddict:fetch(Node, CState#cstate.members),
    Status.

get_members(Members) ->
    get_members(Members, [valid, leaving, exiting]).

get_members(Members, Types) ->
    [Node || {Node, {V, _}} <- Members, lists:member(V, Types)].

update_seen(global, CState) ->
    CState;
update_seen(Node, CState) ->
    Seen2 = orddict:update(Node,
                           fun(SeenVC) ->
                                   vclock:merge([SeenVC, CState#cstate.vclock])
                           end,
                           CState#cstate.vclock,
                           CState#cstate.seen),
    CState#cstate{seen=Seen2}.

update_seen(State, Node, CState) ->
    CState2 = update_seen(Node, CState),
    State2 = save_cstate(State, Node, CState2),
    Changed = (CState2#cstate.seen /= CState#cstate.seen),
    {Changed, State2, CState2}.

update_cstate(NState=#nstate{}, Node, CState) ->
    CState2 = update_seen(Node, CState),
    %%CState2 = increment_cstate_vclock(Node, CState1),
    %%io:format("Update ~p :: ~p~n", [Node, CState2]),
    NState#nstate{cstate=CState2};
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
                             NState#nstate{cstate=CState}
                     end,
                     #nstate{cstate=CState},
                     State#state.nstates),
    State#state{nstates=NS}.

pending_ownership_changes(CState) ->
    lists:any(fun({N1, N2}) -> N1 /= N2 end, CState#cstate.next).

ring_ready(CState) ->
    Seen = CState#cstate.seen,
    Members = get_members(CState#cstate.members),
    VClock = CState#cstate.vclock,
    ?OUT("M: ~p~nS: ~p~n", [Members, Seen]),
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

ring_changed(State, _RRing, {Node, _NState}, CState) ->
    case ring_ready(CState) of
        false ->
            update_cstate(State, Node, CState);
        true ->
            {C1, State2, CState2} = n_maybe_update_claimant(State, Node, CState),
            {C2, State3, CState3} = n_maybe_update_ring(State2, Node, CState2),
            {C3, State4, CState4} = n_maybe_remove_exiting(State3, Node, CState3),

            case (C1 or C2 or C3) of
                true ->
                    CState5 = increment_cstate_vclock(Node, CState4),
                    update_cstate(State4, Node, CState5);
                false ->
                    update_cstate(State4, Node, CState4)
            end
    end.

n_maybe_update_claimant(State, Node, CState) ->
    %%io:format("RM: ~p~n", [CState#cstate.members]),
    Members = get_members(CState#cstate.members, [valid]),
    Claimant = CState#cstate.claimant,
    RVsn = CState#cstate.rvsn,
    NextClaimant = hd(Members ++ [undefined]),
    ClaimantMissing = not lists:member(Claimant, Members),

    case {ClaimantMissing, NextClaimant} of
        {true, Node} ->
            %% Become claimant
            ?assert(Node /= Claimant),
            RVsn2 = vclock:increment(Claimant, RVsn),
            CState2 = CState#cstate{claimant=Node, rvsn=RVsn2},
            {true, State, CState2};
        _ ->
            {false, State, CState}
    end.

n_maybe_update_ring(State, Node, CState) ->
    Claimant = CState#cstate.claimant,
    case Claimant of
        Node ->
            RRing = State#state.random_ring,
            {Changed, CState2} = update_ring(RRing, Node, CState),
            {Changed, State, CState2};
        _ ->
            {false, State, CState}
    end.

n_maybe_remove_exiting(State, Node, CState) ->
    Claimant = CState#cstate.claimant,
    case Claimant of
        Node ->
            Exiting = get_members(CState#cstate.members, [exiting]),
            %%io:format("Claimant ~p removing exiting ~p~n", [Node, Exiting]),
            Changed = (Exiting /= []),
            {State2, CState2} = remove_exiting(State, Node, CState),
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

remove_exiting(State0, Node, CState0) ->
    Exiting = get_members(CState0#cstate.members, [exiting]),
    {State2, CState2} =
        lists:foldl(fun(ENode, {State, CState}) ->
                            {_, State2} = cast(State, Node, ENode, shutdown),
                            CState2 = set_member(Node, CState, ENode, invalid),
                            {State2, CState2}
                    end, {State0, CState0}, Exiting),
    Primary2 = State2#state.primary -- Exiting,
    State3 = State2#state{primary=Primary2},
    {State3, CState2}.

update_ring(RRing, CNode, CState) ->
    Active = get_members(CState#cstate.members, [valid]),
    update_ring(RRing, Active, CNode, CState).

update_ring(_RRing, [], _CNode, CState) ->
    {false, CState};
update_ring(RRing, Active, CNode, CState) ->
    Next0 = CState#cstate.next,
    Ring = CState#cstate.ring,

    %% Remove tuples from next for removed nodes
    InvalidMembers = get_members(CState#cstate.members, [invalid]),
    Next = [Elem || Elem={_, _, NextOwner, Status} <- Next0,
                    (Status =:= complete) or
                        not lists:member(NextOwner, InvalidMembers)],

    NextRing = [{Idx, Node} || {Idx, _, Node, _} <- Next],
    {PendIndices0, _PendOwners0} = lists:unzip(NextRing),
    ERing = lists:ukeymerge(1, NextRing, Ring),
    Active = get_members(CState#cstate.members, [valid]),

    %% Ressign leaving/inactive indices
    MustIndices = lists:foldl(fun({Idx, Node}, L) ->
                                      case lists:member(Node, Active) of
                                          true ->
                                              L;
                                          false ->
                                              [Idx|L]
                                      end
                              end, [], ERing),

    ?OUT("Active: ~p~n", [Active]),
    Counts1 = get_counts(Active, ERing),
    {CountNodes1, _} = lists:unzip(lists:keysort(2, Counts1)),
    CountNodes2 = 
        lists:flatten(lists:duplicate(?RING div erlang:length(CountNodes1),
                                      CountNodes1)),
    Claim1 = claim(CountNodes2, MustIndices -- PendIndices0, RRing),
    Claim2 = [{Idx, orddict:fetch(Idx, Ring), Node, awaiting} || {Idx, Node} <- Claim1],

    %%io:format("Claim1: ~p~n", [Claim1]),
    %%io:format("Claim2: ~p~n", [Claim2]),
    Invalid = get_members(CState#cstate.members, [invalid]),
    {RingChange, LeavingClaim}
        = lists:foldl(fun({Idx, Owner, Node, Status}, {L1, L2}) ->
                              case lists:member(Owner, Invalid) of
                                  true ->
                                      {[{Idx, Node} | L1], L2};
                                  false ->
                                      {L1, [{Idx, Owner, Node, Status} | L2]}
                              end
                      end, {[], []}, Claim2),
    NewRing1 = lists:ukeymerge(1, RingChange, Ring),
    Next2 = Next ++ LeavingClaim,

    %% Rebalance as necessary
    NextRing2 = [{Idx, Node} || {Idx, _, Node, _} <- Next2],
    ERing2 = lists:ukeymerge(1, NextRing2, NewRing1),
    Counts2 = get_counts(Active, ERing2),
    Avg = ?RING div erlang:length(Active),
    X = lists:flatmap(fun({Node,Count}) ->
                              Copies = erlang:max(0, Avg - Count + 1),
                              [{I,Node} || I <- lists:seq(1,Copies)]
                      end, Counts2),
    {_, C} = lists:unzip(lists:sort(X)),

    Indices = [Idx || {Idx, Node} <- NewRing1, not lists:member(Node, C)],
    {PendIndices, _PendOwners} = lists:unzip(NextRing2),
    ValidIndices = Indices -- PendIndices,
    Claim3 = claim(C, ValidIndices, RRing),
    Claim4 = [{Idx, orddict:fetch(Idx, Ring), Node, awaiting} || {Idx, Node} <- Claim3],
    %%io:format("Claim3: ~p~n", [Claim3]),
    %%io:format("Claim4: ~p~n", [Claim4]),
    Next3 = lists:keysort(1, Next2 ++ Claim4),

    ClaimChanged = ((Claim1 /= []) or (Claim3 /= [])),

    Completed = [{Idx, Node} || {Idx, _, Node, S} <- Next3, S =:= complete],
    NewRing2 = lists:ukeymerge(1, Completed, NewRing1),
    Next4 = lists:filter(fun({Idx, _, NewOwner, S}) ->
                                 (S /= complete) or
                                 (orddict:fetch(Idx, NewRing1) /= NewOwner)
                         end, Next3),

    RingChanged = (Ring /= NewRing2),
    Changed = ClaimChanged or RingChanged,
    case Changed of
        true ->
            RVsn2 = vclock:increment(CNode, CState#cstate.rvsn),
            {true, CState#cstate{ring=NewRing2, next=Next4, rvsn=RVsn2}};
        false ->
            {false, CState}
    end.

claim(Nodes, Indices, RRing) ->
    Shuffle = lists:filter(fun(E) -> lists:member(E, Indices) end, RRing),
    ClaimSize = erlang:min(erlang:length(Nodes),
                           erlang:length(Shuffle)),

    Claim = lists:zip(lists:sublist(Shuffle, 1, ClaimSize),
                      lists:sublist(Nodes, 1, ClaimSize)),

    Claim.

%% Update global members set used for test case generation.
update_members(State=#state{members=Members}, CS2) ->
    CS1 = #cstate{members=Members},
    Members2 = reconcile_members(CS1, CS2),
    State#state{members=Members2}.

increment_cstate_vclock(Node, CState) ->
    VClock = vclock:increment(Node, CState#cstate.vclock),
    CState#cstate{vclock=VClock}.

%% Returns true if the nodes can communicate (ie. not split)
can_communicate(_State, N1, N2) when N1 =:= N2 ->
    true;
can_communicate(State, N1, N2) ->
    dict:is_key(N2, State#state.nstates) and
    (not dict:is_key({N1, N2}, State#state.split)).

get_next_owner(Idx, CState) ->
    case lists:keyfind(Idx, 1, CState#cstate.next) of
        false ->
            {undefined, undefined};
        {_, _Owner, NextOwner, Status} ->
            {NextOwner, Status}
    end.

%% VClock timestamps may be different for test generation versus
%% shrinking/checking phases. Normalize to test for equality.
equal_cstate(CS1, CS2) ->
    %% M2 = [{N, {V, undefinedCS1#cstate.members
    T1 = equal_members(CS1#cstate.members, CS2#cstate.members),
    T2 = true, %%equal_vclock(CS1#cstate.vclock, CS2#cstate.vclock),
    T3 = equal_vclock(CS1#cstate.rvsn, CS2#cstate.rvsn),
    T4 = equal_seen(CS1, CS2),
    CS3=CS1#cstate{vclock=undefined, members=undefined, rvsn=undefined, seen=[]},
    CS4=CS2#cstate{vclock=undefined, members=undefined, rvsn=undefined, seen=[]},
    T5 = (CS3 =:= CS4),
    T1 and T2 and T3 and T4 and T5.

equal_members(M1, M2) ->
    L = orddict:merge(fun(_, {V1, VC1}, {V2, VC2}) ->
                              V1 =:= V2 andalso
                                  equal_vclock(VC1, VC2)
                      end, M1, M2),
    {_, R} = lists:unzip(L),
    lists:all(fun(X) -> X =:= true end, R).

filtered_seen(CS) ->
    Members = get_members(CS#cstate.members),
    filtered_seen(Members, CS).

filtered_seen([], CS) ->
    CS#cstate.seen;
filtered_seen(Members, CS) ->
    orddict:filter(fun(N, _) -> lists:member(N, Members) end, CS#cstate.seen).

    
equal_seen(CS1, CS2) ->
    Seen1 = filtered_seen(CS1),
    Seen2 = filtered_seen(CS2),
    L = orddict:merge(fun(_, VC1, VC2) ->
                              equal_vclock(VC1, VC2)
                      end, Seen1, Seen2),
    {_, R} = lists:unzip(L),
    lists:all(fun(X) -> X =:= true end, R).
    
equal_vclock(VC1, VC2) ->
    VC3 = [{Node, {Count, 1}} || {Node, {Count, _TS}} <- VC1],
    VC4 = [{Node, {Count, 1}} || {Node, {Count, _TS}} <- VC2],
    vclock:equal(VC3, VC4).

valid_read(State, CState, Idx) ->
    Owner = proplists:get_value(Idx, CState#cstate.ring),
    check_read(State, Owner, Idx).

check_read(State, Owner, Idx) ->
    MayFail = lists:member(Idx, State#state.allowed),
    case MayFail of
        true ->
            true;
        false ->
            check_read2(State, Owner, Idx)
    end.

check_read2(State, Owner, Idx) ->
    NState = get_nstate(State, Owner),
    io:format("Checking if ~p has data for ~p~n", [Owner, Idx]),
    Found = lists:member(Idx, NState#nstate.has_data),
    case Found of
        true ->
            true;
        false ->
            io:format("Checking forward~n", []),
            %% Check if we should forward
            OwnerCS = get_cstate(State, Owner),
            case lists:keyfind(Idx, 1, OwnerCS#cstate.next) of
                {_, _, Node, complete} ->
                    ?assert(Node /= Owner),
                    check_read(State, Node, Idx);
                _ ->
                    io:format("Invalid read of ~p / ~p~n", [Idx, Owner]),
                    false
            end
    end.

invoke({call, M, F, A}) ->
    apply(M, F, A).

run(S, Cmd) ->
    io:format("R: ~p~n", [Cmd]),
    ?assertEqual(true, precondition(S, Cmd)),
    R = invoke(Cmd),
    S2 = next_state2(S, R, Cmd),
    %% io:format("M: ~p~n", [S2#state.cstate#cstate.members]),
    %% io:format("C: ~p~n", [S2#state.cstate#cstate.claimant]),
    %% io:format("S: ~p~n", [S2#state.cstate]),
    ?assertEqual(true, postcondition(S, Cmd, R)),
    S2.

run_cmd(S, {_,_,Cmd}) ->
    run(S,Cmd).

run_cmds(Cmds) ->
    State = lists:foldl(fun({_,_,Cmd}, S) -> run(S, Cmd) end, initial_state(), Cmds),
    test_ring_convergence(State),
    State.


manual_test_list() ->
    [fun test_join/0,
     fun test_claimant1/0,
     fun test_claimant2/0,
     fun test_read_forward/0,
     fun test_read_after_remove/0,
     fun test_ownership_change/0,
     fun test_node_shutdown/0,
     fun test_handoff_while_leaving/0
    ].

test_join() ->
    S = run_cmds([{set,{var,1},
                   {call,join_eqc,initial_cluster,[{[1,0,2],[3,4,5]},[0,1,2,3,4,5,6,7]]}},
                  {set,{var,6},{call,join_eqc,join,[4,1]}}]),
    CS4 = get_cstate(S, 4),
    M = get_members(CS4#cstate.members),
    ?assertEqual([0,1,2,4], M).

%% Claimant transition due to remove
test_claimant1() ->
    S = run_cmds([{set,{var,1},
                   {call,join_eqc,initial_cluster,
                    [{[0,1,2],[3,4,5]},[0,1,2,3,4,5,6,7,8,9]]}},
                  {set,{var,2},{call,join_eqc,remove,[1,0]}},
                  {set,{var,8},{call,join_eqc,join,[3,0]}},
                  {set,{var,10},{call,join_eqc,remove,[0,3]}},
                  {set,{var,37},{call,join_eqc,random_gossip,[2,3]}}]),
    CS2 = get_cstate(S, 2),
    ?assertEqual(2, CS2#cstate.claimant).
   

%% Claimant transition due to leave
test_claimant2() ->
    S = run_cmds([{set,{var,1},
                   {call,join_eqc,initial_cluster,
                    [{[4,0,1],[2,3,5]},[0,1,2,3,4,5,6,7,8,9]]}},
                  {set,{var,2},{call,join_eqc,join,[5,0]}},
                  {set,{var,8},{call,join_eqc,leave,[0,5]}},
                  {set,{var,53},{call,join_eqc,leave,[1,0]}},
                  {set,{var,69},{call,join_eqc,random_gossip,[5,0]}},
                  {set,{var,114},{call,join_eqc,random_gossip,[1,5]}},
                  {set,{var,176},{call,join_eqc,random_gossip,[5,1]}},
                  {set,{var,187},{call,join_eqc,random_gossip,[0,5]}},
                  {set,{var,236},{call,join_eqc,random_gossip,[4,0]}}]),
    CS4 = get_cstate(S, 4),
    ?assertEqual(4, CS4#cstate.claimant).

%% Test that after handoff, the vnode forwards to the new owner.
test_read_forward() ->
    _S = run_cmds([{set,{var,1},
                    {call,join_eqc,initial_cluster,
                     [{[0,3,1],[2,4,5]},[0,1,2,3,4,5,6,7,8,9]]}},
                   {set,{var,11},{call,join_eqc,leave,[3,0]}},
                   {set,{var,18},{call,join_eqc,join,[2,1]}},
                   {set,{var,22},{call,join_eqc,remove,[1,2]}},
                   {set,{var,31},{call,join_eqc,remove,[0,2]}},
                   {set,{var,41},{call,join_eqc,random_gossip,[3,2]}},
                   {set,{var,53},{call,join_eqc,random_gossip,[2,3]}},
                   {set,{var,55},{call,join_eqc,random_gossip,[3,2]}},
                   {set,{var,62},{call,join_eqc,maybe_handoff,[3,8]}},
                   {set,{var,63},{call,join_eqc,finish_handoff,[{8,3,2}]}},
                   {set,{var,64},{call,join_eqc,read,[3,8]}}]),
    ok.

%% Test after remove (ie. should fail but we allow it)
test_read_after_remove() ->
    S = run_cmds([{set,{var,1},
                   {call,join_eqc,initial_cluster,
                    [{[1,4,5],[0,2,3]},[0,1,2,3,4,5,6,7,8,9]]}},
                  {set,{var,3},{call,join_eqc,remove,[1,5]}},
                  {set,{var,4},{call,join_eqc,random_gossip,[4,5]}},
                  {set,{var,6},{call,join_eqc,join,[0,4]}},
                  {set,{var,31},{call,join_eqc,read,[0,9]}}]),
    ?assert(lists:member(9, S#state.allowed)),
    ok.

%% Complete test/ownership change
test_ownership_change() ->
    S = run_cmds([{set,{var,1},
                   {call,join_eqc,initial_cluster,
                    [{[4,1,5],[0,2,3]},[0,1,7,2,3,4,5,6,8,9]]}},
                  {set,{var,3},{call,join_eqc,join,[0,1]}},
                  {set,{var,8},{call,join_eqc,join,[2,0]}},
                  {set,{var,16},{call,join_eqc,leave,[1,0]}},
                  {set,{var,35},{call,join_eqc,leave,[0,2]}},
                  {set,{var,58},{call,join_eqc,random_gossip,[0,1]}},
                  {set,{var,69},{call,join_eqc,random_gossip,[0,2]}},
                  {set,{var,168},{call,join_eqc,random_gossip,[1,0]}},
                  {set,{var,171},{call,join_eqc,random_gossip,[4,1]}},
                  {set,{var,175},{call,join_eqc,random_gossip,[5,4]}},
                  {set,{var,176},{call,join_eqc,random_gossip,[2,5]}},
                  {set,{var,197},{call,join_eqc,random_gossip,[1,2]}},
                  {set,{var,204},{call,join_eqc,random_gossip,[5,1]}},
                  {set,{var,214},{call,join_eqc,random_gossip,[4,5]}},
                  {set,{var,241},{call,join_eqc,maybe_handoff,[4,7]}},
                  {set,{var,244},{call,join_eqc,finish_handoff,[{7,4,2}]}},
                  {set,{var,339},{call,join_eqc,random_gossip,[1,4]}},
                  {set,{var,343},{call,join_eqc,random_gossip,[5,1]}},
                  {set,{var,344},{call,join_eqc,random_gossip,[4,1]}},
                  {set,{var,352},{call,join_eqc,random_gossip,[2,4]}}]),
    CS21 = get_cstate(S, 2),
    ?assertEqual(4, orddict:fetch(7, CS21#cstate.ring)),

    S1 = run_cmd(S, {set,{var,356},{call,join_eqc,random_gossip,[2,5]}}),
    CS22 = get_cstate(S1, 2),
    ?assertEqual(2, orddict:fetch(7, CS22#cstate.ring)),
    ok.

test_node_shutdown() ->
    _S = run_cmds([{set,{var,1},
                    {call,join_eqc,initial_cluster,
                     [{[3,4,5],[0,1,2]},[0,1,2,3,4,5,6,7,8,9]]}},
                   {set,{var,5},{call,join_eqc,leave,[3,4]}},
                   {set,{var,12},{call,join_eqc,leave,[5,3]}},
                   {set,{var,13},{call,join_eqc,random_gossip,[3,5]}},
                   {set,{var,40},{call,join_eqc,random_gossip,[3,4]}},
                   {set,{var,43},{call,join_eqc,random_gossip,[5,3]}},
                   {set,{var,45},{call,join_eqc,random_gossip,[4,5]}},
                   {set,{var,58},{call,join_eqc,random_gossip,[3,4]}},
                   {set,{var,66},{call,join_eqc,random_gossip,[5,3]}},
                   {set,{var,70},{call,join_eqc,maybe_handoff,[5,2]}},
                   {set,{var,71},{call,join_eqc,finish_handoff,[{2,5,4}]}},
                   {set,{var,75},{call,join_eqc,random_gossip,[5,3]}},
                   {set,{var,82},{call,join_eqc,random_gossip,[4,5]}},
                   {set,{var,83},{call,join_eqc,random_gossip,[3,4]}},
                   {set,{var,196},{call,join_eqc,random_gossip,[5,3]}},
                   {set,{var,197},{call,join_eqc,maybe_handoff,[5,8]}},
                   {set,{var,201},{call,join_eqc,random_gossip,[4,3]}},
                   {set,{var,202},{call,join_eqc,random_gossip,[3,4]}},
                   {set,{var,207},{call,join_eqc,finish_handoff,[{8,5,4}]}},
                   {set,{var,237},{call,join_eqc,random_gossip,[3,5]}},
                   {set,{var,239},{call,join_eqc,random_gossip,[5,3]}},
                   {set,{var,240},{call,join_eqc,random_gossip,[4,5]}},
                   {set,{var,250},{call,join_eqc,random_gossip,[3,4]}},
                   {set,{var,256},{call,join_eqc,random_gossip,[5,3]}},
                   {set,{var,260},{call,join_eqc,maybe_handoff,[5,2]}},
                   {set,{var,263},{call,join_eqc,finish_handoff,[{2,5,4}]}},
                   {set,{var,334},{call,join_eqc,maybe_handoff,[5,5]}},
                   {set,{var,336},{call,join_eqc,finish_handoff,[{5,5,4}]}},
                   {set,{var,341},{call,join_eqc,random_gossip,[4,5]}},
                   {set,{var,347},{call,join_eqc,random_gossip,[5,4]}},
                   {set,{var,359},{call,join_eqc,random_gossip,[3,5]}},
                   {set,{var,364},{call,join_eqc,random_gossip,[4,3]}},
                   {set,{var,467},{call,join_eqc,maybe_handoff,[3,9]}},
                   {set,{var,469},{call,join_eqc,finish_handoff,[{9,3,4}]}},
                   {set,{var,473},{call,join_eqc,random_gossip,[4,3]}},
                   {set,{var,478},{call,join_eqc,random_gossip,[5,4]}},
                   {set,{var,484},{call,join_eqc,random_gossip,[3,5]}},
                   {set,{var,486},{call,join_eqc,maybe_handoff,[5,8]}},
                   {set,{var,535},{call,join_eqc,random_gossip,[4,3]}},
                   {set,{var,540},{call,join_eqc,random_gossip,[3,4]}},
                   {set,{var,543},{call,join_eqc,random_gossip,[5,3]}},
                   {set,{var,544},{call,join_eqc,maybe_handoff,[5,5]}},
                   {set,{var,558},{call,join_eqc,finish_handoff,[{5,5,4}]}},
                   {set,{var,611},{call,join_eqc,finish_handoff,[{8,5,4}]}},
                   {set,{var,616},{call,join_eqc,random_gossip,[5,3]}},
                   {set,{var,625},{call,join_eqc,random_gossip,[3,5]}},
                   {set,{var,639},{call,join_eqc,random_gossip,[4,3]}}]),
    ok.

test_handoff_while_leaving() ->
    run_cmds([{set,{var,1},
               {call,join_eqc,initial_cluster,
                [{[2,0,3],[1,4,5]},[5,7,2,0,4,1,3,6,8,9]]}},
              {set,{var,3},{call,join_eqc,join,[1,0]}},
              {set,{var,6},{call,join_eqc,join,[5,0]}},
              {set,{var,7},{call,join_eqc,join,[4,0]}},
              {set,{var,13},{call,join_eqc,remove,[2,3]}},
              {set,{var,18},{call,join_eqc,leave,[3,4]}},
              {set,{var,41},{call,join_eqc,random_gossip,[0,3]}},
              {set,{var,48},{call,join_eqc,remove,[3,0]}},
              {set,{var,49},{call,join_eqc,random_gossip,[0,1]}},
              {set,{var,51},{call,join_eqc,leave,[0,5]}},
              {set,{var,53},{call,join_eqc,random_gossip,[1,0]}},
              {set,{var,65},{call,join_eqc,leave,[1,0]}},
              {set,{var,69},{call,join_eqc,random_gossip,[1,0]}},
              {set,{var,73},{call,join_eqc,random_gossip,[0,1]}},
              {set,{var,110},{call,join_eqc,maybe_handoff,[0,6]}},
              {set,{var,111},{call,join_eqc,random_gossip,[0,5]}},
              {set,{var,129},{call,join_eqc,finish_handoff,[{6,0,1}]}},
              {set,{var,130},{call,join_eqc,random_gossip,[1,0]}},
              {set,{var,131},{call,join_eqc,random_gossip,[0,1]}},
              {set,{var,163},{call,join_eqc,random_gossip,[5,0]}},
              {set,{var,171},{call,join_eqc,read,[0,6]}}]),
    ok.
