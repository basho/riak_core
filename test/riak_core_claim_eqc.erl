%%% @author Thomas Arts (thomas.arts@quviq.com)
%%% @doc QuickCheck model to replace riak_core_claim_statem
%%%      by testing that part as well as testing location awareness
%%%      (rack_awareness_test.erl)
%%%
%%%      In reality each node has its own Ring structure. In this test we only build the ring structure
%%%      for 1 claimant.
%%%
%%%      We use the API as defined in riak_core_membership_claim.
%%%
%%%      RUN WITH ./rebar3 as test eqc
%%%
%%%
%%% @end
%%% Created : 21 Feb 2023 by Thomas Arts

-module(riak_core_claim_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-compile([export_all, nowarn_export_all]).



%% -- State ------------------------------------------------------------------
-record(state,
        {
         ring_size,
         placements = []          :: [{Name :: atom(), Location :: atom()}], %% AWS Partition placement groups
         nodes = []               :: [Name :: atom()],                       %% all nodes that should be part of next plan
         ring = undefined,
         claimant = undefined     :: atom(),
         nval = 4,
         committed_nodes = [],
         staged_nodes = []        :: [Name :: atom()],                       %% nodes added/left before claim,
         plan = [],                                                          %% staged nodes after claim
         leaving_nodes = [],
         sufficient = false,
         with_location = false
        }).

%% -- State and state functions ----------------------------------------------
initial_state() ->
  initial_state(#{}).

initial_state(Map) ->
  #state{nval = maps:get(nval, Map, 4),
         ring_size = maps:get(ring_size, Map, 32),
         sufficient = maps:get(sufficient, Map, false),
         with_location = maps:get(with_location, Map, true)}.

%% -- Generators -------------------------------------------------------------

%% -- Common pre-/post-conditions --------------------------------------------
command_precondition_common(S, placements) ->
  S#state.placements == [];
command_precondition_common(S, _Cmd) ->
  S#state.placements /= [].

%% -- Operations -------------------------------------------------------------

wrap_call(S, {call, Mod, Cmd, Args}) ->
    try {ok, apply(Mod, Cmd, [S#state.ring | Args])}
    catch
      throw:{eqc, Reason, Trace} ->
        {error, {'EXIT', Reason, Trace}, []};
      _:Reason:Trace -> {error, {'EXIT', Reason, Trace}, []}
    end.


%% --- Operation: placements ---
%% Create nodes at specific locations
%% We assume AWS partition placement with P placements and N nodes per partition
%% Number of vnodes is determined later when ring_size is chosen to run on
%% this hardware
%%
%% we may want to add a location 'undefined' if we want to test that specific feature
placements_args(S) ->
  [ locnodes(S#state.nval) ].

placements(_, _Primary) ->
  ok.

placements_next(S, _, [Primary]) ->
  S#state{placements = Primary,
          nodes = []}.

placements_features(S, [Primary], _Res) ->
  [{with_location, S#state.with_location},
   {nr_nodes, length(Primary)}].


%% --- Operation: add_node ---
%% Make sure there is a non-started node to add.
add_claimant_pre(S) ->
  S#state.claimant == undefined.

add_claimant_args(S) ->
   [hd(S#state.placements), S#state.with_location, S#state.ring_size].

add_claimant_pre(S, [LocNode, _, RingSize]) ->
  LocNodes = S#state.placements,
  length(LocNodes) =< RingSize andalso
    lists:member(LocNode, LocNodes).

add_claimant(_, {Loc, Node}, WithLocation, RingSize) ->
  NewRing =
    pp(riak_core_ring, fresh, [RingSize, Node]),
  case WithLocation of
    true ->
      pp(riak_core_ring, set_node_location, [Node, Loc, NewRing]);
    false ->
      NewRing
  end.

add_claimant_next(S, Ring, [{_, Node}, _, RingSize]) ->
    S#state{ring = Ring, nodes = [Node], claimant = Node, ring_size = RingSize}.

add_claimant_features(_S, [_, _WithLocation, RingSize], _Res) ->
  [{ring_size, RingSize}].



%% --- Operation: add_node ---
%% Make sure there is a non-started node to add.
add_node_pre(S) ->
  S#state.claimant /= undefined
    andalso S#state.plan == []
    andalso length(S#state.nodes) < length(S#state.placements).

add_node_args(S) ->
  ?LET(NewNode, elements([ {Loc, Node} || {Loc, Node} <- S#state.placements,
                                          not lists:member(Node, S#state.nodes) ]),
           [NewNode, S#state.with_location, S#state.claimant]).

add_node_pre(S, [{Loc, Node}, _, Claimant]) ->
  not lists:member(Node, S#state.nodes) andalso S#state.claimant == Claimant andalso
    lists:member({Loc, Node}, S#state.placements).

add_node(Ring, {Loc, Node}, WithLocation, Claimant) ->
  NewRing =
    pp(riak_core_ring, add_member, [Claimant, Ring, Node]),
  case WithLocation of
    true ->
      pp(riak_core_ring, set_node_location, [Node, Loc, NewRing]);
    false ->
      NewRing
  end.

add_node_next(S=#state{nodes=Nodes}, Ring, [{_, Node}, _, _]) ->
    S#state{ring = Ring, nodes = [Node | Nodes],
            staged_nodes = stage(S#state.staged_nodes, Node, add)}.

add_node_post(_S, [{_Loc, Node}, _, _Claimant], NextRing) ->
    lists:member(Node, riak_core_ring:members(NextRing, [joining])).



%% --- Operation: claim ---

%% @doc claim_pre/3 - Precondition for generation
claim_pre(S) ->
  S#state.ring /= undefined andalso length(S#state.nodes) >= S#state.nval
    andalso necessary_conditions(S)
    andalso S#state.plan == [] andalso S#state.staged_nodes /= [].  %% make sure there is something sensible to do

claim_args(S) ->
  %% v2 does not take leaving nodes into account, but the model does
  [elements([v4]), S#state.nval].

claim(Ring, Algo, Nval) ->
  InitialRemoveRing =
    case {riak_core_ring:members(Ring, [leaving]), Algo} of
      {[], _} ->
        Ring;
      {LeavingNodes, v4} ->
        lists:foldl(
          fun(RN, R) ->
            riak_core_membership_leave:remove_from_cluster(
              R, RN, rand:seed(exrop, os:timestamp()), true, choose_claim_v4)
          end,
          Ring,
          LeavingNodes
        )
    end,
  case Algo of
    v4 ->
      pp(riak_core_membership_claim,
          claim,
          [InitialRemoveRing,
            {riak_core_membership_claim, wants_claim_v2},
            {riak_core_claim_swapping, choose_claim_v4, [{target_n_val, Nval}]}])
  end.

claim_pre(#state{sufficient = true} = S, [v4, _Nval]) ->
    %% Sufficient conditions to actually succeed
    sufficient_conditions(S);
claim_pre(_, [_, _]) ->
    true.


claim_next(S, NewRing, [_, _]) ->
  S#state{ring = NewRing, plan = S#state.staged_nodes, staged_nodes = []}.

claim_post(#state{nval = Nval, ring_size = RingSize, nodes = Nodes} = S, [_, _], NewRing) ->
  Preflists = riak_core_ring:all_preflists(NewRing, Nval),
  LocNval = if Nval > 3 -> Nval - 1;
               true -> Nval end,
  ImperfectPLs =
    lists:foldl(fun(PL,Acc) ->
                    PLNodes = lists:usort([N || {_,N} <- PL]),
                    case length(PLNodes) of
                      Nval ->
                        Acc;
                      _ ->
                        [PL | Acc]
                    end
                end, [], Preflists),
  ImperfectLocations =
    lists:foldl(fun(PL,Acc) ->
                    PLLocs = lists:usort([location(S, N)  || {_, N} <- PL]) -- [undefined],
                    case length(PLLocs) of
                      LocNval ->
                        Acc;
                      Nval ->
                        Acc;
                      _ when S#state.with_location ->
                        [{PLLocs, PL} | Acc];
                      _ ->
                        Acc
                    end
                end, [], Preflists),
  RiakRingSize = riak_core_ring:num_partitions(NewRing),
  RiakNodeCount = length(riak_core_ring:members(NewRing, [joining, valid])),

  BalancedRing =
        riak_core_membership_claim:balanced_ring(RiakRingSize,
                                      RiakNodeCount, NewRing),
  %% S#state.committed_nodes == [] orelse
  eqc_statem:conj(
    [eqc_statem:tag(ring_size, eq(RiakRingSize, RingSize)),
     eqc_statem:tag(node_count, eq(RiakNodeCount, length(Nodes))),
     eqc_statem:tag(meets_target_n, eq(riak_core_membership_claim:meets_target_n(NewRing, Nval), {true, []})),
     eqc_statem:tag(correct_nodes, eq(chash:members(riak_core_ring:chash(NewRing)), lists:sort(Nodes))),
     eqc_statem:tag(perfect_pls, eq(ImperfectPLs, [])),
     eqc_statem:tag(perfect_locations, eq(ImperfectLocations, [])),
     %% eqc_statem:tag(few_moves, length(S#state.committed_nodes) =< 1 orelse length(diff_nodes(S#state.ring, NewRing)) < S#state.ring_size div 2),
     eqc_statem:tag(balanced_ring, BalancedRing)]).

claim_features(#state{nodes = Nodes} = S, [Alg, _], Res) ->
  [{claimed_nodes, length(Nodes)},
   {algorithm, Alg}] ++
    %% and if we add to an already claimed ring
  [{moving, {S#state.ring_size, S#state.nval,
             {joining, length(S#state.nodes -- S#state.committed_nodes)},
             {leaving, length(S#state.committed_nodes -- S#state.nodes)},
             {moves, length(diff_nodes(S#state.ring, Res))}}}
   || length(S#state.committed_nodes) > 1].


diff_nodes(Ring1, Ring2) ->
  %% get the owners per vnode hash
  {Rest, Differences} =
    lists:foldl(fun({Hash2, Node2}, {[{Hash1, Node1} | HNs], Diffs}) when Hash1 == Hash2 ->
                    case Node1 == Node2 of
                      true -> {HNs, Diffs};
                      false -> {HNs, [{vnode_moved, Hash1, Node1, Node2}|Diffs]}
                    end;
                   ({Hash2, _}, {[{Hash1, Node1} | HNs], Diffs}) when Hash1 < Hash2 ->
                       {HNs, [{vnode_split, Hash1, Node1}|Diffs]};
                   ({Hash2, Node2}, {[{Hash1, Node1} | HNs], Diffs}) when Hash1 > Hash2 ->
                       {[{Hash1, Node1} | HNs], [{vnode_diff, Hash2, Node2}|Diffs]}
                end, {riak_core_ring:all_owners(Ring1), []}, riak_core_ring:all_owners(Ring2)),
  [{only_left, E} || E <- Rest] ++ Differences.


%% necessary for a solution to exist:
necessary_conditions(S) when not S#state.with_location ->
  Remainder =  S#state.ring_size rem length(S#state.nodes),
  Remainder == 0 orelse Remainder >= S#state.nval;
necessary_conditions(S) ->
  Locations = to_config(S),
  Rounds = S#state.ring_size div S#state.nval,
  NumNodes = length(S#state.nodes),
  MinOccs = S#state.ring_size div NumNodes,
  MaxOccs =
    if  S#state.ring_size rem NumNodes == 0 -> MinOccs;
        true -> 1 + MinOccs
    end,

  MinOccForLocWith =
    fun(N) -> max(N * MinOccs, S#state.ring_size - MaxOccs * lists:sum(Locations -- [N])) end,
  MaxOccForLocWith =
    fun(N) -> min(N * MaxOccs, S#state.ring_size - MinOccs * lists:sum(Locations -- [N])) end,

  lists:all(fun(B) -> B end,
            [length(Locations) >= S#state.ring_size || Rounds < 2] ++
              [length(Locations) >= S#state.nval] ++
              [ MinOccForLocWith(Loc) =< Rounds || Loc <- Locations ] ++
              [ Rounds =< MaxOccForLocWith(LSize)
                || S#state.nval == length(Locations), LSize <- Locations] ++
              [ S#state.ring_size rem S#state.nval == 0
                || S#state.nval == length(Locations) ]
           ).

sufficient_conditions(S) ->
  Locations = to_config(S),
  known_solution(S#state.ring_size, Locations, S#state.nval) orelse
  (length(Locations) >= S#state.nval + 2
   andalso length(S#state.nodes) < S#state.ring_size div 2
   andalso lists:min(Locations) >= lists:max(Locations) - 1).

to_config(S) ->
    LocNodes =
        lists:foldl(fun(N, Acc) ->
                            Loc = location(S, N),
                            Acc#{Loc =>  maps:get(Loc, Acc, 0) + 1}
                    end, #{}, S#state.nodes),
    maps:values(LocNodes).





%% --- Operation: leave_node ---
leave_node_pre(S)  ->
    length(S#state.nodes) > 1 %% try > 1 not to delete the initial node
      andalso S#state.committed_nodes/= []
      andalso S#state.plan == [].   

leave_node_args(S) ->
  %% TODO consider re-leaving leaved nodes
  [elements(S#state.nodes),
   S#state.with_location,
   S#state.claimant].

leave_node_pre(#state{nodes=_Nodes}, [Node, _, Claimant]) ->
  Claimant /= Node. %% andalso lists:member(Node, Nodes).

leave_node(Ring, NodeName, _WithLocation, Claimant) ->
  pp(riak_core_ring, leave_member, [Claimant, Ring, NodeName]).

leave_node_next(S=#state{nodes = Nodes}, NewRing, [Node, _, _]) ->
  S#state{ring = NewRing, nodes = lists:delete(Node, Nodes),
          staged_nodes = stage(S#state.staged_nodes, Node, leave)}.

leave_node_post(_S, [NodeName, _, _], NextRing) ->
  lists:member(NodeName, riak_core_ring:members(NextRing, [leaving])).

%% --- Operation: commit ---

%% In the code this is an involved process with gossiping and transfering data.
%% Here we just assume that all works out fine and make joining nodes valid nodes
%% in the result of the planned new ring.
%% In other words, we assume that the plan is established and only update
%% the joining nodes to valid.

commit_pre(S) ->
  S#state.plan /= [].

commit_args(S) ->
  [S#state.claimant].

commit(Ring, Claimant) ->
  JoiningNodes = riak_core_ring:members(Ring, [joining]),   %% [ Node || {Node, joining} <- riak_core_ring:all_member_status(Ring) ],
  Ring0 =
    lists:foldl(
      fun(Node, R) ->
        riak_core_ring:set_member(Claimant, R, Node, valid, same_vclock)
      end,
      Ring,
      JoiningNodes),
  LeavingNodes = riak_core_ring:members(Ring, [leaving]),
  lists:foldl(
    fun(Node, R) ->
      riak_core_ring:remove_member(Claimant, R, Node)
    end,
    Ring0,
    LeavingNodes).

commit_next(S, NewRing, [_]) ->
  S#state{ring = NewRing, staged_nodes = [], plan = [], committed_nodes = S#state.nodes}.

commit_post(#state{nodes = Nodes}, [_], Ring) ->
  eq(Nodes -- riak_core_ring:members(Ring, [valid]), []).





weight(S, add_node) when not S#state.with_location ->
  1 + 4*(length(S#state.placements) - length(S#state.nodes));
weight(S, add_located_nodes) when S#state.with_location->
  0;
weight(S, leave_node) ->
  1 + (length(S#state.committed_nodes) div 4);
weight(_S, _Cmd) -> 1.



%% --- ... more operations

%% -- Property ---------------------------------------------------------------

prop_claim() ->
  prop_claim([relaxed]).

prop_claim(Options) ->
  Relaxed = proplists:get_bool(relaxed, Options),
  %% If relaxed, we restrict configurations to those that we can easily
  %% determine have a solution (sufficient_condition).
  case ets:whereis(timing) of
    undefined -> ets:new(timing, [public, named_table, bag]);
    _ -> ok
  end,
  ?FORALL({Nval, RingSize, WithLocation}, {choose(2, 5), ringsize(), true},
  ?FORALL(Cmds, commands(?MODULE, initial_state(#{nval => Nval,
                                                  ring_size => RingSize,
                                                  sufficient => Relaxed,
                                                  with_location => WithLocation})),
  begin
    put(ring_nr, 0),
    % application:set_env(riak_core, full_rebalance_onleave, true),
    % application:set_env(riak_core, choose_claim_fun, choose_claim_v4),
    {H, S, Res} = run_commands(Cmds),
    Config = lists:sort(to_config(S)),
    measure(length, commands_length(Cmds),
    features([{RingSize, Config, Nval} ||  WithLocation  andalso
                                             S#state.plan /= [] andalso Res == ok],
    aggregate_feats([claimed_nodes, ring_size, with_location, nr_nodes, moving, algorithm],
                    call_features(H),
            check_command_names(Cmds,
               pretty_commands(?MODULE, Cmds, {H, S, Res},
                               Res == ok)))))
  end)).

aggregate_feats([], _, Prop) -> Prop;
aggregate_feats([Op | Ops], Features, Prop) ->
    aggregate(with_title(Op),
              [F || {Id, F} <- Features, Id == Op],
              aggregate_feats(Ops, Features, Prop)).


location(S, N) when is_record(S, state) ->
  location(S#state.placements, N);
location(LocNodes, N) ->
  case lists:keyfind(N, 2, LocNodes) of
    {Loc, _} -> Loc;
    _ -> exit({not_found, N, LocNodes})
  end.

bugs() -> bugs(10).

bugs(N) -> bugs(N, []).

bugs(Time, Bugs) ->
  more_bugs(eqc:testing_time(Time, prop_claim()), 20, Bugs).

locnodes(Nval) ->
  ?LET(MaxLoc, choose(Nval, Nval * 2), configs(MaxLoc, Nval)).

ringsize() ->
  ?LET(Exp, choose(5, 8),
       power2(Exp)).

configs(MaxLocations, Nval) when MaxLocations < Nval ->
  {error, too_few_locations};
configs(MaxLocations, Nval) ->
  ?LET(Max, choose(1, 8),
       ?LET(Less, vector(MaxLocations - Nval, choose(1, Max)),
            to_locnodes([Max || _ <- lists:seq(1, Nval)] ++ Less))).

to_locnodes(NodeList) ->
  NodesSet = [ list_to_atom(lists:concat([n, Nr])) || Nr <- lists:seq(1, lists:sum(NodeList))],
  LocationsSet = [ list_to_atom(lists:concat([loc, Nr])) || Nr <- lists:seq(1, length(NodeList))],
  to_locnodes(lists:zip(LocationsSet, NodeList), NodesSet).

to_locnodes([], []) ->
  [];
to_locnodes([{Loc, Nr}| LocNrs], NodesSet) ->
  Nodes = lists:sublist(NodesSet, Nr),
  [ {Loc, Node} || Node <- Nodes ] ++
    to_locnodes(LocNrs, NodesSet -- Nodes).

stage(Staged, Node, Kind) ->
  lists:keydelete(Node, 1, Staged) ++ [{Node, Kind}].


power2(0) ->
  1;
power2(1) ->
  2;
power2(N) when N rem 2 == 0 ->
  X = power2(N div 2),
  X*X;
power2(N) ->
  2*power2(N-1).


pp(M, F, As) ->
  _Call = lists:flatten([io_lib:format("~p:~p(", [M, F])] ++
                        string:join([as_ring(arg, A) || A <-As], ",") ++
                        [")."]),
  try {Time, R} = timer:tc(M, F, As),
       %% eqc:format("~s = ~s\n", [as_ring(res, R), _Call ]),
       if Time > 150000 -> ets:insert(timing, [{F, As, Time}]);
          true -> ok
       end,
       R
  catch _:Reason:ST ->
      eqc:format("~s \n", [ _Call ]),
      throw({eqc, Reason, ST})
  end.

as_ring(Kind, Term) when is_tuple(Term) ->
  case element(1, Term) of
    chstate_v2 ->
      case Kind of
        arg ->
          OldRing = get(ring_nr),
          lists:concat(["Ring_",OldRing]);
        res ->
          OldRing = put(ring_nr, get(ring_nr) + 1),
          lists:concat(["Ring_",OldRing + 1])
      end;
    _ -> lists:flatten(io_lib:format("~p", [Term]))
  end;
as_ring(_, Term) ->
  lists:flatten(io_lib:format("~p", [Term])).

equal({X, Ls1}, {X, Ls2}) ->
  equal(Ls1, Ls2);
equal([X|Ls1], [X|Ls2]) ->
  equal(Ls1, Ls2);
equal(X, Y) ->
  equals(X, Y).

%% create a config and add or leave nodes for next config
prop_translate() ->
  ?FORALL(LocNodes1, ?LET(X, configs(4, 2), shuffle(X)),
    ?FORALL([LocNodesA, LocNodesL], vector(2, ?LET(C, configs(4,2), sublist(C))),
      ?FORALL(LocNodes2, shuffle((LocNodes1 -- LocNodesL) ++ LocNodesA),
          begin
            Leaving = [ N || {L, N} <- LocNodes1, not lists:member({L, N}, LocNodes2)],
            {_R, OldLocRel} = riak_core_claim_swapping:to_binring(LocNodes1, Leaving),
            StayTheSame = [ {Idx, {L, N}} || {Idx, {L, N}} <- OldLocRel, not lists:member(N, Leaving) ],
            {_Config, NewLocRel} = riak_core_claim_swapping:to_config2(LocNodes2, OldLocRel),
            equals([ Same
                     || {Idx1, {L,N}} = Same <- StayTheSame,
                        {Idx2, _} <- [lists:keyfind({L,N}, 2, NewLocRel)],
                        Idx1 == Idx2], StayTheSame)
          end))).

known_solution(Size, Config, NVal) ->
  lists:member({Size, Config, #{node => NVal, location => NVal}}, solution_list()).

solution_list() ->
  [{64,[1,1,2,3],#{location => 2,node => 2}},
   {64,[1,1,3,2,2,3,3],#{location => 2,node => 2}},
   {64,[1,1,3,3,2,3,1],#{location => 4,node => 4}},
   {64,[1,1,4,5,5],#{location => 2,node => 2}},
   {64,[1,2,1,1,1,2,3],#{location => 2,node => 2}},
   {64,[1,2,1,2,3,1,2],#{location => 3,node => 3}},
   {64,[1,2,1,3,2,3],#{location => 4,node => 4}},
   {64,[1,2,2,4,4,2,2],#{location => 3,node => 3}},
   {64,[1,2,3,3],#{location => 3,node => 3}},
   {64,[1,2,4,4,3,6,5],#{location => 3,node => 3}},
   {64,[1,3,1,1,1,1,3],#{location => 3,node => 3}},
   {64,[1,3,2],#{location => 2,node => 2}},
   {64,[1,3,2,2,1,3,2],#{location => 3,node => 3}},
   {64,[1,3,2,2,5,6],#{location => 3,node => 3}},
   {64,[1,3,3,1,1,3,1],#{location => 2,node => 2}},
   {64,[1,3,4,2,3,2,1],#{location => 2,node => 2}},
   {64,[1,4,2,4],#{location => 2,node => 2}},
   {64,[1,4,3],#{location => 2,node => 2}},
   {64,[1,4,3,1,4,1],#{location => 2,node => 2}},
   {64,[1,4,4,1,4,3],#{location => 2,node => 2}},
   {64,[1,5,3,5],#{location => 2,node => 2}},
   {64,[1,5,4,3,6],#{location => 2,node => 2}},
   {64,[2,1,3],#{location => 2,node => 2}},
   {64,[2,1,3,5,5,2,3],#{location => 2,node => 2}},
   {64,[2,2,4,3,6],#{location => 2,node => 2}},
   {64,[2,3,1,1,1,2,2],#{location => 2,node => 2}},
   {64,[2,4,1,4,1],#{location => 3,node => 3}},
   {64,[2,4,1,5,2,5],#{location => 3,node => 3}},
   {64,[2,4,2,4,6],#{location => 2,node => 2}},
   {64,[2,4,3,4,3],#{location => 3,node => 3}},
   {64,[2,4,3,5],#{location => 2,node => 2}},
   {64,[2,5,2,2,5,4],#{location => 2,node => 2}},
   {64,[2,5,2,3,4,5],#{location => 2,node => 2}},
   {64,[2,5,3,2,1],#{location => 2,node => 2}},
   {64,[2,5,3,2,3,3],#{location => 3,node => 3}},
   {64,[2,5,4,5,1,4],#{location => 3,node => 3}},
   {64,[3,1,2,1,3,2],#{location => 3,node => 3}},
   {64,[3,1,3,3,1,1,3],#{location => 2,node => 2}},
   {64,[3,1,3,3,3,3,2],#{location => 3,node => 3}},
   {64,[3,2,1,2,1,1],#{location => 2,node => 2}},
   {64,[3,2,1,3,3,3],#{location => 2,node => 2}},
   {64,[3,2,2,5,3],#{location => 2,node => 2}},
   {64,[3,2,3,4,4,4,2],#{location => 4,node => 4}},
   {64,[3,3,1,2,1,2],#{location => 3,node => 3}},
   {64,[3,3,2,2,2,1,3],#{location => 3,node => 3}},
   {64,[3,3,3,1,3,1,1],#{location => 3,node => 3}},
   {64,[3,3,4,2,3,2],#{location => 2,node => 2}},
   {64,[3,3,5,4,3],#{location => 3,node => 3}},
   {64,[3,4,1],#{location => 2,node => 2}},
   {64,[3,4,1,3,2,1,2],#{location => 4,node => 4}},
   {64,[3,4,2,3,3],#{location => 2,node => 2}},
   {64,[3,4,4,1,4],#{location => 4,node => 4}},
   {64,[3,4,4,2,1,2,2],#{location => 2,node => 2}},
   {64,[3,4,4,4,5,4],#{location => 2,node => 2}},
   {64,[4,1,1,2,3,4,3],#{location => 4,node => 4}},
   {64,[4,1,4,3],#{location => 2,node => 2}},
   {64,[4,2,3,4,2,2],#{location => 3,node => 3}},
   {64,[4,2,5],#{location => 2,node => 2}},
   {64,[4,3,1,3,4,2,1],#{location => 3,node => 3}},
   {64,[4,3,1,4,2,4],#{location => 2,node => 2}},
   {64,[4,3,3,2,1],#{location => 2,node => 2}},
   {64,[4,3,3,5,2,3,5],#{location => 3,node => 3}},
   {64,[4,4,2,1],#{location => 2,node => 2}},
   {64,[4,4,2,4,2],#{location => 2,node => 2}},
   {64,[4,4,3,1,2],#{location => 2,node => 2}},
   {64,[4,5,3],#{location => 2,node => 2}},
   {64,[5,1,6],#{location => 2,node => 2}},
   {64,[5,3,3,3,4,4],#{location => 2,node => 2}},
   {64,[5,3,3,5,1,3,4],#{location => 3,node => 3}},
   {64,[5,4,5,2,3,6],#{location => 3,node => 3}},
   {64,[5,5,2],#{location => 2,node => 2}},
   {64,[5,5,2,1,5,5],#{location => 2,node => 2}},
   {64,[5,5,6,5,2,1],#{location => 2,node => 2}},
   {64,[6,2,4,3],#{location => 2,node => 2}},
   {64,[6,6,3,4,2,5,4],#{location => 4,node => 4}},
   {128,[1,1,1,1,2,2,3],#{location => 3,node => 3}},
   {128,[1,1,1,3,1,1,2],#{location => 2,node => 2}},
   {128,[1,1,2,3,1],#{location => 2,node => 2}},
   {128,[1,1,3,1,3],#{location => 2,node => 2}},
   {128,[1,1,3,2,3,3],#{location => 2,node => 2}},
   {128,[1,2,2,1,4,2],#{location => 2,node => 2}},
   {128,[1,2,2,2,3,2],#{location => 3,node => 3}},
   {128,[1,2,2,5,4,1],#{location => 2,node => 2}},
   {128,[1,2,3],#{location => 2,node => 2}},
   {128,[1,2,3,1,1,3,5],#{location => 2,node => 2}},
   {128,[1,2,3,3,3],#{location => 2,node => 2}},
   {128,[1,2,4,1],#{location => 2,node => 2}},
   {128,[1,2,4,4],#{location => 2,node => 2}},
   {128,[1,3,1,1],#{location => 2,node => 2}},
   {128,[1,3,3,1,2,1],#{location => 2,node => 2}},
   {128,[1,3,3,1,2,3,3],#{location => 3,node => 3}},
   {128,[1,4,1,3,2,2],#{location => 2,node => 2}},
   {128,[1,4,2,2,3,1,2],#{location => 2,node => 2}},
   {128,[1,4,4,2,2,6],#{location => 3,node => 3}},
   {128,[1,4,6,1,4],#{location => 2,node => 2}},
   {128,[1,5,1,4,2,6],#{location => 2,node => 2}},
   {128,[1,5,4,1,5],#{location => 3,node => 3}},
   {128,[2,1,1,3,1,2,2],#{location => 4,node => 4}},
   {128,[2,1,3,4],#{location => 2,node => 2}},
   {128,[2,1,4,2,3],#{location => 2,node => 2}},
   {128,[2,1,5,1,3,3],#{location => 2,node => 2}},
   {128,[2,1,5,3],#{location => 2,node => 2}},
   {128,[2,2,3,1],#{location => 2,node => 2}},
   {128,[2,2,3,1,2],#{location => 2,node => 2}},
   {128,[2,2,3,2,2,5,6],#{location => 3,node => 3}},
   {128,[2,2,3,4,3,3,2],#{location => 3,node => 3}},
   {128,[2,2,3,4,5,4],#{location => 3,node => 3}},
   {128,[2,2,5,1,3,2,4],#{location => 2,node => 2}},
   {128,[2,3,1,3,1,3,3],#{location => 3,node => 3}},
   {128,[2,3,1,3,2,3,1],#{location => 2,node => 2}},
   {128,[2,3,2,2,3,1],#{location => 4,node => 4}},
   {128,[2,3,2,4,2,3,4],#{location => 2,node => 2}},
   {128,[2,3,3,1,1],#{location => 2,node => 2}},
   {128,[2,3,3,1,2,3,3],#{location => 2,node => 2}},
   {128,[2,3,3,2,4,2],#{location => 2,node => 2}},
   {128,[2,3,3,4,2,1],#{location => 3,node => 3}},
   {128,[2,3,3,4,3],#{location => 3,node => 3}},
   {128,[2,3,4,2,1],#{location => 2,node => 2}},
   {128,[2,3,4,2,3],#{location => 3,node => 3}},
   {128,[2,3,5,1,3,3],#{location => 3,node => 3}},
   {128,[2,4,1,1],#{location => 2,node => 2}},
   {128,[2,4,3,1,3],#{location => 3,node => 3}},
   {128,[2,4,3,1,5,3],#{location => 2,node => 2}},
   {128,[2,4,4,4],#{location => 2,node => 2}},
   {128,[2,4,4,4,1,2,4],#{location => 3,node => 3}},
   {128,[2,4,4,4,5,2],#{location => 4,node => 4}},
   {128,[2,5,1,4,5,4,3],#{location => 4,node => 4}},
   {128,[2,5,2,4,4,4],#{location => 3,node => 3}},
   {128,[3,1,3,2,4,2,1],#{location => 4,node => 4}},
   {128,[3,1,3,3,3],#{location => 3,node => 3}},
   {128,[3,1,4,1,2,2,3],#{location => 2,node => 2}},
   {128,[3,2,1,3],#{location => 2,node => 2}},
   {128,[3,2,2,1,2,2,1],#{location => 2,node => 2}},
   {128,[3,2,3,1,1],#{location => 3,node => 3}},
   {128,[3,2,4,4,5,5],#{location => 2,node => 2}},
   {128,[3,3,1,1,2,1],#{location => 2,node => 2}},
   {128,[3,3,1,3],#{location => 2,node => 2}},
   {128,[3,3,2,2,5,5,2],#{location => 2,node => 2}},
   {128,[3,3,3,6,2,4,2],#{location => 4,node => 4}},
   {128,[3,3,4,2],#{location => 2,node => 2}},
   {128,[3,3,4,2],#{location => 3,node => 3}},
   {128,[3,3,5,1],#{location => 2,node => 2}},
   {128,[3,4,1,4,4],#{location => 4,node => 4}},
   {128,[3,4,3,4,2,1,4],#{location => 2,node => 2}},
   {128,[3,4,4,2,2,2],#{location => 3,node => 3}},
   {128,[3,5,3,2,2,1,2],#{location => 2,node => 2}},
   {128,[3,5,4,5,1,3],#{location => 2,node => 2}},
   {128,[4,1,4,2,1],#{location => 3,node => 3}},
   {128,[4,1,5,3],#{location => 2,node => 2}},
   {128,[4,2,1,3,4,4,3],#{location => 2,node => 2}},
   {128,[4,2,2,1,2,4],#{location => 2,node => 2}},
   {128,[4,2,2,3],#{location => 2,node => 2}},
   {128,[4,2,3,2,1,2,4],#{location => 3,node => 3}},
   {128,[4,2,4,4],#{location => 2,node => 2}},
   {128,[4,3,2,3,1],#{location => 2,node => 2}},
   {128,[4,4,1,1,4,2,4],#{location => 2,node => 2}},
   {128,[4,4,2,5],#{location => 2,node => 2}},
   {128,[4,4,4,3,5,2,3],#{location => 3,node => 3}},
   {128,[4,5,1,4,5,2,3],#{location => 2,node => 2}},
   {128,[4,5,2,3,2,4,2],#{location => 4,node => 4}},
   {128,[4,5,3,5],#{location => 3,node => 3}},
   {128,[4,6,6,2],#{location => 2,node => 2}},
   {128,[5,1,5,5,2,3],#{location => 4,node => 4}},
   {128,[5,2,1,2,2,1,4],#{location => 2,node => 2}},
   {128,[5,2,1,3,4],#{location => 2,node => 2}},
   {128,[5,2,4,3,5,3,5],#{location => 4,node => 4}},
   {128,[5,2,6,6,1,5,5],#{location => 2,node => 2}},
   {128,[5,3,4,4,2,5,1],#{location => 3,node => 3}},
   {128,[5,5,1,3],#{location => 2,node => 2}},
   {128,[5,5,1,4],#{location => 2,node => 2}},
   {128,[5,5,4,6,2,5],#{location => 2,node => 2}},
   {128,[5,6,1],#{location => 2,node => 2}},
   {128,[5,6,4,1,6,3,2],#{location => 2,node => 2}},
   {128,[5,6,4,6,6],#{location => 3,node => 3}},
   {128,[6,1,1,5,3,6],#{location => 2,node => 2}},
   {128,[6,1,2,4,5,2],#{location => 2,node => 2}},
   {128,[6,2,1,1,5,5,3],#{location => 3,node => 3}},
   {128,[6,2,5,2,6,6,2],#{location => 4,node => 4}},
   {256,[1,1,1,2,3,2,3],#{location => 3,node => 3}},
   {256,[1,1,3,2,3,3],#{location => 3,node => 3}},
   {256,[1,1,4,4,2,2],#{location => 3,node => 3}},
   {256,[1,2,3],#{location => 2,node => 2}},
   {256,[1,2,3,2,3,1,1],#{location => 2,node => 2}},
   {256,[1,3,2,3,4,3],#{location => 2,node => 2}},
   {256,[1,3,3,3,4,4],#{location => 2,node => 2}},
   {256,[1,3,5,3,1,3],#{location => 3,node => 3}},
   {256,[1,4,2,1],#{location => 2,node => 2}},
   {256,[1,4,3,3,4],#{location => 3,node => 3}},
   {256,[1,4,4,3,2,2,3],#{location => 3,node => 3}},
   {256,[1,4,4,4,3],#{location => 4,node => 4}},
   {256,[2,1,1,2,3,1,1],#{location => 2,node => 2}},
   {256,[2,1,3,1],#{location => 2,node => 2}},
   {256,[2,1,4,1,3,6,4],#{location => 2,node => 2}},
   {256,[2,1,5,4,3,5],#{location => 2,node => 2}},
   {256,[2,2,5,4,3],#{location => 3,node => 3}},
   {256,[2,3,1,3],#{location => 3,node => 3}},
   {256,[2,3,5,2,6,2],#{location => 3,node => 3}},
   {256,[2,3,5,4,5],#{location => 2,node => 2}},
   {256,[2,4,1,5,1],#{location => 2,node => 2}},
   {256,[2,4,2,4,4,2],#{location => 4,node => 4}},
   {256,[2,6,3,4,5,1],#{location => 2,node => 2}},
   {256,[2,6,5,5,5,1,2],#{location => 2,node => 2}},
   {256,[3,1,2,1,3,1,1],#{location => 2,node => 2}},
   {256,[3,1,2,1,5,3],#{location => 2,node => 2}},
   {256,[3,1,2,2,5,3,6],#{location => 2,node => 2}},
   {256,[3,1,2,4,1,4],#{location => 2,node => 2}},
   {256,[3,1,3,1],#{location => 2,node => 2}},
   {256,[3,1,3,2,2,2,2],#{location => 2,node => 2}},
   {256,[3,1,4,4],#{location => 3,node => 3}},
   {256,[3,2,1,3],#{location => 2,node => 2}},
   {256,[3,2,3,3,1,3,3],#{location => 3,node => 3}},
   {256,[3,2,4,3,4,1],#{location => 2,node => 2}},
   {256,[3,2,4,4,5,4],#{location => 3,node => 3}},
   {256,[3,2,5],#{location => 2,node => 2}},
   {256,[3,3,1,3,3],#{location => 2,node => 2}},
   {256,[3,3,4,2],#{location => 2,node => 2}},
   {256,[3,3,4,4,1],#{location => 2,node => 2}},
   {256,[3,3,5,4,3,3,4],#{location => 2,node => 2}},
   {256,[3,3,6,3,3,5],#{location => 3,node => 3}},
   {256,[3,4,2,2,4,3],#{location => 4,node => 4}},
   {256,[3,5,1,3,3,5],#{location => 3,node => 3}},
   {256,[3,5,1,6,5],#{location => 2,node => 2}},
   {256,[3,5,3,4],#{location => 3,node => 3}},
   {256,[3,5,4],#{location => 2,node => 2}},
   {256,[4,1,1,3,4,5],#{location => 3,node => 3}},
   {256,[4,1,2,2],#{location => 2,node => 2}},
   {256,[4,1,2,3,1,2,3],#{location => 2,node => 2}},
   {256,[4,1,2,3,4,1],#{location => 2,node => 2}},
   {256,[4,1,3],#{location => 2,node => 2}},
   {256,[4,1,3,4,1,5],#{location => 3,node => 3}},
   {256,[4,1,4,1],#{location => 2,node => 2}},
   {256,[4,1,4,1,2,2],#{location => 2,node => 2}},
   {256,[4,1,5,2,3,2,2],#{location => 2,node => 2}},
   {256,[4,2,1,2,3,3,6],#{location => 3,node => 3}},
   {256,[4,2,1,5,2,3,2],#{location => 3,node => 3}},
   {256,[4,2,3,2],#{location => 2,node => 2}},
   {256,[4,2,3,2,2,3],#{location => 2,node => 2}},
   {256,[4,2,3,2,4,2,3],#{location => 3,node => 3}},
   {256,[4,2,4,2,2,2],#{location => 4,node => 4}},
   {256,[4,2,5,3,6,6,1],#{location => 4,node => 4}},
   {256,[4,3,2,3,2,2],#{location => 4,node => 4}},
   {256,[4,3,2,5,1,5,5],#{location => 3,node => 3}},
   {256,[4,3,3,1,1,3],#{location => 2,node => 2}},
   {256,[4,3,4,3,4,1,1],#{location => 3,node => 3}},
   {256,[4,3,4,4,3,5,6],#{location => 4,node => 4}},
   {256,[4,4,1,2,1,3,3],#{location => 4,node => 4}},
   {256,[4,4,1,3],#{location => 3,node => 3}},
   {256,[4,4,2,3],#{location => 3,node => 3}},
   {256,[4,5,4,1],#{location => 2,node => 2}},
   {256,[4,5,4,6,3,3,2],#{location => 2,node => 2}},
   {256,[5,1,5],#{location => 2,node => 2}},
   {256,[5,2,1,3],#{location => 2,node => 2}},
   {256,[5,2,5,2,6],#{location => 2,node => 2}},
   {256,[5,2,5,3,3,3,3],#{location => 2,node => 2}},
   {256,[5,2,6,4],#{location => 2,node => 2}},
   {256,[5,3,3,2,1,5,6],#{location => 2,node => 2}},
   {256,[5,4,3,4],#{location => 3,node => 3}},
   {256,[5,4,4,5,5,2],#{location => 3,node => 3}},
   {256,[5,5,4,1],#{location => 3,node => 3}},
   {256,[5,6,1,4,6,4,1],#{location => 4,node => 4}},
   {256,[6,1,1,3,3],#{location => 2,node => 2}},
   {256,[6,2,5,3,1,6,1],#{location => 3,node => 3}},
   {256,[6,3,1,3],#{location => 2,node => 2}},
   {256,[6,3,3,6,6,5,4],#{location => 2,node => 2}},
   {256,[6,4,3,1,4],#{location => 2,node => 2}},
   {256,[6,6,2,3,3,6],#{location => 3,node => 3}},
   {256,[6,6,3,1,5,2],#{location => 2,node => 2}},
   {256,[6,6,4,5],#{location => 2,node => 2}},
   {256,[6,6,5,2,2,6],#{location => 2,node => 2}},
   {256,[6,6,6,3,5,6],#{location => 2,node => 2}},
   {512,[1,1,2,1,4,1,4],#{location => 2,node => 2}},
   {512,[1,1,2,3,3,2,2],#{location => 2,node => 2}},
   {512,[1,1,3,2,1],#{location => 2,node => 2}},
   {512,[1,1,3,2,2,4,3],#{location => 3,node => 3}},
   {512,[1,2,1,2,2,3],#{location => 3,node => 3}},
   {512,[1,2,3],#{location => 2,node => 2}},
   {512,[1,2,3,3,1],#{location => 3,node => 3}},
   {512,[1,3,2,2,3,1,3],#{location => 4,node => 4}},
   {512,[1,3,4],#{location => 2,node => 2}},
   {512,[1,3,4,3,5],#{location => 3,node => 3}},
   {512,[1,4,2,3],#{location => 2,node => 2}},
   {512,[1,4,2,3,1,3,2],#{location => 4,node => 4}},
   {512,[1,4,4],#{location => 2,node => 2}},
   {512,[1,4,4,2,4],#{location => 3,node => 3}},
   {512,[1,4,4,4],#{location => 2,node => 2}},
   {512,[1,4,5,2,4,1,4],#{location => 3,node => 3}},
   {512,[1,5,2,1,2,5,2],#{location => 2,node => 2}},
   {512,[1,5,3,3,4,1,3],#{location => 4,node => 4}},
   {512,[1,5,4],#{location => 2,node => 2}},
   {512,[1,5,5],#{location => 2,node => 2}},
   {512,[1,6,2,4,2,4],#{location => 3,node => 3}},
   {512,[2,1,2,3,3,1,1],#{location => 3,node => 3}},
   {512,[2,1,3,2,3,3,4],#{location => 2,node => 2}},
   {512,[2,1,4,2,3,1,1],#{location => 3,node => 3}},
   {512,[2,1,4,3,2],#{location => 2,node => 2}},
   {512,[2,1,4,4,1,3],#{location => 2,node => 2}},
   {512,[2,1,4,4,4,4,2],#{location => 2,node => 2}},
   {512,[2,2,3,2,1,4,1],#{location => 2,node => 2}},
   {512,[2,3,1,4,2,1,6],#{location => 2,node => 2}},
   {512,[2,3,2,1],#{location => 2,node => 2}},
   {512,[2,3,2,4,4,2,5],#{location => 3,node => 3}},
   {512,[2,3,3,2,1],#{location => 2,node => 2}},
   {512,[2,3,3,2,3,1],#{location => 4,node => 4}},
   {512,[2,4,1,2,2],#{location => 2,node => 2}},
   {512,[2,4,2,4],#{location => 3,node => 3}},
   {512,[2,4,2,4,2],#{location => 3,node => 3}},
   {512,[2,4,4,1,4,4,2],#{location => 3,node => 3}},
   {512,[2,5,1,5,2],#{location => 3,node => 3}},
   {512,[2,5,5,4,5,4],#{location => 4,node => 4}},
   {512,[3,1,1,1,3,2],#{location => 3,node => 3}},
   {512,[3,1,1,2,3,1,1],#{location => 3,node => 3}},
   {512,[3,1,1,2,3,3,3],#{location => 4,node => 4}},
   {512,[3,1,1,3,3,1],#{location => 3,node => 3}},
   {512,[3,1,1,4,4,3],#{location => 3,node => 3}},
   {512,[3,1,3,3,2,1],#{location => 3,node => 3}},
   {512,[3,1,3,5],#{location => 2,node => 2}},
   {512,[3,2,1,1],#{location => 2,node => 2}},
   {512,[3,2,2,1,3,3],#{location => 2,node => 2}},
   {512,[3,2,2,1,3,4],#{location => 3,node => 3}},
   {512,[3,2,2,2,3,1],#{location => 2,node => 2}},
   {512,[3,2,4,1,2],#{location => 2,node => 2}},
   {512,[3,2,4,5,3],#{location => 2,node => 2}},
   {512,[3,2,5,4,3,2],#{location => 3,node => 3}},
   {512,[3,3,2,2,1,1],#{location => 3,node => 3}},
   {512,[3,3,3,1,2],#{location => 2,node => 2}},
   {512,[3,3,5,4,1,1,4],#{location => 2,node => 2}},
   {512,[3,4,2,2],#{location => 2,node => 2}},
   {512,[3,4,2,3],#{location => 2,node => 2}},
   {512,[3,4,3,1,6,3,6],#{location => 3,node => 3}},
   {512,[3,4,3,4,2,4],#{location => 2,node => 2}},
   {512,[3,4,4,1],#{location => 3,node => 3}},
   {512,[3,4,4,2],#{location => 3,node => 3}},
   {512,[3,4,5,5],#{location => 2,node => 2}},
   {512,[3,5,4,1,1],#{location => 2,node => 2}},
   {512,[3,5,4,1,5],#{location => 3,node => 3}},
   {512,[3,6,2,2,5],#{location => 3,node => 3}},
   {512,[4,1,2,3,2,2],#{location => 2,node => 2}},
   {512,[4,1,2,4,1,4],#{location => 3,node => 3}},
   {512,[4,1,2,4,5,3,4],#{location => 4,node => 4}},
   {512,[4,1,4,2,2,2],#{location => 3,node => 3}},
   {512,[4,1,4,2,5,4],#{location => 3,node => 3}},
   {512,[4,2,1,4,4],#{location => 2,node => 2}},
   {512,[4,2,3,2,6],#{location => 2,node => 2}},
   {512,[4,2,3,4,6],#{location => 3,node => 3}},
   {512,[4,3,2],#{location => 2,node => 2}},
   {512,[4,3,2,2,1,4],#{location => 4,node => 4}},
   {512,[4,3,2,4,3,3,3],#{location => 3,node => 3}},
   {512,[4,3,3,1,2],#{location => 2,node => 2}},
   {512,[4,3,5,4,4,5],#{location => 4,node => 4}},
   {512,[4,4,1,5,2],#{location => 3,node => 3}},
   {512,[4,4,1,6,6,3,3],#{location => 2,node => 2}},
   {512,[4,4,2,4,2],#{location => 3,node => 3}},
   {512,[4,4,2,4,4,1],#{location => 3,node => 3}},
   {512,[4,4,4,2,3],#{location => 2,node => 2}},
   {512,[4,4,5,3,5,4],#{location => 4,node => 4}},
   {512,[4,4,5,5,3,2,1],#{location => 2,node => 2}},
   {512,[4,5,3],#{location => 2,node => 2}},
   {512,[4,5,4,4,6,5],#{location => 3,node => 3}},
   {512,[4,5,5,6,6],#{location => 3,node => 3}},
   {512,[4,6,5],#{location => 2,node => 2}},
   {512,[5,1,2,1,2,2],#{location => 2,node => 2}},
   {512,[5,1,4,4],#{location => 2,node => 2}},
   {512,[5,2,2,4,3,1],#{location => 2,node => 2}},
   {512,[5,2,3],#{location => 2,node => 2}},
   {512,[5,2,4,1],#{location => 2,node => 2}},
   {512,[5,2,4,3,1],#{location => 3,node => 3}},
   {512,[5,2,5,5,3,1,1],#{location => 2,node => 2}},
   {512,[5,3,3,5],#{location => 2,node => 2}},
   {512,[5,3,6,5],#{location => 3,node => 3}},
   {512,[5,5,1,3,4],#{location => 3,node => 3}},
   {512,[5,5,4,4,5,1],#{location => 2,node => 2}},
   {512,[5,6,4,3],#{location => 3,node => 3}},
   {512,[6,5,4,4],#{location => 2,node => 2}}].
