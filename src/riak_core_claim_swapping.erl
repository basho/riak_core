%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
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

%% @doc This claim algorithm works for location awareness.
%% In a ring, nodes may be at different locations and the algorithm
%% tries to computes a claim that respects target_n_val w.r.t.
%% those locations. This implies that the nodes themselve meet
%% target_n_val, since each node lives in exactly one location.
%%
%% The algorithm allows for two different target_n_val values, one
%% for nodes and one for locations. However, great care should be
%% taken when using different values.
%%
%% Nodes that do not have a location associated with it, will end up
%% all in the same dummy location. This means that if there are no
%% locations at all, they all end up in the same location.
%% This would normally mean we cannot meet the target_n_val. Therefore,
%% we treat the case without locations as a special case and
%% only look at the target_n_val for nodes when there is no location
%% defined.
%%
%% We always start from a given ring to get a solution and try
%% best effort to find a solution with a minimal amount of transfers.
%% If we cannot find such a solution, we fall back to generating
%% a solution from scratch, which may involve many transfers.
%%
%% Not all configurations do have a solution. If no solution can be found,
%% the algorithm provides is best effort solution.

-module(riak_core_claim_swapping).

-export([claim_v5/1, claim_v5/2]).

%% LEAVE is a thing we can fix here. Make sure that translation of the nodes is such that
%% leaving node is the top of a location.. eg B1 B2 B3 B4, then B4 is the node you want to remove
%% after this mapping on old node, new node only gets B1 B2 B3 and computes minimal
                                                % transfers to solve that. Back translation now gives right nodes.


%% For backward compatibility.
%% The algorithm does not use any wants claim logic.
%% -spec wants_claim_v5(riak_core_ring:riak_core_ring()) -> no.
%% wants_claim_v5(Ring) ->
%%     wants_claim_v5(Ring, node()).

%% -spec wants_claim_v5(riak_core_ring:riak_core_ring(), node()) -> no.
%% wants_claim_v5(_Ring, _Node) ->
%%     no.


-spec claim_v5(riak_core_ring:riak_core_ring()) -> riak_core_ring:riak_core_ring().
claim_v5(Ring) ->
    Params = riak_core_membership_claim:default_choose_params(),
    claim_v5(Ring, Params).

-spec claim_v5(riak_core_ring:riak_core_ring(), [{atom(), term()}]) ->
          riak_core_ring:riak_core_ring().
claim_v5(Ring, Params0) ->
    Params = riak_core_membership_claim:default_choose_params(Params0),
    TargetN = proplists:get_value(target_n_val, Params),
    LocationDict = riak_core_ring:get_nodes_locations(Ring),
    HasLocations = riak_core_location:has_location_set_in_cluster(LocationDict),
    %% all locations, even those that may be empty because claimants have left
    TargetLN =
        if HasLocations -> proplists:get_value(target_location_n_val, Params, TargetN);
           true -> 1
        end,
    RingSize = riak_core_ring:num_partitions(Ring),
    NVals = {TargetN, TargetLN},

    %% Now we need to map the locations and nodes to a configuration that
    %% basically is a list of locations with the number of nodes in it.
    %% We compute both the old and the new ring, such that we can perform updates.
    %% This is mapped back after the algorithm is applied.
    %% Therefore it is important to have leaving nodes mapped to
    %% indices that are not occuring in the new ring

    %% Compute old ring

    {BinRing0, _OldLocRel} = to_binring(Ring),
    io:format("OldRing = ~p\n", [BinRing0]),

    {Config, NewLocRel} = to_config(Ring),
    LocRel = NewLocRel,

    io:format("Config = ~p RingSize ~p nval ~p\n", [Config, RingSize, NVals]),
    BinRing1 = riak_core_claim_binring_alg:update(BinRing0, Config, NVals),

    BinRing =
        case riak_core_claim_binring_alg:zero_violations(BinRing1, NVals) of
            false ->
                io:format("~s\n->\n~s\n", [ring:show(BinRing0, NVals),
                                           ring:show(BinRing1, NVals)]),
                riak_core_claim_binring_alg:solve(RingSize, Config, NVals);
            true ->
                BinRing1
        end,

    Inc = chash:ring_increment(RingSize),
    SolvedNodes =
        [ begin
              {_Loc, Node} = proplists:get_value({LocIdx, NodeIdx}, LocRel),
              {Inc * (Idx-1), Node}
          end || {Idx, {LocIdx, NodeIdx}} <- enumerate(ring:to_list(BinRing)) ],

    NewRing =
        lists:foldl(
          fun({Idx, N}, Ring0) ->
                  riak_core_ring:transfer_node(Idx, N, Ring0)
          end,
          Ring,
          SolvedNodes),

    NewRing.


to_binring(Ring) ->
    LocationDict = riak_core_ring:get_nodes_locations(Ring),
    LeavingMembers = riak_core_ring:members(Ring, [leaving]),
    %% Make sure leaving members at the end
    AllOwners =
        [ Owner || {_, Owner} <- riak_core_ring:all_owners(Ring)],

    LocationRing =
        [ {riak_core_location:get_node_location(N, LocationDict), N} || N <- AllOwners ],

    Locs = lists:usort([ L || {L, _} <- LocationRing ]),
    LocNodes = [ {Loc, uleaving_last([N || {L, N} <- LocationRing, L == Loc], LeavingMembers)}
                 || Loc <- Locs ],

    LocationRel =
        [{{LocIdx, Idx}, {Loc, N}} || {LocIdx, {Loc, Ns}} <- enumerate(LocNodes),
                                      {Idx, N} <- enumerate(Ns)],
    io:format("Old Relation: ~p\n", [LocationRel]),

    Nodes = [ begin
                {Node, _} = lists:keyfind({L, N}, 2, LocationRel),
                Node
              end || {L, N} <- LocationRing ],
    {ring:from_list(Nodes), LocationRel}.

to_config(Ring) ->
    Claiming = riak_core_ring:claiming_members(Ring),
    LocationDict = riak_core_ring:get_nodes_locations(Ring),
    LocationNodes = [ {riak_core_location:get_node_location(N, LocationDict), N} || N <- Claiming ],
    io:format("Location Nodes = ~p\n", [LocationNodes]),
    %% keep order of locations the same as in old ring
    Locs = lists:usort([ L || {L, _} <- LocationNodes ]),
    LocNodes = [ {Loc, [N || {L, N} <- LocationNodes, L == Loc]} || Loc <- Locs ],
    LocationRel =
        [{{LocIdx, Idx}, {Loc, N}} || {LocIdx, {Loc, Ns}} <- enumerate(LocNodes),
                                      {Idx, N} <- enumerate(Ns)],
    io:format("New Relation: ~p\n", [LocationRel]),

    {[ length(Ns) || {_, Ns} <- LocNodes ], LocationRel}.

uleaving_last(Nodes, LeavingNodes) ->
    UNodes = lists:usort(Nodes),
    ULeaving = lists:usort(LeavingNodes),
    uleaving_last(UNodes, ULeaving, UNodes -- ULeaving).

uleaving_last(_Nodes, [], Acc) ->
    Acc;
uleaving_last(Nodes, [Leave|Leaves], Acc) ->
    case lists:member(Leave, Nodes) of
        true ->  uleaving_last(Nodes, Leaves, Acc ++ [Leave]);
        false -> uleaving_last(Nodes, Leaves, Acc)
    end.

%% in OTP 25 one can use lists:enumerate
enumerate(List) ->
    lists:zip(lists:seq(1, length(List)), List).

%% ===================================================================
%% eunit tests
%% ===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

simple_cluster_t1_test() ->
    RingSize = 32,
    TargetN = 4,
    NodeList = [n1, n2, n3, n4, n5, n6],
    R0 = riak_core_ring:fresh(RingSize, n1),
    R1 =
        lists:foldl(
            fun(N, AccR) -> riak_core_ring:add_member(n1, AccR, N) end,
            R0,
            NodeList -- [n1]),
    Props = [{target_n_val, TargetN}],
    RClaim =
       claim_v5(R1, Props),
    ?assert(true, riak_core_membership_claim:meets_target_n(RClaim, TargetN)).


location_t1_test_() ->
    JoiningNodes =
        [{n2, loc1},
        {n3, loc2}, {n4, loc2},
        {n5, loc3}, {n6, loc3},
        {n7, loc4}, {n8, loc4},
        {n9, loc5}, {n10, loc5}
    ],
    {"[2, 2, 2, 2, 2] nval 4",
     {inparallel,
      [location_claim_tester(n1, loc1, JoiningNodes, 64, 4),
       location_claim_tester(n1, loc1, JoiningNodes, 128, 4),
       location_claim_tester(n1, loc1, JoiningNodes, 256, 4)
       %% Don't test large rings in automated testing
       %% location_claim_tester(n1, loc1, JoiningNodes, 512, 4),
       %% location_claim_tester(n1, loc1, JoiningNodes, 1024, 4),
       %% location_claim_tester(n1, loc1, JoiningNodes, 2048, 4)
      ]}}.

location_t2_test_() ->
    JoiningNodes =
        [{n2, loc1},
         {n3, loc2}, {n4, loc2},
         {n5, loc3}, {n6, loc3},
         {n7, loc4}, {n8, loc4}
        ],
    {"[2, 2, 2, 2] nval 4",
     {inparallel,
      [location_claim_tester(n1, loc1, JoiningNodes, 64, 4),
       location_claim_tester(n1, loc1, JoiningNodes, 128, 4),
       location_claim_tester(n1, loc1, JoiningNodes, 256, 4)
       %% Don't test large rings in automated testing
       %% location_claim_tester(n1, loc1, JoiningNodes, 512, 4),
       %% location_claim_tester(n1, loc1, JoiningNodes, 1024, 4),
       %% location_claim_tester(n1, loc1, JoiningNodes, 2048, 4),
      ]}}.

location_t8_test_() ->
    JoiningNodes =
        [{l1n2, loc1}, {l1n3, loc1}, {l1n4, loc1},
         {l2n1, loc2}, {l2n2, loc2}, {l2n3, loc2},
         {l3n1, loc3}, {l3n2, loc3}, {l3n3, loc3},
         {l4n1, loc4}, {l4n2, loc4}, {l4n3, loc4}],
    {"[4, 3, 3, 3] nval 4",
     location_claim_tester(l1n1, loc1, JoiningNodes, 256, 3)
     %% Don't test large rings in automated testing
     %% location_claim_tester(n1, loc1, JoiningNodes, 512, 4),
     %% location_claim_tester(n1, loc1, JoiningNodes, 1024, 4),
     %% location_claim_tester(n1, loc1, JoiningNodes, 2048, 4)
    }.

location_claim_tester(N1, N1Loc, NodeLocList, RingSize, TargetN) ->
    {"Ringsize "++integer_to_list(RingSize),
    {timeout, 120,
     fun() ->
             io:format(
               "Testing NodeList ~w with RingSize ~w~n",
               [[{N1, N1Loc}|NodeLocList], RingSize]
              ),
             R1 =
                 riak_core_ring:set_node_location(
                   N1,
                   N1Loc,
                   riak_core_ring:fresh(RingSize, N1)),

             RClaim = add_nodes_to_ring(R1, N1, NodeLocList, [{target_n_val, TargetN}]),
             {RingSize, Mappings} = riak_core_ring:chash(RClaim),

             check_for_failures(Mappings, TargetN, RClaim)
     end}}.

add_nodes_to_ring(Ring, Claimant, NodeLocList, Params) ->
    NewRing = lists:foldl(
                fun({N, L}, AccR) ->
                        AccR0 = riak_core_ring:add_member(Claimant, AccR, N),
                        riak_core_ring:set_node_location(N, L, AccR0)
                end,
                Ring,
                NodeLocList),
    claim_v5(NewRing, Params).


check_for_failures(Mappings, TargetN, RClaim) ->
    NLs = riak_core_ring:get_nodes_locations(RClaim),
    LocationMap =
        lists:map(
            fun({Idx, N}) ->
                    {Idx, riak_core_location:get_node_location(N, NLs)}
            end,
            Mappings),
    Prefix = lists:sublist(LocationMap, TargetN),
    CheckableMap = LocationMap ++ Prefix,
    {_, Failures} =
        lists:foldl(
            fun({Idx, L}, {LastNminus1, Fails}) ->
                case lists:member(L, LastNminus1) of
                    false ->
                        {[L|lists:sublist(LastNminus1, TargetN - 2)], Fails};
                    true ->
                        {[L|lists:sublist(LastNminus1, TargetN - 2)],
                            [{Idx, L, LastNminus1}|Fails]}
                end
            end,
            {[], []},
            CheckableMap
        ),
    lists:foreach(fun(F) -> io:format("Failure ~p~n", [F]) end, Failures),
    ?assert(length(Failures) == 0).


location_multistage_t1_test_() ->
    %% This is a tricky corner case where we would fail to meet TargetN for
    %% locations if joining all 9 nodes in one claim (as old sequential_claim will
    %% not succeed).  However, If we join 8 nodes, then add the 9th, TargetN
    %% is always achieved
    JoiningNodes =
        [{l1n2, loc1},
            {l2n3, loc2}, {l2n4, loc2},
            {l3n5, loc3}, {l3n6, loc3},
            {l4n7, loc4}, {l4n8, loc4}
        ],
     {inparallel,
      [
       location_multistage_claim_tester(64, JoiningNodes, 4, l5n9, loc5, 4),
       location_multistage_claim_tester(128, JoiningNodes, 4, l5n9, loc5, 4),
       location_multistage_claim_tester(256, JoiningNodes, 4, l5n9, loc5, 4),
       location_multistage_claim_tester(512, JoiningNodes, 4, l5n9, loc5, 4),
       location_multistage_claim_tester(1024, JoiningNodes, 4, l5n9, loc5, 4),
       location_multistage_claim_tester(2048, JoiningNodes, 4, l5n9, loc5, 4)
       ]}.


location_multistage_claim_tester(RingSize, JoiningNodes, TargetN, NewNode, NewLocation, VerifyN) ->
     fun() ->
             SW0 = os:timestamp(),
             N1 = l1n1,
             N1Loc = loc1,
             io:format(
               "Testing NodeList ~w with RingSize ~w~n",
               [[{N1, N1Loc}|JoiningNodes], RingSize]
              ),
             R1 =
                 riak_core_ring:set_node_location(
                   N1,
                   N1Loc,
                   riak_core_ring:fresh(RingSize, N1)),

             Params = [{target_n_val, TargetN}],
             SW1 = os:timestamp(),
             RClaimInit = add_nodes_to_ring(R1, N1, JoiningNodes, Params),

             SW2 = os:timestamp(),
             io:format("Reclaiming without committing~n"),

             RingExtendA =
                 riak_core_ring:set_node_location(
                   NewNode,
                   NewLocation,
                   riak_core_ring:add_member(N1, RClaimInit, NewNode)),
             RClaimExtendA = claim_v5(RingExtendA, Params),

             io:format("Commit initial claim~n"),
             SW3 = os:timestamp(),

             RClaimInitCommit =
                 riak_core_ring:increment_vclock(
                   node(),
                   riak_core_ring:clear_location_changed(RClaimInit)),

             io:format("Reclaiming following commit~n"),
             SW4 = os:timestamp(),

             RingExtendB =
                 riak_core_ring:set_node_location(
                   NewNode,
                   NewLocation,
                   riak_core_ring:add_member(N1, RClaimInitCommit, NewNode)),
             RClaimExtendB = claim_v5(RingExtendB, Params),

             {_RingSizeInit, MappingsInit} = riak_core_ring:chash(RClaimInit),
             {RingSizeA, MappingsA} = riak_core_ring:chash(RClaimExtendA),
             {RingSizeB, MappingsB} = riak_core_ring:chash(RClaimExtendB),

             SW5 = os:timestamp(),

             ?assert(RingSizeA == RingSizeB),
             ?assert(MappingsA == MappingsB),

             io:format("Testing initial Mappings:~n~n~p~n", [MappingsInit]),
             check_for_failures(MappingsInit, VerifyN, RClaimInit),
             io:format("Testing secondary Mappings:~n~n~p~n", [MappingsB]),
             check_for_failures(MappingsB, VerifyN, RClaimExtendB),

             SW6 = os:timestamp(),
             io:format(
               "Test for RingSize ~w had timings:"
               "Setup ~w  First Claim ~w  Next Claim ~w Commit ~w Other Claims ~w Verify ~w~n",
               [RingSize,
                timer:now_diff(SW1, SW0) div 1000,
                timer:now_diff(SW2, SW1) div 1000,
                timer:now_diff(SW3, SW2) div 1000,
                timer:now_diff(SW4, SW3) div 1000,
                timer:now_diff(SW5, SW4) div 1000,
                timer:now_diff(SW6, SW5) div 1000]
              )
     end.

location_typical_expansion_test_() ->
    {inparallel, [location_typical_expansion_tester(64),
                  location_typical_expansion_tester(128),
                  location_typical_expansion_tester(256),
                  location_typical_expansion_tester(512)]}.

location_typical_expansion_tester(RingSize) ->
    {"Ringsize "++integer_to_list(RingSize),
     fun() ->
             N1 = l1n1,
             N1Loc = loc1,
             TargetN = 4,
             InitJoiningNodes =
                 [{l1n2, loc1},
                  {l2n3, loc2}, {l2n4, loc2},
                  {l3n5, loc3}, {l3n6, loc3},
                  {l4n7, loc4}, {l4n8, loc4}],

             io:format(
               "Testing NodeList ~w with RingSize ~w~n",
               [[{N1, N1Loc}|InitJoiningNodes], RingSize]
              ),
             Params = [{target_n_val, TargetN}],
             R1 =
                 riak_core_ring:set_node_location(
                   N1,
                   N1Loc,
                   riak_core_ring:fresh(RingSize, N1)),

             RClaimInit = add_nodes_to_ring(R1, N1, InitJoiningNodes, Params),
             {RingSize, MappingsInit} = riak_core_ring:chash(RClaimInit),

             check_for_failures(MappingsInit, TargetN, RClaimInit),

             Stage1Ring =
                 lists:foldl(
                   fun(JN, R) ->
                           riak_core_ring:set_member(node(), R, JN, valid, same_vclock)
                   end,
                   RClaimInit,
                   riak_core_ring:members(RClaimInit, [joining])
                  ),

             RClaimStage2 = add_node(Stage1Ring, N1, l5n9, loc5, Params),
             {RingSize, Mappings2} = riak_core_ring:chash(RClaimStage2),
             check_for_failures(Mappings2, TargetN, RClaimStage2),
             Stage2Ring = commit_change(RClaimStage2),

             RClaimStage3 = add_node(Stage2Ring, N1, l5n10, loc5, Params),
             {RingSize, Mappings3} = riak_core_ring:chash(RClaimStage3),
             check_for_failures(Mappings3, TargetN, RClaimStage3),
             Stage3Ring = commit_change(RClaimStage3),

             RClaimStage4 = add_node(Stage3Ring, N1, l6n11, loc6, Params),
             {RingSize, Mappings4} = riak_core_ring:chash(RClaimStage4),
             check_for_failures(Mappings4, TargetN, RClaimStage4),
             Stage4Ring = commit_change(RClaimStage4),

             RClaimStage5 = add_node(Stage4Ring, N1, l6n12, loc6, Params),
             {RingSize, Mappings5} = riak_core_ring:chash(RClaimStage5),
             check_for_failures(Mappings5, TargetN, RClaimStage5),
             Stage5Ring = commit_change(RClaimStage5),

             RClaimStage6 = add_node(Stage5Ring, N1, l1n13, loc1, Params),
             {RingSize, Mappings6} = riak_core_ring:chash(RClaimStage6),
             check_for_failures(Mappings6, TargetN, RClaimStage6),
             Stage6Ring = commit_change(RClaimStage6),

             RClaimStage7 = add_node(Stage6Ring, N1, l2n14, loc2, Params),
             {RingSize, Mappings7} = riak_core_ring:chash(RClaimStage7),
             check_for_failures(Mappings7, TargetN, RClaimStage7),
             Stage7Ring = commit_change(RClaimStage7),

             RClaimStage8 = add_node(Stage7Ring, N1, l3n15, loc3, Params),
             {RingSize, Mappings8} = riak_core_ring:chash(RClaimStage8),
             check_for_failures(Mappings8, TargetN, RClaimStage8),
             Stage8Ring = commit_change(RClaimStage8),

             RClaimStage9 = add_node(Stage8Ring, N1, l4n16, loc4, Params),
             {RingSize, Mappings9} = riak_core_ring:chash(RClaimStage9),
             check_for_failures(Mappings9, TargetN, RClaimStage9),
             _Stage9Ring = commit_change(RClaimStage9)
     end}.


add_node(Ring, Claimant, Node, Location, Params) ->
    RingC = add_nodes_to_ring(Ring, Claimant, [{Node, Location}], Params),

    OwnersPre = riak_core_ring:all_owners(Ring),
    OwnersPost = riak_core_ring:all_owners(RingC),
    OwnersZip = lists:zip(OwnersPre, OwnersPost),
    Next =
        [{Idx, PrevOwner, NewOwner, [], awaiting} ||
            {{Idx, PrevOwner}, {Idx, NewOwner}} <- OwnersZip,
            PrevOwner /= NewOwner],

    NodeCountD =
        lists:foldl(
            fun({_Idx, N}, D) ->
                dict:update_counter(N, 1, D)
            end,
            dict:new(),
            OwnersPost
        ),
    NodeCounts =
        lists:map(fun({_N, C}) -> C end, dict:to_list(NodeCountD)),
    io:format(
        % user,
        "NodeCounts~w~n",
        [dict:to_list(NodeCountD)]),
    io:format(
        % user,
        "Adding node ~w in location ~w - ~w transfers ~w max ~w min vnodes~n",
        [Node, Location,
            length(Next), lists:max(NodeCounts), lists:min(NodeCounts)]),
    ?assert(
        (lists:min(NodeCounts) == (lists:max(NodeCounts) - 1)) or
        (lists:min(NodeCounts) == lists:max(NodeCounts))
    ),
    % ?assert(length(Next) =< ExpectedTransferMax),
    RingC.

commit_change(Ring) ->
    lists:foldl(
        fun(JN, R) ->
            riak_core_ring:set_member(node(), R, JN, valid, same_vclock)
        end,
        Ring,
        riak_core_ring:members(Ring, [joining])
    ).

-endif.
