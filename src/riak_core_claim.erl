%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc The default functions used for claiming partition ownership.  Generally,
%%      a wants_claim function should return either {yes, Integer} or 'no' where
%%      Integer is the number of additional partitions wanted by this node.  A
%%      choose_claim function should return a riak_core_ring with more
%%      partitions claimed by this node than in the input ring.

%% The usual intention for partition ownership assumes relative heterogeneity of
%% capacity and connectivity.  Accordingly, the standard claim functions attempt
%% to maximize "spread" -- expected distance between partitions claimed by each
%% given node.  This is in order to produce the expectation that for any
%% reasonably short span of consecutive partitions, there will be a minimal
%% number of partitions owned by the same node.

%% The exact amount that is considered tolerable is determined by the
%% application env variable "target_n_val".  The functions in riak_core_claim
%% will ensure that all sequences up to target_n_val long contain no repeats if
%% at all possible.  The effect of this is that when the number of nodes in the
%% system is smaller than target_n_val, a potentially large number of partitions
%% must be moved in order to safely add a new node.  After the cluster has grown
%% beyond that size, a minimal number of partitions (1/NumNodes) will generally
%% be moved.

%% If the number of nodes does not divide evenly into the number of partitions,
%% it may not be possible to perfectly achieve the maximum spread constraint.
%% In that case, Riak will minimize the cases where the constraint is violated
%% and they will all exist near the origin point of the ring.

%% A good way to decide on the setting of target_n_val for your application is
%% to set it to the largest value you expect to use for any bucket's n_val.  The
%% default is 4.

-module(riak_core_claim).
-export([claim/1, claim/3, claim_until_balanced/2, claim_until_balanced/4]).
-export([default_wants_claim/1, default_wants_claim/2,
         default_choose_claim/1, default_choose_claim/2, default_choose_claim/3,
         never_wants_claim/1, never_wants_claim/2,
         random_choose_claim/1, random_choose_claim/2, random_choose_claim/3]).
-export([wants_claim_v1/1, wants_claim_v1/2,
         wants_claim_v2/1, wants_claim_v2/2,
         wants_claim_v3/1, wants_claim_v3/2,
         choose_claim_v1/1, choose_claim_v1/2, choose_claim_v1/3,
         choose_claim_v2/1, choose_claim_v2/2, choose_claim_v2/3,
         choose_claim_v3/1, choose_claim_v3/2, choose_claim_v3/3,
         claim_rebalance_n/2, claim_diversify/3, claim_diagonal/3,
         wants/1, wants_owns_diff/2, meets_target_n/2, diagonal_stripe/2]).

-ifdef(TEST).
-ifdef(EQC).
-export([prop_claim_ensures_unique_nodes/1, prop_wants/0, prop_wants_counts/0]).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(DEF_TARGET_N, 4).

claim(Ring) ->
    Want = app_helper:get_env(riak_core, wants_claim_fun),
    Choose = app_helper:get_env(riak_core, choose_claim_fun),
    claim(Ring, Want, Choose).

claim(Ring, Want, Choose) ->
    Members = riak_core_ring:claiming_members(Ring),
    lists:foldl(fun(Node, Ring0) ->
                        claim_until_balanced(Ring0, Node, Want, Choose)
                end, Ring, Members).

claim_until_balanced(Ring, Node) ->
    Want = app_helper:get_env(riak_core, wants_claim_fun),
    Choose = app_helper:get_env(riak_core, choose_claim_fun),
    claim_until_balanced(Ring, Node, Want, Choose).

claim_until_balanced(Ring, Node, {WMod, WFun}=Want, Choose) ->
    NeedsIndexes = apply(WMod, WFun, [Ring, Node]),
    case NeedsIndexes of
        no ->
            Ring;
        {yes, _NumToClaim} ->
            NewRing = case Choose of
                          {CMod, CFun} ->
                              CMod:CFun(Ring, Node);
                          {CMod, CFun, Params} ->
                              CMod:CFun(Ring, Node, Params)
                      end,
            claim_until_balanced(NewRing, Node, Want, Choose)
    end.

%% ===================================================================
%% Claim Function Implementations 
%% ===================================================================

%% @spec default_choose_claim(riak_core_ring()) -> riak_core_ring()
%% @doc Choose a partition at random.
default_choose_claim(Ring) ->
    default_choose_claim(Ring, node()).

default_choose_claim(Ring, Node) ->
    choose_claim_v2(Ring, Node).

default_choose_claim(Ring, Node, Params) ->
    choose_claim_v2(Ring, Node, Params).

%% @spec default_wants_claim(riak_core_ring()) -> {yes, integer()} | no
%% @doc Want a partition if we currently have less than floor(ringsize/nodes).
default_wants_claim(Ring) ->
    default_wants_claim(Ring, node()).

default_wants_claim(Ring, Node) ->
    wants_claim_v2(Ring, Node).

%% @deprecated
wants_claim_v1(Ring) ->
    wants_claim_v1(Ring, node()).

%% @deprecated
wants_claim_v1(Ring0, Node) ->
    Ring = riak_core_ring:upgrade(Ring0),
    %% Calculate the expected # of partitions for a perfectly balanced ring. Use
    %% this expectation to determine the relative balance of the ring. If the
    %% ring isn't within +-2 partitions on all nodes, we need to rebalance.
    ExpParts = get_expected_partitions(Ring, Node),
    PCounts = lists:foldl(fun({_Index, ANode}, Acc) ->
                                  orddict:update_counter(ANode, 1, Acc)
                          end, [{Node, 0}], riak_core_ring:all_owners(Ring)),
    RelativeCounts = [I - ExpParts || {_ANode, I} <- PCounts],
    WantsClaim = (lists:min(RelativeCounts) < -2) or (lists:max(RelativeCounts) > 2),
    case WantsClaim of
        true ->
            {yes, 0};
        false ->
            no
    end.

wants_claim_v2(Ring) ->
    wants_claim_v2(Ring, node()).

wants_claim_v2(Ring, Node) ->
    Active = riak_core_ring:claiming_members(Ring),
    Owners = riak_core_ring:all_owners(Ring),
    Counts = get_counts(Active, Owners),
    NodeCount = erlang:length(Active),
    RingSize = riak_core_ring:num_partitions(Ring),
    Avg = RingSize div NodeCount,
    Count = proplists:get_value(Node, Counts, 0),
    case Count < Avg of
        false ->
            no;
        true ->
            {yes, Avg - Count}
    end.

%% Wants claim v3 - calculates the wants the same way as choose_claim_v3
%% and checks if they have changed since it was last run.
wants_claim_v3(Ring) ->
    wants_claim_v3(Ring, node()).

wants_claim_v3(Ring, _Node) ->
    Wants = wants(Ring),
    
    %% This case will only hold true during claim_until_balanced
    %% as only the ownership information is transferred after
    %% running claim not the metadata.
    case riak_core_ring:get_meta(claimed, Ring) of
        {ok, {claim_v3, Wants}} ->
            lager:debug("WantsClaim3(~p) no.  Current ring claimed for ~p\n", 
                        [_Node, Wants]),
            no;
        {ok, {claim_v3, CurWants}} ->
            lager:debug("WantsClaim3(~p) yes.  Current ring claimed for "
                        "different wants\n~p\n",
                        [_Node, CurWants]),
            {yes, 1};
        undefined ->
            %% First time through claim_until_balanced, check for override
            %% to recalculate.
            case app_helper:get_env(riak_core, force_reclaim, false) of
                true ->
                    application:unset_env(riak_core, force_reclaim),
                    lager:info("Forced rerun of claim algorithm - "
                               "unsetting force_reclaim"),
                    {yes, 1};
                false ->
                    %% Otherwise, base wants decision on whether the current 
                    %% wants versus current ownership if the claim does not
                    %% manage to claim all requested nodes then the temporary
                    %% 'claim_v3' metadata will stop the loop
                    Owns = get_counts(riak_core_ring:claiming_members(Ring),
                                      riak_core_ring:all_owners(Ring)),
                    Deltas = wants_owns_diff(Wants, Owns),
                    Diffs = lists:sum([abs(Diff) || {_, Diff} <- Deltas]),
                    case Diffs of
                        0 ->
                            lager:debug("WantsClaim3(~p) no.  All wants met.\n", 
                                        [_Node]),
                            no;
                        _ ->
                            lager:debug("WantsClaim3(~p) yes - ~p.\n"
                                        "Does not meet wants - diffs ~p\n",
                                        [_Node, Diffs, Deltas]),
                            {yes, Diffs}
                    end
            end
    end.

%% Provide default choose parameters if none given
default_choose_params() ->
    default_choose_params([]).

default_choose_params(Params) ->
    case proplists:get_value(target_n_val, Params) of
        undefined ->
            TN = app_helper:get_env(riak_core, target_n_val, ?DEF_TARGET_N),
            [{target_n_val, TN} | Params];
        _->
            Params
    end.

%% @deprecated
choose_claim_v1(Ring) ->
    choose_claim_v1(Ring, node()).

%% @deprecated
choose_claim_v1(Ring0, Node) ->
    choose_claim_v1(Ring0, Node, []).

choose_claim_v1(Ring0, Node, Params0) ->
    Params = default_choose_params(Params0),
    Ring = riak_core_ring:upgrade(Ring0),
    TargetN = proplists:get_value(target_n_val, Params),
    case meets_target_n(Ring, TargetN) of
        {true, TailViolations} ->
            %% if target N is met, then it doesn't matter where
            %% we claim vnodes, as long as we don't violate the
            %% target N with any of our additions
            %% (== claim partitions at least N steps apart)
            claim_with_n_met(Ring, TailViolations, Node);
        false ->
            %% we don't meet target N yet, rebalance
            claim_rebalance_n(Ring, Node)
    end.

choose_claim_v2(Ring) ->
    choose_claim_v2(Ring, node()).

choose_claim_v2(Ring, Node) ->
    Params = default_choose_params(),
    choose_claim_v2(Ring, Node, Params).

choose_claim_v2(Ring, Node, Params0) ->
    Params = default_choose_params(Params0),
    Active = riak_core_ring:claiming_members(Ring),
    Owners = riak_core_ring:all_owners(Ring),
    Counts = get_counts(Active, Owners),
    RingSize = riak_core_ring:num_partitions(Ring),
    NodeCount = erlang:length(Active),
    Avg = RingSize div NodeCount,
    ActiveDeltas = [{Member, Avg - Count} || {Member, Count} <- Counts],
    Deltas = add_default_deltas(Owners, ActiveDeltas, 0),
    {_, Want} = lists:keyfind(Node, 1, Deltas),
    TargetN = proplists:get_value(target_n_val, Params),
    AllIndices = lists:zip(lists:seq(0, length(Owners)-1),
                           [Idx || {Idx, _} <- Owners]),

    EnoughNodes =
        (NodeCount > TargetN)
        or ((NodeCount == TargetN) and (RingSize rem TargetN =:= 0)),
    case EnoughNodes of
        true ->
            %% If we have enough nodes to meet target_n, then we prefer to
            %% claim indices that are currently causing violations, and then
            %% fallback to indices in linear order. The filtering steps below
            %% will ensure no new violations are introduced.
            Violated = lists:flatten(find_violations(Ring, TargetN)),
            Violated2 = [lists:keyfind(Idx, 2, AllIndices) || Idx <- Violated],
            Indices = Violated2 ++ (AllIndices -- Violated2);
        false ->
            %% If we do not have enough nodes to meet target_n, then we prefer
            %% claiming the same indices that would occur during a
            %% re-diagonalization of the ring with target_n nodes, falling
            %% back to linear offsets off these preferred indices when the
            %% number of indices desired is less than the computed set.
            Padding = lists:duplicate(TargetN, undefined),
            Expanded = lists:sublist(Active ++ Padding, TargetN),
            PreferredClaim = riak_core_claim:diagonal_stripe(Ring, Expanded),
            PreferredNth = [begin
                                {Nth, Idx} = lists:keyfind(Idx, 2, AllIndices),
                                Nth
                            end || {Idx,Owner} <- PreferredClaim,
                                   Owner =:= Node],
            Offsets = lists:seq(0, RingSize div length(PreferredNth)),
            AllNth = lists:sublist([(X+Y) rem RingSize || Y <- Offsets,
                                                          X <- PreferredNth],
                                   RingSize),
            Indices = [lists:keyfind(Nth, 1, AllIndices) || Nth <- AllNth]
    end,

    %% Filter out indices that conflict with the node's existing ownership
    Indices2 = prefilter_violations(Ring, Node, AllIndices, Indices,
                                    TargetN, RingSize),
    %% Claim indices from the remaining candidate set
    Claim = select_indices(Owners, Deltas, Indices2, TargetN, RingSize),
    Claim2 = lists:sublist(Claim, Want),
    NewRing = lists:foldl(fun(Idx, Ring0) ->
                                  riak_core_ring:transfer_node(Idx, Node, Ring0)
                          end, Ring, Claim2),

    RingChanged = ([] /= Claim2),
    RingMeetsTargetN = meets_target_n(NewRing, TargetN),
    case {RingChanged, EnoughNodes, RingMeetsTargetN} of
        {false, _, _} ->
            %% Unable to claim, fallback to re-diagonalization
            claim_rebalance_n(Ring, Node);
        {_, true, false} ->
            %% Failed to meet target_n, fallback to re-diagonalization
            claim_rebalance_n(Ring, Node);
        _ ->
            NewRing
    end.

meets_target_n(Ring, TargetN) ->
    Owners = lists:keysort(1, riak_core_ring:all_owners(Ring)),
    meets_target_n(Owners, TargetN, 0, [], []).
meets_target_n([{Part,Node}|Rest], TargetN, Index, First, Last) ->
    case lists:keytake(Node, 1, Last) of
        {value, {Node, LastIndex, _}, NewLast} ->
            if Index-LastIndex >= TargetN ->
                    %% node repeat respects TargetN
                    meets_target_n(Rest, TargetN, Index+1, First,
                                   [{Node, Index, Part}|NewLast]);
               true ->
                    %% violation of TargetN
                    false
            end;
        false ->
            %% haven't seen this node yet
            meets_target_n(Rest, TargetN, Index+1,
                           [{Node, Index}|First], [{Node, Index, Part}|Last])
    end;
meets_target_n([], TargetN, Index, First, Last) ->
    %% start through end guarantees TargetN
    %% compute violations at wrap around, but don't fail
    %% because of them: handle during reclaim
    Violations = 
        lists:filter(fun({Node, L, _}) ->
                             {Node, F} = proplists:lookup(Node, First),
                             (Index-L)+F < TargetN
                     end,
                     Last),
    {true, [ Part || {_, _, Part} <- Violations ]}.


choose_claim_v3(Ring) ->
    choose_claim_v3(Ring, node()).

choose_claim_v3(Ring, ClaimNode) ->
    Params = [{target_n_val, app_helper:get_env(riak_core, target_n_val, 
                                                ?DEF_TARGET_N)}],
    choose_claim_v3(Ring, ClaimNode, Params).

choose_claim_v3(Ring, _ClaimNode, Params) ->
    S = length(riak_core_ring:active_members(Ring)),
    Q = riak_core_ring:num_partitions(Ring),
    TN = proplists:get_value(target_n_val, Params, ?DEF_TARGET_N),
    Wants = wants(Ring),
    lager:debug("Claim3 started: S=~p Q=~p TN=~p\n", [S, Q, TN]),
    lager:debug("       wants: ~p\n", [Wants]),
    {Partitions, Owners} = lists:unzip(riak_core_ring:all_owners(Ring)),

    %% Seed the random number generator for predictable results
    %% run the claim, then put it back if possible
    OldSeed = random:seed(proplists:get_value(seed, Params, {1,2,3})),
    {NewOwners, NewMetrics} = claim_v3(Wants, Owners, Params),
    case OldSeed of
        undefined ->
            ok;
        _ ->
            {_,_,_} = random:seed(OldSeed),
            ok
    end,

    lager:debug("Claim3 metrics: ~p\n", [NewMetrics]),
    %% Build a new ring from it
    NewRing = lists:foldl(fun({_P, OldOwn, OldOwn}, R0) ->
                                  R0;
                             ({P, _OldOwn, NewOwn}, R0) ->
                                  riak_core_ring:transfer_node(P, NewOwn, R0)
                          end, Ring, 
                          lists:zip3(Partitions, Owners, NewOwners)),
    riak_core_ring:update_meta(claimed, {claim_v3, Wants}, NewRing).

%%
%% Claim V3 - unlike the v1/v2 algorithms, v3 treats claim as an optimization problem.
%% In it's current form it creates a number of possible claim plans and evaluates
%% them for violations, balance and diversity, choosing the 'best' plan.
%%
%% Violations are a count of how many partitions owned by the same node are within target-n
%% of one another. Lower is better, 0 is desired if at all possible.
%%
%% Balance is a measure of the number of partitions owned versus the number of partitions
%% wanted.  Want is supplied to the algorithm by the caller as a list of node/counts.  The
%% score for deviation is the RMS of the difference between what the node wanted and what it 
%% has.  Lower is better, 0 if all wants are mets.
%%
%% Diversity measures how often nodes are close to one another in the preference
%% list.  The more diverse (spread of distances apart), the more evenly the
%% responsibility for a failed node is spread across the cluster.  Diversity is
%% calculated by working out the count of each distance for each node pair
%% (currently distances are limited up to target N) and computing the RMS on that.
%% Lower diversity score is better, 0 if nodes are perfectly diverse.
%%
claim_v3(Wants, Owners, Params) ->
    TN = proplists:get_value(target_n_val, Params, ?DEF_TARGET_N),
    Q = length(Owners),
    Claiming = [N || {N,W} <- Wants, W > 0],
    Trials = proplists:get_value(trials, Params, 100),
    case length(Claiming) > TN of
        true ->
            NIs = build_nis(Wants, Owners),

            lager:debug("claim3 - NIs\n",[]),
            _ = [lager:debug("  ~p\n", [NI]) || NI <- NIs],

            %% Generate plans that resolve violations and overloads
            Plans = lists:usort(make_plans(Trials, NIs, Q, TN)),

            %% Work out which plan meets the balance and diversity objectives
            {_NewOwners, NewMetrics} = New = evaluate_plans(Plans, Wants, Q, TN),

            case proplists:get_value(violations, NewMetrics) of
                0 ->
                    New;
                _ ->
                    lager:debug("claimv3: Could not make plan without violations, diversifying\n",
                                []),
                   %% If could not build ring without violations, diversify it
                    claim_diversify(Wants, Owners, Params)
            end;
        false ->
            lager:debug("claimv3: Not enough nodes to run (have ~p need ~p), diagonalized\n",
                        [length(Claiming), TN+1]),
            claim_diagonal(Wants, Owners, Params)
    end.

%% Claim diversify tries to build a perfectly diverse ownership list that meets
%% target N.  It uses wants to work out which nodes want partitions, but does
%% not honor the counts currently.  The algorithm incrementally builds the ownership
%% list, updating the adjacency matrix needed to compute the diversity score as each
%% node is added and uses it to drive the selection of the next nodes.
claim_diversify(Wants, Owners, Params) ->
    TN = proplists:get_value(target_n_val, Params, ?DEF_TARGET_N),
    Q = length(Owners),
    Claiming = [N || {N,W} <- Wants, W > 0],
    {ok, NewOwners, _AM} = riak_core_claim_util:construct(
                             riak_core_claim_util:gen_complete_len(Q), Claiming, TN),
    {NewOwners, [diversified]}.

%% Claim nodes in seq a,b,c,a,b,c trying to handle the wraparound
%% case to meet target N
claim_diagonal(Wants, Owners, Params) ->
    TN = proplists:get_value(target_n_val, Params, ?DEF_TARGET_N),
    Claiming = lists:sort([N || {N,W} <- Wants, W > 0]),
    S = length(Claiming),
    Q = length(Owners),
    Reps = Q div S,
    %% Handle the ring wrapround case.  If possible try to pick nodes
    %% that are not within the first TN of Claiming, if enough nodes
    %% are available.
    Tail = Q - Reps * S,
    Last = case S >= TN + Tail of
               true -> % If number wanted can be filled excluding first TN nodes
                   lists:sublist(lists:nthtail(TN - Tail, Claiming), Tail);
               _ ->
                   lists:sublist(Claiming, Tail)
           end,
    {lists:flatten([lists:duplicate(Reps, Claiming), Last]), [diagonalized]}.

claim_rebalance_n(Ring0, Node) ->
    Ring = riak_core_ring:upgrade(Ring0),
    Nodes = lists:usort([Node|riak_core_ring:claiming_members(Ring)]),
    Zipped = diagonal_stripe(Ring, Nodes),
    lists:foldl(fun({P, N}, Acc) ->
                        riak_core_ring:transfer_node(P, N, Acc)
                end,
                Ring,
                Zipped).

diagonal_stripe(Ring, Nodes) ->
    %% diagonal stripes guarantee most disperse data
    Partitions = lists:sort([ I || {I, _} <- riak_core_ring:all_owners(Ring) ]),
    Zipped = lists:zip(Partitions,
                       lists:sublist(
                         lists:flatten(
                           lists:duplicate(
                             1+(length(Partitions) div length(Nodes)),
                             Nodes)),
                         1, length(Partitions))),
    Zipped.

random_choose_claim(Ring) ->
    random_choose_claim(Ring, node()).

random_choose_claim(Ring, Node) ->
    random_choose_claim(Ring, Node, []).

random_choose_claim(Ring0, Node, _Params) ->
    Ring = riak_core_ring:upgrade(Ring0),
    riak_core_ring:transfer_node(riak_core_ring:random_other_index(Ring),
                                 Node, Ring).

%% @spec never_wants_claim(riak_core_ring()) -> no
%% @doc For use by nodes that should not claim any partitions.
never_wants_claim(_) -> no.
never_wants_claim(_,_) -> no.

%% ===================================================================
%% Private
%% ===================================================================

%% @private
claim_hole(Ring, Mine, Owners, Node) ->
    Choices = case find_biggest_hole(Mine) of
                  {I0, I1} when I0 < I1 ->
                      %% start-middle of the ring
                      lists:takewhile(
                        fun({I, _}) -> I /= I1 end,
                        tl(lists:dropwhile(
                             fun({I, _}) -> I /= I0 end,
                             Owners)));
                  {I0, I1} when I0 > I1 ->
                      %% wrap-around end-start of the ring
                      tl(lists:dropwhile(
                           fun({I, _}) -> I /= I0 end, Owners))
                          ++lists:takewhile(
                              fun({I, _}) -> I /= I1 end, Owners);
                  {I0, I0} ->
                      %% node only has one claim
                      {Start, End} =
                          lists:splitwith(
                            fun({I, _}) -> I /= I0 end,
                            Owners),
                      tl(End)++Start
              end,
    Half = length(Choices) div 2,
    {I, _} = lists:nth(Half, Choices),
    riak_core_ring:transfer_node(I, Node, Ring).

%% @private
claim_with_n_met(Ring, TailViolations, Node) ->
    CurrentOwners = lists:keysort(1, riak_core_ring:all_owners(Ring)),
    Nodes = lists:usort([Node|riak_core_ring:claiming_members(Ring)]),
    case lists:sort([ I || {I, N} <- CurrentOwners, N == Node ]) of
        [] ->
            %% node hasn't claimed anything yet - just claim stuff
            Spacing = length(Nodes),
            [{First,_}|OwnList] =
                case TailViolations of
                    [] ->
                        %% no wrap-around problems - choose whatever
                        lists:nthtail(Spacing-1, CurrentOwners);
                    [TV|_] ->
                        %% attempt to cure a wrap-around problem
                        lists:dropwhile(
                             fun({I, _}) -> I /= TV end,
                             lists:reverse(CurrentOwners))
                end,
            {_, NewRing} = lists:foldl(
                             fun({I, _}, {0, Acc}) ->
                                     {Spacing, riak_core_ring:transfer_node(I, Node, Acc)};
                                (_, {S, Acc}) ->
                                     {S-1, Acc}
                             end,
                             {Spacing, riak_core_ring:transfer_node(First, Node, Ring)},
                             OwnList),
            NewRing;
        Mine ->
            %% node already has claims - respect them
            %% pick biggest hole & sit in the middle
            %% rebalance will cure any mistake on the next pass
            claim_hole(Ring, Mine, CurrentOwners, Node)
    end.

%% @private
find_biggest_hole(Mine) ->
    lists:foldl(fun({I0, I1}, none) ->
                        {I0, I1};
                   ({I0, I1}, {C0, C1}) when I0 < I1->
                        %% start-middle of the ring
                        if I1-I0 > C1-C0 ->
                                {I0, I1};
                           true ->
                                {C0, C1}
                        end;
                   ({I0, I1}, {C0, C1}) ->
                        %% wrap-around end-start of the ring
                        Span = I1+trunc(math:pow(2, 160))-1-I0,
                        if Span > C1-C0 ->
                                {I0, I1};
                           true ->
                                {C0, C1}
                        end
                end,
                none,
                lists:zip(Mine, tl(Mine)++[hd(Mine)])).

%% @private
%%
%% @doc Determines indices that violate the given target_n spacing
%% property.
find_violations(Ring, TargetN) ->
    Owners = riak_core_ring:all_owners(Ring),
    Suffix = lists:sublist(Owners, TargetN-1),
    Owners2 = Owners ++ Suffix,
    %% Use a sliding window to determine violations
    {Bad, _} = lists:foldl(fun(P={Idx, Owner}, {Out, Window}) ->
                                   Window2 = lists:sublist([P|Window], TargetN-1),
                                   case lists:keyfind(Owner, 2, Window) of
                                       {PrevIdx, Owner} ->
                                           {[[PrevIdx, Idx] | Out], Window2};
                                       false ->
                                           {Out, Window2}
                                   end
                           end, {[], []}, Owners2),
    lists:reverse(Bad).

%% @private
%%
%% @doc Counts up the number of partitions owned by each node.
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

%% @private
add_default_deltas(IdxOwners, Deltas, Default) ->
    {_, Owners} = lists:unzip(IdxOwners),
    Owners2 = lists:usort(Owners),
    Defaults = [{Member, Default} || Member <- Owners2],
    lists:ukeysort(1, Deltas ++ Defaults).

%% @private
get_expected_partitions(Ring, Node) ->
    riak_core_ring:num_partitions(Ring) div get_member_count(Ring, Node).

%% @private
get_member_count(Ring, Node) ->
    %% Determine how many nodes are involved with the ring; if the requested
    %% node is not yet part of the ring, include it in the count.
    AllMembers = riak_core_ring:claiming_members(Ring),
    case lists:member(Node, AllMembers) of
        true ->
            length(AllMembers);
        false ->
            length(AllMembers) + 1
    end.

%% @private
%%
%% @doc Filter out candidate indices that would violate target_n given
%% a node's current partition ownership.
prefilter_violations(Ring, Node, AllIndices, Indices, TargetN, RingSize) ->
    CurrentIndices = riak_core_ring:indices(Ring, Node),
    CurrentNth = [lists:keyfind(Idx, 2, AllIndices) || Idx <- CurrentIndices],
    [{Nth, Idx} || {Nth, Idx} <- Indices,
                   lists:all(fun({CNth, _}) ->
                                     spaced_by_n(CNth, Nth, TargetN, RingSize)
                             end, CurrentNth)].

%% @private
%%
%% @doc Select indices from a given candidate set, according to two
%% goals.
%%
%% 1. Ensure greedy/local target_n spacing between indices. Note that this
%%    goal intentionally does not reject overall target_n violations.
%%
%% 2. Select indices based on the delta between current ownership and
%%    expected ownership. In other words, if A owns 5 partitions and
%%    the desired ownership is 3, then we try to claim at most 2 partitions
%%    from A.
select_indices(_Owners, _Deltas, [], _TargetN, _RingSize) ->
    [];
select_indices(Owners, Deltas, Indices, TargetN, RingSize) ->
    OwnerDT = dict:from_list(Owners),
    {FirstNth, _} = hd(Indices),
    %% The `First' symbol indicates whether or not this is the first
    %% partition to be claimed by this node.  This assumes that the
    %% node doesn't already own any partitions.  In that case it is
    %% _always_ safe to claim the first partition that another owner
    %% is willing to part with.  It's the subsequent partitions
    %% claimed by this node that must not break the target_n invariant.
    {Claim, _, _, _} =
        lists:foldl(fun({Nth, Idx}, {Out, LastNth, DeltaDT, First}) ->
                            Owner = dict:fetch(Idx, OwnerDT),
                            Delta = dict:fetch(Owner, DeltaDT),
                            MeetsTN = spaced_by_n(LastNth, Nth, TargetN,
                                                  RingSize),
                            case (Delta < 0) and (First or MeetsTN) of
                                true ->
                                    NextDeltaDT =
                                        dict:update_counter(Owner, 1, DeltaDT),
                                    {[Idx|Out], Nth, NextDeltaDT, false};
                                false ->
                                    {Out, LastNth, DeltaDT, First}
                            end
                    end,
                    {[], FirstNth, dict:from_list(Deltas), true},
                    Indices),
    lists:reverse(Claim).

%% @private
%%
%% @doc Determine if two positions in the ring meet target_n spacing.
spaced_by_n(NthA, NthB, TargetN, RingSize) ->
    case NthA > NthB of
        true ->
            NFwd = NthA - NthB,
            NBack = NthB - NthA + RingSize;
        false ->
            NFwd = NthA - NthB + RingSize,
            NBack = NthB - NthA
    end,
    (NFwd >= TargetN) and (NBack >= TargetN).

%% @private
%%
%% @doc Build node info list from Wants and Owners.  
build_nis(Wants, Owners) ->
    Initial = [{N, orddict:new()} || {N, _W} <- Wants],
    {_, Ownership} = lists:foldl(fun(N, {I,A}) ->
                                         {I+1, orddict:append_list(N, [I], A)}
                                 end, {0, Initial}, Owners),
    [{Node, Want, Owned} || {Node, Want} <- Wants, {Node1, Owned} <- Ownership, Node == Node1].

%% For each node in wants, work out how many more partition each node wants (positive) or is
%% overloaded by (negative) compared to what it owns.
wants_owns_diff(Wants, Owns) ->
    [ case lists:keyfind(N, 1, Owns) of
          {N, O} ->
              {N, W - O};
          false ->
              {N,W}
      end || {N, W} <- Wants ].
    
%% Given a ring, work out how many partition each wants to be
%% considered balanced
wants(Ring) ->
    Active = lists:sort(riak_core_ring:claiming_members(Ring)),
    Inactive = riak_core_ring:all_members(Ring) -- Active,
    Q = riak_core_ring:num_partitions(Ring),
    ActiveWants = lists:zip(Active, wants_counts(length(Active), Q)),
    InactiveWants = [ {N, 0} || N <- Inactive ],
    lists:sort(ActiveWants ++ InactiveWants).

%% @private
%% Given a number of nodes and ring size, return a list of 
%% desired ownership, S long that add up to Q
wants_counts(S, Q) ->
    Max = roundup(Q / S),
    case S * Max - Q of 
        0 ->
            lists:duplicate(S, Max);
        X ->
            lists:duplicate(X, Max - 1) ++ lists:duplicate(S - X, Max)
    end.

%% Round up to next whole integer - ceil
roundup(I) when I >= 0 ->
    T = erlang:trunc(I),
    case (I - T) of
        Neg when Neg < 0 -> T;
        Pos when Pos > 0 -> T + 1;
        _ -> T
    end.

%% @private Evaluate a list of plans and return the best.
evaluate_plans(Plans, Wants, Q, TN) ->
    {_, FOM} =
        lists:foldl(fun(Plan, {Trial, {_RunningOwners, RunningMetrics}=T}) ->
                            OM = {_Owners, Metrics} = score_plan(Plan, Wants, Q, TN),
                            case better_plan(Metrics, RunningMetrics) of
                                true ->
                                    lager:debug("Claim3: Trial ~p found better plan: ~p\n",
                                                [Trial, Metrics]),
                                    {Trial + 1, OM};
                                _ ->
                                    {Trial + 1, T}
                            end
                    end, {1, {undefined, undefined}}, Plans),
    FOM.

%% @private
%% Return true if plan P1 is better than plan P2, assumes the metrics
%% are ordered [{violations, Violations}, {balance, Balance}, {diversity, Diversity}]}.
%%
better_plan(_M1, undefined) ->
    true; %% M1 is a better plan than no plan
better_plan(M1, M2) ->
    %% For now, the values we want are in the order we care about, make sure we
    %% get minimum violations, best balance, best diversity (0 is best)
    V1 = lists:unzip(M1),
    V2 = lists:unzip(M2),
    V1 < V2.

%% @private
%% Score the plan - return a tuple of {Owners, Metrics}
%% where metrics scores violations, balance and diversity
score_plan(NIs, Wants, Q, TN) ->
    Owners = make_owners(NIs),
    AM = riak_core_claim_util:adjacency_matrix(Owners),
    Diversity = riak_core_claim_util:score_am(AM, TN),

    Balance = balance(Wants, NIs),

    %% TODO: Change this to an exact count of violations.
    %% This is the list of claimable violations per-node
    %% so will over-count.  The scoring algorithm
    %% works as any violations will give a non-zero count
    %% and will need to be resolved before any balance or
    %% diversity scores are accounted for.
    Violations = length(lists:flatten([Vs || {_, _, _, Vs} <- violations(NIs, Q, TN),
                                             length(Vs) > 0])),

    {Owners, [{violations, Violations}, {balance, Balance}, {diversity, Diversity}]}.

%% @private
%% Convert a nodeinfo list with Nodes and lists of owned indices to an
%% ownership list
make_owners(NIs) ->
    IdxNodes = [ [ {Idx, Node} || Idx <- Idxs] || {Node, _Want, Idxs} <- NIs],
    [Owner || {_, Owner} <- lists:sort(lists:flatten(IdxNodes))].

%% @private
%% Compute the balance score - sum of squared difference from desired count
balance(Wants, NIs) ->
    lists:sum([begin
                   {Node, Want, Idxs} = lists:keyfind(Node, 1, NIs),
                   Diff = Want - length(Idxs),
                   Diff * Diff
               end || {Node, Want} <- Wants]).

%% @private 
%% Make the number of plans requested
make_plans(NumPlans, NIs, Q, TN) ->
    lists:usort([make_plan(NIs, Q, TN) || _ <- lists:seq(1,NumPlans)]).

%% @private
%% Make a plan to meet the Wants in the NodeInfos
%% First resovle any violations, then resolve any overloads
make_plan(NIs, Q, TN) ->
    %% Make a list of all indices that violate target N
    %% and allow other nodes to take them until they hit
    %% the number they want.  Violating nodes should
    %% give up to their violation count.
    VExchs = violations(NIs, Q, TN),
    NIs1 = take_idxs(VExchs, NIs, Q, TN),

    %% Make a list of indices from overloaded nodes
    OLExchs = overloads(NIs1),
    FinalNIs = take_idxs(OLExchs, NIs1, Q, TN),

    %% TODO: Add step to allow minor perturbations of the ring to improve
    %% diversity
    lists:sort(FinalNIs).

%% @private
%% Return a list of exchanges that resolves indices in violation
violations(NIs, Q, TN) ->
    NodeViolations = [{Node, indices_within_n(Idxs, Q, TN)} || {Node, _Want, Idxs} <- NIs],

    VIdxs = ordsets:from_list(lists:flatten([CIdxs || {_, CIdxs} <- NodeViolations])),
    [begin
         Give = length(V),
         Take = gt0(Want - length(Idxs)),
         {Node, Give, Take, VIdxs} 
     end || {Node, Want, Idxs} <- NIs, {Node1, V} <- NodeViolations, Node == Node1].

%% @private
%% Return a list of exchanges to fix overloads 
overloads(NIs) ->
    OLIdxs = ordsets:from_list(lists:flatten([Idxs || {_Node, Want, Idxs} <- NIs,
                                                      length(Idxs) > Want])),
    [begin
         Give = gt0(length(Idxs) - Want),
         Take = gt0(Want - length(Idxs)),
         case Take of
             0 ->
                 {Node, Give, Take, []};
             _ ->
                 {Node, Give, Take, OLIdxs}
         end
     end || {Node, Want, Idxs} <- NIs].

%% @private
%% Given a list of Exchanges of the form [{Node, #Give, #Take, ClaimableIdxs}]
%% randomly select from exchanges until there are no more nodes that wish to take
%% indices that can.  Update the owned indexes in the provided NodeInfos 
%% of the form [{Node, Want, OwnedIdxs]}
%% 
take_idxs(Exchanges, NIs, Q, TN) ->
    %% work out globally unavailable indexes from nodes that do not wish
    %% to give any indices- find OIdxs for all exchanges with give=0
    GUIdxs = ordsets:from_list(
               lists:flatten(
                 [OIdxs || {Node, 0, _Take, _CIdxs} <- Exchanges,
                           {Node1, _Want, OIdxs} <- NIs,
                           Node == Node1])),
    %% Remove any exchanges in GUIdxs or that would violate TN for the node 
    Exchanges1 = [{Node, Give, Take, remove_unclaimable(CIdxs, GUIdxs, Node, NIs, Q, TN)} || 
                     {Node, Give, Take, CIdxs} <- Exchanges],

    %% Recursively take indices until all takes are satisfied
    take_idxs0(Exchanges1, NIs, Q, TN).
    
take_idxs0(Exchanges, NIs, Q, TN) ->
    %% Pick a recipient for a claimed index
    case [{Node, CIdxs} || {Node, _Give, Take, CIdxs} <- Exchanges, Take > 0, CIdxs /= []] of
        [] ->
            NIs;
        Takers ->
            {TNode, TCIdxs} = random_el(Takers),
            CIdx = random_el(TCIdxs),

            %% Find the owner of CIdx and remove it from the giving node owned idxs in NIs
            [ {GNode, GWant, GOIdxs} ] = [ T || {_Node, _GWant, GIdxs}=T <- NIs,
                                                ordsets:is_element(CIdx, GIdxs) ],
            NIs1 = lists:keyreplace(GNode, 1, NIs, 
                                    {GNode, GWant, ordsets:del_element(CIdx, GOIdxs)}),

            %% Add CIdx to owned indices in NIs
            {TNode, TWant, TOIdxs} = lists:keyfind(TNode, 1, NIs1),
            NIs2 = lists:keyreplace(TNode, 1, NIs1, 
                                    {TNode, TWant, ordsets:add_element(CIdx, TOIdxs)}),

            %% If the Give count is zero in the recipients it has given up all it is prepared
            %% to, so remove all idxs owned by the give node from other claimable indices in
            %% the recipients.
            %% Also remove the indices within TN of CIdx from the TakeNode
            {GNode, GGive, _GTake, _GCIdxs} = lists:keyfind(GNode, 1, Exchanges),

            %% Update the recipients list, removing any nodes that have taken the 
            %% number they requested from the recipients list, and removing the
            %% indices owned by any nodes that have given all they wanted.

            UIdxs = case GGive - 1 > 0 of % unclaimable indices
                        true -> %% Still idxs to give, just remove claimed idx
                            [CIdx];
                        false ->
                            GOIdxs
                    end,
            %% Indexes unclaiamble by the take node
            TUIdxs = ordsets:union(UIdxs, 
                                   ordsets:from_list(expand_idx(CIdx, Q, TN))),

            Exchanges2 = lists:foldl(
                            fun({Node, Give, Take, CIdxs}, Acc) when Node == TNode ->
                                    [{TNode, Give, Take - 1, ordsets:subtract(CIdxs, TUIdxs)} | Acc];
                               ({Node, Give, Take, CIdxs}, Acc) when Node == GNode ->
                                    [{GNode, Give - 1, Take, ordsets:subtract(CIdxs, UIdxs)} | Acc];
                               ({Node, Give, Take, CIdxs}, Acc) ->
                                    [{Node, Give, Take, ordsets:subtract(CIdxs, UIdxs)} | Acc]
                            end, [], Exchanges),
            %% TODO: Consider removing reverse, not necessary for algorithm
            take_idxs0(lists:reverse(Exchanges2), NIs2, Q, TN)
    end.

%% @private
%% expand Idx by TN-1 in each direction
expand_idx(Idx, Q, TN) ->
    [X rem Q || X <- lists:seq(Q + Idx - (TN - 1), Q + Idx + TN - 1)].

%% @private
%% Remove unclaimable indexes from CIdxs that are in GUIdxs or within TN of indices
%% owned by Node from CIdxs
remove_unclaimable(CIdxs, GUIdxs, Node, NIs, Q, TN) ->
    {_Node, _Want, OIdxs} = lists:keyfind(Node, 1, NIs),
    NUIdxs = ordsets:from_list(
               lists:flatten([ [(Q + Idx + Off) rem Q,
                                (Q + Idx - Off) rem Q] || Idx <- OIdxs,
                                                          Off <- lists:seq(1, TN - 1) ])),
    ordsets:subtract(ordsets:subtract(CIdxs, NUIdxs), GUIdxs).

%% @private 
%% Return the value if greater than zero, otherwise zero
gt0(I) when I > 0 ->
    I;
gt0(_) ->
    0.

%% @private 
%% Pick a random element from the list
random_el(L) ->
    lists:nth(urand(length(L)), L).

%% @private 
%% Return a random number between Low, High inclusive
%% Abstract away choice of random number generation
urand(High) ->
    urand(1, High).

urand(Low, High) ->
    Low + random:uniform(High - Low + 1) - 1.

%% @private
%% return all the indices within TN of Indices
indices_within_n([], _Q, _TN) ->
    [];
indices_within_n([_I], _Q, _TN) ->
    [];
indices_within_n(Indices, Q, TN) ->
    indices_within_n(Indices, TN, lists:last(Indices), Q, []).

indices_within_n([], _TN, _Last, _Q, Acc) ->
    lists:usort(Acc);
indices_within_n([This | Indices], TN, Last, Q, Acc) ->
    Acc1 = case circular_distance(Last, This, Q) < TN of
               true ->
                   [Last, This| Acc];
               false ->
                   Acc
           end,
    indices_within_n(Indices, TN, This, Q, Acc1).

%% @private
%% Circular distance, indices start at 0
%% Distance of 0, 1 == 1
%% [a,b,c,a,b,c] - distance of a apart distance(0, 3) == 3
circular_distance(I1, I2, Q) ->
    min((Q + I1 - I2) rem Q, (Q + I2 - I1) rem Q).

%% ===================================================================
%% Unit tests
%% ===================================================================
-ifdef(TEST).

wants_claim_test() ->
    riak_core_ring_manager:setup_ets(test),
    riak_core_test_util:setup_mockring1(),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ?assertEqual({yes, 1}, default_wants_claim(Ring)),
    riak_core_ring_manager:cleanup_ets(test),
    riak_core_ring_manager:stop().

find_biggest_hole_test() ->
    Max = trunc(math:pow(2, 160)),
    Part16 = Max/16,

    %% single partition claimed
    ?assertEqual({Part16*5, Part16*5},
                 find_biggest_hole([Part16*5])),
    
    %% simple hole is in the middle
    ?assertEqual({Part16*3, Part16*13},
                 find_biggest_hole([Part16*3, Part16*13])),
    %% complex hole in the middle
    ?assertEqual({Part16*5, Part16*10},
                 find_biggest_hole([Part16*3, Part16*5,
                                    Part16*10, Part16*15])),
    
    %% simple hole is around the end
    ?assertEqual({Part16*10, Part16*8},
                 find_biggest_hole([Part16*8, Part16*10])),
    %% complex hole is around the end
    ?assertEqual({Part16*13, Part16*3},
                 find_biggest_hole([Part16*3, Part16*7,
                                    Part16*10, Part16*13])).

-ifdef(EQC).


-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-define(POW_2(N), trunc(math:pow(2, N))).

test_nodes(Count) ->
    [node() | [list_to_atom(lists:concat(["n_", N])) || N <- lists:seq(1, Count-1)]].

prop_claim_ensures_unique_nodes_v2_test_() ->
    Prop = eqc:numtests(100, ?QC_OUT(prop_claim_ensures_unique_nodes(choose_claim_v2))),
    {timeout, 120, fun() -> ?assert(eqc:quickcheck(Prop)) end}.

prop_claim_ensures_unique_nodes_v3_test_() ->
    Prop = eqc:numtests(5, ?QC_OUT(prop_claim_ensures_unique_nodes(choose_claim_v3))),
    {timeout, 240, fun() -> ?assert(eqc:quickcheck(Prop)) end}.

prop_claim_ensures_unique_nodes(ChooseFun) ->
    %% NOTE: We know that this doesn't work for the case of {_, 3}.
    ?FORALL({PartsPow, NodeCount}, {choose(4,9), choose(4,15)}, %{choose(4, 9), choose(4, 15)},
            begin
                Nval = 3,
                TNval = Nval + 1,
                Params = [{target_n_val, TNval}],

                Partitions = ?POW_2(PartsPow),
                [Node0 | RestNodes] = test_nodes(NodeCount),

                R0 = riak_core_ring:fresh(Partitions, Node0),
                Rfinal = lists:foldl(fun(Node, Racc) ->
                                             Racc0 = riak_core_ring:add_member(Node0, Racc, Node),
                                             ?MODULE:ChooseFun(Racc0, Node, Params)
                                     end, R0, RestNodes),

                Preflists = riak_core_ring:all_preflists(Rfinal, Nval),
                Counts = orddict:to_list(
                           lists:foldl(fun(PL,Acc) ->
                                               PLNodes = lists:usort([N || {_,N} <- PL]),
                                               case length(PLNodes) of
                                                   Nval ->
                                                       Acc;
                                                   _ ->
                                                       ordsets:add_element(PL, Acc)
                                               end
                                       end, [], Preflists)),
                ?WHENFAIL(
                   begin
                       io:format(user, "{Partitions, Nodes} {~p, ~p}~n",
                                 [Partitions, NodeCount]),
                       io:format(user, "Owners: ~p~n",
                                 [riak_core_ring:all_owners(Rfinal)])
                   end,
                   conjunction([{meets_target_n,
                                 equals({true,[]},
                                        meets_target_n(Rfinal, TNval))},
                                {unique_nodes, equals([], Counts)}]))
            end).

wants_counts_test() ->
    ?assert(eqc:quickcheck(?QC_OUT((prop_wants_counts())))).

prop_wants_counts() ->
    ?FORALL({S, Q}, {large_pos(100), large_pos(100000)},
            begin
                Wants = wants_counts(S, Q),
                conjunction([{len, equals(S, length(Wants))},
                             {sum, equals(Q, lists:sum(Wants))}])
            end).

wants_test() ->
    ?assert(eqc:quickcheck(?QC_OUT((prop_wants())))).

prop_wants() ->
    ?FORALL({NodeStatus, Q},
            {?SUCHTHAT(L, non_empty(list(elements([leaving, joining]))),
                       lists:member(joining, L)),
             ?LET(X, choose(1,16), trunc(math:pow(2, X)))},
            begin
                R0 = riak_core_ring:fresh(Q, tnode(1)),
                {_, R2, Active} = 
                    lists:foldl(
                      fun(S, {I, R1, A1}) ->
                              N = tnode(I),
                              case S of
                                  joining ->
                                      {I+1, riak_core_ring:add_member(N, R1, N), [N|A1]};
                                  _ ->
                                      {I+1, riak_core_ring:leave_member(N, R1, N), A1}
                              end
                      end, {1, R0, []}, NodeStatus),
                Wants = wants(R2),
                
                %% Check any non-claiming nodes are set to 0
                %% Check all nodes are present
                {ActiveWants, InactiveWants} = 
                    lists:partition(fun({N,_W}) -> lists:member(N, Active) end, Wants),
                                                 
                ActiveSum = lists:sum([W || {_,W} <- ActiveWants]),
                InactiveSum = lists:sum([W || {_,W} <- InactiveWants]),
                ?WHENFAIL(
                   begin
                       io:format(user, "NodeStatus: ~p\n", [NodeStatus]),
                       io:format(user, "Active: ~p\n", [Active]),
                       io:format(user, "Q: ~p\n", [Q]),
                       io:format(user, "Wants: ~p\n", [Wants]),
                       io:format(user, "ActiveWants: ~p\n", [ActiveWants]),
                       io:format(user, "InactiveWants: ~p\n", [InactiveWants])
                   end,
                   conjunction([{wants, equals(length(Wants), length(NodeStatus))},
                                {active, equals(Q, ActiveSum)},
                                {inactive, equals(0, InactiveSum)}]))
            end).
             
%% Large positive integer between 1 and Max
large_pos(Max) ->
    ?LET(X, largeint(), 1 + (abs(X) rem Max)).

take_idxs_test() ->
    ?assert(eqc:quickcheck(?QC_OUT((prop_take_idxs())))).

prop_take_idxs() ->
    ?FORALL({OwnersSeed, CIdxsSeed, ExchangesSeed, TNSeed},
            {non_empty(list(largeint())),  % [OwnerSeed]
             non_empty(list(largeint())),  % [CIdxSeed]
             non_empty(list({int(), int()})), % {GiveSeed, TakeSeed}
             int()}, % TNSeed
            begin
                %% Generate Nis - duplicate owners seed to make sure Q > S
                S = length(ExchangesSeed),
                Dup = roundup(S / length(OwnersSeed)),
                Owners = lists:flatten(
                           lists:duplicate(Dup, 
                                           [tnode(abs(OwnerSeed) rem S) || 
                                               OwnerSeed <- OwnersSeed])),
                Q = length(Owners),
                TN = 1+abs(TNSeed),
                
               
                Ownership0 = orddict:from_list([{tnode(I), []} || I <- lists:seq(0, S -1)]),
                Ownership = lists:foldl(fun({I,O},A) ->
                                                orddict:append_list(O, [I], A)
                                        end, 
                                        Ownership0,
                                        lists:zip(lists:seq(0, Q-1), Owners)),
                NIs = [{Node, undefined, Owned} || {Node, Owned} <- Ownership],

                %% Generate claimable indices
                CIdxs = ordsets:from_list([abs(Idx) rem Q || Idx <- CIdxsSeed]),
                
                %% io:format(user, "ExchangesSeed (~p): ~p\n", [length(ExchangesSeed),
                %%                                              ExchangesSeed]),
                %% io:format(user, "NIs (~p): ~p\n", [length(NIs), NIs]),

                %% Generate exchanges
                Exchanges = [{Node,  % node name
                              abs(GiveSeed) rem (length(OIdxs) + 1), % maximum indices to give
                              abs(TakeSeed) rem (Q+1), % maximum indices to take
                              CIdxs} || % indices that can be claimed by node
                                {{Node, _Want, OIdxs}, {GiveSeed, TakeSeed}} <- 
                                    lists:zip(NIs, ExchangesSeed)],

                %% Fire the test
                NIs2 = take_idxs(Exchanges, NIs, Q, TN),
                
                %% Check All nodes are still in NIs
                %% Check that no node lost more than it wanted to give
                ?WHENFAIL(
                   begin
                       io:format(user, "Exchanges:\n~p\n", [Exchanges]),
                       io:format(user, "NIs:\n~p\n", [NIs]),
                       io:format(user, "NIs2:\n~p\n", [NIs2]),
                       io:format(user, "Q: ~p\nTN: ~p\n", [Q, TN])
                   end,
                   check_deltas(Exchanges, NIs, NIs2, Q, TN))
                   %% conjunction([{len, equals(length(NIs), length(NIs2))},
                   %%              {delta, check_deltas(Exchanges, NIs, NIs2, Q, TN)}]))
            end).

tnode(I) ->
    list_to_atom("n"++integer_to_list(I)).

%% Check that no node gained more than it wanted to take
%% Check that none of the nodes took more partitions than allowed
%% Check that no nodes violate target N
check_deltas(Exchanges, Before, After, Q, TN) ->
    conjunction(
      lists:flatten(
        [begin
             Gave = length(OIdxs1 -- OIdxs2), % in original and not new
             Took = length(OIdxs2 -- OIdxs1),
             V1 = count_violations(OIdxs1, Q, TN),
             V2 = count_violations(OIdxs2, Q, TN),
             [{{give, Node, Gave, Give}, Gave =< Give},
              {{take, Node, Took, Take}, Took =< Take},
              {{valid, Node, V1, V2},
               V2 == 0 orelse
               V1 > 0 orelse % check no violations if there were not before
               OIdxs1 == []}] % or the node held no indices so violation was impossible
         end || {{Node, Give, Take, _CIdxs}, {Node, _Want1, OIdxs1}, {Node, _Want2, OIdxs2}} <-
                    lists:zip3(lists:sort(Exchanges), lists:sort(Before), lists:sort(After))])).

count_violations([], _Q, _TN) ->
    0;
count_violations(Idxs, Q, TN) ->
    SOIdxs = lists:sort(Idxs),
    {_, Violations} = lists:foldl(
                        fun(This,{Last,Vs}) ->
                                case Last - This >= TN of
                                    true ->
                                        {This, Vs};
                                    _ ->
                                        {This, Vs + 1}
                                end
                        end, {Q + hd(SOIdxs), 0}, lists:reverse(SOIdxs)),
    Violations.

-endif. % EQC
-endif. % TEST
