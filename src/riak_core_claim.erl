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
         wants/1, wants_owns_diff/2, meets_target_n/2, diagonal_stripe/2,
         sequential_claim/2, get_counts/2]).
-export([remove_from_cluster/3]).

-ifdef(TEST).
-compile(export_all).
-ifdef(EQC).
-export([prop_claim_ensures_unique_nodes/1, prop_wants/0, prop_wants_counts/0,eqc_check/2,
         prop_claim_ensures_unique_nodes_v2/0, % prop_claim_ensures_unique_nodes_v3/0,
         prop_take_idxs/0]).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(DEF_TARGET_N, 4).

-type delta() :: {node(), Ownership::non_neg_integer(), Delta::integer()}.
-type deltas() :: [delta()].

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
        {yes, NumToClaim} ->
            UpdRing =
                case NumToClaim of
                    location_change ->
                        riak_core_ring:clear_location_changed(Ring);
                    _ ->
                        Ring
                end,
            NewRing = 
                case Choose of
                    {CMod, CFun} ->
                        CMod:CFun(UpdRing, Node);
                    {CMod, CFun, Params} ->
                        CMod:CFun(UpdRing, Node, Params)
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
    case riak_core_ring:legacy_ring(Ring) of
        true ->
            choose_claim_v1(Ring, Node);
        false ->
            choose_claim_v2(Ring, Node)
    end.

default_choose_claim(Ring, Node, Params) ->
    case riak_core_ring:legacy_ring(Ring) of
        true ->
            choose_claim_v1(Ring, Node, Params);
        false ->
            choose_claim_v2(Ring, Node, Params)
    end.

-spec default_wants_claim(
    riak_core_ring:riak_core_ring()) ->
        no|{yes, location_change|non_neg_integer()}.
%% @doc Want a partition if we currently have less than floor(ringsize/nodes).
default_wants_claim(Ring) ->
    default_wants_claim(Ring, node()).

-spec default_wants_claim(
    riak_core_ring:riak_core_ring(), node()) ->
        no|{yes, location_change|non_neg_integer()}.
default_wants_claim(Ring, Node) ->
    case riak_core_ring:legacy_ring(Ring) of
        true ->
            wants_claim_v1(Ring, Node);
        false ->
            wants_claim_v2(Ring, Node)
    end.

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

-spec wants_claim_v2(
    riak_core_ring:riak_core_ring()) ->
        no|{yes, location_change|non_neg_integer()}.
wants_claim_v2(Ring) ->
    wants_claim_v2(Ring, node()).

-spec wants_claim_v2(
    riak_core_ring:riak_core_ring(), node()) ->
        no|{yes, location_change|non_neg_integer()}. 
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
            case riak_core_ring:has_location_changed(Ring) of
              true ->
                {yes, location_change};
              false ->
                no
            end;
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
    %% Active::[node()]
    Active = riak_core_ring:claiming_members(Ring),
    %% Owners::[{index(), node()}]
    Owners = riak_core_ring:all_owners(Ring),
    %% Ownerships ::[node(), non_neg_integer()]
    Ownerships = get_counts(Active, Owners),
    RingSize = riak_core_ring:num_partitions(Ring),
    NodeCount = erlang:length(Active),
    %% Deltas::[node(), integer()]
    Deltas = get_deltas(RingSize, NodeCount, Owners, Ownerships),
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
            Violated =
                lists:flatten(
                    find_node_violations(Ring, TargetN)
                    ++ find_location_violations(Ring, TargetN)),
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
            ExpandedLocation = get_nodes_by_location(Expanded, Ring),
            PreferredClaim = riak_core_claim:diagonal_stripe(Ring, ExpandedLocation),
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
    Indices2 =
        prefilter_violations(
            Ring, Node, AllIndices, Indices, TargetN, RingSize),
    %% Claim indices from the remaining candidate set
    Claim2 = 
        case select_indices(Owners, Deltas, Indices2, TargetN, RingSize) of
            [] -> [];
            Claim ->  lists:sublist(Claim, Want)
        end,
    NewRing =
        lists:foldl(
            fun(Idx, Ring0) ->
                riak_core_ring:transfer_node(Idx, Node, Ring0)
            end,
            Ring,
            Claim2),

    RingChanged = ([] /= Claim2),
    RingMeetsTargetN = meets_target_n(NewRing, TargetN),
    case {RingChanged, EnoughNodes, RingMeetsTargetN} of
        {false, _, _} ->
            %% Unable to claim, fallback to re-diagonalization
            sequential_claim(Ring, Node, TargetN);
        {_, true, false} ->
            %% Failed to meet target_n, fallback to re-diagonalization
            sequential_claim(Ring, Node, TargetN);
        _ ->
            NewRing
    end.

%% @private for each node in owners return a tuple of owner and delta
%% where delta is an integer that expresses how many nodes the owner
%% needs it's ownership to change by. A positive means the owner needs
%% that many more partitions, a negative means the owner can lose that
%% many paritions.
-spec get_deltas(RingSize::pos_integer(),
                 NodeCount::pos_integer(),
                 Owners::[{Index::non_neg_integer(), node()}],
                 Ownerships::[{node(), non_neg_integer()}]) ->
                        [{node(), integer()}].
get_deltas(RingSize, NodeCount, Owners, Ownerships) ->
    Avg = RingSize / NodeCount,
    %% the most any node should own
    Max = ceiling(Avg),
    ActiveDeltas = [{Member, Ownership, normalise_delta(Avg - Ownership)}
                    || {Member, Ownership} <- Ownerships],
    BalancedDeltas = rebalance_deltas(ActiveDeltas, Max, RingSize),
    add_default_deltas(Owners, BalancedDeltas, 0).

%% @private a node can only claim whole partitions, but if RingSize
%% rem NodeCount /= 0, a delta will be a float. This function decides
%% if that float should be floored or ceilinged
-spec normalise_delta(float()) -> integer().
normalise_delta(Delta) when Delta < 0 ->
    %% if the node has too many (a negative delta) give up the most
    %% you can (will be rebalanced)
    ceiling(abs(Delta)) * -1;
normalise_delta(Delta) ->
    %% if the node wants partitions, ask for the fewest for least
    %% movement
    trunc(Delta).

%% @private so that we don't end up with an imbalanced ring where one
%% node has more vnodes than it should (e.g. [{n1, 6}, {n2, 6}, {n3,
%% 6}, {n4, 8}, {n5,6}] we rebalance the deltas so that select_indices
%% doesn't leave some node not giving up enough partitions
-spec rebalance_deltas(deltas(),
                       Max::pos_integer(), RingSize::pos_integer()) ->
                              [{node(), integer()}].
rebalance_deltas(ActiveDeltas, Max, RingSize) ->
    AppliedDeltas = [Own + Delta || {_, Own, Delta} <- ActiveDeltas],

    case lists:sum(AppliedDeltas) - RingSize of
        0 ->
            [{Node, Delta} || {Node, _Cnt, Delta} <- ActiveDeltas];
        N when N < 0 ->
            increase_keeps(ActiveDeltas, N, Max, [])
    end.

%% @private increases the delta for (some) nodes giving away
%% partitions to the max they can keep
-spec increase_keeps(deltas(),
                     WantsError::integer(),
                     Max::pos_integer(),
                     Acc:: deltas() | []) ->
                            [{node(), integer()}].
increase_keeps(Rest, 0, _Max, Acc) ->
    [{Node, Delta} || {Node, _Own, Delta} <- lists:usort(lists:append(Rest, Acc))];
increase_keeps([], N, Max, Acc) when N < 0 ->
    increase_takes(lists:reverse(Acc), N, Max, []);
increase_keeps([{Node, Own, Delta} | Rest], N, Max, Acc) when Delta < 0 ->
    WouldOwn = Own + Delta,
    Additive = case WouldOwn +1 =< Max of
                   true -> 1;
                   false -> 0
               end,
    increase_keeps(Rest, N+Additive, Max, [{Node, Own, Delta+Additive} | Acc]);
increase_keeps([NodeDelta | Rest], N, Max, Acc) ->
    increase_keeps(Rest, N, Max, [NodeDelta | Acc]).

%% @private increases the delta for (some) nodes taking partitions to the max
%% they can ask for
-spec increase_takes(deltas(),
                      WantsError::integer(),
                      Max::pos_integer(),
                      Acc::deltas() | []) ->
                             Rebalanced::[{node(), integer()}].
increase_takes(Rest, 0, _Max, Acc) ->
    [{Node, Delta} || {Node, _Own, Delta} <- lists:usort(lists:append(Rest, Acc))];
increase_takes([], N, _Max, Acc) when N < 0 ->
    [{Node, Delta} || {Node, _Own, Delta} <- lists:usort(Acc)];
increase_takes([{Node, Own, Delta} | Rest], N, Max, Acc) when Delta > 0 ->
    WouldOwn = Own + Delta,
    Additive = case WouldOwn +1 =< Max of
                   true -> 1;
                   false -> 0
               end,
    increase_takes(Rest, N+Additive, Max, [{Node, Own, Delta+Additive} | Acc]);
increase_takes([NodeDelta | Rest], N, Max, Acc) ->
    increase_takes(Rest, N, Max, [NodeDelta | Acc]).

    
meets_target_n(Ring, TargetN) ->
    Owners = lists:keysort(1, riak_core_ring:all_owners(Ring)),
    meets_target_n(Owners, TargetN, 0, [], []).

meets_target_n([{Part, Node}|Rest], TargetN, Index, First, Last) ->
    case lists:keytake(Node, 1, Last) of
        {value, {Node, LastIndex, _}, NewLast} ->
            if Index - LastIndex >= TargetN ->
                %% node repeat respects TargetN
                meets_target_n(Rest, TargetN, Index + 1, First,
                                [{Node, Index, Part}|NewLast]);
            true ->
                %% violation of TargetN
                false
        end;
        false ->
            %% haven't seen this node yet
            meets_target_n(Rest, TargetN, Index + 1,
                            [{Node, Index}|First], [{Node, Index, Part}|Last])
    end;
meets_target_n([], TargetN, Index, First, Last) ->
    %% start through end guarantees TargetN
    %% compute violations at wrap around, but don't fail
    %% because of them: handle during reclaim
    Violations = 
        lists:filter(
            fun({Node, L, _}) ->
                {Node, F} = proplists:lookup(Node, First),
                if ((Index - L) + F) < TargetN ->
                        true;
                    true ->
                        false
                end
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
    OldSeedState = rand:export_seed(),
    _ = rand:seed(exs64, proplists:get_value(seed, Params, {1,2,3})),
    {NewOwners, NewMetrics} = claim_v3(Wants, Owners, Params),
    case OldSeedState of
        undefined ->
            ok;
        _ ->
            _ = rand:seed(OldSeedState),
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

sequential_claim(Ring, Node) ->
    TN = app_helper:get_env(riak_core, target_n_val, ?DEF_TARGET_N),
    sequential_claim(Ring, Node, TN).

%% @private fall back to diagonal striping vnodes across nodes in a
%% sequential round robin (eg n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3
%% etc) However, different to `claim_rebalance_n', this function
%% attempts to eliminate tail violations (for example a ring that
%% starts/ends n1 | n2 | ...| n3 | n4 | n1)
-spec sequential_claim(
    riak_core_ring:riak_core_ring(), node(), integer()) ->
        riak_core_ring:riak_core_ring().
sequential_claim(Ring0, Node, TargetN) ->
    Ring = riak_core_ring:upgrade(Ring0),
    OrigNodes = lists:usort([Node|riak_core_ring:claiming_members(Ring)]),
    Nodes = get_nodes_by_location(OrigNodes, Ring),
    NodeCount = length(Nodes),
    RingSize = riak_core_ring:num_partitions(Ring),

    Overhang = RingSize rem NodeCount,
    HasTailViolation = (Overhang > 0 andalso Overhang < TargetN),
    Shortfall = TargetN - Overhang,
    SolveableNodeViolation =
        solveable_violation(RingSize, NodeCount, TargetN, Shortfall)
        and HasTailViolation,

    LocationsSupported =
        riak_core_location:support_locations_claim(Ring, TargetN),
    {SolveableLocationViolation, LocationShortfall} =
        case {LocationsSupported, Overhang, RingSize div NodeCount} of
            {true, OH, Loops} when OH > 0, OH > TargetN, Loops > 1 ->
                MinDistance =
                    check_for_location_tail_violation(
                        Nodes, Ring, OH, TargetN),
                case MinDistance of
                    MD when MD =< TargetN ->
                        SLV = 
                            solveable_violation(
                                RingSize, NodeCount, TargetN, TargetN - MD),
                        {SLV, TargetN - MD};
                    _ ->
                        {false, 0}
                end;
            _ ->
                {false, 0}
        end,
    
    Partitions = lists:sort([ I || {I, _} <- riak_core_ring:all_owners(Ring) ]),
    Zipped = 
        case {SolveableLocationViolation, SolveableNodeViolation} of
            {true, _} ->
                Nodelist =
                    solve_tail_violations(RingSize, Nodes, LocationShortfall),
                lists:zip(Partitions, Nodelist);
            {_, true} ->
                Nodelist =
                    solve_tail_violations(RingSize, Nodes, Shortfall),
                lists:zip(Partitions, Nodelist);
            _ ->
                diagonal_stripe(Ring, Nodes)
        end,

    lists:foldl(
        fun({P, N}, Acc) -> riak_core_ring:transfer_node(P, N, Acc) end,
        Ring,
        Zipped).


-spec check_for_location_tail_violation(
    list(node()),
    riak_core_ring:riak_core_ring(),
    pos_integer(),
    pos_integer()) -> pos_integer().
check_for_location_tail_violation(Nodes, Ring, OH, TargetN) ->
    LastNodes = lists:sublist(Nodes, 1 + OH - TargetN, TargetN),
    FirstNodes = lists:sublist(Nodes, TargetN),
    LocationD = riak_core_ring:get_nodes_locations(Ring),
    LocationFinder =
        fun(N) -> riak_core_location:get_node_location(N, LocationD) end,
    LastLocations = lists:map(LocationFinder, LastNodes),
    FirstLocations =
        lists:zip(
            lists:map(LocationFinder, FirstNodes),
            lists:seq(0, TargetN - 1)),
    {MinDistance, _} =
        lists:foldl(
            fun(L, {MinStep, TailStep}) ->
                case lists:keyfind(L, 1, FirstLocations) of
                    {L, N} ->
                        {min(TailStep + N, MinStep), TailStep - 1};
                    false ->
                        {MinStep, TailStep - 1}
                end
            end,
            {TargetN, TargetN - 1},
            LastLocations),
    MinDistance.


-spec solveable_violation(
    pos_integer(), pos_integer(), pos_integer(), pos_integer()) -> boolean().
solveable_violation(RingSize, NodeCount, TargetN, Shortfall) ->
    case RingSize div NodeCount of
        LoopCount when LoopCount >= Shortfall ->
            true;
        LoopCount ->
            SplitSize = Shortfall div LoopCount,
            BiggestTake = Shortfall - ((LoopCount - 1) * SplitSize),
            (NodeCount - BiggestTake) >= TargetN
    end.

%% @doc
%% The node list mosut be of length ring size.  It is made up of a set of
%% complete loops of the node list, and then a partial loop with the addition
%% of the shortfall.  The for each node in the shortfall a node in the complete
%% loops must be removed
-spec solve_tail_violations(
    pos_integer(), [node()], non_neg_integer()) -> [[node()]].
solve_tail_violations(RingSize, Nodes, Shortfall) ->
    {LastLoop, Remainder} = 
        lists:split(RingSize rem length(Nodes), Nodes),
    ExcessLoop = lists:sublist(Remainder, Shortfall),
    Tail = LastLoop ++ ExcessLoop,
    LoopCount = RingSize div length(Nodes),
    RemoveList =
        divide_list_for_removes(lists:reverse(ExcessLoop), LoopCount),
    CompleteLoops =
        lists:append(
            lists:duplicate(LoopCount - length(RemoveList), Nodes)),
    PartialLoops =
        lists:map(
            fun(ENL) -> lists:subtract(Nodes, ENL) end, 
            RemoveList),
    CompleteLoops ++ lists:append(PartialLoops) ++ Tail.

%% @doc 
%% Normally need to remove one of the excess nodes each loop around the node
%% list.  However, if there are not enough loops, more than one can be removed
%% per loop - assuming the solveable_violation/4 condition passes (i.e. this
%% will not breach the TargetN).
-spec divide_list_for_removes(list(node()), pos_integer())
        -> list(list(node())).
divide_list_for_removes(Excess, LoopCount) when LoopCount >= length(Excess) ->
    lists:map(fun(N) -> [N] end, Excess);
divide_list_for_removes(Excess, 1) ->
    [Excess];
divide_list_for_removes(Excess, LoopCount) ->
    FetchesPerLoop = length(Excess) div LoopCount,
    LastFetch = length(Excess) - FetchesPerLoop * (LoopCount - 1),
    {[], GroupedFetches} =
        lists:foldl(
            fun(FC, {ENs, GroupedENs}) ->
                {NextGroup, Remainder} = lists:split(FC, ENs),
                {Remainder, GroupedENs ++ [NextGroup]}
            end,
            {Excess, []},
            lists:duplicate(LoopCount - 1, FetchesPerLoop) ++ [LastFetch]
        ),
    GroupedFetches.


%% @private every module has a ceiling function
-spec ceiling(float()) -> integer().
ceiling(F) ->
    T = trunc(F),
    case F - T == 0 of
        true ->
            T;
        false ->
            T + 1
    end.

claim_rebalance_n(Ring0, Node) ->
    Ring = riak_core_ring:upgrade(Ring0),
    OrigNodes = lists:usort([Node|riak_core_ring:claiming_members(Ring)]),
    Nodes = get_nodes_by_location(OrigNodes, Ring),
    Zipped = diagonal_stripe(Ring, Nodes),

    lists:foldl(fun({P, N}, Acc) ->
                        riak_core_ring:transfer_node(P, N, Acc)
                end,
                Ring,
                Zipped).

diagonal_stripe(Ring, Nodes) ->
    %% diagonal stripes guarantee most disperse data
    Partitions = lists:sort([ I || {I, _} <- riak_core_ring:all_owners(Ring) ]),
    Zipped = 
        lists:zip(
            Partitions,
            lists:sublist(
                lists:flatten(
                    lists:duplicate(
                        1 + (length(Partitions) div length(Nodes)), Nodes)),
                    1,
                    length(Partitions))),
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
%% Cluster leave operations
%% ===================================================================

remove_from_cluster(Ring, ExitingNode, Seed) ->
    % Transfer indexes to other nodes...
    Owners = riak_core_ring:all_owners(Ring),
    Members = riak_core_ring:claiming_members(Ring),
    ExitRing =
        case attempt_simple_transfer(Ring, ExitingNode, Seed,
                                        Owners, Members) of
            {ok, NR} ->
                NR;
            _ ->
                %% re-diagonalize
                %% first hand off all claims to *any* one else,
                %% just so rebalance doesn't include exiting node
                HN = hd(lists:delete(ExitingNode, Members)),
                TempRing =
                    lists:foldl(fun({I,N}, R) when N == ExitingNode ->
                                        riak_core_ring:transfer_node(I, HN, R);
                                    (_, R) ->
                                        R
                                end,
                                Ring,
                                Owners),
                riak_core_claim:sequential_claim(TempRing, HN)
        end,
    ExitRing.

-ifdef(TEST).
-type transfer_ring() :: [{integer(), term()}].
-else.
-type transfer_ring() :: riak_core_ring:riak_core_ring().
-endif.

%% @doc Simple transfer of leaving node's vnodes to safe place
%% Where safe place is any node that satisfies target_n_val for that vnode -
%% but with a preference to transfer to a node that has a lower number of 
%% vnodes currently allocated.
%% If safe places cannot be found for all vnodes returns `target_n_fail`
%% Simple transfer is not location aware, but generally this wll be an initial
%% phase of a plan, and hence a temporary home - so location awareness is not
%% necessary.
%% `riak_core.full_rebalance_onleave = true` may be used to avoid this step,
%% although this may result in a large number of transfers
-spec attempt_simple_transfer(transfer_ring(),
                                term(),
                                random:ran(),
                                [{integer(), term()}],
                                [term()]) ->
                                    {ok, transfer_ring()}|
                                        target_n_fail|
                                        force_rebalance.
attempt_simple_transfer(Ring, ExitingNode, Seed, Owners, Members) ->
    ForceRebalance =
        app_helper:get_env(riak_core, full_rebalance_onleave, false),
    case ForceRebalance of
        true ->
            force_rebalance;
        false ->
            TargetN = app_helper:get_env(riak_core, target_n_val),
            Counts =
                riak_core_claim:get_counts(Members, Owners),
            RingFun = 
                fun(Partition, Node, R) ->
                    riak_core_ring:transfer_node(Partition, Node, R),
                    R
                end,
            simple_transfer(Owners,
                            {RingFun, TargetN, ExitingNode},
                            Ring,
                            {Seed, [], Counts})
    end.

%% @doc Simple transfer of leaving node's vnodes to safe place
%% Iterates over Owners, which must be sorted by Index (from 0...), and
%% attempts to safely re-allocate each ownerhsip which is currently set to
%% the exiting node
-spec simple_transfer([{integer(), term()}],
                        {fun((integer(),
                                term(),
                                transfer_ring()) -> transfer_ring()),
                            pos_integer(),
                            term()},
                        transfer_ring(),
                        {random:ran(),
                            [{integer(), term()}],
                            [{term(), non_neg_integer()}]}) ->
                                {ok, transfer_ring()}|target_n_fail.
simple_transfer([{P, ExitingNode}|Rest],
                        {RingFun, TargetN, ExitingNode},
                        Ring,
                        {Seed, Prev, Counts}) ->
    %% The ring is split into two parts:
    %% Rest - this is forward looking from the current partition, in partition
    %% order (ascending by partition number)
    %% Prev - this is the part of the ring that has already been processed, 
    %% which is also in partition order (but descending by index number)
    %%
    %% With a ring size of 8, having looped to partition 3:
    %% Rest = [{4, N4}, {5, N5}, {6, N6}, {7, N7}]
    %% Prev = [{2, N2}, {1, N1}, {0, N0}]
    %%
    %% If we have a partition that is on the Exiting Node it is necessary to
    %% look forward (TargetN - 1) allocations in Rest.  It is also necessary
    %% to look backward (TargetN - 1) allocations in Prev (from the rear of the
    %% Prev list).
    %%
    %% This must be treated as a Ring though - as we reach an end of the list
    %% the search must wrap around to the other end of the alternate list (i.e.
    %% from 0 -> 7 and from 7 -> 0).
    CheckRingFun =
        fun(ForwardL, BackL) ->
            Steps = TargetN - 1,
            UnsafeNodeTuples =
                case length(ForwardL) of 
                    L when L < Steps ->
                        ForwardL ++
                            lists:sublist(lists:reverse(BackL), Steps - L);
                    _ ->
                        lists:sublist(ForwardL, Steps)
                end,
            fun({Node, _Count}) ->
                %% Nodes will remian as candidates if they are not in the list
                %% of unsafe nodes
                not lists:keymember(Node, 2, UnsafeNodeTuples)
            end
        end,
    %% Filter candidate Nodes looking back in the ring at previous allocations.
    %% The starting list of candidates is the list the claiming members in
    %% Counts.
    CandidatesB = lists:filter(CheckRingFun(Prev, Rest), Counts),
    %% Filter candidate Nodes looking forward in the ring at existing
    %% allocations
    CandidatesF = lists:filter(CheckRingFun(Rest, Prev), CandidatesB),

    %% Qualifying candidates will be tuples of {Node, Count} where the Count
    %% is that node's current count of allocated vnodes
    case CandidatesF of
        [] ->
            target_n_fail;
        Qualifiers ->
            %% Look at the current allocated vnode counts for each qualifying
            %% node, and find all qualifying nodes with the lowest of these
            %% counts
            [{Q0, BestCnt}|Others] = lists:keysort(2, Qualifiers),
            PreferredCandidates =
                [{Q0, BestCnt}|
                    lists:takewhile(fun({_, C}) -> C == BestCnt end, Others)],
            
            %% Final selection of a node as a destination for this partition,
            %% The node Counts must be updated to reflect this allocation, and
            %% the RingFun applied to actually queue the transfer
            {Rand, Seed2} = rand:uniform_s(length(PreferredCandidates), Seed),
            {Chosen, BestCnt} = lists:nth(Rand, PreferredCandidates),
            UpdRing = RingFun(P, Chosen, Ring),
            UpdCounts =
                lists:keyreplace(Chosen, 1, Counts, {Chosen, BestCnt + 1}),
            simple_transfer(Rest,
                            {RingFun, TargetN, ExitingNode},
                            UpdRing,
                            {Seed2, [{P, Chosen}|Prev], UpdCounts})
    end;
simple_transfer([{P, N}|Rest], Statics, Ring, {Seed, Prev, Counts}) ->
    %% This is already allocated to a node other than the exiting node, so
    %% simply transition to the Previous ring accumulator
    simple_transfer(Rest, Statics, Ring, {Seed, [{P, N}|Prev], Counts});
simple_transfer([], _Statics, Ring, _LoopAccs) ->
    {ok, Ring}.


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
-spec find_node_violations(
    riak_core_ring:riak_core_ring(), pos_integer()) ->
        list({non_neg_integer(), non_neg_integer()}).
find_node_violations(Ring, TargetN) ->
    Owners = riak_core_ring:all_owners(Ring),
    find_violations(Owners, TargetN).

-spec find_location_violations(
    riak_core_ring:riak_core_ring(), pos_integer()) ->
        list({non_neg_integer(), non_neg_integer()}).
find_location_violations(Ring, TargetN) ->
    case riak_core_location:support_locations_claim(Ring, TargetN) of
        true ->
            find_violations(
                riak_core_location:get_location_owners(Ring), TargetN);
        false ->
            []
    end.

-spec find_violations(
    list({non_neg_integer(), atom()}), pos_integer()) ->
        list({non_neg_integer(), non_neg_integer()}).
find_violations(Owners, TargetN) ->
    Suffix = lists:sublist(Owners, TargetN-1),
    %% Add owners at the front to the tail, to confirm no tail violations
    OwnersWithTail = Owners ++ Suffix,
    %% Use a sliding window to determine violations
    {Bad, _} =
        lists:foldl(
            fun(P={Idx, Owner}, {Out, Window}) ->
                Window2 = lists:sublist([P|Window], TargetN-1),
                case lists:keyfind(Owner, 2, Window) of
                    {PrevIdx, Owner} ->
                        {[[PrevIdx, Idx] | Out], Window2};
                    false ->
                        {Out, Window2}
                end
            end,
            {[], []},
            OwnersWithTail),
    lists:reverse(Bad).

%% @private
%%
%% @doc Counts up the number of partitions owned by each node.
-spec get_counts([node()], [{non_neg_integer(), node()}]) ->
                        [{node(), pos_integer()}].
get_counts(Nodes, PartitionOwners) ->
    Empty = [{Node, 0} || Node <- Nodes],
    Counts = lists:foldl(fun({_Idx, Node}, Counts) ->
                                 case lists:member(Node, Nodes) of
                                     true ->
                                         dict:update_counter(Node, 1, Counts);
                                     false ->
                                         Counts
                                 end
                         end, dict:from_list(Empty), PartitionOwners),
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
    LocalNodes =
        case riak_core_location:support_locations_claim(Ring, TargetN) of
            true ->
                [Node|riak_core_location:local_nodes(Ring, Node)];
            false ->
                [Node]
        end,
    CurrentIndices =
        lists:usort(
            lists:flatten(
                lists:map(
                    fun(N) -> riak_core_ring:indices(Ring, N) end,
                    LocalNodes))),
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
        lists:foldl(
            fun({Nth, Idx}, {Out, LastNth, DeltaDT, First}) ->
                Owner = dict:fetch(Idx, OwnerDT),
                Delta = dict:fetch(Owner, DeltaDT),
                MeetsTN = spaced_by_n(LastNth, Nth, TargetN, RingSize),
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
    Low + rand:uniform(High - Low + 1) - 1.

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

%% @private
%% Get active nodes ordered by taking location parameters into account
-spec get_nodes_by_location([node()|undefined], riak_core_ring:riak_core_ring()) ->
  [node()|undefined].
get_nodes_by_location(Nodes, Ring) ->
    NodesLocations = riak_core_ring:get_nodes_locations(Ring),
    case riak_core_location:has_location_set_in_cluster(NodesLocations) of
        false ->
            Nodes;
        true ->
            LocationNodesD =
                riak_core_location:get_location_nodes(Nodes, NodesLocations),
            stripe_nodes_by_location(LocationNodesD)
    end.

-spec stripe_nodes_by_location(dict:dict()) -> list(node()|undefined).
stripe_nodes_by_location(NodesByLocation) ->
    [LNodes|RestLNodes] =
        sort_lists_by_length(
            lists:map(fun({_L, NL}) -> NL end, dict:to_list(NodesByLocation))),
    stripe_nodes_by_location(RestLNodes, lists:map(fun(N) -> [N] end, LNodes)).

stripe_nodes_by_location([], Acc) ->
    lists:flatten(Acc);
stripe_nodes_by_location([LNodes|OtherLNodes], Acc) ->
    SortedAcc = sort_lists_by_length(Acc),
    {UpdatedAcc, []} =
        lists:mapfoldl(
            fun(NodeList, LocationNodesToAdd) ->
                case LocationNodesToAdd of
                    [NodeToAdd|TailNodes] ->
                        {NodeList ++ [NodeToAdd], TailNodes};
                    [] ->
                        {NodeList, []}
                end
            end,
            LNodes,
            SortedAcc),
    stripe_nodes_by_location(OtherLNodes, UpdatedAcc).

sort_lists_by_length(ListOfLists) ->
    lists:sort(fun(L1, L2) -> length(L1) > length(L2) end, ListOfLists).

%% ===================================================================
%% Unit tests
%% ===================================================================

-ifdef(TEST).



test_ring_fun(P, N, R) ->
    lists:keyreplace(P, 1, R, {P, N}).

count_nodes(TestRing) ->
    CountFun =
        fun({_P, N}, Acc) ->
            case lists:keyfind(N, 1, Acc) of
                false ->
                    lists:ukeysort(1, [{N, 1}|Acc]);
                {N, C} ->
                    lists:ukeysort(1, [{N, C + 1}|Acc])
            end
        end,
    lists:foldl(CountFun, [], TestRing).

simple_transfer_simple_test() ->
    R0 = [{0, n5}, {1, n1}, {2, n2}, {3, n3},
            {4, n4}, {5, n5}, {6, n3}, {7, n2}],
    SomeTime = {1632,989499,279637},
    FixedSeed = rand:seed(exrop, SomeTime),
    {ok, R1} =
        simple_transfer(R0,
                        {fun test_ring_fun/3, 3, n4},
                        R0,
                        {FixedSeed,
                            [],
                            lists:keydelete(n4, 1, count_nodes(R0))}),
    ?assertMatch({4, n1}, lists:keyfind(4, 1, R1)),
    
    {ok, R2} =
        simple_transfer(R0,
                        {fun test_ring_fun/3, 3, n5},
                        R0,
                        {FixedSeed,
                            [],
                            lists:keydelete(n5, 1, count_nodes(R0))}),
    ?assertMatch({0, n4}, lists:keyfind(0, 1, R2)),
    ?assertMatch({5, n1}, lists:keyfind(5, 1, R2)),

    {ok, R3} =
        simple_transfer(R0,
                        {fun test_ring_fun/3, 3, n1},
                        R0,
                        {FixedSeed,
                            [],
                            lists:keydelete(n1, 1, count_nodes(R0))}),
    ?assertMatch({1, n4}, lists:keyfind(1, 1, R3)),

    target_n_fail =
        simple_transfer(R0,
                        {fun test_ring_fun/3, 3, n3},
                        R0,
                        {FixedSeed,
                            [],
                            lists:keydelete(n3, 1, count_nodes(R0))}),
    
    target_n_fail =
        simple_transfer(R0,
                        {fun test_ring_fun/3, 3, n2},
                        R0,
                        {FixedSeed,
                            [],
                            lists:keydelete(n2, 1, count_nodes(R0))}),
    
    %% Target n failures due to wrap-around tail violations
    R4 = [{0, n5}, {1, n1}, {2, n2}, {3, n3},
            {4, n4}, {5, n2}, {6, n3}, {7, n4}],
    
    target_n_fail =
        simple_transfer(R4,
                        {fun test_ring_fun/3, 3, n5},
                        R4,
                        {FixedSeed,
                            [],
                            lists:keydelete(n5, 1, count_nodes(R4))}),
    
    target_n_fail =
        simple_transfer(R4,
                        {fun test_ring_fun/3, 3, n4},
                        R4,
                        {FixedSeed,
                            [],
                            lists:keydelete(n4, 1, count_nodes(R4))}).

simple_transfer_needstobesorted_test() ->
    lists:foreach(fun transfer_needstobesorted_tester/1, lists:seq(1, 100)).

transfer_needstobesorted_tester(I) ->
    R0 = [{6,n3}, {13,n3}, {12,n6}, {11,n5}, {10,n4}, {9,n3}, {8,n2},
            {7,n1}, {5,n6}, {4,n5}, {3,n4}, {2,n3}, {1,n2}, {0,n1}],
    VariableSeed = rand:seed(exrop, {1632, 989499, I * 13}),
    {ok, R1} =
        simple_transfer(lists:keysort(1, R0),
                        {fun test_ring_fun/3, 3, n3},
                        R0,
                        {VariableSeed,
                            [],
                            lists:keydelete(n3, 1, count_nodes(R0))}),
    ?assertMatch({13, n4}, lists:keyfind(13, 1, R1)).

simple_transfer_evendistribution_test() ->
    R0 = [{0, n1}, {1, n2}, {2, n3}, {3, n4}, {4, n5}, 
            {5, n6}, {6, n7}, {7, n8}, {8, n9}, {9, n10},
            {10, n1}, {11, n2}, {12, n3}, {13, n4}, {14, n5},
            {15, n6}, {16, n7}, {17, n8}, {18, n9}, {19, n10},
            {20, n1}, {21, n2}, {22, n3}, {23, n4}, {24, n5}, 
            {25, n6}, {26, n7}, {27, n8}, {28, n9}, {29, n10},
            {30, n1}, {31, n2}, {32, n3}, {33, n4}, {34, n5},
            {35, n6}, {36, n7}, {37, n8}, {38, n9}, {39, n10},
            {40, n1}, {41, n2}, {42, n3}, {43, n4}, {44, n5}, 
            {45, n6}, {46, n7}, {47, n8}, {48, n9}, {49, n10},
            {50, n1}, {51, n2}, {52, n3}, {53, n4}, {54, n5},
            {55, n6}, {56, n1}, {57, n2}, {58, n3}, {59, n10},
            {60, n5}, {61, n6}, {62, n7}, {63, n8}],
    
    SomeTime = {1632,989499,279637},
    FixedSeed = rand:seed(exrop, SomeTime),
    {ok, R1} =
        simple_transfer(R0,
                        {fun test_ring_fun/3, 3, n1},
                        R0,
                        {FixedSeed,
                            [],
                            lists:keydelete(n1, 1, count_nodes(R0))}),
    
    NodeCounts = lists:keysort(2, count_nodes(R1)),
    io:format("NodeCounts ~w~n", [NodeCounts]),
    [{_LN, LC}|Rest] = NodeCounts,
    [{_HN, HC}|_] = lists:reverse(Rest),
    true = HC - LC == 2.

wants_claim_test() ->
    riak_core_ring_manager:setup_ets(test),
    riak_core_test_util:setup_mockring1(),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ?assertEqual({yes, 1}, default_wants_claim(Ring)),
    riak_core_ring_manager:cleanup_ets(test),
    riak_core_ring_manager:stop().

location_seqclaim_t1_test() ->
    JoiningNodes =
        [{n2, loc1},
        {n3, loc2}, {n4, loc2},
        {n5, loc3}, {n6, loc3},
        {n7, loc4}, {n8, loc4},
        {n9, loc5}, {n10, loc5}
    ],
    location_claim_tester(n1, loc1, JoiningNodes, 64),
    location_claim_tester(n1, loc1, JoiningNodes, 128),
    location_claim_tester(n1, loc1, JoiningNodes, 256),
    location_claim_tester(n1, loc1, JoiningNodes, 512),
    location_claim_tester(n1, loc1, JoiningNodes, 1024),
    location_claim_tester(n1, loc1, JoiningNodes, 2048).

location_seqclaim_t2_test() ->
    JoiningNodes =
        [{n2, loc1},
            {n3, loc2}, {n4, loc2},
            {n5, loc3}, {n6, loc3},
            {n7, loc4}, {n8, loc4}
        ],
    location_claim_tester(n1, loc1, JoiningNodes, 64),
    location_claim_tester(n1, loc1, JoiningNodes, 128),
    location_claim_tester(n1, loc1, JoiningNodes, 256),
    location_claim_tester(n1, loc1, JoiningNodes, 512),
    location_claim_tester(n1, loc1, JoiningNodes, 1024),
    location_claim_tester(n1, loc1, JoiningNodes, 2048).

location_seqclaim_t3_test() ->
    JoiningNodes =
        [{n2, loc1},
            {n3, loc2}, {n4, loc2},
            {n5, loc3}, {n6, loc3},
            {n7, loc4}, {n8, loc4},
            {n9, loc5}, {n10, loc5},
            {n11, loc6}, {n12, loc7}, {n13, loc8}
        ],
    location_claim_tester(n1, loc1, JoiningNodes, 64),
    location_claim_tester(n1, loc1, JoiningNodes, 128),
    location_claim_tester(n1, loc1, JoiningNodes, 256),
    location_claim_tester(n1, loc1, JoiningNodes, 512),
    location_claim_tester(n1, loc1, JoiningNodes, 1024),
    location_claim_tester(n1, loc1, JoiningNodes, 2048).

location_seqclaim_t4_test() ->
    JoiningNodes =
        [{l1n2, loc1}, {l1n3, loc1}, {l1n4, loc1},
            {l1n5, loc1}, {l1n6, loc1}, {l1n7, loc1}, {l1n8, loc1},
            {l2n1, loc2}, {l2n2, loc2}, {l2n3, loc2}, {l2n4, loc2},
            {l2n5, loc2}, {l2n6, loc2}, {l2n7, loc2}, {l2n8, loc2},
            {l3n1, loc3}, {l3n2, loc3}, {l3n3, loc3}, {l3n4, loc3},
            {l3n5, loc3}, {l3n6, loc3}, {l3n7, loc3}, {l3n8, loc3},
            {l4n1, loc4}, {l4n2, loc4}, {l4n3, loc4}, {l4n4, loc4},
            {l4n5, loc4}, {l4n6, loc4}, {l4n7, loc4}, {l4n8, loc4},
            {l5n1, loc5}, {l5n2, loc5}, {l5n3, loc5}, {l5n4, loc5},
            {l5n5, loc5}, {l5n6, loc5}, {l5n7, loc5},
            {l6n1, loc6}, {l6n2, loc6}, {l6n3, loc6}, {l6n4, loc6},
            {l6n5, loc6}, {l6n6, loc6}, {l6n7, loc6},
            {l7n1, loc7}, {l7n2, loc7}, {l7n3, loc7}],
    location_claim_tester(l1n1, loc1, JoiningNodes, 128),
    location_claim_tester(l1n1, loc1, JoiningNodes, 256),
    location_claim_tester(l1n1, loc1, JoiningNodes, 512),
    location_claim_tester(l1n1, loc1, JoiningNodes, 1024),
    location_claim_tester(l1n1, loc1, JoiningNodes, 2048).

location_seqclaim_t5_test() ->
    JoiningNodes =
        [{l1n2, loc1}, {l1n3, loc1}, {l1n4, loc1},
            {l2n1, loc2}, {l2n2, loc2}, {l2n3, loc2}, {l2n4, loc2},
            {l3n1, loc3}, {l3n2, loc3}, {l3n3, loc3}, {l3n4, loc3},
            {l4n1, loc4}, {l4n2, loc4}, {l4n3, loc4}, {l4n4, loc4},
            {l5n1, loc5}, {l5n2, loc5}, {l5n3, loc5},
            {l6n1, loc6}, {l6n2, loc6}, {l6n3, loc6}, {l6n4, loc6},
            {l7n1, loc7}, {l7n2, loc7}],
    location_claim_tester(l1n1, loc1, JoiningNodes, 128),
    location_claim_tester(l1n1, loc1, JoiningNodes, 256),
    location_claim_tester(l1n1, loc1, JoiningNodes, 512),
    location_claim_tester(l1n1, loc1, JoiningNodes, 1024),
    location_claim_tester(l1n1, loc1, JoiningNodes, 2048).

location_seqclaim_t6_test() ->
    JoiningNodes =
        [{l1n2, loc1}, {l1n3, loc1}, {l1n4, loc1},
            {l1n5, loc1}, {l1n6, loc1}, {l1n7, loc1}, {l1n8, loc1},
            {l2n1, loc2}, {l2n2, loc2}, {l2n3, loc2}, {l2n4, loc2},
            {l2n5, loc2}, {l2n6, loc2}, {l2n7, loc2}, {l2n8, loc2},
            {l3n1, loc3}, {l3n2, loc3}, {l3n3, loc3}, {l3n4, loc3},
            {l3n5, loc3}, {l3n6, loc3}, {l3n7, loc3}, {l3n8, loc3},
            {l4n1, loc4}, {l4n2, loc4}, {l4n3, loc4}, {l4n4, loc4},
            {l4n5, loc4}, {l4n6, loc4}, {l4n7, loc4}, {l4n8, loc4},
            {l5n1, loc5}, {l5n2, loc5},
            {l6n1, loc6}, {l6n2, loc6}, {l6n3, loc6}, {l6n4, loc6},
            {l6n5, loc6}, {l6n6, loc6}, {l6n7, loc6}, {l6n8, loc8}],
    location_claim_tester(l1n1, loc1, JoiningNodes, 256),
    location_claim_tester(l1n1, loc1, JoiningNodes, 512),
    location_claim_tester(l1n1, loc1, JoiningNodes, 1024),
    location_claim_tester(l1n1, loc1, JoiningNodes, 2048).

% location_seqclaim_t7_test() ->
%     JoiningNodes =
%         [{n2, loc1},
%             {n3, loc2}, {n4, loc2},
%             {n5, loc3}, {n6, loc3},
%             {n7, loc4}, {n8, loc4},
%             {n9, loc5}
%         ],
%     location_claim_tester(n1, loc1, JoiningNodes, 64),
%     location_claim_tester(n1, loc1, JoiningNodes, 128),
%     location_claim_tester(n1, loc1, JoiningNodes, 256),
%     location_claim_tester(n1, loc1, JoiningNodes, 512),
%     location_claim_tester(n1, loc1, JoiningNodes, 1024),
%     location_claim_tester(n1, loc1, JoiningNodes, 2048).


location_claimv2_t1_test() ->
    JoiningNodes =
        [{l1n2, loc1}, {l1n3, loc1}, {l1n4, loc1}, {l1n5, loc1},
        {l2n1, loc2}, {l2n2, loc2}, {l2n3, loc2}, {l2n4, loc2}, {l2n5, loc2},
        {l3n1, loc3}, {l3n2, loc3}, {l3n3, loc3}, {l3n4, loc3}, {l3n5, loc3},
        {l4n1, loc4}, {l4n2, loc4}, {l4n3, loc4}, {l4n4, loc4}, {l4n5, loc4},
        {l5n1, loc5}, {l5n2, loc5}, {l5n3, loc5}, {l5n4, loc5},
        {l6n1, loc6}, {l6n2, loc6}, {l6n3, loc6}],
    location_claim_tester(
        l1n1, loc1, JoiningNodes, 128, sequential_claim, 3).



location_claim_tester(N1, N1Loc, NodeLocList, RingSize) ->
    location_claim_tester(
        N1, N1Loc, NodeLocList, RingSize, sequential_claim, 4).

location_claim_tester(N1, N1Loc, NodeLocList, RingSize, ClaimFun, TargetN) ->
    io:format(
        "Testing NodeList ~w with RingSize ~w~n",
        [[{N1, N1Loc}|NodeLocList], RingSize]
    ),
    riak_core_ring_manager:setup_ets(test),
    R1 = 
        riak_core_ring:set_node_location(
            N1,
            N1Loc,
            riak_core_ring:fresh(RingSize, N1)),

    RAll =
        lists:foldl(
            fun({N, L}, AccR) ->
                AccR0 = riak_core_ring:add_member(N1, AccR, N),
                riak_core_ring:set_node_location(N, L, AccR0)
            end,
            R1,
            NodeLocList
        ),

    riak_core_ring_manager:set_ring_global(RAll),
    
    Params =
        case ClaimFun of
            sequential_claim ->
                TargetN;
            choose_claim_v2 ->
                [{target_n_val, 3}]
        end,
    RClaim =
        claim(
            RAll,
            {riak_core_claim,default_wants_claim},
            {riak_core_claim, ClaimFun, Params}),
    {RingSize, Mappings} = riak_core_ring:chash(RClaim),
    NLs = riak_core_ring:get_nodes_locations(RClaim),
    LocationMap =
        lists:map(
            fun({Idx, N}) ->
                    {Idx, riak_core_location:get_node_location(N, NLs)}
            end,
            Mappings),
    Prefix = lists:sublist(LocationMap, 3),
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
    ?assert(length(Failures) == 0),
    riak_core_ring_manager:cleanup_ets(test),
    riak_core_ring_manager:stop().


divide_excess_test() ->
    ?assertMatch(
        [[n1], [n2], [n3]], divide_list_for_removes([n1, n2, n3], 3)),
    ?assertMatch(
        [[n1, n2, n3]], divide_list_for_removes([n1, n2, n3], 1)),
    ?assertMatch(
        [[n1], [n2, n3]], divide_list_for_removes([n1, n2, n3], 2)),
    ?assertMatch(
        [[n1, n2], [n3, n4]], divide_list_for_removes([n1, n2, n3, n4], 2)),
    ?assertMatch(
        [[n1, n2], [n3, n4, n5]],
            divide_list_for_removes([n1, n2, n3, n4, n5], 2)).    

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

%% @private console helper function to return node lists for claiming
%% partitions
-spec gen_diag(pos_integer(), pos_integer()) -> [Node::atom()].
gen_diag(RingSize, NodeCount) ->
    Nodes = [list_to_atom(lists:concat(["n_", N])) || N <- lists:seq(1, NodeCount)],
    {HeadNode, RestNodes} = {hd(Nodes), tl(Nodes)},
    R0 = riak_core_ring:fresh(RingSize, HeadNode),
    RAdded = lists:foldl(fun(Node, Racc) ->
                                 riak_core_ring:add_member(HeadNode, Racc, Node)
                         end,
                         R0, RestNodes),
    Diag = diagonal_stripe(RAdded, Nodes),
    {_P, N} = lists:unzip(Diag),
    N.

%% @private call with result of gen_diag/1 only, does the list have
%% tail violations, returns true if so, false otherwise.
-spec has_violations([Node::atom()]) -> boolean().
has_violations(Diag) ->
    RS = length(Diag),
    NC = length(lists:usort(Diag)),
    Overhang = RS rem NC,
    (Overhang > 0 andalso Overhang < 4). %% hardcoded target n of 4


-ifdef(EQC).


-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-define(POW_2(N), trunc(math:pow(2, N))).

eqc_check(File, Prop) ->
    {ok, Bytes} = file:read_file(File),
    CE = binary_to_term(Bytes),
    eqc:check(Prop, CE).

test_nodes(Count) ->
    [node() | [list_to_atom(lists:concat(["n_", N])) || N <- lists:seq(1, Count-1)]].

test_nodes(Count, StartNode) ->
    [list_to_atom(lists:concat(["n_", N])) || N <- lists:seq(StartNode, StartNode + Count)].

claim_ensures_unique_nodes_v2_test_() ->
    Prop = eqc:testing_time(30, ?QC_OUT(prop_claim_ensures_unique_nodes_v2())),
    {timeout, 120, fun() -> ?assert(eqc:quickcheck(Prop)) end}.

claim_ensures_unique_nodes_adding_groups_v2_test_() ->
    Prop = eqc:testing_time(30, ?QC_OUT(prop_claim_ensures_unique_nodes_adding_groups(choose_claim_v2))),
    {timeout, 120, fun() -> ?assert(eqc:quickcheck(Prop)) end}.

claim_ensures_unique_nodes_adding_singly_v2_test_() ->
    Prop = eqc:testing_time(30, ?QC_OUT(prop_claim_ensures_unique_nodes_adding_singly(choose_claim_v2))),
    {timeout, 120, fun() -> ?assert(eqc:quickcheck(Prop)) end}.

%% Run few tests in eunit and more if needed by calling "./rebar3 eqc"
% claim_ensures_unique_nodes_v3_test_() ->
%     Prop = eqc:numtests(5, ?QC_OUT(prop_claim_ensures_unique_nodes_old(choose_claim_v3))),
%     {timeout, 240, fun() -> ?assert(eqc:quickcheck(Prop)) end}.

prop_claim_ensures_unique_nodes_v2() ->
    prop_claim_ensures_unique_nodes(choose_claim_v2).

%% No longer test properties of claim_v3.
%% Although claim_v3 continues to exist as a hidden configuration option, it
%% is known to fail to meet the required properties, and claim_v2 should be
%% used in all known circumstances.
%%
%% TODO : Remove claim_v3 from the code base
%%
%% This TODO is currently deferred due to the difficulty of understanding
%% how to test the full possibility of cluster change scenarios.  Perhaps
%% there may be circumstances where a probabilistic approach to planning
%% cluster changes may still be beneficial
%%
% prop_claim_ensures_unique_nodes_v3() ->
%    prop_claim_ensures_unique_nodes(choose_claim_v3).

%% NOTE: this is a less than adequate test that has been re-instated
%% so that we don't leave the code worse than we found it. Work that
%% fixed claim_v2's tail violations and vnode balance issues did not
%% fix the same for v3, but the test that v3 ran had been updated for
%% those fixes, leaving a failing v3 test. This test is the original
%% test re-instated to pass.
prop_claim_ensures_unique_nodes_old(ChooseFun) ->
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

prop_claim_ensures_unique_nodes(ChooseFun) ->
    %% NOTE: We know that this doesn't work for the case of {_, 3}.
    %% NOTE2: uses undocumented "double_shrink", is expensive, but should get
    %% around those case where we shrink to a non-minimal case because
    %% some intermediate combinations of ring_size/node have no violations
    ?FORALL({PartsPow, NodeCount}, eqc_gen:double_shrink({choose(4, 9), choose(4, 15)}),
            begin
                Nval = 3,
                TNval = Nval + 1,
                _Params = [{target_n_val, TNval}],

                Partitions = ?POW_2(PartsPow),
                [Node0 | RestNodes] = test_nodes(NodeCount),

                R0 = riak_core_ring:fresh(Partitions, Node0),
                RAdded = lists:foldl(fun(Node, Racc) ->
                                             riak_core_ring:add_member(Node0, Racc, Node)
                                     end, R0, RestNodes),

                Rfinal = claim(RAdded, {?MODULE, wants_claim_v2}, {?MODULE, ChooseFun}),

                Preflists = riak_core_ring:all_preflists(Rfinal, Nval),
                ImperfectPLs = orddict:to_list(
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
                                {perfect_preflists, equals([], ImperfectPLs)},
                                {balanced_ring, balanced_ring(Partitions, NodeCount, Rfinal)}]))
            end).

%% @TODO this fails, we didn't fix v3
%% prop_claim_ensures_unique_nodes_adding_groups_v3_test_() ->
%%     Prop = eqc:numtests(5, ?QC_OUT(prop_claim_ensures_unique_nodes(choose_claim_v3))),
%%     {timeout, 240, fun() -> ?assert(eqc:quickcheck(Prop)) end}.

prop_claim_ensures_unique_nodes_adding_groups(ChooseFun) ->
    %% NOTE: We know that this doesn't work for the case of {_, 3}.
    %% NOTE2: uses undocumented "double_shrink", is expensive, but should get
    %% around those case where we shrink to a non-minimal case because
    %% some intermediate combinations of ring_size/node have no violations
    ?FORALL({PartsPow, BaseNodes, AddedNodes}, 
            eqc_gen:double_shrink({choose(4, 9), choose(2, 10), choose(2, 5)}),
            begin
                Nval = 3,
                TNval = Nval + 1,
                _Params = [{target_n_val, TNval}],

                Partitions = ?POW_2(PartsPow),
                [Node0 | RestNodes] = test_nodes(BaseNodes),
                AddNodes = test_nodes(AddedNodes-1, BaseNodes),
                NodeCount = BaseNodes + AddedNodes,
                %% io:format("Base: ~p~n",[[Node0 | RestNodes]]),
                %% io:format("Added: ~p~n",[AddNodes]),

                R0 = riak_core_ring:fresh(Partitions, Node0),
                RBase = lists:foldl(fun(Node, Racc) ->
                                             riak_core_ring:add_member(Node0, Racc, Node)
                                     end, R0, RestNodes),

                Rinterim = claim(RBase, {?MODULE, wants_claim_v2}, {?MODULE, ChooseFun}),
                RAdded = lists:foldl(fun(Node, Racc) ->
                                             riak_core_ring:add_member(Node0, Racc, Node)
                                     end, Rinterim, AddNodes),

                Rfinal = claim(RAdded, {?MODULE, wants_claim_v2}, {?MODULE, ChooseFun}),

                Preflists = riak_core_ring:all_preflists(Rfinal, Nval),
                ImperfectPLs = orddict:to_list(
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
                                {perfect_preflists, equals([], ImperfectPLs)},
                                {balanced_ring, balanced_ring(Partitions, NodeCount, Rfinal)}]))
            end).


%% @TODO take this out (and add issue/comment in commit) not fixed
%% prop_claim_ensures_unique_nodes_adding_singly_v3_test_() ->
%%     Prop = eqc:testing_time(30, ?QC_OUT(prop_claim_ensures_unique_nodes_adding_singly(choose_claim_v3))),
%%     {timeout, 240, fun() -> ?assert(eqc:quickcheck(Prop)) end}.

prop_claim_ensures_unique_nodes_adding_singly(ChooseFun) ->
    %% NOTE: We know that this doesn't work for the case of {_, 3}.
    %% NOTE2: uses undocumented "double_shrink", is expensive, but should get
    %% around those case where we shrink to a non-minimal case because
    %% some intermediate combinations of ring_size/node have no violations
    ?FORALL({PartsPow, NodeCount}, eqc_gen:double_shrink({choose(4, 9), choose(4, 15)}),
            begin
                Nval = 3,
                TNval = Nval + 1,
                Params = [{target_n_val, TNval}],

                Partitions = ?POW_2(PartsPow),
                [Node0 | RestNodes] = test_nodes(NodeCount),

                R0 = riak_core_ring:fresh(Partitions, Node0),
                Rfinal = lists:foldl(fun(Node, Racc) ->
                                             Racc0 = riak_core_ring:add_member(Node0, Racc, Node),
                                             %% TODO which is it? Claim or ChooseFun??
                                             %%claim(Racc0, {?MODULE, wants_claim_v2}, {?MODULE, ChooseFun})
                                             ?MODULE:ChooseFun(Racc0, Node, Params)
                                     end, R0, RestNodes),
                Preflists = riak_core_ring:all_preflists(Rfinal, Nval),
                ImperfectPLs = orddict:to_list(
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
                                {perfect_preflists, equals([], ImperfectPLs)},
                                {balanced_ring, balanced_ring(Partitions, NodeCount, Rfinal)}]))
            end).



%% @private check that no node claims more than it should
-spec balanced_ring(RingSize::integer(), NodeCount::integer(),
                    riak_core_ring:riak_core_ring()) ->
                           boolean().
balanced_ring(RingSize, NodeCount, Ring) ->
    TargetClaim = ceiling(RingSize / NodeCount),
    MinClaim = RingSize div NodeCount,
    AllOwners0 = riak_core_ring:all_owners(Ring),
    AllOwners = lists:keysort(2, AllOwners0),
    {BalancedMax, AccFinal} = lists:foldl(fun({_Part, Node}, {_Balanced, [{Node, Cnt} | Acc]}) when Cnt >= TargetClaim ->
                                             {false, [{Node, Cnt+1} | Acc]};
                                        ({_Part, Node}, {Balanced, [{Node, Cnt} | Acc]}) ->
                                             {Balanced, [{Node, Cnt+1} | Acc]};
                                        ({_Part, NewNode}, {Balanced, Acc}) ->
                                             {Balanced, [{NewNode, 1} | Acc]}
                                     end,
                                     {true, []},
                                     AllOwners),
    BalancedMin = lists:all(fun({_Node, Cnt}) -> Cnt >= MinClaim end, AccFinal),
    case BalancedMax andalso BalancedMin of
        true ->
            true;
        false ->
            {TargetClaim, MinClaim, lists:sort(AccFinal)}
    end.


wants_counts_test() ->
    ?assert(eqc:quickcheck(?QC_OUT((prop_wants_counts())))).

prop_wants_counts() ->
    ?FORALL({S, Q}, {large_pos(100), large_pos(100000)},
            begin
                Wants = wants_counts(S, Q),
                conjunction([{len, equals(S, length(Wants))},
                             {sum, equals(Q, lists:sum(Wants))}])
            end).

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
