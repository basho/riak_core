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
%% default is 3.

-module(riak_core_claim).
-export([default_wants_claim/1, default_choose_claim/1,
         never_wants_claim/1, random_choose_claim/1]).
-export([default_choose_claim/2,
         claim_rebalance_n/2]).

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @spec default_wants_claim(riak_core_ring()) -> {yes, integer()} | no
%% @doc Want a partition if we currently have less than floor(ringsize/nodes).
default_wants_claim(Ring) ->
    default_wants_claim(Ring, node()).

default_wants_claim(Ring, Node) ->
    %% Determine how many nodes are involved with the ring; if the requested
    %% node is not yet part of the ring, include it in the count.
    AllMembers = riak_core_ring:all_members(Ring),
    case lists:member(Node, AllMembers) of
        true ->
            Mval = length(AllMembers);
        false ->
            Mval = length(AllMembers) + 1
    end,

    %% Calculate the expected # of partitions for a perfectly balanced ring. Use
    %% this expectation to determine the relative balance of the ring. If the
    %% ring isn't within +-2 partitions on all nodes, we need to rebalance.
    ExpParts = riak_core_ring:num_partitions(Ring) div Mval,
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

%% @spec default_choose_claim(riak_core_ring()) -> riak_core_ring()
%% @doc Choose a partition at random.
default_choose_claim(Ring) ->
    default_choose_claim(Ring, node()).

default_choose_claim(Ring, Node) ->
    TargetN = app_helper:get_env(riak_core, target_n_val),
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

claim_with_n_met(Ring, TailViolations, Node) ->
    CurrentOwners = lists:keysort(1, riak_core_ring:all_owners(Ring)),
    Nodes = lists:usort([Node|riak_core_ring:all_members(Ring)]),
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

claim_rebalance_n(Ring, Node) ->
    %% diagonal stripes guarantee most disperse data
    Nodes = lists:usort([Node|riak_core_ring:all_members(Ring)]),
    Partitions = lists:sort([ I || {I, _} <- riak_core_ring:all_owners(Ring) ]),
    Zipped = lists:zip(Partitions,
                       lists:sublist(
                         lists:flatten(
                           lists:duplicate(
                             1+(length(Partitions) div length(Nodes)),
                             Nodes)),
                         1, length(Partitions))),
    lists:foldl(fun({P, N}, Acc) ->
                        riak_core_ring:transfer_node(P, N, Acc)
                end,
                Ring,
                Zipped).

random_choose_claim(Ring) ->
    riak_core_ring:transfer_node(riak_core_ring:random_other_index(Ring),
                            node(), Ring).

%% @spec never_wants_claim(riak_core_ring()) -> no
%% @doc For use by nodes that should not claim any partitions.
never_wants_claim(_) -> no.


%% ===================================================================
%% Unit tests
%% ===================================================================
-ifdef(TEST).

wants_claim_test() ->
    %% riak_core_ring_events:start_link(),
    %% riak_core_ring_manager:start_link(test),
    riak_core_test_util:setup_mockring1(),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ?assertEqual(no, default_wants_claim(Ring)),
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

prop_claim_ensures_unique_nodes_test_() ->
    Prop = eqc:numtests(500, ?QC_OUT(prop_claim_ensures_unique_nodes())),
    {timeout, 120, fun() -> ?assert(eqc:quickcheck(Prop)) end}.

prop_claim_ensures_unique_nodes() ->
    %% NOTE: We know that this doesn't work for the case of {_, 3}.
    ?FORALL({PartsPow, NodeCount}, {choose(4, 9), choose(4, 15)},
            begin
                Nval = 3,
                application:set_env(riak_core, target_n_val, Nval + 1),

                Partitions = ?POW_2(PartsPow),
                [Node0 | RestNodes] = test_nodes(NodeCount),

                R0 = riak_core_ring:fresh(Partitions, Node0),
                Rfinal = lists:foldl(fun(Node, Racc) ->
                                             default_choose_claim(Racc, Node)
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
                ?assertEqual([], Counts),
                true
            end).

-endif. % EQC
-endif. % TEST
