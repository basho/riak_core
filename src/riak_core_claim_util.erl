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

%% Claim utilities

-module(riak_core_claim_util).

-export([ring_stats/2, violation_stats/2, balance_stats/1, diversity_stats/2]).
-export([print_failure_analysis/3, failure_analysis/3, node_sensitivity/3, node_load/3,
          print_analysis/1, print_analysis/2, sort_by_down_fbmax/1]).
-export([adjacency_matrix/1, summarize_am/1, adjacency_matrix_from_al/1, 
         adjacency_list/1, fixup_dam/2, score_am/2, count/2, rms/1]).
-export([make_ring/1, gen_complete_diverse/1, gen_complete_len/1, construct/3]).
-export([num_perms/2, num_combs/2, fac/1, perm_gen/1, down_combos/2, 
         rotations/1, substitutions/2]).
-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(load,    {node,    % Node name
                  num_pri, % Number of primaries
                  num_fb,  % Number of fallbacks
                  norm_fb}). % Normalised fallbacks - ratio of how many there are
                  
-record(failure, {down = [], % List of downed nodes
                  load = [], % List of #load{} records per up node
                  fbmin,
                  fbmean,
                  fbstddev,
                  fb10,
                  fb90,
                  fbmax}).

-define(DICT, dict).  % macro for dictionary implementation, simplifies debugging

          
%% -------------------------------------------------------------------
%% Ring statistics
%% -------------------------------------------------------------------

ring_stats(R, TN) ->
    violation_stats(R, TN) ++
        balance_stats(R) ++
        diversity_stats(R, TN).
    
%% TargetN violations
violation_stats(R, TN) ->
    [{violations, length(riak_core_ring_util:check_ring(R, TN))}].

balance_stats(R) ->
    Q = riak_core_ring:num_partitions(R),
    M = length(riak_core_ring:claiming_members(R)),
    AllOwners = riak_core_ring:all_owners(R),
    Counts = lists:foldl(fun({_,N},A) -> orddict:update_counter(N,1,A) end, [], AllOwners),
    Avg = Q / M,
    Balance = lists:sum([begin
                             Delta = trunc(Avg - Count),
                             Delta * Delta
                         end || {_, Count} <- Counts]),
    [{balance, Balance},
     {ownership, Counts}].

diversity_stats(R, TN) ->
    {_, Owners} = lists:unzip(riak_core_ring:all_owners(R)),
    AM = adjacency_matrix(Owners),
    try
        [{diversity, riak_core_claim_util:score_am(AM, TN)}]
    catch
        _:empty_list ->
            [{diversity, undefined}]
    end.
 
%% -------------------------------------------------------------------
%% Failure analysis
%% -------------------------------------------------------------------

%% Print failure analysis on standard_io
print_failure_analysis(R, TargetN, NumFailures) ->
    print_analysis(failure_analysis(R, TargetN, NumFailures)).

failure_analysis(R, TargetN, NumFailures) ->
    sort_by_down_fbmax(node_sensitivity(R, TargetN, NumFailures)).

%% Mark each node down in turn and see how the spread of load is.
%%
%% Return a list of failures records, one for each case examined
node_sensitivity(R, NVal, Depth) ->
    Members = riak_core_ring:all_members(R),
    DownCombos = down_combos(Depth, Members),
    LoadCombos = [{Down, node_load(R, NVal, Down)} || Down <- DownCombos],
    [analyze_load(Down, Load) || {Down, Load} <- LoadCombos].

%% For a given ring, with a list of downed nodes, compute
%% all preference lists then count up the number that
%% each node participates in.
%%
%% [{node(), Primaries, Fallbacks, PortionOfKeySpace}]
%%
node_load(R, NVal, DownNodes) ->
    VL = vnode_load(R, NVal, DownNodes),
    TotFBs = lists:sum([NumFBs || {_N,_,NumFBs} <- VL]),
    [#load{node = N,
           num_pri = NumPris,
           num_fb = NumFBs,
           norm_fb = norm_fb(NumFBs, TotFBs)} || 
        {N, NumPris, NumFBs} <- VL].

vnode_load(R, NVal, DownNodes) ->
    UpNodes = riak_core_ring:all_members(R) -- DownNodes,
    Keys = [<<(I+1):160/integer>> ||
               {I,_Owner} <- riak_core_ring:all_owners(R)],
    %% NValParts = Nval * riak_core_ring:num_partitions(R),
    AllPLs = [riak_core_apl:get_apl_ann(Key, NVal, R, UpNodes) || Key <- Keys],
    FlatPLs = lists:flatten(AllPLs),
    [begin
         Pris = lists:usort([_Idx || {{_Idx, PN}, primary} <- FlatPLs, PN == N]),
         FBs = lists:usort([_Idx || {{_Idx, FN}, fallback} <- FlatPLs, FN == N]) -- Pris,
         {N, length(Pris), length(FBs)}
     end || N <- UpNodes].

%% @private Normalize fallbacks
norm_fb(_, 0) ->
    0;
norm_fb(Num, Tot) ->
    Num / Tot.

%% @private analyze the load on each 
analyze_load(Down, Load) ->
    FBStats = lists:foldl(fun(#load{num_fb = NumFB}, Acc) ->
                                  basho_stats_histogram:update(NumFB, Acc)
                          end,
                          basho_stats_histogram:new(1, 1024, 1024), Load),
    {FBMin, FBMean, FBMax, _FBVar, FBStdDev} = basho_stats_histogram:summary_stats(FBStats),
    FB10 = basho_stats_histogram:quantile(0.10, FBStats),
    FB90 = basho_stats_histogram:quantile(0.90, FBStats),
    #failure{down = Down, load = Load, fbmin = FBMin, fbmean = FBMean, fbstddev = FBStdDev,
             fb10 = FB10, fb90 = FB90, fbmax = FBMax}.


%%
%% Print the load analysis for each of the combinations of down nodes analyzed
%%  Min/Mean/SD/10th/90th/Max - statistics of fallback vnodes started on any node
%%       given the DownNodes
%%  DownNodes - list of nodes that were down for the calculation
%%  Worst - list of {node,fallbacks} showing up to the 3 worst affected nodes
%%          as a result of DownNodes.
%% 
print_analysis(LoadAnalysis) ->
    print_analysis(standard_io, LoadAnalysis).
    
print_analysis(IoDev, LoadAnalysis) ->
    io:format(IoDev, " Min  Mean/  SD  10th  90th   Max  DownNodes/Worst\n", []),
    print_analysis1(IoDev, LoadAnalysis).

%% @private
print_analysis1(_IoDev, []) ->
    ok;
print_analysis1(IoDev, [#failure{down = Down, load = Load, fbmin = FBMin,
                          fbmean = FBMean, fbstddev = FBStdDev,
                          fb10 = FB10, fb90 = FB90, fbmax = FBMax} | Rest]) ->
    %% Find the 3 worst FBmax
    Worst = 
        [{N,NumFB} || #load{node = N, num_fb = NumFB} <-
                          lists:sublist(lists:reverse(lists:keysort(#load.num_fb, Load)), 3)],
    
    io:format(IoDev, "~4b  ~4b/~4b  ~4b  ~4b  ~4b  ~w/~w\n", 
              [FBMin, toint(FBMean), toint(FBStdDev), toint(FB10), toint(FB90), FBMax, Down, Worst]),
    print_analysis1(IoDev, Rest).

%% @private round to nearest int
toint(F) when is_number(F) ->
    round(F+0.5);
toint(X) ->
    X.

%% Order failures by number of nodes down ascending, then fbmax, then down list
sort_by_down_fbmax(Failures) ->
    Cmp = fun(#failure{down = DownA, fbmax = FBMaxA},
              #failure{down = DownB, fbmax = FBMaxB}) ->
                  %% length(DownA) =< length(DownB) andalso 
                  %%     FBMaxA >= FBMaxB andalso 
                  %%     DownA =< DownB
                  case {length(DownA), length(DownB)} of
                      {DownALen, DownBLen} when DownALen < DownBLen ->
                          true;
                      {DownALen, DownBLen} when DownALen > DownBLen ->
                          false;
                      _ ->
                          if
                              FBMaxA > FBMaxB ->
                                  true;
                              FBMaxA < FBMaxB ->
                                  false;
                              true ->
                                  DownA >= DownB
                          end
                  end
          end,
    lists:sort(Cmp, Failures).

%% -------------------------------------------------------------------
%% Adjacency calculations
%% -------------------------------------------------------------------

%% Adjacency measures are useful for preflist diversity. Three different
%% forms of measurement are used.
%%
%% An adjacency list is a list of node pairs and the distance between them
%% by pointwise measuring the distance to each of the other nodes
%% taking into account wraparounds (forward distance only).
%%
%% The ownership list a,b,c,b,a,c,b,c would compute the following adjacency
%% list
%%
%% AL = lists:sort(adjacency_list([a,b,c,b,a,c,b])).
%% [{{a,b},0}, <-- entry for first a
%%  {{a,b},1}, <-- entry for second a
%%  {{a,c},0},
%%  {{a,c},1},
%%  {{b,a},0}, <-- entry for second b
%%  {{b,a},0}, <-- entry for last b
%%  {{b,a},2}, <-- entry for first b (skipping over the second b)
%%  {{b,c},0},
%%  {{b,c},1},
%%  {{b,c},2},
%%  {{c,a},1},
%%  {{c,a},1},
%%  {{c,b},0},
%%  {{c,b},0}]
%%
%% The adjacency matrix rolls up each of the distances int a list for
%% each node pair
%%
%% lists:sort(adjacency_matrix_from_al(AL)).
%% [{{a,b},[0,1]},
%%  {{a,c},[0,1]},
%%  {{b,a},[0,0,2]},
%%  {{b,c},[0,1,2]},
%%  {{c,a},[1,1]},
%%  {{c,b},[0,0]}]
%%
%% Preflists with good diversity have equal counts of each distance
%% between nodes, so the scoring measure for adjacency is the RMS
%% of each distance, for each distance less than target N
%%
%% riak_core_claim_util:score_am(AM, 3).
%% 2.1818181818181817
%%
%% Lower scores are better.
%%

%% Create a pair of node names and a list of distances. The resulting matrix
%% is not guaranteed to be ordered by anything.
adjacency_matrix(Owners) ->
    M = lists:usort(Owners),
    Tid = ets:new(am, [private, duplicate_bag]),
    try
        adjacency_matrix_populate(Tid, M, Owners, Owners++Owners),
        adjacency_matrix_result(Tid, ets:first(Tid), [])
    after
        ets:delete(Tid)
    end.

%% @private extract the adjacency matrix from the duplicate bag
adjacency_matrix_result(_Tid, '$end_of_table', Acc) ->
    Acc;
adjacency_matrix_result(Tid, NodePair, Acc) ->
    ALs = ets:lookup(Tid, NodePair),
    Ds = [ D || {_, D} <- ALs ],
    adjacency_matrix_result(Tid, ets:next(Tid, NodePair), [{NodePair, Ds} | Acc]).

adjacency_matrix_populate(_Tid, _M, [], _OwnersCycle) ->
    ok;
adjacency_matrix_populate(Tid, M, [Node | Owners], [Node | OwnersCycle]) ->
    adjacency_matrix_add_dist(Tid, Node, M--[Node], OwnersCycle, 0),
    adjacency_matrix_populate(Tid, M, Owners, OwnersCycle).

%% @private Compute the distance from node to the next of M nodes
adjacency_matrix_add_dist(_Tid, _Node, _M, [], _) ->
    ok;
adjacency_matrix_add_dist(_Tid, _Node, [], _OwnersCycle, _) ->
    ok;
adjacency_matrix_add_dist(Tid, Node, M, [OtherNode | OwnersCycle], Distance) ->
    case lists:member(OtherNode, M) of
        true -> % haven't seen this node yet, add distance
            ets:insert(Tid, {{Node, OtherNode}, Distance}),
            adjacency_matrix_add_dist(Tid, Node, M -- [OtherNode], OwnersCycle, Distance + 1);
        _ -> % already passed OtherNode
            adjacency_matrix_add_dist(Tid, Node, M, OwnersCycle, Distance + 1)
    end.
 
%% Make adjacency summary by working out counts of each distance
%% (zero-padding to make it print nicely)
summarize_am(AM) ->
    [{NPair, count_distances(Ds)} || {NPair, Ds} <- AM].

%% Take a list of distances: [4, 3, 0, 1, 1, 3, 3] and
%% create a list counting distance by position [1, 2, 0, 3, 1]
count_distances([]) ->
    [];
count_distances(Ds) ->
    MaxD = lists:max(Ds), 
    PosCounts = lists:foldl(fun(D,Acc) ->
                                    orddict:update_counter(D, 1, Acc)
                            end, 
                            orddict:from_list([{D,0} || D <- lists:seq(0,MaxD)]),
                            Ds),
    %% PosCounts orddict must be initialized to make sure no distances
    %% are missing in the list comprehension
    [Count || {_Pos, Count} <- PosCounts]. 

%% Compute adjacency matrix from an adjacency list
adjacency_matrix_from_al(AL) ->
    %% Make a count by distance of N1,N2
    ?DICT:to_list(
       lists:foldl(fun({NPair,D}, Acc) ->
                           ?DICT:append_list(NPair, [D], Acc)
                   end, ?DICT:new(), AL)).


%% Create a pair of node names and a list of distances
adjacency_list(Owners) ->
    M = lists:usort(Owners),
    adjacency_list(M, Owners, Owners++Owners, []).

adjacency_list(_M, [], _OwnersCycle, Acc) ->
    Acc;
adjacency_list(M, [Node | Owners], [Node | OwnersCycle], Acc) ->
    adjacency_list(M, Owners, OwnersCycle, distances(Node, M--[Node], OwnersCycle, 0, Acc)).

%% Compute the distance from node to the next of M nodes
distances(_Node, _M, [], _, Distances) ->
    Distances;
distances(_Node, [], _OwnersCycle, _, Distances) ->
    Distances;
distances(Node, M, [OtherNode | OwnersCycle], Distance, Distances) ->
    case lists:member(OtherNode, M) of
        true -> % haven't seen this node yet, add distance
            distances(Node, M -- [OtherNode], OwnersCycle, Distance + 1, 
                      [{{Node, OtherNode}, Distance} | Distances]);
        _ -> % already passed OtherNode
            distances(Node, M, OwnersCycle, Distance + 1, Distances)
    end.

%% For each pair, get the count of distances < NVal
score_am([], _NVal) ->
    undefined;
score_am(AM, NVal) ->
    Cs = lists:flatten(
           [begin
                [C || {D,C} <- count(Ds, NVal), D < NVal]
            end || {_Pair,Ds} <- AM]),
    rms(Cs).

count(L, NVal) ->
    Acc0 = orddict:from_list([{D, 0} || D <- lists:seq(0, NVal-1)]),
    lists:foldl(fun(E,A) -> orddict:update_counter(E, 1, A) end, Acc0, L).
                         
rms([]) ->
    throw(empty_list);
rms(L) ->
    Mean = lists:sum(L) / length(L),
    lists:sum([(Mean - E) * (Mean - E) || E <- L]).


%% -------------------------------------------------------------------
%% Ring construction
%% -------------------------------------------------------------------

%% Make a ring of size length(Nodes) ordering the nodes as given
make_ring(Nodes) ->
    R0 = riak_core_ring:fresh(length(Nodes), hd(Nodes)),
    Idxs = [I || {I,_} <- riak_core_ring:all_owners(R0)],
    NewOwners = lists:zip(Idxs, Nodes),
    R1 = lists:foldl(fun(N,R) ->
                             riak_core_ring:add_member(hd(Nodes), R, N)
                     end, R0, Nodes),
    lists:foldl(fun({I,N}, R) ->
                        riak_core_ring:transfer_node(I, N, R)
                end, R1, NewOwners).



%% Generate a completion test function that makes sure all required
%% distances are created
gen_complete_diverse(RequiredDs) ->
    fun(Owners, DAM) ->
            OwnersLen = length(Owners),
            NextPow2 = next_pow2(OwnersLen),
            {met_required(Owners, DAM, RequiredDs) andalso
                OwnersLen == NextPow2, NextPow2}
    end.

%% Generate until a fixed length has been hit
gen_complete_len(Len) ->
    fun(Owners, _AM) ->
            {length(Owners) == Len, Len}
    end.

%% M = list of node names
%% NVal = target nval
construct(Complete, M, NVal) ->
    DAM1 = empty_adjacency_matrix(M),
    construct(Complete, M, [lists:last(M)], DAM1, NVal).

%% Make an empty adjacency matrix for all pairs of members
empty_adjacency_matrix(M) ->
    lists:foldl(fun(Pair,AM0) ->
                        ?DICT:append_list(Pair, [], AM0)
                end, ?DICT:new(), [{F,T} || F <- M, T <- M, F /= T]).

construct(Complete, M, Owners, DAM, NVal) ->
    %% Work out which pairs do not have the requiredDs
    case Complete(Owners, DAM) of
        {true, _DesiredLen}->
            {ok, Owners, DAM};
        {false, DesiredLen} ->
            %% Easy ones - restrict the eligible list to not include the N-1 
            %% previous nodes.  If within NVal-1 of possibly closing the ring
            %% then restrict in that direction as well.
            Eligible0 = M -- lists:sublist(Owners, NVal - 1),
            Eligible = case DesiredLen - length(Owners) of
                            Left when Left >= NVal ->
                                Eligible0; % At least Nval lest, no restriction
                            Left ->
                                Eligible0 -- lists:sublist(lists:reverse(Owners), NVal - Left)
                        end,
            case Eligible of
                [] ->
                    %% No eligible nodes - not enough to meet NVal, use any node
                    lager:debug("construct -- unable to construct without violating NVal"),
                    {Owners1, DAM1} = prepend_next_owner(M, M, Owners, DAM, NVal),
                    construct(Complete, M, Owners1, DAM1, NVal);
                _ ->
                    {Owners1, DAM1} = prepend_next_owner(M, Eligible, Owners, DAM, NVal),
                    construct(Complete, M, Owners1, DAM1, NVal)
            end
    end.

%% Returns true only when we have met all required distances across all
%% possible pairs in the adjacency matrix
met_required(Owners, DAM, RequiredDs) ->
    FixupDAM = fixup_dam(Owners, DAM),
    case [Pair ||  {Pair, Ds} <- ?DICT:to_list(FixupDAM), 
                   (RequiredDs -- Ds) /= [] ] of
        [] ->
            true;
        _ ->
            false
    end.

%% Return next greatest power of 2
next_pow2(X) ->
    next_pow2(X, 2).

next_pow2(X, R) when X =< R ->
    R;
next_pow2(X, R) ->
    next_pow2(X, R*2).

%% For each eligible, work out which node improves diversity the most
%% Take the AM scores and cap by TargetN and find the node that
%% improves the RMS 
prepend_next_owner(M, [Node], Owners, DAM, _TN) -> % only one node, not a lot of decisions to make
    prepend(M, Node, Owners, DAM);
prepend_next_owner(M, Eligible, Owners, DAM, TN) ->
    {_BestScore, Owners2, DAM2} = 
        lists:foldl(fun(Node, {RunningScore, _RunningO, _RunningDAM}=Acc) ->
                            {Owners1, DAM1} = prepend(M, Node, Owners, DAM),
                            case score_am(?DICT:to_list(DAM1), TN) of
                                BetterScore when BetterScore < RunningScore ->
                                    {BetterScore, Owners1, DAM1};
                                _ ->
                                Acc
                            end
                    end, {undefined, undefined, undefined}, Eligible),
    {Owners2, DAM2}.

%% Prepend N to the front of Owners, and update AM
prepend(M, N, Owners, DAM) ->
    Ds = distances2(M -- [N], Owners),
    DAM2 = lists:foldl(fun({T,D},DAM1) ->
                              ?DICT:append_list({N,T},[D],DAM1)
                      end, DAM, Ds),
    {[N | Owners], DAM2}.

%% Calculate the distances to each of the M nodes until
%% a distance for each has been found.
distances2(M, Owners) ->
    distances2(M, Owners, 0, []).

distances2([], _Owners, _D, Acc) ->
    Acc;
distances2(_M, [], _D, Acc) ->
    Acc;
distances2(M, [T | Owners], D, Acc) ->
    case lists:member(T, M) of
        true ->
            distances2(M -- [T], Owners, D + 1, [{T, D} | Acc]);
        false ->
            distances2(M, Owners, D + 1, Acc)
    end.

%% Fix up the dictionary AM adding in entries for the end of the owners list
%% wrapping around to the start.
fixup_dam(Owners, DAM) ->
    fixup_dam(lists:usort(Owners), lists:reverse(Owners), Owners, 0, DAM).

fixup_dam([], _ToFix, _Owners, _D, DAM) ->
    DAM;
fixup_dam(_M, [], _Owners, _D, DAM) ->
    DAM;
fixup_dam(M, [N | ToFix], Owners, D, DAM) ->
    M2 = M -- [N],
    Ds = distances2(M2, Owners, D, []),
    DAM2 = lists:foldl(fun({T,D0},DAM1) ->
                              ?DICT:append_list({N,T},[D0],DAM1)
                      end, DAM, Ds),
    fixup_dam(M2, ToFix, Owners, D + 1, DAM2).

%% -------------------------------------------------------------------
%% Permutations and combinations
%% -------------------------------------------------------------------

%% Permutations - number of ways to pick N out of K
num_perms(K, N) when K =< N ->
    fac(N) div (fac(N - K)).

%% Combinations - number of ways to combine N elements out of K
num_combs(K, N) when K =< N ->
    fac(N) div (K * fac(N - K)).

%% Factorials
fac(0) -> 
    1;
fac(N) when N > 0 ->
    N * fac(N-1).

%% Generate all permutations of list L
perm_gen([E]) ->
    [[E]];
perm_gen(L) ->
    lists:append([ begin
                       [ [X | Y] || Y <- perm_gen(lists:delete(X, L))]
                   end || X <- L]).
  

%% Pick all combinations of Depth nodes from the MemFbers list
%% 0 = []
%% 1 = [1], [2], [3]
%% 2 = [1,1], [1,2], [1,3], [2, 1], [2, 2], [2, 3], [3, 1], [3, 2], [3, 3]
down_combos(Depth, Members) ->
    down_combos(Depth, Members, []).

down_combos(0, _Members, Down) ->
    lists:usort([lists:usort(D) || D <- Down]);
down_combos(Depth, Members, []) ->
    down_combos(Depth - 1, Members, [[N] || N <- Members]);
down_combos(Depth, Members, Down) ->
    %% Add each of the members to the front of Down and iterate
    Down2 = [[N | D] || N <- Members, D <- Down],
    down_combos(Depth - 1, Members, Down2).


%% Generate all rotated versions of an ownership list
rotations([H|T] = L) -> 
    rotations(length(L) - 1, T ++ [H], [L]).

rotations(0, _, Acc) ->
    lists:reverse(Acc);
rotations(Rem, [H|T] = L, Acc) ->
    rotations(Rem - 1, T ++ [H], [L | Acc]).

%% Generate a list with each possible substitution for a name 
substitutions(L, Names) ->
    PNames = perm_gen(Names),
    [substitute(Names, P, L) || P <- PNames].

%% Replace elements in L looking up Name and replacing with element in Mapping
substitute(Names, Mapping, L) ->
    D = dict:from_list(lists:zip(Names, Mapping)),
    [dict:fetch(E, D) || E <- L].

%% -------------------------------------------------------------------
%% Unit Tests
%% -------------------------------------------------------------------

-ifdef(TEST).
-ifdef(EQC).

property_adjacency_summary_test_() ->
    {timeout, 60, ?_test(eqc:quickcheck(eqc:testing_time(30, prop_adjacency_summary())))}.

longer_list(K, G) ->
    ?SIZED(Size, resize(trunc(K*Size), list(resize(Size, G)))).

%% Compare directly constructing the adjacency matrix against
%% one using prepend/fixup.
prop_adjacency_summary() ->
    ?FORALL({OwnersSeed, S},
            {non_empty(longer_list(40, largeint())), ?LET(X, int(), 1 + abs(X))},
            begin
                Owners = [list_to_atom("n"++integer_to_list(1 + (abs(I) rem S))) || I <- OwnersSeed],
                AM = adjacency_matrix(Owners),
                AS = summarize_am(AM),

                {Owners2, _DAM2, FixDAM2} = build(Owners),
                AS2 = summarize_am(?DICT:to_list(FixDAM2)),


                ?WHENFAIL(
                   begin
                       io:format(user, "S=~p\nOwners =~p\n", [S, Owners]),
                       io:format(user, "=== AM ===\n~p\n", [AM]),
                       io:format(user, "=== FixAM2 ===\n~p\n", [?DICT:to_list(FixDAM2)]),
                       io:format(user, "=== AS2 ===\n~p\n", [AS2])
                   end,
                   conjunction([{owners, equals(Owners, Owners2)},
                                {am2,    equals(lists:sort(AS), lists:sort(AS2))}]))
            end).

build(Owners) ->
    build(lists:usort(Owners), lists:reverse(Owners), [], ?DICT:new()).

build(_M, [], Owners, DAM) ->
    {Owners, DAM, fixup_dam(Owners, DAM)};
build(M, [N|Rest], Owners, DAM) ->
    {Owners1, DAM1} = prepend(M, N, Owners, DAM),
    build(M, Rest, Owners1, DAM1).

-endif. % EQC
-endif. % TEST.
