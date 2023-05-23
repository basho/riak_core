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
%%
%% This is a purely functional algorithm for claiming nodes in a
%% ring. The reason to separate a pure algorithm is to allow
%% well tested code with subtle future changes.
%%
%% The algorithm can be studied in isolation to make it easier
%% to understand how it works.
%%
%% This algorithm computes a layout of nodes in a ring, such that
%% when data is replicated on TargetN consecutive nodes in that ring,
%% the consecutive nodes never co-incide.
%%
%% Riak has always had the possibility to compute such placements,
%% but when adding location awareness, things get a bit tricky.
%% Now we do not have just one location "A", but possibly 2 locations,
%% A and B. Such that we do never want two adjacent locations.
%% With ringsize 16 and 2 nodes for each location A and B a solution
%% would be:
%% A2 B2 A1 B1 A1 B1 A2 B2 A1 B1 A2 B2 A1 B1 A2 B2
%% Where an important additional property of this algorithm is that
%% none of the nodes is over represented. That is, each node occurs
%% either N or N+1 times in the list. We would violate this
%% requirement if 2 A1 nodes are replaced by an A2.
%%
%% Thus, the algorithm computes, given a configuration of nodes,
%% a ring size and a TargetN, a placement in a ring.
%%
%% We refer to the markup document in the docs directory for more
%% details on the use of the algorithm.
%%
%% Below we describe the actual algorithm in more detail.
%%
%% The algorithm is brute-force trying to get to a placement given
%% a ring size, target n-val and configuration of number of nodes in
%% each location.
%%
%% Step 1.
%% Since the ring should be balanced, the initial step is to
%% put all the given nodes in a sequence such that n-val is met.
%% For example, if there are 7 nodes in two locations (say 3 and 4
%% respectively) then the algorithm first places those 7 nodes in
%% the best way it can w.r.t. the given n-val. Clearly, if we have
%% only 2 locations, n-val should be at most 2. But then, there are
%% solutions that place those 7 nodes such that the locations (and
%% therewith the nodes) are not consecutive, even when wrapping around.
%%
%% Step 2.
%% Repeat this small ring as often as possible in the ringsize.
%% Thus, if ring size is 32 and we have 7 nodes, then we can repeat the
%% small ring 4 times and are 4 nodes short.
%% It would be unrealistic to provide a ring size that is less than
%% the number of nodes one provides. So assume it to fit always at least
%% once.
%%
%% Step 3.
%% Fill the gaps with additional nodes (part of the small ring) if needed
%% to get to full ring size.
%% While n-val not reached (zero violations is false):
%%   swap nodes (exchange position) or move nodes
%%      (moving vnode I to before vnode J).
%%
%% Step 3 gives a best effort solution, but given the enormous amount of
%% possible operations, it can take while to return. But it always terminate.
%% In order to make it terminate, we added heuristics in "worth_brute_force".
%% If we consider the number of violations too large for success, we just
%% don't do Step 3 or only partly until we solve the Nval for nodes.
%%
%% When we update a ring, then we want as little transfers as possible,
%% so first an effort is performed to just swap nodes. If that would not
%% work to get a solution, a solve attempt is taken to get best-effort
%% again.


-module(riak_core_claim_binring_alg).

-export([solve/3,
         update/3,
         node_loc_violations/2,
         moves/2,
         to_list/1, from_list/1]).

-ifdef(TEST).
-compile([export_all, nowarn_export_all]).
-define(DEBUG_FUNS, true).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-endif.

-ifdef(DEBUG).
-compile([export_all, nowarn_export_all]).
-ifndef(DEBUG_FUNS).
-define(DEBUG_FUNS, true).
-endif.
-define(PROFILE, true).
-include_lib("eqc/include/eqc_profile.hrl").
-define(debug(Fmt, Args), io:format(Fmt, Args)).
-else.
-define(BENCHMARK(_, X), X).
-define(debug(Fmt, Args), ok).
-endif.

%% -- Ring representation -------------------------------------------------

%% Represent the ring as a binary with one byte location index followed by one
%% byte node index.
-type ring()      :: binary().
-type ring_size() :: non_neg_integer().
-type nval()      :: pos_integer().
-type node_nval() :: nval().
-type loc_nval()  :: nval().
-type nvals()     :: {node_nval(), loc_nval()}.
-type nvalsmap()  :: #{node => node_nval(), location => loc_nval()}.

-type config() :: [non_neg_integer()].  %% List of node counts per location

-type loc()   :: byte().
-type ix()    :: byte().
-type pnode() :: {loc(), ix()}.     %% Physical node
-type vnode() :: non_neg_integer(). %% Virtual node (index in ring)

-spec ring_size(ring()) -> non_neg_integer().
ring_size(Ring) -> byte_size(Ring) div 2.

-spec from_list([pnode()]) -> ring().
from_list(Nodes) -> << <<Loc, Ix>> || {Loc, Ix} <- Nodes >>.

-spec to_list(ring()) -> [pnode()].
to_list(Ring) -> [ {Loc, Ix} || <<Loc, Ix>> <= Ring ].

-spec get_node(ring(), vnode()) -> pnode().
get_node(Ring, Ix) -> hd(window(Ring, Ix, 1)).

-spec set_node(ring(), vnode(), pnode()) -> ring().
set_node(Ring, VNode, {Loc, Ix}) ->
    B = VNode * 2,
    <<Before:B/binary, _:16, After/binary>> = Ring,
    <<Before/binary, Loc, Ix, After/binary>>.

%% Insert a node at the given vnode, making the ring one bigger.
-spec insert_node(ring(), vnode(), pnode()) -> ring().
insert_node(Ring, VNode, {Loc, Ix}) ->
    B = VNode * 2,
    <<Before:B/binary, After/binary>> = Ring,
    <<Before/binary, Loc, Ix, After/binary>>.

-spec delete_node(ring(), vnode()) -> ring().
delete_node(Ring, VNode) ->
    B = VNode * 2,
    <<Before:B/binary, _:16, After/binary>> = Ring,
    <<Before/binary, After/binary>>.

%% Return a window of size 2 * NVal - 1 centered on the given vnode.
-spec window(ring(), vnode(), nval()) -> [pnode()].
window(Ring, Ix, NVal) ->
    Size = ring_size(Ring),
    Len  = 2 * NVal - 1,
    if Len > Size -> window(<<Ring/binary, Ring/binary>>, Ix, NVal);
       true ->
            Lo   = Ix - NVal + 1,
            Hi   = Lo + Len,
            Win  = if Lo < 0    -> <<(slice(Lo + Size, -Lo, Ring))/binary,
                                     (slice(0, Hi, Ring))/binary>>;
                      Hi > Size -> <<(slice(Lo, Size - Lo, Ring))/binary,
                                     (slice(0, Hi - Size, Ring))/binary>>;
                      true      -> slice(Lo, Len, Ring)
                   end,
            to_list(Win)
    end.

-spec slice(non_neg_integer(), non_neg_integer(), binary()) -> binary().
slice(Addr, Len, Bin) ->
    A = Addr * 2,
    L = Len * 2,
    <<_:A/binary, Res:L/binary, _/binary>> = Bin,
    Res.

-spec moves(ring(), ring()) -> non_neg_integer().
moves(Ring1, Ring2) ->
    length([ 1 || {N1, N2} <- lists:zip(to_list(Ring1), to_list(Ring2)), N1 /= N2 ]).

%% -- NVal condition ------------------------------------------------------

-type violations() :: {non_neg_integer(), non_neg_integer()}.

-define(zero_v, {0, 0}).
-define(is_zero_v(V), (element(1, V) == 0 andalso element(2, V) == 0)).

to_nvals(#{location := LNVal, node := NVal}) ->
  {NVal, LNVal}.

zip_v(F, {A1, B1},         {A2, B2})         -> {F(A1, A2), F(B1, B2)};
zip_v(F, {A1, B1, C1},     {A2, B2, C2})     -> {F(A1, A2), F(B1, B2), F(C1, C2)};
zip_v(F, {A1, B1, C1, D1}, {A2, B2, C2, D2}) -> {F(A1, A2), F(B1, B2), F(C1, C2), F(D1, D2)}.

-spec add_v(violations(), violations()) -> violations().
add_v(V1, V2) -> zip_v(fun erlang:'+'/2, V1, V2).

-spec sub_v(violations(), violations()) -> violations().
sub_v(V1, V2) -> zip_v(fun erlang:'-'/2, V1, V2).

-spec sum_v([violations()]) -> violations().
sum_v(Vs) -> lists:foldl(fun add_v/2, ?zero_v, Vs).

-spec node_v(violations()) -> non_neg_integer().
node_v(V) ->
  element(2, V).

-spec loc_v(violations()) -> non_neg_integer().
loc_v(V) ->
  element(1, V).

-spec node_loc_violations(ring(), nvalsmap()) -> {non_neg_integer(), non_neg_integer()}.
node_loc_violations(Ring, NValsMap) ->
    V = violations(Ring, to_nvals(NValsMap)),
    {node_v(V), loc_v(V)}.

%% What's the maximum distance from an updated vnode where a violation change
%% can happen.
-spec max_violation_dist(nvals()) -> non_neg_integer().
max_violation_dist({N, L}) -> max(N, L).

-spec violations(ring(), nvals()) -> violations().
violations(Ring, NVals) ->
    violations(Ring, NVals, 0, ring_size(Ring) - 1).

-spec violations(ring(), nvals(), vnode(), vnode()) -> violations().
violations(Ring, NVals, A, B) ->
    violations(Ring, NVals, lists:seq(A, B)).

%% Returns number of node and location violations caused by the given vnode.
-spec violations(ring(), nvals(), vnode() | [vnode()]) -> violations().
violations(Ring, NVals, VNodes) when is_list(VNodes) ->
    sum_v([ violations(Ring, NVals, I) || I <- VNodes ]);
violations(Ring, NVals, VNode) ->
    ?BENCHMARK(violations, begin
                               {NVal, LVal} = NVals,
                               Locs = fun(Ns) -> [ L || {L, _} <- Ns ] end,
                               NV  = window_violations(window(Ring, VNode, NVal), NVal),
                               LocV  = fun(D) -> window_violations(Locs(window(Ring, VNode, LVal + D)), LVal + D) end,
                               LV    = LocV(0),
                               {LV, NV}
                           end).

%% Given a window of size 2 * NVal - 1 centered on an element X, count the
%% number of collisions with X in the slices of size NVal. For example
%% window_violations([1, 0, 1, 1, 1], 3) == 4 because of these 4 collisions:
%%    [1, 0, 1]  [0, 1, 1]  [1, 1, 1]
%%     !     *       *  !    *  !  !
%% (where ! marks a collision and * marks the center element)
window_violations(Win, NVal) ->
    window_violations(Win, 0, NVal).

%% Ignore violations inside the cut-off (i.e. distance to the center =< CutOff).
window_violations(Win, CutOff, NVal) ->
    Masked = lists:zip(Win, lists:duplicate(NVal - 1 - CutOff, check)
                       ++ lists:duplicate(CutOff, skip)
                       ++ [original]
                       ++ lists:duplicate(CutOff, skip)
                       ++ lists:duplicate(NVal - 1 - CutOff, check)),
    X = lists:nth(NVal, Win),
    Windows = [ lists:sublist(Masked, I, NVal) || I <- lists:seq(1, length(Win) - NVal + 1) ],
    length([ X || W <- Windows
                      , not lists:member({X, skip}, W)  %% If we have a skipped collision we don't care about other collisions
                      , {Y, check} <- W, X == Y ]).

%% -- Node count allocation -----------------------------------------------

-spec nodes_in_config(config()) -> [pnode()].
nodes_in_config(Locs) ->
    [ {L, I}
      || {I, _, L} <- lists:sort(
                        [ {I, -N, L}
                          || {L, N} <- enumerate(Locs)
                                 , I      <- lists:seq(1, N) ])
    ].

enumerate(Xs) -> lists:zip(lists:seq(1, length(Xs)), Xs).

%% When ring size is not divisible by the number of nodes, some nodes need to
%% occur an extra time in the ring. We pick those from the smaller locations to
%% make locations as balanced as possible.
-spec extra_nodes(ring_size(), config()) -> [pnode()].
extra_nodes(RingSize, Config) ->
    NumNodes = lists:sum(Config),
    Extra    = RingSize rem NumNodes,
    Count    = RingSize div NumNodes,
    distribute_extra_nodes(lists:sort([ {Count * N, L, 1, N} || {L, N} <- enumerate(Config) ]), Extra).

distribute_extra_nodes(_, 0) -> [];
distribute_extra_nodes([{_, _, Ix, Num} | Locs], Extra) when Ix > Num ->
    distribute_extra_nodes(Locs, Extra);
distribute_extra_nodes([{Total, Loc, Ix, Num} | Locs], Extra) ->
    Entry = {Total + 1, Loc, Ix + 1, Num},
    [{Loc, Ix} | distribute_extra_nodes(lists:merge([Entry], Locs), Extra - 1)].

%% -- Brute force node swapping -------------------------------------------

brute_force(Ring, NVals) ->
    brute_force(Ring, NVals, []).

brute_force(Ring, NVals, Options) ->
    ?BENCHMARK(brute_force,
               brute_force(Ring, NVals, Options, violations(Ring, NVals))).

brute_force(Ring, NVals, Options, V) ->
    brute_force(Ring, NVals, Options, V, false).

brute_force(Ring, NVals, Options, V, OrigSwaps) ->
    TryHard = proplists:get_bool(try_hard, Options),
    AlwaysBruteForce = proplists:get_bool(brute_force, Options),
    StopNodeOnly = proplists:get_bool(node_only, Options) andalso node_v(V) == 0,
    case V of
        _ when not TryHard, ?is_zero_v(V) -> Ring;
        ?zero_v -> Ring;
        _ when StopNodeOnly, not AlwaysBruteForce -> Ring;
        _ ->
            N = ring_size(Ring),
            Swaps =
                case OrigSwaps of
                    false ->
                        generate_swaps(N, Options);
                    OrigSwaps when is_list(OrigSwaps) ->
                        OrigSwaps
                end,
            brute_force(Ring, NVals, V, Options, Ring, ?zero_v, Swaps, Swaps)
    end.

generate_swaps(N, Options) ->
    [ {swap, I, J} || I <- lists:seq(0, N - 2), J <- lists:seq(I, N - 1) ] ++
        lists:sort(fun({move, I1, J1}, {move, I2, J2}) -> abs(I1 - J1) =< abs(I2 - J2) end,
                   [ {move, I, J} || not proplists:get_bool(only_swap, Options)
                                         , I <- lists:seq(0, N - 1), J <- lists:seq(0, N - 1)
                                         , D <- [mod_dist(J, I, N)]
                                         , D > 2 orelse D < -1   %% Moving just one step is a swap
                   ]).

mod_dist(I, J, N) ->
    D = (J - I + N) rem N,
    if D * 2 > N -> D - N;
       true      -> D
    end.

%% TODO: Don't use DeltaV for BestV (total violations instead)
brute_force(_Ring, NVals, V, Options, Best, BestV, [], OrigSwaps) when BestV < ?zero_v ->
    ?debug("~s\n", [show(Best, NVals)]),
    brute_force(Best, NVals, Options, add_v(V, BestV), OrigSwaps);
brute_force(_Ring, _NVals, _V, _Options, Best, _BestV, [], _OrigSwaps) -> Best;
brute_force(Ring, NVals, V, Options, Best, BestV, [Op | Swaps], OrigSwaps) ->
    {Ring1, DV} = op(Ring, NVals, Op),
    TryHard = proplists:get_bool(try_hard, Options),
    if DV < ?zero_v, not TryHard ->
            ?debug("~s\n", [show(Ring1, NVals)]),
            brute_force(Ring1, NVals, Options, add_v(V, DV), OrigSwaps);
       DV < BestV ->
            brute_force(Ring, NVals, V, Options, Ring1, DV, Swaps, OrigSwaps);
       true ->
            brute_force(Ring, NVals, V, Options, Best, BestV, Swaps, OrigSwaps)
    end.

op(Ring, NVals, {swap, I, J}) ->
    ?BENCHMARK(swap, begin
                         Ring1 = swap(Ring, I, J),
                         OldV  = violations(Ring,  NVals, [I, J]),
                         NewV  = violations(Ring1, NVals, [I, J]),
                         DV    = sub_v(NewV, OldV),
                         %% Each violation is double-counted when we sum the entire ring
                         {Ring1, add_v(DV, DV)}
                     end);
op(Ring, NVals, {move, I, J}) ->
    ?BENCHMARK(move, begin
                         %% {move, I, J} means moving vnode I to before vnode J
                         Ring1 = move(Ring, I, J),
                         N     = ring_size(Ring),
                         NVal  = max_violation_dist(NVals),
                         %% To compute the delta violations we figure out which vnodes in the original
                         %% ring are affected by the move. These are the vnodes within NVal - 1 of the
                         %% source or destination, except to the right of the destination where only
                         %% NVal - 2 nodes are affected.
                         OldIxs = lists:usort([ (K + 10 * N) rem N || K <- lists:seq(I - NVal + 1, I + NVal - 1) ++
                                                                          lists:seq(J - NVal + 1, J + NVal - 2) ]),
                         %% We need to compare the violations before and after for the affected
                         %% indices, so we need to know where they end up in the new ring. Only
                         %% indices between I and J are affected.
                         Remap = fun(K) when K >= J, K < I -> K + 1;   %% {J = a .. K = * .. I = x} -> {J = x, J+1 = a, .. K+1 = *}
                                    (K) when K > I,  K < J -> K - 1;   %% {I = x .. K = * .. J = a} -> {K-1 = *, J-1 = x, J = a}
                                    (K) when K == I, I < J -> J - 1;   %% {I = * .. J = a}          -> {J-1 = *, J = a}
                                    (K) when K == I, J < I -> J;       %% {J = a .. I = *}          -> {J = *, J+1 = a ..}
                                    (K)                    -> K
                                 end,
                         NewIxs = lists:map(Remap, OldIxs),
                         OldV   = violations(Ring,  NVals, OldIxs),
                         NewV   = violations(Ring1, NVals, NewIxs),
                         DV     = sub_v(NewV, OldV),
                         {Ring1, DV}
                     end).

move(Ring, I, I) -> Ring;
move(Ring, I, J) ->
    Node = get_node(Ring, I),
    if I < J -> insert_node(delete_node(Ring, I), J - 1, Node);
       true  -> insert_node(delete_node(Ring, I), J, Node)
    end.

swap(Ring, I, J) ->
    X = get_node(Ring, I),
    Y = get_node(Ring, J),
    set_node(set_node(Ring, I, Y), J, X).

worth_brute_force(RingSize, V) ->
    NodeOnly = node_v(V) < RingSize div 2,
    Full = loc_v(V) + node_v(V) < RingSize,
    if Full -> brute_force;
       NodeOnly -> node_only;
       true -> no_brute_force
    end.

maybe_brute_force(Ring, NVals, Options) ->
    case worth_brute_force(ring_size(Ring), violations(Ring, NVals)) of
        brute_force ->
            brute_force(Ring, NVals, Options);
        node_only ->
            brute_force(Ring, NVals, [node_only|Options]);
        no_brute_force ->
            Ring
    end.


%% -- The solver ----------------------------------------------------------

-spec solve(ring_size(), config(), nvalsmap()) -> ring().
solve(RingSize, Config, NValsMap) ->
    solve(RingSize, Config, NValsMap, []).

-spec solve(ring_size(), config(), nvalsmap(), proplists:proplist()) -> ring().
solve(RingSize, [1], _NValsMap, _) ->
    from_list(lists:duplicate(RingSize, {1,1}));
solve(RingSize, Config, NValsMap, Options) ->
    NVals = to_nvals(NValsMap),
    NumNodes  = lists:sum(Config),
    Rounds    = RingSize div NumNodes,
    AllNodes  = nodes_in_config(Config),
    SmallRing = small_ring(AllNodes, NVals),
    ?debug("SmallRing:\n~s\n", [show(SmallRing, NVals)]),
    Extras    = extra_nodes(RingSize, Config),
    Cycle     = fun(R) -> << <<SmallRing/binary>> || _ <- lists:seq(1, R) >> end,
    ToRemove  = AllNodes -- Extras,
    BigRingD  = solve_node_deletions(Cycle(Rounds + 1), NVals, ToRemove),
    VD        = violations(BigRingD, NVals),
    ?debug("Delete\n~s\n", [show(BigRingD, NVals)]),
    NoBruteForce = proplists:get_bool(no_brute_force, Options),
    AlwaysBruteForce = proplists:get_bool(brute_force, Options),
    case VD of
        ?zero_v -> BigRingD;
        _ when NumNodes > RingSize ->
            %% Should not ask for this case
            if NoBruteForce -> BigRingD;
               AlwaysBruteForce -> brute_force(BigRingD, NVals);
               true -> maybe_brute_force(BigRingD, NVals, [])
            end;
        _       ->
            BigRingI  = solve_node_insertions(Cycle(Rounds), NVals, Extras),
            ?debug("Insert\n~s\n", [show(BigRingI, NVals)]),
            VI        = violations(BigRingI, NVals),
            BFRing =
               if VI < VD ->
                    ?debug("Chose insert\n", []),
                    BigRingI;
               true    ->
                    ?debug("Chose delete\n", []),
                    BigRingD
            end,
            if NoBruteForce -> BFRing;
               AlwaysBruteForce -> brute_force(BigRingD, NVals);
               true -> maybe_brute_force(BFRing, NVals, [])
            end
    end.

%% The "small ring" is the solution when RingSize == NumNodes. If we can solve
%% that we can repeat that pattern without introducing any violations. If we
%% can't solve the small ring, rather than going with the best non-solution we
%% add a fake location with a single node and try to solve that instead
%% (inserting more and more fake locations until we get a solution). These fake
%% nodes are stripped before we return.
%% The rationale for this is that we get something where inserting more nodes
%% can produce a solution, and for the big ring we do need to insert extra
%% nodes if NumNodes is not a power of two.
small_ring(AllNodes, NVals) ->
    small_ring(AllNodes, NVals, -1).

small_ring(AllNodes, NVals, FakeLoc) ->
    SmallRing = brute_force(from_list(AllNodes), NVals, [try_hard]),
    case violations(SmallRing, NVals) of
        V when ?is_zero_v(V) ->
            [ ?debug("SmallRing (with fakes)\n~s\n", [show(SmallRing, NVals)]) || FakeLoc < -1 ],
            remove_fake(SmallRing);
        _ -> small_ring([{FakeLoc, 1} | AllNodes], NVals, FakeLoc - 1)
    end.

remove_fake(Ring) ->
    from_list([ Node || Node = {Loc, _} <- to_list(Ring), Loc < 128 ]).

solve_node_insertions(Ring, NVals, Nodes) ->
    lists:foldl(fun(N, R) -> solve_node_insertion(R, NVals, N) end,
                Ring, Nodes).

solve_node_insertion(Ring, NVals, Node) ->
    solve_node_insertion(Ring, NVals, Node, 0, ring_size(Ring), undefined, undefined).

solve_node_insertion(_, _, _, I, Size, BestR, _) when I >= Size -> BestR;
solve_node_insertion(Ring, NVals, Node, I, Size, BestR, BestV) ->
    Ring1 = insert_node(Ring, I, Node),
    V = violations(Ring1, NVals), %% TODO: recompute local violation changes
    if BestV == undefined; V < BestV ->
            solve_node_insertion(Ring, NVals, Node, I + 1, Size, Ring1, V);
       true ->
            solve_node_insertion(Ring, NVals, Node, I + 1, Size, BestR, BestV)
    end.

solve_node_deletions(Ring, NVals, Nodes) ->
    lists:foldl(fun(N, R) -> solve_node_deletion(R, NVals, N) end,
                Ring, Nodes).

solve_node_deletion(Ring, NVals, Node) ->
    solve_node_deletion(Ring, NVals, Node, 0, ring_size(Ring), undefined, undefined).

solve_node_deletion(_, _, _, I, Size, BestR, _) when I >= Size -> BestR;
solve_node_deletion(Ring, NVals, Node, I, Size, BestR, BestV) ->
    case get_node(Ring, I) == Node of
        false -> solve_node_deletion(Ring, NVals, Node, I + 1, Size, BestR, BestV);
        true  ->
            Ring1 = delete_node(Ring, I),
            V = violations(Ring1, NVals), %% TODO: recompute local violation changes
            if BestV == undefined; V < BestV -> solve_node_deletion(Ring, NVals, Node, I + 1, Size, Ring1, V);
               true -> solve_node_deletion(Ring, NVals, Node, I + 1, Size, BestR, BestV)
            end
    end.

%% -- Updating ------------------------------------------------------------

nodes_in_ring(RingSize, Config) ->
    X = RingSize div lists:sum(Config),
    lists:append(lists:duplicate(X, nodes_in_config(Config))) ++ extra_nodes(RingSize, Config).

-spec update(ring(), config(), nvalsmap()) -> ring().
update(OldRing, [1], NValsMap) ->
    solve(ring_size(OldRing), [1], NValsMap);
update(OldRing, Config, NValsMap) ->
    NVals = to_nvals(NValsMap),
    %% Diff old and new config
    RingSize = ring_size(OldRing),
    OldNodes = to_list(OldRing),
    NewNodes = nodes_in_ring(RingSize, Config),
    ToAdd    = NewNodes -- OldNodes,
    ToRemove = OldNodes -- NewNodes,
    %% Swap in new nodes for old nodes (in a moderately clever way)
    NewRing = swap_in_nodes(OldRing, ToAdd, ToRemove, NVals),
    maybe_brute_force(NewRing, NVals, [{only_swap, true}]).

swap_in_nodes(Ring, [], [], _NVals) -> Ring;
swap_in_nodes(Ring, [New | ToAdd], ToRemove, NVals) ->
    {Ring1, Removed} = find_swap(Ring, New, ToRemove, NVals),
    swap_in_nodes(Ring1, ToAdd, ToRemove -- [Removed], NVals).

find_swap(Ring, New, ToRemove, NVals) ->
    Swap = fun(I) ->
                   Old = get_node(Ring, I),
                   [ begin
                         Ring1 = set_node(Ring, I, New),
                         V = violations(Ring1, NVals, I),
                         {V, Ring1, Old}
                     end || lists:member(Old, ToRemove) ]
           end,
    {_V, Ring1, Removed} = lists:min(lists:flatmap(Swap, lists:seq(0, ring_size(Ring) - 1))),
    {Ring1, Removed}.

%% -- Debugging -----------------------------------------------------------

-ifdef(DEBUG_FUNS).
pp_violations({L, N}) -> pp_violations({L, N, 0});
pp_violations({L, N, L1}) -> pp_violations({L, N, L1, 0});
pp_violations({L, N, A, B}) ->
    [ io_lib:format("~p", [L])
    , [ io_lib:format(" + ~pn", [N]) || N /= 0 ]
    , [ io_lib:format(" + ~pa", [A]) || A /= 0 ]
    , [ io_lib:format(" + ~pb", [B]) || B /= 0 ]
    ].

show(Ring, NVals) ->
    Color = fun(?zero_v, S)              -> S;
               (V, S) when ?is_zero_v(V) -> "\e[34m" ++ S ++ "\e[0m";
               (_, S)                    -> "\e[31m" ++ S ++ "\e[0m" end,
    TotalV = violations(Ring, NVals),
    Vs     = [ violations(Ring, NVals, I) || I <- lists:seq(0, ring_size(Ring) - 1) ],
    lists:flatten(io_lib:format("~s(~s violations)",
                                [ [io_lib:format(Color(V, "~c~p "), [L + $A - 1, I]) || {{L, I}, V} <- lists:zip(to_list(Ring), Vs)]
                                , pp_violations(TotalV) ])).

show_solve(RingSize, Config, NValsMap) ->
    NVals = to_nvals(NValsMap),
    io:format("~s\n", [show(solve(RingSize, Config, NValsMap), NVals)]).

show_update(RingSize, OldConfig, NewConfig, NValsMap) ->
    NVals = to_nvals(NValsMap),
    OldRing = solve(RingSize, OldConfig, NValsMap),
    NewRing = update(OldRing, NewConfig, NValsMap),
    io:format("Old\n~s\nNew\n~s\nDiff=~p\n", [show(OldRing, NVals), show(NewRing, NVals), moves(OldRing, NewRing)]).
-endif.

%% -- Tetsing --------------------------------------------------------------

-ifdef(TEST).

%% -- Unit tests for experimentation ---------------------------------------
%% These tests take a bit of time when running.
%% Not intended to be included in automatic testing.

known_hard_tests() ->
    Tests = [ {16,  [4, 3, 3, 2],       3, ?zero_v}
            , {32,  [3, 2, 1, 4, 3],    3, ?zero_v}
            , {32,  [5, 6, 5, 1, 1],    3, ?zero_v}
            , {128, [1, 1, 1, 1, 1, 1], 5, ?zero_v}
            , {16,  [4, 4, 4, 3],       4, ?zero_v}
            , {16,  [4, 4, 3, 3],       4, ?zero_v}
            , {16,  [4, 3, 3, 3],       4, ?zero_v}
            , {32,  [4, 3, 3, 3],       4, ?zero_v}
            , {48,  [4, 3, 3, 3],       4, ?zero_v}
            , {32,  [2, 2, 2, 2, 2],    4, {2,0}}
            , {16,  [2, 2, 1, 2, 2],    4, ?zero_v}
            , {16,  [2, 2, 4, 2],       4, ?zero_v}
            , {16,  [3, 2, 2, 2],       4, ?zero_v}
            , {32,  [3, 2, 2, 2],       4, {8, 0}}
            , {32,  [3, 3, 3, 1, 1],    4, {16,0}}
            , {16,  [1, 3, 2, 1, 1, 1], 4, {4, 0}}
            , {64,  [2, 2, 1, 2, 2, 2], 5, ?zero_v}
            , {256, [6, 5, 2],          2, ?zero_v}
            , {64,  [3, 3, 3, 2, 1],    4, {4,0}}
            , {32,  [3, 3, 3, 3, 1],    4, {4,0}}
            , {512, [4, 4, 4, 4, 1],    4, {4,0}}
            ],
    [ {Size, Config, NVal, '->', V}
      || {Size, Config, NVal, Expect} <- Tests
           , V <- [violations(solve(Size, Config, #{location => NVal, node => NVal}), {NVal, NVal})]
           , V /= Expect
    ].

typical_scenarios_tests() ->
    %% We simulate updates from fresh ring to more and more nodes and locations
    NVal = 4,
    Tests = [ [1]
            , [2, 2, 2, 2]
            , [2, 2, 2, 2, 1]
            , [2, 2, 2, 2, 2]
            , [2, 2, 2, 2, 2, 1]
            , [2, 2, 2, 2, 2, 2]
            , [3, 2, 2, 2, 2, 2]
            , [3, 3, 2, 2, 2, 2]
            , [3, 3, 3, 2, 2, 2]
            , [3, 3, 3, 3, 2, 2]
            ],
    Results =
        [ lists:foldl(
            fun(_Config, Err={error, _}) ->
                    Err;
               (Config, {undefined, Diffs}) ->
                    {solve(Size, Config, #{location => NVal, node => NVal}), Diffs};
               (Config, {OldRing, Diffs}) ->
                    NewRing = update(OldRing, Config, #{location => NVal, node => NVal}),
                    V       = violations(NewRing, {NVal, NVal}),
                    Diff    = moves(OldRing, NewRing),
                    if ?is_zero_v(V) -> {NewRing, Diffs ++ [Diff]};
                       true -> {error, {Size, OldRing, NewRing, Config, V}}
                    end
            end, {undefined, [0]}, Tests)
          || Size <- [64, 128, 256, 512, 1024]
        ],
    HistoricDiffs =
     [[0,56,8,8,6,8,26,5,4,4],
      [0,112,15,12,12,14,9,12,9,8],
      [0,224,29,31,24,21,21,21,23,16],
      [0,448,57,59,47,48,39,45,36,32],
      [0,896,114,119,94,85,78,87,79,64]],
    case [ Err || {error, Err} <- Results ] of
        []   ->
           true =
               lists:all(fun({L1, L2}) ->
                             lists:sum(L1) =< lists:sum(L2)
                         end, lists:zip([ Diff || {_Ring, Diff} <- Results ], HistoricDiffs));
        Errs -> {error, Errs}
    end.

wcets() ->
  [ io:format("Size ~4b Time ~.1f sec\n", [Size, wcet(Size, 4) / 1000000])
    || Size <- [16, 32, 64, 128, 256, 512, 1024, 2048] ].

wcet(RingSize, NVal) ->
  NValMap = #{location => NVal, node => NVal},
  Ring = solve(RingSize, [1], NValMap),
  {T, _} = timer:tc(fun() -> brute_force(Ring, {NVal, NVal}) end),
  T.

-ifdef(EQC).

%% -- Generators ----------------------------------------------------------

pnode() -> {choose(1, 16), choose(1, 16)}.

ring() -> non_empty(list(pnode())).

nvals() -> ?LET(NVal, choose(1, 5),
                ?LET(LVal, choose(1, NVal),
                     {NVal, LVal})).

op(N) ->
    Ix = choose(0, N - 1),
    ?SUCHTHAT(Op, {elements([swap, move]), Ix, Ix},
              case Op of
                  {swap, _, _} -> true;
                  {move, _, _} -> true
              end).

%% -- Properties ----------------------------------------------------------

prop_window() ->
    ?FORALL(Nodes, ring(),
            ?FORALL({Ix, NVal}, {choose(0, length(Nodes) - 1), choose(1, 5)},
                    begin
                        Ring   = from_list(Nodes),
                        Window = subring(Nodes, Ix - NVal + 1, 2 * NVal - 1),
                        equals(window(Ring, Ix, NVal), Window)
                    end)).

prop_get_node() ->
    ?FORALL(Nodes, ring(),
            begin
                Ring = from_list(Nodes),
                equals([ get_node(Ring, I) || I <- lists:seq(0, ring_size(Ring) - 1) ],
                       Nodes)
            end).

subring(Xs, Ix, Len) when Ix < 0 -> subring(Xs ++ Xs, Ix + length(Xs), Len);
subring(Xs, Ix, Len) when Ix + Len > length(Xs) -> subring(Xs ++ Xs, Ix, Len);
subring(Xs, Ix, Len) -> lists:sublist(Xs, Ix + 1, Len).

prop_swap_violations() ->
    ?FORALL(Nodes, ring(),
            ?FORALL({Op, NVals}, {op(length(Nodes)), nvals()},
                    begin
                        Ring        = from_list(Nodes),
                        V           = violations(Ring, NVals),
                        {Ring1, DV} = op(Ring, NVals, Op),
                        V1          = violations(Ring1, NVals),
                        ?WHENFAIL(io:format("Original: ~s\nSwapped:  ~s\nV  = ~p\nV1 = ~p\nDV = ~p\n",
                                            [show(Ring, NVals), show(Ring1, NVals), V, V1, DV]),
                                  equals(add_v(V, DV), V1))
                    end)).

%% In legacy riak there are no locations and only NVal for nodes is
%% imoportant. Property compares that we can both implement that
%% with 1 location and location nval == 1 or each node its own
%% location.
prop_no_locations() ->
    ?FORALL({Size, Nodes, NVal}, {elements([16, 32, 64, 128, 256, 512]), choose(1, 64), choose(1,5)},
            begin
                {OneT, OneRing} = timer:tc(fun() -> solve(Size, [Nodes], #{node => NVal, location => 1}) end),
                {_, OneViolations} = violations(OneRing, {NVal, 1}),
                {SepT, SepRing} = timer:tc(fun() -> solve(Size, lists:duplicate(Nodes, 1), #{node => NVal, location => NVal}) end),
                {_, SepViolations} = violations(SepRing, {NVal, NVal}),
                measure(one_location, OneT,
                        measure(sep_location, SepT,
                                equals(OneViolations, SepViolations)))
            end).

config_gen() ->
   ?LET(N, choose(1,7), ?LET(M, choose(2, 6), vector(N, choose(M, M + 2)))).

prop_brute_force_optimize() ->
   in_parallel(
   ?FORALL({Size, Config, NValsMap}, {elements([128, 256, 512]),
                                      config_gen(),
                                      ?LET(N, choose(3, 4), #{node => N, location => default(N, choose(2, N))})},
   ?IMPLIES(length(Config) >= maps:get(location, NValsMap),
            begin
                NVals = to_nvals(NValsMap),
                {T1, Ring1} = timer:tc(fun() -> solve(Size, Config, NValsMap, [no_brute_force]) end),
                [{_, RV1}, {_, RV2}, {_, RV3}] = Res =
                  case violations(Ring1, NVals) of
                    ?zero_v -> [{T1, ?zero_v}, {0, ?zero_v}, {0, ?zero_v}];
                    V1 ->
                      {T2, Ring2} = timer:tc(fun() -> brute_force(Ring1, NVals, [node_only]) end),
                      V2 = violations(Ring2, NVals),
                      {T3, Ring3} = timer:tc(fun() -> brute_force(Ring2, NVals) end),
                      V3 = violations(Ring3, NVals),
                      [{T1 / 1000000, V1}, {T2 / 1000000, V2}, {T3 / 1000000, V3}]
                  end,
                Improved = length(lists:usort([ RV1, RV2, RV3 ])) > 1,
                WorthBF = worth_brute_force(Size, RV1),
                FailNodeOnly = node_v(RV2) == 0 andalso WorthBF == no_brute_force,
                FailBruteForce = ?is_zero_v(RV3) andalso WorthBF /= brute_force,
                ?WHENFAIL(eqc:format("Worth brute force ~p for ~p\n", [WorthBF, Res]),
                aggregate([{Size, Config, NValsMap, Res, WorthBF} || Improved andalso WorthBF /= brute_force],
                          conjunction([{node_only, not FailNodeOnly},
                                       {brute_force, not FailBruteForce}])))
            end))).

-endif.
-endif.
