%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created :  5 Jun 2017 by Russell Brown <russell@wombat.me>

-module(riak_core_claim_statem).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(CLAIMANT, node_0).

-compile(export_all).

%% -- State ------------------------------------------------------------------
-record(state,
        {
          nodes=[?CLAIMANT] :: [atom()], %% nodes that have been added
          node_counter=1 :: non_neg_integer(), %% to aid with naming nodes
          ring = undefined
        }).

%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{}.

%% -- Operations -------------------------------------------------------------

%% --- Operation: fresh_ring ---
%% @doc fresh_ring_pre/1 - Precondition for generation
-spec fresh_ring_pre(S :: eqc_statem:symbolic_state()) -> boolean().
fresh_ring_pre(#state{ring=undefined}) ->
    true;
fresh_ring_pre(_S) ->
    false.

%% @doc fresh_ring_args - Argument generator
-spec fresh_ring_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
fresh_ring_args(_S) ->
    [choose(4, 9)].

%% @doc fresh_ring - The actual operation
fresh_ring(RingSize) ->
    RS = trunc(math:pow(2, RingSize)),
    riak_core_ring:fresh(RS, ?CLAIMANT).

%% @doc fresh_ring_next - Next state function
-spec fresh_ring_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
fresh_ring_next(S, Ring, [_RingSize]) ->
    S#state{ring=Ring}.

%% --- Operation: add_node ---
%% @doc add_node_pre/1 - Precondition for generation
-spec add_node_pre(S :: eqc_statem:symbolic_state()) -> boolean().
add_node_pre(#state{ring=undefined}) ->
    false;
%% TODO make this dynamic based on ring_size
add_node_pre(#state{nodes=Nodes}) when length(Nodes) > 15 ->
    false;
add_node_pre(_) ->
    true.

%% @doc add_node_args - Argument generator
-spec add_node_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
add_node_args(#state{node_counter=NC, ring=Ring}) ->
    %% TODO consider re-adding removed nodes
    [list_to_atom("node_" ++ integer_to_list(NC)),
     Ring].

%% @doc add_node - The actual operation
add_node(NodeName, Ring) ->
    riak_core_ring:add_member(?CLAIMANT, Ring, NodeName).

%% @doc add_node_next - Next state function
-spec add_node_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
add_node_next(S=#state{node_counter=NC, nodes=Nodes}, Ring, [Node, _RingIn]) ->
    S#state{ring=Ring, node_counter=NC+1, nodes=[Node | Nodes]}.

%% @doc add_node_post - Postcondition for add_node
-spec add_node_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
add_node_post(_S, [NodeName, _Ring], NextRing) ->
    lists:member(NodeName, riak_core_ring:members(NextRing, [joining])).


%% --- Operation: claim ---
%% @doc claim_pre/1 - Precondition for generation
-spec claim_pre(S :: eqc_statem:symbolic_state()) -> boolean().
claim_pre(#state{ring=undefined}) ->
    false;
claim_pre(_S) ->
    true.

%% @doc claim_args - Argument generator
-spec claim_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
claim_args(#state{ring=Ring}) ->
    [Ring].

%% @doc claim - The actual operation
claim(Ring) ->
    riak_core_claim:claim(Ring, {riak_core_claim, wants_claim_v2}, {riak_core_claim, choose_claim_v2}).

%% @doc claim_next - Next state function
-spec claim_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
claim_next(S, NewRing, [_OldRing]) ->
    S#state{ring=NewRing}.

%% @doc claim_post - Postcondition for claim
-spec claim_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
claim_post(#state{nodes=Nodes}, [_Ring], _NewRing) when length(Nodes) < 4 ->
    %% TODO check the ring here
    true;
claim_post(_S, [_Ring], NewRing) ->
    Nval = 3,
    TNval = 4,
    Preflists = riak_core_ring:all_preflists(NewRing, Nval),
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

    case {riak_core_claim:meets_target_n(NewRing, TNval),
          ImperfectPLs,
          riak_core_claim:balanced_ring(ring_size(NewRing), node_count(NewRing), NewRing)} of
        {{true, []}, [], true} ->
            true;
        {X, Y, Z} ->
            {ring_size(NewRing), node_count(NewRing),
             {{meets_target_n, X},
              {perfect_pls, Y},
              {balanced_ring, Z}}}
    end.

%% -- Property ---------------------------------------------------------------
%% @doc <i>Optional callback</i>, Invariant, checked for each visited state
%%      during test execution.
%% -spec invariant(S :: eqc_statem:dynamic_state()) -> boolean().
%% invariant(_S) ->
%% true.

%% @doc weight/2 - Distribution of calls
-spec weight(S, Cmd) -> integer()
    when S   :: eqc_statem:symbolic_state(),
         Cmd :: atom().
weight(_S, add_node) -> 1;
weight(_S, _Cmd) -> 1.

%% @doc Default generated property
-spec prop_riak_core_claim_statem() -> eqc:property().
prop_riak_core_claim_statem() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                {H, S, Res} = run_commands(?MODULE, Cmds),
                Ring = S#state.ring,
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                aggregate(command_names(Cmds),
                                          measure(ring_size, ring_size(Ring),
                                                  measure(node_count, node_count(Ring),
                                                          Res == ok))))
            end).

ring_size(undefined) ->
    0;
ring_size(Ring) ->
    riak_core_ring:num_partitions(Ring).

node_count(undefined) ->
    0;
node_count(Ring) ->
    length(riak_core_ring:all_members(Ring)).

-endif.
