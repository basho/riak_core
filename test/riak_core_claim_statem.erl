%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created :  5 Jun 2017 by Russell Brown <russell@wombat.me>

-module(riak_core_claim_statem).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
%%-include_lib("eqc/include/eqc_statem.hrl").
 -include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(CLAIMANT, node_0).

-compile(export_all).


%% -- State ------------------------------------------------------------------
-record(state,
        {
          ring_size,
          nodes=[?CLAIMANT] :: [atom()], %% nodes that have been added
          node_counter=1 :: non_neg_integer(), %% to aid with naming nodes
          ring = undefined,
          committed_nodes = []
        }).

%% @doc run the statem with a ring of size `math:pow(`N', 2)'.
-spec with_ring_size(pos_integer()) -> eqc_statem:symbolic_state().
with_ring_size(N) ->
    RingSize = trunc(math:pow(2, N)),
    #state{ring_size=RingSize, ring=riak_core_ring:fresh(RingSize, ?CLAIMANT)}.

%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state_data() -> eqc_statem:symbolic_state().
initial_state_data() ->
    #state{}.

initial_state(_S) ->
    starting.

starting() ->
    [{planning, add_node}].

planning() ->
    [{planning, add_node},
     {planning, leave_node},
     {claiming, claim}].

claiming() ->
    [{planning, add_node},
     {planning, leave_node}].

%% -- Operations -------------------------------------------------------------

%% --- Operation: add_node ---
%% @doc add_node_pre/1 - Precondition for generation
add_node_pre(_From, _To, S=#state{nodes=Nodes}) when
      (S#state.ring_size div length(Nodes)) =< 3  ->
    false;
add_node_pre(_From, _To, _) ->
    true.

%% @doc add_node_args - Argument generator
-spec add_node_args(From, To, S) -> eqc_gen:gen([term()])
   when From :: eqc_fsm:state_name(),
        To   :: eqc_fsm:state_name(),
        S    :: eqc_statem:symbolic_state().
add_node_args(_From, _To, #state{node_counter=NC, ring=Ring}) ->
    %% TODO consider re-adding removed nodes
    [list_to_atom("node_" ++ integer_to_list(NC)),
     Ring].

%% @doc add_node - The actual operation
add_node(NodeName, Ring) ->
    R = riak_core_ring:add_member(?CLAIMANT, Ring, NodeName),
    R.

%% @doc add_node_next - Next state function
-spec add_node_next(_From, _To, S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
add_node_next(_From, _To, S=#state{node_counter=NC, nodes=Nodes}, Ring, [Node, _RingIn]) ->
    S#state{ring=Ring, node_counter=NC+1, nodes=[Node | Nodes]}.

%% @doc add_node_post - Postcondition for add_node
-spec add_node_post(_From, _To, S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
add_node_post(_Frim, _To, _S, [NodeName, _Ring], NextRing) ->
    lists:member(NodeName, riak_core_ring:members(NextRing, [joining])).

%% --- Operation: leave_node ---
%% @doc leave_node_pre/1 - Precondition for generation
leave_node_pre(_From, _To, #state{nodes=Nodes}) when length(Nodes) < 5 ->
    false;
leave_node_pre(_From, _To, _) ->
    true.

%% @doc leave_node_args - Argument generator
-spec leave_node_args(From, To, S) -> eqc_gen:gen([term()])
   when From :: eqc_fsm:state_name(),
        To   :: eqc_fsm:state_name(),
        S    :: eqc_statem:symbolic_state().
leave_node_args(_From, _To, #state{nodes=Nodes, ring=Ring}) ->
    %% TODO consider re-leaveing leaved nodes
    [elements(Nodes),
     Ring].

leave_node_pre(_From, _To, #state{nodes=Nodes}, [Node, _Ring]) ->
    lists:member(Node, Nodes);
leave_node_pre(_, _, _, _) ->
    false.

%% @doc leave_node - The actual operation
leave_node(NodeName, Ring) ->
    R = riak_core_ring:leave_member(?CLAIMANT, Ring, NodeName),
    R.

%% @doc leave_node_next - Next state function
-spec leave_node_next(_From, _To, S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
leave_node_next(_From, _To, S=#state{committed_nodes=Committed, nodes=Nodes}, Ring, [Node, _RingIn]) ->
    S#state{ring=Ring, committed_nodes=lists:delete(Node , Committed), nodes=lists:delete(Node, Nodes)}.

%% @doc leave_node_post - Postcondition for leave_node
-spec leave_node_post(_From, _To, S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
leave_node_post(_Frim, _To, _S, [NodeName, _Ring], NextRing) ->
    lists:member(NodeName, riak_core_ring:members(NextRing, [leaving])).

%% --- Operation: claim ---

%% @doc claim_pre/3 - Precondition for generation
-spec claim_pre(_From, _To, S :: eqc_statem:symbolic_state()) -> boolean().
claim_pre(_From, _To, #state{ring=undefined}) ->
    false;
claim_pre(_From, _To, _S) ->
    true.

%% @doc claim_args - Argument generator
-spec claim_args(_From, _To, S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
claim_args(_From, _To, #state{ring=Ring}) ->
    [Ring].

%% @doc claim - The actual operation
claim(Ring) ->
    R =riak_core_claim:claim(Ring, {riak_core_claim, wants_claim_v2}, {riak_core_claim, choose_claim_v2}),
    R.

%% @doc claim_next - Next state function
-spec claim_next(_From, _To, S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
claim_next(_From, _To, S=#state{nodes=Nodes}, NewRing, [_OldRing]) ->
    S#state{ring=NewRing, committed_nodes=Nodes}.

%% @doc claim_post - Postcondition for claim
-spec claim_post(_From, _To, S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
claim_post(_From, _To, #state{nodes=Nodes}, [_Ring], _NewRing) when length(Nodes) < 4 ->
    true;
claim_post(_From, _To, _S, [_Ring], NewRing) ->
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
          riak_core_claim:balanced_ring(ring_size(NewRing),
                                        node_count(NewRing), NewRing)} of
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

%% @doc Default generated property
-spec prop_claim(eqc_statem:symbolic_state()) -> eqc:property().
prop_claim(InitialState) ->
    ?FORALL(Cmds, commands(?MODULE, {starting, InitialState}),
            begin
                {H, {_FinalStateName, S}, Res} = run_commands(?MODULE, Cmds),
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
    length(riak_core_ring:members(Ring, [joining, valid])).

weight(_From, _To, add_node, _Args) ->
    5;
weight(_, _, _, _) ->
    1.

%% eunit stuff
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

claim_test() ->
    eqc:quickcheck(?QC_OUT(prop_claim(with_ring_size(5)))).

eqc_check(File, Prop) ->
    {ok, Bytes} = file:read_file(File),
    CE = binary_to_term(Bytes),
    eqc:check(Prop, CE).

%% Helpers
transfer_ring(Ring) ->
    Owners = riak_core_ring:all_owners(Ring),
    RFinal = lists:foldl(fun({Idx, Owner}, Racc) ->
                                 riak_core_ring:transfer_node(Idx, Owner, Racc) end,
                         Ring, Owners),
    RFinal.



-endif.
