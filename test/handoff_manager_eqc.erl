%%% @author John Daily <jd@epep.us>
%%% @copyright (C) 2014, John Daily
%%% @doc
%%%
%%% @end
%%% Created : 10 Oct 2014 by John Daily <jd@epep.us>

-module(handoff_manager_eqc).


-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-compile(export_all).

-record(state,{}).

%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{}.

%% ------ Common pre-/post-conditions
%% @doc General command filter, checked before a command is generated.
-spec command_precondition_common(S :: eqc_statem:symbolic_state(),
                                  Command :: atom()) -> boolean().
command_precondition_common(_S, _Command) ->
    true.

%% @doc General precondition, applied *before* specialized preconditions.
-spec precondition_common(S :: eqc_statem:symbolic_state(),
                          C :: eqc_statem:call()) -> boolean().
precondition_common(_S, _Call) ->
    true.

%% @doc General postcondition, applied *after* specialized postconditions.
-spec postcondition_common(S :: eqc_statem:dynamic_state(),
                           C :: eqc_statem:call(), Res :: term()) -> boolean().
postcondition_common(_S, _Call, _Res) ->
    true.

%% ------ Grouped operator: add_inbound
%% @doc add_inbound_command - Command generator
-spec add_inbound_command(S :: eqc_statem:symbolic_state()) ->
        eqc_gen:gen(eqc_statem:call()).
add_inbound_command(_S) ->
    {call, ?MODULE, add_inbound, []}.

%% @doc add_inbound_pre - Precondition for generation
-spec add_inbound_pre(S :: eqc_statem:symbolic_state()) -> boolean().
add_inbound_pre(_S) ->
    true. %% Condition for S

%% @doc add_inbound_pre - Precondition for add_inbound
-spec add_inbound_pre(S :: eqc_statem:symbolic_state(),
                      Args :: [term()]) -> boolean().
add_inbound_pre(_S, _Args) ->
    true. %% Condition for S + Args

%% @doc add_inbound_next - Next state function
-spec add_inbound_next(S :: eqc_statem:symbolic_state(),
                       V :: eqc_statem:var(),
                       Args :: [term()]) -> eqc_statem:symbolic_state().
add_inbound_next(S, _Value, _Args) ->
    S.

%% @doc add_inbound_post - Postcondition for add_inbound
-spec add_inbound_post(S :: eqc_statem:dynamic_state(),
                       Args :: [term()], R :: term()) -> true | term().
add_inbound_post(_S, _Args, _Res) ->
    true.

%% @doc add_inbound_blocking - Is the operation blocking in this State
%% -spec add_inbound_blocking(S :: eqc_statem:symbolic_state(),
%%         Args :: [term()]) -> boolean().
%% add_inbound_blocking(_S, _Args) ->
%%   false.

%% @doc add_inbound_adapt - How to adapt a call in this State
%% -spec add_inbound_adapt(S :: eqc_statem:symbolic_state(),
%%     Args :: [term()]) -> boolean().
%% add_inbound_adapt(_S, _Args) ->
%%   exit(adapt_not_possible).

%% @doc add_inbound_dynamicpre - Dynamic precondition for add_inbound
%% -spec add_inbound_dynamicpre(S :: eqc_statem:dynamic_state(),
%%         Args :: [term()]) -> boolean().
%% add_inbound_dynamicpre(_S, _Args) ->
%%   true.

%% ------ ... more operations

%% @doc <i>Optional callback</i>, Invariant, checked for each visited state
%%      during test execution.
%% -spec invariant(S :: eqc_statem:dynamic_state()) -> boolean().
%% invariant(_S) ->
%%   true.

%% @doc weight/2 - Distribution of calls
-spec weight(S :: eqc_statem:symbolic_state(), Command :: atom()) -> integer().
weight(_S, add_inbound) -> 1;
weight(_S, _Cmd) -> 1.

%% @doc Default generated property
-spec prop_handoff_manager() -> eqc:property().
prop_handoff_manager() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                {H, S, Res} = run_commands(?MODULE,Cmds),
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                Res == ok)
            end).
