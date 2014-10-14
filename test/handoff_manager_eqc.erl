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

-record(state,{handoffs=[],max_concurrency=0}).

%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{handoffs=[],max_concurrency=2}.

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
%% We can send an empty list as the `SSLOpts' argument to
%% `add_inbound/1` since we mock `handoff_receiver'
-spec add_inbound_command(S :: eqc_statem:symbolic_state()) ->
        eqc_gen:gen(eqc_statem:call()).
add_inbound_command(_S) ->
    {call, riak_core_handoff_manager, add_inbound, [[]]}.

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


%% ------ Grouped operator: kill_handoffs
%% kill_handoffs is a function of limited utility, only invoked by the
%% object version upgrade/downgrade via riak_kv_reformat:run/2.
%% @doc kill_handoffs_command - Command generator
-spec kill_handoffs_command(S :: eqc_statem:symbolic_state()) ->
        eqc_gen:gen(eqc_statem:call()).
kill_handoffs_command(_S) ->
    {call, riak_core_handoff_manager, kill_handoffs, []}.

%% @doc kill_handoffs_pre - Precondition for generation
-spec kill_handoffs_pre(S :: eqc_statem:symbolic_state()) -> boolean().
kill_handoffs_pre(_S) ->
    true. %% Condition for S

%% @doc kill_handoffs_pre - Precondition for kill_handoffs
-spec kill_handoffs_pre(S :: eqc_statem:symbolic_state(),
                        Args :: [term()]) -> boolean().
kill_handoffs_pre(_S, _Args) ->
    true. %% Condition for S + Args

%% @doc kill_handoffs_next - Next state function
-spec kill_handoffs_next(S :: eqc_statem:symbolic_state(),
                         V :: eqc_statem:var(),
                         Args :: [term()]) -> eqc_statem:symbolic_state().
kill_handoffs_next(S, _Value, _Args) ->
    S#state{handoffs=[],max_concurrency=0}.

%% @doc kill_handoffs_post - Postcondition for kill_handoffs
-spec kill_handoffs_post(S :: eqc_statem:dynamic_state(),
                         Args :: [term()], R :: term()) -> true | term().
kill_handoffs_post(_S, _Args, _Res) ->
    true.

%% @doc kill_handoffs_blocking - Is the operation blocking in this State
%% -spec kill_handoffs_blocking(S :: eqc_statem:symbolic_state(),
%%         Args :: [term()]) -> boolean().
%% kill_handoffs_blocking(_S, _Args) ->
%%   false.

%% @doc kill_handoffs_adapt - How to adapt a call in this State
%% -spec kill_handoffs_adapt(S :: eqc_statem:symbolic_state(),
%%     Args :: [term()]) -> boolean().
%% kill_handoffs_adapt(_S, _Args) ->
%%   exit(adapt_not_possible).

%% @doc kill_handoffs_dynamicpre - Dynamic precondition for kill_handoffs
%% -spec kill_handoffs_dynamicpre(S :: eqc_statem:dynamic_state(),
%%         Args :: [term()]) -> boolean().
%% kill_handoffs_dynamicpre(_S, _Args) ->
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
                setup(),
                {H, S, Res} = run_commands(?MODULE,Cmds),
                teardown(),
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                Res == ok)
            end).

%% Stolen from Jordan's earlier work
setup() ->
    teardown(),
    Vars = [{ring_creation_size, 8},
            {ring_state_dir, "<nostore>"},
            {cluster_name, "test"},
            {handoff_concurrency, 2},
            {disable_outbound_handoff, false},
            {disable_inbound_handoff, false},
            %% Don't allow rolling start of vnodes as it will cause a
            %% race condition with `all_nodes'.
            {vnode_rolling_start, 0}],
    OldVars = [begin
                   Old = app_helper:get_env(riak_core, AppKey),
                   ok = application:set_env(riak_core, AppKey, Val),
                   {AppKey, Old}
               end || {AppKey, Val} <- Vars],
    %% our mock vnode don't need async pools for this test
    application:set_env(riak_core, core_vnode_eqc_pool_size, 0),
    riak_core_ring_events:start_link(), %% TODO: do we reaLly need ring events
    riak_core_ring_manager:start_link(test),
    riak_core_vnode_sup:start_link(),
    riak_core_vnode_proxy_sup:start_link(),
    riak_core_vnode_manager:start_link(),
    riak_core_handoff_manager:start_link(),
    riak_core_handoff_sender_sup:start_link(),
    riak_core_handoff_receiver_sup:start_link(),
    riak_core:register([{vnode_module, mock_vnode}]),

    meck:new(riak_core_handoff_sender),
    meck:new(riak_core_handoff_receiver),
    meck:expect(riak_core_handoff_sender, start_link,
                fun(_TargetNode, _Mod, _TypeAndOpts, _Vnode) ->
                        Pid = spawn_link(fun() ->
                                           timer:sleep(20000)
                                         end),
                        {ok, Pid}
                end),
    meck:expect(riak_core_handoff_receiver, start_link,
                fun(_SslOpts) ->
                        Pid = spawn_link(fun() ->
                                           timer:sleep(20000)
                                         end),
                        {ok, Pid}
                end),
    OldVars.

teardown() ->
    stop_pid(whereis(riak_core_ring_events)),
    stop_pid(whereis(riak_core_vnode_sup)),
    stop_pid(whereis(riak_core_vnode_proxy_sup)),
    stop_pid(whereis(riak_core_vnode_manager)),
    stop_pid(whereis(riak_core_handoff_manager)),
    stop_pid(whereis(riak_core_handoff_sender_sup)),
    stop_pid(whereis(riak_core_handoff_receiver_sup)),
    riak_core_ring_manager:stop(),
    catch meck:unload(riak_core_handoff_sender),
    catch meck:unload(riak_core_handoff_receiver).

stop_pid(undefined) ->
    ok;
stop_pid(Pid) ->
    unlink(Pid),
    exit(Pid, shutdown),
    ok = wait_for_pid(Pid).

wait_for_pid(Pid) ->
    Mref = erlang:monitor(process, Pid),
    receive
        {'DOWN',Mref,process,_,_} ->
            ok
    after
        5000 ->
            {error, didnotexit}
    end.

current_concurrency() ->
    Receivers=supervisor:count_children(riak_core_handoff_receiver_sup),
    Senders=supervisor:count_children(riak_core_handoff_sender_sup),
    ActiveReceivers=proplists:get_value(active,Receivers),
    ActiveSenders=proplists:get_value(active,Senders),
    (ActiveReceivers+ActiveSenders).
