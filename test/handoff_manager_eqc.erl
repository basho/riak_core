%%% @author John Daily <jd@epep.us>
%%% @copyright (C) 2014, John Daily
%%% @doc
%%%
%%% @end
%%% Created : 10 Oct 2014 by John Daily <jd@epep.us>

-module(handoff_manager_eqc).


%%%% YET TO DO XXX

%% More intensive comparison of handoff manager state in invariant
%% Termination/errors during transfers

%% %% Next generation
%% Resize transfer
%% Errors during transfer start

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-compile(export_all).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

%% How many times we should sleep before giving up on handoff
%% processes that we expected to terminate
-define(MAX_EXIT_WAITS, 5).

%% How many milliseconds we should sleep waiting on processes to die
-define(EXIT_WAIT, 50).

-record(state,{handoffs=[],max_concurrency=2,inbound_count=0,
               outbound_disabled=false, inbound_disabled=false}).


check() ->
    eqc:check(eqc_statem:show_states(?QC_OUT(prop_handoff_manager()))).



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

%% ------ Grouped operator: add_outbound
%% @doc add_outbound_command - Command generator
add_outbound(HOType,Module,Idx,Node,VnodePid,Opts) ->
    riak_core_handoff_manager:add_outbound(HOType,Module,Idx,Idx,Node,VnodePid,Opts).

get_vnode_pid(HashId, Module) ->
    {ok, Pid} =
        riak_core_vnode_master:get_vnode_pid(HashId, Module),
    Pid.

-spec add_outbound_args(S :: eqc_statem:symbolic_state()) ->
                               list().
add_outbound_args(_S) ->
    ?LET({HandoffType, PartID, Node, Size, SizeType},
         {eqc_gen:oneof([ownership_handoff, hinted_handoff, repair]),
          eqc_gen:oneof([0, 1, 2, 3, 4, 5, 6, 7]),
          node(),
          eqc_gen:nat(), eqc_gen:oneof([bytes, objects])},
         [ HandoffType,
           mock_vnode,
           PartID * chash:ring_increment(8),
           Node,
           {call,?MODULE,get_vnode_pid,
            [PartID * chash:ring_increment(8), mock_vnode]},
           [{size, {Size, SizeType}}]
         ]).

%% @doc add_outbound_next - Next state function

-spec add_outbound_next(S :: eqc_statem:symbolic_state(),
                        V :: eqc_statem:var(),
                        Args :: [term()]) -> eqc_statem:symbolic_state().
add_outbound_next(#state{handoffs=Handoffs,max_concurrency=MaxConcurrency}=S, Result, Args) ->
    Idx = lists:nth(3, Args),
    PreviousResults = proplists:get_value({outbound, Idx}, Handoffs, []),
    PreviousOk = has_outbound(PreviousResults),
    add_outbound_if(S, PreviousResults, PreviousOk,
                    length(valid_handoffs(Handoffs)) >= MaxConcurrency, Result,
                    Idx).

%% No existing (or no successful existing), max concurrency reached
add_outbound_if(S, _ListOfResults, false, true, _Result, _Idx) ->
    S;
%% Existing, has been started successfully
add_outbound_if(S, _ListOfResults, true, _Max, _Result, _Idx) ->
    S;
%% No match
add_outbound_if(#state{handoffs=Handoffs}=S, ListOfResults, false, false, Result, Idx) ->
    S#state{handoffs=lists:keystore({outbound, Idx}, 1,
                                    Handoffs, {{outbound, Idx}, [Result] ++ ListOfResults})}.




%% @doc add_outbound_post - Postcondition for add_outbound
-spec add_outbound_post(S :: eqc_statem:dynamic_state(),
                        Args :: [term()], R :: term()) -> true | term().
add_outbound_post(#state{handoffs=Handoffs,max_concurrency=MaxConcurrency}, _Args, {error, max_concurrency}) ->
    outbound_disabled() orelse
        eq(0, max(0, MaxConcurrency - length(valid_handoffs(Handoffs))));
add_outbound_post(_S, _Args, {ok, _Pid}) ->
    true;
add_outbound_post(_S, _Args, _Failure) ->
    false.


%% ------ Grouped operator: add_inbound
%% @doc add_inbound_command - Command generator
%% We can send an empty list as the `SSLOpts' argument to
%% `add_inbound/1` since we mock `handoff_receiver'
add_inbound(SSLOpts) ->
    riak_core_handoff_manager:add_inbound(SSLOpts).

-spec add_inbound_args(S :: eqc_statem:symbolic_state()) ->
                                 list().
add_inbound_args(_S) ->
    [[]].

%% @doc add_inbound_next - Next state function
-spec add_inbound_next(S :: eqc_statem:symbolic_state(),
                       V :: eqc_statem:var(),
                       Args :: [term()]) -> eqc_statem:symbolic_state().

add_inbound_next(S=#state{handoffs=Handoffs,max_concurrency=MaxConcurrency,inbound_count=IC}, Value, _Args) ->
    case length(valid_handoffs(Handoffs)) >= MaxConcurrency of
        true ->
            S;
        false ->
            S#state{handoffs=Handoffs++[{{inbound, IC}, Value}],
                    inbound_count=IC+1}
    end.


%% @doc add_inbound_post - Postcondition for add_inbound
-spec add_inbound_post(S :: eqc_statem:dynamic_state(),
                       Args :: [term()], R :: term()) -> true | term().
add_inbound_post(#state{handoffs=Handoffs}, _Args, {ok, _Pid}) ->
    eq(current_concurrency(inbound), length(valid_handoffs(Handoffs, inbound)) + 1);
add_inbound_post(#state{handoffs=Handoffs}, _Args, {error,max_concurrency}) ->
    inbound_disabled() orelse
        eq(current_concurrency(inbound),
           length(valid_handoffs(Handoffs, inbound))).

%% ------ Grouped operator: kill_handoffs
%% kill_handoffs is a function of limited utility, only invoked by the
%% object version upgrade/downgrade via riak_kv_reformat:run/2.
%% @doc kill_handoffs_command - Command generator
kill_handoffs() ->
    riak_core_handoff_manager:kill_handoffs().

-spec kill_handoffs_args(S :: eqc_statem:symbolic_state()) ->
                                 list().
kill_handoffs_args(_S) ->
    [].

%% @doc kill_handoffs_next - Next state function
-spec kill_handoffs_next(S :: eqc_statem:symbolic_state(),
                         V :: eqc_statem:var(),
                         Args :: [term()]) -> eqc_statem:symbolic_state().
kill_handoffs_next(S, _Value, _Args) ->
    S#state{handoffs=[],max_concurrency=0}.

%% @doc kill_handoffs_post - Postcondition for kill_handoffs
-spec kill_handoffs_post(S :: eqc_statem:dynamic_state(),
                         Args :: [term()], R :: term()) -> true | term().
kill_handoffs_post(_S, _Args, ok) ->
    try_wait(current_concurrency() =< 0, 0, ?MAX_EXIT_WAITS).


%% ------ Grouped operator: set_concurrency
%% @doc set_concurrency_command - Command generator
set_concurrency(Limit) ->
    riak_core_handoff_manager:set_concurrency(Limit).

-spec set_concurrency_args(S :: eqc_statem:symbolic_state()) ->
                                 list(integer()).
set_concurrency_args(_S) ->
     [max_concurrency()].

%% @doc set_concurrency_next - Next state function
-spec set_concurrency_next(S :: eqc_statem:symbolic_state(),
                           V :: eqc_statem:var(),
                           Args :: [term()]) -> eqc_statem:symbolic_state().
set_concurrency_next(S=#state{handoffs=HS}, _Value,
                     [Limit]) ->
    ValidHandoffs = valid_handoffs(HS),
    case (length(ValidHandoffs) > Limit) of
        true ->
            {Kept, _Discarded} = lists:split(Limit, ValidHandoffs),
            S#state{max_concurrency=Limit,handoffs=Kept};
        false ->
            S#state{max_concurrency=Limit}
    end.

%% @doc set_concurrency_post - Postcondition for set_concurrency
%% Must account for possibility that exiting handoff processes will still be around when this is invoked.
-spec set_concurrency_post(S :: eqc_statem:dynamic_state(),
                           Args :: [term()], R :: term()) -> true | term().
set_concurrency_post(_S,
                     [MaxConcurrency],
                     ok) ->
    %% Dumb method to wait for handoff workers to exit
    try_wait(current_concurrency() =< MaxConcurrency, MaxConcurrency, ?MAX_EXIT_WAITS).

try_wait(true, _MaxConcurrency, _Attempt) ->
    true;
try_wait(false, _MaxConcurrency, Attempt) when Attempt =< 0 ->
    false;
try_wait(false, MaxConcurrency, Attempt) ->
    timer:sleep(?EXIT_WAIT),
    try_wait(current_concurrency() =< MaxConcurrency, MaxConcurrency, Attempt - 1).

%% ------ Grouped operator: get_concurrency
%% @doc get_concurrency_command - Command generator
get_concurrency() ->
    riak_core_handoff_manager:get_concurrency().

-spec get_concurrency_args(S :: eqc_statem:symbolic_state()) ->
                                  list().
get_concurrency_args(_S) ->
    [].

%% @doc get_concurrency_next - Next state function
-spec get_concurrency_next(S :: eqc_statem:symbolic_state(),
                           V :: eqc_statem:var(),
                           Args :: [term()]) -> eqc_statem:symbolic_state().
get_concurrency_next(S, _Value, _Args) ->
    S.

%% @doc get_concurrency_post - Postcondition for get_concurrency
-spec get_concurrency_post(S :: eqc_statem:dynamic_state(),
                           Args :: [term()], R :: term()) -> true | term().
get_concurrency_post(#state{max_concurrency=ExpectedConcurrency}, [],
                     MaxConcurrency) ->
    ExpectedConcurrency =:= MaxConcurrency andalso
        current_concurrency() =< MaxConcurrency.

%% ------ Grouped operator: enable_outbound
%% @doc enable_outbound_command - Command generator

enable_outbound() ->
    application:set_env(riak_core, disable_outbound_handoff, false).

-spec enable_outbound_args(S :: eqc_statem:symbolic_state()) ->
                                  list().
enable_outbound_args(_S) ->
    [].

%% @doc enable_outbound_next - Next state function
-spec enable_outbound_next(S :: eqc_statem:symbolic_state(),
                           V :: eqc_statem:var(),
                           Args :: [term()]) -> eqc_statem:symbolic_state().
enable_outbound_next(S, _Value, _Args) ->
    S#state{outbound_disabled=false}.

%% @doc enable_outbound_post - Postcondition for enable_outbound
-spec enable_outbound_post(S :: eqc_statem:dynamic_state(),
                           Args :: [term()], R :: term()) -> true | term().
enable_outbound_post(_S, _Args, ok) ->
    not outbound_disabled().


%% ------ Grouped operator: disable_outbound
%% @doc disable_outbound_command - Command generator

disable_outbound() ->
    application:set_env(riak_core, disable_outbound_handoff, true).

-spec disable_outbound_args(S :: eqc_statem:symbolic_state()) ->
                                  list().
disable_outbound_args(_S) ->
    [].

%% @doc disable_outbound_next - Next state function
-spec disable_outbound_next(S :: eqc_statem:symbolic_state(),
                           V :: eqc_statem:var(),
                           Args :: [term()]) -> eqc_statem:symbolic_state().
disable_outbound_next(S, _Value, _Args) ->
    S#state{outbound_disabled=true}.

%% @doc disable_outbound_post - Postcondition for disable_outbound
-spec disable_outbound_post(S :: eqc_statem:dynamic_state(),
                           Args :: [term()], R :: term()) -> true | term().
disable_outbound_post(_S, _Args, ok) ->
    outbound_disabled().



%% ------ Grouped operator: enable_inbound
%% @doc enable_inbound_command - Command generator

enable_inbound() ->
    application:set_env(riak_core, disable_inbound_handoff, false).

-spec enable_inbound_args(S :: eqc_statem:symbolic_state()) ->
                                  list().
enable_inbound_args(_S) ->
    [].

%% @doc enable_inbound_next - Next state function
-spec enable_inbound_next(S :: eqc_statem:symbolic_state(),
                           V :: eqc_statem:var(),
                           Args :: [term()]) -> eqc_statem:symbolic_state().
enable_inbound_next(S, _Value, _Args) ->
    S#state{inbound_disabled=false}.

%% @doc enable_inbound_post - Postcondition for enable_inbound
-spec enable_inbound_post(S :: eqc_statem:dynamic_state(),
                           Args :: [term()], R :: term()) -> true | term().
enable_inbound_post(_S, _Args, ok) ->
    not inbound_disabled().


%% ------ Grouped operator: disable_inbound
%% @doc disable_inbound_command - Command generator

disable_inbound() ->
    application:set_env(riak_core, disable_inbound_handoff, true).

-spec disable_inbound_args(S :: eqc_statem:symbolic_state()) ->
                                  list().
disable_inbound_args(_S) ->
    [].

%% @doc disable_inbound_next - Next state function
-spec disable_inbound_next(S :: eqc_statem:symbolic_state(),
                           V :: eqc_statem:var(),
                           Args :: [term()]) -> eqc_statem:symbolic_state().
disable_inbound_next(S, _Value, _Args) ->
    S#state{inbound_disabled=true}.

%% @doc disable_inbound_post - Postcondition for disable_inbound
-spec disable_inbound_post(S :: eqc_statem:dynamic_state(),
                           Args :: [term()], R :: term()) -> true | term().
disable_inbound_post(_S, _Args, ok) ->
    inbound_disabled().




%% ------ ... more operations

%% @doc <i>Optional callback</i>, Invariant, checked for each visited state
%%      during test execution.
-spec invariant(S :: eqc_statem:dynamic_state()) -> boolean().
invariant(#state{handoffs=Handoffs, max_concurrency=Concurrency,
                 outbound_disabled=OutboundDisabled,
                 inbound_disabled=InboundDisabled
                }) ->
    eq(0, max(length(valid_handoffs(Handoffs)) - Concurrency, 0))
        andalso eq(outbound_disabled(), OutboundDisabled)
        andalso eq(inbound_disabled(), InboundDisabled).

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
                aggregate(command_names(Cmds),
                          pretty_commands(?MODULE, Cmds, {H, S, Res},
                                Res == ok))
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
    riak_core_eventhandler_sup:start_link(),
    riak_core_vnode_sup:start_link(),
    riak_core_eventhandler_sup:start_link(),
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

current_concurrency(inbound) ->
    Receivers=supervisor:count_children(riak_core_handoff_receiver_sup),
    ActiveReceivers=proplists:get_value(active,Receivers),
    ActiveReceivers;
current_concurrency(outbound) ->
    Senders=supervisor:count_children(riak_core_handoff_sender_sup),
    ActiveSenders=proplists:get_value(active,Senders),
    ActiveSenders.

max_concurrency() ->
    choose(0, 10).

valid_handoffs(HS, Direction) ->
    lists:filter(
      fun({{HandoffDirection, _}, _}) when HandoffDirection =:= Direction ->
              true;
         (_) ->
              false
      end,
      valid_handoffs(HS)).

valid_handoffs(HS) ->
    lists:filter(
      fun({{inbound, _Idx}, {ok, _}}) ->
              true;
         ({{inbound, _Idx}, _Error}) ->
              false;
         ({{outbound, _Idx}, ReturnVals}) ->
              has_outbound(ReturnVals)
      end,
      HS).

%% check if return values from add_outbound/4 contain
%% a {ok, SenderPid}
has_outbound(ReturnVals) ->
    lists:keymember(ok, 1, ReturnVals).

inbound_disabled() ->
    {ok, Bool} = application:get_env(riak_core, disable_inbound_handoff),
    Bool.

outbound_disabled() ->
    {ok, Bool} = application:get_env(riak_core, disable_outbound_handoff),
    Bool.
