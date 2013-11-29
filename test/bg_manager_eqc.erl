%%% @author Jordan West <>
%%% @copyright (C) 2013, Jordan West
%%% @doc
%%%
%%% @end
%%% Created : 13 Nov 2013 by Jordan West <>

-module(bg_manager_eqc).

%%-ifdef(TEST).
%% -ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-compile(export_all).

-type bg_eqc_type() :: atom().
-type bg_eqc_limit() :: non_neg_integer().

-record(state,{
          %% whether or not the bgmgr is running
          alive  :: boolean(),
          %% processes started by the test and the processes state
          procs  :: [{pid(), running | not_running}],
          %% concurrency limits for lock types
          limits :: [{bg_eqc_type(), bg_eqc_limit()}],
          %% max counts per "period" for token types
          counts :: [{bg_eqc_type(), bg_eqc_limit()}],
          %% locks held (or once held then released) for each lock type
          %% and their state
          locks  :: [{bg_eqc_type(), [{reference(), pid(), [], held | released}]}],
          %% number of tokens taken by type
          tokens :: [{bg_eqc_type(), non_neg_integer()}]

         }).

run_eqc() ->
    run_eqc(100).

run_eqc(Type) when is_atom(Type) ->
    run_eqc(100, Type);
run_eqc(N) ->
    run_eqc(N, simple).

run_eqc(N, simple) ->
    run_eqc(N, prop_bgmgr());
run_eqc(N, para) ->
    run_eqc(N, parallel);
run_eqc(N, parallel) ->
    run_eqc(N, prop_bgmgr_parallel());
run_eqc(N, Prop) ->
    eqc:quickcheck(eqc:numtests(N, Prop)).

run_check() ->
    eqc:check(prop_bgmgr()).

run_recheck() ->
    eqc:recheck(prop_bgmgr()).


%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
initial_state() ->
    #state{
       alive  = true,
       procs  = [],
       limits = [],
       counts = [],
       locks  = [],
       tokens = []
      }.

%% ------ Grouped operator: set_concurrency_limit
%% @doc set_concurrency_limit_command - Command generator
set_concurrency_limit_args(_S) ->
    %% TODO: change Kill (3rd arg, to boolean gen)
    [lock_type(), lock_limit(), false].

%% @doc set_concurrency_limit precondition
set_concurrency_limit_pre(S) ->
    is_alive(S).

%% @doc set_concurreny_limit command
set_concurrency_limit(Type, Limit, false) ->
    riak_core_bg_manager:set_concurrency_limit(Type, Limit).

%% @doc state transition for set_concurrency_limit command
set_concurrency_limit_next(S=#state{limits=Limits}, _Value, [Type,Limit,_Kill]) ->
    S#state{ limits = lists:keystore(Type, 1, Limits, {Type, Limit}) }.

%% @doc set_concurrency_limit_post - Postcondition for set_concurrency_limit
set_concurrency_limit_post(S, [Type,_Limit,_Kill], Res) ->
    %% check returned value is equal to value we have in state prior to this call
    %% since returned value is promised to be previous one that was set
    eq(limit(Type, S), Res).

%% ------ Grouped operator: concurrency_limit
%% @doc concurrency_limit command arguments generator
concurrency_limit_args(_S) ->
    [lock_type()].

%% @doc concurrency_limit precondition
concurrency_limit_pre(S) ->
    is_alive(S).

%% @doc concurrency limit command
concurrency_limit(Type) ->
    riak_core_bg_manager:concurrency_limit(Type).

%% @doc Postcondition for concurrency_limit
concurrency_limit_post(S, [Type], Limit) ->
    ExpectedLimit = limit(Type, {unregistered, Type}, S),
    eq(ExpectedLimit, Limit).

%% ------ Grouped operator: concurrency_limit_reached
%% @doc concurrency_limit_reached command argument generator
concurrency_limit_reached_args(_S) ->
    [lock_type()].

%% @doc concurrency_limit_reached precondition
concurrency_limit_reached_pre(S) ->
    is_alive(S).

%% @doc concurrency_limit_reached command
concurrency_limit_reached(Type) ->
    riak_core_bg_manager:concurrency_limit_reached(Type).

%% @doc concurrency_limit_reached_post - Postcondition for concurrency_limit_reached
concurrency_limit_reached_post(S, [Type], {unregistered, Type}) ->
    eq(limit(Type, undefined, S), undefined);
concurrency_limit_reached_post(S, [Type], Res) ->
    Limit = limit(Type, S),
    ExistingCount = length(held_locks(Type, S)),
    eq(ExistingCount >= Limit, Res).


%% ------ Grouped operator: get_lock
%% @doc argument generator for get_lock command
get_lock_args(S) ->
    %% TODO: test getting locks on behalf of calling process instead of other process
    %% TODO: test trying to get lock on behalf of killed process?
    [lock_type(), oneof(running_procs(S)), []].

%% @doc Precondition for generation of get_lock command
get_lock_pre(S) ->
    %% need some running procs to get locks on behalf of
    RunningProcs = length(running_procs(S)) > 0,
    RunningProcs andalso is_alive(S).

%% @doc Precondition for generation of get_lock command
get_lock_pre(S, [Type, _Pid, _Meta]) ->
    %% must call set_concurrency_limit at least once
    %% TODO: we can probably remove and test this restriction instead
    is_integer(limit(Type, undefined, S)).

get_lock(Type, Pid, Meta) ->
    case riak_core_bg_manager:get_lock(Type, Pid, Meta) of
        {ok, Ref} -> Ref;
        Other -> Other
    end.


%% @doc State transition for get_lock command
%% `Res' is either the lock reference or max_concurrency
get_lock_next(S, Res, [Type, Pid, Meta]) ->
    TypeLimit = limit(Type, S),
    Held = held_locks(Type, S),
    case length(Held) < TypeLimit of
        %% got lock
        true -> add_held_lock(Type, Res, Pid, Meta, S);
        %% failed to get lock
        false -> S
    end.

%% @doc Postcondition for get_lock
get_lock_post(S, [Type, _Pid, _Meta], max_concurrency) ->
    %% Since S reflects the state before we check that it
    %% was already at the limit.
    Limit = limit(Type, S),
    ExistingCount = length(held_locks(Type, S)),
    %% check >= because we may have lowered limit *without*
    %% forcing some processes to release their locks by killing them
    case ExistingCount >= Limit of
        true -> true;
        false ->
            %% hack to get more informative post-cond failure (like eq)
            {ExistingCount, 'not >=', Limit}
    end;
get_lock_post(S, [Type, _Pid, _Meta], _LockRef) ->
    %% Since S reflects the state before we check that it
    %% was not already at the limit.
    Limit = limit(Type, S),
    ExistingCount = length(held_locks(Type, S)),
    case ExistingCount <  Limit of
        true -> true;
        false ->
            %% hack to get more informative post-cond failure (like eq)
            {ExistingCount, 'not <', Limit}
    end.

%% ------ Grouped operator: start_process
%% @doc args generator for start_process
start_process_args(_S) ->
    [].

%% @doc start_process_pre - Precondition for generation
start_process_pre(S) ->
    %% limit the number of running processes in the test, we should need an unbounded amount
    %% TODO: move "20" to define
    length(running_procs(S)) < 5.

start_process() ->
    spawn(fun() ->
                  receive die -> ok
                  %% this protects us against leaking too many processes when running
                  %% prop_bgmgr_parallel(), which doesn't clean up the processes it starts
                  after 360000 -> timeout
                  end
          end).

%% @doc state transition for start_process command
start_process_next(S=#state{procs=Procs}, Value, []) ->
    S#state{ procs = lists:keystore(Value, 1, Procs, {Value, running}) }.

%% @doc postcondition for start_process
start_process_post(_S, [], Pid)->
    is_process_alive(Pid).

%% ------ Grouped operator: stop_process
%% @doc stop_process_command - Argument generator
stop_process_args(S) ->
    [oneof(running_procs(S))].

%% @doc stop_process_pre - Precondition for generation
stop_process_pre(S) ->
    %% need some running procs in order to kill one.
    length(running_procs(S)) > 0.

%% @doc stop_process_pre - Precondition for stop_process
stop_process_pre(S, [Pid]) ->
    %% only interesting to kill processes that hold locks
    lists:keyfind(Pid, 2, all_locks(S)) /= false.

%% @doc stop_process command
stop_process(Pid) ->
    Pid ! die,
    wait_for_pid(Pid).

%% @doc state transition for stop_process command
stop_process_next(S=#state{procs=Procs}, _Value, [Pid]) ->
    %% mark process as no longer running and release all locks held by the process
    UpdatedProcs = lists:keystore(Pid, 1, Procs, {Pid, not_running}),
    release_locks(Pid, S#state{procs = UpdatedProcs}).


%% @doc postcondition for stop_process
stop_process_post(_S, [Pid], ok) ->
    not is_process_alive(Pid);
stop_process_post(_S, [Pid], {error, didnotexit}) ->
    {error, {didnotexit, Pid}}.

%% ------ Grouped operator: set_token_rate
%% @doc set_token_rate arguments generator
set_token_rate_args(_S) ->
    %% NOTE: change token_type() to lock_type() to provoke failure due to registration of lock/token under same name
    %% (waiting for fix in bg mgr).
    [token_type(), token_count()].

%% @doc set_token_rate precondition
set_token_rate_pre(S) ->
    is_alive(S).

%% @doc set_token_rate state transition
set_token_rate_next(S=#state{counts=Counts}, _Value, [Type, Count]) ->
    S#state{ counts = lists:keystore(Type, 1, Counts, {Type, Count}) }.

%% @doc set_token_rate command
set_token_rate(Type, Count) ->
    %% we refill tokens as a command in the model so we use
    %% token rate to give us the biggest refill period we can get.
    %% no test should run longer than that or we have a problem.
    riak_core_bg_manager:set_token_rate(Type, mk_token_rate(Count)).

%% @doc Postcondition for set_token_rate
set_token_rate_post(S, [Type, _Count], Res) ->
    %% check returned value is equal to value we have in state prior to this call
    %% since returned value is promised to be previous one that was set
    eq(Res, mk_token_rate(max_num_tokens(Type, 0, S))).

%% ------ Grouped operator: token_rate
%% @doc token_rate_command
token_rate_args(_S) ->
    [token_type()].

%% @doc token_rate precondition
token_rate_pre(S) ->
    is_alive(S).

%% @doc token_rate command
token_rate(Type) ->
    riak_core_bg_manager:token_rate(Type).

%% @doc Postcondition for token_rate
token_rate_post(S, [Type], Res) ->
    ExpectedRate = mk_token_rate(max_num_tokens(Type, {unregistered, Type}, S)),
    eq(ExpectedRate, Res).

%% ------ Grouped operator: get_token
%% @doc get_token args generator
get_token_args(S) ->
    %% TODO: generate meta for future query tests
    ArityTwo = [[token_type(), oneof(running_procs(S))] || length(running_procs(S)) > 0],
    ArityOne = [[token_type()]],
    oneof(ArityTwo ++ ArityOne).

%% @doc Precondition for get_token
get_token_pre(S, [Type, _Pid]) ->
    get_token_pre(S, [Type]);
get_token_pre(S, [Type]) ->
    %% must call set_token_rate at least once
    %% TODO: we can probably remove and test this restriction instead
    is_integer(max_num_tokens(Type, undefined, S)) andalso is_alive(S).

%% @doc get_token state transition
get_token_next(S, Value, [Type, _Pid]) ->
    get_token_next(S, Value, [Type]);
get_token_next(S, _Value, [Type]) ->
    CurCount = num_tokens(Type, S),
    %% NOTE: this assumes the precondition requires we call set_token_rate at least once
    %% in case we don't we treat the max as 0
    Max = max_num_tokens(Type, 0, S),
    case CurCount < Max of
        true -> increment_token_count(Type, S);
        false -> S
    end.

get_token(Type) ->
    riak_core_bg_manager:get_token(Type).

get_token(Type, Pid) ->
    riak_core_bg_manager:get_token(Type, Pid).

%% @doc Postcondition for get_token
get_token_post(S, [Type, _Pid], Res) ->
    get_token_post(S, [Type], Res);
get_token_post(S, [Type], max_concurrency) ->
    CurCount = num_tokens(Type, S),
    %% NOTE: this assumes the precondition requires we call set_token_rate at least once
    %% in case we don't we treat the max as 0
    Max = max_num_tokens(Type, 0, S),
    case CurCount >= Max of
        true -> true;
        false ->
            %% hack to get more info out of postcond failure
            {CurCount, 'not >=', Max}
    end;
get_token_post(S, [Type], ok) ->
    CurCount = num_tokens(Type, S),
    %% NOTE: this assumes the precondition requires we call set_token_rate at least once
    %% in case we don't we treat the max as 0
    Max = max_num_tokens(Type, 0, S),
    case CurCount < Max of
        true -> true;
        false ->
            {CurCount, 'not <', Max}
    end.

%% ------ Grouped operator: refill_tokens
%% @doc refill_tokens args generator
refill_tokens_args(_S) ->
    [token_type()].

%% @doc refill_tokens precondition
refill_tokens_pre(S, [Type]) ->
    %% only refill tokens if we have registered type (called set_token_rate at least once)
    is_integer(max_num_tokens(Type, undefined, S)) andalso is_alive(S).

%% @doc refill_tokens state transition
refill_tokens_next(S, _Value, [Type]) ->
    reset_token_count(Type, S).

refill_tokens(Type) ->
    riak_core_bg_manager ! {refill_tokens, Type},
    %% TODO: find way to get rid of this timer sleep
    timer:sleep(100).


%% ------ Grouped operator: crash
%% @doc crash args generator
crash_args(_S) ->
    [].

%% @doc precondition for crash command
crash_pre(#state{alive=Alive}) ->
    %% only crash if actually running
    Alive.

%% @doc state transition for crash command
crash_next(S, _Value, _Args) ->
    S#state{ alive = false }.

%% @doc crash command
crash() ->
    stop_pid(whereis(riak_core_bg_manager)).

%% @doc crash command post condition
crash_post(_S, _Args, _Res) ->
    %% TODO: anything we want to validate here?
    true.

%% ------ Grouped operator: revive
%% @doc revive arguments generator
revive_args(_S) ->
    [].

%% @doc revive precondition
revive_pre(#state{alive=Alive}) ->
    %% only revive if we are in a crashed state
    not Alive.

%% @doc revive_next - Next state function
revive_next(S, _Value, _Args) ->
    S#state{ alive = true }.

%% @doc revive command
revive() ->
    {ok, BgMgr} = riak_core_bg_manager:start_link(),
    unlink(BgMgr).

%% @doc revive_post - Postcondition for revive
revive_post(_S, _Args, _Res) ->
    %% TODO: what to validate here, if anything?
    true.


%% -- Generators
lock_type() ->
    oneof([a,b,c,d]). %%,e,f,g,h,i]).

token_type() ->
    oneof(['A','B','C','D']). %%,'E','F','G','H','I']).

lock_limit() ->
    choose(0, 5).

token_count() ->
    choose(0, 5).

%% @doc weight/2 - Distribution of calls
weight(_S, set_concurrency_limit) -> 3;
weight(_S, concurrency_limit) -> 3;
weight(_S, concurrency_limit_reached) -> 3;
weight(_S, start_process) -> 3;
weight(#state{alive=true}, stop_process) -> 5;
weight(#state{alive=false}, stop_process) -> 50;
weight(_S, get_lock) -> 20;
weight(_S, set_token_rate) -> 3;
weight(_S, token_rate) -> 0;
weight(_S, get_token) -> 20;
weight(_S, refill_tokens) -> 10;
weight(_S, crash) -> 3;
weight(_S, revive) -> 1;
weight(_S, _Cmd) -> 1.

%% Other Functions
limit(Type, State) ->
    limit(Type, 0, State).

limit(Type, Default, #state{limits=Limits}) ->
    case lists:keyfind(Type, 1, Limits) of
        false -> Default;
        {Type, Limit} -> Limit
    end.

num_tokens(Type, #state{tokens=Tokens}) ->
    case lists:keyfind(Type, 1, Tokens) of
        false -> 0;
        {Type, NumTokens} -> NumTokens
    end.

max_num_tokens(Type, Default, #state{counts=Counts}) ->
    case lists:keyfind(Type, 1, Counts) of
        false -> Default;
        {Type, Limit} -> Limit
    end.

is_alive(#state{alive=Alive}) ->
    Alive.

mk_token_rate({unregistered, _}=Unreg) ->
    Unreg;
mk_token_rate(0) ->
    undefined;
mk_token_rate(Count) ->
    %% 4294967295 is erlang:send_after max which is used for token refilling
    {4294967295, Count}.

running_procs(#state{procs=Procs}) ->
    [Pid || {Pid, running} <- Procs].

all_locks(#state{locks=Locks}) ->
    lists:flatten([ByType || {_Type, ByType} <- Locks]).

all_locks(Type, #state{locks=Locks}) ->
    case lists:keyfind(Type, 1, Locks) of
        false -> [];
        {Type, All} -> All
    end.

held_locks(Type, State) ->
    [{Ref, Pid, Meta, held} || {Ref, Pid, Meta, held} <- all_locks(Type, State)].

update_locks(Type, TypeLocks, State=#state{locks=Locks}) ->
    State#state{ locks = lists:keystore(Type, 1, Locks, {Type, TypeLocks}) }.

add_held_lock(Type, Ref, Pid, Meta, State) ->
    All = all_locks(Type, State),
    update_locks(Type, [{Ref, Pid, Meta, held} | All], State).

release_locks(Pid, State=#state{locks=Locks}) ->
    lists:foldl(fun({Type, ByType}, StateAcc) ->
                        NewLocks = mark_locks_released(Pid, ByType),
                        update_locks(Type, NewLocks, StateAcc)
                end,
                State, Locks).

mark_locks_released(Pid, Locks) ->
    WithoutPid = [Lock || Lock <- Locks, element(2, Lock) =/= Pid],
    MarkedReleased = [{Ref, LockPid, Meta, released} || {Ref, LockPid, Meta, _}  <- Locks, LockPid =:= Pid],
    MarkedReleased ++ WithoutPid.

increment_token_count(Type, State=#state{tokens=Tokens}) ->
    CurCount = num_tokens(Type, State),
    State#state{ tokens = lists:keystore(Type, 1, Tokens, {Type, CurCount + 1}) }.

reset_token_count(Type, State=#state{tokens=Tokens}) ->
    State#state{ tokens = lists:keystore(Type, 1, Tokens, {Type, 0}) }.


stop_pid(Other) when not is_pid(Other) ->
    ok;
stop_pid(Pid) ->
    unlink(Pid),
    exit(Pid, shutdown),
    ok = wait_for_pid(Pid).

wait_for_pid(Pid) ->
    Mref = erlang:monitor(process, Pid),
    receive
        {'DOWN', Mref, process, _, _} ->
            ok
    after
        5000 ->
            {error, didnotexit}
    end.

bg_manager_monitors() ->
    bg_manager_monitors(whereis(riak_core_bg_manager)).

bg_manager_monitors(undefined) ->
    crashed;
bg_manager_monitors(Pid) ->
    process_info(Pid, monitors).

prop_bgmgr() ->
    ?FORALL(Cmds, commands(?MODULE),
            aggregate(command_names(Cmds),
                      ?TRAPEXIT(
                         begin
                             stop_pid(whereis(riak_core_table_manager)),
                             stop_pid(whereis(riak_core_bg_manager)),
                             {ok, _TableMgr} = riak_core_table_manager:start_link([{background_mgr_table,
                                                                                   [protected, bag, named_table]}]),
                             {ok, BgMgr} = riak_core_bg_manager:start_link(),
                             %% unlink bgmgr so we can crash it, unfortunately we won't surface non-intentional
                             %% crashes as well :(
                             unlink(BgMgr),
                             {H, S, Res} = run_commands(?MODULE,Cmds),
                             Table = ets:tab2list(background_mgr_table),
                             Monitors = bg_manager_monitors(),
                             RunnngPids = running_procs(S),
                             %% cleanup processes not killed during test
                             [stop_pid(Pid) || Pid <- RunnngPids],
                             stop_pid(whereis(riak_core_table_manager)),
                             stop_pid(whereis(riak_core_bg_manager)),
                             ?WHENFAIL(
                                begin
                                    io:format("~n~nFinal State: ~n"),
                                    io:format("---------------~n"),
                                    io:format("alive = ~p~n", [S#state.alive]),
                                    io:format("procs = ~p~n", [S#state.procs]),
                                    io:format("limits = ~p~n", [S#state.limits]),
                                    io:format("locks = ~p~n", [S#state.locks]),
                                    io:format("counts = ~p~n", [S#state.counts]),
                                    io:format("tokens = ~p~n", [S#state.tokens]),
                                    io:format("---------------~n"),
                                    io:format("~n~nbackground_mgr_table: ~n"),
                                    io:format("---------------~n"),
                                    io:format("~p~n", [Table]),
                                    io:format("---------------~n"),
                                    io:format("~n~nbg_manager monitors: ~n"),
                                    io:format("---------------~n"),
                                    io:format("~p~n", [Monitors]),
                                    io:format("---------------~n")

                                end,
                                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                                Res == ok))
                         end))).


prop_bgmgr_parallel() ->
    ?FORALL(Cmds, parallel_commands(?MODULE),
            aggregate(command_names(Cmds),
                      ?TRAPEXIT(
                         begin
                             stop_pid(whereis(riak_core_table_manager)),
                             stop_pid(whereis(riak_core_bg_manager)),
                             {ok, TableMgr} = riak_core_table_manager:start_link([{background_mgr_table,
                                                                                   [protected, bag, named_table]}]),
                             {ok, BgMgr} = riak_core_bg_manager:start_link(),
                             %% unlink bgmgr so we can crash it, unfortunately we won't surface non-intentional
                             %% crashes as well :(
                             unlink(BgMgr),
                             {Seq, Par, Res} = run_parallel_commands(?MODULE,Cmds),
                             Table = ets:tab2list(background_mgr_table),
                             Monitors = bg_manager_monitors(),
                             stop_pid(TableMgr),
                             stop_pid(BgMgr),
                             ?WHENFAIL(
                                begin
                                    io:format("~n~nbackground_mgr_table: ~n"),
                                    io:format("---------------~n"),
                                    io:format("~p~n", [Table]),
                                    io:format("---------------~n"),
                                    io:format("~n~nbg_manager monitors: ~n"),
                                    io:format("---------------~n"),
                                    io:format("~p~n", [Monitors]),
                                    io:format("---------------~n")
                                end,
                                pretty_commands(?MODULE, Cmds, {Seq, Par, Res},
                                                Res == ok))
                         end))).

%% -endif.
%% -endif.
