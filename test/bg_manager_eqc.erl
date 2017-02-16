%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(bg_manager_eqc).

-ifdef(TEST).
-ifdef(EQC).

-include("riak_core_bg_manager.hrl").
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-compile(export_all).

-type bg_eqc_type() :: atom().
-type bg_eqc_limit() :: non_neg_integer().

-record(state,{
          %% whether or not the bgmgr is running
          alive  :: boolean(),
          %% whether or not the global bypass switch is engaged
          bypassed :: boolean(),
          %% whether or not the global enable switch is engaged
          enabled :: boolean(),
          %% processes started by the test and the processes state
          procs  :: [{pid(), running | not_running}],
          %% resources that are disabled are on the list
          disabled :: [{bg_eqc_type()}],
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

bgmgr_test_() ->
    {timeout, 60,
     fun() ->
              ?assert(eqc:quickcheck(?QC_OUT(eqc:testing_time(30, prop_bgmgr()))))
     end
    }.

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
       bypassed = false,
       enabled = true,
       disabled = [],
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
    is_integer(limit(Type, unregistered, S)).

get_lock(Type, Pid, Meta) ->
    case riak_core_bg_manager:get_lock(Type, Pid, Meta) of
        {ok, Ref} -> Ref;
        Other -> Other
    end.


%% @doc State transition for get_lock command
%% `Res' is either the lock reference or max_concurrency
get_lock_next(S=#state{enabled=Enabled, bypassed=Bypassed}, Res, [Type, Pid, Meta]) ->
    TypeLimit = limit(Type, S),
    Held = held_locks(Type, S),
    ReallyEnabled = Enabled andalso resource_enabled(Type, S),
    case (ReallyEnabled andalso length(Held) < TypeLimit) orelse Bypassed of
        %% got lock
        true -> add_held_lock(Type, Res, Pid, Meta, S);
        %% failed to get lock
        false -> S
    end.

%% @doc Postcondition for get_lock
%% We expect to get max_concurrency if globally disabled or we hit the limit.
%% We expect to get ok if bypassed or under the limit.
get_lock_post(#state{bypassed=true}, [_Type, _Pid, _Meta], max_concurrency) ->
    'max_concurrency returned when bypassed';
get_lock_post(S=#state{enabled=Enabled}, [Type, _Pid, _Meta], max_concurrency) ->
    %% Since S reflects the state before we check that it
    %% was already at the limit.
    Limit = limit(Type, S),
    ExistingCount = length(held_locks(Type, S)),
    %% check >= because we may have lowered limit *without*
    %% forcing some processes to release their locks by killing them
    ReallyEnabled = Enabled andalso resource_enabled(Type, S),
    case (not ReallyEnabled) orelse ExistingCount >= Limit of
        true -> true;
        false ->
            %% hack to get more informative post-cond failure (like eq)
            {ExistingCount, 'not >=', Limit}
    end;
get_lock_post(S=#state{bypassed=Bypassed, enabled=Enabled}, [Type, _Pid, _Meta], _LockRef) ->
    %% Since S reflects the state before we check that it
    %% was not already at the limit.
    Limit = limit(Type, S),
    ExistingCount = length(held_locks(Type, S)),
    ReallyEnabled = Enabled andalso resource_enabled(Type, S),
    case (ReallyEnabled andalso ExistingCount <  Limit) orelse Bypassed of
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
    Res = riak_core_test_util:wait_for_pid(Pid),
    %% while not part of the test, this provides extra insurance that the
    %% background manager receives the monitor message for the failed pid
    %% (waiting for the test process to receive its monitor message is not
    %% enough). This relies on local erlang message semantics a bit and may
    %% not be bullet proof. The catch handles the case where the bg manager
    %% has been crashed by the test.
    catch riak_core_bg_manager:enabled(),
    Res.

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
%% Note that set_token_rate takes a rate, which is {Period, Count},
%% but this test generates it's own refill messages, so rate is not modeled.
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
    eq(Res, mk_token_rate(max_num_tokens(Type, undefined, S))).

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
    is_integer(max_num_tokens(Type, unregistered, S)) andalso is_alive(S).

%% @doc get_token state transition
get_token_next(S, Value, [Type, _Pid]) ->
    get_token_next(S, Value, [Type]);
get_token_next(S=#state{bypassed=Bypassed, enabled=Enabled}, _Value, [Type]) ->
    CurCount = num_tokens(Type, S),
    %% NOTE: this assumes the precondition requires we call set_token_rate at least once
    %% in case we don't we treat the max as 0
    Max = max_num_tokens(Type, unregistered, S),
    ReallyEnabled = Enabled andalso resource_enabled(Type, S),
    case (ReallyEnabled andalso CurCount < Max) orelse Bypassed of
        true -> increment_token_count(Type, S);
        false -> S
    end.

get_token(Type) ->
    riak_core_bg_manager:get_token(Type).

get_token(Type, Pid) ->
    riak_core_bg_manager:get_token(Type, Pid).

%% @doc Postcondition for get_token
%% We expect to get max_concurrency if globally disabled or we hit the limit.
%% We expect to get ok if bypassed or under the limit.
get_token_post(S, [Type, _Pid], Res) ->
    get_token_post(S, [Type], Res);
get_token_post(#state{bypassed=true}, [_Type], max_concurrency) ->
    'max_concurrency returned while bypassed';
get_token_post(S=#state{enabled=Enabled}, [Type], max_concurrency) ->
    CurCount = num_tokens(Type, S),
    %% NOTE: this assumes the precondition requires we call set_token_rate at least once
    %% in case we don't we treat the max as 0
    Max = max_num_tokens(Type, unregistered, S),
    ReallyEnabled = Enabled andalso resource_enabled(Type, S),
    case (not ReallyEnabled) orelse CurCount >= Max of
        true -> true;
        false ->
            %% hack to get more info out of postcond failure
            {CurCount, 'not >=', Max}
    end;
get_token_post(S=#state{bypassed=Bypassed, enabled=Enabled}, [Type], ok) ->
    CurCount = num_tokens(Type, S),
    %% NOTE: this assumes the precondition requires we call set_token_rate at least once
    %% in case we don't we treat the max as 0
    Max = max_num_tokens(Type, unregistered, S),
    ReallyEnabled = Enabled andalso resource_enabled(Type, S),
    case (ReallyEnabled andalso CurCount < Max) orelse Bypassed of
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
    is_integer(max_num_tokens(Type, unregistered, S)) andalso is_alive(S).

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
    riak_core_test_util:stop_pid(riak_core_bg_manager).

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
    {ok, _BgMgr} = riak_core_bg_manager:start().

%% @doc revive_post - Postcondition for revive
revive_post(_S, _Args, _Res) ->
    %% TODO: what to validate here, if anything?
    true.

%% ------ Grouped operator: all_resources query
%% @doc all_resources arguments generator
all_resources_args(_S) ->
    [oneof([all, lock_type(), token_type()])].

%% @doc all_resources precondition
all_resources_pre(S) ->
    is_alive(S).

%% @doc all_resources next state function
all_resources_next(S, _Value, _Args) ->
    S.

%% @doc all_resources command
all_resources(Resource) ->
    riak_core_bg_manager:all_resources(Resource).

%% @doc all_resources postcondition
all_resources_post(State, [Resource], Result) ->
    %% only one of these will have non-zero result unless Resource = all
    NumLocks = length(held_locks(Resource, State)),
    NumTokens = num_tokens_taken(Resource, State),
    %% TODO: could validate record entries in addition to correct counts
    eq(length(Result), NumLocks+NumTokens).

%% ------ Grouped operator: bypass
%% @doc bypass arguments generator
bypass_args(_S) ->
    [oneof([true, false])].

%% @doc bypass precondition
bypass_pre(S) ->
    is_alive(S).

%% @doc bypass next state function
bypass_next(S, _Value, [Switch]) ->
    S#state{bypassed=Switch}.

%% @doc bypass command
bypass(Switch) ->
    Res = riak_core_bg_manager:bypass(Switch), %% expect 'ok'
    Value = riak_core_bg_manager:bypassed(),  %% expect eq(Value, Switch)
    {Res, Value}.

%% @doc bypass postcondition
bypass_post(_S, [Switch], Result) ->
    eq(Result, {ok, Switch}).

%% ------ Grouped operator: bypassed
%% @doc bypass arguments generator
bypassed_args(_S) ->
    [].

%% @doc eanble precondition
bypassed_pre(S) ->
    is_alive(S).

%% @doc bypassed next state function
bypassed_next(S, _Value, []) ->
    S.

%% @doc bypassed command
bypassed() ->
    riak_core_bg_manager:bypassed().

%% @doc bypassed postcondition
bypassed_post(#state{bypassed=Bypassed}, _Value, Result) ->
    eq(Result, Bypassed).

%% ------ Grouped operator: enable
%% @doc bypass arguments generator
enable_args(_S) ->
    [oneof([[], token_type(), lock_type()])].

%% @doc enable precondition
%% global enable
enable_pre(S) ->
    is_alive(S).

%% per resource enable
enable_pre(S,[Type]) ->
    is_integer(max_num_tokens(Type, unregistered, S)) andalso is_alive(S).

%% @doc enable next state function
%% global enable
enable_next(S, _Value, []) ->
    S#state{enabled=true};
%% per resource enable
enable_next(S, _Value, [Type]) ->
    enable_resource(Type, S).

%% @doc enable command
enable() ->
    riak_core_bg_manager:enable().

enable(Resource) ->
    riak_core_bg_manager:enable(Resource).

%% @doc enable postcondition
%% global enable
enable_post(S, [], Result) ->
    eq(Result, status_of(true, S#state{enabled=true}));
%% per resource enable
enable_post(S, [_Resource], Result) ->
    ResourceEnabled = true,
    eq(Result, status_of(ResourceEnabled, S)).

%% ------ Grouped operator: disable
%% @doc bypass arguments generator
disable_args(_S) ->
    [oneof([[], token_type(), lock_type()])].

%% @doc eanble precondition
%% global disable
disable_pre(S) ->
    is_alive(S).

%% per resource disable
disable_pre(S,[Type]) ->
    is_integer(max_num_tokens(Type, unregistered, S)) andalso is_alive(S).

%% @doc disable next state function
%% global disable
disable_next(S, _Value, []) ->
    S#state{enabled=false};
%% per resource disable
disable_next(S, _Value, [Type]) ->
    disable_resource(Type, S).

%% @doc disable command
disable() ->
    riak_core_bg_manager:disable().

disable(Resource) ->
    riak_core_bg_manager:disable(Resource).

%% @doc disable postcondition
%% global
disable_post(S, [], Result) ->
    Ignored = true,
    eq(Result, status_of(Ignored, S#state{enabled=false}));
%% per resource
disable_post(S, [_Resource], Result) ->
    ResourceEnabled = false,
    eq(Result, status_of(ResourceEnabled, S)).

%% ------ Grouped operator: enabled
%% @doc bypass arguments generator
enabled_args(_S) ->
    [].

%% @doc eanble precondition
enabled_pre(S) ->
    is_alive(S).

%% @doc enabled next state function
enabled_next(S, _Value, []) ->
    S.

%% @doc enabled command
enabled() ->
    riak_core_bg_manager:enabled().

%% @doc enabled postcondition
enabled_post(S, _Value, Result) ->
    eq(Result, status_of(true, S)).

%%------------ helpers -------------------------
%% @doc resources are disabled iff they appear on the "disabled" list
resource_enabled(Resource, #state{disabled=Disabled}) ->
    not lists:member(Resource, Disabled).

%% @doc enable the resource by removing from the "disabled" list
enable_resource(Resource, State=#state{disabled=Disabled}) ->
    State#state{disabled=lists:delete(Resource, Disabled)}.

disable_resource(Resource, State=#state{disabled=Disabled}) ->
    State#state{disabled=[Resource | lists:delete(Resource, Disabled)]}.

%% @doc return status considering Resource status, enbaled, and bypassed
status_of(_Enabled, #state{bypassed=true}) -> bypassed;
status_of(true, #state{enabled=true}) -> enabled;
status_of(_E,_S) -> disabled.

%% -- Generators
lock_type() ->
    oneof([a,b,c,d]). %%,e,f,g,h,i]).

token_type() ->
    oneof(['A','B','C','D']). %%,'E','F','G','H','I']).

lock_limit() ->
    choose(0, 5).

token_count() ->
    choose(0, 5).

all_resources_() ->
    choose(0, 10).

%% @doc weight/2 - Distribution of calls
weight(_S, set_concurrency_limit) -> 3;
weight(_S, concurrency_limit) -> 3;
weight(_S, concurrency_limit_reached) -> 3;
weight(_S, start_process) -> 3;
weight(#state{alive=true}, stop_process) -> 3;
weight(#state{alive=false}, stop_process) -> 3;
weight(_S, get_lock) -> 20;
weight(_S, set_token_rate) -> 3;
weight(_S, token_rate) -> 0;
weight(_S, get_token) -> 20;
weight(_S, refill_tokens) -> 10;
weight(_S, all_resources) -> 3;
weight(_S, crash) -> 3;
weight(_S, revive) -> 1;
weight(_S, _Cmd) -> 1.

%% Other Functions
limit(Type, State) ->
    limit(Type, undefined, State).

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

num_tokens_taken(all, #state{tokens=Tokens}) ->
    lists:foldl(fun({_Resource, Count}, Sum) -> Count+Sum end, 0, Tokens);
num_tokens_taken(Resource, #state{tokens=Tokens}) ->
    lists:foldl(fun(Count, Sum) -> Count+Sum end,
                0,
                [Count || {R, Count} <- Tokens, R == Resource]).

is_alive(#state{alive=Alive}) ->
    Alive.

mk_token_rate({unregistered, _}=Unreg) ->
    Unreg;
mk_token_rate(undefined) ->
    undefined;
mk_token_rate(Count) ->
    %% erlang:send_after max is used so that we can trigger token refilling from EQC test
    {max_send_after(), Count}.

max_send_after() ->
    4294967295.

running_procs(#state{procs=Procs}) ->
    [Pid || {Pid, running} <- Procs].

all_locks(#state{locks=Locks}) ->
    lists:flatten([ByType || {_Type, ByType} <- Locks]).

all_locks(all, State) ->
    all_locks(State);
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
                             riak_core_test_util:stop_pid(riak_core_bg_manager),
                             {ok, _BgMgr} = riak_core_bg_manager:start(),
                             {H, S, Res} = run_commands(?MODULE,Cmds),
                             InfoTable = ets:tab2list(?BG_INFO_ETS_TABLE),
                             EntryTable = ets:tab2list(?BG_ENTRY_ETS_TABLE),
                             Monitors = bg_manager_monitors(),
                             RunnngPids = running_procs(S),
                             %% cleanup processes not killed during test
                             [riak_core_test_util:stop_pid(Pid) || Pid <- RunnngPids],
                             riak_core_test_util:stop_pid(riak_core_bg_manager),
                             ?WHENFAIL(
                                begin
                                    io:format("~n~nFinal State: ~n"),
                                    io:format("---------------~n"),
                                    io:format("alive = ~p~n", [S#state.alive]),
                                    io:format("bypassed = ~p~n", [S#state.bypassed]),
                                    io:format("enabled = ~p~n", [S#state.enabled]),
                                    io:format("procs = ~p~n", [S#state.procs]),
                                    io:format("limits = ~p~n", [S#state.limits]),
                                    io:format("locks = ~p~n", [S#state.locks]),
                                    io:format("counts = ~p~n", [S#state.counts]),
                                    io:format("tokens = ~p~n", [S#state.tokens]),
                                    io:format("---------------~n"),
                                    io:format("~n~nbackground_mgr tables: ~n"),
                                    io:format("---------------~n"),
                                    io:format("~p~n", [InfoTable]),
                                    io:format("---------------~n"),
                                    io:format("~p~n", [EntryTable]),
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
                             riak_core_test_util:stop_pid(riak_core_bg_manager),
                             {ok, BgMgr} = riak_core_bg_manager:start(),
                             {Seq, Par, Res} = run_parallel_commands(?MODULE,Cmds),
                             InfoTable = ets:tab2list(?BG_INFO_ETS_TABLE),
                             EntryTable = ets:tab2list(?BG_ENTRY_ETS_TABLE),
                             Monitors = bg_manager_monitors(),
                             riak_core_test_util:stop_pid(BgMgr),
                             ?WHENFAIL(
                                begin
                                    io:format("~n~nbackground_mgr tables: ~n"),
                                    io:format("---------------~n"),
                                    io:format("~p~n", [InfoTable]),
                                    io:format("---------------~n"),
                                    io:format("~p~n", [EntryTable]),
                                    io:format("---------------~n"),
                                    io:format("~n~nbg_manager monitors: ~n"),
                                    io:format("---------------~n"),
                                    io:format("~p~n", [Monitors]),
                                    io:format("---------------~n")
                                end,
                                pretty_commands(?MODULE, Cmds, {Seq, Par, Res},
                                                Res == ok))
                         end))).

-endif.
-endif.
