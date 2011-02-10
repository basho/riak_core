%%
%% Copyright (c) 2011 Basho Technologies, Inc.  All Rights Reserved.
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

-module(gen_fsm_test_driver).

-define(INFINITE_TIMEOUT, 99999999999).

%% API for gen_fsm
-export([start/3
         % ,
         % send_event/2, sync_send_event/2, sync_send_event/3,
         % send_all_state_event/2,
         % sync_send_all_state_event/2, sync_send_all_state_event/3,
         % reply/2
        ]).

%% API for our extensions
-export([run_to_completion/4, destroy_id/1,
         get_state/1, get_statedata/1, get_trace/1,
         save_state/2, save_statedata/2,
         add_trace/2
        ]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start(FsmID, Mod, Args) ->
    io:format("Start: ~p\n", [FsmID]),
    destroy_id(FsmID),
    erlang:put(make_key(FsmID, trace), []),
    simplify_init(Mod:init(Args)).

run_to_completion(FsmID, Mod, InitIter, Events) ->
    run_iterations(FsmID, Mod, InitIter, Events).

get_state(FsmID) ->
    erlang:get(make_key(FsmID, state)).

get_statedata(FsmID) ->
    erlang:get(make_key(FsmID, statedata)).

%% NOTE: For external use only: reverse() can make life difficult!

get_trace(FsmID) ->
    lists:reverse(case erlang:get(make_key(FsmID, trace)) of
                      undefined -> [];
                      Else      -> Else
                  end).

save_state(FsmID, State) ->
    erlang:put(make_key(FsmID, state), State).

save_statedata(FsmID, StateData) ->
    erlang:put(make_key(FsmID, statedata), StateData).

add_trace(Options, TraceEvent) when is_list(Options) ->
    add_trace(proplists:get_value(my_ref, Options), TraceEvent);
add_trace(FsmID, TraceEvent) ->
    Traces = erlang:get(make_key(FsmID, trace)),
    erlang:put(make_key(FsmID, trace), [TraceEvent|Traces]).

destroy_id(FsmID) ->
    erlang:erase(make_key(FsmID, trace)),
    erlang:erase(make_key(FsmID, state)),
    erlang:erase(make_key(FsmID, statedata)),
    ok.

make_key(FsmID, Type) ->
    {?MODULE, FsmID, Type}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

run_iterations(FsmID, Mod, {next_state, StateName, StateData, 0}, Events) ->
    NextI = Mod:StateName(timeout, StateData),
    run_iterations(FsmID, Mod, simplify(FsmID, unused, NextI), Events);
run_iterations(_FsmID, _Mod, {next_state, StateName, StateData, _}, []) ->
    {need_events, StateName, StateData};
run_iterations(_FsmID, _Mod, {stop, StateName, StateData}, UnconsumedEvents) ->
    {stopped, StateName, StateData, UnconsumedEvents};
run_iterations(FsmID, Mod, {next_state, StateName, StateData, _}, [E|Es]) ->
    case E of
        {send_event, Event} ->
            NextI = Mod:StateName(Event, StateData),
            run_iterations(FsmID, Mod, simplify(FsmID, unused, NextI), Es);
        {send_all_state_event, Event} ->
            NextI = Mod:handle_event(Event, StateName, StateData),
            run_iterations(FsmID, Mod, simplify(FsmID, unused, NextI), Es);
        {sync_send_event, From, Event} ->
            NextI = Mod:StateName(Event, From, StateData),
            run_iterations(FsmID, Mod, simplify(FsmID, From, NextI), Es);
        {sync_send_all_state_event, From, Event} ->
            NextI = Mod:handle_sync_event(Event, From, StateName, StateData),
            run_iterations(FsmID, Mod, simplify(FsmID, From, NextI), Es)
    end.

%% return {next_state, StateName, StateData, integer()} |
%%        exit()

simplify_init({stop, Reason}) ->
    exit({stop_not_supported, Reason});
simplify_init(ignore) ->
    exit(ignore_not_supported);
simplify_init({ok, StateName, StateData}) ->
    {next_state, StateName, StateData, ?INFINITE_TIMEOUT};
simplify_init({ok, StateName, StateData, Timeout}) ->
    {next_state, StateName, StateData, simplify_timeout(Timeout)}.

%% return {stop, Reason, StateData} |
%%        {next_state, StateName, StateData, Timeout}

simplify(_FsmID, _From, {stop, _Reason, _StateData} = X) ->
    X;
simplify(FsmID, From, {stop, Reason, Reply, StateData}) ->
    add_trace(FsmID, {reply, From, Reply}),
    {stop, Reason, StateData};
simplify(_FsmID, _From, {next_state, StateName, StateData}) ->
    {next_state, StateName, StateData, ?INFINITE_TIMEOUT};
simplify(_FsmID, _From, {next_state, StateName, StateData, Timeout}) ->
    {next_state, StateName, StateData, simplify_timeout(Timeout)};
simplify(FsmID, From, {reply, Reply, StateName, StateData}) ->
    add_trace(FsmID, {reply, From, Reply}),
    {next_state, StateName, StateData, ?INFINITE_TIMEOUT};
simplify(FsmID, From, {reply, Reply, StateName, StateData, Timeout}) ->
    add_trace(FsmID, {reply, From, Reply}),
    {next_state, StateName, StateData, simplify_timeout(Timeout)}.

simplify_timeout(infinity) ->
    ?INFINITE_TIMEOUT;
simplify_timeout(hibernate) ->
    ?INFINITE_TIMEOUT;
simplify_timeout(Int) when is_integer(Int) ->
    Int.

