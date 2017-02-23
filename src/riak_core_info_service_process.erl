%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Basho Technologies, Inc.  All Rights Reserved.
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

%% @see riak_core_info_service
-module(riak_core_info_service_process).
-behaviour(gen_server).

-record(state, {
    service_provider = undefined :: riak_core_info_service:callback(),
    response_handler = undefined :: riak_core_info_service:callback(),
    shutdown = undefined :: riak_core_info_service:callback()
}).

-export([start_link/4,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% `gen_server' implementation

start_link(Registration, Shutdown, Provider, ResponseHandler) ->
    State = #state{shutdown = Shutdown,
                   service_provider = Provider,
                   response_handler = ResponseHandler},
    {ok, Pid} = gen_server:start_link(?MODULE, State, []),
    apply_callback(Registration, [Pid]),
    {ok, Pid}.

init(State) ->
    {ok, State}.

handle_info({invoke, ProviderParams, HandlerContext}, State) ->
    handle_invoke_message(ProviderParams, HandlerContext, State);

handle_info(callback_shutdown, State) ->
    {stop, normal, State}.

terminate(_Reason, State) ->
    handle_shutdown_message(State).

code_change(_OldVsn, State, _Extra) ->
    State.

handle_call(Request, _From, State) ->
    terminate_not_implemented(Request, State).

handle_cast(Request, State) ->
    terminate_not_implemented(Request, State).

%% Private

terminate_not_implemented(Request, State) ->
    lager:warning("Terminating - unknown message received: ~p", [Request]),
    {stop, not_implemented, State}.

handle_invoke_message(ProviderParams, HandlerContext,
                      #state{service_provider = Provider,
                             response_handler = ResponseHandler } = State) ->
    Result = apply_callback(Provider, ProviderParams),
    Reply = {Result, ProviderParams, HandlerContext},
    apply_callback(ResponseHandler, [Reply]),
    {noreply, State}.

handle_shutdown_message(#state{shutdown = Shutdown}=State) ->
    apply_callback(Shutdown, [self()]),
    {stop, normal, State}.

apply_callback(undefined, _CallSpecificArgs) ->
    ok;
apply_callback({Mod, Fun, StaticArgs}, CallSpecificArgs) ->
    %% Invoke the callback, passing static + call-specific args for this call
    erlang:apply(Mod, Fun, StaticArgs ++ CallSpecificArgs).

-ifdef(TEST).
callback_target() ->
    ok.

callback_target(static, dynamic) ->
    ok.

shutdown_target(Key, _Pid) ->
    put(Key, true).

callback1_test() ->
    ?assertEqual(ok, apply_callback({?MODULE, callback_target, []}, [])).

callback2_test() ->
    ?assertEqual(ok, apply_callback({?MODULE, callback_target, [static]}, [dynamic])).

shutdown_test() ->
    Pkey = '_rcisp_test_shutdown',
    put(Pkey, false),
    ?assertEqual(false, get(Pkey)),
    ?assertMatch({stop, normal, _}, handle_shutdown_message(#state{shutdown={?MODULE, shutdown_target, [Pkey]}})),
    ?assertEqual(true, get(Pkey)),
    put(Pkey, undefined).


-endif.
