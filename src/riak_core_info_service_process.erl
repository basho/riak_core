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

-type callback() :: riak_core_info_service:callback().

-record(state, {
    registration :: callback(),
    service_provider :: callback(),
    response_handler :: callback(),
    shutdown :: callback()
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

%% All arguments must be fully-defined callbacks.
start_link(Registration, Shutdown, Provider, ResponseHandler)
  when is_tuple(Registration),
       is_tuple(Shutdown),
       is_tuple(Provider),
       is_tuple(ResponseHandler) ->
    State = #state{registration = Registration,
                   shutdown = Shutdown,
                   service_provider = Provider,
                   response_handler = ResponseHandler},
    gen_server:start_link(?MODULE, State, []).

init(#state{registration=Registration}=State) ->
    %% Registration callback *must* return `ok' or this process will
    %% shut down
    handle_consumer_response(apply_callback(Registration, [self()]), registration, State).

handle_info({invoke, ProviderParams, HandlerContext}, State) ->
    handle_invoke_message(ProviderParams, HandlerContext, State);

handle_info(callback_shutdown, State) ->
    {stop, normal, State}.

terminate(_Reason, State) ->
    handle_shutdown_message(State).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

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
    %% At this layer we don't care much whether a callback succeeded
    {_, Result} = apply_callback(Provider, ProviderParams),

    Reply = {Result, ProviderParams, HandlerContext},
    handle_consumer_response(apply_callback(ResponseHandler, [Reply]), response, State).

handle_shutdown_message(#state{shutdown = Shutdown}=State) ->
    _ = apply_callback(Shutdown, [self()]),
    {stop, normal, State}.

%% The original response to the callback will be wrapped in an `ok' or
%% `exit' tuple, primarily so that the service process can terminate
%% if something goes wrong during registration.
-spec apply_callback(callback(), list(term())) -> {ok, term()} |
                                                  {error, term()}.
apply_callback({Mod, Fun, StaticArgs}, CallSpecificArgs) ->
    %% Invoke the callback, passing static + call-specific args for this call
    Args = StaticArgs ++ CallSpecificArgs,
    _ = case (catch erlang:apply(Mod, Fun, Args)) of
            {'EXIT', {Reason, _Stack}}=Exit->
                %% provider called erlang:error(Reason)
                lager:warning("~p:~p/~B exited (~p)",
                              [Mod, Fun, length(Args), Reason]),
                {error, Exit};
            {'EXIT', Why}=Exit ->
                %% provider called erlang:exit(Why)!
                lager:warning("~p:~p/~B exited (~p)",
                              [Mod, Fun, length(Args), Why]),
                {error, Exit};
            Result ->
                {ok, Result}
        end.

handle_consumer_response({ok, ok}, response, State) ->
    {noreply, State};
handle_consumer_response({ok, ok}, registration, State) ->
    {ok, State};
handle_consumer_response({ok, Other}, response, State) ->
    lager:error("Response handler gave ~p result, shutting down", [Other]),
    {stop, invalid_return, State};
handle_consumer_response({ok, Other}, registration, _State) ->
    lager:error("Registration gave ~p result, shutting down", [Other]),
    {stop, invalid_return};
handle_consumer_response(_Error, response, State) ->
    lager:error("Response handler failed, shutting down"),
    {stop, response_handler_failure, State};
handle_consumer_response(_Error, registration, _State) ->
    lager:error("Registration failed, shutting down"),
    {stop, registration_error}.

-ifdef(TEST).
callback_target() ->
    ok.

callback_target(static, dynamic) ->
    ok.

shutdown_target(Key, _Pid) ->
    put(Key, true).

callback1_test() ->
    ?assertEqual({ok, ok}, apply_callback({?MODULE, callback_target, []}, [])).

callback2_test() ->
    ?assertEqual({ok, ok}, apply_callback({?MODULE, callback_target, [static]}, [dynamic])).

shutdown_test() ->
    Pkey = '_rcisp_test_shutdown',
    put(Pkey, false),
    ?assertEqual(false, get(Pkey)),
    ?assertMatch({stop, normal, _}, handle_shutdown_message(#state{shutdown={?MODULE, shutdown_target, [Pkey]}})),
    ?assertEqual(true, get(Pkey)),
    put(Pkey, undefined).


-endif.
