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
-module(riak_core_info_service_process).
-behaviour(gen_server).

-record(state, {
    source = undefined :: riak_core_info_service:callback(),
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


%% `gen_server' implementation

start_link(Registration, Shutdown, Source, ResponseHandler) ->
    State = #state{shutdown  = Shutdown,
                   source  = Source,
                   response_handler = ResponseHandler},
    {ok, Pid} = gen_server:start_link(?MODULE, State, []),
    apply_callback(Registration, [Pid]),
    {ok, Pid}.

init(State) ->
    {ok, State}.

handle_info({invoke, SourceParams, HandlerContext}, State) ->
    handle_invoke_message(SourceParams, HandlerContext, State);

handle_info(callback_shutdown, State) ->
    handle_shutdown_message(State).

terminate(_Reason, State) ->
    handle_shutdown_message(State).

code_change(_OldVsn, State, _Extra) ->
    State.

handle_call(_Request, _From, _State) ->
    erlang:error(not_implemented).

handle_cast(_Request, _State) ->
    erlang:error(not_implemented).

%% Private

handle_invoke_message(SourceParams, HandlerContext,
                      #state{source = Source,
                             response_handler = ResponseHandler } = State) ->
    Result = apply_callback(Source, SourceParams),
    Reply = {Result, SourceParams, HandlerContext},
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
