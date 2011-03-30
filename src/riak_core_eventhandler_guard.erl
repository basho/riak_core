%% -------------------------------------------------------------------
%%
%% riak_core_eventhandler_guard: Guard process for persistent event handlers.
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_core_eventhandler_guard).
-behaviour(gen_server).
-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-record(state, {}).

start_link(HandlerMod, Handler, Args) ->
    gen_server:start_link(?MODULE, [HandlerMod, Handler, Args], []).

init([HandlerMod, Handler, Args]) ->
    ok = gen_event:add_sup_handler(HandlerMod, Handler, Args),
    {ok, #state{}}.

handle_call(_Request, _From, State) -> {reply, ok, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info({gen_event_EXIT, Handler, Reason}, State) ->
    error_logger:error_msg("~w: ~s: handler ~w exited for reason ~s",
                           [self(), ?MODULE, Handler, Reason]),
    {stop, gen_event_EXIT, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

