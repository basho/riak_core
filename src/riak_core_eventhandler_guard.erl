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
-export([start_link/3, start_link/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-record(state, {handlermod, handler, exitfun}).

start_link(HandlerMod, Handler, Args) ->
    start_link(HandlerMod, Handler, Args, undefined).

start_link(HandlerMod, Handler, Args, ExitFun) ->
    gen_server:start_link(?MODULE, [HandlerMod, Handler, Args, ExitFun], []).

init([HandlerMod, Handler, Args, ExitFun]) ->
    ok = gen_event:add_sup_handler(HandlerMod, Handler, Args),
    {ok, #state{handlermod=HandlerMod, handler=Handler, exitfun=ExitFun}}.

handle_call(_Request, _From, State) -> {reply, ok, State}.

handle_cast(_Msg, State) -> {noreply, State}.


handle_info({gen_event_EXIT, _Handler, shutdown}, State) ->
    {stop, normal, State};
handle_info({gen_event_EXIT, _Handler, normal}, State) ->
    {stop, normal, State};
handle_info({gen_event_EXIT, Handler, _Reason}, State=#state{exitfun=undefined}) ->
    {stop, {gen_event_EXIT, Handler}, State};
handle_info({gen_event_EXIT, Handler, Reason}, State=#state{exitfun=ExitFun}) ->
    ExitFun(Handler, Reason),
    {stop, {gen_event_EXIT, Handler}, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{}) ->
    ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

