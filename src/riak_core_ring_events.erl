%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_core_ring_events).

-behaviour(gen_event).

%% API
-export([start_link/0,
         add_handler/2,
         add_sup_handler/2,
         add_guarded_handler/2,
         add_callback/1,
         add_sup_callback/1,
         add_guarded_callback/1,
         delete_handler/2,
         ring_update/1,
         force_update/0,
         ring_sync_update/1,
         force_sync_update/0,
         get_pid/0]).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state, { callback }).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    gen_event:start_link({local, ?MODULE}).

add_handler(Handler, Args) ->
    gen_event:add_handler(?MODULE, Handler, Args).

add_sup_handler(Handler, Args) ->
    gen_event:add_sup_handler(?MODULE, Handler, Args).

add_guarded_handler(Handler, Args) ->
    riak_core:add_guarded_event_handler(?MODULE, Handler, Args).

add_callback(Fn) when is_function(Fn) ->
    HandlerName = {?MODULE, make_ref()},
    gen_event:add_handler(?MODULE, HandlerName, [Fn]),
    HandlerName.

add_sup_callback(Fn) when is_function(Fn) ->
    HandlerName = {?MODULE, make_ref()},
    gen_event:add_sup_handler(?MODULE, HandlerName, [Fn]),
    HandlerName.

add_guarded_callback(Fn) when is_function(Fn) ->
    HandlerName = {?MODULE, make_ref()},
    riak_core:add_guarded_event_handler(?MODULE, HandlerName, [Fn]),
    HandlerName.

delete_handler(HandlerName, ReasonArgs) ->
    gen_event:delete_handler(?MODULE, HandlerName, ReasonArgs).

force_update() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ring_update(Ring).

ring_update(Ring) ->
    gen_event:notify(?MODULE, {ring_update, Ring}).

force_sync_update() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ring_sync_update(Ring).

ring_sync_update(Ring) ->
    gen_event:sync_notify(?MODULE, {ring_update, Ring}).

get_pid() ->
    whereis(?MODULE).

%% ===================================================================
%% gen_event callbacks
%% ===================================================================

init([Fn]) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Fn(Ring),
    {ok, #state { callback = Fn }}.

handle_event({ring_update, Ring}, State) ->
    (State#state.callback)(Ring),
    {ok, State}.

handle_call(_Request, State) ->
    {ok, ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-ifdef(TEST).

delete_handler_test_() ->
    RingMgr = riak_core_ring_manager,
    {setup,
     fun() ->
             meck:new(RingMgr, [passthrough]),
             meck:expect(RingMgr, get_my_ring,
                         fun() -> {ok, fake_ring_here} end),
             ?MODULE:start_link()
     end,
     fun(_) ->
             meck:unload(RingMgr)
     end,
     [
      fun () ->
              Name = ?MODULE:add_sup_callback(
                       fun(Arg) -> RingMgr:test_dummy_func(Arg) end),

              BogusRing = bogus_ring_stand_in,
              ?MODULE:ring_update(BogusRing),
              ok = ?MODULE:delete_handler(Name, unused),
              {error, _} = ?MODULE:delete_handler(Name, unused),

              %% test_dummy_func is called twice: once by add_sup_callback()
              %% and once by ring_update().
              [
               {_, {RingMgr, get_my_ring, _}, _},
               {_, {RingMgr, test_dummy_func, _}, _},
               {_, {RingMgr, test_dummy_func, [BogusRing]}, _}
              ] = meck:history(RingMgr),
              ok
      end
     ]}.

-endif. % TEST
