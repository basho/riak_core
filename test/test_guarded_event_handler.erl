%% -------------------------------------------------------------------
%%
%% test_guarded_event_handler
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
-module(test_guarded_event_handler).
-behaviour(gen_event).
-export([start_link/0]).
-export([init/1, handle_event/2, handle_call/2, 
         handle_info/2, terminate/2, code_change/3]).
-export([get_events/0]).
-record(state, {events=[]}).

-include_lib("eunit/include/eunit.hrl").

start_link() ->
    gen_event:start_link({local, ?MODULE}).

get_events() ->
    gen_event:call(?MODULE, ?MODULE, get_events).

init([]) ->
    {ok, #state{}}.

handle_event({event, E}, State=#state{events=Events}) ->
    {ok, State#state{events=[E|Events]}};
handle_event(crash, State) ->
    exit(crash),
    {ok, State}.

handle_call(get_events, State) ->
    {ok, State#state.events, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(Reason, _State) ->
    Reason.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-ifdef(TEST).

guarded_handler_test_() ->
    { setup, local,
      fun setup/0,
      fun cleanup/1,
      [
       fun guarded_handler_test_case/0
      ]
    }.

setup() ->
    riak_core_eventhandler_sup:start_link(),
    ?MODULE:start_link().

cleanup(_Pid) ->
    %% ARG: ugly hack to not die when the supervisor exits.
    process_flag(trap_exit, true),
    gen_event:stop(?MODULE).

wait_for_exitfun() ->
    receive 
        {?MODULE, {'EXIT', crash}} ->
            ok
    after 5000 ->
            fail
    end.
    
guarded_handler_test_case() ->
    Self = self(),
    F = fun(Handler, Reason) ->
                Self ! {Handler, Reason}
        end,
    riak_core:add_guarded_event_handler(?MODULE, ?MODULE, [], F),
    gen_event:notify(?MODULE, {event, foo}),
    ?assertEqual(?MODULE:get_events(), [foo]),
    gen_event:notify(?MODULE, crash),
    ?assertEqual(wait_for_exitfun(), ok),
    wait_for_handler(?MODULE, 1000, 100),
    gen_event:notify(?MODULE, {event, baz}),
    ?assertEqual(?MODULE:get_events(), [baz]),
    ?assertEqual(riak_core:delete_guarded_event_handler(?MODULE,?MODULE,quux), quux),
    ?assertNot(lists:member(?MODULE, gen_event:which_handlers(?MODULE))),
    ?assertEqual([], supervisor:which_children(riak_core_eventhandler_sup)).

wait_for_handler(_, 0, _) ->
    fail;
wait_for_handler(Name, Count, Sleep) ->
    case lists:member(Name, gen_event:which_handlers(?MODULE)) of
        true -> ok;
        false ->
            timer:sleep(Sleep),
            wait_for_handler(Name, Count - 1, Sleep);
        _ ->
            ok
    end.

-endif.
