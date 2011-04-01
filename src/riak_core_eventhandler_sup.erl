%% -------------------------------------------------------------------
%%
%% riak_core_eventhandler_sup: supervise minder processes for gen_event handlers
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

%% @doc supervise riak_core_eventhandler_guard processes
-module(riak_core_eventhandler_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).
-export([start_guarded_handler/3, start_guarded_handler/4, stop_guarded_handler/3]).

start_guarded_handler(HandlerMod, Handler, Args) ->
    start_guarded_handler(HandlerMod, Handler, Args, undefined).

start_guarded_handler(HandlerMod, Handler, Args, ExitFun) ->
    case supervisor:start_child(?MODULE, handler_spec(HandlerMod, Handler, Args, ExitFun)) of
        {ok, _Pid} -> ok;
        Other -> Other
    end.

stop_guarded_handler(HandlerMod, Handler, Args) ->
    case lists:member(Handler, gen_event:which_handlers(HandlerMod)) of
        true ->
            case gen_event:delete_handler(HandlerMod, Handler, Args) of
                {error, module_not_found} ->
                    {error, module_not_found};
                O ->
                    Id = {HandlerMod, Handler},
                    ok = supervisor:terminate_child(?MODULE, Id),
                    ok = supervisor:delete_child(?MODULE, Id),
                    O
            end;
        false ->
            {error, module_not_found}
    end.

handler_spec(HandlerMod, Handler, Args, ExitFun) ->
    {{HandlerMod, Handler},
     {riak_core_eventhandler_guard, start_link, [HandlerMod, Handler, Args, ExitFun]},
     transient, 5000, worker, [riak_core_eventhandler_guard]}.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @private
init([]) ->
    {ok, {{one_for_one, 10, 10}, []}}.


