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

%% @doc A custom event handler to the `riak_sysmon' application's
%% `system_monitor' event manager.
%%
%% This module attempts to discover more information about a process
%% that generates a system_monitor event.

-module(riak_core_sysmon_handler).

-behaviour(gen_event).

%% API
-export([add_handler/0]).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2, 
         handle_info/2, terminate/2, code_change/3]).

-record(state, {}).

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================

add_handler() ->
    %% Vulnerable to race conditions (installing handler multiple
    %% times), but risk is zero in the common OTP app startup case.
    case lists:member(?MODULE,
                      gen_event:which_handlers(riak_sysmon_handler)) of
        true ->
            ok;
        false ->
            riak_sysmon_filter:add_custom_handler(?MODULE, [])
    end.

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a new event handler is added to an event manager,
%% this function is called to initialize the event handler.
%%
%% @spec init(Args) -> {ok, State}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever an event manager receives an event sent using
%% gen_event:notify/2 or gen_event:sync_notify/2, this function is
%% called for each installed event handler to handle the event.
%%
%% @spec handle_event(Event, State) ->
%%                          {ok, State} |
%%                          {swap_handler, Args1, State1, Mod2, Args2} |
%%                          remove_handler
%% @end
%%--------------------------------------------------------------------
handle_event({monitor, Pid, Type, Info}, State) ->
    {Fmt, Args} = format_pretty_proc_info(Pid, almost_current_function),
    lager:info("monitor ~w ~w "++ Fmt ++ " ~w",
                          [Type, Pid] ++ Args ++ [Info]),
    {ok, State};
handle_event(Event, State) ->
    lager:info("Monitor got ~p", [Event]),
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever an event manager receives a request sent using
%% gen_event:call/3,4, this function is called for the specified
%% event handler to handle the request.
%%
%% @spec handle_call(Request, State) ->
%%                   {ok, Reply, State} |
%%                   {swap_handler, Reply, Args1, State1, Mod2, Args2} |
%%                   {remove_handler, Reply}
%% @end
%%--------------------------------------------------------------------
handle_call(_Call, State) ->
    Reply = not_supported,
    {ok, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called for each installed event handler when
%% an event manager receives any other message than an event or a
%% synchronous request (or a system message).
%%
%% @spec handle_info(Info, State) ->
%%                         {ok, State} |
%%                         {swap_handler, Args1, State1, Mod2, Args2} |
%%                         remove_handler
%% @end
%%--------------------------------------------------------------------
handle_info(die_for_testing_purposes_only, _State) ->
    %% exit({told_to_die, lists:duplicate(500000, $x)});
    exit({told_to_die, lists:duplicate(50, $x)});
handle_info(Info, State) ->
    lager:info("handle_info got ~p", [Info]),
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever an event handler is deleted from an event manager, this
%% function is called. It should be the opposite of Module:init/1 and
%% do any necessary cleaning up.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

format_pretty_proc_info(Pid) ->
    format_pretty_proc_info(Pid, current_function).

format_pretty_proc_info(Pid, Acf) ->
    try
        case get_pretty_proc_info(Pid, Acf) of
            undefined ->
                {"", []};
            Res ->
                {"~w", [Res]}
        end
    catch X:Y ->
        {"Pid ~w, ~W ~W at ~w\n",
            [Pid, X, 20, Y, 20, erlang:get_stacktrace()]}
    end.

get_pretty_proc_info(Pid) ->
    get_pretty_proc_info(Pid, current_function).

get_pretty_proc_info(Pid, Acf) ->
    case process_info(Pid, [registered_name, initial_call, current_function,
                            message_queue_len]) of
        undefined ->
            undefined;
        [] ->
            undefined;
        [{registered_name, RN0}, ICT1, {_, CF}, {_, MQL}] ->
            ICT = case proc_lib:translate_initial_call(Pid) of
                     {proc_lib, init_p, 5} ->   % not by proc_lib, see docs
                         ICT1;
                     ICT2 ->
                         {initial_call, ICT2}
                 end,
            RNL = if RN0 == [] -> [];
                     true      -> [{name, RN0}]
                  end,
            RNL ++ [ICT, {Acf, CF}, {message_queue_len, MQL}]
    end.
