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
-module(riak_core_vnode).
-behaviour(gen_fsm).
-include_lib("riak_core_vnode.hrl").
-export([behaviour_info/1]).
-export([start_link/2, pure_start/4,
         send_command/2,
         send_command_after/2]).
-export([init/1, 
         active/2, 
         active/3, 
         handle_event/3,
         handle_sync_event/4, 
         handle_info/3, 
         terminate/3, 
         code_change/4]).
-export([reply/2]).
-export([get_mod_index/1, make_vnode_request/1]).

-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
behaviour_info(callbacks) ->
    [{init,1},
     {handle_command,3},
     {handoff_starting,2},
     {handoff_cancelled,1},
     {handoff_finished,2},
     {handle_handoff_command,3},
     {handle_handoff_data,2},
     {encode_handoff_item,2},
     {is_empty,1},
     {terminate,2},
     {delete,1}];
behaviour_info(_Other) ->
    undefined.

-define(DEFAULT_TIMEOUT, 60000).
-define(LOCK_RETRY_TIMEOUT, 10000).
-define(MODSTATE, State#state{mod=Mod,modstate=ModState}).
-define(PURE_DRIVER, gen_fsm_test_driver).

-record(state, {
          index :: partition(),
          mod :: module(),
          modstate :: term(),
          handoff_token :: non_neg_integer(),
          handoff_node=none :: none | node(),
          inactivity_timeout,
          pure_p = false :: boolean(),
          pure_opts = [] :: list()
         }).

start_link(Mod, Index) ->
    gen_fsm:start_link(?MODULE, {Mod, Index, []}, []).

pure_start(FsmID, Mod, Index, PureOpts) ->
    ?PURE_DRIVER:start(FsmID, ?MODULE, {Mod, Index, PureOpts}).

%% Send a command message for the vnode module by Pid - 
%% typically to do some deferred processing after returning yourself
send_command(Pid, Request) ->
    gen_fsm:send_event(Pid, make_vnode_request(Request)).

%% Sends a command to the FSM that called it after Time 
%% has passed.
-spec send_command_after(integer(), term()) -> reference().
send_command_after(Time, Request) ->
    gen_fsm:send_event_after(Time, make_vnode_request(Request)).
    

init({Mod, Index, PureOpts}) ->
    %%TODO: Should init args really be an array if it just gets Init?
    process_flag(trap_exit, true),
    PureP = proplists:get_value(debug, PureOpts, false),
    S0 = #state{index=Index, mod=Mod, 
                pure_p=PureP,pure_opts=PureOpts},
    {ok, ModState} = impure(S0, {mod, Mod}, init, [[Index]]),
    impure(S0, riak_core_handoff_manager, remove_exclusion, [Mod, Index]),
    Timeout = impure(S0, app_helper, get_env,
                     [riak_core, vnode_inactivity_timeout, ?DEFAULT_TIMEOUT]),
    {ok, active, S0#state{modstate=ModState, inactivity_timeout=Timeout}, 0}.

get_mod_index(VNode) ->
    gen_fsm:sync_send_all_state_event(VNode, get_mod_index).

continue(State) ->
    {next_state, active, State, State#state.inactivity_timeout}.

continue(State, NewModState) ->
    continue(State#state{modstate=NewModState}).
    

vnode_command(Sender, Request, State=#state{mod=Mod, modstate=ModState}) ->
    case impure(State, {mod, Mod}, handle_command,
                [Request, Sender, ModState]) of
        {reply, Reply, NewModState} ->
            impure(State, {mod, ?MODULE}, reply, [Sender, Reply]),
            continue(State, NewModState);
        {noreply, NewModState} ->
            continue(State, NewModState);
        {stop, Reason, NewModState} ->
            {stop, Reason, State#state{modstate=NewModState}}
    end.

vnode_handoff_command(Sender, Request, State=#state{index=Index,
                                                    mod=Mod, 
                                                    modstate=ModState, 
                                                    handoff_node=HN}) ->
    case impure(State, {mod, Mod}, handle_handoff_command,
                [Request, Sender, ModState]) of
        {reply, Reply, NewModState} ->
            impure(State, {mod, ?MODULE}, reply, [Sender, Reply]),
            continue(State, NewModState);
        {noreply, NewModState} ->
            continue(State, NewModState);
        {forward, NewModState} ->
            RegName = impure(State, riak_core_vnode_master, reg_name, [Mod]),
            impure(State, riak_core_vnode_master, command, 
                   [{Index, HN}, Request, Sender, RegName]),
            continue(State, NewModState);
        {drop, NewModState} ->
            continue(State, NewModState);
        {stop, Reason, NewModState} ->
            {stop, Reason, State#state{modstate=NewModState}}
    end.

active(timeout, State=#state{mod=Mod, modstate=ModState}) ->
    case should_handoff(State) of
        {true, TargetNode} ->
            case impure(State, {mod, Mod}, handoff_starting,
                        [TargetNode, ModState]) of
                {true, NewModState} ->
                    start_handoff(State#state{modstate=NewModState}, TargetNode);
                {false, NewModState} ->
                    continue(State, NewModState)
            end;
        false ->
            continue(State)
    end;
active(?VNODE_REQ{sender=Sender, request=Request},
       State=#state{handoff_node=HN}) when HN =:= none ->
    vnode_command(Sender, Request, State);
active(?VNODE_REQ{sender=Sender, request=Request},State) ->
    vnode_handoff_command(Sender, Request, State);
active(handoff_complete, State=#state{mod=Mod, 
                                      modstate=ModState,
                                      index=Idx, 
                                      handoff_node=HN,
                                      handoff_token=HT}) ->
    impure(State, riak_core_handoff_manager, release_handoff_lock,
           [{Mod, Idx}, HT]),
    impure(State, {mod, Mod}, handoff_finished, [HN, ModState]),
    {ok, NewModState} = impure(State, {mod, Mod}, delete, [ModState]),
    impure(State, riak_core_handoff_manager, add_exclusion, [Mod, Idx]),
    {stop, normal, State#state{modstate=NewModState, handoff_node=none}}.

active(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, active, State, State#state.inactivity_timeout}.

handle_event(R=?VNODE_REQ{}, _StateName, State) ->
    active(R, State).

handle_sync_event(get_mod_index, _From, StateName,
                  State=#state{index=Idx,mod=Mod}) ->
    {reply, {Mod, Idx}, StateName, State, State#state.inactivity_timeout};
handle_sync_event({handoff_data,BinObj}, _From, StateName, 
                  State=#state{mod=Mod, modstate=ModState}) ->
    case impure(State, {mod, Mod}, handle_handoff_data, [BinObj, ModState]) of
        {reply, ok, NewModState} ->
            {reply, ok, StateName, State#state{modstate=NewModState},
             State#state.inactivity_timeout};
        {reply, {error, Err}, NewModState} ->
            impure(State, error_logger, error_msg,
                   ["Error storing handoff obj: ~p~n", [Err]]),
            {reply, {error, Err}, StateName, State#state{modstate=NewModState}, 
             State#state.inactivity_timeout}
    end.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State, State#state.inactivity_timeout}.

terminate(Reason, _StateName, #state{mod=Mod, modstate=ModState} = State) ->
    impure(State, {mod, Mod}, terminate, [Reason, ModState]),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

should_handoff(#state{index=Idx, mod=Mod} = S) ->
    {ok, Ring} = impure(S, riak_core_ring_manager, get_my_ring, []),
    Me = impure(S, erlang, node, []),
    case riak_core_ring:index_owner(Ring, Idx) of
        Me ->
            false;
        TargetNode ->
            A = case string:tokens(atom_to_list(Mod), "_") of
                    ["riak", Middle, "vnode"] -> Middle;
                    [First, "vnode"]          -> First
                end,
            App = list_to_atom("riak_" ++ A),
            AppNodes = impure(S, riak_core_node, watcher_nodes, [App]),
            case lists:member(TargetNode, AppNodes) of
                false  -> false;
                true -> {true, TargetNode}
            end
    end.

start_handoff(State=#state{index=Idx, mod=Mod, modstate=ModState}, TargetNode) ->
    case impure(State, {mod, Mod}, is_empty, [ModState]) of
        {true, NewModState} ->
            {ok, NewModState1} = impure(State,
                                        {mod, Mod}, delete, [NewModState]),
            impure(State, riak_core_handoff_manager, add_exclusion, [Mod, Idx]),
            {stop, normal, State#state{modstate=NewModState1}};
        {false, NewModState} ->  
            case impure(State, riak_core_handoff_manager, get_handoff_lock,
                        [{Mod, Idx}]) of
                {error, max_concurrency} ->
                    {ok, NewModState1} = impure(State,
                                                {mod, Mod}, handoff_cancelled,
                                                [NewModState]),
                    NewState = State#state{modstate=NewModState1},
                    {next_state, active, NewState, ?LOCK_RETRY_TIMEOUT};
                {ok, {handoff_token, HandoffToken}} ->
                    NewState = State#state{modstate=NewModState, 
                                           handoff_token=HandoffToken,
                                           handoff_node=TargetNode},
                    impure(NewState, riak_core_handoff_sender, start_link,
                           [TargetNode, Mod, Idx]),
                    continue(NewState)
            end
    end.
            

%% @doc Send a reply to a vnode request.  If 
%%      the Ref is undefined just send the reply
%%      for compatibility with pre-0.12 requestors.
%%      If Ref is defined, send it along with the
%%      reply.
%%      
-spec reply(sender(), term()) -> true.
reply({fsm, undefined, From}, Reply) ->
    gen_fsm:send_event(From, Reply);
reply({fsm, Ref, From}, Reply) ->
    gen_fsm:send_event(From, {Ref, Reply});
reply({server, undefined, From}, Reply) ->
    gen_server:reply(From, Reply);
reply({server, Ref, From}, Reply) ->
    gen_server:reply(From, {Ref, Reply});
reply({raw, Ref, From}, Reply) ->
    From ! {Ref, Reply};
reply(ignore, _Reply) ->
    ok.
                   
make_vnode_request(Request) ->
    ?VNODE_REQ{request=Request}.

%% Impure handling stuff

imp_interp(Fun, Arg) when not is_tuple(Fun), is_function(Fun, 1) ->
    Fun(Arg);
imp_interp(Else, _) ->
    Else.

impure(#state{pure_p = false}, {mod, Mod}, Func, ArgList) ->
    erlang:apply(Mod, Func, ArgList);
impure(#state{pure_p = false}, Mod, Func, ArgList) ->
    erlang:apply(Mod, Func, ArgList);
impure(#state{pure_opts = PureOpts}, {mod, _Mod}, Func, ArgList) ->
    imp_interp(proplists:get_value({mod, Func}, PureOpts), ArgList);
impure(#state{pure_opts = PureOpts}, Mod, Func, ArgList) ->
    imp_interp(proplists:get_value({Mod, Func}, PureOpts), ArgList).

