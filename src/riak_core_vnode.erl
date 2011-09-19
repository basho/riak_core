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
-module('riak_core_vnode').
-behaviour(gen_fsm).
-include_lib("riak_core_vnode.hrl").
-export([behaviour_info/1]).
-export([start_link/2,
         start_link/3,
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
-export([get_mod_index/1,
         update_forwarding/2,
         trigger_handoff/1]).

-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
behaviour_info(callbacks) ->
    [{init,1},
     {handle_command,3},
     {handle_coverage,4},
     {handle_exit,3},
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

%% handle_exit/3 is an optional behaviour callback that can be implemented. 
%% It will be called in the case that a process that is linked to the vnode
%% process dies and allows the module using the behaviour to take appropriate
%% action. It is called by handle_info when it receives an {'EXIT', Pid, Reason}
%% message and the function signature is: handle_exit(Pid, Reason, State).
%%
%% It should return a tuple indicating the next state for the fsm. For a list of
%% valid return types see the documentation for the gen_fsm handle_info callback.
%%
%% Here is what the spec for handle_exit/3 would look like:
%% -spec handle_exit(pid(), atom(), term()) ->
%%                          {noreply, term()} |
%%                          {stop, term(), term()}

%% handle_info/2 is an optional behaviour callback too.
%% It will be called in the case when a vnode receives any other message
%% than an EXIT message.
%% The function signature is: handle_info(Info, State).
%% It should return a tuple of the form {ok, NextState}
%%
%% Here is what the spec for handle_info/2 would look like:
%% -spec handle_info(term(), term()) -> {ok, term()}

-define(DEFAULT_TIMEOUT, 60000).
-define(LOCK_RETRY_TIMEOUT, 10000).
-define(MODSTATE, State#state{mod=Mod,modstate=ModState}).
-record(state, {
          index :: partition(),
          mod :: module(),
          modstate :: term(),
          forward :: node(),
          handoff_token :: non_neg_integer(),
          handoff_node=none :: none | node(),
          handoff_pid :: pid(),
          pool_pid :: pid() | undefined,
          inactivity_timeout}).

start_link(Mod, Index) ->
    start_link(Mod, Index, 0).

start_link(Mod, Index, InitialInactivityTimeout) ->
    gen_fsm:start_link(?MODULE, [Mod, Index, InitialInactivityTimeout], []).

%% Send a command message for the vnode module by Pid - 
%% typically to do some deferred processing after returning yourself
send_command(Pid, Request) ->
    gen_fsm:send_event(Pid, ?VNODE_REQ{request=Request}).


%% Sends a command to the FSM that called it after Time 
%% has passed.
-spec send_command_after(integer(), term()) -> reference().
send_command_after(Time, Request) ->
    gen_fsm:send_event_after(Time, ?VNODE_REQ{request=Request}).
    

init([Mod, Index, InitialInactivityTimeout]) ->
    %%TODO: Should init args really be an array if it just gets Init?
    process_flag(trap_exit, true),
    {ModState, Props} = case Mod:init([Index]) of
        {ok, MS} -> {MS, []};
        {ok, MS, P} -> {MS, P}
    end,
    PoolPid = case lists:keyfind(pool, 1, Props) of
        {pool, WorkerModule, PoolSize, WorkerArgs} ->
            lager:debug("starting worker pool ~p with size of ~p~n",
                [WorkerModule, PoolSize]),
            {ok, Pid} = riak_core_vnode_worker_pool:start_link(WorkerModule,
                PoolSize, Index, WorkerArgs, worker_props),
            Pid;
        _ -> undefined
    end,
    riak_core_handoff_manager:remove_exclusion(Mod, Index),
    Timeout = app_helper:get_env(riak_core, vnode_inactivity_timeout, ?DEFAULT_TIMEOUT),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    State = #state{index=Index, mod=Mod, modstate=ModState,
                   inactivity_timeout=Timeout, pool_pid=PoolPid},
    State2 = update_forwarding_mode(Ring, State),
    {ok, active, State2, InitialInactivityTimeout}.

get_mod_index(VNode) ->
    gen_fsm:sync_send_all_state_event(VNode, get_mod_index).

update_forwarding(VNode, Ring) ->
    gen_fsm:send_all_state_event(VNode, {update_forwarding, Ring}).

trigger_handoff(VNode) ->
    gen_fsm:send_all_state_event(VNode, trigger_handoff).

continue(State) ->
    {next_state, active, State, State#state.inactivity_timeout}.

continue(State, NewModState) ->
    continue(State#state{modstate=NewModState}).

%% Active vnodes operate in three states: normal, handoff, and forwarding.
%%
%% In the normal state, vnode commands are passed to handle_command. When
%% a handoff is trigger, handoff_node is set to the target node and the vode
%% is said to be in the handoff state.
%%
%% In the handoff state, vnode commands are passed to handle_handoff_command.
%% However, a vnode may be blocked during handoff (and therefore not servicing
%% commands) if the handoff procedure is non-blocking (eg. in riak_kv when not
%% using async fold).
%%
%% After handoff, a vnode may move into forwarding state. The forwarding state
%% is a product of the new gossip/membership code and will not occur if the
%% node is running in legacy mode. The forwarding state represents the case
%% where the vnode has already handed its data off to the new owner, but the
%% new owner is not yet listed as the current owner in the ring. This may occur
%% because additional vnodes are still waiting to handoff their data to the
%% new owner, or simply because the ring has yet to converge on the new owner.
%% In the forwarding state, all vnode commands and coverage commands are
%% forwarded to the new owner for processing.

update_forwarding_mode(Ring, State=#state{index=Index, mod=Mod}) ->
    Node = node(),
    case riak_core_ring:next_owner(Ring, Index, Mod) of
        {Node, NextOwner, complete} ->
            riak_core_vnode_manager:set_not_forwarding(self(), false),
            State#state{forward=NextOwner};
        _ ->
            riak_core_vnode_manager:set_not_forwarding(self(), true),
            State#state{forward=undefined}
    end.

vnode_command(Sender, Request, State=#state{index=Index,
                                            mod=Mod,
                                            modstate=ModState,
                                            forward=Forward,
                                            pool_pid=Pool}) ->
    %% Check if we should forward
    case Forward of
        undefined ->
            Action = Mod:handle_command(Request, Sender, ModState);
        NextOwner ->
            lager:debug("Forwarding ~p -> ~p: ~p~n", [node(), NextOwner, Index]),
            riak_core_vnode_master:command({Index, NextOwner}, Request, Sender,
                                           riak_core_vnode_master:reg_name(Mod)),
            Action = continue
    end,
    case Action of
        continue ->
            continue(State, ModState);
        {reply, Reply, NewModState} ->
            reply(Sender, Reply),
            continue(State, NewModState);
        {noreply, NewModState} ->
            continue(State, NewModState);
        {async, Work, From, NewModState} ->
            %% dispatch some work to the vnode worker pool
            %% the result is sent back to 'From'
            riak_core_vnode_worker_pool:handle_work(Pool, Work, From),
            continue(State, NewModState);
        {stop, Reason, NewModState} ->
            {stop, Reason, State#state{modstate=NewModState}}
    end.

vnode_coverage(Sender, Request, KeySpaces, State=#state{index=Index,
                                                        mod=Mod,
                                                        modstate=ModState,
                                                        pool_pid=Pool,
                                                        forward=Forward}) ->
    %% Check if we should forward
    case Forward of
        undefined ->
            Action = Mod:handle_coverage(Request, KeySpaces, Sender, ModState);
        NextOwner ->
            lager:debug("Forwarding coverage ~p -> ~p: ~p~n", [node(), NextOwner, Index]),
            riak_core_vnode_master:coverage(Request, {Index, NextOwner},
                                            KeySpaces, Sender,
                                            riak_core_vnode_master:reg_name(Mod)),
            Action = continue
    end,
    case Action of
        continue ->
            continue(State, ModState);
        {reply, Reply, NewModState} ->
            reply(Sender, Reply),
            continue(State, NewModState);
        {noreply, NewModState} ->
            continue(State, NewModState);
        {async, Work, From, NewModState} ->
            %% dispatch some work to the vnode worker pool
            %% the result is sent back to 'From'
            riak_core_vnode_worker_pool:handle_work(Pool, Work, From),
            continue(State, NewModState);
        {stop, Reason, NewModState} ->
            {stop, Reason, State#state{modstate=NewModState}}
    end.

vnode_handoff_command(Sender, Request, State=#state{index=Index,
                                                    mod=Mod, 
                                                    modstate=ModState, 
                                                    handoff_node=HN,
                                                    pool_pid=Pool}) ->
    case Mod:handle_handoff_command(Request, Sender, ModState) of
        {reply, Reply, NewModState} ->
            reply(Sender, Reply),
            continue(State, NewModState);
        {noreply, NewModState} ->
            continue(State, NewModState);
        {async, Work, From, NewModState} ->
            %% dispatch some work to the vnode worker pool
            %% the result is sent back to 'From'
            riak_core_vnode_worker_pool:handle_work(Pool, Work, From),
            continue(State, NewModState);
        {forward, NewModState} ->
            riak_core_vnode_master:command({Index, HN}, Request, Sender, 
                                           riak_core_vnode_master:reg_name(Mod)),
            continue(State, NewModState);
        {drop, NewModState} ->
            continue(State, NewModState);
        {stop, Reason, NewModState} ->
            {stop, Reason, State#state{modstate=NewModState}}
    end.

active(timeout, State) ->
    maybe_handoff(State);
active(?COVERAGE_REQ{keyspaces=KeySpaces, 
                     request=Request,
                     sender=Sender},
       State=#state{handoff_node=HN}) when HN =:= none ->
    vnode_coverage(Sender, Request, KeySpaces, State);
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
    riak_core_handoff_manager:release_handoff_lock({Mod, Idx}, HT),
    Mod:handoff_finished(HN, ModState),
    finish_handoff(State);
active({handoff_error, _Err, _Reason}, State=#state{mod=Mod, 
                                                    modstate=ModState,
                                                    index=Idx, 
                                                    handoff_token=HT}) ->
    riak_core_handoff_manager:release_handoff_lock({Mod, Idx}, HT),
    %% it would be nice to pass {Err, Reason} to the vnode but the 
    %% API doesn't currently allow for that.
    Mod:handoff_cancelled(ModState),
    continue(State#state{handoff_node=none});
active({update_forwarding, Ring}, State) ->
    NewState = update_forwarding_mode(Ring, State),
    continue(NewState);
active(trigger_handoff, State) ->
     maybe_handoff(State).

active(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, active, State, State#state.inactivity_timeout}.

finish_handoff(State=#state{mod=Mod, 
                            modstate=ModState,
                            index=Idx,
                            pool_pid=Pool,
                            handoff_node=HN}) ->
    case riak_core_gossip:finish_handoff(Idx, node(), HN, Mod) of
        forward ->
            case is_pid(Pool) of
                true ->
                    riak_core_vnode_worker_pool:shutdown_pool(Pool, 60000);
                _ ->
                    ok
            end,
            {ok, NewModState} = Mod:delete(ModState),
            {stop, normal, State#state{modstate=NewModState,
                                       handoff_node=none,
                                       pool_pid=undefined,
                                       handoff_pid=undefined}};
        continue ->
            continue(State#state{handoff_node=none,
                                 handoff_pid=undefined});
        shutdown ->
            case is_pid(Pool) of
                true ->
                    riak_core_vnode_worker_pool:shutdown_pool(Pool, 60000);
                _ ->
                    ok
            end,
            {ok, NewModState} = Mod:delete(ModState),
            riak_core_handoff_manager:add_exclusion(Mod, Idx),
            {stop, normal, State#state{modstate=NewModState,
                                       handoff_node=none,
                                       pool_pid=undefined,
                                       handoff_pid=undefined}}
    end.

handle_event(R={update_forwarding, _Ring}, _StateName, State) ->
    active(R, State);
handle_event(R=trigger_handoff, _StateName, State) ->
    active(R, State);
handle_event(R=?VNODE_REQ{}, _StateName, State) ->
    active(R, State);
handle_event(R=?COVERAGE_REQ{}, _StateName, State) ->
    active(R, State).


handle_sync_event(get_mod_index, _From, StateName,
                  State=#state{index=Idx,mod=Mod}) ->
    {reply, {Mod, Idx}, StateName, State, State#state.inactivity_timeout};
handle_sync_event({handoff_data,BinObj}, _From, StateName, 
                  State=#state{mod=Mod, modstate=ModState}) ->
    case Mod:handle_handoff_data(BinObj, ModState) of
        {reply, ok, NewModState} ->
            {reply, ok, StateName, State#state{modstate=NewModState},
             State#state.inactivity_timeout};
        {reply, {error, Err}, NewModState} ->
            lager:error("~p failed to store handoff obj: ~p", [Mod, Err]),
            {reply, {error, Err}, StateName, State#state{modstate=NewModState}, 
             State#state.inactivity_timeout}
    end.

handle_info({'EXIT', Pid, _Reason}, _StateName, State=#state{handoff_pid=Pid}) ->
    continue(State#state{handoff_pid=undefined});

handle_info({'EXIT', Pid, Reason}, StateName, State=#state{mod=Mod,modstate=ModState}) ->
    %% A linked processes has died so use the
    %% handle_exit callback to allow the vnode 
    %% process to take appropriate action.
    %% If the function is not implemented default
    %% to crashing the process.
    try
        case Mod:handle_exit(Pid, Reason, ModState) of
            {noreply,NewModState} ->
                {next_state, StateName, State#state{modstate=NewModState},
                    State#state.inactivity_timeout};
            {stop, Reason, NewModState} ->
                 {stop, Reason, State#state{modstate=NewModState}}
        end
    catch
        _ErrorType:undef ->
            {stop, linked_process_crash, State}
    end;

handle_info(Info, StateName, State=#state{mod=Mod,modstate=ModState}) ->
    case erlang:function_exported(Mod, handle_info, 2) of
        true ->
            {ok, NewModState} = Mod:handle_info(Info, ModState),
            {next_state, StateName, State#state{modstate=NewModState},
             State#state.inactivity_timeout};
        false ->
            {next_state, StateName, State, State#state.inactivity_timeout}
    end.

terminate(Reason, _StateName, #state{mod=Mod, modstate=ModState,
        pool_pid=Pool,index=Index}) ->
    riak_core_vnode_master:unregister_vnode(Index,
        riak_core_vnode_master:reg_name(Mod)),
    case is_pid(Pool) of
        true ->
            riak_core_vnode_worker_pool:shutdown_pool(Pool, 60000);
        _ ->
            ok
    end,
    Mod:terminate(Reason, ModState).

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

maybe_handoff(State=#state{mod=Mod, modstate=ModState}) ->
    case should_handoff(State) of
        {true, TargetNode} ->
            case Mod:handoff_starting(TargetNode, ModState) of
                {true, NewModState} ->
                    start_handoff(State#state{modstate=NewModState}, TargetNode);
                {false, NewModState} ->
                    continue(State, NewModState)
            end;
        false ->
            continue(State)
    end.

should_handoff(#state{handoff_node=HN}) when HN /= none ->
    %% Already handing off
    false;
should_handoff(#state{index=Idx, mod=Mod}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Me = node(),
    {_, NextOwner, _} = riak_core_ring:next_owner(Ring, Idx),
    Owner = riak_core_ring:index_owner(Ring, Idx),
    Ready = riak_core_ring:ring_ready(Ring),
    TargetNode = case {Ready, Owner, NextOwner} of
                     {_, _, Me} ->
                         Me;
                     {_, Me, undefined} ->
                         Me;
                     {true, Me, _} ->
                         NextOwner;
                     {_, _, undefined} ->
                         Owner;
                     {_, _, _} ->
                         Me
                 end,
    case TargetNode of
        Me ->
            false;
        _ ->
            case app_for_vnode_module(Mod) of
                undefined -> false;
                {ok, App} ->
                    case lists:member(TargetNode, 
                                      riak_core_node_watcher:nodes(App)) of
                        false  -> false;
                        true -> {true, TargetNode}
                    end
            end
    end.

start_handoff(State=#state{index=Idx, mod=Mod, modstate=ModState}, TargetNode) ->
    case Mod:is_empty(ModState) of
        {true, NewModState} ->
            finish_handoff(State#state{modstate=NewModState,
                                       handoff_node=TargetNode});
        {false, NewModState} ->  
            case riak_core_handoff_manager:get_handoff_lock({Mod, Idx}) of
                {error, max_concurrency} ->
                    {ok, NewModState1} = Mod:handoff_cancelled(NewModState),
                    NewState = State#state{modstate=NewModState1},
                    {next_state, active, NewState, ?LOCK_RETRY_TIMEOUT};
                {ok, {handoff_token, HandoffToken}} ->
                    NewState = State#state{modstate=NewModState, 
                                           handoff_token=HandoffToken,
                                           handoff_node=TargetNode},
                    {ok, HandoffPid} = riak_core_handoff_sender:start_link(TargetNode, Mod, Idx),
                    continue(NewState#state{handoff_pid=HandoffPid})
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

app_for_vnode_module(Mod) when is_atom(Mod) ->
    case application:get_env(riak_core, vnode_modules) of
        {ok, Mods} ->
            case lists:keysearch(Mod, 2, Mods) of
                {value, {App, Mod}} ->
                    {ok, App};
                false ->
                    undefined
            end;
        undefined -> undefined
    end.
