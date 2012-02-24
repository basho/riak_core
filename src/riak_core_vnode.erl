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
-include("riak_core_vnode.hrl").
-export([behaviour_info/1]).
-export([start_link/3,
         start_link/4,
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
-export([reply/2,
         monitor/1]).
-export([get_mod_index/1,
         set_forwarding/2,
         trigger_handoff/2,
         core_status/1,
         handoff_error/3]).

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
          handoff_node=none :: none | node(),
          pool_pid :: pid() | undefined,
          manager_event_timer :: reference(),
          inactivity_timeout}).

start_link(Mod, Index, Forward) ->
    start_link(Mod, Index, 0, Forward).

start_link(Mod, Index, InitialInactivityTimeout, Forward) ->
    gen_fsm:start_link(?MODULE,
                       [Mod, Index, InitialInactivityTimeout, Forward], []).

%% Send a command message for the vnode module by Pid -
%% typically to do some deferred processing after returning yourself
send_command(Pid, Request) ->
    gen_fsm:send_event(Pid, ?VNODE_REQ{request=Request}).


%% Sends a command to the FSM that called it after Time
%% has passed.
-spec send_command_after(integer(), term()) -> reference().
send_command_after(Time, Request) ->
    gen_fsm:send_event_after(Time, ?VNODE_REQ{request=Request}).


init([Mod, Index, InitialInactivityTimeout, Forward]) ->
    process_flag(trap_exit, true),
    {ModState, Props} = case Mod:init([Index]) of
        {ok, MS} -> {MS, []};
        {ok, MS, P} -> {MS, P};
        {error, R} -> {error, R}
    end,
    case {ModState, Props} of
        {error, Reason} ->
            {stop, Reason};
        _ ->
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
            State = #state{index=Index, mod=Mod, modstate=ModState, forward=Forward,
                inactivity_timeout=Timeout, pool_pid=PoolPid},
            lager:debug("vnode :: ~p/~p :: ~p~n", [Mod, Index, Forward]),
            {ok, active, State, InitialInactivityTimeout}
    end.

handoff_error(Vnode, Err, Reason) ->
    gen_fsm:send_event(Vnode, {handoff_error, Err, Reason}).

get_mod_index(VNode) ->
    gen_fsm:sync_send_all_state_event(VNode, get_mod_index).

set_forwarding(VNode, ForwardTo) ->
    gen_fsm:send_all_state_event(VNode, {set_forwarding, ForwardTo}).

trigger_handoff(VNode, TargetNode) ->
    gen_fsm:send_all_state_event(VNode, {trigger_handoff, TargetNode}).

core_status(VNode) ->
    gen_fsm:sync_send_all_state_event(VNode, core_status).

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

active(timeout, State=#state{mod=Mod, index=Idx}) ->
    riak_core_vnode_manager:vnode_event(Mod, Idx, self(), inactive),
    continue(State);
active(?COVERAGE_REQ{keyspaces=KeySpaces,
                     request=Request,
                     sender=Sender}, State) ->
    %% Coverage request handled in handoff and non-handoff.  Will be forwarded if set.
    vnode_coverage(Sender, Request, KeySpaces, State);
active(?VNODE_REQ{sender=Sender, request=Request},
       State=#state{handoff_node=HN}) when HN =:= none ->
    vnode_command(Sender, Request, State);
active(?VNODE_REQ{sender=Sender, request=Request},State) ->
    vnode_handoff_command(Sender, Request, State);
active(handoff_complete, State) ->
    State2 = start_manager_event_timer(handoff_complete, State),
    continue(State2);
active({handoff_error, _Err, _Reason}, State) ->
    State2 = start_manager_event_timer(handoff_error, State),
    continue(State2);
active({send_manager_event, Event}, State) ->
    State2 = start_manager_event_timer(Event, State),
    continue(State2);
active({trigger_handoff, TargetNode}, State) ->
     maybe_handoff(State, TargetNode);
active(unregistered, State=#state{mod=Mod, index=Index}) ->
    %% Add exclusion so the ring handler will not try to spin this vnode
    %% up until it receives traffic.
    riak_core_handoff_manager:add_exclusion(Mod, Index),
    lager:debug("~p ~p vnode excluded and unregistered.",
                [Index, Mod]),
    {stop, normal, State#state{handoff_node=none,
                               pool_pid=undefined}}.

active(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, active, State, State#state.inactivity_timeout}.

%% This code lives in riak_core_vnode rather than riak_core_vnode_manager
%% because the ring_trans call is a synchronous call to the ring manager,
%% and it is better to block an individual vnode rather than the vnode
%% manager. Blocking the manager can impact all vnodes. This code is safe
%% to execute on multiple parallel vnodes because of the synchronization
%% afforded by having all ring changes go through the single ring manager.
mark_handoff_complete(Idx, Prev, New, Mod) ->
    Result = riak_core_ring_manager:ring_trans(
      fun(Ring, _) ->
              Owner = riak_core_ring:index_owner(Ring, Idx),
              {_, NextOwner, Status} = riak_core_ring:next_owner(Ring, Idx, Mod),
              NewStatus = riak_core_ring:member_status(Ring, New),

              case {Owner, NextOwner, NewStatus, Status} of
                  {Prev, New, _, awaiting} ->
                      Ring2 = riak_core_ring:handoff_complete(Ring, Idx, Mod),
                      %% Optimization. Only alter the local ring without
                      %% triggering a gossip, thus implicitly coalescing
                      %% multiple vnode handoff completion events. In the
                      %% future we should decouple vnode handoff state from
                      %% the ring structure in order to make gossip independent
                      %% of ring size.
                      {set_only, Ring2};
                  _ ->
                      ignore
              end
      end, []),

    case Result of
        {ok, NewRing} ->
            NewRing = NewRing;
        _ ->
            {ok, NewRing} = riak_core_ring_manager:get_my_ring()
    end,

    Owner = riak_core_ring:index_owner(NewRing, Idx),
    {_, NextOwner, Status} = riak_core_ring:next_owner(NewRing, Idx, Mod),
    NewStatus = riak_core_ring:member_status(NewRing, New),

    case {Owner, NextOwner, NewStatus, Status} of
        {_, _, invalid, _} ->
            %% Handing off to invalid node, don't give-up data.
            continue;
        {Prev, New, _, _} ->
            forward;
        {Prev, _, _, _} ->
            %% Handoff wasn't to node that is scheduled in next, so no change.
            continue;
        {_, _, _, _} ->
            shutdown
    end.

finish_handoff(State=#state{mod=Mod,
                            modstate=ModState,
                            index=Idx,
                            handoff_node=HN,
                            pool_pid=Pool}) ->
    case mark_handoff_complete(Idx, node(), HN, Mod) of
        continue ->
            continue(State#state{handoff_node=none});
        Res when Res == forward; Res == shutdown ->
            %% Have to issue the delete now.  Once unregistered the
            %% vnode master will spin up a new vnode on demand.
            %% Shutdown the async pool beforehand, don't want callbacks
            %% running on non-existant data.
            case is_pid(Pool) of
                true ->
                    %% state.pool_pid will be cleaned up by handle_info message.
                    riak_core_vnode_worker_pool:shutdown_pool(Pool, 60000);
                _ ->
                    ok
            end,
            {ok, NewModState} = Mod:delete(ModState),
            lager:debug("~p ~p vnode finished handoff and deleted.",
                        [Idx, Mod]),
            riak_core_vnode_manager:unregister_vnode(Idx, Mod),
            lager:debug("vnode hn/fwd :: ~p/~p :: ~p -> ~p~n",
                        [State#state.mod, State#state.index, State#state.forward, HN]),
            continue(State#state{modstate={deleted,NewModState}, % like to fail if used
                                 handoff_node=none,
                                 forward=HN})
    end.

handle_event({set_forwarding, undefined}, _StateName,
             State=#state{modstate={deleted, _ModState}}) ->
    %% The vnode must forward requests when in the deleted state, therefore
    %% ignore requests to stop forwarding.
    continue(State);
handle_event({set_forwarding, ForwardTo}, _StateName, State) ->
    lager:debug("vnode fwd :: ~p/~p :: ~p -> ~p~n",
                [State#state.mod, State#state.index, State#state.forward, ForwardTo]),
    continue(State#state{forward=ForwardTo});
handle_event(finish_handoff, _StateName,
             State=#state{modstate={deleted, _ModState}}) ->
    stop_manager_event_timer(State),
    continue(State#state{handoff_node=none});
handle_event(finish_handoff, _StateName, State=#state{mod=Mod,
                                                      modstate=ModState,
                                                      handoff_node=HN}) ->
    stop_manager_event_timer(State),
    case HN of
        none ->
            continue(State);
        _ ->
            {ok, NewModState} = Mod:handoff_finished(HN, ModState),
            finish_handoff(State#state{modstate=NewModState})
    end;
handle_event(cancel_handoff, _StateName, State=#state{mod=Mod,
                                                      modstate=ModState}) ->
    %% it would be nice to pass {Err, Reason} to the vnode but the 
    %% API doesn't currently allow for that.
    stop_manager_event_timer(State),
    case State#state.handoff_node of
        none ->
            continue(State);
        _ ->
            {ok, NewModState} = Mod:handoff_cancelled(ModState),
            continue(State#state{handoff_node=none, modstate=NewModState})
    end;
handle_event({trigger_handoff, _TargetNode}, _StateName,
             State=#state{modstate={deleted, _ModState}}) ->
    continue(State);
handle_event(R={trigger_handoff, _TargetNode}, _StateName, State) ->
    active(R, State);
handle_event(R=?VNODE_REQ{}, _StateName, State) ->
    active(R, State);
handle_event(R=?COVERAGE_REQ{}, _StateName, State) ->
    active(R, State).


handle_sync_event(get_mod_index, _From, StateName,
                  State=#state{index=Idx,mod=Mod}) ->
    {reply, {Mod, Idx}, StateName, State, State#state.inactivity_timeout};
handle_sync_event({handoff_data,_BinObj}, _From, StateName,
                  State=#state{modstate={deleted, _ModState}}) ->
    {reply, {error, vnode_exiting}, StateName, State,
     State#state.inactivity_timeout};
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
    end;
handle_sync_event(core_status, _From, StateName, State=#state{index=Index,
                                                              mod=Mod,
                                                              modstate=ModState,
                                                              handoff_node=HN,
                                                              forward=FN}) ->
    Mode = case {FN, HN} of
               {undefined, none} ->
                   active;
               {undefined, HN} ->
                   handoff;
               {FN, none} ->
                   forward;
               _ ->
                   undefined
           end,
    Status = [{index, Index}, {mod, Mod}] ++
        case FN of
            undefined ->
                [];
            _ ->
                [{forward, FN}]
        end++
        case HN of
            none ->
                [];
            _ ->
                [{handoff_node, HN}]
        end ++
        case ModState of
            {deleted, _} ->
                [deleted];
            _ ->
                []
        end,
    {reply, {Mode, Status}, StateName, State, State#state.inactivity_timeout}.


handle_info({'EXIT', Pid, Reason}, _StateName,
            State=#state{mod=Mod, index=Index, pool_pid=Pid}) ->
    case Reason of
        Reason when Reason == normal; Reason == shutdown ->
            ok;
        _ ->
            lager:error("~p ~p worker pool crashed ~p\n", [Index, Mod, Reason])
    end,
    continue(State#state{pool_pid=undefined});

handle_info(Info, _StateName,
            State=#state{mod=Mod,modstate={deleted, _},index=Index}) ->
    lager:info("~p ~p ignored handle_info ~p - vnode unregistering\n",
               [Index, Mod, Info]),
    continue(State);
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
            {stop, Reason1, NewModState} ->
                 {stop, Reason1, State#state{modstate=NewModState}}
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
        pool_pid=Pool}) ->
    %% Shutdown if the pool is still alive - there could be a race on
    %% delivery of the unregistered event and successfully shutting
    %% down the pool.
    case is_pid(Pool) andalso is_process_alive(Pool) of
        true ->
            riak_core_vnode_worker_pool:shutdown_pool(Pool, 60000);
        _ ->
            ok
    end,
    case ModState of
        %% Handoff completed, Mod:delete has been called, now terminate.
        {deleted, ModState1} ->
            Mod:terminate(Reason, ModState1);
        _ ->
            Mod:terminate(Reason, ModState)
    end.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

maybe_handoff(State=#state{modstate={deleted, _}}, _TargetNode) ->
    %% Modstate has been deleted, waiting for unregistered.  No handoff.
    continue(State);
maybe_handoff(State=#state{index=Idx, mod=Mod, modstate=ModState,
                           handoff_node=HN}, TargetNode) ->
    case HN of
        none ->
            ok;
        TargetNode ->
            ok;
        _ ->
            lager:info("~s/~b: handoff request to ~p before "
                       "finishing handoff to ~p", [Mod, Idx, TargetNode, HN])
    end,
    case Mod:handoff_starting(TargetNode, ModState) of
        {true, NewModState} ->
            start_handoff(State#state{modstate=NewModState}, TargetNode);
        {false, NewModState} ->
            continue(State, NewModState)
    end.

start_handoff(State=#state{index=Idx, mod=Mod, modstate=ModState}, TargetNode) ->
    case Mod:is_empty(ModState) of
        {true, NewModState} ->
            finish_handoff(State#state{modstate=NewModState,
                                       handoff_node=TargetNode});
        {false, NewModState} ->
            case riak_core_handoff_manager:add_outbound(Mod,Idx,TargetNode,self()) of
                {ok,_Pid} ->
                    NewState = State#state{modstate=NewModState,
                                           handoff_node=TargetNode},
                    continue(NewState);
                {error,_Reason} ->
                    continue(State#state{modstate=NewModState})
            end
    end.


%% @doc Send a reply to a vnode request.  If
%%      the Ref is undefined just send the reply
%%      for compatibility with pre-0.12 requestors.
%%      If Ref is defined, send it along with the
%%      reply.
%%
-spec reply(sender(), term()) -> any().
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

%% @doc Set up a monitor for the pid named by a {@type sender()} vnode
%% argument.  If `Sender' was the atom `ignore', this function sets up
%% a monitor on `self()' in order to return a valid (if useless)
%% monitor reference.
-spec monitor(Sender::sender()) -> Monitor::reference().
monitor({fsm, _, From}) ->
    erlang:monitor(process, From);
monitor({server, _, {Pid, _Ref}}) ->
    erlang:monitor(process, Pid);
monitor({raw, _, From}) ->
    erlang:monitor(process, From);
monitor(ignore) ->
    erlang:monitor(process, self()).

%% Individual vnode processes and the vnode manager are tightly coupled. When
%% vnode events occur, the vnode must ensure that the events are forwarded to
%% the vnode manager, which will make a state change decision and send an
%% appropriate message back to the vnode. To minimize blocking, asynchronous
%% messaging is used. It is possible for the vnode manager to crash and miss
%% messages sent by the vnode. Therefore, the vnode periodically resends event
%% messages until an appropriate message is received back from the vnode
%% manager. The event timer functions below implement this logic.
start_manager_event_timer(Event, State=#state{mod=Mod, index=Idx}) ->
    riak_core_vnode_manager:vnode_event(Mod, Idx, self(), Event),
    stop_manager_event_timer(State),
    T2 = gen_fsm:send_event_after(30000, {send_manager_event, Event}),
    State#state{manager_event_timer=T2}.

stop_manager_event_timer(#state{manager_event_timer=undefined}) ->
    ok;
stop_manager_event_timer(#state{manager_event_timer=T}) ->
    gen_fsm:cancel_timer(T).
