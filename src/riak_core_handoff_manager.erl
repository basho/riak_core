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

%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_core_handoff_manager).
-behaviour(gen_server).

%% gen_server api
-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

%% exclusion api
-export([add_exclusion/2,
         get_exclusions/1,
         remove_exclusion/2
        ]).

%% handoff api
-export([add_outbound/4,
         clear_queue/0,
         handoff_status/0,
         set_concurrency/1,
         update_status/2
        ]).

-include_lib("riak_core/include/riak_core_handoff.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, { excl,
                 handoffs        :: [#handoff{}],
                 handoff_queue   :: [#handoff{}]
               }).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #state{excl=ordsets:new(), handoffs=[], handoff_queue=[]}}.


%% enqueue a handoffs to happen at a later point
add_outbound(VnodePid, Module, Idx, Node) ->
    gen_server:call(?MODULE,{add_outbound,VnodePid,Module,Idx,Node}).

%% cancel all pending handoffs
clear_queue() ->
    gen_server:call(?MODULE, cancel_pending_handoffs).

%% get a pair of all currently active and pending handoffs
handoff_status() ->
    gen_server:call(?MODULE, handoff_status).

%% change the limit of concurrent handoffs (outbound and inbound)
set_concurrency(Limit) ->
    %% TODO: if Limit < CurrentLimit, kill old handoffs?
    application:set_env(riak_core,handoff_concurrency,Limit).

update_status(SenderPid, Status) ->
    gen_server:call(?MODULE, {update_status, SenderPid, Status}).

add_exclusion(Module, Index) ->
    gen_server:cast(?MODULE, {add_exclusion, {Module, Index}}).

remove_exclusion(Module, Index) ->
    gen_server:cast(?MODULE, {del_exclusion, {Module, Index}}).

get_exclusions(Module) ->
    gen_server:call(?MODULE, {get_exclusions, Module}, infinity).


%% synchronous events
handle_call({get_exclusions, Module}, _From, State=#state{excl=Excl}) ->
    Reply =  [I || {M, I} <- ordsets:to_list(Excl), M =:= Module],
    {reply, {ok, Reply}, State};
handle_call(handoff_status, _From, State) ->
    Status=[{active,State#state.handoffs},{pending,State#state.handoff_queue}],
    {reply, {ok, Status}, State};
handle_call({add_outbound,Pid,M,I,N}, _From, State=#state{handoff_queue=Q}) ->
    NewState=State#state{handoff_queue=enqueue_handoff(M,I,N,Pid,Q)},
    {reply, ok, process_handoff_queue(NewState)};
handle_call(cancel_pending_handoffs, _From, State) ->
    {reply, ok, State#state{handoff_queue=[]}};
handle_call({update_status,SenderPid,Status},_From,State=#state{handoffs=HS}) ->
    case lists:keytake(SenderPid,#handoff.sender_pid,HS) of
        {value,H,NewHS} ->
            Handoff=H#handoff{status=Status},
            {reply, ok, State#state{handoffs=[Handoff|NewHS]}};
        _ ->
            {reply, {error, not_found}, State}
    end.

%% fire and forget events
handle_cast(process_handoff_queue, State) ->
    {noreply,process_handoff_queue(State)};
handle_cast({del_exclusion, {Mod, Idx}}, State=#state{excl=Excl}) ->
    {noreply, State#state{excl=ordsets:del_element({Mod, Idx}, Excl)}};
handle_cast({add_exclusion, {Mod, Idx}}, State=#state{excl=Excl}) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    riak_core_ring_events:ring_update(Ring),
    {noreply, State#state{excl=ordsets:add_element({Mod, Idx}, Excl)}}.


%% we monitor sender processes, when one goes down, we can start another
handle_info({'DOWN',_Ref,process,Pid,Reason},State=#state{handoffs=HS,
                                                          handoff_queue=Q}) ->
    {value,_Handoff,NewHS}=lists:keytake(Pid,#handoff.sender_pid,HS),
    case Reason of
        normal ->
            NewState=process_handoff_queue(State#state{handoffs=NewHS});
        _ ->
            %% requeue the failed attempt (TODO:)
            NewState=process_handoff_queue(State#state{handoffs=NewHS,
                                                       handoff_queue=Q})
    end,
    {noreply,NewState};
handle_info(_Info, State) ->
    {noreply, State}.

%% stop the gen_server
terminate(_Reason, _State) ->
    ok.

%% hot-swap code
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private - shared gen_server functionality

%% add a handoff to a queue, return a new queue
enqueue_handoff(Mod,Idx,Node,VnodePid,Q) ->
    {Q2,Pred}=lists:foldl(
                fun (H=#handoff{module=M,index=I,target_node=N,vnode_pid=P},
                     {NewQ,Flag}) ->
                        %% a different module or index, keep it
                        case (M==Mod) and (I==Idx) of
                            false -> {[H | NewQ], Flag};
                            true ->
                                %% or if the same node and pid, keep it
                                case (N==Node) and (P==VnodePid) of
                                    true -> {[H | NewQ],false};
                                    false -> {NewQ,Flag}
                                end
                        end
                end,
                {[],true},
                Q),

    %% the predicate indicates whether or not a new handoff should be added
    case Pred of
        true ->
            Handoff=#handoff{ module=Mod,
                              index=Idx,
                              target_node=Node,
                              restarts=0,
                              timestamp=now(),
                              vnode_pid=VnodePid
                            },
            Q2 ++ [Handoff];
        false ->
            Q2
    end.

%% true if handoff_concurrency (inbound + outbound) hasn't yet been reached
handoff_concurrency_limit_reached() ->
    Receivers=supervisor:count_children(riak_core_handoff_receiver_sup),
    Senders=supervisor:count_children(riak_core_handoff_sender_sup),
    ActiveReceivers=proplists:get_value(active,Receivers),
    ActiveSenders=proplists:get_value(active,Senders),
    Limit=app_helper:get_env(riak_core,handoff_concurrency,1),
    Limit =< (ActiveReceivers + ActiveSenders).

%% pop a handoff off the queue and start it up
process_handoff_queue(State) ->
    process_handoff_queue(handoff_concurrency_limit_reached(),State).

%% only pop a handoff and start it if we're under our limit
process_handoff_queue(false,State) ->
    State;
process_handoff_queue(true,State=#state{handoffs=HS,handoff_queue=Q}) ->
    case Q of
        [H=#handoff{module=M,index=I,target_node=N,vnode_pid=Pid}|QS] ->
            {ok,Sender}=riak_core_handoff_sender_sup:start_sender(Pid,N,M,I),
            erlang:monitor(process,Sender),
            NewHS=[H#handoff{sender_pid=Sender,timestamp=now()}|HS],
            State#state{handoffs=NewHS,handoff_queue=QS};
        [] ->
            State
    end.

%%
%% EUNIT tests...
%%

-ifdef (TEST).

handoff_test_ () ->
    {spawn,
     {setup,

      %% called when the tests start and complete...
      fun () -> {ok,Pid}=start_link(), Pid end,
      fun (Pid) -> exit(Pid,kill) end,

      %% actual list of test
      [?_test(simple_handoff())
      ]}}.

simple_handoff () ->
    ?assertEqual({ok,[{active,[]},{pending,[]}]},handoff_status()),

    %% add a handoff and confirm that it's there
    %%add_handoff(riak_kv_vnode,0,'node@nohost'),
    %?assertEqual({ok,[{{riak_kv_vnode,0},'node@nohost'}]},all_handoffs()),
    %?assertEqual({ok,[{riak_kv_vnode,'node@nohost'}]},get_handoffs(0)),

    %% remove the handoff and make sure it's gone
    %%remove_handoff(riak_kv_vnode,0),
    %?assertEqual({ok,[]},all_handoffs()),
    %?assertEqual({ok,[]},get_handoffs(0)),

    %% done
    ok.

-endif.
