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
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([add_exclusion/2, get_handoff_lock/1, get_exclusions/1]).
-export([remove_exclusion/2]).
-export([release_handoff_lock/2]).
-export([add_outbound/3,clear_queue/0,all_handoffs/0]).
-record(state, {excl,handoffs,handoff_queue}).

-include_lib("eunit/include/eunit.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #state{excl=ordsets:new(), handoffs=[], handoff_queue=queue:new()}}.


%% enqueue a handoffs to happen at a later point
add_outbound(Module, Idx, Node) ->
    ok=gen_server:call(?MODULE,{add_outbound,Module,Idx,Node}),
    gen_server:cast(?MODULE,process_handoff_queue).

%% cancel all pending handoffs
clear_queue() ->
    gen_server:cast(?MODULE, cancel_pending_handoffs).

%% get a pair of all currently active and pending handoffs
all_handoffs() ->
    gen_server:call(?MODULE, all_handoffs).

add_exclusion(Module, Index) ->
    gen_server:cast(?MODULE, {add_exclusion, {Module, Index}}).

remove_exclusion(Module, Index) ->
    gen_server:cast(?MODULE, {del_exclusion, {Module, Index}}).

get_exclusions(Module) ->
    gen_server:call(?MODULE, {get_exclusions, Module}, infinity).

get_handoff_lock(LockId) ->
    TokenCount = app_helper:get_env(riak_core, handoff_concurrency, 1),
    get_handoff_lock(LockId, TokenCount).

get_handoff_lock(_LockId, 0) ->
    {error, max_concurrency};
get_handoff_lock(LockId, Count) ->
    case global:set_lock({{handoff_token, Count}, {node(), LockId}}, [node()], 0) of
        true ->
            {ok, {handoff_token, Count}};
        false ->
            get_handoff_lock(LockId, Count-1)
    end.

release_handoff_lock(LockId, Token) ->
    global:del_lock({{handoff_token,Token}, {node(), LockId}}, [node()]).


handle_call({get_exclusions, Module}, _From, State=#state{excl=Excl}) ->
    Reply =  [I || {M, I} <- ordsets:to_list(Excl), M =:= Module],
    {reply, {ok, Reply}, State};
handle_call(all_handoffs, _From, State=#state{handoff_queue=Q}) ->
    Active=[Handoff || {_Pid,Handoff} <- State#state.handoffs],
    Pending=queue:to_list(Q),
    {reply, {ok, Active, Pending}, State};
handle_call({add_outbound,Mod,Idx,Node},_From,State=#state{handoff_queue=Q}) ->
    Handoff={{Mod,Idx},Node},
    case queue:member(Handoff,Q) of
        false -> {reply,ok,State#state{handoff_queue=queue:in(Handoff,Q)}};
        true -> {reply,ok,State}
    end.


handle_cast(process_handoff_queue, State) ->
    {noreply,process_handoff_queue(State)};
handle_cast(cancel_pending_handoffs, State) ->
    {noreply,State#state{handoff_queue=queue:new()}};
handle_cast({del_exclusion, {Mod, Idx}}, State=#state{excl=Excl}) ->
    {noreply, State#state{excl=ordsets:del_element({Mod, Idx}, Excl)}};
handle_cast({add_exclusion, {Mod, Idx}}, State=#state{excl=Excl}) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    riak_core_ring_events:ring_update(Ring),
    {noreply, State#state{excl=ordsets:add_element({Mod, Idx}, Excl)}}.


%% we monitor sender processes, when one goes down, we can start another
handle_info({'DOWN',_Ref,process,Pid,_Reason},State=#state{handoffs=HS}) ->
    NewHS=lists:keydelete(Pid,1,HS),
    {noreply,process_handoff_queue(State#state{handoffs=NewHS})};
handle_info(_Info, State) ->
    {noreply, State}.

%% stop the gen_server
terminate(_Reason, _State) ->
    ok.

%% hot-swap code
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private - shared gen_server functionality

%% pop a handoff off the queue and start it up
process_handoff_queue(State) ->
    NumSenders=riak_core_handoff_sender_sup:active_senders(),
    NumReceivers=riak_core_handoff_receiver_sup:active_receivers(),
    MaxHandoffs=app_helper:get_env(riak_core,max_handoffs,1),
    process_handoff_queue((NumSenders + NumReceivers) < MaxHandoffs,State).

%% only pop a handoff and start it if we're under our limit
process_handoff_queue(false,State) ->
    State;
process_handoff_queue(true,State=#state{handoffs=Handoffs,handoff_queue=Q}) ->
    case queue:out(Q) of
        {{value,Handoff={{Mod,Idx},Node}},Q2} ->
            {ok,Pid}=riak_core_handoff_sender_sup:start_sender(Node,Mod,Idx),
            erlang:monitor(process,Pid),
            NewHoffs=[{Pid,Handoff}|Handoffs],
            State#state{handoffs=NewHoffs,handoff_queue=Q2};
        {empty,_} ->
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
    ?assertEqual({ok,[]},all_handoffs()),

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
