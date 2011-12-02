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
-export([add_handoff/3,remove_handoff/2,all_handoffs/0,get_handoffs/1]).
-record(state, {excl,handoffs}).

-include_lib("eunit/include/eunit.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #state{excl=ordsets:new(), handoffs=[]}}.

add_handoff(Module, Idx, Node) ->
    gen_server:cast(?MODULE, {add_handoff, {Module, Idx, Node}}).

remove_handoff(Module, Idx) ->
    gen_server:cast(?MODULE, {remove_handoff, {Module, Idx}}).

all_handoffs() ->
    gen_server:call(?MODULE, all_handoffs).

get_handoffs(Idx) ->
    gen_server:call(?MODULE, {get_handoffs, Idx}).

add_exclusion(Module, Index) ->
    gen_server:cast(?MODULE, {add_exclusion, {Module, Index}}).

remove_exclusion(Module, Index) ->
    gen_server:cast(?MODULE, {del_exclusion, {Module, Index}}).

get_exclusions(Module) ->
    gen_server:call(?MODULE, {get_exclusions, Module}, infinity).

get_handoff_lock(LockId) ->
    TokenCount = app_helper:get_env(riak_core, handoff_concurrency, 4),
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
handle_call(all_handoffs, _From, State=#state{handoffs=Hoffs}) ->
    {reply, {ok, Hoffs}, State};
handle_call({get_handoffs, Idx}, _From, State=#state{handoffs=Hoffs}) ->
    Filtered=[{Mod,TargetNode} || {{Mod,I},TargetNode} <- Hoffs, I == Idx],
    {reply, {ok, Filtered}, State}.

handle_cast({add_handoff, {Mod, Idx, Node}}, State=#state{handoffs=Hoffs}) ->
    NewHoffs=[{{Mod,Idx},Node}|Hoffs],
    {noreply, State#state{handoffs=NewHoffs}};
handle_cast({remove_handoff,Hoff={_Mod,_Idx}}, State=#state{handoffs=Hoffs}) ->
    NewHoffs=lists:keydelete(Hoff,1,Hoffs),
    {noreply, State#state{handoffs=NewHoffs}};
handle_cast({del_exclusion, {Mod, Idx}}, State=#state{excl=Excl}) ->
    {noreply, State#state{excl=ordsets:del_element({Mod, Idx}, Excl)}};
handle_cast({add_exclusion, {Mod, Idx}}, State=#state{excl=Excl}) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    case riak_core_ring:my_indices(Ring) of
        [] ->
            %% Trigger a ring update to ensure the node shuts down
            riak_core_ring_events:ring_update(Ring);
        _ ->
            ok
    end,
    {noreply, State#state{excl=ordsets:add_element({Mod, Idx}, Excl)}}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

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
    add_handoff(riak_kv_vnode,0,'node@nohost'),
    ?assertEqual({ok,[{{riak_kv_vnode,0},'node@nohost'}]},all_handoffs()),
    ?assertEqual({ok,[{riak_kv_vnode,'node@nohost'}]},get_handoffs(0)),

    %% remove the handoff and make sure it's gone
    remove_handoff(riak_kv_vnode,0),
    ?assertEqual({ok,[]},all_handoffs()),
    ?assertEqual({ok,[]},get_handoffs(0)),

    %% done
    ok.

-endif.
