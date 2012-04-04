%% -------------------------------------------------------------------
%%
%% riak_vnode_master: dispatch to vnodes
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

%% @doc dispatch to vnodes

-module(riak_core_vnode_master).
-include("riak_core_vnode.hrl").
-behaviour(gen_server).
-export([start_link/1, start_link/2, start_link/3, get_vnode_pid/2,
         start_vnode/2, command/3, command/4, sync_command/3,
         coverage/5,
         command_return_vnode/4,
         sync_command/4,
         sync_spawn_command/3, make_request/3,
         make_coverage_request/4, all_nodes/1, reg_name/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).
-record(state, {idxtab, sup_name, vnode_mod, legacy}).

-define(DEFAULT_TIMEOUT, 5000).

make_name(VNodeMod,Suffix) -> list_to_atom(atom_to_list(VNodeMod)++Suffix).
reg_name(VNodeMod) ->  make_name(VNodeMod, "_master").

%% Given atom 'riak_kv_vnode_master', return 'riak_kv_vnode'.
vmaster_to_vmod(VMaster) ->
    L = atom_to_list(VMaster),
    list_to_atom(lists:sublist(L,length(L)-7)).

start_link(VNodeMod) ->
    start_link(VNodeMod, undefined).

start_link(VNodeMod, LegacyMod) ->
    start_link(VNodeMod, LegacyMod, undefined).

start_link(VNodeMod, LegacyMod, Service) ->
    RegName = reg_name(VNodeMod),
    gen_server:start_link({local, RegName}, ?MODULE,
                          [Service,VNodeMod,LegacyMod,RegName], []).

start_vnode(Index, VNodeMod) ->
    riak_core_vnode_manager:start_vnode(Index, VNodeMod).

get_vnode_pid(Index, VNodeMod) ->
    riak_core_vnode_manager:get_vnode_pid(Index, VNodeMod).

command(Preflist, Msg, VMaster) ->
    command(Preflist, Msg, ignore, VMaster).

%% Send the command to the preflist given with responses going to Sender
command([], _Msg, _Sender, _VMaster) ->
    ok;
command([{Index, Pid}|Rest], Msg, Sender, VMaster) when is_pid(Pid) ->
    gen_fsm:send_event(Pid, make_request(Msg, Sender, Index)),
    command(Rest, Msg, Sender, VMaster);
command([{Index,Node}|Rest], Msg, Sender, VMaster) ->
    proxy_cast({VMaster, Node}, make_request(Msg, Sender, Index)),
    command(Rest, Msg, Sender, VMaster);

%% Send the command to an individual Index/Node combination
command({Index, Pid}, Msg, Sender, _VMaster) when is_pid(Pid) ->
    gen_fsm:send_event(Pid, make_request(Msg, Sender, Index));
command({Index,Node}, Msg, Sender, VMaster) ->
    proxy_cast({VMaster, Node}, make_request(Msg, Sender, Index)).

%% Send a command to a covering set of vnodes
coverage(Msg, CoverageVNodes, Keyspaces, {Type, Ref, From}, VMaster)
  when is_list(CoverageVNodes) ->
    [proxy_cast({VMaster, Node},
                make_coverage_request(Msg,
                                      Keyspaces, 
                                      {Type, {Ref, {Index, Node}}, From},
                                      Index)) ||
        {Index, Node} <- CoverageVNodes];
coverage(Msg, {Index, Node}, Keyspaces, Sender, VMaster) ->
    proxy_cast({VMaster, Node},
               make_coverage_request(Msg, Keyspaces, Sender, Index)).
    
%% Send the command to an individual Index/Node combination, but also
%% return the pid for the vnode handling the request, as `{ok,
%% VnodePid}'.
command_return_vnode({Index,Node}, Msg, Sender, VMaster) ->
    Req = make_request(Msg, Sender, Index),
    case app_helper:get_env(riak_core, legacy_vnode_routing, true) of
        true ->
            gen_server:call({VMaster, Node}, {return_vnode, Req});
        false ->
            Mod = vmaster_to_vmod(VMaster),
            riak_core_vnode_proxy:command_return_vnode({Mod,Index,Node}, Req)
    end.

%% Send a synchronous command to an individual Index/Node combination.
%% Will not return until the vnode has returned
sync_command(IndexNode, Msg, VMaster) ->
    sync_command(IndexNode, Msg, VMaster, ?DEFAULT_TIMEOUT).

sync_command({Index,Node}, Msg, VMaster, Timeout) ->
    %% Issue the call to the master, it will update the Sender with
    %% the From for handle_call so that the {reply} return gets
    %% sent here.
    gen_server:call({VMaster, Node},
                    make_request(Msg, {server, undefined, undefined}, Index), Timeout).

%% Send a synchronous spawned command to an individual Index/Node combination.
%% Will not return until the vnode has returned, but the vnode_master will
%% continue to handle requests.
sync_spawn_command({Index,Node}, Msg, VMaster) ->
    gen_server:call({VMaster, Node},
                    {spawn, make_request(Msg, {server, undefined, undefined}, Index)},
                    infinity).


%% Make a request record - exported for use by legacy modules
-spec make_request(vnode_req(), sender(), partition()) -> #riak_vnode_req_v1{}.
make_request(Request, Sender, Index) ->
    #riak_vnode_req_v1{
              index=Index,
              sender=Sender,
              request=Request}.

%% Make a request record - exported for use by legacy modules
-spec make_coverage_request(vnode_req(), [{partition(), [partition()]}], sender(), partition()) -> #riak_coverage_req_v1{}.
make_coverage_request(Request, KeySpaces, Sender, Index) ->
    #riak_coverage_req_v1{index=Index,
                          keyspaces=KeySpaces,
                          sender=Sender,
                          request=Request}.

%% Request a list of Pids for all vnodes
%% @deprecated
%% Provided for compatibility with older vnode master API. New code should
%% use riak_core_vnode_manager:all_vnode/1 which returns a mod/index/pid
%% list rather than just a pid list.
all_nodes(VNodeMod) ->
    VNodes = riak_core_vnode_manager:all_vnodes(VNodeMod),
    [Pid || {_Mod, _Idx, Pid} <- VNodes].

%% @private
init([Service, VNodeMod, LegacyMod, _RegName]) ->
    gen_server:cast(self(), {wait_for_service, Service}),
    {ok, #state{idxtab=undefined,
                vnode_mod=VNodeMod,
                legacy=LegacyMod}}.

proxy_cast({VMaster, Node}, Req) ->
    case app_helper:get_env(riak_core, legacy_vnode_routing, true) of
        true ->
            gen_server:cast({VMaster, Node}, Req);
        false ->
            do_proxy_cast({VMaster, Node}, Req)
    end.

do_proxy_cast({VMaster, Node}, Req=?VNODE_REQ{index=Idx}) ->
    Mod = vmaster_to_vmod(VMaster),
    Proxy = riak_core_vnode_proxy:reg_name(Mod, Idx, Node),
    gen_fsm:send_event(Proxy, Req),
    ok;
do_proxy_cast({VMaster, Node}, Req=?COVERAGE_REQ{index=Idx}) ->
    Mod = vmaster_to_vmod(VMaster),
    Proxy = riak_core_vnode_proxy:reg_name(Mod, Idx, Node),
    gen_fsm:send_event(Proxy, Req),
    ok;
do_proxy_cast({VMaster, Node}, Other) ->
    gen_server:cast({VMaster, Node}, Other).

handle_cast({wait_for_service, Service}, State) ->
    case Service of
        undefined ->
            ok;
        _ ->
            lager:debug("Waiting for service: ~p", [Service]),
            riak_core:wait_for_service(Service)
    end,
    {noreply, State};
handle_cast(Req=?VNODE_REQ{index=Idx}, State=#state{vnode_mod=Mod}) ->
    Proxy = riak_core_vnode_proxy:reg_name(Mod, Idx),
    gen_fsm:send_event(Proxy, Req),
    {noreply, State};
handle_cast(Req=?COVERAGE_REQ{index=Idx}, State=#state{vnode_mod=Mod}) ->
    Proxy = riak_core_vnode_proxy:reg_name(Mod, Idx),
    gen_fsm:send_event(Proxy, Req),
    {noreply, State};
handle_cast(Other, State=#state{legacy=Legacy}) when Legacy =/= undefined ->
    case catch Legacy:rewrite_cast(Other) of
        {ok, ?VNODE_REQ{}=Req} ->
            handle_cast(Req, State);
        _ ->
            {noreply, State}
    end.

handle_call({return_vnode, Req=?VNODE_REQ{index=Idx}}, _From,
            State=#state{vnode_mod=Mod}) ->
    {ok, Pid} =
        riak_core_vnode_proxy:command_return_vnode({Mod,Idx,node()}, Req),
    {reply, {ok, Pid}, State};
handle_call(Req=?VNODE_REQ{index=Idx, sender={server, undefined, undefined}},
            From, State=#state{vnode_mod=Mod}) ->
    Proxy = riak_core_vnode_proxy:reg_name(Mod, Idx),
    gen_fsm:send_event(Proxy, Req?VNODE_REQ{sender={server, undefined, From}}),
    {noreply, State};
handle_call({spawn,
             Req=?VNODE_REQ{index=Idx, sender={server, undefined, undefined}}},
            From, State=#state{vnode_mod=Mod}) ->
    Proxy = riak_core_vnode_proxy:reg_name(Mod, Idx),
    Sender = {server, undefined, From},
    spawn_link(
      fun() -> gen_fsm:send_all_state_event(Proxy, Req?VNODE_REQ{sender=Sender}) end),
    {noreply, State};
handle_call(Other, From, State=#state{legacy=Legacy}) when Legacy =/= undefined ->
    case catch Legacy:rewrite_call(Other, From) of
        {ok, ?VNODE_REQ{}=Req} ->
            handle_call(Req, From, State);
        _ ->
            {noreply, State}
    end.

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->  {ok, State}.
