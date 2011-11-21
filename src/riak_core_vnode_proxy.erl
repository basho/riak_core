%% -------------------------------------------------------------------
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
-module(riak_core_vnode_proxy).
-export([start_link/2, init/1, reg_name/2, reg_name/3, call/2, cast/2,
         unregister_vnode/2, command_return_vnode/2]).
-export([system_continue/3, system_terminate/4, system_code_change/4]).

-record(state, {mod, index, vnode_pid, vnode_mref}).

reg_name(Mod, Index) ->
    list_to_atom(lists:flatten(io_lib:format("proxy_~s_~b", [Mod, Index]))).

reg_name(Mod, Index, Node) ->
    {reg_name(Mod, Index), Node}.

start_link(Mod, Index) ->
    RegName = reg_name(Mod, Index),
    proc_lib:start_link(?MODULE, init, [[self(), RegName, Mod, Index]]).

init([Parent, RegName, Mod, Index]) ->
    erlang:register(RegName, self()),
    proc_lib:init_ack(Parent, {ok, self()}),
    State = #state{mod=Mod, index=Index},
    loop(Parent, State).

unregister_vnode(Mod, Index) ->
    call(reg_name(Mod, Index), unregister_vnode).

command_return_vnode({Mod,Index,Node}, Req) ->
    call(reg_name(Mod, Index, Node), {return_vnode, Req}).

call(Name, Msg) ->
    {ok,Res} = (catch gen:call(Name, '$vnode_proxy_call', Msg)),
    Res.

cast(Name, Msg) ->
    catch erlang:send(Name, {'$vnode_proxy_cast', Msg}),
    ok.

system_continue(Parent, _, State) ->
    loop(Parent, State).

system_terminate(Reason, _Parent, _, _State) ->
    exit(Reason).

system_code_change(State, _, _, _) ->
    {ok, State}.

%% @private
loop(Parent, State) ->
    receive
        {'$vnode_proxy_call', From, Msg} ->
            {reply, Reply, NewState} = handle_call(Msg, From, State),
            gen:reply(From, Reply),
            loop(Parent, NewState);
        {'$vnode_proxy_cast', Msg} ->
            {noreply, NewState} = handle_cast(Msg, State),
            loop(Parent, NewState);
        {'DOWN', _Mref, process, _Pid, _} ->
            NewState = State#state{vnode_pid=undefined, vnode_mref=undefined},
            loop(Parent, NewState);
        {system, From, Msg} ->
            sys:handle_system_msg(Msg, From, Parent, ?MODULE, [], State);
        Msg ->
            {noreply, NewState} = handle_proxy(Msg, State),
            loop(Parent, NewState)
    end.

%% @private
handle_call(unregister_vnode, _From, State) ->
    catch demonitor(State#state.vnode_mref, [flush]),
    NewState = State#state{vnode_pid=undefined, vnode_mref=undefined},
    {reply, ok, NewState};
handle_call({return_vnode, Req}, _From, State) ->
    {Pid, NewState} = get_vnode_pid(State),
    gen_fsm:send_event(Pid, Req),
    {reply, {ok, Pid}, NewState};

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_proxy(Msg, State) ->
    {Pid, NewState} = get_vnode_pid(State),
    Pid ! Msg,
    {noreply, NewState}.

%% @private
get_vnode_pid(State=#state{mod=Mod, index=Index, vnode_pid=undefined}) ->
    {ok, Pid} = riak_core_vnode_manager:get_vnode_pid(Index, Mod),
    Mref = erlang:monitor(process, Pid),
    NewState = State#state{vnode_pid=Pid, vnode_mref=Mref},
    {Pid, NewState};
get_vnode_pid(State=#state{vnode_pid=Pid}) ->
    {Pid, State}.
