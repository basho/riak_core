%% -------------------------------------------------------------------
%%
%% riak_handoff_receiver: incoming data handler for TCP-based handoff
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

%% @doc incoming data handler for TCP-based handoff

-module(riak_core_handoff_receiver).
-include("riak_core_handoff.hrl").
-behaviour(gen_server2).
-export([start_link/0,                          % Don't use SSL
         start_link/1,                          % SSL options list, empty=no SSL
         set_socket/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {sock :: port(), 
                ssl_opts :: [] | list(),
                tcp_mod :: atom(),
                partition :: non_neg_integer(), 
                vnode_mod = riak_kv_vnode:: module(),
                vnode :: pid(), 
                count = 0 :: non_neg_integer()}).


start_link() ->
    start_link([]).

start_link(SslOpts) ->
    gen_server2:start_link(?MODULE, [SslOpts], []).

set_socket(Pid, Socket) ->
    gen_server2:call(Pid, {set_socket, Socket}).

init([SslOpts]) -> 
    {ok, #state{ssl_opts = SslOpts,
                tcp_mod  = if SslOpts /= [] -> ssl;
                              true          -> gen_tcp
                           end}}.

handle_call({set_socket, Socket0}, _From, State = #state{ssl_opts = SslOpts}) ->
    SockOpts = [{active, once}, {packet, 4}, {header, 1}],
    Socket = if SslOpts /= [] ->
                     {ok, Skt} = ssl:ssl_accept(Socket0, SslOpts, 30*1000),
                     ok = ssl:setopts(Skt, SockOpts),
                     Skt;
                true ->
                     ok = inet:setopts(Socket0, SockOpts),
                     Socket0
             end,
    {reply, ok, State#state { sock = Socket }}.

handle_info({tcp_closed,_Socket},State=#state{partition=Partition,count=Count}) ->
    lager:info("Handoff receiver for partition ~p exited after processing ~p"
                          " objects", [Partition, Count]),
    {stop, normal, State};
handle_info({tcp_error, _Socket, _Reason}, State=#state{partition=Partition,count=Count}) ->
    lager:info("Handoff receiver for partition ~p exited after processing ~p"
                          " objects", [Partition, Count]),
    {stop, normal, State};
handle_info({tcp, Socket, Data}, State) ->
    [MsgType|MsgData] = Data,
    case catch(process_message(MsgType, MsgData, State)) of
        {'EXIT', Reason} ->
            lager:error("Handoff receiver for partition ~p exited abnormally after "
                                   "processing ~p objects: ~p", [State#state.partition, State#state.count, Reason]),
            {stop, normal, State};
        NewState when is_record(NewState, state) ->
            InetMod = if NewState#state.ssl_opts /= [] -> ssl;
                         true                          -> inet
                      end,
            InetMod:setopts(Socket, [{active, once}]),
            {noreply, NewState}
    end;
handle_info({ssl_closed, Socket}, State) ->
    handle_info({tcp_closed, Socket}, State);
handle_info({ssl_error, Socket, Reason}, State) ->
    handle_info({tcp_error, Socket, Reason}, State);
handle_info({ssl, Socket, Data}, State) ->
    handle_info({tcp, Socket, Data}, State).

process_message(?PT_MSG_INIT, MsgData, State=#state{vnode_mod=VNodeMod}) ->
    <<Partition:160/integer>> = MsgData,
    lager:info("Receiving handoff data for partition ~p:~p", [VNodeMod, Partition]),
    {ok, VNode} = riak_core_vnode_master:get_vnode_pid(Partition, VNodeMod),
    State#state{partition=Partition, vnode=VNode};
process_message(?PT_MSG_OBJ, MsgData, State=#state{vnode=VNode, count=Count}) ->
    Msg = {handoff_data, MsgData},
    case gen_fsm:sync_send_all_state_event(VNode, Msg, 60000) of
        ok ->
            State#state{count=Count+1};
        E={error, _} ->
            exit(E)
    end;
process_message(?PT_MSG_OLDSYNC, MsgData, State=#state{sock=Socket,
                                                       tcp_mod=TcpMod}) ->
    TcpMod:send(Socket, <<?PT_MSG_OLDSYNC:8,"sync">>),
    <<VNodeModBin/binary>> = MsgData,
    VNodeMod = binary_to_atom(VNodeModBin, utf8),
    State#state{vnode_mod=VNodeMod};
process_message(?PT_MSG_SYNC, _MsgData, State=#state{sock=Socket,
                                                     tcp_mod=TcpMod}) ->
    TcpMod:send(Socket, <<?PT_MSG_SYNC:8, "sync">>),
    State;
process_message(?PT_MSG_CONFIGURE, MsgData, State) ->
    ConfProps = binary_to_term(MsgData),
    State#state{vnode_mod=proplists:get_value(vnode_mod, ConfProps),
                partition=proplists:get_value(partition, ConfProps)};
process_message(_, _MsgData, State=#state{sock=Socket,
                                          tcp_mod=TcpMod}) ->
    TcpMod:send(Socket, <<255:8,"unknown_msg">>),
    State.

handle_cast(_Msg, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

