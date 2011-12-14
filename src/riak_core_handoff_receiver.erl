%% -------------------------------------------------------------------
%%
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
%%
%% @doc Handle incoming handoff data for a partition.

-module(riak_core_handoff_receiver).
-include_lib("riak_core_handoff.hrl").
-behaviour(gen_server).

%% API
-export([start_link/0,                          % Don't use SSL
         start_link/1,                          % SSL options list, empty=no SSL
         set_socket/2]).

%% Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {sock :: port(), 
                ssl_opts :: [] | list(),
                tcp_mod :: atom(),
                partition :: non_neg_integer(), 
                vnode_mod = riak_kv_vnode:: module(),
                vnode :: pid(), 
                count = 0 :: non_neg_integer()}).

%% -------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

start_link() ->
    start_link([]).

start_link(SslOpts) ->
    gen_server:start_link(?MODULE, [SslOpts], []).

set_socket(Pid, Socket) ->
    gen_server:call(Pid, {set_socket, Socket}).

%% -------------------------------------------------------------------
%% Callbacks
%% -------------------------------------------------------------------

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

handle_info({tcp_closed,_Socket},State=#state{partition=Partition,
                                              count=Count,
                                              vnode_mod=Mod}) ->
    lager:error("socket unexpectedly closed by sender for partition"
                "~p ~p after ~p msgs", [Mod, Partition, Count]),
    {stop, {tcp_closed, unexpected_close}, State};

handle_info({tcp_error, _Socket, _Reason}, State=#state{partition=Partition,
                                                        count=Count,
                                                        vnode_mod=Mod}) ->
    lager:error("socket error for partition ~p ~p after ~p msgs objects",
                [Mod, Partition, Count]),
    {stop, {tcp_error, socket_error}, State};

handle_info({tcp, Socket, Data}, State) ->
    [MsgType|MsgData] = Data,
    case catch(process_message(MsgType, MsgData, State)) of
        {'EXIT', Reason} ->
            lager:error("Handoff receiver for partition ~p exited abnormally "
                        "after processing ~p objects: ~p",
                        [State#state.partition, State#state.count, Reason]),
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

handle_cast(complete, State=#state{partition=Partition, count=Count,
                                   vnode_mod=Mod}) ->
    lager:info("handoff completed for partition ~p ~p after processing ~p msgs",
               [Mod, Partition, Count]),
    {stop, normal, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% -------------------------------------------------------------------
%% Private
%% -------------------------------------------------------------------

process_message(?PT_MSG_INIT, MsgData, State=#state{vnode_mod=VNodeMod}) ->
    <<Partition:160/integer>> = MsgData,
    lager:info("Receiving handoff data for partition ~p:~p",
               [VNodeMod, Partition]),
    {ok, VNode} = riak_core_vnode_manager:get_vnode_pid(Partition, VNodeMod),
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
process_message(?PT_MSG_CONFIGURE, MsgData, State=#state{sock=Socket,
                                                         tcp_mod=TcpMod}) ->
    ConfProps = binary_to_term(MsgData),
    Mod = proplists:get_value(vnode_mod, ConfProps),
    Partition = proplists:get_value(partition, ConfProps),
    {ok, VNode} = riak_core_vnode_manager:get_vnode_pid(Partition, Mod),
    %% TODO The reply is empty but in future might be used for
    %% negotiation between sender/receiver.
    Reply = term_to_binary([]),
    ok = TcpMod:send(Socket, <<?PT_MSG_CONFIGURE, Reply/binary>>),
    State#state{vnode_mod=Mod, partition=Partition, vnode=VNode};

process_message(?PT_MSG_COMPLETE, _MsgData, State=#state{sock=Socket,
                                                         tcp_mod=TcpMod}) ->
    ok = TcpMod:send(Socket, <<?PT_MSG_COMPLETE:8>>),
    gen_server:cast(self(), complete),
    State;

process_message(?PT_MSG_VSN, _MsgData, State=#state{sock=Socket,
                                                    tcp_mod=TcpMod}) ->
    Data = term_to_binary(?PROTO_VSN),
    TcpMod:send(Socket, <<?PT_MSG_VSN:8,Data/binary>>),
    State;
process_message(_, _MsgData, State=#state{sock=Socket,
                                          tcp_mod=TcpMod}) ->
    TcpMod:send(Socket, <<?PT_MSG_UNKNOWN:8,"unknown_msg">>),
    State.
