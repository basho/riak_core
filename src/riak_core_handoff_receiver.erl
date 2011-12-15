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
-behaviour(gen_fsm).

%% API
-export([start_link/0,                          % Don't use SSL
         start_link/1,                          % SSL options list, empty=no SSL
         set_socket/2]).

%% States
-export([wait_for_socket/2,
         handshake/2,
         negotiate/2,
         receiving/2,
         finalize/2]).

%% Callbacks
-export([init/1,
         handle_info/3,
         terminate/3,
         code_change/4]).

-record(ctx, {sock :: port(),
              ssl_opts :: [] | list(),
              tcp_mod :: atom(),
              partition :: non_neg_integer(),
              vnode_mod = riak_kv_vnode:: module(),
              vnode :: pid(),
              count = 0 :: non_neg_integer(),
              proto_vsn :: pos_integer()}).

%% -------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

start_link() ->
    start_link([]).

start_link(SslOpts) ->
    gen_fsm:start_link(?MODULE, [SslOpts], []).

set_socket(Recv, Sock) ->
    gen_fsm:send_event(Recv, {set_socket, Sock}),
    ok.

%% -------------------------------------------------------------------
%% States
%% -------------------------------------------------------------------

wait_for_socket({sock, Sock}, Ctx=#ctx{ssl_opts=SSLOpts}) ->
    SockOpts = [{active, once}, {packet, 4}, {header, 1}],
    Sock2 = if SSLOpts /= [] ->
                    {ok, Skt} = ssl:ssl_accept(Sock, SSLOpts, 30*1000),
                    ok = ssl:setopts(Skt, SockOpts),
                    Skt;
               true ->
                    ok = inet:setopts(Sock, SockOpts),
                    Sock
            end,
    {next_state, handshake, Ctx#ctx{sock=Sock2}}.

handshake([?PT_MSG_VSN|Vsn], Ctx=#ctx{sock=Sock, tcp_mod=TcpMod}) ->
    Data = term_to_binary(?PROTO_VSN),
    TcpMod:send(Sock, <<?PT_MSG_VSN:8,Data/binary>>),
    Ctx2 = Ctx#ctx{proto_vsn=Vsn},
    {next_state, negotiate, Ctx2, 0};

handshake([?PT_MSG_OLDSYNC|VNodeModBin], Ctx=#ctx{sock=Sock, tcp_mod=TcpMod}) ->
    TcpMod:send(Sock, <<?PT_MSG_OLDSYNC:8,"sync">>),
    VNodeMod = binary_to_atom(VNodeModBin, utf8),
    %% talking to legacy sender, wait for partition info
    {next_state, handshake, Ctx#ctx{vnode_mod=VNodeMod}};

handshake([?PT_MSG_INIT|<<Partition:160/integer>>],
          Ctx=#ctx{vnode_mod=VNodeMod}) ->
    lager:info("receiving handoff data for partition ~p:~p",
               [VNodeMod, Partition]),
    {ok, VNode} = riak_core_vnode_manager:get_vnode_pid(Partition, VNodeMod),
    Ctx2 = Ctx#ctx{partition=Partition, vnode=VNode},
    {next_state, receiving, Ctx2}.

negotiate([?PT_MSG_CONFIGURE|Cfg], Ctx=#ctx{sock=Sock,
                                            tcp_mod=TcpMod}) ->
    Cfg2 = binary_to_term(Cfg),
    Mod = proplists:get_value(vnode_mod, Cfg2),
    Partition = proplists:get_value(partition, Cfg2),
    {ok, VNode} = riak_core_vnode_manager:get_vnode_pid(Partition, Mod),
    %% TODO The reply is empty but in future might be used for
    %% negotiation between sender/receiver.
    Reply = term_to_binary([]),
    ok = TcpMod:send(Sock, <<?PT_MSG_CONFIGURE:8, Reply/binary>>),
    ok = TcpMod:send(Sock, <<?PT_MSG_START:8>>),
    Ctx2 = Ctx#ctx{vnode_mod=Mod, partition=Partition, vnode=VNode},
    {next_state, receiving, Ctx2}.

receiving([?PT_MSG_OBJ|Data], Ctx=#ctx{vnode=VNode, count=Count}) ->
    Msg = {handoff_data, Data},
    case gen_fsm:sync_send_all_state_event(VNode, Msg, 60000) of
        ok -> {next_state, receiving, Ctx#ctx{count=Count+1}};
        {error, Reason} ->
            %% TODO Aborting entire process for failure to process one
            %% msg is extreme.  In future we should be smarting about
            %% retrying msgs.
            lager:error("error while processing msg ~p", [Reason]),
            {stop, {error_processing_msg, Reason}, Ctx}
    end;

receiving([?PT_MSG_OLDSYNC|<<"sync">>], Ctx=#ctx{sock=Sock,
                                                 tcp_mod=TcpMod}) ->
    %% talking to legacy sender
    TcpMod:send(Sock, <<?PT_MSG_OLDSYNC:8,"sync">>),
    {next_state, receiving, Ctx};

receiving([?PT_MSG_SYNC|<<>>], Ctx=#ctx{sock=Sock,
                                        tcp_mod=TcpMod,
                                        proto_vsn=Vsn}) ->
    case Vsn of
        1 ->
            TcpMod:send(Sock, <<?PT_MSG_SYNC:8, "sync">>),
            {next_state, finalize, Ctx, 0};
        2 ->
            TcpMod:send(Sock, <<?PT_MSG_SYNC:8, "sync">>),
            {next_state, receiving, Ctx}
    end;

receiving([?PT_MSG_FINALIZE|<<>>], Ctx) ->
    {next_state, finalize, Ctx, 0}.

finalize(timeout, Ctx=#ctx{count=Count,
                           vnode_mod=VNodeMod,
                           partition=Partition,
                           tcp_mod=TcpMod,
                           sock=Sock,
                           proto_vsn=Vsn}) ->
    lager:info("handoff completed for partition ~p ~p after processing ~p msgs",
               [VNodeMod, Partition, Count]),
    case Vsn of
        1 ->
            {stop, normal, Ctx};
        2 ->
            ok = TcpMod:send(Sock, <<?PT_MSG_FINALIZE:8>>),
            {stop, normal, Ctx}
    end.

%% -------------------------------------------------------------------
%% Callbacks
%% -------------------------------------------------------------------

init([SslOpts]) ->
    {ok, wait_for_socket,
     #ctx{ssl_opts = SslOpts,
          tcp_mod  = if SslOpts /= [] -> ssl;
                        true          -> gen_tcp
                     end}}.

handle_info({tcp_closed,_Sock}, _StateName,
            Ctx=#ctx{partition=Partition, count=Count, vnode_mod=VNodeMod}) ->
    lager:error("socket unexpectedly closed by sender for partition"
                "~p ~p after ~p msgs", [VNodeMod, Partition, Count]),
    {stop, {tcp_closed, unexpected_close}, Ctx};

handle_info({tcp_error, _Sock, _Reason}, _StateName,
            Ctx=#ctx{partition=Partition, count=Count, vnode_mod=Mod}) ->
    lager:error("socket error for partition ~p ~p after ~p msgs objects",
                [Mod, Partition, Count]),
    {stop, {tcp_error, socket_error}, Ctx};

handle_info({tcp, Sock, Data}, StateName, Ctx=#ctx{ssl_opts=SSLOpts}) ->
    InetMod = if SSLOpts /= [] -> ssl;
                 true -> inet
              end,
    InetMod:setopts(Sock, [{active, once}]),
    gen_fsm:send_event(self(), Data),
    {next_state, StateName, Ctx};

handle_info({ssl_closed, Sock}, StateName, Ctx) ->
    handle_info({tcp_closed, Sock}, StateName, Ctx);
handle_info({ssl_error, Sock, Reason}, StateName, Ctx) ->
    handle_info({tcp_error, Sock, Reason}, StateName, Ctx);
handle_info({ssl, Sock, Data}, StateName, Ctx) ->
    handle_info({tcp, Sock, Data}, StateName, Ctx).

terminate(_Reason, _StateName, _Ctx) -> ok.

code_change(_OldVsn, _StateName, Ctx, _Extra) -> {ok, Ctx}.
