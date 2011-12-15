%% -------------------------------------------------------------------
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
%%
%% @doc Handoff partition data.

-module(riak_core_handoff_sender).
-behavior(gen_fsm).

%% API
-export([get_handoff_ssl_options/0,
         start_link/4]).

%% States
-export([handshake/2,
         negotiate/2,
         sending/2,
         finalize/2]).

%% Callbacks
-export([init/1,
         %% handle_event/3,
         %% handle_sync_event/4,
         handle_info/3,
         code_change/4,
         terminate/3]).

-include_lib("riak_core_vnode.hrl").
-include_lib("riak_core_handoff.hrl").
-define(ACK_COUNT, 1000).
-record(ctx, {target,
              vnode_mod,
              partition,
              vnode,
              proto_vsn,
              sock,
              tcp_mod,
              inet_mod,
              ssl_opts}).

%% -------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

get_handoff_ssl_options() ->
    case app_helper:get_env(riak_core, handoff_ssl_options, []) of
        [] ->
            [];
        Props ->
            try
                %% We'll check if the file(s) exist but won't check
                %% file contents' sanity.
                ZZ = [{_, {ok, _}} = {ToCheck, file:read_file(Path)} ||
                         ToCheck <- [certfile, keyfile, cacertfile, dhfile],
                         Path <- [proplists:get_value(ToCheck, Props)],
                         Path /= undefined],
                spawn(fun() -> self() ! ZZ end), % Avoid term...never used err
                %% Props are OK
                Props
            catch
                error:{badmatch, {FailProp, BadMat}} ->
                    lager:error("SSL handoff config error: property ~p: ~p.",
                                [FailProp, BadMat]),
                    [];
                X:Y ->
                    lager:error("failure processing SSL handoff config "
                                "~p: ~p:~p",
                                [Props, X, Y]),
                    []
            end
    end.

start_link(Target, Mod, Partition, VNode) ->
    SSLOpts = get_handoff_ssl_options(),
    gen_fsm:start_link(?MODULE, [Target, Mod, Partition, VNode, SSLOpts], []).

%% -------------------------------------------------------------------
%% States
%% -------------------------------------------------------------------

handshake(timeout, Ctx=#ctx{target=Target,
                            vnode_mod=VNodeMod,
                            partition=Partition,
                            ssl_opts=SSLOpts}) ->
    lager:info("starting handoff of partition ~p ~p from ~p to ~p",
               [VNodeMod, Partition, node(), Target]),
    [_Name,Host] = string:tokens(atom_to_list(Target), "@"),
    {ok, Port} = get_handoff_port(Target),
    SockOpts = [binary, {packet, 4}, {header,1}, {active, once}],
    {Sock, TcpMod, InetMod} =
        if SSLOpts /= [] ->
                {ok, Skt} = ssl:connect(Host, Port, SSLOpts ++ SockOpts,
                                        15000),
                {Skt, ssl, ssl};
           true ->
                {ok, Skt} = gen_tcp:connect(Host, Port, SockOpts, 15000),
                {Skt, gen_tcp, inet}
        end,
    ok = TcpMod:send(Sock, <<?PT_MSG_VSN:8>>),
    Ctx2 = Ctx#ctx{inet_mod=InetMod, tcp_mod=TcpMod, sock=Sock},
    {next_state, handshake, Ctx2};

handshake([?PT_MSG_VSN|Vsn], Ctx) ->
    Ctx2 = Ctx#ctx{proto_vsn=binary_to_term(Vsn)},
    {next_state, negotiate, Ctx2, 0};

handshake([?PT_MSG_UNKNOWN|<<"unknown_msg">>], Ctx) ->
    %% legacy handshake
    Ctx2 = Ctx#ctx{proto_vsn=1},
    {next_state, negotiate, Ctx2, 0}.

negotiate(timeout, Ctx=#ctx{proto_vsn=Vsn,
                            vnode_mod=VNodeMod,
                            partition=Partition,
                            sock=Sock,
                            tcp_mod=TcpMod}) ->
    Msg =
        case Vsn of
            1 ->
                %% legacy negotiate
                Data = atom_to_binary(VNodeMod, utf8),
                <<?PT_MSG_OLDSYNC:8,Data/binary>>;
            2 ->
                Cfg = [{vnode_mod, VNodeMod},
                       {partition, Partition}],
                Data = term_to_binary(Cfg),
                <<?PT_MSG_CONFIGURE:8,Data/binary>>
        end,
    ok = TcpMod:send(Sock, Msg),
    {next_state, negotiate, Ctx};

negotiate([?PT_MSG_CONFIGURE|_Cfg], Ctx) ->
    %% TODO Currently there is nothing for sender/recv to negotiate
    %% but leave this here so that they may in future release.
    {next_state, negotiate, Ctx};

negotiate([?PT_MSG_OLDSYNC|<<"sync">>],
          Ctx=#ctx{partition=Partition,
                   sock=Sock,
                   tcp_mod=TcpMod}) ->
    %% legacy negotiation
    Msg = <<?PT_MSG_INIT:8,Partition:160/integer>>,
    ok = TcpMod:send(Sock, Msg),
    {next_state, negotiate, Ctx};

negotiate([?PT_MSG_START|<<>>], Ctx) ->
    %% TODO Verify ctx?
    {next_state, sending, Ctx, 0}.

sending(timeout, Ctx=#ctx{vnode_mod=VNodeMod,
                          vnode=VNode,
                          partition=Partition,
                          sock=Sock,
                          tcp_mod=TcpMod,
                          target=Target}) ->
    start_fold(Target, VNodeMod, Partition, VNode, TcpMod, Sock),
    {next_state, finalize, Ctx, 0}.

finalize(timeout, Ctx=#ctx{proto_vsn=Vsn,
                           sock=Sock,
                           tcp_mod=TcpMod}) ->
    case Vsn of
        1 ->
            %% Send last sync to verify that all data has been written.  The
            %% contract assumed here is that the receiver cannot respond to
            %% this sync until it has handled all previous msgs.
            ok = TcpMod:send(Sock, <<?PT_MSG_SYNC:8>>);
        2 ->
            ok = TcpMod:send(Sock, <<?PT_MSG_FINALIZE:8>>)
    end,
    {next_state, finalize, Ctx};

finalize([?PT_MSG_SYNC|<<"sync">>], Ctx) ->
    {stop, normal, Ctx};

finalize([?PT_MSG_FINALIZE|<<>>], Ctx) ->
    {stop, normal, Ctx}.

%% -------------------------------------------------------------------
%% Callbacks
%% -------------------------------------------------------------------

init([Target, VNodeMod, Partition, VNode, SSLOpts]) ->
    Ctx = #ctx{target=Target,
               vnode_mod=VNodeMod,
               partition=Partition,
               vnode=VNode,
               ssl_opts=SSLOpts},
    {ok, handshake, Ctx, 0}.


handle_info({tcp, Sock, Data}, StateName, Ctx=#ctx{inet_mod=InetMod}) ->
    InetMod:setopts(Sock, [{active, once}]),
    gen_fsm:send_event(self(), Data),
    {next_state, StateName, Ctx};

handle_info({Err=tcp_error, _Sock, Reason},
            _StateName,
            Ctx=#ctx{target=Target, vnode_mod=VNodeMod, partition=Partition,
                     vnode=VNode}) ->
    lager:error("socket error for partition ~p ~p transferring "
                "from ~p to ~p failed ~p:~p",
                [VNodeMod, Partition, node(), Target, Err, Reason]),
    gen_fsm:send_event(VNode, {handoff_error, Err, Reason}),
    {stop, {Err, Reason}, Ctx};

handle_info({tcp_closed, _Sock},
            _StateName,
            Ctx=#ctx{target=Target, vnode_mod=VNodeMod, partition=Partition,
                     vnode=VNode}) ->
    lager:error("socket unexpectedly closed by receiver for partition ~p ~p "
                "from ~p to ~p ",
                [VNodeMod, Partition, node(), Target]),
    gen_fsm:send_event(VNode, {handoff_error, tcp_closed, unexpected_close}),
    {stop, {tcp_closed, unexpected_close}, Ctx};

handle_info(Req, _StateName, Ctx) ->
    lager:error("unexpected info ~p", [Req]),
    {noreply, Ctx}.

terminate(_Reason, _StateName, _Ctx) -> ignore.

code_change(_OldVsn, _StateName, Ctx, _Extra) -> {ok, Ctx}.

%% -------------------------------------------------------------------
%% Private
%% -------------------------------------------------------------------

get_handoff_port(Node) when is_atom(Node) ->
    case catch(gen_server2:call({riak_core_handoff_listener, Node}, handoff_port, infinity)) of
        {'EXIT', _}  ->
            %% Check old location from previous release
            gen_server2:call({riak_kv_handoff_listener, Node}, handoff_port, infinity);
        Other -> Other
    end.

start_fold(Target, Mod, Partition, VNode, TcpMod, Sock) ->
     try
         VMaster = list_to_atom(atom_to_list(Mod) ++ "_master"),

         StartFoldTime = now(),
         {Sock,VNode,Mod,TcpMod,_Ack,SentCount,ErrStatus} =
             riak_core_vnode_master:sync_command({Partition, node()},
                                                 ?FOLD_REQ{
                                                    foldfun=fun visit_item/3,
                                                    acc0={Sock,VNode,Mod,TcpMod,0,0,ok}},
                                                 VMaster, infinity),
         EndFoldTime = now(),
         FoldTimeDiff = timer:now_diff(EndFoldTime, StartFoldTime) / 1000000,
         case ErrStatus of
             ok ->
                 lager:info("handoff of partition ~p ~p from ~p to ~p "
                            "completed: sent ~p objects in ~.2f seconds",
                            [Mod, Partition, node(), Target, SentCount,
                             FoldTimeDiff]),
                 gen_fsm:send_event(VNode, handoff_complete);
             {error, ErrReason} ->
                 lager:error("handoff of partition ~p ~p from ~p to ~p "
                             "FAILED after sending ~p objects in ~.2f "
                             "seconds: ~p",
                             [Mod, Partition, node(), Target,
                              SentCount, FoldTimeDiff, ErrReason]),
                 gen_fsm:send_event(VNode, {handoff_error,
                                                fold_error, ErrReason})
         end
     catch
         Err:Reason ->
             Trace = erlang:get_stacktrace(),
             lager:error("handoff of partition ~p ~p from ~p to ~p "
                         "failed ~p:~p ~p",
                         [Mod, Partition, node(), Target, Err, Reason, Trace]),
             gen_fsm:send_event(VNode, {handoff_error, Err, Reason})
     end.

%% When a tcp error occurs, the ErrStatus argument is set to {error, Reason}.
%% Since we can't abort the fold, this clause is just a no-op.
visit_item(_K, _V, {Sock, VNode, Mod, TcpMod, Ack, Total,
                    {error, Reason}}) ->
    {Sock, VNode, Mod, TcpMod, Ack, Total, {error, Reason}};
visit_item(K, V, {Sock, VNode, Mod, TcpMod, ?ACK_COUNT, Total, _Err}) ->
    case TcpMod:send(Sock, <<?PT_MSG_SYNC:8>>) of
        ok ->
            case TcpMod:recv(Sock, 0) of
                {ok,[?PT_MSG_SYNC|<<"sync">>]} ->
                    visit_item(K, V, {Sock, VNode, Mod, TcpMod, 0, Total, ok});
                {error, Reason} ->
                    {Sock, VNode, Mod, TcpMod, 0, Total, {error, Reason}}
            end;
        {error, Reason} ->
            {Sock, VNode, Mod, TcpMod, 0, Total, {error, Reason}}
    end;
visit_item(K, V, {Sock, VNode, Mod, TcpMod, Ack, Total, _ErrStatus}) ->
    BinObj = Mod:encode_handoff_item(K, V),
    M = <<?PT_MSG_OBJ:8,BinObj/binary>>,
    case TcpMod:send(Sock, M) of
        ok ->
            {Sock, VNode, Mod, TcpMod, Ack+1, Total+1, ok};
        {error, Reason} ->
            {Sock, VNode, Mod, TcpMod, Ack, Total, {error, Reason}}
    end.
