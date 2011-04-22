%% -------------------------------------------------------------------
%%
%% riak_handoff_sender: send a partition's data via TCP-based handoff
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

%% @doc send a partition's data via TCP-based handoff

-module(riak_core_handoff_sender).
-export([start_link/3, get_handoff_ssl_options/0]).
-include_lib("riak_core_vnode.hrl").
-include_lib("riak_core_handoff.hrl").
-define(ACK_COUNT, 1000).

start_link(TargetNode, Module, Partition) ->
    Self = self(),
    SslOpts = get_handoff_ssl_options(),
    Pid = spawn_link(fun()->start_fold(TargetNode, Module,Partition, Self, SslOpts) end),
    {ok, Pid}.

start_fold(TargetNode, Module, Partition, ParentPid, SslOpts) ->
     try
         error_logger:info_msg("Starting handoff of partition ~p ~p to ~p~n", 
                               [Module, Partition, TargetNode]),
         [_Name,Host] = string:tokens(atom_to_list(TargetNode), "@"),
         {ok, Port} = get_handoff_port(TargetNode),
         SockOpts = [binary, {packet, 4}, {header,1}, {active, false}],
         {Socket, TcpMod} =
             if SslOpts /= [] ->
                     {ok, Skt} = ssl:connect(Host, Port, SslOpts ++ SockOpts,
                                             15000),
                     {Skt, ssl};
                true ->
                     {ok, Skt} = gen_tcp:connect(Host, Port, SockOpts, 15000),
                     {Skt, gen_tcp}
             end,

         %% Piggyback the sync command from previous releases to send
         %% the vnode type across.  If talking to older nodes they'll
         %% just do a sync, newer nodes will decode the module name.
         %% After 0.12.0 the calls can be switched to use PT_MSG_SYNC
         %% and PT_MSG_CONFIGURE
         VMaster = list_to_atom(atom_to_list(Module) ++ "_master"),
         ModBin = atom_to_binary(Module, utf8),
         Msg = <<?PT_MSG_OLDSYNC:8,ModBin/binary>>,
         ok = TcpMod:send(Socket, Msg),
         {ok,[?PT_MSG_OLDSYNC|<<"sync">>]} = TcpMod:recv(Socket, 0),
         M = <<?PT_MSG_INIT:8,Partition:160/integer>>,
         ok = TcpMod:send(Socket, M),
         StartFoldTime = now(),
         {Socket,ParentPid,Module,TcpMod,_Ack,SentCount,ErrStatus} =
             riak_core_vnode_master:sync_command({Partition, node()},
                                                 ?FOLD_REQ{
                                                    foldfun=fun visit_item/3,
                                                    acc0={Socket,ParentPid,Module,TcpMod,0,0,ok}},
                                                 VMaster, infinity),
         EndFoldTime = now(),
         case ErrStatus of
             ok ->
                 error_logger:info_msg("Handoff of partition ~p ~p to ~p "
                                       "completed: sent ~p objects in ~.2f "
                                       "seconds\n", 
                                       [Module, Partition, TargetNode, 
                                        SentCount,
                                        timer:now_diff(
                                          EndFoldTime, 
                                          StartFoldTime) / 1000000]),
                 gen_fsm:send_event(ParentPid, handoff_complete);
             {error, ErrReason} ->
                 error_logger:error_msg("Handoff of partition ~p ~p to ~p "
                                        "FAILED: (~p) after sending ~p objects "
                                        "in ~.2f seconds\n", 
                                        [Module, Partition, TargetNode, 
                                         ErrReason, SentCount,
                                         timer:now_diff(
                                           EndFoldTime, 
                                           StartFoldTime) / 1000000]),
                 gen_fsm:send_event(ParentPid, {handoff_error, 
                                                fold_error, ErrReason})
         end
     catch
         Err:Reason ->
             error_logger:error_msg("Handoff sender ~p ~p failed ~p:~p\n", 
                                    [Module, Partition, Err,Reason]),
             gen_fsm:send_event(ParentPid, {handoff_error, Err, Reason})
     end.

%% When a tcp error occurs, the ErrStatus argument is set to {error, Reason}.
%% Since we can't abort the fold, this clause is just a no-op.
visit_item(_K, _V, {Socket, ParentPid, Module, TcpMod, Ack, Total,
                    {error, Reason}}) ->
    {Socket, ParentPid, Module, TcpMod, Ack, Total, {error, Reason}};
visit_item(K, V, {Socket, ParentPid, Module, TcpMod, ?ACK_COUNT, Total, _Err}) ->
    M = <<?PT_MSG_OLDSYNC:8,"sync">>,
    case TcpMod:send(Socket, M) of
        ok ->
            case TcpMod:recv(Socket, 0) of
                {ok,[?PT_MSG_OLDSYNC|<<"sync">>]} ->
                    visit_item(K, V, {Socket, ParentPid, Module, TcpMod, 0, Total, ok});
                {error, Reason} ->
                    {Socket, ParentPid, Module, TcpMod, 0, Total, {error, Reason}}
            end;
        {error, Reason} ->
            {Socket, ParentPid, Module, TcpMod, 0, Total, {error, Reason}}
    end;
visit_item(K, V, {Socket, ParentPid, Module, TcpMod, Ack, Total, _ErrStatus}) ->
    BinObj = Module:encode_handoff_item(K, V),
    M = <<?PT_MSG_OBJ:8,BinObj/binary>>,
    case TcpMod:send(Socket, M) of
        ok ->
            {Socket, ParentPid, Module, TcpMod, Ack+1, Total+1, ok};
        {error, Reason} ->
            {Socket, ParentPid, Module, TcpMod, Ack, Total, {error, Reason}}
    end.

get_handoff_port(Node) when is_atom(Node) ->
    case catch(gen_server2:call({riak_core_handoff_listener, Node}, handoff_port, infinity)) of
        {'EXIT', _}  ->
            %% Check old location from previous release
            gen_server2:call({riak_kv_handoff_listener, Node}, handoff_port, infinity);
        Other -> Other
    end.

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
                    error_logger:error_msg("riak_core handoff_ssl_options "
                                           "config error: property ~p: ~p.  "
                                           "Disabling handoff SSL\n",
                                           [FailProp, BadMat]),
                    [];
                X:Y ->
                    error_logger:error_msg("riak_core handoff_ssl_options "
                                           "failure {~p, ~p} processing config "
                                           "~p.  Disabling handoff SSL\n",
                                           [X, Y, Props]),
                    []
            end
    end.
