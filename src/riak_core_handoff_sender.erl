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
-export([start_link/4, get_handoff_ssl_options/0]).
-include("riak_core_vnode.hrl").
-include("riak_core_handoff.hrl").
-define(ACK_COUNT, 1000).
%% can be set with env riak_core, handoff_timeout
-define(TCP_TIMEOUT, 60000).
%% can be set with env riak_core, handoff_status_interval
%% note this is in seconds
-define(STATUS_INTERVAL, 2).

start_link(TargetNode, Module, Partition, Vnode) ->
    SslOpts = get_handoff_ssl_options(),
    Pid = spawn_link(fun()->start_fold(TargetNode,
                                       Module,
                                       Partition,
                                       Vnode,
                                       SslOpts)
                     end),
    {ok, Pid}.

start_fold(TargetNode, Module, Partition, ParentPid, SslOpts) ->
     try
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

         RecvTimeout = get_handoff_receive_timeout(),

         %% Now that handoff_concurrency applies to both outbound and
         %% inbound conns there is a chance that the receiver may
         %% decide to reject the senders attempt to start a handoff.
         %% In the future this will be part of the actual wire
         %% protocol but for now the sender must assume that a closed
         %% socket at this point is a rejection by the receiver to
         %% enforce handoff_concurrency.
         case TcpMod:recv(Socket, 0, RecvTimeout) of
             {ok,[?PT_MSG_OLDSYNC|<<"sync">>]} -> ok;
             {error, timeout} -> exit({shutdown, timeout});
             {error, closed} -> exit({shutdown, max_concurrency})
         end,

         lager:info("Starting handoff of partition ~p ~p from ~p to ~p",
                    [Module, Partition, node(), TargetNode]),

         M = <<?PT_MSG_INIT:8,Partition:160/integer>>,
         ok = TcpMod:send(Socket, M),
         StartFoldTime = os:timestamp(),

         MRef = monitor(process, ParentPid),
         process_flag(trap_exit, true),
         SPid = self(),
         Stats = #ho_stats{interval_end=future_now(get_status_interval())},

         Req = ?FOLD_REQ{foldfun=fun visit_item/3,
                         acc0={Socket, ParentPid, Module, TcpMod,
                               0, 0, Stats, Partition, ok}},

         HPid =
             spawn_link(
               fun() ->
                       %% match structure here because otherwise
                       %% you'll end up in infinite loop below if you
                       %% return something other than what it expects.
                       R = {Socket,ParentPid,Module,TcpMod,_Ack,_,_,_,_} =
                           riak_core_vnode_master:sync_command({Partition, node()},
                                                               Req,
                                                               VMaster, infinity),
                       SPid ! {MRef, R}
               end),

         receive
             {'DOWN', MRef, process, ParentPid, _Info} ->
                 exit(HPid, kill),
                 lager:error("Handoff of partition ~p ~p from ~p to ~p failed "
                             " because vnode died",
                             [Module, Partition, node(), TargetNode]),

                 error;

             {'EXIT', From, E} ->
                 lager:error("Handoff of partition ~p ~p from ~p to ~p failed "
                             " because ~p died with reason ~p",
                             [Module, Partition, node(), TargetNode, From, E]),
                 error;

             {MRef, {Socket,ParentPid,Module,TcpMod,_Ack,SentCount,_,_,ErrStatus}} ->

                 case ErrStatus of
                     ok ->
                         %% One last sync to make sure the message has been received.
                         %% post-0.14 vnodes switch to handoff to forwarding immediately
                         %% so handoff_complete can only be sent once all of the data is
                         %% written.  handle_handoff_data is a sync call, so once
                         %% we receive the sync the remote side will be up to date.
                         lager:debug("~p ~p Sending final sync", [Partition, Module]),
                         ok = TcpMod:send(Socket, <<?PT_MSG_SYNC:8>>),

                         case TcpMod:recv(Socket, 0, RecvTimeout) of
                             {ok,[?PT_MSG_SYNC|<<"sync">>]} ->
                                 lager:debug("~p ~p Final sync received", [Partition, Module]);
                             {error, timeout} -> exit({shutdown, timeout})
                         end,

                         FoldTimeDiff = end_fold_time(StartFoldTime),

                         lager:info("Handoff of partition ~p ~p from ~p to ~p "
                                    "completed: sent ~p objects in ~.2f "
                                    "seconds",
                                    [Module, Partition, node(), TargetNode,
                                     SentCount, FoldTimeDiff]),
                         gen_fsm:send_event(ParentPid, handoff_complete);
                     {error, ErrReason} ->
                         FoldTimeDiff = end_fold_time(StartFoldTime),
                         lager:error("Handoff of partition ~p ~p from ~p to ~p "
                                     "FAILED after sending ~p objects "
                                     "in ~.2f seconds: ~p",
                                     [Module, Partition, node(), TargetNode,
                                      SentCount, FoldTimeDiff, ErrReason]),
                         if ErrReason == timeout ->
                                 riak_core_stat:update(handoff_timeouts);
                            true -> ok
                         end,
                         gen_fsm:send_event(ParentPid, {handoff_error,
                                                        fold_error, ErrReason})
                 end
         end
     catch
         exit:{shutdown,max_concurrency} ->
             %% In this case the receiver hungup on the sender because
             %% of handoff_concurrency.  You don't want to log
             %% anything because this is normal.
             ok;
         exit:{shutdown, timeout} ->
             %% A receive timeout during handoff
             riak_core_stat:update(handoff_timeouts),
             lager:warning(
               "TCP recv timeout in handoff of partition ~p ~p from ~p to ~p",
               [Module, Partition, node(), TargetNode]);
         Err:Reason ->
             lager:error("Handoff of partition ~p ~p from ~p to ~p failed ~p:~p ~p",
                         [Module, Partition, node(), TargetNode,
                          Err, Reason, erlang:get_stacktrace()]),
             gen_fsm:send_event(ParentPid, {handoff_error, Err, Reason})
     end.

%% When a tcp error occurs, the ErrStatus argument is set to {error, Reason}.
%% Since we can't abort the fold, this clause is just a no-op.
%%
%% TODO: turn accumulator into record, this tuple is out of control
visit_item(_K, _V, {Socket, Parent, Module, TcpMod, Ack, Total, Stats,
                    Partition, {error, Reason}}) ->
    {Socket, Parent, Module, TcpMod, Ack, Total, Stats, Partition,
     {error, Reason}};
visit_item(K, V, {Socket, Parent, Module, TcpMod, ?ACK_COUNT, Total,
                  Stats, Partition, _Err}) ->
    RecvTimeout = get_handoff_receive_timeout(),
    M = <<?PT_MSG_OLDSYNC:8,"sync">>,
    NumBytes = byte_size(M),

    Stats2 = incr_bytes(Stats, NumBytes),
    Stats3 = maybe_send_status({Module, Partition}, Stats2),

    case TcpMod:send(Socket, M) of
        ok ->
            case TcpMod:recv(Socket, 0, RecvTimeout) of
                {ok,[?PT_MSG_OLDSYNC|<<"sync">>]} ->
                    visit_item(K, V, {Socket, Parent, Module, TcpMod, 0,
                                      Total, Stats3, Partition, ok});
                {error, Reason} ->
                    {Socket, Parent, Module, TcpMod, 0, Total,
                     Stats3, Partition, {error, Reason}}
            end;
        {error, Reason} ->
            {Socket, Parent, Module, TcpMod, 0, Total, Stats3,
             Partition, {error, Reason}}
    end;
visit_item(K, V, {Socket, Parent, Module, TcpMod, Ack, Total, Stats,
                  Partition, _ErrStatus}) ->
    BinObj = Module:encode_handoff_item(K, V),
    M = <<?PT_MSG_OBJ:8,BinObj/binary>>,
    NumBytes = byte_size(M),

    Stats2 = incr_bytes(incr_objs(Stats), NumBytes),
    Stats3 = maybe_send_status({Module, Partition}, Stats2),

    case TcpMod:send(Socket, M) of
        ok ->
            {Socket, Parent, Module, TcpMod, Ack+1, Total+1, Stats3,
             Partition, ok};
        {error, Reason} ->
            {Socket, Parent, Module, TcpMod, Ack, Total, Stats3,
             Partition, {error, Reason}}
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
                    lager:error("SSL handoff config error: property ~p: ~p.",
                                [FailProp, BadMat]),
                    [];
                X:Y ->
                    lager:error("Failure processing SSL handoff config "
                                "~p: ~p:~p",
                                [Props, X, Y]),
                    []
            end
    end.

get_handoff_receive_timeout() ->
    app_helper:get_env(riak_core, handoff_timeout, ?TCP_TIMEOUT).

end_fold_time(StartFoldTime) ->
    EndFoldTime = os:timestamp(),
    timer:now_diff(EndFoldTime, StartFoldTime) / 1000000.

%% @private
%%
%% @doc Produce the value of `now/0' as if it were called `S' seconds
%% in the future.
-spec future_now(pos_integer()) -> erlang:timestamp().
future_now(S) ->
    {Megas, Secs, Micros} = os:timestamp(),
    {Megas, Secs + S, Micros}.

%% @private
%%
%% @doc Check if the given timestamp `TS' has elapsed.
-spec is_elapsed(erlang:timestamp()) -> boolean().
is_elapsed(TS) ->
    os:timestamp() >= TS.

%% @private
%%
%% @doc Increment `Stats' byte count by `NumBytes'.
-spec incr_bytes(ho_stats(), non_neg_integer()) -> NewStats::ho_stats().
incr_bytes(Stats=#ho_stats{bytes=Bytes}, NumBytes) ->
    Stats#ho_stats{bytes=Bytes + NumBytes}.

%% @private
%%
%% @doc Increment `Stats' object count by 1.
-spec incr_objs(ho_stats()) -> NewStats::ho_stats().
incr_objs(Stats=#ho_stats{objs=Objs}) ->
    Stats#ho_stats{objs=Objs+1}.

%% @private
%%
%% @doc Check if the interval has elapsed and if so send handoff stats
%%      for `ModIdx' to the manager and return a new stats record
%%      `NetStats'.
-spec maybe_send_status({module(), non_neg_integer()}, ho_stats()) ->
                               NewStats::ho_stats().
maybe_send_status(ModIdx, Stats=#ho_stats{interval_end=IntervalEnd}) ->
    case is_elapsed(IntervalEnd) of
        true ->
            Stats2 = Stats#ho_stats{last_update=os:timestamp()},
            riak_core_handoff_manager:status_update(ModIdx, Stats2),
            #ho_stats{interval_end=future_now(get_status_interval())};
        false ->
            Stats
    end.

get_status_interval() ->
    app_helper:get_env(riak_core, handoff_status_interval, ?STATUS_INTERVAL).
