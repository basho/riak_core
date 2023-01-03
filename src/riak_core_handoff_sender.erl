%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

-compile({nowarn_deprecated_function, 
            [{gen_fsm, send_event, 2}]}).

-include("riak_core_vnode.hrl").
-include("riak_core_handoff.hrl").
-include("stacktrace.hrl").
-define(ACK_COUNT, 1000).
%% can be set with env riak_core, handoff_timeout
-define(TCP_TIMEOUT, 60000).
%% can be set with env riak_core, handoff_status_interval
%% note this is in seconds
-define(STATUS_INTERVAL, 2).

-define(log_info(Str, Args),
        lager:info("~p transfer of ~p from ~p ~p to ~p ~p failed " ++ Str,
                   [Type, Module, SrcNode, SrcPartition, TargetNode,
                    TargetPartition] ++ Args)).
-define(log_fail(Str, Args),
        lager:error("~p transfer of ~p from ~p ~p to ~p ~p failed " ++ Str,
                    [Type, Module, SrcNode, SrcPartition, TargetNode,
                     TargetPartition] ++ Args)).

%% Accumulator for the visit item HOF
-record(ho_acc,
        {
          ack                  :: non_neg_integer(),
          error                :: ok | {error, any()},
          filter               :: function(),
          module               :: module(),
          parent               :: pid(),
          socket               :: any(),
          src_target           :: {non_neg_integer(), non_neg_integer()},
          stats                :: #ho_stats{},
          tcp_mod              :: module(),

          total_objects        :: non_neg_integer(),
          total_bytes          :: non_neg_integer(),

          item_queue           :: [binary()],
          item_queue_length    :: non_neg_integer(),
          item_queue_byte_size :: non_neg_integer(),

          acksync_threshold    :: pos_integer(),
          acklog_threshold     :: pos_integer(),
          acklog_last          :: erlang:timestamp(),
          handoff_batch_threshold_size :: pos_integer(), % bytes
          handoff_batch_threshold_count :: pos_integer(),

          rcv_timeout          :: pos_integer(), % milliseconds

          type                 :: ho_type(),

          notsent_acc          :: term() | undefined,
          notsent_fun          :: function()| undefined
        }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(TargetNode, Module, {Type, Opts}, Vnode) ->
    SslOpts = get_handoff_ssl_options(),
    Pid = 
        spawn_link(
            fun()->
                start_fold(TargetNode, Module, {Type, Opts}, Vnode, SslOpts)
            end
        ),
    {ok, Pid}.

%%%===================================================================
%%% Private
%%%===================================================================

start_fold(TargetNode, Module, {Type, Opts}, ParentPid, SslOpts) ->
    SrcNode = node(),
    SrcPartition = get_src_partition(Opts),
    TargetPartition = get_target_partition(Opts),

    try
        %% Give workers one more chance to abort or get a lock or whatever.
        FoldOpts = maybe_call_handoff_started(Module, SrcPartition),

        Filter = get_filter(Opts),
        [_Name,Host] = string:tokens(atom_to_list(TargetNode), "@"),
        {ok, Port} = get_handoff_port(TargetNode),
        TNHandoffIP =
        case get_handoff_ip(TargetNode) of
            error ->
                Host;
            {ok, "0.0.0.0"} ->
                Host;
            {ok, Other} ->
                Other
        end,
        SockOpts = [binary, {packet, 4}, {header,1}, {active, false}],
        {Socket, TcpMod} =
            if SslOpts /= [] ->
                {ok, Skt} =
                    ssl:connect(
                        TNHandoffIP, Port, SslOpts ++ SockOpts, 15000),
                {Skt, ssl};
            true ->
                {ok, Skt} =
                gen_tcp:connect(TNHandoffIP, Port, SockOpts, 15000),
                {Skt, gen_tcp}
            end,

        Config = [{vnode_mod, Module}, {partition, TargetPartition}],
        ConfigBin = term_to_binary(Config),
        ok = TcpMod:send(Socket, <<?PT_MSG_CONFIGURE:8, ConfigBin/binary>>),

        RecvTimeout = get_handoff_timeout(),

        AckSyncThreshold =
            app_helper:get_env(riak_core, handoff_acksync_threshold, 1),
            % This will force a sync before each batch by default - so sending
            % the next batch will be delayed until the previous batch has been
            % processed.  Prior to 3.0.13, this was set to a default of 25
        AckLogThreshold =
            app_helper:get_env(riak_core, handoff_acklog_threshold, 100),
            % As the batch size if 1MB, this will log progress every 100MB
        HandoffBatchThresholdSize =
            app_helper:get_env(
                riak_core, handoff_batch_threshold, 1024 * 1024),
            % Batch threshold is in bytes
        HandoffBatchThresholdCount =
            app_helper:get_env(
                riak_core, handoff_batch_threshold_count, 2000),

        %% Now that handoff_concurrency applies to both outbound and
        %% inbound conns there is a chance that the receiver may
        %% decide to reject the senders attempt to start a handoff.
        %% In the future this will be part of the actual wire
        %% protocol but for now the sender must assume that a closed
        %% socket at this point is a rejection by the receiver to
        %% enforce handoff_concurrency.
        case send_sync(TcpMod, Socket, RecvTimeout) of
            ok ->
                ok;
            {error, DirectionS, timeout} ->
                lager:error(
                    "Initial sync message returned ~w error timeout "
                    "between src_partition=~p trg_partition=~p "
                    "type=~w module=~w ",
                    [DirectionS,
                        SrcPartition, TargetPartition,
                        Type, Module]),
                exit({shutdown, timeout});
            {error, _, closed} ->
                exit({shutdown, max_concurrency})
        end,

        lager:info("Starting ~p transfer of ~p from ~p ~p to ~p ~p",
                [Type, Module, SrcNode, SrcPartition,
                    TargetNode, TargetPartition]),

        M = <<?PT_MSG_INIT:8,TargetPartition:160/integer>>,
        ok = TcpMod:send(Socket, M),
        StartFoldTime = os:timestamp(),
        Stats = #ho_stats{interval_end=future_now(get_status_interval())},
        UnsentAcc0 = get_notsent_acc0(Opts),
        UnsentFun = get_notsent_fun(Opts),

        Req = 
            riak_core_util:make_fold_req(
                fun visit_item/3,
                #ho_acc{
                    ack=0,
                    error=ok,
                    filter=Filter,
                    module=Module,
                    parent=ParentPid,
                    socket=Socket,
                    src_target={SrcPartition, TargetPartition},
                    stats=Stats,
                    tcp_mod=TcpMod,

                    total_bytes=0,
                    total_objects=0,

                    item_queue=[],
                    item_queue_length=0,
                    item_queue_byte_size=0,

                    acksync_threshold=AckSyncThreshold,
                    acklog_threshold=AckLogThreshold,
                    acklog_last = os:timestamp(),
                    handoff_batch_threshold_size = HandoffBatchThresholdSize,
                    handoff_batch_threshold_count = HandoffBatchThresholdCount,
                    rcv_timeout = RecvTimeout,

                    type=Type,
                    notsent_acc=UnsentAcc0,
                    notsent_fun=UnsentFun},
                false,
                FoldOpts),

        %% IFF the vnode is using an async worker to perform the fold
        %% then sync_command will return error on vnode crash,
        %% otherwise it will wait forever but vnode crash will be
        %% caught by handoff manager.  I know, this is confusing, a
        %% new handoff system will be written soon enough.

        VMaster = list_to_atom(atom_to_list(Module) ++ "_master"),
        AccRecord0 =
            riak_core_vnode_master:sync_command(
                {SrcPartition, SrcNode}, Req, VMaster, infinity),

        %% Send any straggler entries remaining in the buffer:
        AccRecord = send_objects(AccRecord0#ho_acc.item_queue, AccRecord0),

        if AccRecord == {error, vnode_shutdown} ->
                ?log_info("because the local vnode was shutdown", []),
                throw({be_quiet, error, local_vnode_shutdown_requested});
            true ->
                ok % If not #ho_acc, get badmatch below
        end,
        #ho_acc{
            error=ErrStatus,
            module=Module,
            parent=ParentPid,
            tcp_mod=TcpMod,
            total_objects=TotalObjects,
            total_bytes=TotalBytes,
            stats=FinalStats,
            notsent_acc=NotSentAcc} = AccRecord,

        case ErrStatus of
            ok ->
                %% One last sync to make sure the message has been received.
                %% post-0.14 vnodes switch to handoff to forwarding immediately
                %% so handoff_complete can only be sent once all of the data is
                %% written.  handle_handoff_data is a sync call, so once
                %% we receive the sync the remote side will be up to date.
                lager:debug(
                    "~p ~p Sending final sync",
                    [SrcPartition, Module]),
                case send_sync(TcpMod, Socket, RecvTimeout) of
                    ok ->
                        ok;
                    {error, DirectionE, timeout} -> 
                        lager:error(
                            "Final sync message returned ~w error timeout "
                            "between src_partition=~p trg_partition=~p "
                            "type=~w module=~w ",
                            [DirectionE,
                                SrcPartition, TargetPartition,
                                Type, Module]
                        ),
                        exit({shutdown, timeout})
                end,

                FoldTimeDiff = end_fold_time(StartFoldTime),
                ThroughputBytes = TotalBytes/FoldTimeDiff,

                ok = 
                lager:info(
                    "~p transfer of ~p from ~p ~p to ~p ~p"
                    " completed: sent ~s bytes in ~p of ~p objects"
                    " in ~.2f seconds (~s/second)",
                    [Type, Module,
                        SrcNode, SrcPartition,
                        TargetNode, TargetPartition,
                    riak_core_format:human_size_fmt(
                            "~.2f", TotalBytes),
                        FinalStats#ho_stats.objs, TotalObjects, FoldTimeDiff,
                        riak_core_format:human_size_fmt(
                            "~.2f", ThroughputBytes)]),
                case Type of
                    repair ->
                        ok;
                    resize ->
                        gen_fsm:send_event(
                            ParentPid, {resize_transfer_complete, NotSentAcc});
                    _ ->
                        gen_fsm:send_event(
                            ParentPid, handoff_complete)
                end;
            {error, ErrReason} ->
                if ErrReason == timeout ->
                        exit({shutdown, timeout});
                true ->
                        exit({shutdown, {error, ErrReason}})
                end
        end
     catch
         exit:{shutdown,max_concurrency} ->
             %% Need to fwd the error so the handoff mgr knows
             exit({shutdown, max_concurrency});
         exit:{shutdown, timeout} ->
             %% A receive timeout during handoff
             riak_core_stat:update(handoff_timeouts),
             ?log_fail("because of TCP recv timeout", []),
             exit({shutdown, timeout});
         exit:{shutdown, {error, Reason}} ->
             ?log_fail("because of ~p", [Reason]),
             gen_fsm:send_event(
                ParentPid, {handoff_error, fold_error, Reason}),
             exit({shutdown, {error, Reason}});
         throw:{be_quiet, Err, Reason} ->
             gen_fsm:send_event(ParentPid, {handoff_error, Err, Reason});
         ?_exception_(Err, Reason, StackToken) ->
             ?log_fail("because of ~p:~p ~p",
                       [Err, Reason, ?_get_stacktrace_(StackToken)]),
             gen_fsm:send_event(ParentPid, {handoff_error, Err, Reason})
     end.

visit_item(K, V, Acc) ->
    #ho_acc{filter=Filter,
            module=Module,
            total_objects=TotalObjects,
            item_queue=ItemQueue,
            item_queue_length=ItemQueueLength,
            item_queue_byte_size=ItemQueueByteSize,
            handoff_batch_threshold_size=HandoffBatchThresholdSize,
            handoff_batch_threshold_count=HandoffBatchThresholdCount,
            notsent_fun=NotSentFun,
            notsent_acc=NotSentAcc} = Acc,
    case Filter(K) of
        true ->
            case Module:encode_handoff_item(K, V) of
                corrupted ->
                    {Bucket, Key} = K,
                    lager:warning(
                        "Unreadable object ~p/~p discarded", [Bucket, Key]),
                    Acc;
                BinObj ->
                    ItemQueue2 = [BinObj | ItemQueue],
                    ItemQueueLength2 = ItemQueueLength + 1,
                    ItemQueueByteSize2 = ItemQueueByteSize + byte_size(BinObj),

                    Acc2 =
                        Acc#ho_acc{
                            item_queue_length=ItemQueueLength2,
                            item_queue_byte_size=ItemQueueByteSize2},
                    
                    BatchReady = 
                        (ItemQueueByteSize2 =< HandoffBatchThresholdSize) or
                            (ItemQueueLength2 =< HandoffBatchThresholdCount),
                    case BatchReady of
                        true  ->
                            Acc2#ho_acc{item_queue=ItemQueue2};
                        false ->
                            send_objects(ItemQueue2, Acc2)
                    end
            end;
        false ->
            NewNotSentAcc = handle_not_sent_item(NotSentFun, NotSentAcc, K),
            Acc#ho_acc{
                error=ok,
                total_objects=TotalObjects+1,
                notsent_acc=NewNotSentAcc}
    end.

handle_not_sent_item(NotSentFun, Acc, Key) when is_function(NotSentFun) ->
    NotSentFun(Key, Acc);
handle_not_sent_item(undefined, _, _) ->
    undefined.

send_objects([], Acc) ->
    Acc;
send_objects(ItemsReverseList, Acc) ->

    Items = lists:reverse(ItemsReverseList),

    #ho_acc{ack=Ack,
            module=Module,
            socket=Socket,
            src_target={SrcPartition, TargetPartition},
            stats=Stats,
            tcp_mod=TcpMod,
            acksync_threshold=AckSyncThreshold,
            acklog_threshold=AckLogThreshold,
            acklog_last=AckLogLast,
            rcv_timeout=RecvTimeout,
            type=Type,
            total_objects=TotalObjects,
            total_bytes=TotalBytes,
            item_queue_length=NObjects
           } = Acc,

    ObjectList = term_to_binary(Items),
    BatchSize = byte_size(ObjectList),
    BatchCount = length(Items),

    SyncClock = os:timestamp(),
    ReceiverInSync =
        case Ack rem AckSyncThreshold of
            0 ->
                case send_sync(TcpMod, Socket, RecvTimeout) of
                    ok ->
                        ok;
                    {error, Direction, Reason} ->
                        lager:error(
                            "Sync message returned ~w error ~w "
                            "between src_partition=~p trg_partition=~p "
                            "type=~w module=~w ",
                            [Direction, Reason,
                                SrcPartition, TargetPartition,
                                Type, Module]
                        ),
                        {error, Reason}
                end;
            _ ->
                ok
        end,
    UpdAckLogLast =
        case {ReceiverInSync, Ack rem AckLogThreshold} of
            {ok, 0} ->
                lager:info(
                    "Receiver in sync after batch_set=~w and total_batches=~w "
                    "with next batch having batch_size=~w item_count=~w"
                    "between src_partition=~p trg_partition=~p "
                    "type=~w module=~w "
                    "last_sync_time=~w ms batch_set_time=~w ms",
                    [min(Ack, AckLogThreshold), Ack,
                        BatchSize, BatchCount,
                        SrcPartition, TargetPartition,
                        Type, Module,
                        timer:now_diff(os:timestamp(), SyncClock) div 1000,
                        timer:now_diff(os:timestamp(), AckLogLast) div 1000]
                );
            {ok, _} ->
                AckLogLast;
            {{error, SyncFailure}, _} ->
                throw(Acc#ho_acc{error = {error, SyncFailure}})
        end,

    M = <<?PT_MSG_BATCH:8, ObjectList/binary>>,

    NumBytes = byte_size(M),
    Stats2 = incr_bytes(incr_objs(Stats, NObjects), NumBytes),
    Stats3 =
        maybe_send_status({Module, SrcPartition, TargetPartition}, Stats2),

    case TcpMod:send(Socket, M) of
        ok ->
            Acc#ho_acc{ack=Ack+1, error=ok, stats=Stats3,
                       total_objects=TotalObjects+NObjects,
                       total_bytes=TotalBytes+NumBytes,
                       acklog_last=UpdAckLogLast,
                       item_queue=[],
                       item_queue_length=0,
                       item_queue_byte_size=0};
        {error, SendFailure} ->
            lager:error(
                "Send batch returned error ~w "
                "between src_partition=~p trg_partition=~p "
                "type=~w module=~w ",
                [SendFailure,
                    SrcPartition, TargetPartition,
                    Type, Module]
            ),
            throw(Acc#ho_acc{error={error, SendFailure}, stats=Stats3})
    end.

get_handoff_ip(Node) when is_atom(Node) ->
    case riak_core_util:safe_rpc(
            Node,
            riak_core_handoff_listener,
            get_handoff_ip,
            [], 
            infinity) of
        {badrpc, _} ->
            error;
        Res ->
            Res
    end.

get_handoff_port(Node) when is_atom(Node) ->
    riak_core_gen_server:call(
        {riak_core_handoff_listener, Node},
        handoff_port,
        infinity).

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

get_handoff_timeout() ->
    %% Whenever a Sync message is sent, the process will wait for this
    %% timeout, and throw an exception closing the fold if the timeout is
    %% reached.
    %% A sync message is sent every handoff_ack_sync_threshold batches, as 
    %% well as when initialising and closing the handoff.
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
%% @doc Increment `Stats' object count by NObjs:
-spec incr_objs(ho_stats(), non_neg_integer()) -> NewStats::ho_stats().
incr_objs(Stats=#ho_stats{objs=Objs}, NObjs) ->
    Stats#ho_stats{objs=Objs+NObjs}.

%% @private
%%
%% @doc Check if the interval has elapsed and if so send handoff stats
%%      for `ModSrcTgt' to the manager and return a new stats record
%%      `NetStats'.
-spec maybe_send_status({module(), non_neg_integer(), non_neg_integer()},
                        ho_stats()) ->
                               NewStats::ho_stats().
maybe_send_status(ModSrcTgt, Stats=#ho_stats{interval_end=IntervalEnd}) ->
    case is_elapsed(IntervalEnd) of
        true ->
            Stats2 = Stats#ho_stats{last_update=os:timestamp()},
            riak_core_handoff_manager:status_update(ModSrcTgt, Stats2),
            #ho_stats{interval_end=future_now(get_status_interval())};
        false ->
            Stats
    end.

get_status_interval() ->
    app_helper:get_env(riak_core, handoff_status_interval, ?STATUS_INTERVAL).

get_src_partition(Opts) ->
    proplists:get_value(src_partition, Opts).

get_target_partition(Opts) ->
    proplists:get_value(target_partition, Opts).

get_notsent_acc0(Opts) ->
    proplists:get_value(notsent_acc0, Opts).

get_notsent_fun(Opts) ->
    case proplists:get_value(notsent_fun, Opts) of
        none -> fun(_, _) -> undefined end;
        Fun -> Fun
    end.

-spec get_filter(proplists:proplist()) -> predicate().
get_filter(Opts) ->
    case proplists:get_value(filter, Opts) of
        none -> fun(_) -> true end;
        Filter -> Filter
    end.

-spec send_sync(
    module(), gen_tcp:socket(), pos_integer()) -> ok|{error, snd|recv, any()}.
send_sync(TcpMod, Socket, RecvTimeout) ->
    case TcpMod:send(Socket, <<?PT_MSG_SYNC:8, "sync">>) of
        ok ->
            case TcpMod:recv(Socket, 0, RecvTimeout) of
                {ok,[?PT_MSG_SYNC|<<"sync">>]} ->
                    ok;
                {error, Reason} ->
                    {error, rcv, Reason}
            end;
        {error, Reason} ->
            {error, snd, Reason}
    end.

%% @private
%% @doc The optional call to handoff_started/2 allows vnodes
%% one last chance to abort the handoff process and to supply options
%% to be passed to the ?FOLD_REQ if not aborted.the function is passed
%% the source vnode's partition number because the callback does not
%% have access to the full vnode state at this time. In addition the
%% worker pid is passed so the vnode may use that information in its
%% decision to cancel the handoff or not e.g. get a lock on behalf of
%% the process.
maybe_call_handoff_started(Module, SrcPartition) ->
    case lists:member({handoff_started, 2}, Module:module_info(exports)) of
        true ->
            WorkerPid = self(),
            case Module:handoff_started(SrcPartition, WorkerPid) of
                {ok, FoldOpts} ->
                    FoldOpts;
                {error, max_concurrency} ->
                    %% Handoff of that partition is busy or can't proceed.
                    %% Stopping with max_concurrency will cause this partition
                    %% to be retried again later.
                    exit({shutdown, max_concurrency});
                {error, Error} ->
                    exit({shutdown, Error})
            end;
        false ->
            %% optional callback not implemented, so we carry on, w/ no 
            %% additional fold options
            []
    end.
