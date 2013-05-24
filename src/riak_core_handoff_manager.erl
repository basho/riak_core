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

%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_core_handoff_manager).
-behaviour(gen_server).

%% gen_server api
-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

%% exclusion api
-export([add_exclusion/2,
         get_exclusions/1,
         remove_exclusion/2
        ]).

%% handoff api
-export([add_outbound/5,
         add_inbound/1,
         xfer/3,
         kill_xfer/3,
         status/0,
         status/1,
         status_update/2,
         set_concurrency/1,
         get_concurrency/0,
         set_recv_data/2,
         kill_handoffs/0
        ]).

-include("riak_core_handoff.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state,
        { excl,
          handoffs=[] :: [handoff_status()]
        }).

%% this can be overridden with riak_core handoff_concurrency
-define(HANDOFF_CONCURRENCY,2).
-define(HO_EQ(HOA, HOB),
        HOA#handoff_status.mod_src_tgt == HOB#handoff_status.mod_src_tgt
        andalso HOA#handoff_status.timestamp == HOB#handoff_status.timestamp).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #state{excl=ordsets:new(), handoffs=[]}}.

add_outbound(Module,Idx,Node,VnodePid,Opts) ->
    case application:get_env(riak_core, disable_outbound_handoff) of
        {ok, true} ->
            {error, max_concurrency};
        _ ->
            gen_server:call(?MODULE,{add_outbound,Module,Idx,Node,VnodePid,Opts},infinity)
    end.

add_inbound(SSLOpts) ->
    case application:get_env(riak_core, disable_inbound_handoff) of
        {ok, true} ->
            {error, max_concurrency};
        _ ->
            gen_server:call(?MODULE,{add_inbound,SSLOpts},infinity)
    end.

%% @doc Initiate a transfer from `SrcPartition' to `TargetPartition'
%%      for the given `Module' using the `FilterModFun' filter.
-spec xfer({index(), node()}, mod_partition(), {module(), atom()}) -> ok.
xfer({SrcPartition, SrcOwner}, {Module, TargetPartition}, FilterModFun) ->
    %% NOTE: This will not work with old nodes
    ReqOrigin = node(),
    gen_server:cast({?MODULE, SrcOwner},
                    {send_handoff, Module,
                     {SrcPartition, TargetPartition},
                     ReqOrigin, FilterModFun}).

%% @doc Associate `Data' with the inbound handoff `Recv'.
-spec set_recv_data(pid(), proplists:proplist()) -> ok.
set_recv_data(Recv, Data) ->
    gen_server:call(?MODULE, {set_recv_data, Recv, Data}, infinity).

status() ->
    status(none).

status(Filter) ->
    gen_server:call(?MODULE, {status, Filter}, infinity).

%% @doc Send status updates `Stats' to the handoff manager for a
%%      particular handoff identified by `ModSrcTgt'.
-spec status_update(mod_src_tgt(), ho_stats()) -> ok.
status_update(ModSrcTgt, Stats) ->
    gen_server:cast(?MODULE, {status_update, ModSrcTgt, Stats}).

set_concurrency(Limit) ->
    gen_server:call(?MODULE,{set_concurrency,Limit}, infinity).

get_concurrency() ->
    gen_server:call(?MODULE, get_concurrency, infinity).

%% @doc Kill the transfer of `ModSrcTarget' with `Reason'.
-spec kill_xfer(node(), tuple(), any()) -> ok.
kill_xfer(SrcNode, ModSrcTarget, Reason) ->
    gen_server:cast({?MODULE, SrcNode}, {kill_xfer, ModSrcTarget, Reason}).

kill_handoffs() ->
    set_concurrency(0).

add_exclusion(Module, Index) ->
    gen_server:cast(?MODULE, {add_exclusion, {Module, Index}}).

remove_exclusion(Module, Index) ->
    gen_server:cast(?MODULE, {del_exclusion, {Module, Index}}).

get_exclusions(Module) ->
    gen_server:call(?MODULE, {get_exclusions, Module}, infinity).


%%%===================================================================
%%% Callbacks
%%%===================================================================

handle_call({get_exclusions, Module}, _From, State=#state{excl=Excl}) ->
    Reply =  [I || {M, I} <- ordsets:to_list(Excl), M =:= Module],
    {reply, {ok, Reply}, State};
handle_call({add_outbound,Mod,Idx,Node,Pid,Opts},_From,State=#state{handoffs=HS}) ->
    case send_handoff(Mod,Idx,Node,Pid,HS,Opts) of
        {ok,Handoff=#handoff_status{transport_pid=Sender}} ->
            HS2 = HS ++ [Handoff],
            {reply, {ok,Sender}, State#state{handoffs=HS2}};
        {false,_ExistingHandoff=#handoff_status{transport_pid=Sender}} ->
            {reply, {ok,Sender}, State};
        Error ->
            {reply, Error, State}
    end;
handle_call({add_inbound,SSLOpts},_From,State=#state{handoffs=HS}) ->
    case receive_handoff(SSLOpts) of
        {ok,Handoff=#handoff_status{transport_pid=Receiver}} ->
            HS2 = HS ++ [Handoff],
            {reply, {ok,Receiver}, State#state{handoffs=HS2}};
        Error ->
            {reply, Error, State}
    end;

handle_call({set_recv_data, Recv, Data}, _From, State=#state{handoffs=HS}) ->
    case lists:keyfind(Recv, #handoff_status.transport_pid, HS) of
        false ->
            throw({error, "set_recv_data called for non-existing receiver",
                   Recv, Data});
        #handoff_status{}=H ->
            H2 = H#handoff_status{
                   mod_src_tgt=proplists:get_value(mod_src_tgt, Data),
                   vnode_pid=proplists:get_value(vnode_pid, Data)
                  },
            HS2 = lists:keyreplace(Recv, #handoff_status.transport_pid, HS, H2),
            {reply, ok, State#state{handoffs=HS2}}
    end;

handle_call({xfer_status, Xfer}, _From, State=#state{handoffs=HS}) ->
    TP = Xfer#handoff_status.transport_pid,
    case lists:keyfind(TP, #handoff_status.transport_pid, HS) of
        false -> {reply, not_found, State};
        _ -> {reply, in_progress, State}
    end;

handle_call({status, Filter}, _From, State=#state{handoffs=HS}) ->
    Status = lists:filter(filter(Filter), [build_status(HO) || HO <- HS]),
    {reply, Status, State};

handle_call({set_concurrency,Limit},_From,State=#state{handoffs=HS}) ->
    application:set_env(riak_core,handoff_concurrency,Limit),
    case Limit < erlang:length(HS) of
        true ->
            %% Note: we don't update the state with the handoffs that we're
            %% keeping because we'll still get the 'DOWN' messages with
            %% a reason of 'max_concurrency' and we want to be able to do
            %% something with that if necessary.
            {_Keep,Discard}=lists:split(Limit,HS),
            [erlang:exit(Pid,max_concurrency) ||
                #handoff_status{transport_pid=Pid} <- Discard],
            {reply, ok, State};
        false ->
            {reply, ok, State}
    end;

handle_call(get_concurrency, _From, State) ->
    Concurrency = get_concurrency_limit(),
    {reply, Concurrency, State}.

handle_cast({del_exclusion, {Mod, Idx}}, State=#state{excl=Excl}) ->
    Excl2 = ordsets:del_element({Mod, Idx}, Excl),
    {noreply, State#state{excl=Excl2}};

handle_cast({add_exclusion, {Mod, Idx}}, State=#state{excl=Excl}) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    case riak_core_ring:my_indices(Ring) of
        [] ->
            %% Trigger a ring update to ensure the node shuts down
            riak_core_ring_events:ring_update(Ring);
        _ ->
            ok
    end,
    Excl2 = ordsets:add_element({Mod, Idx}, Excl),
    {noreply, State#state{excl=Excl2}};

handle_cast({status_update, ModSrcTgt, StatsUpdate}, State=#state{handoffs=HS}) ->
    case lists:keyfind(ModSrcTgt, #handoff_status.mod_src_tgt, HS) of
        false ->
            lager:error("status_update for non-existing handoff ~p", [ModSrcTgt]),
            {noreply, State};
        HO ->
            Stats2 = update_stats(StatsUpdate, HO#handoff_status.stats),
            HO2 = HO#handoff_status{stats=Stats2},
            HS2 = lists:keyreplace(ModSrcTgt, #handoff_status.mod_src_tgt, HS, HO2),
            {noreply, State#state{handoffs=HS2}}
    end;

handle_cast({send_handoff, Mod, {Src, Target}, ReqOrigin,
             {FilterMod, FilterFun}=FMF},
            State=#state{handoffs=HS}) ->
    Filter = FilterMod:FilterFun(Target),
    %% TODO: make a record?
    {ok, VNode} = riak_core_vnode_manager:get_vnode_pid(Src, Mod),
    case send_handoff({Mod, Src, Target}, ReqOrigin, VNode, HS,
                      {Filter, FMF}, ReqOrigin, []) of
        {ok, Handoff} ->
            HS2 = HS ++ [Handoff],
            {noreply, State#state{handoffs=HS2}};
        _ ->
            {noreply, State}
    end;

handle_cast({kill_xfer, ModSrcTarget, Reason}, State) ->
    HS = State#state.handoffs,
    HS2 = kill_xfer_i(ModSrcTarget, Reason, HS),
    {noreply, State#state{handoffs=HS2}}.

handle_info({'DOWN', Ref, process, _Pid, Reason}, State=#state{handoffs=HS}) ->
    case lists:keytake(Ref, #handoff_status.transport_mon, HS) of
        {value,
         #handoff_status{mod_src_tgt={M, S, I}, direction=Dir, vnode_pid=Vnode,
                         vnode_mon=VnodeM, req_origin=Origin},
         NewHS
        } ->
            WarnVnode =
                case Reason of
                    %% if the reason the handoff process died was anything other
                    %% than 'normal' we should log the reason why as an error
                    normal ->
                        false;
                    X when X == max_concurrency orelse
                           (element(1, X) == shutdown andalso
                            element(2, X) == max_concurrency) ->
                        lager:info("An ~w handoff of partition ~w ~w was terminated for reason: ~w~n", [Dir,M,I,Reason]),
                        true;
                    _ ->
                        lager:error("An ~w handoff of partition ~w ~w was terminated for reason: ~w~n", [Dir,M,I,Reason]),
                        true
                end,

            %% if we have the vnode process pid, tell the vnode why the
            %% handoff stopped so it can clean up its state
            case WarnVnode andalso is_pid(Vnode) of
                true ->
                    riak_core_vnode:handoff_error(Vnode, 'DOWN', Reason);
                _ ->
                    case Origin of
                        none -> ok;
                        _ ->
                            %% Use proplist instead so it's more
                            %% flexible in future, or does
                            %% capabilities nullify that?
                            Msg = {M, S, I},
                            riak_core_vnode_manager:xfer_complete(Origin, Msg)
                    end,
                    ok
            end,

            %% No monitor on vnode for receiver
            if VnodeM /= undefined -> demonitor(VnodeM);
               true -> ok
            end,

            %% removed the handoff from the list of active handoffs
            {noreply, State#state{handoffs=NewHS}};
        false ->
            case lists:keytake(Ref, #handoff_status.vnode_mon, HS) of
                {value,
                 #handoff_status{mod_src_tgt={M,_,I}, direction=Dir,
                                 transport_pid=Trans, transport_mon=TransM},
                 NewHS} ->
                    %% In this case the vnode died and the handoff
                    %% sender must be killed.
                    lager:error("An ~w handoff of partition ~w ~w was "
                                "terminated because the vnode died",
                                [Dir, M, I]),
                    demonitor(TransM),
                    exit(Trans, vnode_died),
                    {noreply, State#state{handoffs=NewHS}};
                _ ->
                    {noreply, State}
            end
    end.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Private
%%%===================================================================

build_status(HO) ->
    #handoff_status{mod_src_tgt={Mod, SrcP, TargetP},
                    src_node=SrcNode,
                    target_node=TargetNode,
                    direction=Dir,
                    status=Status,
                    timestamp=StartTS,
                    transport_pid=TPid,
                    type=Type}=HO,
    {status_v2, [{mod, Mod},
                 {src_partition, SrcP},
                 {target_partition, TargetP},
                 {src_node, SrcNode},
                 {target_node, TargetNode},
                 {direction, Dir},
                 {status, Status},
                 {start_ts, StartTS},
                 {sender_pid, TPid},
                 {stats, calc_stats(HO)},
                 {type, Type}]}.

calc_stats(#handoff_status{stats=Stats,timestamp=StartTS,size=Size}) ->
    case dict:find(last_update, Stats) of
        error ->
            no_stats;
        {ok, LastUpdate} ->
            Objs = dict:fetch(objs, Stats),
            Bytes = dict:fetch(bytes, Stats),
            CalcSize = get_size(Size),
            Done = calc_pct_done(Objs, Bytes, CalcSize),
            ElapsedS = timer:now_diff(LastUpdate, StartTS) / 1000000,
            ObjsS = round(Objs / ElapsedS),
            BytesS = round(Bytes / ElapsedS),
            [{objs_total, Objs},
             {objs_per_s, ObjsS},
             {bytes_per_s, BytesS},
             {last_update, LastUpdate},
             {size, CalcSize},
             {pct_done_decimal, Done}]
    end.

get_size({F, dynamic}) ->
    F();
get_size(S) ->
    S.

calc_pct_done(_, _, undefined) ->
    undefined;
calc_pct_done(Objs, _, {Size, objects}) ->
    Objs / Size;
calc_pct_done(_, Bytes, {Size, bytes}) ->
    Bytes / Size.

filter(none) ->
    fun(_) -> true end;
filter({Key, Value}=_Filter) ->
    fun({status_v2, Status}) ->
            case proplists:get_value(Key, Status) of
                Value -> true;
                _ -> false
            end
    end.

get_concurrency_limit () ->
    app_helper:get_env(riak_core,handoff_concurrency,?HANDOFF_CONCURRENCY).

%% true if handoff_concurrency (inbound + outbound) hasn't yet been reached
handoff_concurrency_limit_reached () ->
    Receivers=supervisor:count_children(riak_core_handoff_receiver_sup),
    Senders=supervisor:count_children(riak_core_handoff_sender_sup),
    ActiveReceivers=proplists:get_value(active,Receivers),
    ActiveSenders=proplists:get_value(active,Senders),
    get_concurrency_limit() =< (ActiveReceivers + ActiveSenders).

send_handoff(Mod, Partition, Node, Pid, HS, Opts) ->
    send_handoff({Mod, Partition, Partition}, Node, Pid, HS, {none, none}, none, Opts).

%% @private
%%
%% @doc Start a handoff process for the given `Mod' from
%%      `Src'/`VNode' to `Target'/`Node' using the given `Filter'
%%      function which is a predicate applied to the key.  The
%%      `Origin' is the node this request originated from so a reply
%%      can't be sent on completion.
-spec send_handoff({module(), index(), index()}, node(),
                   pid(), list(),
                   {predicate() | none, {module(), atom()} | none}, node()) ->
                          {ok, handoff_status()}
                              | {error, max_concurrency}
                              | {false, handoff_status()}.
send_handoff({Mod, Src, Target}, Node, Vnode, HS, {Filter, FilterModFun}, Origin, Opts) ->
    case handoff_concurrency_limit_reached() of
        true ->
            {error, max_concurrency};
        false ->
            ShouldHandoff=
                case lists:keyfind({Mod, Src, Target}, #handoff_status.mod_src_tgt, HS) of
                    false ->
                        true;
                    Handoff=#handoff_status{target_node=Node,vnode_pid=Vnode} ->
                        {false,Handoff};
                    #handoff_status{transport_pid=Sender} ->
                        %% found a running handoff with a different vnode
                        %% source or a different arget ndoe, kill the current
                        %% one and the new one will start up
                        erlang:exit(Sender,resubmit_handoff_change),
                        true
                end,

            case ShouldHandoff of
                true ->
                    VnodeM = monitor(process, Vnode),
                    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
                    %% assumes local node is doing the sending
                    Primary = riak_core_ring:is_primary(Ring, {Src, node()}),
                    HOType = if Primary ->
                                     if Src == Target -> ownership_handoff;
                                        true -> repair
                                     end;
                                true -> hinted_handoff
                             end,

                    %% start the sender process
                    case HOType of
                        repair ->
                            {ok, Pid} =
                                riak_core_handoff_sender_sup:start_repair(Mod,
                                                                          Src,
                                                                          Target,
                                                                          Vnode,
                                                                          Node,
                                                                          Filter);
                        _ ->
                            {ok,Pid} =
                                riak_core_handoff_sender_sup:start_handoff(HOType,
                                                                           Mod,
                                                                           Target,
                                                                           Vnode,
                                                                           Node)
                    end,
                    PidM = monitor(process, Pid),
                    Size = validate_size(proplists:get_value(size, Opts)),

                    %% successfully started up a new sender handoff
                    {ok, #handoff_status{ transport_pid=Pid,
                                          transport_mon=PidM,
                                          direction=outbound,
                                          timestamp=os:timestamp(),
                                          src_node=node(),
                                          target_node=Node,
                                          mod_src_tgt={Mod, Src, Target},
                                          vnode_pid=Vnode,
                                          vnode_mon=VnodeM,
                                          status=[],
                                          stats=dict:new(),
                                          type=HOType,
                                          req_origin=Origin,
                                          filter_mod_fun=FilterModFun,
                                          size=Size
                                        }
                    };

                %% handoff already going, just return it
                AlreadyExists={false,_CurrentHandoff} ->
                    AlreadyExists
            end
    end.

%% spawn a receiver process
receive_handoff (SSLOpts) ->
    case handoff_concurrency_limit_reached() of
        true ->
            {error, max_concurrency};
        false ->
            {ok,Pid}=riak_core_handoff_receiver_sup:start_receiver(SSLOpts),
            PidM = monitor(process, Pid),

            %% successfully started up a new receiver
            {ok, #handoff_status{ transport_pid=Pid,
                                  transport_mon=PidM,
                                  direction=inbound,
                                  timestamp=os:timestamp(),
                                  mod_src_tgt={undefined, undefined, undefined},
                                  src_node=undefined,
                                  target_node=undefined,
                                  status=[],
                                  stats=dict:new(),
                                  req_origin=none
                                }
            }
    end.

update_stats(StatsUpdate, Stats) ->
    #ho_stats{last_update=LU, objs=Objs, bytes=Bytes}=StatsUpdate,
    Stats2 = dict:update_counter(objs, Objs, Stats),
    Stats3 = dict:update_counter(bytes, Bytes, Stats2),
    dict:store(last_update, LU, Stats3).

validate_size(Size={N, U}) when is_number(N) andalso
                           N > 0 andalso
                           (U =:= bytes orelse U =:= objects) ->
    Size;
validate_size(Size={F, dynamic}) when is_function(F) ->
    Size;
validate_size(_) ->
    undefined.


%% @private
%%
%% @doc Kill and remove _each_ xfer associated with `ModSrcTarget'
%%      with `Reason'.  There might be more than one because repair
%%      can have two simultaneous inbound xfers.
kill_xfer_i(ModSrcTarget, Reason, HS) ->
    case lists:keytake(ModSrcTarget, #handoff_status.mod_src_tgt, HS) of
        false ->
            HS;
        {value, Xfer, HS2} ->
            #handoff_status{mod_src_tgt={Mod, SrcPartition, TargetPartition},
                            type=Type,
                            target_node=TargetNode,
                            src_node=SrcNode,
                            transport_pid=TP
                           } = Xfer,
            Msg = "~p transfer of ~p from ~p ~p to ~p ~p killed for reason ~p",
            case Type of
                undefined ->
                    ok;
                _ ->
                    lager:info(Msg, [Type, Mod, SrcNode, SrcPartition,
                                     TargetNode, TargetPartition, Reason])
            end,
            exit(TP, {kill_xfer, Reason}),
            kill_xfer_i(ModSrcTarget, Reason, HS2)
    end.

%%%===================================================================
%%% Tests
%%%===================================================================

-ifdef (TEST_BROKEN_AZ_TICKET_1042).

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
    ?assertEqual([],status()),

    %% clear handoff_concurrency and make sure a handoff fails
    ?assertEqual(ok,set_concurrency(0)),
    ?assertEqual({error,max_concurrency},add_inbound([])),
    ?assertEqual({error,max_concurrency},add_outbound(riak_kv,0,node(),self())),

    %% allow for a single handoff
    ?assertEqual(ok,set_concurrency(1)),

    %% done
    ok.

-endif.
