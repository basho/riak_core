%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
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

-module(riak_core_vnode_manager).

-behaviour(gen_server).

-export([start_link/0, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([all_vnodes/0, all_vnodes/1, all_vnodes_status/0, ring_changed/1,
         force_handoffs/0, repair/3, repair_status/1, xfer_complete/2,
         kill_repairs/1]).
-export([all_index_pid/1, get_vnode_pid/2, start_vnode/2,
         unregister_vnode/2, unregister_vnode/3, vnode_event/4]).
%% Field debugging
-export([get_tab/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(idxrec, {key, idx, mod, pid, monref}).

-record(xfer_status, {
          status                :: pending | complete,
          mod_src_target        :: {module(), index(), index()}
         }).
-type xfer_status() :: #xfer_status{}.

-record(repair,
        {
          mod_partition         :: mod_partition(),
          filter_mod_fun        :: {module(), atom()},
          minus_one_xfer        :: xfer_status(),
          plus_one_xfer         :: xfer_status(),
          pairs                 :: [{index(), node()}]
        }).
-type repair() :: #repair{}.
-type repairs() :: [repair()].

-record(state, {idxtab,
                forwarding :: [pid()],
                handoff :: [{term(), integer(), pid(), node()}],
                known_modules :: [term()],
                never_started :: [{integer(), term()}],
                vnode_start_tokens :: integer(),
                repairs :: repairs()
               }).

-include("riak_core_handoff.hrl").
-include("riak_core_vnode.hrl").
-define(XFER_EQ(A, ModSrcTgt), A#xfer_status.mod_src_target == ModSrcTgt).
-define(XFER_COMPLETE(X), X#xfer_status.status == complete).
-define(DEFAULT_OWNERSHIP_TRIGGER, 8).
-define(ETS, ets_vnode_mgr).

%% ===================================================================
%% Public API
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).

all_vnodes_status() ->
    gen_server:call(?MODULE, all_vnodes_status).

%% @doc Repair the given `ModPartition' pair for `Service' using the
%%      given `FilterModFun' to filter keys.
-spec repair(atom(), {module(), partition()}, {module(), atom()}) ->
                    {ok, Pairs::[{partition(), node()}]} |
                    {down, Down::[{partition(), node()}]} |
                    ownership_change_in_progress.
repair(Service, {_Module, Partition}=ModPartition, FilterModFun) ->
    %% Fwd the request to the partition owner to guarantee that there
    %% is only one request per partition.
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Owner = riak_core_ring:index_owner(Ring, Partition),
    Msg = {repair, Service, ModPartition, FilterModFun},
    gen_server:call({?MODULE, Owner}, Msg).

%% @doc Get the status of the repair process for a given `ModPartition'.
-spec repair_status(mod_partition()) -> in_progress | not_found.
repair_status({_Module, Partition}=ModPartition) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Owner = riak_core_ring:index_owner(Ring, Partition),
    gen_server:call({?MODULE, Owner}, {repair_status, ModPartition}).

%% TODO: make cast with retry on handoff sender side and handshake?
%%
%% TODO: second arg has specific form but maybe make proplist?
-spec xfer_complete(node(), tuple()) -> ok.
xfer_complete(Origin, Xfer) ->
    gen_server:call({?MODULE, Origin}, {xfer_complete, Xfer}).

kill_repairs(Reason) ->
    gen_server:cast(?MODULE, {kill_repairs, Reason}).

ring_changed(_TaintedRing) ->
    %% The ring passed into ring events is the locally modified tainted ring.
    %% Since the vnode manager uses operations that cannot work on the
    %% tainted ring, we must retreive the raw ring directly.
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    gen_server:cast(?MODULE, {ring_changed, Ring}).

%% @doc Provided for support/debug purposes. Forces all running vnodes to start
%%      handoff. Limited by handoff_concurrency setting and therefore may need
%%      to be called multiple times to force all handoffs to complete.
force_handoffs() ->
    gen_server:cast(?MODULE, force_handoffs).

unregister_vnode(Index, VNodeMod) ->
    unregister_vnode(Index, self(), VNodeMod).

unregister_vnode(Index, Pid, VNodeMod) ->
    gen_server:cast(?MODULE, {unregister, Index, VNodeMod, Pid}).

start_vnode(Index, VNodeMod) ->
    gen_server:cast(?MODULE, {Index, VNodeMod, start_vnode}).

vnode_event(Mod, Idx, Pid, Event) ->
    gen_server:cast(?MODULE, {vnode_event, Mod, Idx, Pid, Event}).

get_tab() ->
    gen_server:call(?MODULE, get_tab, infinity).

get_vnode_pid(Index, VNodeMod) ->
    gen_server:call(?MODULE, {Index, VNodeMod, get_vnode}, infinity).

%% ===================================================================
%% ETS-based API: try to determine response by reading protected ETS
%%                table, falling back to a vnode manager call if
%%                ETS table is missing.
%% ===================================================================

all_vnodes() ->
    case get_all_vnodes() of
        [] ->
            %% ETS error could produce empty list, call manager to be sure.
            gen_server:call(?MODULE, all_vnodes);
        Result ->
            Result
    end.

all_vnodes(Mod) ->
    case get_all_vnodes(Mod) of
        [] ->
            %% ETS error could produce empty list, call manager to be sure.
            gen_server:call(?MODULE, {all_vnodes, Mod});
        Result ->
            Result
    end.

all_index_pid(VNodeMod) ->
    case get_all_index_pid(VNodeMod, ets_error) of
        ets_error ->
            gen_server:call(?MODULE, {all_index_pid, VNodeMod}, infinity);
        Result ->
            Result
    end.

%% ===================================================================
%% Protected ETS Accessors
%% ===================================================================

get_all_index_pid(Mod, Default) ->
    try
        [list_to_tuple(L)
         || L <- ets:match(?ETS, {idxrec, '_', '$1', Mod, '$2', '_'})]
    catch
        _:_ ->
            Default
    end.

get_all_vnodes() ->
    Mods = [Mod || {_App, Mod} <- riak_core:vnode_modules()],
    get_all_vnodes(Mods).

get_all_vnodes(Mods) when is_list(Mods) ->
    lists:flatmap(fun(Mod) -> get_all_vnodes(Mod) end, Mods);
get_all_vnodes(Mod) ->
    IdxPids = get_all_index_pid(Mod, []),
    [{Mod, Idx, Pid} || {Idx, Pid} <- IdxPids].

%% ===================================================================
%% gen_server behaviour
%% ===================================================================

%% @private
init(_State) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    Mods = [Mod || {_, Mod} <- riak_core:vnode_modules()],
    State = #state{forwarding=[], handoff=[],
                   known_modules=[], never_started=[], vnode_start_tokens=0,
                   repairs=[]},
    State2 = find_vnodes(State),
    AllVNodes = get_all_vnodes(Mods),
    State3 = update_forwarding(AllVNodes, Mods, Ring, State2),
    State4 = update_handoff(AllVNodes, Ring, State3),
    schedule_management_timer(),
    {ok, State4}.

%% @private
find_vnodes(State) ->
    %% Get the current list of vnodes running in the supervisor. We use this
    %% to rebuild our ETS table for routing messages to the appropriate
    %% vnode.
    VnodePids = [Pid || {_, Pid, worker, _}
                            <- supervisor:which_children(riak_core_vnode_sup),
                        is_pid(Pid) andalso is_process_alive(Pid)],
    IdxTable = ets:new(?ETS, [{keypos, 2}, named_table, protected]),

    %% If the vnode manager is being restarted, scan the existing
    %% vnode children and work out which module and index they are
    %% responsible for.  During startup it is possible that these
    %% vnodes may be shutting down as we check them if there are
    %% several types of vnodes active.
    PidIdxs = lists:flatten(
                [try
                     [{Pid, riak_core_vnode:get_mod_index(Pid)}]
                 catch
                     _:_Err ->
                         []
                 end || Pid <- VnodePids]),

    %% Populate the ETS table with processes running this VNodeMod (filtered
    %% in the list comprehension)
    F = fun(Pid, Idx, Mod) ->
                Mref = erlang:monitor(process, Pid),
                #idxrec { key = {Idx,Mod}, idx = Idx, mod = Mod, pid = Pid,
                          monref = Mref }
        end,
    IdxRecs = [F(Pid, Idx, Mod) || {Pid, {Mod, Idx}} <- PidIdxs],
    true = ets:insert_new(IdxTable, IdxRecs),
    State#state{idxtab=IdxTable}.

%% @private
handle_call(all_vnodes_status, _From, State) ->
    Reply = get_all_vnodes_status(State),
    {reply, Reply, State};
handle_call(all_vnodes, _From, State) ->
    Reply = get_all_vnodes(),
    {reply, Reply, State};
handle_call({all_vnodes, Mod}, _From, State) ->
    Reply = get_all_vnodes(Mod),
    {reply, Reply, State};
handle_call({all_index_pid, Mod}, _From, State) ->
    Reply = get_all_index_pid(Mod, []),
    {reply, Reply, State};
handle_call({Partition, Mod, get_vnode}, _From, State) ->
    Pid = get_vnode(Partition, Mod, State),
    {reply, {ok, Pid}, State};
handle_call(get_tab, _From, State) ->
    {reply, ets:tab2list(State#state.idxtab), State};

handle_call({repair, Service, {Mod,Partition}=ModPartition, FilterModFun},
            _From, #state{repairs=Repairs}=State) ->

    case get_repair(ModPartition, Repairs) of
        none ->
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            Pairs = repair_pairs(Ring, Partition),
            UpNodes = riak_core_node_watcher:nodes(Service),

            case riak_core_ring:pending_changes(Ring) of
                    [] ->
                        case check_up(Pairs, UpNodes) of
                            true ->
                                {MOP,_} = MinusOne = get_minus_one(Pairs),
                                {POP,_} = PlusOne = get_plus_one(Pairs),
                                riak_core_handoff_manager:xfer(MinusOne,
                                                               ModPartition,
                                                               FilterModFun),
                                riak_core_handoff_manager:xfer(PlusOne,
                                                               ModPartition,
                                                               FilterModFun),
                                MOXStatus = #xfer_status{status=pending,
                                                         mod_src_target={Mod, MOP, Partition}},
                                POXStatus = #xfer_status{status=pending,
                                                         mod_src_target={Mod, POP, Partition}},

                                Repair = #repair{mod_partition=ModPartition,
                                                 filter_mod_fun=FilterModFun,
                                                 pairs=Pairs,
                                                 minus_one_xfer=MOXStatus,
                                                 plus_one_xfer=POXStatus},
                                Repairs2 = Repairs ++ [Repair],
                                State2 = State#state{repairs=Repairs2},
                                lager:debug("add repair ~p", [ModPartition]),
                                {reply, {ok, Pairs}, State2};
                            {false, Down} ->
                                {reply, {down, Down}, State}
                        end;
                    _ ->
                        {reply, ownership_change_in_progress, State}
                end;
        Repair ->
            Pairs = Repair#repair.pairs,
            {reply, {ok, Pairs}, State}
    end;

handle_call({repair_status, ModPartition}, _From, State) ->
    Repairs = State#state.repairs,
    case get_repair(ModPartition, Repairs) of
        none -> {reply, not_found, State};
        #repair{} -> {reply, in_progress, State}
    end;

%% NOTE: The `xfer_complete' logic assumes two things:
%%
%%       1. The `xfer_complete' msg will always be sent to the owner
%%       of the partition under repair (also called the "target").
%%
%%       2. The target partition is always a local, primary partition.
handle_call({xfer_complete, ModSrcTgt}, _From, State) ->
    Repairs = State#state.repairs,
    {Mod, _, Partition} = ModSrcTgt,
    ModPartition = {Mod, Partition},
    case get_repair(ModPartition, Repairs) of
        none ->
            lager:error("Received xfer_complete for non-existing repair: ~p",
                        [ModPartition]),
            {reply, ok, State};
        #repair{minus_one_xfer=MOX, plus_one_xfer=POX}=R ->
            R2 = if ?XFER_EQ(MOX, ModSrcTgt) ->
                         MOX2 = MOX#xfer_status{status=complete},
                         R#repair{minus_one_xfer=MOX2};
                    ?XFER_EQ(POX, ModSrcTgt) ->
                         POX2 = POX#xfer_status{status=complete},
                         R#repair{plus_one_xfer=POX2};
                    true ->
                         lager:error("Received xfer_complete for "
                                     "non-existing xfer: ~p", [ModSrcTgt])
                 end,

            case {?XFER_COMPLETE(R2#repair.minus_one_xfer),
                  ?XFER_COMPLETE(R2#repair.plus_one_xfer)} of
                {true, true} ->
                    {reply, ok, State#state{repairs=remove_repair(R2, Repairs)}};
                _ ->
                    {reply, ok, State#state{repairs=replace_repair(R2, Repairs)}}
            end
    end;

handle_call(_, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast({Partition, Mod, start_vnode}, State) ->
    get_vnode(Partition, Mod, State),
    {noreply, State};
handle_cast({unregister, Index, Mod, Pid}, #state{idxtab=T} = State) ->
    %% Update forwarding state to ensure vnode is not restarted in
    %% incorrect forwarding state if next request arrives before next
    %% ring event.
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    State2 = update_forwarding({Mod, Index}, Ring, State),
    ets:match_delete(T, {idxrec, {Index, Mod}, Index, Mod, Pid, '_'}),
    riak_core_vnode_proxy:unregister_vnode(Mod, Index, Pid),
    {noreply, State2};
handle_cast({vnode_event, Mod, Idx, Pid, Event}, State) ->
    handle_vnode_event(Event, Mod, Idx, Pid, State);
handle_cast(force_handoffs, State) ->
    AllVNodes = get_all_vnodes(),
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    State2 = update_handoff(AllVNodes, Ring, State),

    [maybe_trigger_handoff(Mod, Idx, Pid, State2)
     || {Mod, Idx, Pid} <- AllVNodes],

    {noreply, State2};
handle_cast({ring_changed, Ring}, State) ->
    %% Update vnode forwarding state
    AllVNodes = get_all_vnodes(),
    Mods = [Mod || {_, Mod} <- riak_core:vnode_modules()],
    State2 = update_forwarding(AllVNodes, Mods, Ring, State),

    %% Update handoff state
    State3 = update_handoff(AllVNodes, Ring, State2),

    %% Trigger ownership transfers.
    Transfers = riak_core_ring:pending_changes(Ring),
    trigger_ownership_handoff(Transfers, Mods, State3),

    {noreply, State3};

handle_cast(maybe_start_vnodes, State) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    State2 = maybe_start_vnodes(Ring, State),
    {noreply, State2};

handle_cast({kill_repairs, Reason}, State) ->
    lager:warning("Killing all repairs: ~p", [Reason]),
    kill_repairs(State#state.repairs, Reason),
    {noreply, State#state{repairs=[]}};

handle_cast(_, State) ->
    {noreply, State}.

handle_info(management_tick, State) ->
    schedule_management_timer(),
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    Mods = [Mod || {_, Mod} <- riak_core:vnode_modules()],
    AllVNodes = get_all_vnodes(Mods),
    State2 = update_handoff(AllVNodes, Ring, State),
    Transfers = riak_core_ring:pending_changes(Ring),

    %% Kill/cancel any repairs during ownership changes
    State3 =
        case Transfers of
            [] ->
                State2;
            _ ->
                Repairs = State#state.repairs,
                kill_repairs(Repairs, ownership_change),
                trigger_ownership_handoff(Transfers, Mods, State2),
                State2#state{repairs=[]}
        end,

    MaxStart = app_helper:get_env(riak_core, vnode_rolling_start, 16),
    State4 = State3#state{vnode_start_tokens=MaxStart},
    State5 = maybe_start_vnodes(Ring, State4),

    Repairs2 = check_repairs(State4#state.repairs),
    {noreply, State5#state{repairs=Repairs2}};

handle_info({'DOWN', MonRef, process, _P, _I}, State) ->
    delmon(MonRef, State),
    {noreply, State}.

%% @private
handle_vnode_event(inactive, Mod, Idx, Pid, State) ->
    maybe_trigger_handoff(Mod, Idx, Pid, State),
    {noreply, State};
handle_vnode_event(handoff_complete, Mod, Idx, Pid, State) ->
    NewHO = orddict:erase({Mod, Idx}, State#state.handoff),
    gen_fsm:send_all_state_event(Pid, finish_handoff),
    {noreply, State#state{handoff=NewHO}};
handle_vnode_event(handoff_error, Mod, Idx, Pid, State) ->
    NewHO = orddict:erase({Mod, Idx}, State#state.handoff),
    gen_fsm:send_all_state_event(Pid, cancel_handoff),
    {noreply, State#state{handoff=NewHO}}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ===================================================================
%% Internal functions
%% ===================================================================

schedule_management_timer() ->
    ManagementTick = app_helper:get_env(riak_core,
                                        vnode_management_timer,
                                        10000),
    erlang:send_after(ManagementTick, ?MODULE, management_tick).

trigger_ownership_handoff(Transfers, Mods, State) ->
    Limit = app_helper:get_env(riak_core,
                               forced_ownership_handoff,
                               ?DEFAULT_OWNERSHIP_TRIGGER),
    Throttle = lists:sublist(Transfers, Limit),
    Awaiting = [{Mod, Idx} || {Idx, Node, _, CMods, S} <- Throttle,
                              Mod <- Mods,
                              S =:= awaiting,
                              Node =:= node(),
                              not lists:member(Mod, CMods)],
    [maybe_trigger_handoff(Mod, Idx, State) || {Mod, Idx} <- Awaiting],
    ok.

%% @private
idx2vnode(Idx, Mod, _State=#state{idxtab=T}) ->
    case ets:lookup(T, {Idx, Mod}) of
        [I] -> I#idxrec.pid;
        []  -> no_match
    end.

%% @private
delmon(MonRef, _State=#state{idxtab=T}) ->
    ets:match_delete(T, {idxrec, '_', '_', '_', '_', MonRef}).

%% @private
add_vnode_rec(I,  _State=#state{idxtab=T}) -> ets:insert(T,I).

%% @private
get_vnode(Idx, Mod, State) ->
    case idx2vnode(Idx, Mod, State) of
        no_match ->
            ForwardTo = get_forward(Mod, Idx, State),
            {ok, Pid} = riak_core_vnode_sup:start_vnode(Mod, Idx, ForwardTo),
            MonRef = erlang:monitor(process, Pid),
            add_vnode_rec(#idxrec{key={Idx,Mod},idx=Idx,mod=Mod,pid=Pid,
                                  monref=MonRef}, State),
            Pid;
        X -> X
    end.

get_forward(Mod, Idx, #state{forwarding=Fwd}) ->
    case orddict:find({Mod, Idx}, Fwd) of
        {ok, ForwardTo} ->
            ForwardTo;
        _ ->
            undefined
    end.

check_forward(Ring, Mod, Index) ->
    Node = node(),
    case riak_core_ring:next_owner(Ring, Index, Mod) of
        {Node, NextOwner, complete} ->
            {{Mod, Index}, NextOwner};
        _ ->
            {{Mod, Index}, undefined}
    end.

compute_forwarding(Mods, Ring) ->
    {AllIndices, _} = lists:unzip(riak_core_ring:all_owners(Ring)),
    Forwarding = [check_forward(Ring, Mod, Index) || Index <- AllIndices,
                                                     Mod <- Mods],
    orddict:from_list(Forwarding).

update_forwarding(AllVNodes, Mods, Ring,
                  State=#state{forwarding=Forwarding}) ->
    NewForwarding = compute_forwarding(Mods, Ring),

    %% Inform vnodes that have changed forwarding status
    VNodes = lists:sort([{{Mod, Idx}, Pid} || {Mod, Idx, Pid} <- AllVNodes]),
    Diff = NewForwarding -- Forwarding,
    [change_forward(VNodes, Mod, Idx, ForwardTo)
     || {{Mod, Idx}, ForwardTo} <- Diff],

    State#state{forwarding=NewForwarding}.

update_forwarding({Mod, Idx}, Ring, State=#state{forwarding=Forwarding}) ->
    {_, ForwardTo} = check_forward(Ring, Mod, Idx),
    NewForwarding = orddict:store({Mod, Idx}, ForwardTo, Forwarding),
    State#state{forwarding=NewForwarding}.

change_forward(VNodes, Mod, Idx, ForwardTo) ->
    case orddict:find({Mod, Idx}, VNodes) of
        error ->
            ok;
        {ok, Pid} ->
            riak_core_vnode:set_forwarding(Pid, ForwardTo),
            ok
    end.

update_handoff(AllVNodes, Ring, State) ->
    case riak_core_ring:ring_ready(Ring) of
        false ->
            State;
        true ->
            NewHO = lists:flatten([case should_handoff(Ring, Mod, Idx) of
                                       false ->
                                           [];
                                       {true, TargetNode} ->
                                           [{{Mod, Idx}, TargetNode}]
                                   end || {Mod, Idx, _Pid} <- AllVNodes]),
            State#state{handoff=orddict:from_list(NewHO)}
    end.

should_handoff(Ring, Mod, Idx) ->
    {_, NextOwner, _} = riak_core_ring:next_owner(Ring, Idx),
    Owner = riak_core_ring:index_owner(Ring, Idx),
    Ready = riak_core_ring:ring_ready(Ring),
    case determine_handoff_target(Ready, Owner, NextOwner) of
        undefined ->
            false;
        TargetNode ->
            case app_for_vnode_module(Mod) of
                undefined -> false;
                {ok, App} ->
                    case lists:member(TargetNode, 
                                      riak_core_node_watcher:nodes(App)) of
                        false  -> false;
                        true -> {true, TargetNode}
                    end
            end
    end.

determine_handoff_target(Ready, Owner, NextOwner) ->
    Me = node(),
    TargetNode = case {Ready, Owner, NextOwner} of
                     {_, _, Me} ->
                         Me;
                     {_, Me, undefined} ->
                         Me;
                     {true, Me, _} ->
                         NextOwner;
                     {_, _, undefined} ->
                         Owner;
                     {_, _, _} ->
                         Me
                 end,
    case TargetNode of
        Me ->
            undefined;
        _ ->
            TargetNode
    end.

app_for_vnode_module(Mod) when is_atom(Mod) ->
    case application:get_env(riak_core, vnode_modules) of
        {ok, Mods} ->
            case lists:keysearch(Mod, 2, Mods) of
                {value, {App, Mod}} ->
                    {ok, App};
                false ->
                    undefined
            end;
        undefined -> undefined
    end.

maybe_trigger_handoff(Mod, Idx, State) ->
    Pid = get_vnode(Idx, Mod, State),
    maybe_trigger_handoff(Mod, Idx, Pid, State).

maybe_trigger_handoff(Mod, Idx, Pid, _State=#state{handoff=HO}) ->
    case orddict:find({Mod, Idx}, HO) of
        {ok, TargetNode} ->
            riak_core_vnode:trigger_handoff(Pid, TargetNode),
            ok;
        error ->
            ok
    end.

get_all_vnodes_status(#state{forwarding=Forwarding, handoff=HO}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Owners = riak_core_ring:all_owners(Ring),
    VNodes = get_all_vnodes(),
    Mods = [Mod || {_App, Mod} <- riak_core:vnode_modules()],

    ThisNode = node(),
    Types = [case Owner of
                 ThisNode ->
                     {{Mod, Idx}, {type, primary}};
                 _ ->
                     {{Mod, Idx}, {type, secondary}}
             end || {Idx, Owner} <- Owners,
                    Mod <- Mods],
    Types2 = lists:keysort(1, Types),
    Pids = [{{Mod, Idx}, {pid, Pid}} || {Mod, Idx, Pid} <- VNodes],
    Pids2 = lists:keysort(1, Pids),
    Forwarding2 = [{MI, {forwarding, Node}} || {MI,Node} <- Forwarding,
                                               Node /= undefined],
    Handoff2 = [{MI, {should_handoff, Node}} || {MI,Node} <- HO],

    MergeFn = fun(_, V1, V2) when is_list(V1) and is_list(V2) ->
                      V1 ++ V2;
                 (_, V1, V2) when is_list(V1) ->
                      V1 ++ [V2];
                 (_, V1, V2) ->
                      [V1, V2]
              end,
    Status = lists:foldl(fun(B, A) ->
                                 orddict:merge(MergeFn, A, B)
                         end, Types2, [Pids2, Forwarding2, Handoff2]),
    Status.

update_never_started(Ring, State) ->
    {Indices, _} = lists:unzip(riak_core_ring:all_owners(Ring)),
    lists:foldl(fun({_App, Mod}, StateAcc) ->
                        case lists:member(Mod, StateAcc#state.known_modules) of
                            false ->
                                update_never_started(Mod, Indices, StateAcc);
                            true ->
                                StateAcc
                        end
                end, State, riak_core:vnode_modules()).

update_never_started(Mod, Indices, State) ->
    IdxPids = get_all_index_pid(Mod, []),
    AlreadyStarted = [Idx || {Idx, _Pid} <- IdxPids],
    NeverStarted = ordsets:subtract(ordsets:from_list(Indices),
                                    ordsets:from_list(AlreadyStarted)),
    NeverStarted2 = [{Idx, Mod} || Idx <- NeverStarted],
    NeverStarted3 = NeverStarted2 ++ State#state.never_started,
    KnownModules = [Mod | State#state.known_modules],
    State#state{known_modules=KnownModules, never_started=NeverStarted3}.

maybe_start_vnodes(Ring, State) ->
    State2 = update_never_started(Ring, State),
    State3 = maybe_start_vnodes(State2),
    State3.

maybe_start_vnodes(State=#state{vnode_start_tokens=Tokens,
                                never_started=NeverStarted}) ->
    case {Tokens, NeverStarted} of
        {0, _} ->
            State;
        {_, []} ->
            State;
        {_, [{Idx, Mod} | NeverStarted2]} ->
            get_vnode(Idx, Mod, State),
            gen_server:cast(?MODULE, maybe_start_vnodes),
            State#state{vnode_start_tokens=Tokens-1,
                        never_started=NeverStarted2}
    end.

-spec check_repairs(repairs()) -> Repairs2::repairs().
check_repairs(Repairs) ->
    Check =
        fun(R=#repair{minus_one_xfer=MOX, plus_one_xfer=POX}, Repairs2) ->
                Pairs = R#repair.pairs,
                MO = get_minus_one(Pairs),
                PO = get_plus_one(Pairs),
                MOX2 = maybe_retry(R, MO, MOX),
                POX2 = maybe_retry(R, PO, POX),

                if ?XFER_COMPLETE(MOX2) andalso ?XFER_COMPLETE(POX2) ->
                        Repairs2;
                   true ->
                        R2 = R#repair{minus_one_xfer=MOX2, plus_one_xfer=POX2},
                        [R2|Repairs2]
                end
        end,
    lists:reverse(lists:foldl(Check, [], Repairs)).

%% TODO: get all this repair, xfer status and Src business figured out.
-spec maybe_retry(repair(), tuple(), xfer_status()) -> Xfer2::xfer_status().
maybe_retry(R, {SrcPartition, _}=Src, Xfer) ->
    case Xfer#xfer_status.status of
        complete ->
            Xfer;
        pending ->
            {Mod, _, Partition} = Xfer#xfer_status.mod_src_target,
            FilterModFun = R#repair.filter_mod_fun,

            riak_core_handoff_manager:xfer(Src, {Mod, Partition}, FilterModFun),
            #xfer_status{status=pending,
                         mod_src_target={Mod, SrcPartition, Partition}}
    end.

%% @private
%%
%% @doc Verify that all nodes are up involved in the repair.
-spec check_up([{non_neg_integer(), node()}], [node()]) ->
                      true | {false, Down::[{non_neg_integer(), node()}]}.
check_up(Pairs, UpNodes) ->
    Down = [Pair || {_Partition, Owner}=Pair <- Pairs,
                    not lists:member(Owner, UpNodes)],
    case Down of
        [] -> true;
        _ -> {false, Down}
    end.

%% @private
%%
%% @doc Get the three `{Partition, Owner}' pairs involved in a repair
%%      operation for the given `Ring' and `Partition'.
-spec repair_pairs(riak_core_ring:riak_core_ring(), non_neg_integer()) ->
                          [{Partition::non_neg_integer(), Owner::node()}].
repair_pairs(Ring, Partition) ->
    Owner = riak_core_ring:index_owner(Ring, Partition),
    CH = riak_core_ring:chash(Ring),
    [_, Before] = chash:predecessors(<<Partition:160/integer>>, CH, 2),
    [After] = chash:successors(<<Partition:160/integer>>, CH, 1),
    [Before, {Partition, Owner}, After].

%% @private
%%
%% @doc Get the corresponding repair entry in `Repairs', if one
%%      exists, for the given `ModPartition'.
-spec get_repair(mod_partition(), repairs()) -> repair() | none.
get_repair(ModPartition, Repairs) ->
    case lists:keyfind(ModPartition, #repair.mod_partition, Repairs) of
        false -> none;
        Val -> Val
    end.

%% @private
%%
%% @doc Remove the repair entry.
-spec remove_repair(repair(), repairs()) -> repairs().
remove_repair(Repair, Repairs) ->
    lists:keydelete(Repair#repair.mod_partition, #repair.mod_partition, Repairs).

%% @private
%%
%% @doc Replace the matching repair entry with `Repair'.
-spec replace_repair(repair(), repairs()) -> repairs().
replace_repair(Repair, Repairs) ->
    lists:keyreplace(Repair#repair.mod_partition, #repair.mod_partition,
                     Repairs, Repair).

%% @private
%%
%% @doc Get the `{Partition, Owner}' pair that comes before the
%%      partition under repair.
-spec get_minus_one([{index(), node()}]) -> {index(), node()}.
get_minus_one([MinusOne, _, _]) ->
    MinusOne.

%% @private
%%
%% @doc Get the `{Partition, Owner}' pair that comes after the
%%      partition under repair.
-spec get_plus_one([{index(), node()}]) -> {index(), node()}.
get_plus_one([_, _, PlusOne]) ->
    PlusOne.

%% @private
%%
%% @doc Kill all outbound and inbound xfers related to `Repairs'
%%      targeting this node with `Reason'.
-spec kill_repairs([repair()], term()) -> ok.
kill_repairs(Repairs, Reason) ->
    [kill_repair(Repair, Reason) || Repair <- Repairs],
    ok.

kill_repair(Repair, Reason) ->
    {Mod, Partition} = Repair#repair.mod_partition,
    Pairs = Repair#repair.pairs,
    {_,MOOwner} = get_minus_one(Pairs),
    {_,POOwner} = get_minus_one(Pairs),
    MOX = Repair#repair.minus_one_xfer,
    POX = Repair#repair.plus_one_xfer,
    MOModSrcTarget = MOX#xfer_status.mod_src_target,
    POModSrcTarget = POX#xfer_status.mod_src_target,
    %% Kill the remote senders
    riak_core_handoff_manager:kill_xfer(MOOwner,
                                        MOModSrcTarget,
                                        Reason),
    riak_core_handoff_manager:kill_xfer(POOwner,
                                        POModSrcTarget,
                                        Reason),
    %% Kill the local receivers
    riak_core_handoff_manager:kill_xfer(node(),
                                        {Mod, undefined, Partition},
                                        Reason).
