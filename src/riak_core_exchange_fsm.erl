%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_core_exchange_fsm).
-behaviour(gen_fsm_compat).

%% API
-export([start/7]).

%% FSM states
-export([prepare_exchange/2,
         update_trees/2,
         key_exchange/2]).

%% gen_fsm_compat callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).

-type index() :: non_neg_integer().
-type index_n() :: {index(), pos_integer()}.
-type vnode() :: {index(), node()}.

-record(state, {local       :: vnode(),
                remote      :: vnode(),
                index_n     :: index_n(),
                local_tree  :: pid(),
                remote_tree :: pid() | undefined,
                built       :: non_neg_integer(),
                timer       :: reference() | undefined,
                timeout     :: pos_integer() | undefined,
                vnode       :: atom(),
                service     :: atom()
               }).

%% Per state transition timeout used by certain transitions
-define(DEFAULT_ACTION_TIMEOUT, 60000). %% 1 minute

%% Use occasional calls to disk_log:log() for some backpressure periodically
-define(LOG_BATCH_SIZE, 5000).

%%%===================================================================
%%% API
%%%===================================================================

start(Service, LocalVN, RemoteVN, IndexN, Tree, Manager, VNode) ->
    gen_fsm_compat:start(?MODULE, [Service, LocalVN, RemoteVN, IndexN, Tree, Manager, VNode], []).

%%%===================================================================
%%% gen_fsm_compat callbacks
%%%===================================================================

init([Service, LocalVN, RemoteVN, IndexN, LocalTree, Manager, VNode]) ->
    Timeout = app_helper:get_env(riak_core,
                                 anti_entropy_timeout,
                                 ?DEFAULT_ACTION_TIMEOUT),
    monitor(process, Manager),
    monitor(process, LocalTree),
    State = #state{local=LocalVN,
                   remote=RemoteVN,
                   index_n=IndexN,
                   local_tree=LocalTree,
                   timeout=Timeout,
                   built=0,
                   vnode=VNode,
                   service=Service},
    gen_fsm_compat:send_event(self(), start_exchange),
    lager:debug("Starting exchange: ~p", [LocalVN]),
    {ok, prepare_exchange, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.

handle_info({'DOWN', _, _, _, _}, _StateName, State) ->
    %% Either the entropy manager, local hashtree, or remote hashtree has
    %% exited. Stop exchange.
    {stop, normal, State};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% gen_fsm_compat states
%%%===================================================================

%% @doc Initial state. Attempt to acquire all necessary exchange locks.
%%      In order, acquire local concurrency lock, local tree lock,
%%      remote concurrency lock, and remote tree lock. Exchange will
%%      timeout if locks cannot be acquired in a timely manner.
prepare_exchange(start_exchange, State=#state{remote=RemoteVN,
                                              index_n=IndexN,
                                              service=Service}) ->
    case riak_core_entropy_manager:get_lock(Service, exchange) of
        ok ->
            case riak_core_index_hashtree:get_lock(State#state.local_tree,
                                                   local_fsm) of
                ok ->
                    remote_exchange_request(Service, RemoteVN, IndexN),
                    Timer = gen_fsm_compat:send_event_after(State#state.timeout,
                                                     timeout),
                    {next_state, prepare_exchange, State#state{timer=Timer}};
                _ ->
                    send_exchange_status(already_locked, State),
                    {stop, normal, State}
            end;
        Error ->
            send_exchange_status(Error, State),
            {stop, normal, State}
    end;
prepare_exchange(timeout, State) ->
    do_timeout(State);
prepare_exchange({remote_exchange, Pid}, State) when is_pid(Pid) ->
    _ = gen_fsm_compat:cancel_timer(State#state.timer),
    monitor(process, Pid),
    State2 = State#state{remote_tree=Pid, timer=undefined},
    update_trees(start_exchange, State2);
prepare_exchange({remote_exchange, Error}, State) ->
    _ = gen_fsm_compat:cancel_timer(State#state.timer),
    send_exchange_status({remote, Error}, State),
    {stop, normal, State#state{timer=undefined}}.

%% @doc Now that locks have been acquired, ask both the local and remote
%%      hashtrees to perform a tree update. If updates do not occur within
%%      a timely manner, the exchange will timeout. Since the trees will
%%      continue to finish the update even after the exchange times out,
%%      a future exchange should eventually make progress.
update_trees(start_exchange, State=#state{local=LocalVN,
                                          remote=RemoteVN,
                                          local_tree=LocalTree,
                                          remote_tree=RemoteTree,
                                          index_n=IndexN}) ->
    lager:debug("Sending to ~p", [LocalVN]),
    lager:debug("Sending to ~p", [RemoteVN]),

    update_request(LocalTree, LocalVN, IndexN),
    update_request(RemoteTree, RemoteVN, IndexN),
    {next_state, update_trees, State};

update_trees({not_responsible, VNodeIdx, IndexN}, State) ->
    lager:debug("VNode ~p does not cover preflist ~p", [VNodeIdx, IndexN]),
    send_exchange_status({not_responsible, VNodeIdx, IndexN}, State),
    {stop, normal, State};
update_trees({tree_built, _, _}, State) ->
    Built = State#state.built + 1,
    case Built of
        2 ->
            lager:debug("Moving to key exchange"),
            {next_state, key_exchange, State, 0};
        _ ->
            {next_state, update_trees, State#state{built=Built}}
    end.

%% @doc Now that locks have been acquired and both hashtrees have been updated,
%%      perform a key exchange and trigger read repair for any divergent keys.
key_exchange(timeout, State=#state{local=LocalVN,
                                   remote=RemoteVN,
                                   local_tree=LocalTree,
                                   remote_tree=RemoteTree,
                                   index_n=IndexN,
                                   vnode=VNode,
                                   service=Service}) ->
    lager:debug("Starting key exchange between ~p and ~p", [LocalVN, RemoteVN]),
    lager:debug("Exchanging hashes for preflist ~p", [IndexN]),

    TmpDir = tmp_dir(),
    {NA, NB, NC} = Now = WriteLog = erlang:timestamp(),
    LogFile1 = lists:flatten(io_lib:format("~s/in.~p.~p.~p",
                                           [TmpDir, NA, NB, NC])),
    LogFile2 = lists:flatten(io_lib:format("~s/out.~p.~p.~p",
                                           [TmpDir, NA, NB, NC])),
    Remote = fun(get_bucket, {L, B}) ->
                     exchange_bucket(RemoteTree, IndexN, L, B);
                (key_hashes, Segment) ->
                     exchange_segment(RemoteTree, IndexN, Segment);
                (init, _Y) ->
                     %% Our return value is ignored, so we can't return
                     %% the disk log handle here.  However, disk_log is
                     %% magically stateful, so we don't need to change
                     %% the exchange API to accomodate us.
                     {ok, _} = open_disk_log(Now, LogFile1, read_write),
                     ok;
                (final, _Y) ->
                     ok = disk_log:sync(Now),
                     ok = disk_log:close(Now),
                     ok;
                (start_exchange_level, {_Level, _Buckets}) ->
                     ok;
                (start_exchange_segments, _Segments) ->
                     ok;
                (_X, _Y) ->
                     lager:error("~s LINE ~p: ~p ~p", [?MODULE, ?LINE, _X, _Y]),
                     ok
             end,

    %% Unclear if we should allow exchange to run indefinitely or enforce
    %% a timeout. The problem is that depending on the number of keys and
    %% key differences, exchange can take arbitrarily long. For now, go with
    %% unbounded exchange, with the ability to cancel exchanges through the
    %% entropy manager if needed.
    AccFun = fun(KeyDiff, Acc) ->
                     lists:foldl(fun({DiffReason, BKeyBin}, Count) ->
                                         {B, K} = binary_to_term(BKeyBin),
                                         T = {B, K, DiffReason},
                                         if Count rem ?LOG_BATCH_SIZE == 0 ->
                                                 ok = disk_log:log(WriteLog, T);
                                            true ->
                                                 ok = disk_log:alog(WriteLog, T)
                                         end,
                                         Count+1
                                 end, Acc, KeyDiff)
             end,
    %% TODO: Add stats for AAE
    Count = riak_core_index_hashtree:compare(IndexN, Remote, AccFun, 0,
                                             LocalTree),
    if Count == 0 ->
            Complete = true,
            ok;
       true ->
            %% Sort the keys.  For vnodes that use backends that preserve
            %% lexicographic sort order for BKeys, this is a big
            %% improvement.  For backends that do not, e.g. Bitcask, sorting
            %% by BKey is unlikely to be any worse.  For Riak CS's use
            %% pattern, sorting may have some benefit since block N is
            %% likely to be nearby on disk of block N+1.
            StartTime = erlang:timestamp(),
            ok = sort_disk_log(LogFile1, LogFile2),
            lager:debug("~s:key_exchange: sorting time = ~p seconds\n",
                        [?MODULE, timer:now_diff(erlang:timestamp(), StartTime) / 1000000]),
            {ok, ReadLog} = open_disk_log(Now, LogFile2, read_only),
            FoldRes =
                fold_disk_log(fun(Diff, Acc) ->
                                      read_repair_keydiff(VNode, LocalVN, RemoteVN,
                                                          Diff),
                                      Acc + 1
                              end, 0, ReadLog),
            disk_log:close(ReadLog),
            if Count == FoldRes ->
                    Complete = true,
                    ok;
               true ->
                    lager:error("~s:key_exchange: Count ~p /= FoldRes ~p\n",
                                [?MODULE, Count, FoldRes]),
                    send_exchange_status(failed, State),
                    Complete = false
            end,
            lager:info("Repaired ~b keys during active anti-entropy exchange "
                       "of ~p between ~p and ~p",
                       [Count, IndexN, LocalVN, RemoteVN])
    end,
    [exchange_complete(Service, LocalVN, RemoteVN, IndexN, Count) || Complete],
    _ = file:delete(LogFile1),
    _ = file:delete(LogFile2),
    {stop, normal, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
exchange_bucket(Tree, IndexN, Level, Bucket) ->
    riak_core_index_hashtree:exchange_bucket(IndexN, Level, Bucket, Tree).

%% @private
exchange_segment(Tree, IndexN, Segment) ->
    riak_core_index_hashtree:exchange_segment(IndexN, Segment, Tree).

%% @private
read_repair_keydiff(VNode, LocalVN, RemoteVN, {Bucket, Key, _Reason}) ->
    %% TODO: Even though this is at debug level, it's still extremely
    %%       spammy. Should this just be removed? We can always use
    %%       redbug to trace read_repair_keydiff when needed. Of course,
    %%       users can't do that.
    %% lager:debug("Anti-entropy forced read repair: ~p/~p", [Bucket, Key]),
    VNode:aae_repair(Bucket, Key),
    %% Force vnodes to update AAE tree in case read repair wasn't triggered
    riak_core_aae_vnode:rehash(VNode:master(), [LocalVN, RemoteVN], Bucket, Key),
    timer:sleep(riak_core_entropy_manager:get_aae_throttle()),
    ok.

%% @private
update_request(Tree, {Index, _}, IndexN) ->
    as_event(fun() ->
                     case riak_core_index_hashtree:update(IndexN, Tree) of
                         ok ->
                             {tree_built, Index, IndexN};
                         not_responsible ->
                             {not_responsible, Index, IndexN}
                     end
             end).

remote_exchange_request(Service, RemoteVN, IndexN) ->
    FsmPid = self(),
    as_event(fun() ->
                     riak_core_entropy_manager:start_exchange_remote(Service,
                                                                     RemoteVN,
                                                                     IndexN,
                                                                     FsmPid)
             end).

%% @private
as_event(F) ->
    Self = self(),
    spawn_link(fun() ->
                       Result = F(),
                       gen_fsm_compat:send_event(Self, Result)
               end),
    ok.

%% @private
do_timeout(State=#state{local=LocalVN,
                        remote=RemoteVN,
                        index_n=IndexN}) ->
    lager:info("Timeout during exchange between (local) ~p and (remote) ~p, "
               "(preflist) ~p", [LocalVN, RemoteVN, IndexN]),
    send_exchange_status({timeout, RemoteVN, IndexN}, State),
    {stop, normal, State#state{timer=undefined}}.

%% @private
send_exchange_status(Status, #state{local=LocalVN,
                                    remote=RemoteVN,
                                    index_n=IndexN,
                                    service=Service}) ->
    riak_core_entropy_manager:exchange_status(Service, LocalVN, RemoteVN, IndexN, Status).

exchange_complete(Service, {LocalIdx, _}, {RemoteIdx, RemoteNode}, IndexN, Repaired) ->
    riak_core_entropy_info:exchange_complete(Service, LocalIdx, RemoteIdx, IndexN, Repaired),
    rpc:call(RemoteNode, riak_core_entropy_info, exchange_complete,
             [Service, RemoteIdx, LocalIdx, IndexN, Repaired]).

open_disk_log(Name, Path, RWorRO) ->
    open_disk_log(Name, Path, RWorRO, [{type, halt}, {format, internal}]).

open_disk_log(Name, Path, RWorRO, OtherOpts) ->
    disk_log:open([{name, Name}, {file, Path}, {mode, RWorRO}|OtherOpts]).

sort_disk_log(InputFile, OutputFile) ->
    {ok, ReadLog} = open_disk_log(erlang:timestamp(), InputFile, read_only),
    _ = file:delete(OutputFile),
    {ok, WriteLog} = open_disk_log(erlang:timestamp(), OutputFile, read_write),
    Input = sort_disk_log_input(ReadLog),
    Output = sort_disk_log_output(WriteLog),
    try
        file_sorter:sort(Input, Output, [{format, term}, {tmpdir, tmp_dir()}])
    after
        ok = disk_log:close(ReadLog),
        ok = disk_log:close(WriteLog)
    end.

sort_disk_log_input(ReadLog) ->
    sort_disk_log_input(ReadLog, start).

sort_disk_log_input(ReadLog, Cont) ->
    fun(close) ->
            ok;
       (read) ->
            case disk_log:chunk(ReadLog, Cont) of
                {error, Reason} ->
                    {error, Reason};
                {Cont2, Terms} ->
                    {Terms, sort_disk_log_input(ReadLog, Cont2)};
                {Cont2, Terms, _Badbytes} ->
                    {Terms, sort_disk_log_input(ReadLog, Cont2)};
                eof ->
                    end_of_input
            end
    end.

sort_disk_log_output(WriteLog) ->
    sort_disk_log_output(WriteLog, 1).

sort_disk_log_output(WriteLog, Count) ->
    fun(close) ->
            ok;
       (Terms) ->
            %% Typical length of terms is on the order of 1-1500
            %% e.g. [{Bucket1, Key1, missing|remote_missing|different}, ...]
            if Count rem 100 == 0 ->
                    disk_log:log_terms(WriteLog, Terms);
               true ->
                    disk_log:alog_terms(WriteLog, Terms)
            end,
            sort_disk_log_output(WriteLog, Count + 1)
    end.

fold_disk_log(Fun, Acc, DiskLog) ->
    fold_disk_log(disk_log:chunk(DiskLog, start), Fun, Acc, DiskLog).

-ifndef('21.0').
fold_disk_log(eof, _Fun, Acc, _DiskLog) ->
    Acc;
fold_disk_log({Cont, Terms}, Fun, Acc, DiskLog) ->
    Acc2 = try
               lists:foldl(Fun, Acc, Terms)
           catch X:Y ->
                   lager:error("~s:fold_disk_log: caught ~p ~p @ ~p\n",
                               [?MODULE, X, Y, erlang:get_stacktrace()]),
                   Acc
           end,
    fold_disk_log(disk_log:chunk(DiskLog, Cont), Fun, Acc2, DiskLog).
-else.
fold_disk_log(eof, _Fun, Acc, _DiskLog) ->
    Acc;
fold_disk_log({Cont, Terms}, Fun, Acc, DiskLog) ->
    Acc2 = try
               lists:foldl(Fun, Acc, Terms)
           catch X:Y:Stack ->
                   lager:error("~s:fold_disk_log: caught ~p ~p @ ~p\n",
                               [?MODULE, X, Y, Stack]),
                   Acc
           end,
    fold_disk_log(disk_log:chunk(DiskLog, Cont), Fun, Acc2, DiskLog).
-endif.

tmp_dir() ->
    PDD = app_helper:get_env(riak_core, platform_data_dir, "/tmp"),
    TmpDir = filename:join(PDD, ?MODULE),
    TmpCanary = filename:join(TmpDir, "canary"),
    ok = filelib:ensure_dir(TmpCanary),
    TmpDir.
