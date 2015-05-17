%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc the local view of the cluster's ring configuration
%%
%% Numerous processes concurrently read and access the ring in a
%% variety of time sensitive code paths. To make this efficient,
%% `riak_core' uses `mochiglobal' which exploits the Erlang constant
%% pool to provide constant-time access to the ring without needing
%% to copy data into individual process heaps.
%%
%% However, updating a `mochiglobal' value is very slow, and becomes slower
%% the larger the item being stored. With large rings, the delay can
%% become too long during periods of high ring churn, where hundreds of
%% ring events are being triggered a second.
%%
%% As of Riak 1.4, `riak_core' uses a hybrid approach to solve this
%% problem. When a ring is first written, it is written to a shared ETS
%% table. If no ring events have occurred for 90 seconds, the ring is
%% then promoted to `mochiglobal'.  This provides fast updates during
%% periods of ring churn, while eventually providing very fast reads
%% after the ring stabilizes. The downside is that reading from the ETS
%% table before promotion is slower than `mochiglobal', and requires
%% copying the ring into individual process heaps.
%%
%% To alleviate the slow down while in the ETS phase, `riak_core'
%% exploits the fact that most time sensitive operations access the ring
%% in order to read only a subset of its data: bucket properties and
%% partition ownership. Therefore, these pieces of information are
%% extracted from the ring and stored in the ETS table as well to
%% minimize copying overhead. Furthermore, the partition ownership
%% information (represented by the {@link chash} structure) is converted
%% into a binary {@link chashbin} structure before being stored in the
%% ETS table. This `chashbin' structure is fast to copy between processes
%% due to off-heap binary sharing. Furthermore, this structure provides a
%% secondary benefit of being much faster than the traditional `chash'
%% structure for normal operations.
%%
%% As of Riak 1.4, it is therefore recommended that operations that
%% can be performed by directly using the bucket properties API or
%% `chashbin' structure do so using those methods rather than
%% retrieving the ring via `get_my_ring/0' or `get_raw_ring/0'.

-module(riak_core_ring_manager).
-define(RING_KEY, riak_ring).
-behaviour(gen_server).

-export([start_link/0,
         start_link/1,
         get_my_ring/0,
         get_raw_ring/0,
         get_raw_ring_chashbin/0,
         get_chash_bin/0,
         get_ring_id/0,
         get_bucket_meta/1,
         refresh_my_ring/0,
         refresh_ring/2,
         set_my_ring/1,
         write_ringfile/0,
         prune_ringfiles/0,
         read_ringfile/1,
         find_latest_ringfile/0,
         force_update/0,
         do_write_ringfile/1,
         ring_trans/2,
         run_fixups/3,
         set_cluster_name/1,
         stop/0,
         is_stable_ring/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
        terminate/2, code_change/3]).

-record(state, {
        mode,
        raw_ring,
        ring_changed_time,
        inactivity_timer
    }).

-export([setup_ets/1, cleanup_ets/1, set_ring_global/1]). %% For EUnit testing
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(ETS, ets_riak_core_ring_manager).

-define(PROMOTE_TIMEOUT, 90000).

%% ===================================================================
%% Public API
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [live], []).


%% Testing entry point
start_link(test) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [test], []).


%% @spec get_my_ring() -> {ok, riak_core_ring:riak_core_ring()} | {error, Reason}
get_my_ring() ->
    Ring = case riak_core_mochiglobal:get(?RING_KEY) of
               ets ->
                   case ets:lookup(?ETS, ring) of
                       [{_, RingETS}] ->
                           RingETS;
                       _ ->
                           undefined
                   end;
               RingMochi ->
                   RingMochi
           end,
    case Ring of
        Ring when is_tuple(Ring) -> {ok, Ring};
        undefined -> {error, no_ring}
    end.

get_raw_ring() ->
    try
        Ring = ets:lookup_element(?ETS, raw_ring, 2),
        {ok, Ring}
    catch
        _:_ ->
            gen_server:call(?MODULE, get_raw_ring, infinity)
    end.

get_raw_ring_chashbin() ->
    try
        Ring = ets:lookup_element(?ETS, raw_ring, 2),
        {ok, CHBin} = get_chash_bin(),
        {ok, Ring, CHBin}
    catch
        _:_ ->
            gen_server:call(?MODULE, get_raw_ring_chashbin, infinity)
    end.

%% @spec refresh_my_ring() -> ok
refresh_my_ring() ->
    gen_server:call(?MODULE, refresh_my_ring, infinity).

refresh_ring(Node, ClusterName) ->
    gen_server:cast({?MODULE, Node}, {refresh_my_ring, ClusterName}).

%% @spec set_my_ring(riak_core_ring:riak_core_ring()) -> ok
set_my_ring(Ring) ->
    gen_server:call(?MODULE, {set_my_ring, Ring}, infinity).

get_ring_id() ->
    case ets:lookup(?ETS, id) of
        [{_, Id}] ->
            Id;
        _ ->
            {0,0}
    end.

%% @doc Return metadata for the given bucket. If a bucket
%% for the non-default type is provided {error, no_type}
%% is returned when the type does not exist
get_bucket_meta({<<"default">>, Name}) ->
    get_bucket_meta(Name);
get_bucket_meta({_Type, _Name}=Bucket) ->
    %% reads from cluster metadata ets table
    %% these aren't stored in ring manager ever
    riak_core_bucket:get_bucket(Bucket);
get_bucket_meta(Bucket) ->
    case ets:lookup(?ETS, {bucket, Bucket}) of
        [] ->
            undefined;
        [{_, undefined}] ->
            undefined;
        [{_, Meta}] ->
            {ok, Meta}
    end.

%% @doc Return the {@link chashbin} generated from the current ring
get_chash_bin() ->
    case ets:lookup(?ETS, chashbin) of
        [{chashbin, CHBin}] ->
            {ok, CHBin};
        _ ->
            {error, no_ring}
    end.

%% @spec write_ringfile() -> ok
write_ringfile() ->
    gen_server:cast(?MODULE, write_ringfile).

ring_trans(Fun, Args) ->
    gen_server:call(?MODULE, {ring_trans, Fun, Args}, infinity).

set_cluster_name(Name) ->
    gen_server:call(?MODULE, {set_cluster_name, Name}, infinity).

is_stable_ring() ->
    gen_server:call(?MODULE, is_stable_ring, infinity).

%% @doc Exposed for support/debug purposes. Forces the node to change its
%%      ring in a manner that will trigger reconciliation on gossip.
force_update() ->
    ring_trans(
      fun(Ring, _) ->
              NewRing = riak_core_ring:update_member_meta(node(), Ring, node(),
                                                          unused, now()),
              {new_ring, NewRing}
      end, []),
    ok.

do_write_ringfile(Ring) ->
    case ring_dir() of
        "<nostore>" -> nop;
        Dir ->
            {{Year, Month, Day},{Hour, Minute, Second}} = calendar:universal_time(),
            TS = io_lib:format(".~B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B",
                               [Year, Month, Day, Hour, Minute, Second]),
            Cluster = app_helper:get_env(riak_core, cluster_name),
            FN = Dir ++ "/riak_core_ring." ++ Cluster ++ TS,
            do_write_ringfile(Ring, FN)
    end.

do_write_ringfile(Ring, FN) ->
    ok = filelib:ensure_dir(FN),
    try
        ok = riak_core_util:replace_file(FN, term_to_binary(Ring))
    catch
        _:Err ->
            lager:error("Unable to write ring to \"~s\" - ~p\n", [FN, Err]),
            {error,Err}
    end.


%% @spec find_latest_ringfile() -> string()
find_latest_ringfile() ->
    Dir = ring_dir(),
    case file:list_dir(Dir) of
        {ok, Filenames} ->
            Cluster = app_helper:get_env(riak_core, cluster_name),
            Timestamps = [list_to_integer(TS) || {"riak_core_ring", C1, TS} <-
                                                     [list_to_tuple(string:tokens(FN, ".")) || FN <- Filenames],
                                                 C1 =:= Cluster],
            SortedTimestamps = lists:reverse(lists:sort(Timestamps)),
            case SortedTimestamps of
                [Latest | _] ->
                    {ok, Dir ++ "/riak_core_ring." ++ Cluster ++ "." ++ integer_to_list(Latest)};
                _ ->
                    {error, not_found}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @spec read_ringfile(string()) -> riak_core_ring:riak_core_ring() | {error, any()}
read_ringfile(RingFile) ->
    case file:read_file(RingFile) of
        {ok, Binary} ->
            binary_to_term(Binary);
        {error, Reason} -> {error, Reason}
    end.

%% @spec prune_ringfiles() -> ok | {error, Reason}
prune_ringfiles() ->
    case ring_dir() of
        "<nostore>" -> ok;
        Dir ->
            Cluster = app_helper:get_env(riak_core, cluster_name),
            case file:list_dir(Dir) of
                {error,enoent} -> ok;
                {error, Reason} ->
                    {error, Reason};
                {ok, []} -> ok;
                {ok, Filenames} ->
                    Timestamps = [TS || {"riak_core_ring", C1, TS} <-
                     [list_to_tuple(string:tokens(FN, ".")) || FN <- Filenames],
                                        C1 =:= Cluster],
                    if Timestamps /= [] ->
                            %% there are existing ring files
                            TSPat = [io_lib:fread("~4d~2d~2d~2d~2d~2d",TS) ||
                                        TS <- Timestamps],
                            TSL = lists:reverse(lists:sort([TS ||
                                                               {ok,TS,[]} <- TSPat])),
                            Keep = prune_list(TSL),
                            KeepTSs = [lists:flatten(
                                         io_lib:format(
                                           "~B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B",K))
                                       || K <- Keep],
                            DelFNs = [Dir ++ "/" ++ FN || FN <- Filenames,
                                                          lists:all(fun(TS) ->
                                                                            string:str(FN,TS)=:=0
                                                                    end, KeepTSs)],
                            _ = [file:delete(DelFN) || DelFN <- DelFNs],
                            ok;
                       true ->
                            %% directory wasn't empty, but there are no ring
                            %% files in it
                            ok
                    end
            end
    end.


%% @private (only used for test instances)
stop() ->
    gen_server:cast(?MODULE, stop).


%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([Mode]) ->
    setup_ets(Mode),
    Ring = reload_ring(Mode),
    State = set_ring(Ring, #state{mode = Mode}),
    riak_core_ring_events:ring_update(Ring),
    {ok, State}.

reload_ring(test) ->
    riak_core_ring:fresh(16,node());
reload_ring(live) ->
    case riak_core_ring_manager:find_latest_ringfile() of
        {ok, RingFile} ->
            case riak_core_ring_manager:read_ringfile(RingFile) of
                {error, Reason} ->
                    lager:critical("Failed to read ring file: ~p",
                                   [lager:posix_error(Reason)]),
                    throw({error, Reason});
                Ring ->
                    Ring
            end;
        {error, not_found} ->
            lager:warning("No ring file available."),
            riak_core_ring:fresh();
        {error, Reason} ->
            lager:critical("Failed to load ring file: ~p",
                           [lager:posix_error(Reason)]),
            throw({error, Reason})
    end.

handle_call(get_raw_ring, _From, #state{raw_ring=Ring} = State) ->
    {reply, {ok, Ring}, State};
handle_call(get_raw_ring_chashbin, _From, #state{raw_ring=Ring} = State) ->
    {ok, CHBin} = get_chash_bin(),
    {reply, {ok, Ring, CHBin}, State};
handle_call({set_my_ring, RingIn}, _From, State) ->
    Ring = riak_core_ring:upgrade(RingIn),
    State2 = prune_write_notify_ring(Ring, State),
    {reply,ok,State2};
handle_call(refresh_my_ring, _From, State) ->
    %% This node is leaving the cluster so create a fresh ring file
    FreshRing = riak_core_ring:fresh(),
    State2 = set_ring(FreshRing, State),
    %% Make sure the fresh ring gets written before stopping
    ok = do_write_ringfile(FreshRing),

    %% Handoff is complete and fresh ring is written
    %% so we can safely stop now.
    riak_core:stop("node removal completed, exiting."),

    {reply,ok,State2};
handle_call({ring_trans, Fun, Args}, _From, State=#state{raw_ring=Ring}) ->
    case catch Fun(Ring, Args) of
        {new_ring, NewRing} ->
            State2 = prune_write_notify_ring(NewRing, State),
            riak_core_gossip:random_recursive_gossip(NewRing),
            {reply, {ok, NewRing}, State2};
        {set_only, NewRing} ->
            State2 = prune_write_ring(NewRing, State),
            {reply, {ok, NewRing}, State2};
        {reconciled_ring, NewRing} ->
            State2 = prune_write_notify_ring(NewRing, State),
            riak_core_gossip:recursive_gossip(NewRing),
            {reply, {ok, NewRing}, State2};
        ignore ->
            {reply, not_changed, State};
        {ignore, Reason} ->
            {reply, {not_changed, Reason}, State};
        Other ->
            lager:error("ring_trans: invalid return value: ~p",
                                   [Other]),
            {reply, not_changed, State}
    end;
handle_call({set_cluster_name, Name}, _From, State=#state{raw_ring=Ring}) ->
    NewRing = riak_core_ring:set_cluster_name(Ring, Name),
    State2 = prune_write_notify_ring(NewRing, State),
    {reply, ok, State2};
handle_call(is_stable_ring, _From, State) ->
    {IsStable, _DeltaMS} = is_stable_ring(State),
    {reply, IsStable, State}.

handle_cast(stop, State) ->
    {stop,normal,State};

handle_cast({refresh_my_ring, ClusterName}, State) ->
    {ok, Ring} = get_my_ring(),
    case riak_core_ring:cluster_name(Ring) of
        ClusterName ->
            handle_cast(refresh_my_ring, State);
        _ ->
            {noreply, State}
    end;
handle_cast(refresh_my_ring, State) ->
    {_, _, State2} = handle_call(refresh_my_ring, undefined, State),
    {noreply, State2};

handle_cast(write_ringfile, test) ->
    {noreply,test};

handle_cast(write_ringfile, State=#state{raw_ring=Ring}) ->
    ok = do_write_ringfile(Ring),
    {noreply,State}.


handle_info(inactivity_timeout, State) ->
    case is_stable_ring(State) of
        {true,DeltaMS} ->
            lager:debug("Promoting ring after ~p", [DeltaMS]),
            promote_ring(),
            State2 = State#state{inactivity_timer=undefined},
            {noreply, State2};
        {false,DeltaMS} ->
            Remaining = ?PROMOTE_TIMEOUT - DeltaMS,
            State2 = set_timer(Remaining, State),
            {noreply, State2}
    end;
handle_info(_Info, State) ->
    {noreply, State}.


%% @private
terminate(_Reason, _State) ->
    ok.


%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ===================================================================
%% Internal functions
%% ===================================================================

ring_dir() ->
    case app_helper:get_env(riak_core, ring_state_dir) of
        undefined ->
            filename:join(app_helper:get_env(riak_core, platform_data_dir, "data"), "ring");
        D ->
            D
    end.

prune_list([X|Rest]) ->
    lists:usort(lists:append([[X],back(1,X,Rest),back(2,X,Rest),
                  back(3,X,Rest),back(4,X,Rest),back(5,X,Rest)])).
back(_N,_X,[]) -> [];
back(N,X,[H|T]) ->
    case lists:nth(N,X) =:= lists:nth(N,H) of
        true -> back(N,X,T);
        false -> [H]
    end.

%% @private
run_fixups([], _Bucket, BucketProps) ->
    BucketProps;
run_fixups([{App, Fixup}|T], BucketName, BucketProps) ->
    BP = try Fixup:fixup(BucketName, BucketProps) of
        {ok, NewBucketProps} ->
            NewBucketProps;
        {error, Reason} ->
            lager:error("Error while running bucket fixup module "
                "~p from application ~p on bucket ~p: ~p", [Fixup, App,
                    BucketName, Reason]),
            BucketProps
    catch
        What:Why ->
            lager:error("Crash while running bucket fixup module "
                "~p from application ~p on bucket ~p : ~p:~p", [Fixup, App,
                    BucketName, What, Why]),
            BucketProps
    end,
    run_fixups(T, BucketName, BP).

set_ring(Ring, State) ->
    set_ring_global(Ring),
    Now = os:timestamp(),
    State2 = State#state{raw_ring=Ring, ring_changed_time=Now},
    State3 = maybe_set_timer(?PROMOTE_TIMEOUT, State2),
    State3.

maybe_set_timer(Duration, State=#state{inactivity_timer=undefined}) ->
    set_timer(Duration, State);
maybe_set_timer(_Duration, State) ->
    State.

set_timer(Duration, State) ->
    Timer = erlang:send_after(Duration, self(), inactivity_timeout),
    State#state{inactivity_timer=Timer}.

setup_ets(Mode) ->
    %% Destroy prior version of ETS table. This is necessary for certain
    %% eunit tests, but is unneeded for normal Riak operation.
    catch ets:delete(?ETS),
    Access = case Mode of
                 live -> protected;
                 test -> public
             end,
    ?ETS = ets:new(?ETS, [named_table, Access, {read_concurrency, true}]),
    Id = reset_ring_id(),
    ets:insert(?ETS, [{changes, 0}, {promoted, 0}, {id, Id}]),
    ok.

cleanup_ets(test) ->
    ets:delete(?ETS).

reset_ring_id() ->
    %% Maintain ring id epoch using mochiglobal to ensure ring id remains
    %% monotonic even if the riak_core_ring_manager crashes and restarts
    Epoch = case riak_core_mochiglobal:get(riak_ring_id_epoch) of
                undefined ->
                    0;
                Value ->
                    Value
            end,
    riak_core_mochiglobal:put(riak_ring_id_epoch, Epoch + 1),
    {Epoch + 1, 0}.

%% Set the ring in mochiglobal/ETS.  Exported during unit testing
%% to make test setup simpler - no need to spin up a riak_core_ring_manager
%% process.
set_ring_global(Ring) ->
    DefaultProps = case application:get_env(riak_core, default_bucket_props) of
        {ok, Val} ->
            Val;
        _ ->
            []
    end,
    %% run fixups on the ring before storing it in mochiglobal
    FixedRing = case riak_core:bucket_fixups() of
        [] -> Ring;
        Fixups ->
            Buckets = riak_core_ring:get_buckets(Ring),
            lists:foldl(
                fun(Bucket, AccRing) ->
                        BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
                        %% Merge anything in the default properties but not in
                        %% the bucket's properties. This is to ensure default
                        %% properties added after the bucket is created are
                        %% inherited to the bucket.
                        MergedProps = riak_core_bucket:merge_props(
                            BucketProps, DefaultProps),

                        %% fixup the ring
                        NewBucketProps = run_fixups(Fixups, Bucket, MergedProps),
                        %% update the bucket in the ring
                        riak_core_ring:update_meta({bucket,Bucket},
                            NewBucketProps,
                            AccRing)
                end, Ring, Buckets)
    end,
    %% Mark ring as tainted to check if it is ever leaked over gossip or
    %% relied upon for any non-local ring operations.
    TaintedRing = riak_core_ring:set_tainted(FixedRing),

    %% Extract bucket properties and place into ETS table. We want all bucket
    %% additions, modifications, and deletions to appear in a single atomic
    %% operation. Since ETS does not provide a means to change + delete
    %% multiple values in a single operation, we emulate the deletion by
    %% overwriting all deleted buckets with the "undefined" atom that has
    %% special meaning in `riak_core_bucket:get_bucket_props/2`. We then
    %% cleanup these values in a subsequent `ets:match_delete`.
    OldBuckets = ets:select(?ETS, [{{{bucket, '$1'}, '_'}, [], ['$1']}]),
    BucketDefaults = [{{bucket, Bucket}, undefined} || Bucket <- OldBuckets],
    BucketMeta =
        [{{bucket, Bucket}, Meta}
         || Bucket <- riak_core_ring:get_buckets(TaintedRing),
            {ok,Meta} <- [riak_core_ring:get_meta({bucket, Bucket}, TaintedRing)]],
    BucketMeta2 = lists:ukeysort(1, BucketMeta ++ BucketDefaults),
    CHBin = chashbin:create(riak_core_ring:chash(TaintedRing)),
    {Epoch, Id} = ets:lookup_element(?ETS, id, 2),
    Actions = [{ring, TaintedRing},
               {raw_ring, Ring},
               {id, {Epoch,Id+1}},
               {chashbin, CHBin} | BucketMeta2],
    ets:insert(?ETS, Actions),
    ets:match_delete(?ETS, {{bucket, '_'}, undefined}),
    case riak_core_mochiglobal:get(?RING_KEY) of
        ets ->
            ok;
        _ ->
            riak_core_mochiglobal:put(?RING_KEY, ets)
    end,
    ok.

promote_ring() ->
    {ok, Ring} = get_my_ring(),
    riak_core_mochiglobal:put(?RING_KEY, Ring).

%% Persist a new ring file, set the global value and notify any listeners
prune_write_notify_ring(Ring, State) ->
    State2 = prune_write_ring(Ring, State),
    riak_core_ring_events:ring_update(Ring),
    State2.

prune_write_ring(Ring, State) ->
    riak_core_ring:check_tainted(Ring, "Error: Persisting tainted ring"),
    ok = riak_core_ring_manager:prune_ringfiles(),
    _ = do_write_ringfile(Ring),
    State2 = set_ring(Ring, State),
    State2.

is_stable_ring(#state{ring_changed_time=Then}) ->
    DeltaUS = erlang:max(0, timer:now_diff(os:timestamp(), Then)),
    DeltaMS = DeltaUS div 1000,
    IsStable = DeltaMS >= ?PROMOTE_TIMEOUT,
    {IsStable, DeltaMS}.

%% ===================================================================
%% Unit tests
%% ===================================================================
-ifdef(TEST).

back_test() ->
    X = [1,2,3],
    List1 = [[1,2,3],[4,2,3], [7,8,3], [11,12,13], [1,2,3]],
    List2 = [[7,8,9], [1,2,3]],
    List3 = [[1,2,3]],
    ?assertEqual([[4,2,3]], back(1, X, List1)),
    ?assertEqual([[7,8,9]], back(1, X, List2)),
    ?assertEqual([], back(1, X, List3)),
    ?assertEqual([[7,8,3]], back(2, X, List1)),
    ?assertEqual([[11,12,13]], back(3, X, List1)).

prune_list_test() ->
    TSList1 = [[2011,2,28,16,32,16],[2011,2,28,16,32,36],[2011,2,28,16,30,27],[2011,2,28,16,32,16],[2011,2,28,16,32,36]],
    TSList2 = [[2011,2,28,16,32,36],[2011,2,28,16,31,16],[2011,2,28,16,30,27],[2011,2,28,16,32,16],[2011,2,28,16,32,36]],
    PrunedList1 = [[2011,2,28,16,30,27],[2011,2,28,16,32,16]],
    PrunedList2 = [[2011,2,28,16,31,16],[2011,2,28,16,32,36]],
    ?assertEqual(PrunedList1, prune_list(TSList1)),
    ?assertEqual(PrunedList2, prune_list(TSList2)).

set_ring_global_test() ->
    setup_ets(test),
    application:set_env(riak_core,ring_creation_size, 4),
    Ring = riak_core_ring:fresh(),
    set_ring_global(Ring),
    promote_ring(),
    ?assert(riak_core_ring:nearly_equal(Ring, riak_core_mochiglobal:get(?RING_KEY))),
    cleanup_ets(test).

set_my_ring_test() ->
    setup_ets(test),
    application:set_env(riak_core,ring_creation_size, 4),
    Ring = riak_core_ring:fresh(),
    set_ring_global(Ring),
    {ok, MyRing} = get_my_ring(),
    ?assert(riak_core_ring:nearly_equal(Ring, MyRing)),
    cleanup_ets(test).

refresh_my_ring_test() ->
    setup_ets(test),
    Core_Settings = [{ring_creation_size, 4},
                     {ring_state_dir, "/tmp"},
                     {cluster_name, "test"}],
    [begin
         put({?MODULE,AppKey}, app_helper:get_env(riak_core, AppKey)),
         ok = application:set_env(riak_core, AppKey, Val)
     end || {AppKey, Val} <- Core_Settings],
    riak_core_ring_events:start_link(),
    riak_core_ring_manager:start_link(test),
    riak_core_vnode_sup:start_link(),
    riak_core_vnode_master:start_link(riak_core_vnode),
    riak_core_test_util:setup_mockring1(),
    ?assertEqual(ok, riak_core_ring_manager:refresh_my_ring()),
    riak_core_ring_manager:stop(),
    %% Cleanup the ring file created for this test
    {ok, RingFile} = find_latest_ringfile(),
    file:delete(RingFile),
    [ok = application:set_env(riak_core, AppKey, get({?MODULE, AppKey}))
     || {AppKey, _Val} <- Core_Settings],
    ok.

-define(TEST_RINGDIR, "ring_manager_eunit").
-define(TEST_RINGFILE, (?TEST_RINGDIR ++ "/test.ring")).
-define(TMP_RINGFILE,  (?TEST_RINGFILE ++ ".tmp")).
do_write_ringfile_test() ->
    %% Make sure no data exists from previous runs
    file:change_mode(?TMP_RINGFILE, 8#00644),
    file:delete(?TMP_RINGFILE),
    file:change_mode(?TEST_RINGFILE, 8#00644),
    file:delete(?TEST_RINGFILE),

    %% Check happy path
    GenR = fun(Name) -> riak_core_ring:fresh(64, Name) end,
    ?assertEqual(ok, do_write_ringfile(GenR(happy), ?TEST_RINGFILE)),

    %% Check write fails (create .tmp file with no write perms)
    ok = file:write_file(?TMP_RINGFILE, <<"no write for you">>),
    ok = file:change_mode(?TMP_RINGFILE, 8#00444),
    ?assertMatch({error,_}, do_write_ringfile(GenR(tmp_perms), ?TEST_RINGFILE)),
    ok = file:change_mode(?TMP_RINGFILE, 8#00644),
    ok = file:delete(?TMP_RINGFILE),

    %% Check rename fails
    ok = file:change_mode(?TEST_RINGDIR, 8#00444),
    ?assertMatch({error,_}, do_write_ringfile(GenR(ring_perms), ?TEST_RINGFILE)),
    ok = file:change_mode(?TEST_RINGDIR, 8#00755).

is_stable_ring_test() ->
    {A,B,C} = Now = os:timestamp(),
    TimeoutSecs = ?PROMOTE_TIMEOUT div 1000,
    Within = {A, B - (TimeoutSecs div 2), C},
    Outside = {A, B - (TimeoutSecs + 1), C},
    ?assertMatch({true,_},is_stable_ring(#state{ring_changed_time={0,0,0}})),
    ?assertMatch({true,_},is_stable_ring(#state{ring_changed_time=Outside})),
    ?assertMatch({false,_},is_stable_ring(#state{ring_changed_time=Within})),
    ?assertMatch({false,_},is_stable_ring(#state{ring_changed_time=Now})).

-endif.
