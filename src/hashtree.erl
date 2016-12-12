%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc
%% This module implements a persistent, on-disk hash tree that is used
%% predominately for active anti-entropy exchange in Riak. The tree consists
%% of two parts, a set of unbounded on-disk segments and a fixed size hash
%% tree (that may be on-disk or in-memory) constructed over these segments.
%%
%% A graphical description of this design can be found in: docs/hashtree.md
%%
%% Each segment logically represents an on-disk list of (key, hash) pairs.
%% Whereas the hash tree is represented as a set of levels and buckets, with a
%% fixed width (or fan-out) between levels that determines how many buckets of
%% a child level are grouped together and hashed to represent a bucket at the
%% parent level. Each leaf in the tree corresponds to a hash of one of the
%% on-disk segments. For example, a tree with a width of 4 and 16 segments
%% would look like the following:
%%
%% level   buckets
%% 1:      [0]
%% 2:      [0 1 2 3]
%% 3:      [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15]
%%
%% With each bucket entry of the form ``{bucket-id, hash}'', eg. ``{0,
%% binary()}''.  The hash for each of the entries at level 3 would come from
%% one of the 16 segments, while the hashes for entries at level 1 and 2 are
%% derived from the lower levels.
%%
%% Specifically, the bucket entries in level 2 would come from level 3:
%%   0: hash([ 0  1  2  3])
%%   1: hash([ 4  5  6  7])
%%   2: hash([ 8  9 10 11])
%%   3: hash([12 13 14 15])
%%
%% And the bucket entries in level 1 would come from level 2:
%%   1: hash([hash([ 0  1  2  3])
%%            hash([ 4  5  6  7])
%%            hash([ 8  9 10 11])
%%            hash([12 13 14 15])])
%%
%% When a (key, hash) pair is added to the tree, the key is hashed to
%% determine which segment it belongs to and inserted/upserted into the
%% segment. Rather than update the hash tree on every insert, a dirty bit is
%% set to note that a given segment has changed. The hashes are then updated
%% in bulk before performing a tree exchange
%%
%% To update the hash tree, the code iterates over each dirty segment,
%% building a list of (key, hash) pairs. A hash is computed over this list,
%% and the leaf node in the hash tree corresponding to the given segment is
%% updated.  After iterating over all dirty segments, and thus updating all
%% leaf nodes, the update then continues to update the tree bottom-up,
%% updating only paths that have changed. As designed, the update requires a
%% single sparse scan over the on-disk segments and a minimal traversal up the
%% hash tree.
%%
%% The heavy-lifting of this module is provided by LevelDB. What is logically
%% viewed as sorted on-disk segments is in reality a range of on-disk
%% (segment, key, hash) values written to LevelDB. Each insert of a (key,
%% hash) pair therefore corresponds to a single LevelDB write (no read
%% necessary). Likewise, the update operation is performed using LevelDB
%% iterators.
%%
%% When used for active anti-entropy in Riak, the hash tree is built once and
%% then updated in real-time as writes occur. A key design goal is to ensure
%% that adding (key, hash) pairs to the tree is non-blocking, even during a
%% tree update or a tree exchange. This is accomplished using LevelDB
%% snapshots. Inserts into the tree always write directly to the active
%% LevelDB instance, however updates and exchanges operate over a snapshot of
%% the tree.
%%
%% In order to improve performance, writes are buffered in memory and sent
%% to LevelDB using a single batch write. Writes are flushed whenever the
%% buffer becomes full, as well as before updating the hashtree.
%%
%% Tree exchange is provided by the ``compare/4'' function.
%% The behavior of this function is determined through a provided function
%% that implements logic to get buckets and segments for a given remote tree,
%% as well as a callback invoked as key differences are determined. This
%% generic interface allows for tree exchange to be implemented in a variety
%% of ways, including directly against to local hash tree instances, over
%% distributed Erlang, or over a custom protocol over a TCP socket. See
%% ``local_compare/2'' and ``do_remote/1'' for examples (-ifdef(TEST) only).

-module(hashtree).
-export([new/0,
         new/2,
         new/3,
         insert/3,
         insert/4,
         estimate_keys/1,
         delete/2,
         update_tree/1,
         update_snapshot/1,
         update_perform/1,
         rehash_tree/1,
         flush_buffer/1,
         close/1,
         destroy/1,
         read_meta/2,
         write_meta/3,
         compare/4,
         top_hash/1,
         get_bucket/3,
         key_hashes/2,
         levels/1,
         segments/1,
         width/1,
         mem_levels/1,
         path/1,
         next_rebuild/1,
         set_next_rebuild/2,
         mark_open_empty/2,
         mark_open_and_check/2,
         mark_clean_close/2]).
-export([compare2/4]).
-export([multi_select_segment/3, safe_decode/1]).

-ifdef(namespaced_types).
-type hashtree_dict() :: dict:dict().
-type hashtree_array() :: array:array().
-else.
-type hashtree_dict() :: dict().
-type hashtree_array() :: array().
-endif.

-define(ALL_SEGMENTS, ['*', '*']).
-define(BIN_TO_INT(B), list_to_integer(binary_to_list(B))).

-ifdef(TEST).
-export([fake_close/1, local_compare/2, local_compare1/2]).
-export([run_local/0,
         run_local/1,
         run_concurrent_build/0,
         run_concurrent_build/1,
         run_concurrent_build/2,
         run_multiple/2,
         run_remote/0,
         run_remote/1]).
-endif. % TEST

-ifdef(EQC).
-export([prop_correct/0]).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(TOMBSTONE, <<"TS">>).

-define(NUM_SEGMENTS, (1024*1024)).
-define(WIDTH, 1024).
-define(MEM_LEVELS, 0).

-define(NUM_KEYS_REQUIRED, 1000).

-type tree_id_bin() :: <<_:176>>.
-type segment_bin() :: <<_:256, _:_*8>>.
-type bucket_bin()  :: <<_:320>>.
-type meta_bin()    :: <<_:8, _:_*8>>.

-type proplist() :: proplists:proplist().
-type orddict() :: orddict:orddict().
-type index() :: non_neg_integer().
-type index_n() :: {index(), pos_integer()}.

-type keydiff() :: {missing | remote_missing | different, binary()}.

-type remote_fun() :: fun((get_bucket | key_hashes | start_exchange_level |
                           start_exchange_segments | init | final,
                           {integer(), integer()} | integer() | term()) -> any()).

-type acc_fun(Acc) :: fun(([keydiff()], Acc) -> Acc).

-type select_fun(T) :: fun((orddict()) -> T).

-type next_rebuild() :: full | incremental.

-record(state, {id                 :: tree_id_bin(),
                index              :: index(),
                levels             :: pos_integer(),
                segments           :: pos_integer(),
                width              :: pos_integer(),
                mem_levels         :: integer(),
                tree               :: hashtree_dict(),
                ref                :: term(),
                path               :: string(),
                itr                :: term(),
                next_rebuild       :: next_rebuild(),
                write_buffer       :: [{put, binary(), binary()} |
                                       {delete, binary()}],
                write_buffer_count :: integer(),
                dirty_segments     :: hashtree_array()
               }).

-record(itr_state, {itr                :: term(),
                    id                 :: tree_id_bin(),
                    current_segment    :: '*' | integer(),
                    remaining_segments :: ['*' | integer()],
                    acc_fun            :: fun(([{binary(),binary()}]) -> any()),
                    segment_acc        :: [{binary(), binary()}],
                    final_acc          :: [{integer(), any()}],
                    prefetch=false     :: boolean()
                   }).

-opaque hashtree() :: #state{}.
-export_type([hashtree/0,
              tree_id_bin/0,
              keydiff/0,
              remote_fun/0,
              acc_fun/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec new() -> hashtree().
new() ->
    new({0,0}).

-spec new({index(), tree_id_bin() | non_neg_integer()}) -> hashtree().
new(TreeId) ->
    State = new_segment_store([], #state{}),
    new(TreeId, State, []).

-spec new({index(), tree_id_bin() | non_neg_integer()}, proplist()) -> hashtree();
         ({index(), tree_id_bin() | non_neg_integer()}, hashtree()) -> hashtree().
new(TreeId, Options) when is_list(Options) ->
    State = new_segment_store(Options, #state{}),
    new(TreeId, State, Options);
new(TreeId, LinkedStore = #state{}) ->
    new(TreeId, LinkedStore, []).

-spec new({index(), tree_id_bin() | non_neg_integer()},
          hashtree(),
          proplist()) -> hashtree().
new({Index,TreeId}, LinkedStore, Options) ->
    NumSegments = proplists:get_value(segments, Options, ?NUM_SEGMENTS),
    Width = proplists:get_value(width, Options, ?WIDTH),
    MemLevels = proplists:get_value(mem_levels, Options, ?MEM_LEVELS),
    NumLevels = erlang:trunc(math:log(NumSegments) / math:log(Width)) + 1,
    State = #state{id=encode_id(TreeId),
                   index=Index,
                   levels=NumLevels,
                   segments=NumSegments,
                   width=Width,
                   mem_levels=MemLevels,
                   %% dirty_segments=gb_sets:new(),
                   dirty_segments=bitarray_new(NumSegments),
                   next_rebuild=full,
                   write_buffer=[],
                   write_buffer_count=0,
                   tree=dict:new()},
    State2 = share_segment_store(State, LinkedStore),
    State2.

-spec close(hashtree()) -> hashtree().
close(State) ->
    close_iterator(State#state.itr),
    catch eleveldb:close(State#state.ref),
    State#state{itr=undefined}.

close_iterator(Itr) ->
    try
        eleveldb:iterator_close(Itr)
    catch
        _:_ ->
            ok
    end.

-spec destroy(string() | hashtree()) -> ok | hashtree().
destroy(Path) when is_list(Path) ->
    ok = eleveldb:destroy(Path, []);
destroy(State) ->
    %% Assumption: close was already called on all hashtrees that
    %%             use this LevelDB instance,
    ok = eleveldb:destroy(State#state.path, []),
    State.

-spec insert(binary(), binary(), hashtree()) -> hashtree().
insert(Key, ObjHash, State) ->
    insert(Key, ObjHash, State, []).

-spec insert(binary(), binary() | tombstone, hashtree(), proplist()) -> hashtree().
insert(Key, tombstone, State, Opts) ->
    insert(Key, ?TOMBSTONE, State, Opts);

insert(Key, ObjHash, State, Opts) ->
    Hash = erlang:phash2(Key),
    Segment = Hash rem State#state.segments,
    HKey = encode(State#state.id, Segment, Key),
    case should_insert(HKey, Opts, State) of
        true ->
            State2 = enqueue_action({put, HKey, ObjHash}, State),
            Dirty = bitarray_set(Segment, State2#state.dirty_segments),
            State2#state{dirty_segments=Dirty};
        false ->
            State
    end.

enqueue_action(Action, State) ->
    WBuffer = [Action|State#state.write_buffer],
    WCount = State#state.write_buffer_count + 1,
    State2 = State#state{write_buffer=WBuffer,
                         write_buffer_count=WCount},
    State3 = maybe_flush_buffer(State2),
    State3.

maybe_flush_buffer(State=#state{write_buffer_count=WCount}) ->
    Threshold = 200,
    case WCount > Threshold of
        true ->
            flush_buffer(State);
        false ->
            State
    end.

flush_buffer(State=#state{write_buffer=[], write_buffer_count=0}) ->
    State;
flush_buffer(State=#state{write_buffer=WBuffer}) ->
    %% Write buffer is built backwards, reverse to build update list
    Updates = lists:reverse(WBuffer),
    ok = eleveldb:write(State#state.ref, Updates, []),
    State#state{write_buffer=[],
                write_buffer_count=0}.

-spec delete(binary(), hashtree()) -> hashtree().
delete(Key, State) ->
    Hash = erlang:phash2(Key),
    Segment = Hash rem State#state.segments,
    HKey = encode(State#state.id, Segment, Key),
    State2 = enqueue_action({delete, HKey}, State),
    %% Dirty = gb_sets:add_element(Segment, State2#state.dirty_segments),
    Dirty = bitarray_set(Segment, State2#state.dirty_segments),
    State2#state{dirty_segments=Dirty}.

-spec should_insert(segment_bin(), proplist(), hashtree()) -> boolean().
should_insert(HKey, Opts, State) ->
    IfMissing = proplists:get_value(if_missing, Opts, false),
    case IfMissing of
        true ->
            %% Only insert if object does not already exist
            %% TODO: Use bloom filter so we don't always call get here
            case eleveldb:get(State#state.ref, HKey, []) of
                not_found ->
                    true;
                _ ->
                    false
            end;
        _ ->
            true
    end.

-spec update_snapshot(hashtree()) -> {hashtree(), hashtree()}.
update_snapshot(State=#state{segments=NumSegments}) ->
    State2 = flush_buffer(State),
    SnapState = snapshot(State2),
    State3 = SnapState#state{dirty_segments=bitarray_new(NumSegments)},
    {SnapState, State3}.

-spec update_tree(hashtree()) -> hashtree().
update_tree(State) ->
    State2 = flush_buffer(State),
    State3 = snapshot(State2),
    update_perform(State3).

-spec update_perform(hashtree()) -> hashtree().
update_perform(State=#state{dirty_segments=Dirty, segments=NumSegments}) ->
    NextRebuild = State#state.next_rebuild,
    Segments = case NextRebuild of
                   full ->
                       ?ALL_SEGMENTS;
                   incremental ->
                       %% gb_sets:to_list(Dirty),
                       bitarray_to_list(Dirty)
               end,
    State2 = maybe_clear_buckets(NextRebuild, State),
    State3 = update_tree(Segments, State2),
    %% State2#state{dirty_segments=gb_sets:new()}
    State3#state{dirty_segments=bitarray_new(NumSegments),
                 next_rebuild=incremental}.

%% Clear buckets if doing a full rebuild
maybe_clear_buckets(full, State) ->
    clear_buckets(State);
maybe_clear_buckets(incremental, State) ->
    State.

%% Fold over the 'live' data (outside of the snapshot), removing all
%% bucket entries for the tree.
clear_buckets(State=#state{id=Id, ref=Ref}) ->
    Fun = fun({K,_V},Acc) ->
                  try
                      case decode_bucket(K) of
                          {Id, _, _} ->
                              ok = eleveldb:delete(Ref, K, []),
                              Acc + 1;
                          _ ->
                              throw({break, Acc})
                      end
                  catch
                      _:_ -> % not a decodable bucket
                          throw({break, Acc})
                  end
          end,
    Opts = [{first_key, encode_bucket(Id, 0, 0)}],
    Removed = try
%hashtree.erl:415: The call eleveldb:fold(Ref::any(),Fun::fun((_,_) -> number()),0,Opts::[{'first_key',<<_:320>>},...]) breaks the contract (db_ref(),fold_fun(),any(),read_options()) -> any()

                  eleveldb:fold(Ref, Fun, 0, Opts)
              catch
                  {break, AccFinal} ->
                      AccFinal
              end,
    lager:debug("Tree ~p cleared ~p segments.\n", [Id, Removed]),

    %% Mark the tree as requiring a full rebuild (will be fixed
    %% reset at end of update_trees) AND dump the in-memory
    %% tree.
    State#state{next_rebuild = full,
                tree = dict:new()}.
            

-spec update_tree([integer()], hashtree()) -> hashtree().
update_tree([], State) ->
    State;
update_tree(Segments, State=#state{next_rebuild=NextRebuild, width=Width,
                                   levels=Levels}) ->
    LastLevel = Levels,
    Hashes = orddict:from_list(hashes(State, Segments)),
    %% Paranoia to make sure all of the hash entries are updated as expected
    lager:debug("segments ~p -> hashes ~p\n", [Segments, Hashes]),
    case Segments == ?ALL_SEGMENTS orelse
        length(Segments) == length(Hashes) of
        true ->
            Groups = group(Hashes, Width),
            update_levels(LastLevel, Groups, State, NextRebuild);
        false ->
            %% At this point the hashes are no longer sufficient to update
            %% the upper trees.  Alternative is to crash here, but that would
            %% lose updates and is the action taken on repair anyway.
            %% Save the customer some pain by doing that now and log.
            %% Enable lager debug tracing with lager:trace_file(hashtree, "/tmp/ht.trace"
            %% to get the detailed segment information.
            lager:warning("Incremental AAE hash was unable to find all required data, "
                          "forcing full rebuild of ~p", [State#state.path]),
            update_perform(State#state{next_rebuild = full})
    end.

-spec rehash_tree(hashtree()) -> hashtree().
rehash_tree(State) ->
    State2 = flush_buffer(State),
    State3 = snapshot(State2),
    rehash_perform(State3).

-spec rehash_perform(hashtree()) -> hashtree().
rehash_perform(State) ->
    Hashes = orddict:from_list(hashes(State, ?ALL_SEGMENTS)),
    case Hashes of
        [] ->
            State;
        _ ->
            Groups = group(Hashes, State#state.width),
            LastLevel = State#state.levels,
            %% Always do a full rebuild on rehash
            NewState = update_levels(LastLevel, Groups, State, full),
            NewState
    end.

%% @doc Mark/clear metadata for tree-id opened/closed.
%%      Set next_rebuild to be incremental.
-spec mark_open_empty(index_n()|binary(), hashtree()) -> hashtree().
mark_open_empty(TreeId, State) when is_binary(TreeId) ->
    State1 = write_meta(TreeId, [{opened, 1}, {closed, 0}], State),
    State1#state{next_rebuild=incremental};
mark_open_empty(TreeId, State) ->
    mark_open_empty(term_to_binary(TreeId), State).

%% @doc Check if shutdown/closing of tree-id was clean/dirty by comparing
%%      `closed' to `opened' metadata count for the hashtree, and,
%%      increment opened count for hashtree-id.
%%
%%
%%      If it was a clean shutdown, set `next_rebuild' to be an incremental one.
%%      Otherwise, if it was a dirty shutdown, set `next_rebuild', instead,
%%      to be a full one.
-spec mark_open_and_check(index_n()|binary(), hashtree()) -> hashtree().
mark_open_and_check(TreeId, State) when is_binary(TreeId) ->
    MetaTerm = read_meta_term(TreeId, [], State),
    OpenedCnt = proplists:get_value(opened, MetaTerm, 0),
    ClosedCnt = proplists:get_value(closed, MetaTerm, -1),
    _ = write_meta(TreeId, lists:keystore(opened, 1, MetaTerm,
                                          {opened, OpenedCnt + 1}), State),
    case ClosedCnt =/= OpenedCnt orelse State#state.mem_levels > 0 of
        true ->
            State#state{next_rebuild = full};
        false ->
            State#state{next_rebuild = incremental}
    end;
mark_open_and_check(TreeId, State) ->
    mark_open_and_check(term_to_binary(TreeId), State).

%% @doc Call on a clean-close to update the meta for a tree-id's `closed' count
%%      to match the current `opened' count, which is checked on new/reopen.
-spec mark_clean_close(index_n()|binary(), hashtree()) -> hashtree().
mark_clean_close(TreeId, State) when is_binary(TreeId) ->
    MetaTerm = read_meta_term(TreeId, [], State),
    OpenedCnt = proplists:get_value(opened, MetaTerm, 0),
    _ = write_meta(TreeId, lists:keystore(closed, 1, MetaTerm,
                                          {closed, OpenedCnt}), State);
mark_clean_close(TreeId, State) ->
    mark_clean_close(term_to_binary(TreeId), State).

-spec top_hash(hashtree()) -> [] | [{0, binary()}].
top_hash(State) ->
    get_bucket(1, 0, State).

compare(Tree, Remote, AccFun, Acc) ->
    compare(1, 0, Tree, Remote, AccFun, Acc).

-spec levels(hashtree()) -> pos_integer().
levels(#state{levels=L}) ->
    L.

-spec segments(hashtree()) -> pos_integer().
segments(#state{segments=S}) ->
    S.

-spec width(hashtree()) -> pos_integer().
width(#state{width=W}) ->
    W.

-spec mem_levels(hashtree()) -> integer().
mem_levels(#state{mem_levels=M}) ->
    M.

-spec path(hashtree()) -> string().
path(#state{path=P}) ->
    P.

-spec next_rebuild(hashtree()) -> next_rebuild().
next_rebuild(#state{next_rebuild=NextRebuild}) ->
    NextRebuild.

-spec set_next_rebuild(hashtree(), next_rebuild()) -> hashtree().
set_next_rebuild(Tree, NextRebuild) ->
    Tree#state{next_rebuild = NextRebuild}.

%% Note: meta is currently a one per file thing, even if there are multiple
%%       trees per file. This is intentional. If we want per tree metadata
%%       this will need to be added as a separate thing.
-spec write_meta(binary(), binary()|term(), hashtree()) -> hashtree().
write_meta(Key, Value, State) when is_binary(Key) and is_binary(Value) ->
    HKey = encode_meta(Key),
    ok = eleveldb:put(State#state.ref, HKey, Value, []),
    State;
write_meta(Key, Value0, State) when is_binary(Key) ->
    Value = term_to_binary(Value0),
    write_meta(Key, Value, State).

-spec read_meta(binary(), hashtree()) -> {ok, binary()} | undefined.
read_meta(Key, State) when is_binary(Key) ->
    HKey = encode_meta(Key),
    case eleveldb:get(State#state.ref, HKey, []) of
        {ok, Value} ->
            {ok, Value};
        _ ->
            undefined
    end.

-spec read_meta_term(binary(), term(), hashtree()) -> term().
read_meta_term(Key, Default, State) when is_binary(Key) ->
    case read_meta(Key, State) of
        {ok, Value} ->
            binary_to_term(Value);
        _ ->
            Default
    end.

%% @doc
%% Estimate number of keys stored in the AAE tree. This is determined
%% by sampling segments to to calculate an estimated keys-per-segment
%% value, which is then multiplied by the number of segments. Segments
%% are sampled until either 1% of segments have been visited or 1000
%% keys have been observed.
%%
%% Note: this function must be called on a tree with a valid iterator,
%%       such as the snapshotted tree returned from update_snapshot/1
%%       or a recently updated tree returned from update_tree/1 (which
%%       internally creates a snapshot). Using update_tree/1 is the best
%%       choice since that ensures segments are updated giving a better
%%       estimate.
-spec estimate_keys(hashtree()) -> {ok, integer()}.
estimate_keys(State) ->
    estimate_keys(State, 0, 0, ?NUM_KEYS_REQUIRED).

estimate_keys(#state{segments=Segments}, CurrentSegment, Keys, MaxKeys)
  when (CurrentSegment * 100) >= Segments;
       Keys >= MaxKeys ->
    {ok, (Keys * Segments) div CurrentSegment};

estimate_keys(State, CurrentSegment, Keys, MaxKeys) ->
    [{_, KeyHashes2}] = key_hashes(State, CurrentSegment),
    estimate_keys(State, CurrentSegment + 1, Keys + length(KeyHashes2), MaxKeys).

-spec key_hashes(hashtree(), integer()) -> [{integer(), orddict()}].
key_hashes(State, Segment) ->
    multi_select_segment(State, [Segment], fun(X) -> X end).

-spec get_bucket(integer(), integer(), hashtree()) -> orddict().
get_bucket(Level, Bucket, State) ->
    case Level =< State#state.mem_levels of
        true ->
            get_memory_bucket(Level, Bucket, State);
        false ->
            get_disk_bucket(Level, Bucket, State)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-ifndef(old_hash).
md5(Bin) ->
    crypto:hash(md5, Bin).

-ifdef(TEST).
esha(Bin) ->
    crypto:hash(sha, Bin).
-endif.

esha_init() ->
    crypto:hash_init(sha).

esha_update(Ctx, Bin) ->
    crypto:hash_update(Ctx, Bin).

esha_final(Ctx) ->
    crypto:hash_final(Ctx).
-else.
md5(Bin) ->
    crypto:md5(Bin).

-ifdef(TEST).
esha(Bin) ->
    crypto:sha(Bin).
-endif.

esha_init() ->
    crypto:sha_init().

esha_update(Ctx, Bin) ->
    crypto:sha_update(Ctx, Bin).

esha_final(Ctx) ->
    crypto:sha_final(Ctx).

-endif.

-spec set_bucket(integer(), integer(), any(), hashtree()) -> hashtree().
set_bucket(Level, Bucket, Val, State) ->
    case Level =< State#state.mem_levels of
        true ->
            set_memory_bucket(Level, Bucket, Val, State);
        false ->
            set_disk_bucket(Level, Bucket, Val, State)
    end.

-spec del_bucket(integer(), integer(), hashtree()) -> hashtree().
del_bucket(Level, Bucket, State) ->
    case Level =< State#state.mem_levels of
        true ->
            del_memory_bucket(Level, Bucket, State);
        false ->
            del_disk_bucket(Level, Bucket, State)
    end.

-spec new_segment_store(proplist(), hashtree()) -> hashtree().
new_segment_store(Opts, State) ->
    DataDir = case proplists:get_value(segment_path, Opts) of
                  undefined ->
                      Root = "/tmp/anti/level",
                      <<P:128/integer>> = md5(term_to_binary({erlang:now(), make_ref()})),
                      filename:join(Root, integer_to_list(P));
                  SegmentPath ->
                      SegmentPath
              end,

    DefaultWriteBufferMin = 4 * 1024 * 1024,
    DefaultWriteBufferMax = 14 * 1024 * 1024,
    ConfigVars = get_env(anti_entropy_leveldb_opts,
                         [{write_buffer_size_min, DefaultWriteBufferMin},
                          {write_buffer_size_max, DefaultWriteBufferMax}]),
    Config = orddict:from_list(ConfigVars),

    %% Use a variable write buffer size to prevent against all buffers being
    %% flushed to disk at once when under a heavy uniform load.
    WriteBufferMin = proplists:get_value(write_buffer_size_min, Config, DefaultWriteBufferMin),
    WriteBufferMax = proplists:get_value(write_buffer_size_max, Config, DefaultWriteBufferMax),
    {Offset, _} = random:uniform_s(1 + WriteBufferMax - WriteBufferMin, now()),
    WriteBufferSize = WriteBufferMin + Offset,
    Config2 = orddict:store(write_buffer_size, WriteBufferSize, Config),
    Config3 = orddict:erase(write_buffer_size_min, Config2),
    Config4 = orddict:erase(write_buffer_size_max, Config3),
    Config5 = orddict:store(is_internal_db, true, Config4),
    Config6 = orddict:store(use_bloomfilter, true, Config5),
    Options = orddict:store(create_if_missing, true, Config6),

    ok = filelib:ensure_dir(DataDir),
    {ok, Ref} = eleveldb:open(DataDir, Options),
    State#state{ref=Ref, path=DataDir}.

-spec share_segment_store(hashtree(), hashtree()) -> hashtree().
share_segment_store(State, #state{ref=Ref, path=Path}) ->
    State#state{ref=Ref, path=Path}.

-spec hash(term()) -> empty | binary().
hash([]) ->
    empty;
hash(X) ->
    %% erlang:phash2(X).
    sha(term_to_binary(X)).

sha(Bin) ->
    Chunk = get_env(anti_entropy_sha_chunk, 4096),
    sha(Chunk, Bin).

sha(Chunk, Bin) ->
    Ctx1 = esha_init(),
    Ctx2 = sha(Chunk, Bin, Ctx1),
    SHA = esha_final(Ctx2),
    SHA.

sha(Chunk, Bin, Ctx) ->
    case Bin of
        <<Data:Chunk/binary, Rest/binary>> ->
            Ctx2 = esha_update(Ctx, Data),
            sha(Chunk, Rest, Ctx2);
        Data ->
            Ctx2 = esha_update(Ctx, Data),
            Ctx2
    end.

get_env(Key, Default) ->
    CoreEnv = app_helper:get_env(riak_core, Key, Default),
    app_helper:get_env(riak_kv, Key, CoreEnv).

-spec update_levels(integer(),
                    [{integer(), [{integer(), binary()}]}],
                    hashtree(), next_rebuild()) -> hashtree().
update_levels(0, _, State, _) ->
    State;
update_levels(Level, Groups, State, Type) ->
    {_, _, NewState, NewBuckets} = rebuild_fold(Level, Groups, State, Type),
    lager:debug("level ~p hashes ~w\n", [Level, NewBuckets]),
    Groups2 = group(NewBuckets, State#state.width),
    update_levels(Level - 1, Groups2, NewState, Type).

-spec rebuild_fold(integer(),
                   [{integer(), [{integer(), binary()}]}], hashtree(),
                   next_rebuild()) -> {integer(), next_rebuild(),
                                      hashtree(), [{integer(), binary()}]}.
rebuild_fold(Level, Groups, State, Type) ->
    lists:foldl(fun rebuild_folder/2, {Level, Type, State, []}, Groups).

rebuild_folder({Bucket, NewHashes}, {Level, Type, StateAcc, BucketsAcc}) ->
    Hashes = case Type of
                 full ->
                     orddict:from_list(NewHashes);
                 incremental ->
                     Hashes1 = get_bucket(Level, Bucket,
                                          StateAcc),
                     Hashes2 = orddict:from_list(NewHashes),
                     orddict:merge(
                       fun(_, _, New) -> New end,
                       Hashes1,
                       Hashes2)
             end,
    %% All of the segments that make up this bucket, trim any
    %% newly emptied hashes (likely result of deletion)
    PopHashes = [{S, H} || {S, H} <- Hashes, H /= [], H /= empty],

    case PopHashes of
        [] ->
            %% No more hash entries, if a full rebuild then disk
            %% already clear.  If not, remove the empty bucket.
            StateAcc2 = case Type of
                            full ->
                                StateAcc;
                            incremental ->
                                del_bucket(Level, Bucket, StateAcc)
                        end,
            %% Although not written to disk, propagate hash up to next level
            %% to mark which entries of the tree need updating.
            NewBucket = {Bucket, []},
            {Level, Type, StateAcc2, [NewBucket | BucketsAcc]};
        _ ->
            %% Otherwise, at least one hash entry present, update
            %% and propagate
            StateAcc2 = set_bucket(Level, Bucket, Hashes, StateAcc),
            NewBucket = {Bucket, hash(PopHashes)},
            {Level, Type, StateAcc2, [NewBucket | BucketsAcc]}
    end.


%% Takes a list of bucket-hash entries from level X and groups them together
%% into groups representing entries at parent level X-1.
%%
%% For example, given bucket-hash entries at level X:
%%   [{1,H1}, {2,H2}, {3,H3}, {4,H4}, {5,H5}, {6,H6}, {7,H7}, {8,H8}]
%%
%% The grouping at level X-1 with a width of 4 would be:
%%   [{1,[{1,H1}, {2,H2}, {3,H3}, {4,H4}]},
%%    {2,[{5,H5}, {6,H6}, {7,H7}, {8,H8}]}]
%%
-spec group([{integer(), binary()}], pos_integer())
           -> [{integer(), [{integer(), binary()}]}].
group([], _) ->
    [];
group(L, Width) ->
    {FirstId, _} = hd(L),
    FirstBucket = FirstId div Width,
    {LastBucket, LastGroup, Groups} =
        lists:foldl(fun(X={Id, _}, {LastBucket, Acc, Groups}) ->
                            Bucket = Id div Width,
                            case Bucket of
                                LastBucket ->
                                    {LastBucket, [X|Acc], Groups};
                                _ ->
                                    {Bucket, [X], [{LastBucket, Acc} | Groups]}
                            end
                    end, {FirstBucket, [], []}, L),
    [{LastBucket, LastGroup} | Groups].

-spec get_memory_bucket(integer(), integer(), hashtree()) -> any().
get_memory_bucket(Level, Bucket, #state{tree=Tree}) ->
    case dict:find({Level, Bucket}, Tree) of
        error ->
            orddict:new();
        {ok, Val} ->
            Val
    end.

-spec set_memory_bucket(integer(), integer(), any(), hashtree()) -> hashtree().
set_memory_bucket(Level, Bucket, Val, State) ->
    Tree = dict:store({Level, Bucket}, Val, State#state.tree),
    State#state{tree=Tree}.

-spec del_memory_bucket(integer(), integer(), hashtree()) -> hashtree().
del_memory_bucket(Level, Bucket, State) ->
    Tree = dict:erase({Level, Bucket}, State#state.tree),
    State#state{tree=Tree}.

-spec get_disk_bucket(integer(), integer(), hashtree()) -> any().
get_disk_bucket(Level, Bucket, #state{id=Id, ref=Ref}) ->
    HKey = encode_bucket(Id, Level, Bucket),
    case eleveldb:get(Ref, HKey, []) of
        {ok, Bin} ->
            binary_to_term(Bin);
        _ ->
            orddict:new()
    end.

-spec set_disk_bucket(integer(), integer(), any(), hashtree()) -> hashtree().
set_disk_bucket(Level, Bucket, Val, State=#state{id=Id, ref=Ref}) ->
    HKey = encode_bucket(Id, Level, Bucket),
    Bin = term_to_binary(Val),
    ok = eleveldb:put(Ref, HKey, Bin, []),
    State.

del_disk_bucket(Level, Bucket, State = #state{id = Id, ref = Ref}) ->
    HKey = encode_bucket(Id, Level, Bucket),
    ok = eleveldb:delete(Ref, HKey, []),
    State.

-spec encode_id(binary() | non_neg_integer()) -> tree_id_bin().
encode_id(TreeId) when is_integer(TreeId) ->
    if (TreeId >= 0) andalso
       (TreeId < ((1 bsl 160)-1)) ->
            <<TreeId:176/integer>>;
       true ->
            erlang:error(badarg)
    end;
encode_id(TreeId) when is_binary(TreeId) and (byte_size(TreeId) == 22) ->
    TreeId;
encode_id(_) ->
    erlang:error(badarg).

-spec encode(tree_id_bin(), integer(), binary()) -> segment_bin().
encode(TreeId, Segment, Key) ->
    <<$t,TreeId:22/binary,$s,Segment:64/integer,Key/binary>>.

-spec safe_decode(binary()) -> {tree_id_bin() | bad, integer(), binary()}.
safe_decode(Bin) ->
    case Bin of
        <<$t,TreeId:22/binary,$s,Segment:64/integer,Key/binary>> ->
            {TreeId, Segment, Key};
        _ ->
            {bad, -1, <<>>}
    end.

-spec decode(segment_bin()) -> {tree_id_bin(), non_neg_integer(), binary()}.
decode(Bin) ->
    <<$t,TreeId:22/binary,$s,Segment:64/integer,Key/binary>> = Bin,
    {TreeId, Segment, Key}.

-spec encode_bucket(tree_id_bin(), integer(), integer()) -> bucket_bin().
encode_bucket(TreeId, Level, Bucket) ->
    <<$b,TreeId:22/binary,$b,Level:64/integer,Bucket:64/integer>>.

-spec decode_bucket(bucket_bin()) -> {tree_id_bin(), integer(), integer()}.
decode_bucket(Bin) ->
    <<$b,TreeId:22/binary,$b,Level:64/integer,Bucket:64/integer>> = Bin,
    {TreeId, Level, Bucket}.

-spec encode_meta(binary()) -> meta_bin().
encode_meta(Key) ->
    <<$m,Key/binary>>.

-spec hashes(hashtree(), list('*'|integer())) -> [{integer(), binary()}].
hashes(State, Segments) ->
    multi_select_segment(State, Segments, fun hash/1).

-spec snapshot(hashtree()) -> hashtree().
snapshot(State) ->
    %% Abuse eleveldb iterators as snapshots
    catch eleveldb:iterator_close(State#state.itr),
    {ok, Itr} = eleveldb:iterator(State#state.ref, []),
    State#state{itr=Itr}.

-spec multi_select_segment(hashtree(), list('*'|integer()), select_fun(T))
                          -> [{integer(), T}].
multi_select_segment(#state{id=Id, itr=Itr}, Segments, F) ->
    [First | Rest] = Segments,
    IS1 = #itr_state{itr=Itr,
                     id=Id,
                     current_segment=First,
                     remaining_segments=Rest,
                     acc_fun=F,
                     segment_acc=[],
                     final_acc=[]},
    Seek = case First of
               '*' ->
                   encode(Id, 0, <<>>);
               _ ->
                   encode(Id, First, <<>>)
           end,
    IS2 = try
              iterate(iterator_move(Itr, Seek), IS1)
          after
              %% Always call prefetch stop to ensure the iterator
              %% is safe to use in the compare.  Requires
              %% eleveldb > 2.0.16 or this may segv/hang.
              _ = iterator_move(Itr, prefetch_stop)
          end,
    #itr_state{remaining_segments = LeftOver,
               current_segment=LastSegment,
               segment_acc=LastAcc,
               final_acc=FA} = IS2,

    %% iterate completes without processing the last entries in the state.  Compute
    %% the final visited segment, and add calls to the F([]) for all of the segments
    %% that do not exist at the end of the file (due to deleting the last entry in the
    %% segment).
    Result = [{LeftSeg, F([])} || LeftSeg <- lists:reverse(LeftOver),
                  LeftSeg =/= '*'] ++
    [{LastSegment, F(LastAcc)} | FA],
    case Result of
        [{'*', _}] ->
            %% Handle wildcard select when all segments are empty
            [];
        _ ->
            Result
    end.

iterator_move(undefined, _Seek) ->
    {error, invalid_iterator};
iterator_move(Itr, Seek) ->
    try

        eleveldb:iterator_move(Itr, Seek)
    catch
        _:badarg ->
            {error, invalid_iterator}
    end.

-spec iterate({'error','invalid_iterator'} | {'ok',binary(),binary()},
              #itr_state{}) -> #itr_state{}.

%% Ended up at an invalid_iterator likely due to encountering a missing dirty
%% segment - e.g. segment dirty, but removed last entries for it
iterate({error, invalid_iterator}, IS=#itr_state{current_segment='*'}) ->
    IS;
iterate({error, invalid_iterator}, IS=#itr_state{itr=Itr,
                                                 id=Id,
                                                 current_segment=CurSeg,
                                                 remaining_segments=Segments,
                                                 acc_fun=F,
                                                 segment_acc=Acc,
                                                 final_acc=FinalAcc}) ->
    case Segments of
        [] ->
            IS;
        ['*'] ->
            IS;
        [NextSeg | Remaining] ->
            Seek = encode(Id, NextSeg, <<>>),
            IS2 = IS#itr_state{current_segment=NextSeg,
                               remaining_segments=Remaining,
                               segment_acc=[],
                               final_acc=[{CurSeg, F(Acc)} | FinalAcc]},
            iterate(iterator_move(Itr, Seek), IS2)
    end;
iterate({ok, K, V}, IS=#itr_state{itr=Itr,
                                  id=Id,
                                  current_segment=CurSeg,
                                  remaining_segments=Segments,
                                  acc_fun=F,
                                  segment_acc=Acc,
                                  final_acc=FinalAcc}) ->
    {SegId, Seg, _} = safe_decode(K),
    Segment = case CurSeg of
                  '*' ->
                      Seg;
                  _ ->
                      CurSeg
              end,
    case {SegId, Seg, Segments, IS#itr_state.prefetch} of
        {bad, -1, _, _} ->
            %% Non-segment encountered, end traversal
            IS;
        {Id, Segment, _, _} ->
            %% Still reading existing segment
            IS2 = IS#itr_state{current_segment=Segment,
                               segment_acc=[{K,V} | Acc],
                               prefetch=true},
            iterate(iterator_move(Itr, prefetch), IS2);
        {Id, _, [Seg|Remaining], _} ->
            %% Pointing at next segment we are interested in
            IS2 = IS#itr_state{current_segment=Seg,
                               remaining_segments=Remaining,
                               segment_acc=[{K,V}],
                               final_acc=[{Segment, F(Acc)} | FinalAcc],
                               prefetch=true},
            iterate(iterator_move(Itr, prefetch), IS2);
        {Id, _, ['*'], _} ->
            %% Pointing at next segment we are interested in
            IS2 = IS#itr_state{current_segment=Seg,
                               remaining_segments=['*'],
                               segment_acc=[{K,V}],
                               final_acc=[{Segment, F(Acc)} | FinalAcc],
                               prefetch=true},
            iterate(iterator_move(Itr, prefetch), IS2);
        {Id, _, [NextSeg | Remaining], true} ->
            %% Pointing at uninteresting segment, but need to halt the
            %% prefetch to ensure the iterator can be reused
            IS2 = IS#itr_state{current_segment=NextSeg,
                               segment_acc=[],
                               remaining_segments=Remaining,
                               final_acc=[{Segment, F(Acc)} | FinalAcc],
                               prefetch=true}, % will be after second move
            _ = iterator_move(Itr, prefetch_stop), % ignore the pre-fetch,
            Seek = encode(Id, NextSeg, <<>>),      % and risk wasting a reseek
            iterate(iterator_move(Itr, Seek), IS2);% to get to the next segment
        {Id, _, [NextSeg | Remaining], false} ->
            %% Pointing at uninteresting segment, seek to next interesting one
            Seek = encode(Id, NextSeg, <<>>),
            IS2 = IS#itr_state{current_segment=NextSeg,
                               remaining_segments=Remaining,
                               segment_acc=[],
                               final_acc=[{Segment, F(Acc)} | FinalAcc]},
            iterate(iterator_move(Itr, Seek), IS2);
        {_, _, _, true} ->
            %% Done with traversal, but need to stop the prefetch to
            %% ensure the iterator can be reused. The next operation
            %% with this iterator is a seek so no need to be concerned
            %% with the data returned here.
            _ = iterator_move(Itr, prefetch_stop),
            IS#itr_state{prefetch=false};
        {_, _, _, false} ->
            %% Done with traversal
            IS
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% level-by-level exchange (BFS instead of DFS)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

compare2(Tree, Remote, AccFun, Acc) ->
    Final = Tree#state.levels + 1,
    Local = fun(get_bucket, {L, B}) ->
                    get_bucket(L, B, Tree);
               (key_hashes, Segment) ->
                    [{_, KeyHashes2}] = key_hashes(Tree, Segment),
                    KeyHashes2
            end,
    Opts = [],
    exchange(1, [0], Final, Local, Remote, AccFun, Acc, Opts).

exchange(_Level, [], _Final, _Local, _Remote, _AccFun, Acc, _Opts) ->
    Acc;
exchange(Level, Diff, Final, Local, Remote, AccFun, Acc, Opts) ->
    if Level =:= Final ->
            exchange_final(Level, Diff, Local, Remote, AccFun, Acc, Opts);
       true ->
            Diff2 = exchange_level(Level, Diff, Local, Remote, Opts),
            exchange(Level+1, Diff2, Final, Local, Remote, AccFun, Acc, Opts)
    end.

exchange_level(Level, Buckets, Local, Remote, _Opts) ->
    Remote(start_exchange_level, {Level, Buckets}),
    lists:flatmap(fun(Bucket) ->
                          A = Local(get_bucket, {Level, Bucket}),
                          B = Remote(get_bucket, {Level, Bucket}),
                          Delta = riak_core_util:orddict_delta(lists:keysort(1, A),
                                                                   lists:keysort(1, B)),
              lager:debug("Exchange Level ~p Bucket ~p\nA=~p\nB=~p\nD=~p\n",
                      [Level, Bucket, A, B, Delta]),

                          Diffs = Delta,
                          [BK || {BK, _} <- Diffs]
                  end, Buckets).

exchange_final(_Level, Segments, Local, Remote, AccFun, Acc0, _Opts) ->
    Remote(start_exchange_segments, Segments),
    lists:foldl(fun(Segment, Acc) ->
                        A = Local(key_hashes, Segment),
                        B = Remote(key_hashes, Segment),
                        Delta = riak_core_util:orddict_delta(lists:keysort(1, A),
                                                                 lists:keysort(1, B)),
            lager:debug("Exchange Final\nA=~p\nB=~p\nD=~p\n",
                    [A, B, Delta]),
                        Keys = [begin
                                    {_Id, Segment, Key} = decode(KBin),
                                    Type = key_diff_type(Diff),
                                    {Type, Key}
                                end || {KBin, Diff} <- Delta, not tombstone_diff(Diff)],
                        AccFun(Keys, Acc)
                end, Acc0, Segments).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec compare(integer(), integer(), hashtree(), remote_fun(), acc_fun(X), X) -> X.
compare(Level, Bucket, Tree, Remote, AccFun, KeyAcc) when Level == Tree#state.levels+1 ->
    Keys = compare_segments(Bucket, Tree, Remote),
    AccFun(Keys, KeyAcc);
compare(Level, Bucket, Tree, Remote, AccFun, KeyAcc) ->
    HL1 = get_bucket(Level, Bucket, Tree),
    HL2 = Remote(get_bucket, {Level, Bucket}),
    Union = lists:ukeysort(1, HL1 ++ HL2),
    Inter = ordsets:intersection(ordsets:from_list(HL1),
                                 ordsets:from_list(HL2)),
    Diff = ordsets:subtract(Union, Inter),
    lager:debug("Tree ~p level ~p bucket ~p\nL=~p\nR=~p\nD=\n",
        [Tree, Level, Bucket, HL1, HL2, Diff]),
    KeyAcc3 =
        lists:foldl(fun({Bucket2, _}, KeyAcc2) ->
                            compare(Level+1, Bucket2, Tree, Remote, AccFun, KeyAcc2)
                    end, KeyAcc, Diff),
    KeyAcc3.

-spec compare_segments(integer(), hashtree(), remote_fun()) -> [keydiff()].
compare_segments(Segment, Tree=#state{id=Id}, Remote) ->
    [{_, KeyHashes1}] = key_hashes(Tree, Segment),
    KeyHashes2 = Remote(key_hashes, Segment),
    HL1 = orddict:from_list(KeyHashes1),
    HL2 = orddict:from_list(KeyHashes2),
    Delta = riak_core_util:orddict_delta(HL1, HL2),
    lager:debug("Tree ~p segment ~p diff ~p\n",
                [Tree, Segment, Delta]),
    Keys = [begin
                {Id, Segment, Key} = decode(KBin),
                Type = key_diff_type(Diff),
                {Type, Key}
            end || {KBin, Diff} <- Delta, not tombstone_diff(Diff)],
    Keys.

tombstone_diff(Diff) ->
    key_diff_type(Diff) == tombstone_diff.

key_diff_type({?TOMBSTONE , _}) ->
    tombstone_diff;
key_diff_type({_ , ?TOMBSTONE}) ->
    tombstone_diff;
key_diff_type({'$none', _}) ->
    missing;
key_diff_type({_, '$none'}) ->
    remote_missing;
key_diff_type(_) ->
    different.

%%%===================================================================
%%% bitarray
%%%===================================================================
-define(W, 27).

-spec bitarray_new(integer()) -> hashtree_array().
bitarray_new(N) -> array:new((N-1) div ?W + 1, {default, 0}).

-spec bitarray_set(integer(), hashtree_array()) -> hashtree_array().
bitarray_set(I, A) ->
    AI = I div ?W,
    V = array:get(AI, A),
    V1 = V bor (1 bsl (I rem ?W)),
    array:set(AI, V1, A).

-spec bitarray_to_list(hashtree_array()) -> [integer()].
bitarray_to_list(A) ->
    lists:reverse(
      array:sparse_foldl(fun(I, V, Acc) ->
                                 expand(V, I * ?W, Acc)
                         end, [], A)).

%% Convert bit vector into list of integers, with optional offset.
%% expand(2#01, 0, []) -> [0]
%% expand(2#10, 0, []) -> [1]
%% expand(2#1101, 0,   []) -> [3,2,0]
%% expand(2#1101, 1,   []) -> [4,3,1]
%% expand(2#1101, 10,  []) -> [13,12,10]
%% expand(2#1101, 100, []) -> [103,102,100]
expand(0, _, Acc) ->
    Acc;
expand(V, N, Acc) ->
    Acc2 =
        case (V band 1) of
            1 ->
                [N|Acc];
            0 ->
                Acc
        end,
    expand(V bsr 1, N+1, Acc2).

%%%===================================================================
%%% Experiments
%%%===================================================================

-ifdef(TEST).

run_local() ->
    run_local(10000).
run_local(N) ->
    timer:tc(fun do_local/1, [N]).

run_concurrent_build() ->
    run_concurrent_build(10000).
run_concurrent_build(N) ->
    run_concurrent_build(N, N).
run_concurrent_build(N1, N2) ->
    timer:tc(fun do_concurrent_build/2, [N1, N2]).

run_multiple(Count, N) ->
    Tasks = [fun() ->
                     do_concurrent_build(N, N)
             end || _ <- lists:seq(1, Count)],
    timer:tc(fun peval/1, [Tasks]).

run_remote() ->
    run_remote(100000).
run_remote(N) ->
    timer:tc(fun do_remote/1, [N]).

do_local(N) ->
    A0 = insert_many(N, new()),
    A1 = insert(<<"10">>, <<"42">>, A0),
    A2 = insert(<<"10">>, <<"42">>, A1),
    A3 = insert(<<"13">>, <<"52">>, A2),

    B0 = insert_many(N, new()),
    B1 = insert(<<"14">>, <<"52">>, B0),
    B2 = insert(<<"10">>, <<"32">>, B1),
    B3 = insert(<<"10">>, <<"422">>, B2),

    A4 = update_tree(A3),
    B4 = update_tree(B3),
    KeyDiff = local_compare(A4, B4),
    io:format("KeyDiff: ~p~n", [KeyDiff]),
    close(A4),
    close(B4),
    destroy(A4),
    destroy(B4),
    ok.

do_concurrent_build(N1, N2) ->
    F1 = fun() ->
                 A0 = insert_many(N1, new()),
                 A1 = insert(<<"10">>, <<"42">>, A0),
                 A2 = insert(<<"10">>, <<"42">>, A1),
                 A3 = insert(<<"13">>, <<"52">>, A2),
                 A4 = update_tree(A3),
                 A4
         end,

    F2 = fun() ->
                 B0 = insert_many(N2, new()),
                 B1 = insert(<<"14">>, <<"52">>, B0),
                 B2 = insert(<<"10">>, <<"32">>, B1),
                 B3 = insert(<<"10">>, <<"422">>, B2),
                 B4 = update_tree(B3),
                 B4
         end,

    [A4, B4] = peval([F1, F2]),
    KeyDiff = local_compare(A4, B4),
    io:format("KeyDiff: ~p~n", [KeyDiff]),

    close(A4),
    close(B4),
    destroy(A4),
    destroy(B4),
    ok.

do_remote(N) ->
    %% Spawn new process for remote tree
    Other =
        spawn(fun() ->
                      A0 = insert_many(N, new()),
                      A1 = insert(<<"10">>, <<"42">>, A0),
                      A2 = insert(<<"10">>, <<"42">>, A1),
                      A3 = insert(<<"13">>, <<"52">>, A2),
                      A4 = update_tree(A3),
                      message_loop(A4, 0, 0)
              end),

    %% Build local tree
    B0 = insert_many(N, new()),
    B1 = insert(<<"14">>, <<"52">>, B0),
    B2 = insert(<<"10">>, <<"32">>, B1),
    B3 = insert(<<"10">>, <<"422">>, B2),
    B4 = update_tree(B3),

    %% Compare with remote tree through message passing
    Remote = fun(get_bucket, {L, B}) ->
                     Other ! {get_bucket, self(), L, B},
                     receive {remote, X} -> X end;
                (start_exchange_level, {_Level, _Buckets}) ->
                     ok;
                (start_exchange_segments, _Segments) ->
                     ok;
                (key_hashes, Segment) ->
                     Other ! {key_hashes, self(), Segment},
                     receive {remote, X} -> X end
             end,
    KeyDiff = compare(B4, Remote),
    io:format("KeyDiff: ~p~n", [KeyDiff]),

    %% Signal spawned process to print stats and exit
    Other ! done,
    ok.

message_loop(Tree, Msgs, Bytes) ->
    receive
        {get_bucket, From, L, B} ->
            Reply = get_bucket(L, B, Tree),
            From ! {remote, Reply},
            Size = byte_size(term_to_binary(Reply)),
            message_loop(Tree, Msgs+1, Bytes+Size);
        {key_hashes, From, Segment} ->
            [{_, KeyHashes2}] = key_hashes(Tree, Segment),
            Reply = KeyHashes2,
            From ! {remote, Reply},
            Size = byte_size(term_to_binary(Reply)),
            message_loop(Tree, Msgs+1, Bytes+Size);
        done ->
            %% io:format("Exchanged messages: ~b~n", [Msgs]),
            %% io:format("Exchanged bytes:    ~b~n", [Bytes]),
            ok
    end.

insert_many(N, T1) ->
    T2 =
        lists:foldl(fun(X, TX) ->
                            insert(bin(-X), bin(X*100), TX)
                    end, T1, lists:seq(1,N)),
    T2.

bin(X) ->
    list_to_binary(integer_to_list(X)).

peval(L) ->
    Parent = self(),
    lists:foldl(
      fun(F, N) ->
              spawn(fun() ->
                            Parent ! {peval, N, F()}
                    end),
              N+1
      end, 0, L),
    L2 = [receive {peval, N, R} -> {N,R} end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.

%%%===================================================================
%%% EUnit
%%%===================================================================

-spec local_compare(hashtree(), hashtree()) -> [keydiff()].
local_compare(T1, T2) ->
    Remote = fun(get_bucket, {L, B}) ->
                     get_bucket(L, B, T2);
                (start_exchange_level, {_Level, _Buckets}) ->
                     ok;
                (start_exchange_segments, _Segments) ->
                     ok;
                (key_hashes, Segment) ->
                     [{_, KeyHashes2}] = key_hashes(T2, Segment),
                     KeyHashes2
             end,
    AccFun = fun(Keys, KeyAcc) ->
                     Keys ++ KeyAcc
             end,
    compare2(T1, Remote, AccFun, []).

-spec local_compare1(hashtree(), hashtree()) -> [keydiff()].
local_compare1(T1, T2) ->
    Remote = fun(get_bucket, {L, B}) ->
        get_bucket(L, B, T2);
        (start_exchange_level, {_Level, _Buckets}) ->
            ok;
        (start_exchange_segments, _Segments) ->
            ok;
        (key_hashes, Segment) ->
            [{_, KeyHashes2}] = key_hashes(T2, Segment),
            KeyHashes2
             end,
    AccFun = fun(Keys, KeyAcc) ->
        Keys ++ KeyAcc
             end,
    compare(T1, Remote, AccFun, []).

-spec compare(hashtree(), remote_fun()) -> [keydiff()].
compare(Tree, Remote) ->
    compare(Tree, Remote, fun(Keys, KeyAcc) ->
                                  Keys ++ KeyAcc
                          end).

-spec compare(hashtree(), remote_fun(), acc_fun(X)) -> X.
compare(Tree, Remote, AccFun) ->
    compare(Tree, Remote, AccFun, []).

-spec fake_close(hashtree()) -> hashtree().
fake_close(State) ->
    catch eleveldb:close(State#state.ref),
    State.

%% Verify that `update_tree/1' generates a snapshot of the underlying
%% LevelDB store that is used by `compare', therefore isolating the
%% compare from newer/concurrent insertions into the tree.
snapshot_test() ->
    A0 = insert(<<"10">>, <<"42">>, new()),
    B0 = insert(<<"10">>, <<"52">>, new()),
    A1 = update_tree(A0),
    B1 = update_tree(B0),
    B2 = insert(<<"10">>, <<"42">>, B1),
    KeyDiff = local_compare(A1, B1),
    close(A1),
    close(B2),
    destroy(A1),
    destroy(B2),
    ?assertEqual([{different, <<"10">>}], KeyDiff),
    ok.

delta_test() ->
    T1 = update_tree(insert(<<"1">>, esha(term_to_binary(make_ref())),
                            new())),
    T2 = update_tree(insert(<<"2">>, esha(term_to_binary(make_ref())),
                            new())),
    Diff = local_compare(T1, T2),
    ?assertEqual([{remote_missing, <<"1">>}, {missing, <<"2">>}], Diff),
    Diff2 = local_compare(T2, T1),
    ?assertEqual([{missing, <<"1">>}, {remote_missing, <<"2">>}], Diff2),
    ok.

delete_without_update_test() ->
    A1 = new({0,0},[{segment_path, "t1"}]),
    A2 = insert(<<"k">>, <<1234:32>>, A1),
    A3 = update_tree(A2),

    B1 = new({0,0},[{segment_path, "t2"}]),
    B2 = insert(<<"k">>, <<1234:32>>, B1),
    B3 = update_tree(B2),

    Diff = local_compare(A3, B3),

    C1 = delete(<<"k">>, A3),
    C2 = rehash_tree(C1),
    C3 = flush_buffer(C2),
    close(C3),

    AA1 = new({0,0},[{segment_path, "t1"}]),
    AA2 = update_tree(AA1),
    Diff2 = local_compare(AA2, B3),

    close(B3),
    close(AA2),
    destroy(C3),
    destroy(B3),
    destroy(AA2),

    ?assertEqual([], Diff),
    ?assertEqual([{missing, <<"k">>}], Diff2).

opened_closed_test() ->
    TreeId0 = {0,0},
    TreeId1 = term_to_binary({0,0}),
    A1 = new(TreeId0, [{segment_path, "t1000"}]),
    A2 = mark_open_and_check(TreeId0, A1),
    A3 = insert(<<"totes">>, <<1234:32>>, A2),
    A4 = update_tree(A3),

    B1 = new(TreeId0, [{segment_path, "t2000"}]),
    B2 = mark_open_empty(TreeId0, B1),
    B3 = insert(<<"totes">>, <<1234:32>>, B2),
    B4 = update_tree(B3),

    StatusA4 = {proplists:get_value(opened, read_meta_term(TreeId1, [], A4)),
                proplists:get_value(closed, read_meta_term(TreeId1, [], A4))},
    StatusB4 = {proplists:get_value(opened, read_meta_term(TreeId1, [], B4)),
                proplists:get_value(closed, read_meta_term(TreeId1, [], B4))},

    A5 = set_next_rebuild(A4, incremental),
    A6 = mark_clean_close(TreeId0, A5),
    StatusA6 = {proplists:get_value(opened, read_meta_term(TreeId1, [], A6)),
                proplists:get_value(closed, read_meta_term(TreeId1, [], A6))},

    close(A6),
    close(B4),

    AA1 = new(TreeId0, [{segment_path, "t1000"}]),
    AA2 = mark_open_and_check(TreeId0, AA1),
    AA3 = update_tree(AA2),
    StatusAA3 = {proplists:get_value(opened, read_meta_term(TreeId1, [], AA3)),
                 proplists:get_value(closed, read_meta_term(TreeId1, [], AA3))},

    fake_close(AA3),

    AAA1 = new(TreeId0,[{segment_path, "t1000"}]),
    AAA2 = mark_open_and_check(TreeId0, AAA1),
    StatusAAA2 = {proplists:get_value(opened, read_meta_term(TreeId1, [], AAA2)),
                  proplists:get_value(closed, read_meta_term(TreeId1, [], AAA2))},

    AAA3 = mark_clean_close(TreeId0, AAA2),
    close(AAA3),

    AAAA1 = new({0,0},[{segment_path, "t1000"}]),
    AAAA2 = mark_open_and_check(TreeId0, AAAA1),
    StatusAAAA2 = {proplists:get_value(opened, read_meta_term(TreeId1, [], AAAA2)),
                   proplists:get_value(closed, read_meta_term(TreeId1, [], AAAA2))},

    AAAA3 = mark_clean_close(TreeId0, AAAA2),
    StatusAAAA3 = {proplists:get_value(opened, read_meta_term(TreeId1, [], AAAA3)),
                   proplists:get_value(closed, read_meta_term(TreeId1, [], AAAA3))},
    close(AAAA3),
    destroy(B3),
    destroy(A6),
    destroy(AA3),
    destroy(AAA3),
    destroy(AAAA3),

    ?assertEqual({1,undefined}, StatusA4),
    ?assertEqual({1,0}, StatusB4),
    ?assertEqual(full, A2#state.next_rebuild),
    ?assertEqual(incremental, B2#state.next_rebuild),
    ?assertEqual(incremental, A5#state.next_rebuild),
    ?assertEqual({1,1}, StatusA6),
    ?assertEqual({2,1}, StatusAA3),
    ?assertEqual(incremental, AA2#state.next_rebuild),
    ?assertEqual({3,1}, StatusAAA2),
    ?assertEqual(full, AAA1#state.next_rebuild),
    ?assertEqual({4,3}, StatusAAAA2),
    ?assertEqual({4,4}, StatusAAAA3).

-endif.

%%%===================================================================
%%% EQC
%%%===================================================================

-ifdef(EQC).
sha_test_() ->
    {spawn,
     {timeout, 120,
      fun() ->
              ?assert(eqc:quickcheck(eqc:testing_time(4, prop_sha())))
      end
     }}.

prop_sha() ->
    %% NOTE: Generating 1MB (1024 * 1024) size binaries is incredibly slow
    %% with EQC and was using over 2GB of memory
    ?FORALL({Size, NumChunks}, {choose(1, 1024), choose(1, 16)},
                    ?FORALL(Bin, binary(Size),
                            begin
                                %% we need at least one chunk,
                                %% and then we divide the binary size
                                %% into the number of chunks (as a natural
                                %% number)
                                ChunkSize = max(1, (Size div NumChunks)),
                                sha(ChunkSize, Bin) =:= esha(Bin)
                            end)).

eqc_test_() ->
    {spawn,
     {timeout, 120,
      fun() ->
              ?assert(eqc:quickcheck(eqc:testing_time(4, prop_correct())))
      end
     }}.

objects() ->
    ?SIZED(Size, objects(Size+3)).

objects(N) ->
    ?LET(Keys, shuffle(lists:seq(1,N)),
         [{bin(K), binary(8)} || K <- Keys]
        ).

lengths(N) ->
    ?LET(MissingN1,  choose(0,N),
         ?LET(MissingN2,  choose(0,N-MissingN1),
              ?LET(DifferentN, choose(0,N-MissingN1-MissingN2),
                   {MissingN1, MissingN2, DifferentN}))).

mutate(Binary) ->
    L1 = binary_to_list(Binary),
    [X|Xs] = L1,
    X2 = (X+1) rem 256,
    L2 = [X2|Xs],
    list_to_binary(L2).

prop_correct() ->
    ?FORALL(Objects, objects(),
            ?FORALL({MissingN1, MissingN2, DifferentN}, lengths(length(Objects)),
                    begin
                        {RemoteOnly, Objects2} = lists:split(MissingN1, Objects),
                        {LocalOnly,  Objects3} = lists:split(MissingN2, Objects2),
                        {Different,  Same}     = lists:split(DifferentN, Objects3),

                        Different2 = [{Key, mutate(Hash)} || {Key, Hash} <- Different],

                        Insert = fun(Tree, Vals) ->
                                         lists:foldl(fun({Key, Hash}, Acc) ->
                                                             insert(Key, Hash, Acc)
                                                     end, Tree, Vals)
                                 end,

                        A0 = new(),
                        B0 = new(),

                        [begin
                             A1 = new({0,Id}, A0),
                             B1 = new({0,Id}, B0),

                             A2 = Insert(A1, Same),
                             A3 = Insert(A2, LocalOnly),
                             A4 = Insert(A3, Different),

                             B2 = Insert(B1, Same),
                             B3 = Insert(B2, RemoteOnly),
                             B4 = Insert(B3, Different2),

                             A5 = update_tree(A4),
                             B5 = update_tree(B4),

                             Expected =
                                 [{missing, Key}        || {Key, _} <- RemoteOnly] ++
                                 [{remote_missing, Key} || {Key, _} <- LocalOnly] ++
                                 [{different, Key}      || {Key, _} <- Different],

                             KeyDiff = local_compare(A5, B5),

                             ?assertEqual(lists:usort(Expected),
                                          lists:usort(KeyDiff)),

                             %% Reconcile trees
                             A6 = Insert(A5, RemoteOnly),
                             B6 = Insert(B5, LocalOnly),
                             B7 = Insert(B6, Different),
                             A7 = update_tree(A6),
                             B8 = update_tree(B7),
                             ?assertEqual([], local_compare(A7, B8)),
                             true
                         end || Id <- lists:seq(0, 10)],
                        close(A0),
                        close(B0),
                        destroy(A0),
                        destroy(B0),
                        true
                    end)).

est_prop() ->
    %% It's hard to estimate under 10000 keys
    ?FORALL(N, choose(10000, 500000),
            begin
                {ok, EstKeys} = estimate_keys(update_tree(insert_many(N, new()))),
                Diff = abs(N - EstKeys),
                MaxDiff = N div 5,
                ?debugVal(Diff), ?debugVal(EstKeys),?debugVal(MaxDiff),
                ?assertEqual(true, MaxDiff > Diff),
                true
            end).

est_test_() ->
    {spawn,
     {timeout, 240,
      fun() ->
              ?assert(eqc:quickcheck(eqc:testing_time(10, est_prop())))
      end
     }}.

-endif.
