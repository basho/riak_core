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
%%
%% Hashtree EQC test.
%%
%% Generates a pair of logically identical AAE trees populated with data
%% and some phantom trees in the same leveldb database to exercise all
%% of the cases in iterate.
%%
%% Then runs commands to insert, delete, snapshot, update tree
%% and compare.
%%
%% The expected values are stored in two ETS tables t1 and t2,
%% with the most recently snapshotted values copied to tables s1 and s2.
%% (the initial common seed data is not included in the ETS tables).
%%
%% The hashtree's themselves are stored in the process dictionary under
%% key t1 and t2.  This helps with shrinking as it reduces dependencies
%% between states (or at least that's why I remember doing it).
%%
%% Model state stores where each tree is through the snapshot/update cycle.
%% The command frequencies are deliberately manipulated to make it more
%% likely that compares will take place once both trees are updated.
%%


-module(hashtree_eqc).
-compile([export_all]).

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-include_lib("eunit/include/eunit.hrl").

%% Make it possible to run from '-s hashtree_eqc runfor 60' to run from cmdline
runfor([DurationMinsStr]) ->
    DurationSecs = 60 * list_to_integer(atom_to_list(DurationMinsStr)),
    eqc:quickcheck(eqc:testing_time(DurationSecs, hashtree_eqc:prop_correct())).

hashtree_test_() ->
    {setup,
     fun() ->
             application:set_env(lager, handlers, [{lager_console_backend, info}]),
             application:ensure_started(syntax_tools),
             application:ensure_started(compiler),
             application:ensure_started(goldrush),
             application:ensure_started(lager)
     end,
     fun(_) ->
             application:stop(lager),
             application:stop(goldrush),
             application:stop(compiler),
             application:stop(syntax_tools),
             application:unload(lager),
             delete_ets()
     end,
     [{timeout, 60,
       fun() ->
              lager:info("Any warnings should be investigated.  No lager output expected.\n"),
              ?assert(eqc:quickcheck(?QC_OUT(eqc:testing_time(29,
                                                              hashtree_eqc:prop_correct()))))
      end
      }]}.

-record(state,
    {
      started = false,    % Boolean to prevent commands running before initialization step.
      tree_id,            % Tree Id
      params = undefined, % {Segments, Width, MemLevels}
      snap1 = undefined,  % undefined, created, updated
      snap2 = undefined,  % undefined, created, updated
      num_updates = 0     % number of insert/delete operations
    }).

integer_to_binary(Int) ->
    list_to_binary(integer_to_list(Int)).

-ifdef(new_hash).
sha(Bin) ->
    crypto:hash(sha, Bin).
-else.
sha(Bin) ->
    crypto:sha(Bin).
-endif.

key() ->
    ?LET(Key, int(), ?MODULE:integer_to_binary(Key)).

object() ->
    {key(), sha(term_to_binary(make_ref()))}.

objects() ->
    non_empty(list(object())).

levels() ->
    frequency([{20, 1},
               { 5, 2}, % production
               { 1, 3}]).

mem_levels() -> %% Number of memory levels - strongly favor default setting
    frequency([{20, 0}, %% Default setting to not use memory levels
               { 1, 1},
               { 1, 2},
               { 1, 3},
               { 1, 4}]).

width() ->
    frequency([{  1,    8},
               {100,   16}, % pick for high density segments - WHY???
               {  1,   32},
               {  1,   64},
               {  1,  128},
               {  1,  256},
               {  1,  512},
               { 50, 1024}]). % pick for production

params() -> % {Segments, Width, MemLevels}
    %% Generate in terms of number of levels, from that work out the segments
    ?LET({Levels, Width, MemLevels},{levels(), width(), mem_levels()},
         {calculate_num_segments(Width, Levels), Width, MemLevels}).

calculate_num_segments(Width, Levels) ->
    trunc(math:pow(Width, Levels)).

initial_open_mode() ->
    %% frequency([{1, mark_empty}, {5, mark_open}]).
    ?SHRINK(oneof([mark_empty, mark_open]), [mark_empty]). % should shrink towards mark_empty

%% Generate tree ids - the first one is used for the test, the others are added
%% as empty hashtrees. This is done to provoke issues where we hit the end of a keyspace
%% but not the end of the leveldb data, which exercises different code paths than only ever
%% having 1 hashtree
%%
%% Make sure the TreeId is unique (the second element), does not matter if there
%% are dupes in the first number (Index)
ids() ->
    ?SUCHTHAT(TreeIds,
              non_empty(list({
                  ?LET(X, nat(), 1+X), %% Partition ID - not critical for test
                  ?LET(X,nat(), min(X,255))} %% Tree ID as integer - must be unique
              )),
              lists:keyfind(element(2, hd(TreeIds)), 2, tl(TreeIds)) == false).

%%
%% Generate the commands, split into two cases to force the start to happen as
%% the first command.
%%
command(_S = #state{started = false}) ->
    {call, ?MODULE, start, [params(), ids(), initial_open_mode(), initial_open_mode()]};
command(_S = #state{started = true, tree_id = TreeId,
                    params = {_Segments, _Width, MemLevels},
                    snap1 = Snap1, snap2 = Snap2}) ->
    %% Calculate weighting values for different groups of commands.
    %% Weights to increase snap frequency once update snapshot has begun
    SnapshotsDefined = Snap1 /= undefined orelse Snap2 /= undefined,
    SnapshotFrequency = case SnapshotsDefined of true -> 100; _ -> 1 end,
    SnapshotWeight = 10 * SnapshotFrequency,
    MoreAfterSnapshots = 100 * SnapshotFrequency,
    FewerAfterSnapshots = 101 - SnapshotFrequency,
    Infrequently = 1,
    frequency(
        %% Update snapshots/trees. If memory is enabled must test with update_tree
        %% If not, can use the method used by kv/yz_index_hashtree and separate
        %% the two steps, dumping the result from update_perform.
        [{SnapshotWeight, {call, ?MODULE, update_tree, [t1, s1]}}     || Snap1 == undefined] ++
        [{SnapshotWeight, {call, ?MODULE, update_snapshot, [t1, s1]}} || Snap1 == undefined, MemLevels == 0] ++
        [{SnapshotWeight, {call, ?MODULE, update_perform, [t1]}}      || Snap1 == created, MemLevels == 0] ++
        [{SnapshotWeight, {call, ?MODULE, set_next_rebuild, [t1]}}    || Snap1 == updated] ++
        [{SnapshotWeight, {call, ?MODULE, update_tree, [t2, s2]}}     || Snap2 == undefined] ++
        [{SnapshotWeight, {call, ?MODULE, update_snapshot, [t2, s2]}} || Snap2 == undefined, MemLevels == 0] ++
        [{SnapshotWeight, {call, ?MODULE, update_perform, [t2]}}      || Snap2 == created, MemLevels == 0] ++
        [{SnapshotWeight, {call, ?MODULE, set_next_rebuild, [t2]}}    || Snap2 == updated] ++

        %% Can only run compares when both snapshots are updated.  Boost the frequency
        %% when both are snapshotted (note this is guarded by both snapshot being updatable)
        [{MoreAfterSnapshots, {call, ?MODULE, local_compare, []}}    || Snap1 == updated, Snap2 == updated] ++
        [{MoreAfterSnapshots, {call, ?MODULE, local_compare1, []}}    || Snap1 == updated, Snap2 == updated] ++

        %% Modify the data in the two tables
        [{FewerAfterSnapshots, {call, ?MODULE, write, [t1, objects()]}}] ++
        [{FewerAfterSnapshots, {call, ?MODULE, write, [t2, objects()]}}] ++
        [{FewerAfterSnapshots, {call, ?MODULE, write_both, [objects()]}}] ++
        [{FewerAfterSnapshots, {call, ?MODULE, delete, [t1, key()]}}] ++
        [{FewerAfterSnapshots, {call, ?MODULE, delete, [t2, key()]}}] ++
        [{FewerAfterSnapshots, {call, ?MODULE, delete_both, [key()]}}] ++

        %% Mess around with reopening, crashing and rehashing.
        [{Infrequently, {call, ?MODULE, reopen_tree, [t1, TreeId]}}] ++
        [{Infrequently, {call, ?MODULE, reopen_tree, [t2, TreeId]}}] ++
        [{Infrequently, {call, ?MODULE, unsafe_close, [t1, TreeId]}}] ++
        [{Infrequently, {call, ?MODULE, unsafe_close, [t2, TreeId]}}] ++
        [{Infrequently, {call, ?MODULE, rehash_tree, [t1]}}] ++
        [{Infrequently, {call, ?MODULE, rehash_tree, [t2]}}]
    ).

%%
%% Start the model up - initialize two trees, either mark them as open and empty
%% or request they are checked.  Add additional unused hashtrees with ExtraIds
%% to make sure the iterator code is fully exercised.
%%
%% Store the hashtree records in the process dictionary under keys 't1' and 't2'.
%% 
start(Params, [TreeId | ExtraIds], Tree1OpenOrEmpty, Tree2OpenOrEmpty) ->
    {Segments, Width, MemLevels} = Params,
    %% Return now so we can store symbolic value in procdict in next_state call
    T1A = create_and_open_hashtree(TreeId, Segments, Width, MemLevels,
        Tree1OpenOrEmpty, ExtraIds),

    put(t1, T1A),

    T2A = create_and_open_hashtree(TreeId, Segments, Width, MemLevels,
        Tree2OpenOrEmpty, ExtraIds),

    put(t2, T2A),

    %% Make sure ETS is pristine
    delete_ets(),
    create_ets(),

    %% Return treeid for future hashtree recreation
    TreeId.

%% Create a new hashtree given TreeId, Segments, Width, and MemLevels.
%% Add some extra trees to stress iteration beyond keyspaces.
create_and_open_hashtree(TreeId, Segments, Width, MemLevels, OpenOrEmpty, ExtraIds) ->
    Tree0 = hashtree:new(TreeId, [{segments, Segments},
        {width, Width},
        {mem_levels, MemLevels}]),
    Tree = case OpenOrEmpty of
              mark_empty -> hashtree:mark_open_empty(TreeId, Tree0);
              _ -> hashtree:mark_open_and_check(TreeId, Tree0)
          end,
    add_extra_hashtrees(ExtraIds, Tree),
    Tree.


%% Add some extra tree ids and update the metadata to give
%% the iterator code a workout on non-matching ids.
add_extra_hashtrees(ExtraIds, T) ->
    lists:foldl(fun(ExtraId, Tacc) ->
            Tacc2 = hashtree:new(ExtraId, Tacc),
            Tacc3 = hashtree:mark_open_empty(ExtraId, Tacc2),
            Tacc4 = hashtree:insert(<<"keyfromextratree">>,
                                                <<"valuefromextratree">>, Tacc3),
            hashtree:flush_buffer(Tacc4)
        end, T, ExtraIds).

%% Wrap the hashtree:update_tree call.  This works with memory levels
%% enabled.  Copy the model tree to a snapshot table.
update_tree(T, S) ->
    %% Snapshot the hashtree and store both states
    HT = hashtree:update_tree(get(T)),
    put(T, HT),
    %% Copy the current ets table to the snapshot table.
    copy_tree(T, S),
    ok.

%% Wrap the hashtree:update_snapshot call and set the next rebuild type to full
%% to match the behavior needed by the *_index_hashtree modules that consume
%% hashtree.  Otherwise if the state is treated as incremental on a safe reopen
%% then the tree does not rebuild correctly.
%%
%% Store the snapshot state in the process dictionary under {snapstate, t1} or
%% {snapstate, t2} for use by update_perform.
%%
%% N.B. This does not work with memory levels enabled as update_perform uses the
%% snapshot state which is dumped.

update_snapshot(T, S) ->
    %% Snapshot the hashtree and store both states
    {SS, HT} = hashtree:update_snapshot(get(T)),

    %% Mark as a full rebuild until the update perfom step happens.
    HT2 = hashtree:set_next_rebuild(HT, full),

    put(T, HT2),
    put({snapstate, T}, SS),
    %% Copy the current ets table to the snapshot table.
    copy_tree(T, S),
    ok.


%% 
%% Wrap the hashtree:update_perform call and erase the snapshot hashtree state.
%% Should only happen if a snapshot state exists.
%%
update_perform(T) ->
    _ = hashtree:update_perform(get({snapstate, T})),
    erase({snapstate, T}),
    ok.


%% Set the next rebuild state. Should only happen once update perform has
%% completed.
%%
set_next_rebuild(T) ->
    put(T, hashtree:set_next_rebuild(get(T), incremental)),
    ok.

%% Wrap hashtree:insert to (over)write key with a new hash to a single
%% table and insert into the model tree.
%%
write(T, Objects) ->
    lists:foreach(fun({Key, Hash}) ->
                          put(T, hashtree:insert(Key, Hash, get(T))),
                          ets:insert(T, {Key, Hash})
                  end, Objects),
    ok.

%% Call the other wrapper to write to both trees.
%%
write_both(Objects) ->
    write(t1, Objects),
    write(t2, Objects),
    ok.


%% Wrap hashtree:delete to remove a key from a tree (and remove
%% from the model tree).
%%
%% Keys do not need to be present to remove them from the AAE tree.
delete(T, Key) ->
    put(T, hashtree:delete(Key, get(T))),
    ets:delete(T, Key),
    ok.

%% Call the other wrapper to remove the key from both trees.
delete_both(Key) ->
    delete(t1, Key),
    delete(t2, Key),
    ok.

%% Trigger a rehash of the whole interior tree.  There is a potential
%% race condition with update_snapshot that is avoided by this model,
%% however if called during update_perform executing it will silently
%% break multi_select_segment.
rehash_tree(T) ->
    put(T, hashtree:rehash_tree(get(T))),
    ok.

%% Flush, update tree, mark clean close, close and reopen the AAE tree.
reopen_tree(T, TreeId) ->
    HT = hashtree:flush_buffer(get(T)),
    {Segments, Width, MemLevels} = {hashtree:segments(HT), hashtree:width(HT),
                                    hashtree:mem_levels(HT)},
    Path = hashtree:path(HT),

    UpdatedHT = hashtree:update_tree(HT),
    CleanClosedHT = hashtree:mark_clean_close(TreeId, UpdatedHT),
    hashtree:close(CleanClosedHT),

    T1 = hashtree:new(TreeId, [{segments, Segments},
                              {width, Width},
                              {mem_levels, MemLevels},
                              {segment_path, Path}]),

    put(T, hashtree:mark_open_and_check(TreeId, T1)),
    ok.

%% Simulate an unsafe close.  This flushes the write buffer so that the
%% model has a chance of knowing what the correct repairs should be.
unsafe_close(T, TreeId) ->
    HT = get(T),
    {Segments, Width, MemLevels} = {hashtree:segments(HT), hashtree:width(HT),
                                    hashtree:mem_levels(HT)},
    Path = hashtree:path(HT),
    %% Although this is an unsafe close, it's unsafe in metadata/building
    %% buckets.  Rather than model the queue behavior, flush those and just
    %% check the buckets are correctly recomputed next compare.
    hashtree:flush_buffer(HT),
    hashtree:fake_close(HT),

    T0 = hashtree:new(TreeId, [{segments, Segments},
                              {width, Width},
                              {mem_levels, MemLevels},
                              {segment_path, Path}]),

    put(T, hashtree:mark_open_and_check(TreeId, T0)),

    ok.

%% Use the internal eunit local comparison to check for differences between the
%% two trees.
local_compare() ->
    hashtree:local_compare(get(t1), get(t2)).

local_compare1() ->
    hashtree:local_compare1(get(t1), get(t2)).

%% Preconditions to guard against impossible situations during shrinking.
precondition(#state{started = false}, {call, _, F, _A}) ->
    F == start;
%% Make sure update_tree can only be called with no memory levels
precondition(#state{params = {_, _, MemLevels}, snap1 = Snap1}, {call, _, update_tree, [t1, _]}) ->
    Snap1 == undefined andalso MemLevels == 0;
precondition(#state{params = {_, _, MemLevels}, snap2 = Snap2}, {call, _, update_tree, [t2, _]}) ->
    Snap2 == undefined andalso MemLevels == 0;
%% Make sure only one snapshot, tree update or rebuild is happening in sequence
precondition(#state{snap1 = Snap1}, {call, _, update_snapshot, [t1, _]}) ->
    Snap1 == undefined;
precondition(#state{snap1 = Snap1}, {call, _, update_perform, [t1]}) ->
    Snap1 == created;
precondition(#state{snap1 = Snap1}, {call, _, set_next_rebuild, [t1]}) ->
    Snap1 == updated;
precondition(#state{snap2 = Snap2}, {call, _, update_snapshot, [t2, _]}) ->
    Snap2 == undefined;
precondition(#state{snap2 = Snap2}, {call, _, update_perform, [t2]}) ->
    Snap2 == created;
precondition(#state{snap2 = Snap2}, {call, _, set_next_rebuild, [t2]}) ->
    Snap2 == updated;
%% Only compare once the tree has been updated for the snapshot
precondition(#state{snap1 = Snap1, snap2 = Snap2}, {call, _, local_compare, []}) ->
    Snap1 == updated andalso Snap2 == updated;
precondition(_S, _C) ->
    true.

%% Check the post conditions.  After the initial create,
%% make sure the next rebuilds are set correctly
postcondition(_S,{call,_,start, [_Params, _ExtraIds, T1Mark, T2Mark]},_R) ->
    NextRebuildT1 = hashtree:next_rebuild(get(t1)),
    NextRebuildT2 = hashtree:next_rebuild(get(t2)),
    %% TODO: Convert this to a conjunction
    T1Expect = case T1Mark of
                   mark_empty -> incremental;
                   _ -> full
               end,
    T2Expect = case T2Mark of
                   mark_empty -> incremental;
                   _ -> full
               end,
    eqc_statem:conj([eq({t1, T1Expect}, {t1, NextRebuildT1}),
                     eq({t2, T2Expect}, {t2, NextRebuildT2})]);
%% After a comparison, check against the results against
%% the ETS table containing the *snapshot* copies.
postcondition(_S,{call, _, Function, _},  Result0) when Function == local_compare;
                                                        Function == local_compare1 ->
    Result = lists:sort(Result0),
    T1Top = hashtree:top_hash(get(t1)),
    T2Top = hashtree:top_hash(get(t2)),
    Expect = expect_compare(),
    case Expect of
        [] ->
            eqc_statem:conj([eq({result, Expect}, {result, Result}),
                             eq({top, T1Top}, {top, T2Top})]);
        _ ->
            eq(Expect, Result)
    end;
postcondition(_S,{call,_,_,_},_R) ->
    true.

next_state(S,R,{call, _, start, [Params,_ExtraIds,_,_]}) ->
    %% Start returns the TreeId used, stick in the state
    %% as no hashtree:tree_id call yet.
    S#state{started = true, tree_id = R, params = Params};
next_state(S,_V,{call, _, update_tree, [t1, _]}) ->
    S#state{snap1 = updated};
next_state(S,_V,{call, _, update_tree, [t2, _]}) ->
    S#state{snap2 = updated};
next_state(S,_V,{call, _, update_snapshot, [t1, _]}) ->
    S#state{snap1 = created};
next_state(S,_V,{call, _, update_snapshot, [t2, _]}) ->
    S#state{snap2 = created};
next_state(S,_V,{call, _, update_perform, [t1]}) ->
    S#state{snap1 = updated};
next_state(S,_V,{call, _, update_perform, [t2]}) ->
    S#state{snap2 = updated};
next_state(S,_V,{call, _, set_next_rebuild, [t1]}) ->
    S#state{snap1 = undefined};
next_state(S,_V,{call, _, set_next_rebuild, [t2]}) ->
    S#state{snap2 = undefined};
next_state(S,_V,{call, _, write, [_T, Objs]}) ->
    S#state{num_updates = S#state.num_updates + length(Objs)};
next_state(S,_R,{call, _, write_both, [Objs]}) ->
    S#state{num_updates = S#state.num_updates + 2*length(Objs)};
next_state(S,_V,{call, _, delete, _}) ->
    S#state{num_updates = S#state.num_updates + 1};
next_state(S,_R,{call, _, delete_both, _}) ->
    S#state{num_updates = S#state.num_updates + 2};
next_state(S,_R,{call, _, reopen_tree, [t1, _]}) ->
    S#state{snap1 = undefined};
next_state(S,_R,{call, _, reopen_tree, [t2, _]}) ->
    S#state{snap2 = undefined};
next_state(S,_R,{call, _, unsafe_close, [t1, _]}) ->
    S#state{snap1 = undefined};
next_state(S,_R,{call, _, unsafe_close, [t2, _]}) ->
    S#state{snap2 = undefined};
next_state(S,_R,{call, _, rehash_tree, [t1]}) ->
    S#state{snap1 = undefined};
next_state(S,_R,{call, _, rehash_tree, [t2]}) ->
    S#state{snap2 = undefined};
next_state(S,_R,{call, _, local_compare, []}) ->
    S;
next_state(S,_R,{call, _, local_compare1, []}) ->
    S.


%% Property to generate a series of commands against the
%% hashtrees and afterwards force them to a comparable state.
%%
prop_correct() ->
    ?FORALL(Cmds,commands(?MODULE, #state{}),
            aggregate(command_names(Cmds),
                begin
                    %%io:format(user, "Starting in ~p\n", [self()]),
                    put(t1, undefined),
                    put(t2, undefined),
                    catch ets:delete(t1),
                    catch ets:delete(t2),
                    {_H,S,Res0} = HSR = run_commands(?MODULE,Cmds),
            {Segments, Width, MemLevels} =
                        case S#state.params of
                            undefined ->
                                %% Possible if Cmds just init
                                %% set segments to 1 to avoid div by zero
                                {1, undefined, undefined};
                            Params ->
                                Params
                        end,
                    %% If ok after steps, do a final compare to increase
                    %% the number of tests.
                    Res = case (S#state.started andalso Res0 == ok) of
                              true ->
                                  final_compare(S);
                              _ ->
                                  Res0
                          end,
                    %% Clean up after the test
                    case Res of 
                        ok -> %  if all went well, remove leveldb files
                            catch cleanup_hashtree(get(t1)),
                            catch cleanup_hashtree(get(t2));
                        _ -> % otherwise, leave them around for inspection
                            ok
                    end,
                    NumUpdates = S#state.num_updates,
                    pretty_commands(?MODULE, Cmds, HSR,
                                    ?WHENFAIL(
                                       begin
                       {Segments, Width, MemLevels} = S#state.params,
                       eqc:format("Segments ~p\nWidth ~p\nMemLevels ~p\n",
                              [Segments, Width, MemLevels]),
                       eqc:format("=== t1 ===\n~p\n\n", [ets:tab2list(t1)]),
                       eqc:format("=== s1 ===\n~p\n\n", [safe_tab2list(s1)]),
                       eqc:format("=== t2 ===\n~p\n\n", [ets:tab2list(t2)]),
                       eqc:format("=== s2 ===\n~p\n\n", [safe_tab2list(s2)]),
                       eqc:format("=== ht1 ===\n~w\n~p\n\n", [get(t1), catch dump(get(t1))]),
                       eqc:format("=== ht2 ===\n~w\n~p\n\n", [get(t2), catch dump(get(t2))])
                                       end,
                                       measure(num_updates, NumUpdates,
                                       measure(segment_fill_ratio, NumUpdates / (2 * Segments), % Est of avg fill rate per segment
                       collect(with_title(mem_levels), MemLevels,
                                       collect(with_title(segments), Segments,
                                       collect(with_title(width), Width,
                                               equals(ok, Res))))))))
        end)).

cleanup_hashtree(HT) ->
    Path = hashtree:path(HT),
    HT2 = hashtree:close(HT),
    hashtree:destroy(HT2),
    ok = file:del_dir(Path).



dump(Tree) ->
    Fun = fun(Entries) ->
                  Entries
          end,
    {SnapTree, _Tree2} = hashtree:update_snapshot(Tree),
    hashtree:multi_select_segment(SnapTree, ['*','*'], Fun).


%% Force a final comparison to make sure we get a comparison test
%% for every case we try, as that is after all the point of the module.
final_compare(S) ->
    maybe_update_tree(S#state.snap1, t1, s1),
    maybe_update_tree(S#state.snap2, t2, s2),
    Expect = expect_compare(),
    case lists:sort(hashtree:local_compare(get(t1), get(t2))) of
        Expect ->
            ok;
        Result ->
            {Expect, '/=', Result}
    end.

maybe_update_tree(_SnapState = undefined, Tree, Snap) ->
    update_tree(Tree, Snap); % snapshot and calculate
maybe_update_tree(_SnapState = created, Tree, _Snap) ->
    update_perform(Tree);
maybe_update_tree(_SnapState = updated, _Tree, _Snap) ->
    ok. % Do nothing - waiting for setting rebuild status


expect_compare() ->
    Snap1 = orddict:from_list(ets:tab2list(s1)),
    Snap2 = orddict:from_list(ets:tab2list(s2)),
    SnapDeltas = riak_core_util:orddict_delta(Snap1, Snap2),

    lists:sort(
      [{missing, K} || {K, {'$none', _}} <- SnapDeltas] ++
      [{remote_missing, K} || {K, {_, '$none'}} <- SnapDeltas] ++
          [{different, K} || {K, {V1, V2}} <- SnapDeltas, V1 /= '$none', V2 /= '$none']). %% UNDO SnapDeltas this line
%%
%% Functions for handling the model data stored in ETS tables
%%

ets_tables() -> [t1, s1, t2, s2].

delete_ets() ->
    [catch ets:delete(X) || X <- ets_tables()],
    ok.

create_ets() ->
    [ets_new(X) || X <- ets_tables()],
    ok.


%% Create ETS table with public options
ets_new(T) ->
    ets:new(T, [named_table, public, set]).

%% Convert a table to a list falling back to undefined for printing
%% failure state.
safe_tab2list(Id) ->
    try
        ets:tab2list(Id)
    catch
        _:_ ->
            undefined
    end.

%% Copy the model data from the live to snapshot table for
%% update_tree/update_snapshot.
copy_tree(T, S) ->
    catch ets:delete(S),
    ets_new(S),
    ets:insert(S, ets:tab2list(T)),
    ok.

-endif.
-endif.
