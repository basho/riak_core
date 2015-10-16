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

-module(hashtree_eqc).
-compile([export_all]).

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-define(POWERS, [8, 16, 32, 64, 128, 256, 512, 1024]).

-include_lib("eunit/include/eunit.hrl").

hashtree_test_() ->
    {timeout, 60,
        fun() ->
                ?assert(eqc:quickcheck(?QC_OUT(eqc:testing_time(29,
                            hashtree_eqc:prop_correct()))))
        end
    }.

-record(state,
    {
      started = false,
      params = undefined,
      only1 = [],
      only2 = []
    }).

integer_to_binary(Int) ->
    list_to_binary(integer_to_list(Int)).

-ifndef(old_hash).
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

mark() ->
    frequency([{1, mark_empty}, {5, mark_open}]).

command(_S = #state{started = Started}) ->
    frequency(
        [{1, {call, ?MODULE, start,
              [{oneof(?POWERS), oneof(?POWERS), choose(0, 4)}, mark(), mark()]}} || not Started] ++
        [{1, {call, ?MODULE, update_tree, [t1]}} || Started] ++
        [{1, {call, ?MODULE, update_tree, [t2]}} || Started] ++
        [{50, {call, ?MODULE, write, [t1, objects()]}} || Started] ++
        [{50, {call, ?MODULE, write, [t2, objects()]}} || Started] ++
        [{100,{call, ?MODULE, write_both, [objects()]}} || Started] ++
        [{50, {call, ?MODULE, delete, [t1, key()]}} || Started] ++
        [{50, {call, ?MODULE, delete, [t2, key()]}} || Started] ++
        [{100,{call, ?MODULE, delete_both, [key()]}} || Started] ++
        [{1, {call, ?MODULE, reopen_tree, [t1]}} || Started] ++
        [{1, {call, ?MODULE, reopen_tree, [t2]}} || Started] ++
        [{1, {call, ?MODULE, unsafe_close, [t1]}} || Started] ++
        [{1, {call, ?MODULE, unsafe_close, [t2]}} || Started] ++
        [{1, {call, ?MODULE, rehash_tree, [t1]}} || Started] ++
        [{1, {call, ?MODULE, rehash_tree, [t2]}} || Started] ++
        [{1, {call, ?MODULE, reconcile, []}} || Started] ++
        []
    ).

start(Params, T1Mark, T2Mark) ->
    {Segments, Width, MemLevels} = Params,
    %% Return now so we can store symbolic value in procdict in next_state call
    HT1 = hashtree:new({0,0}, [{segments, Segments},
                               {width, Width},
                               {mem_levels, MemLevels}]),

    T1 = case T1Mark of
        mark_empty -> hashtree:mark_open_empty({0,0}, HT1);
        _ -> hashtree:mark_open_and_check({0,0}, HT1)
    end,

    put(t1, T1),

    HT2 = hashtree:new({0,0}, [{segments, Segments},
                               {width, Width},
                               {mem_levels, MemLevels}]),

    T2 = case T2Mark of
        mark_empty -> hashtree:mark_open_empty({0,0}, HT2);
        _ -> hashtree:mark_open_and_check({0,0}, HT2)
    end,

    put(t2, T2),

    ets:new(t1, [named_table, public, set]),
    ets:new(t2, [named_table, public, set]),
    ok.

write(T, Objects) ->
    lists:foreach(fun({Key, Hash}) ->
                          put(T, hashtree:insert(Key, Hash, get(T))),
                          ets:insert(T, {Key, Hash})
                  end, Objects),
    ok.

write_both(Objects) ->
    write(t1, Objects),
    write(t2, Objects),
    ok.

delete(T, Key) ->
    put(T, hashtree:delete(Key, get(T))),
    ets:delete(T, Key),
    ok.

delete_both(Key) ->
    delete(t1, Key),
    delete(t2, Key),
    ok.

update_tree(T) ->
    put(T, hashtree:update_tree(get(T))),
    ok.

rehash_tree(T) ->
    put(T, hashtree:rehash_tree(get(T))),
    ok.

reopen_tree(T) ->
    HT = hashtree:flush_buffer(get(T)),
    {Segments, Width, MemLevels} = {hashtree:segments(HT), hashtree:width(HT),
                                    hashtree:mem_levels(HT)},
    Path = hashtree:path(HT),

    UpdatedHT = hashtree:update_tree(HT),
    CleanClosedHT = hashtree:mark_clean_close({0,0}, UpdatedHT),
    hashtree:close(CleanClosedHT),

    T1 = hashtree:new({0,0}, [{segments, Segments},
                              {width, Width},
                              {mem_levels, MemLevels},
                              {segment_path, Path}]),

    put(T, hashtree:mark_open_and_check({0,0}, T1)),
    ok.

unsafe_close(T) ->
    HT = get(T),
    {Segments, Width, MemLevels} = {hashtree:segments(HT), hashtree:width(HT),
                                    hashtree:mem_levels(HT)},
    Path = hashtree:path(HT),
    %% Although this is an unsafe close, it's unsafe in metadata/building
    %% buckets.  Rather than model the queue behavior, flush those and just
    %% check the buckets are correctly recomputed next compare.
    hashtree:flush_buffer(HT),
    hashtree:fake_close(HT),

    T0 = hashtree:new({0,0}, [{segments, Segments},
                              {width, Width},
                              {mem_levels, MemLevels},
                              {segment_path, Path}]),

    put(T, hashtree:mark_open_and_check({0,0}, T0)),

    ok.

reconcile() ->
    put(t1, hashtree:update_tree(get(t1))),
    put(t2, hashtree:update_tree(get(t2))),
    KeyDiff = hashtree:local_compare(get(t1), get(t2)),
    Missing = [M || {missing, M} <- KeyDiff],
    RemoteMissing = [M || {remote_missing, M} <- KeyDiff],
    Different = [D || {different, D} <- KeyDiff],

    Insert = fun(T, Vals) ->
                 write(T, Vals)
             end,

    %% Lazy reuse of existing code - could be changed to check ETS directly
    Only1 = ets:tab2list(t1),
    Only2 = ets:tab2list(t2),

    Insert(t1, [lists:keyfind(K, 1, Only2) ||  K <- Missing, lists:keyfind(K, 1,
                Only2) /= false]),
    Insert(t2, [lists:keyfind(K, 1, Only1) ||  K <- RemoteMissing, lists:keyfind(K, 1,
                Only1) /= false]),
    Insert(t2, [lists:keyfind(K, 1, Only1) ||  K <- Different, lists:keyfind(K, 1,
                Only1) /= false]),
    ok.

precondition(#state{started = Started}, {call, _, F, _A}) ->
    case F of
        start ->
            Started == false;
        _ ->
            Started == true
    end.

postcondition(_S,{call,_,start, [_, T1Mark, T2Mark]},_R) ->
    NextRebuildT1 = hashtree:next_rebuild(get(t1)),
    NextRebuildT2 = hashtree:next_rebuild(get(t2)),
    case T1Mark of
        mark_empty -> eq(NextRebuildT1, incremental);
        _ -> eq(NextRebuildT1, full)
    end,
    case T2Mark of
        mark_empty -> eq(NextRebuildT2, incremental);
        _ -> eq(NextRebuildT2, full)
    end;
postcondition(_S,{call,_,_,_},_R) ->
    true.

next_state(S,_R,{call, _, start, [Params,_,_]}) ->
    S#state{started = true, params = Params};
next_state(S,_V,{call, _, write, [t1, Objects]}) ->
    S#state{only1=keymerge(Objects, S#state.only1)};
next_state(S,_V,{call, _, write, [t2, Objects]}) ->
    S#state{only2=keymerge(Objects, S#state.only2)};
next_state(S,_R,{call, _, write_both, [Objects]}) ->
    S#state{only1=keymerge(Objects, S#state.only1),
            only2=keymerge(Objects, S#state.only2)};
next_state(S,_V,{call, _, delete, [t1, Key]}) ->
    S#state{only1=lists:keydelete(Key, 1, S#state.only1)};
next_state(S,_V,{call, _, delete, [t2, Key]}) ->
    S#state{only2=lists:keydelete(Key, 1, S#state.only2)};
next_state(S,_R,{call, _, delete_both, [Key]}) ->
    S#state{only1=lists:keydelete(Key, 1, S#state.only1),
            only2=lists:keydelete(Key, 1, S#state.only2)};
next_state(S,_R,{call, _, reopen_tree, _A}) ->
    S;
next_state(S,_R,{call, _, unsafe_close, _A}) ->
    S;
next_state(S,_R,{call, _, rehash_tree, _A}) ->
    S;
next_state(S,_R,{call, _, reconcile, []}) ->
    Keys = keymerge(S),
    S#state{only1 = Keys,
            only2 = Keys};
next_state(S,_R,{call, _, update_tree, _A}) ->
    S.

prop_correct() ->
    ?FORALL(Cmds,non_empty(commands(?MODULE, #state{})),
            aggregate(command_names(Cmds),
                begin
                    %%io:format(user, "Starting in ~p\n", [self()]),
                    put(t1, undefined),
                    put(t2, undefined),
                    catch ets:delete(t1),
                    catch ets:delete(t2),
                    {_H,S,Res} = HSR = run_commands(?MODULE,Cmds),
                    pretty_commands(?MODULE, Cmds, HSR,
                                    ?WHENFAIL(
                                       begin
                                           catch hashtree:destroy(hashtree:close(get(t1))),
                                           catch hashtree:destroy(hashtree:close(get(t2)))
                                       end,
                            begin
                                    Unique1 = S#state.only1 -- S#state.only2,
                                    Unique2 = S#state.only2 -- S#state.only1,
                                    Expected =  [{missing, Key} || {Key, _} <-
                                        Unique2, not
                                        lists:keymember(Key, 1, S#state.only1)] ++
                                    [{remote_missing, Key} || {Key, _} <-
                                        Unique1, not
                                        lists:keymember(Key, 1, S#state.only2)] ++
                                    [{different, Key} || Key <-
                                        sets:to_list(sets:intersection(sets:from_list([Key
                                                        || {Key,_} <- Unique1]),
                                                sets:from_list([Key || {Key,_}
                                                    <- Unique2])))],

                                case S#state.started of
                                    false ->
                                        true;
                                    _ ->
                                        %% io:format("\nFinal update tree1:\n"),
                                        T10 = hashtree:update_tree(get(t1)),
                                        %% io:format("\nFinal update tree2:\n"),
                                        T20 = hashtree:update_tree(get(t2)),

                                        %% io:format("\nFinal compare:\n"),
                                        KeyDiff = hashtree:local_compare(T10, T20),

                                        D1 = try dump(T10) catch _:Err1 -> Err1 end,
                                        D2 = try dump(T20) catch _:Err2 -> Err2 end,

                                        T11 = hashtree:mark_clean_close({0,0}, T10),
                                        T21 = hashtree:mark_clean_close({0,0}, T20),
                                        catch hashtree:destroy(hashtree:close(T11)),
                                        catch hashtree:destroy(hashtree:close(T21)),
                                        {Segments, Width, MemLevels} = S#state.params,

                                        ?WHENFAIL(
                                           begin
                                               eqc:format("only1:\n~p\n", [S#state.only1]),
                                               eqc:format("only2:\n~p\n", [S#state.only2]),
                                               eqc:format("t1:\n~p\n", [D1]),
                                               eqc:format("t2:\n~p\n", [D2])
                                           end,
                                           collect(with_title(mem_levels), MemLevels,
                                           collect(with_title(segments), Segments,
                                           collect(with_title(width), Width,
                                           collect(with_title(length),
                                           length(S#state.only1) + length(S#state.only2),
                                                   conjunction([{cmds, equals(ok, Res)},
                                                                {diff, equals(lists:usort(Expected),
                                                                              lists:usort(KeyDiff))}]))))))
                                end
                            end
                                ))
                        end)).

dump(Tree) ->
    Fun = fun(Entries) ->
                  Entries
          end,
    {SnapTree, _Tree2} = hashtree:update_snapshot(Tree),
    hashtree:multi_select_segment(SnapTree, ['*','*'], Fun).

keymerge(S) ->
    keymerge(S#state.only1, S#state.only2).

keymerge(SuccList, L) ->
    lists:ukeymerge(1, lists:ukeysort(1, SuccList),
                    lists:ukeysort(1, L)).

-endif.
-endif.
