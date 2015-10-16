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
    binary(4).  % original used ?LET(Key, int(), ?MODULE:integer_to_binary(Key)).

object(_S) ->
    {key(), sha(term_to_binary(make_ref()))}.

command(S = #state{started = Started}) ->
    frequency(
        [{1, {call, ?MODULE, start, [{oneof(?POWERS), oneof(?POWERS), choose(0, 4)}]}} || not Started] ++
        [{1, {call, ?MODULE, update_tree, [t1]}} || Started] ++
        [{1, {call, ?MODULE, update_tree, [t2]}} || Started] ++
        [{50, {call, ?MODULE, write, [t1, object(S)]}} || Started] ++
        [{50, {call, ?MODULE, write, [t2, object(S)]}} || Started] ++
        [{100,{call, ?MODULE, write_both, [object(S)]}} || Started] ++
        %% [{50, {call, ?MODULE, delete, [t1, key()]}} || Started] ++
        %% [{50, {call, ?MODULE, delete, [t2, key()]}} || Started] ++
        %% [{100,{call, ?MODULE, delete_both, [key()]}} || Started] ++
        %% [{1, {call, ?MODULE, reopen_tree, [S#state.params, t1]}} || Started] ++
        %% [{1, {call, ?MODULE, reopen_tree, [S#state.params, t2]}} || Started] ++
          [{1, {call, ?MODULE, reconcile, []}} || Started] ++
          %% TODO: Add rehash_tree
        []
    ).

start(Params) ->
    {Segments, Width, MemLevels} = Params,
    %% Return now so we can store symbolic value in procdict in next_state call
    put(t1, hashtree:new({0,0}, [{segments, Segments},
                                 {width, Width},
                                 {mem_levels, MemLevels}])),
    put(t2, hashtree:new({0,0}, [{segments, Segments},
                                 {width, Width},
                                 {mem_levels, MemLevels}])),
    ets:new(t1, [named_table, public, set]),
    ets:new(t2, [named_table, public, set]),
    true.

write(T, {Key, Hash}) ->
    put(T, hashtree:insert(Key, Hash, get(T))), % write to tree and return previous state
    ets:insert(T, {Key, Hash}),
    ok.

write_both({_Key, _Hash}=KH) ->
    write(t1, KH),
    write(t2, KH),
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

reopen_tree(Params, T) ->
    {Segments, Width, MemLevels} = Params,
    HT = hashtree:flush_buffer(get(T)),
    Path = hashtree:path(HT),
    hashtree:close(HT),
    put(T, hashtree:new({0,0}, [{segments, Segments},
                                {width, Width},
                                {mem_levels, MemLevels},
                                {segment_path, Path}])),
    ok.


reconcile() ->
    put(t1, hashtree:update_tree(get(t1))),
    put(t2, hashtree:update_tree(get(t2))),
    KeyDiff = hashtree:local_compare(get(t1), get(t2)),
    Missing = [M || {missing, M} <- KeyDiff],
    RemoteMissing = [M || {remote_missing, M} <- KeyDiff],
    Different = [D || {different, D} <- KeyDiff],

    Insert = fun(T, Vals) ->
                     lists:foldl(fun({Key, Hash}, Acc) ->
                                         write(T, {Key, Hash}),
                                         Acc
                        end, T, Vals)
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


%% write_differing(Tree1, Tree2, {Key, Hash1}, Hash2) ->
%%     {{Key, Hash1}, {Key, Hash2}, hashtree:insert(Key, Hash1, Tree1),
%%         hashtree:insert(Key, Hash2, Tree2)}.

precondition(#state{started = Started}, {call, _, F, _A}) ->
    case F of
        start ->
            Started == false;
        _ ->
            Started == true
    end.

postcondition(_S,{call,_,_,_},_R) ->
    true.

next_state(S,_R,{call, _, start, [Params]}) ->
    S#state{started = true, params = Params};
next_state(S,_V,{call, _, write, [t1, {Key, Val}]}) ->
    S#state{only1=[{Key, Val}|lists:keydelete(Key, 1,
                S#state.only1)]};
next_state(S,_V,{call, _, write, [t2, {Key, Val}]}) ->
    S#state{only2=[{Key, Val}|lists:keydelete(Key, 1,
                S#state.only2)]};
next_state(S,_R,{call, _, write_both, [{Key, Val}]}) ->
    S#state{only1=[{Key, Val}|lists:keydelete(Key, 1, S#state.only1)],
            only2=[{Key, Val}|lists:keydelete(Key, 1, S#state.only2)]
    };
next_state(S,_V,{call, _, delete, [t1, Key]}) ->
    S#state{only1=lists:keydelete(Key, 1, S#state.only1)};
next_state(S,_V,{call, _, delete, [t2, Key]}) ->
    S#state{only2=lists:keydelete(Key, 1, S#state.only2)};
next_state(S,_R,{call, _, delete_both, [Key]}) ->
    S#state{only1=lists:keydelete(Key, 1, S#state.only1),
            only2=lists:keydelete(Key, 1, S#state.only2)};
next_state(S,_R,{call, _, reconcile, []}) ->
    Keys = lists:ukeymerge(1, lists:ukeysort(1, S#state.only1),
                           lists:ukeysort(1, S#state.only2)),
    S#state{only1 = Keys,
            only2 = Keys};
next_state(S,_R,{call, _, update_tree, _A}) ->
    S.


prop_correct() ->
    ?FORALL(Cmds,non_empty(commands(?MODULE, #state{})),
            aggregate(command_names(Cmds),
                begin
                    %io:format(user, "Starting in ~p\n", [self()]),
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

                                %% io:format(user, "Checks in ~p\nS: ~p\nT1: ~p\nT2: ~p\n",
                                %%           [self(), S, get(t1), get(t2)]),

                                case S#state.started of
                                    false ->
                                        true;
                                    _ ->
                                        T1 = hashtree:update_tree(get(t1)),
                                        T2 = hashtree:update_tree(get(t2)),

                                        KeyDiff = hashtree:local_compare(T1, T2),

                                        %io:format(user, "Expected: ~p\nKeyDiff: ~p\n", [Expected, KeyDiff]),

                                        D1 = dump(T1),
                                        D2 = dump(T2),
                                        catch hashtree:destroy(hashtree:close(T1)),
                                        catch hashtree:destroy(hashtree:close(T2)),
                                        {Segments, Width, MemLevels} = S#state.params,

                                        ?WHENFAIL(
                                           begin
                                               eqc:format("t1:\n~p\n", [D1]),
                                               eqc:format("t2:\n~p\n", [D2])
                                           end,
                                           collect(with_title(mem_levels), MemLevels,
                                           collect(with_title(segments), Segments,
                                           collect(with_title(width), Width,
                                           collect(with_title(length), length(S#state.only1) +
                                                      length(S#state.only2),
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


-endif.
-endif.
