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
        tree1,
        tree2,
        only1 = [],
        only2 = [],
        both = [],
        segments,
        width,
        mem_levels
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

object(_S) ->
    {?LET(Key, int(), ?MODULE:integer_to_binary(Key)), sha(term_to_binary(make_ref()))}.

command(S) ->
    oneof(
        [{call, ?MODULE, start_1, [S]} || S#state.tree1 == undefined] ++
        [{call, ?MODULE, start_2, [S]} || S#state.tree2 == undefined] ++
        [{call, ?MODULE, write_1, [S#state.tree1, object(S)]} ||
            S#state.tree1 /= undefined] ++
        [{call, ?MODULE, write_2, [S#state.tree2, object(S)]} ||
            S#state.tree2 /= undefined] ++
        [{call, ?MODULE, write_both, [S#state.tree1, S#state.tree2, object(S)]} ||
            S#state.tree1 /= undefined, S#state.tree2 /= undefined] ++
        [{call, ?MODULE, update_tree_1, [S#state.tree1]} || S#state.tree1 /= undefined] ++
        [{call, ?MODULE, update_tree_2, [S#state.tree2]} || S#state.tree2 /= undefined] ++
        [{call, ?MODULE, reconcile, [S]} ||
                S#state.tree1 /= undefined, S#state.tree2 /= undefined] ++
        []
    ).

start_1(S) ->
    hashtree:new({0,0}, [{segments, S#state.segments}, {width,
                S#state.width}, {mem_levels, S#state.mem_levels}]).
start_2(S) ->
    hashtree:new({0,0}, [{segments, S#state.segments}, {width,
                S#state.width}, {mem_levels, S#state.mem_levels}]).

write_1(Tree, {Key, Hash}) ->
    hashtree:insert(Key, Hash, Tree).

write_2(Tree, {Key, Hash}) ->
    hashtree:insert(Key, Hash, Tree).

write_both(Tree1, Tree2, {Key, Hash}) ->
    {hashtree:insert(Key, Hash, Tree1), hashtree:insert(Key, Hash, Tree2)}.

update_tree_1(T1) ->
    hashtree:update_tree(T1).

update_tree_2(T2) ->
    hashtree:update_tree(T2).

reconcile(S) ->
    A2 = hashtree:update_tree(S#state.tree1),
    B2 = hashtree:update_tree(S#state.tree2),
    KeyDiff = hashtree:local_compare(A2, B2),
    Missing = [M || {missing, M} <- KeyDiff],
    RemoteMissing = [M || {remote_missing, M} <- KeyDiff],
    Different = [D || {different, D} <- KeyDiff],

    Insert = fun(Tree, Vals) ->
            lists:foldl(fun({Key, Hash}, Acc) ->
                        hashtree:insert(Key, Hash, Acc)
                end, Tree, Vals)
    end,

    A3 = Insert(A2, [lists:keyfind(K, 1, S#state.only2) ||  K <- Missing, lists:keyfind(K, 1,
                S#state.only2) /= false]),
    B3 = Insert(B2, [lists:keyfind(K, 1, S#state.only1) ||  K <- RemoteMissing, lists:keyfind(K, 1,
                S#state.only1) /= false]),
    B4 = Insert(B3, [lists:keyfind(K, 1, S#state.only1) ||  K <- Different, lists:keyfind(K, 1,
                S#state.only1) /= false]),
    Res = {hashtree:update_tree(A3), hashtree:update_tree(B4)},
    Res.


write_differing(Tree1, Tree2, {Key, Hash1}, Hash2) ->
    {{Key, Hash1}, {Key, Hash2}, hashtree:insert(Key, Hash1, Tree1),
        hashtree:insert(Key, Hash2, Tree2)}.

precondition(S,{call,_,start_1,_}) ->
    S#state.tree1 == undefined;
precondition(S,{call,_,start_2,_}) ->
    S#state.tree2 == undefined;
precondition(S,{call,_,write_1,_}) ->
    S#state.tree1 /= undefined;
precondition(S,{call,_,write_2,_}) ->
    S#state.tree2 /= undefined;
precondition(S,{call,_,write_both,_}) ->
    S#state.tree1 /= undefined andalso S#state.tree2 /= undefined;
precondition(S,{call,_,reconcile,_}) ->
    S#state.tree1 /= undefined andalso S#state.tree2 /= undefined;
precondition(S,{call,_,update_tree_1,_}) ->
    S#state.tree1 /= undefined;
precondition(S,{call,_,update_tree_2,_}) ->
    S#state.tree2 /= undefined.

postcondition(_S,{call,_,_,_},_R) ->
    true.

next_state(S,V,{call, _, start_1, [_]}) ->
    S#state{tree1=V, only1=[], both=[]};
next_state(S,V,{call, _, start_2, [_]}) ->
    S#state{tree2=V, only2=[], both=[]};
next_state(S,V,{call, _, write_1, [_, {Key, Val}]}) ->
    S#state{tree1=V, only1=[{Key, Val}|lists:keydelete(Key, 1,
                S#state.only1)]};
next_state(S,V,{call, _, write_2, [_, {Key, Val}]}) ->
    S#state{tree2=V, only2=[{Key, Val}|lists:keydelete(Key, 1,
                S#state.only2)]};
next_state(S,V,{call, _, update_tree_1, [_]}) ->
    S#state{tree1=V};
next_state(S,V,{call, _, update_tree_2, [_]}) ->
    S#state{tree2=V};
next_state(S,R,{call, _, write_both, [_, _, {Key, Val}]}) ->
    S#state{tree1={call, erlang, element, [1, R]},
        tree2={call, erlang, element, [2, R]},
        only1=[{Key, Val}|lists:keydelete(Key, 1, S#state.only1)],
        only2=[{Key, Val}|lists:keydelete(Key, 1, S#state.only2)]
    };
next_state(S,R,{call, _, reconcile, [_]}) ->
    Keys = lists:ukeymerge(1, lists:ukeysort(1, S#state.only1),
        lists:ukeysort(1, S#state.only2)),
    S#state{tree1={call, erlang, element, [1, R]},
        tree2={call, erlang, element, [2, R]},
        only1 = Keys,
        only2 = Keys
    }.


prop_correct() ->
    ?FORALL({Segments, Width, MemLevels}, {oneof(?POWERS), oneof(?POWERS), 4},
    ?FORALL(Cmds,commands(?MODULE, #state{segments=Segments*Segments, width=Width,
                mem_levels=MemLevels, only1=[], only2=[], both=[]}),
        ?TRAPEXIT(
            aggregate(command_names(Cmds),
                begin
                    {_H,S,Res} = HSR = run_commands(?MODULE,Cmds),
                    pretty_commands(?MODULE, Cmds, HSR,
                                    ?WHENFAIL(
                                       begin
                                           catch hashtree:destroy(hashtree:close(S#state.tree1)),
                                           catch hashtree:destroy(hashtree:close(S#state.tree2))
                                       end,
                            begin
                                    ?assertEqual(ok, Res),
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

                                    case S#state.tree1 == undefined orelse
                                        S#state.tree2 == undefined of
                                        true ->
                                            true;
                                        _ ->

                                            T1 = hashtree:update_tree(S#state.tree1),
                                            T2 = hashtree:update_tree(S#state.tree2),

                                            KeyDiff = hashtree:local_compare(T1, T2),

                                            ?assertEqual(lists:usort(Expected),
                                                lists:usort(KeyDiff)),

                                            catch hashtree:destroy(hashtree:close(T1)),
                                            catch hashtree:destroy(hashtree:close(T2)),
                                            true
                                    end
                            end
                                ))
                        end)))).

-endif.
-endif.
