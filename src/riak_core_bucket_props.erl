%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_core_bucket_props).

-export([merge/2,
         validate/1,
         resolve/2,
         defaults/0,
         append_defaults/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec merge([{atom(), any()}], [{atom(), any()}]) -> [{atom(), any()}].
merge(Overriding, Other) ->
    lists:ukeymerge(1, lists:ukeysort(1, Overriding),
                    lists:ukeysort(1, Other)).


-spec validate([{atom(), any()}]) -> {ok, [{atom(), any()}]} |
                                     {error, [{atom(), atom()}]}.
validate(BucketProps) ->
    validate(BucketProps, riak_core:bucket_validators(), []).

validate(BucketProps, [], []) ->
    {ok, BucketProps};
validate(_, [], Errors) ->
    {error, Errors};
validate(BucketProps0, [{_App, Validator}|T], Errors0) ->
    {BucketProps, Errors} = Validator:validate(BucketProps0),
    validate(BucketProps, T, lists:flatten([Errors|Errors0])).


-spec defaults() -> [{atom(), any()}].
defaults() ->
    app_helper:get_env(riak_core, default_bucket_props).

-spec append_defaults([{atom(), any()}]) -> ok.
append_defaults(Items) when is_list(Items) ->
    OldDefaults = app_helper:get_env(riak_core, default_bucket_props, []),
    NewDefaults = merge(OldDefaults, Items),
    FixedDefaults = case riak_core:bucket_fixups() of
        [] -> NewDefaults;
        Fixups ->
            riak_core_ring_manager:run_fixups(Fixups, default, NewDefaults)
    end,
    application:set_env(riak_core, default_bucket_props, FixedDefaults),
    %% do a noop transform on the ring, to make the fixups re-run
    catch(riak_core_ring_manager:ring_trans(fun(Ring, _) ->
                                                    {new_ring, Ring}
                                            end, undefined)),
    ok.

-spec resolve([{atom(), any()}], [{atom(), any()}]) -> [{atom(), any()}].
resolve(PropsA, PropsB) when is_list(PropsA) andalso
                             is_list(PropsB) ->
    PropsASorted = lists:ukeysort(1, PropsA),
    PropsBSorted = lists:ukeysort(1, PropsB),
    {_, Resolved} = lists:foldl(fun({KeyA, _}=PropA, {[{KeyA, _}=PropB | RestB], Acc}) ->
                                        {RestB, [{KeyA, resolve(PropA, PropB)} | Acc]};
                                   (PropA, {RestB, Acc}) ->
                                        {RestB, [PropA | Acc]}
                                end,
                                {PropsBSorted, []},
                                PropsASorted),
    Resolved;
resolve({allow_mult, Mult1}, {allow_mult, Mult2}) ->
    Mult1 orelse Mult2; %% assumes allow_mult=true is default
resolve({basic_quorum, Basic1}, {basic_quorum, Basic2}) ->
    Basic1 andalso Basic2;
resolve({big_vclock, Big1}, {big_vclock, Big2}) ->
    case Big1 > Big2 of
        true -> Big1;
        false -> Big2
    end;
resolve({chash_keyfun, KeyFun1}, {chash_keyfun, _KeyFun2}) ->
    KeyFun1; %% arbitrary choice
resolve({dw, DW1}, {dw, DW2}) ->
    %% 'quorum' wins over set numbers
    case DW1 > DW2 of
        true -> DW1;
        false -> DW2
    end;
resolve({last_write_wins, LWW1}, {last_write_wins, LWW2}) ->
    LWW1 andalso LWW2;
resolve({linkfun, LinkFun1}, {linkfun, _LinkFun2}) ->
    LinkFun1; %% arbitrary choice
resolve({n_val, N1}, {n_val, N2}) ->
    case N1 > N2 of
        true -> N1;
        false -> N2
    end;
resolve({notfound_ok, NF1}, {notfound_ok, NF2}) ->
    NF1 orelse NF2;
resolve({old_vclock, Old1}, {old_vclock, Old2}) ->
    case Old1 > Old2 of
        true -> Old1;
        false -> Old2
    end;
resolve({postcommit, PC1}, {postcommit, PC2}) ->
    resolve_hooks(PC1, PC2);
resolve({pr, PR1}, {pr, PR2}) ->
    case PR1 > PR2 of
        true -> PR1;
        false -> PR2
    end;
resolve({precommit, PC1}, {precommit, PC2}) ->
    resolve_hooks(PC1, PC2);
resolve({pw, PW1}, {pw, PW2}) ->
    case PW1 > PW2 of
        true -> PW1;
        false -> PW2
    end;
resolve({r, R1}, {r, R2}) ->
    case R1 > R2 of
        true -> R1;
        false -> R2
    end;
resolve({rw, RW1}, {rw, RW2}) ->
    case RW1 > RW2 of
        true -> RW1;
        false -> RW2
    end;
resolve({small_vclock, Small1}, {small_vclock, Small2}) ->
    case Small1 > Small2 of
        true -> Small1;
        false -> Small2
    end;
resolve({w, W1}, {w, W2}) ->
    case W1 > W2 of
        true -> W1;
        false -> W2
    end;
resolve({young_vclock, Young1}, {young_vclock, Young2}) ->
    case Young1 > Young2 of
        true -> Young1;
        false -> Young2
    end;
resolve({_, V1}, {_, _V2}) ->
    V1.

resolve_hooks(Hooks1, Hooks2) ->
    lists:usort(Hooks1 ++ Hooks2).

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

simple_resolve_test() ->
    Props1 = [{name,<<"test">>},
              {allow_mult,false},
              {basic_quorum,false},
              {big_vclock,50},
              {chash_keyfun,{riak_core_util,chash_std_keyfun}},
              {dw,quorum},
              {last_write_wins,false},
              {linkfun,{modfun,riak_kv_wm_link_walker,mapreduce_linkfun}},
              {n_val,3},
              {notfound_ok,true},
              {old_vclock,86400},
              {postcommit,[]},
              {pr,0},
              {precommit,[{a, b}]},
              {pw,0},
              {r,quorum},
              {rw,quorum},
              {small_vclock,50},
              {w,quorum},
              {young_vclock,20}],
    Props2 = [{name,<<"test">>},
              {allow_mult, true},
              {basic_quorum, true},
              {big_vclock,60},
              {chash_keyfun,{riak_core_util,chash_std_keyfun}},
              {dw,3},
              {last_write_wins,true},
              {linkfun,{modfun,riak_kv_wm_link_walker,mapreduce_linkfun}},
              {n_val,5},
              {notfound_ok,false},
              {old_vclock,86401},
              {postcommit,[{a, b}]},
              {pr,1},
              {precommit,[{c, d}]},
              {pw,3},
              {r,3},
              {rw,3},
              {w,1},
              {young_vclock,30}],
    Expected = [{name,<<"test">>},
                {allow_mult,true},
                {basic_quorum,false},
                {big_vclock,60},
                {chash_keyfun,{riak_core_util,chash_std_keyfun}},
                {dw,quorum},
                {last_write_wins,false},
                {linkfun,{modfun,riak_kv_wm_link_walker,mapreduce_linkfun}},
                {n_val,5},
                {notfound_ok,true},
                {old_vclock,86401},
                {postcommit,[{a, b}]},
                {pr,1},
                {precommit,[{a, b}, {c, d}]},
                {pw,3},
                {r,quorum},
                {rw,quorum},
                {small_vclock,50},
                {w,quorum},
                {young_vclock,30}],
    ?assertEqual(lists:ukeysort(1, Expected), lists:ukeysort(1, resolve(Props1, Props2))).

-endif.

