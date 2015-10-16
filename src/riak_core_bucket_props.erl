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
         validate/4,
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


-spec validate(create | update,
               {riak_core_bucket_type:bucket_type(), undefined | binary()} | binary(),
               undefined | [{atom(), any()}],
               [{atom(), any()}]) -> {ok, [{atom(), any()}]} | {error, [{atom(), atom()}]}.
validate(CreateOrUpdate, Bucket, ExistingProps, BucketProps) ->
    ReservedErrors = validate_reserved_names(Bucket),
    CoreErrors = validate_core_props(CreateOrUpdate, Bucket, ExistingProps, BucketProps),
    validate(CreateOrUpdate, Bucket, ExistingProps, BucketProps, riak_core:bucket_validators(), [ReservedErrors, CoreErrors]).

validate(_CreateOrUpdate, _Bucket, _ExistingProps, Props, [], ErrorLists) ->
    case lists:flatten(ErrorLists) of
        [] -> {ok, Props};
        Errors -> {error, Errors}
    end;
validate(CreateOrUpdate, Bucket, ExistingProps, BucketProps0, [{_App, Validator}|T], Errors0) ->
    {BucketProps, Errors} = Validator:validate(CreateOrUpdate, Bucket, ExistingProps, BucketProps0),
    validate(CreateOrUpdate, Bucket, ExistingProps, BucketProps, T, [Errors|Errors0]).

validate_core_props(CreateOrUpdate, Bucket, ExistingProps, BucketProps) ->
    lists:foldl(fun(Prop, Errors) ->
                        case validate_core_prop(CreateOrUpdate, Bucket, ExistingProps, Prop) of
                            true ->  Errors;
                            Error -> [Error | Errors]
                        end
                end, [], BucketProps).

validate_core_prop(create, {_Bucket, undefined}, undefined, {claimant, Claimant}) when Claimant =:= node()->
    %% claimant valid on first call to create if claimant is this node
    true;
validate_core_prop(create, {_Bucket, undefined}, undefined, {claimant, _BadClaimant}) ->
    %% claimant not valid on first call to create if claimant is not this node
    {claimant, "Invalid claimant"};
validate_core_prop(create, {_Bucket, undefined}, Existing, {claimant, Claimant}) ->
    %% subsequent creation calls cannot modify claimant and it should exist
    case lists:keyfind(claimant, 1, Existing) of
        false -> {claimant, "No claimant details found in existing properties"};
        {claimant, Claimant} -> true;
        {claimant, _Other} -> {claimant, "Cannot modify claimant property"}
    end;
validate_core_prop(update, {_Bucket, _BucketName}, _Existing, {claimant, _Claimant}) ->
    %% cannot update claimant
    {claimant, "Cannot update claimant property"};
validate_core_prop(update, {_Bucket, _BucketName}, _Existing, {ddl, _DDL}) ->
    %% cannot update time series DDL
    {ddl, "Cannot update time series data definition"};
validate_core_prop(update, {_Bucket, _BucketName}, _Existing, {table_def, _DDL}) ->
    %% cannot update time series DDL (or, if it slips past riak_kv_console,
    %% the table_def SQL(ish) code that is parsed to make a DDL)
    %%
    %% Defining the table_def atom here also sidesteps occasional
    %% errors from existing_atom functions
    {ddl, "Cannot update time series data definition"};
validate_core_prop(create, {_Bucket, undefined}, undefined, {active, false}) ->
    %% first creation call that sets active to false is always valid
    true;
validate_core_prop(create, {_Bucket, undefined}, _Existing, {active, false}) ->
    %% subsequent creation calls that leaves active false is valid
    true;
validate_core_prop(update, {_Bucket, _}, _Existing, {active, true}) ->
    %% calls to update that do not modify active are valid
    true;
validate_core_prop(_, {_Bucket, _}, _Existing, {active, _}) ->
    %% subsequent creation calls or update calls cannot modify active (it is modified directly
    %% by riak_core_claimant)
    {active, "Cannot modify active property"};
validate_core_prop(_, _, _, _) ->
    %% all other properties are valid from the perspective of riak_core
    true.

validate_reserved_names(Bucket) ->
    case validate_reserved_name(Bucket) of
        ok -> [];
        ErrStr -> [{reserved_name, ErrStr}]
    end.

validate_reserved_name({<<"any">>, _}) ->
    "The name 'any' may not be used for bucket types";
validate_reserved_name(_) ->
    ok.

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
                                        {RestB, [{KeyA, resolve_prop(PropA, PropB)} | Acc]};
                                   (PropA, {RestB, Acc}) ->
                                        {RestB, [PropA | Acc]}
                                end,
                                {PropsBSorted, []},
                                PropsASorted),
    Resolved.

resolve_prop({allow_mult, Mult1}, {allow_mult, Mult2}) ->
    Mult1 orelse Mult2; %% assumes allow_mult=true is default
resolve_prop({basic_quorum, Basic1}, {basic_quorum, Basic2}) ->
    Basic1 andalso Basic2;
resolve_prop({big_vclock, Big1}, {big_vclock, Big2}) ->
    max(Big1, Big2);
resolve_prop({chash_keyfun, KeyFun1}, {chash_keyfun, _KeyFun2}) ->
    KeyFun1; %% arbitrary choice
resolve_prop({dw, DW1}, {dw, DW2}) ->
    %% 'quorum' wins over set numbers
    max(DW1, DW2);
resolve_prop({last_write_wins, LWW1}, {last_write_wins, LWW2}) ->
    LWW1 andalso LWW2;
resolve_prop({linkfun, LinkFun1}, {linkfun, _LinkFun2}) ->
    LinkFun1; %% arbitrary choice
resolve_prop({n_val, N1}, {n_val, N2}) ->
    max(N1, N2);
resolve_prop({notfound_ok, NF1}, {notfound_ok, NF2}) ->
    NF1 orelse NF2;
resolve_prop({old_vclock, Old1}, {old_vclock, Old2}) ->
    max(Old1, Old2);
resolve_prop({postcommit, PC1}, {postcommit, PC2}) ->
    resolve_hooks(PC1, PC2);
resolve_prop({pr, PR1}, {pr, PR2}) ->
    max(PR1, PR2);
resolve_prop({precommit, PC1}, {precommit, PC2}) ->
    resolve_hooks(PC1, PC2);
resolve_prop({pw, PW1}, {pw, PW2}) ->
    max(PW1, PW2);
resolve_prop({r, R1}, {r, R2}) ->
    max(R1, R2);
resolve_prop({rw, RW1}, {rw, RW2}) ->
    max(RW1, RW2);
resolve_prop({small_vclock, Small1}, {small_vclock, Small2}) ->
    max(Small1, Small2);
resolve_prop({w, W1}, {w, W2}) ->
    max(W1, W2);
resolve_prop({young_vclock, Young1}, {young_vclock, Young2}) ->
    max(Young1, Young2);
resolve_prop({_, V1}, {_, _V2}) ->
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

