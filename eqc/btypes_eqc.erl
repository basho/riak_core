%% -------------------------------------------------------------------
%%
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
-module(btypes_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-type type_name() :: binary().
-type type_active_status() :: boolean().
-type type_prop_name() :: binary().
-type type_prop_val()  :: boolean().

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-record(state, {
          %% a list of properties we have created
          types :: [{type_name(), type_active_status(), [{type_prop_name(), type_prop_val()}]}]
         }).

btypes_test_() -> {
        timeout, 120,
        ?_test(?assert(
            eqc:quickcheck(?QC_OUT(eqc:numtests(100, prop_btype_invariant())))))
    }.

run_eqc() ->
    run_eqc(100).

run_eqc(N) ->
    eqc:quickcheck(eqc:numtests(N, prop_btype_invariant())).

run_check() ->
    eqc:check(prop_btype_invariant()).

%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{
       types = []
      }.

%% ------ Grouped operator: create_type
%% @doc create_type args generator
create_type_args(S) ->
    [type_name(S), props()].

%% @doc create_type next state function
create_type_next(S=#state{types=Types}, _Value, [TypeName, CreateProps]) ->
    case type_active(TypeName, S) orelse invalid_props(TypeName, CreateProps, S) of
        true -> S;
        false ->
            S#state{ types = lists:keystore(TypeName, 1, Types, {TypeName, false, CreateProps}) }
    end.

%% @doc create_type command
create_type(TypeName, Props) ->
    riak_core_bucket_type:create(TypeName, Props).

%% @doc create_type postcondition
create_type_post(S, [TypeName, Props], Res) ->
    Active = type_active(TypeName, S),
    InvalidProps = invalid_props(TypeName, Props, S),
    case {Active, InvalidProps} of
        {true, _} -> eq({error, already_active}, Res);
        {_, true} ->
            case Res of
                {error, _} -> true;
                _ -> {bad_error_res, Res}
            end;
        {false, false} -> eq(ok, Res)
    end.

%% ------ Grouped operator: activate_type
%% @doc activate_type args generator
activate_type_args(#state{types=[]}) ->
    %% test negative case when no types exist
    [new_type_name()];
activate_type_args(S) ->
    %% TODO: generate fault for type that dne
    [existing_type_name(S)].

%% @doc activate_type next state function
activate_type_next(S=#state{types=Types}, _Value, [TypeName]) ->
    case type(TypeName, S) of
        undefined -> S;
        {TypeName, _, Props} ->
            S#state{ types = lists:keyreplace(TypeName, 1, Types, {TypeName, true, Props}) }
    end.

%% @doc activate_type command
activate_type(TypeName) ->
    riak_core_bucket_type:activate(TypeName).

%% @doc activate_type postcondition
activate_type_post(S, [TypeName], Res) ->
    Expected = case type(TypeName, S) of
                   undefined -> {error, undefined};
                   %% always ok if type exists b/c we spend no time in created state in this test, currently
                   {TypeName, _, _} -> ok
               end,
    eq(Expected, Res).

%% ------ Grouped operator: update_type
%% @doc update_type args generator
update_type_args(S) ->
    [type_name(S), props()].

%% @doc update_type next state function
update_type_next(S=#state{types=Types}, _Value, [TypeName, UpdateProps]) ->
    case type_active(TypeName, S) of
        false -> S;
        true ->
            case invalid_props(TypeName, UpdateProps, S) of
                true -> S;
                false ->
                    {TypeName, true, SetProps} = type(TypeName, S),
                    NewProps = lists:ukeysort(1, UpdateProps ++ SetProps),
                    S#state{ types = lists:keystore(TypeName, 1, Types, {TypeName, true, NewProps}) }
            end
    end.

%% @doc update_type command
update_type(TypeName, Props) ->
    riak_core_bucket_type:update(TypeName, Props).

%% @doc update_type postcondition
update_type_post(S, [TypeName, Props], Res) ->
    Active = type_active(TypeName, S),
    InvalidProps = invalid_props(TypeName, Props, S),
    case {Active, InvalidProps} of
        {false, _} -> eq({error, not_active}, Res);
        {_, true} ->
            case Res of
                {error, _} -> true;
                _ -> {bad_error_res, Res}
            end;
        {true, false} -> eq(ok, Res)
    end.

%% ------ Grouped operator: set_bucket
%% @doc set_bucket args generator
set_bucket_args(#state{types=[]}) ->
    %% test negative case when we have no types
    [{new_type_name(), bucket_name()}, props()];
set_bucket_args(S) ->
    %% TODO: introduce fault to test non-existent types
    [{existing_type_name(S), bucket_name()}, props()].

set_bucket(Bucket, Props) ->
    riak_core_bucket:set_bucket(Bucket, Props).

%% @doc set_bucket postcondition
set_bucket_post(S, [{BucketType, _BucketName}, Props], Res) ->
    Active = type_active(BucketType, S),
    InvalidProps = invalid_props(BucketType, Props, S),
    case {Active, InvalidProps} of
        {false, _} -> eq({error, no_type}, Res);
        {_, true} ->
            case Res of
                {error, _} -> true;
                _ -> {bad_error_res, Res}
            end;
        {true, false} -> eq(ok, Res)
    end.

%% ------ Grouped operator: type_status
%% @doc type_status args generator
type_status_args(#state{types=[]}) ->
    %% if no types, test negative case
    [new_type_name()];
type_status_args(S) ->
    %% TODO: add fault that sometimes generates missing type name for negative testing
    [existing_type_name(S)].

%% @doc type_status command
type_status(TypeName) ->
    riak_core_bucket_type:status(TypeName).

%% @doc type_status postcondition
type_status_post(S, [TypeName], Res) ->
    eq(expected_status(TypeName, S), Res).

%% ------ Grouped operator: get_type
%% @doc get_type args generator
get_type_args(#state{types=[]}) ->
    %% generate negative case when there are no types to get
    [new_type_name()];
get_type_args(S) ->
    %% TODO: use faults to generate negative case
    [existing_type_name(S)].

%% @doc get_type command
get_type(TypeName) ->
    riak_core_bucket_type:get(TypeName).

%% @doc get_type postcondition
get_type_post(S, [TypeName], Res) ->
    case type_active(TypeName, S) of
        false-> eq(undefined, Res);
        true ->
            ExpectedDefaults = riak_core_bucket_type:defaults(),
            {TypeName, true, ExpectedProps} = type(TypeName, S),
            ExpectedAll = lists:ukeysort(1, [{active, true}, {claimant, node()}] ++ ExpectedProps ++ ExpectedDefaults),
            %% properties expected (from state) but not found
            Missing = [Prop || Prop <- ExpectedAll, not lists:member(Prop, Res)],
            %% properties found but not expected (from state)
            Extra = [Prop || Prop <- Res, not lists:member(Prop, ExpectedAll)],
            good_props(Missing, Extra)
    end.

%% ------ Grouped operator: fold

%% @doc fold args generator
fold_args(_S) -> [].

%% @doc fold command
fold() ->
    ordsets:from_list(riak_core_bucket_type:fold(fun folder/2, [])).

%% @doc fold callback which simply picks out the bucket type
folder({BType, _BProps}, Accum) ->
    [BType | Accum].

%% @doc fold postcondition
fold_post(#state{types=Types} = _S, [], Res) ->
    eq(ordsets:from_list([BType || {BType, _Status, _BProps} <- Types]), Res).

%% ------ test helpers

expected_status(TypeName, S) ->
    case type(TypeName, S) of
        undefined -> undefined;
        %% TODO: incorporate other nodes so we can include created status?
        {TypeName, false, _} -> ready;
        {TypeName, true, _} -> active
    end.

type_active(TypeName, S) ->
    case type(TypeName, S) of
        undefined -> false;
        {TypeName, Active, _} -> Active
    end.

type(TypeName, #state{types=Types}) ->
    case lists:keyfind(TypeName, 1, Types) of
        false -> undefined;
        TypeDetails ->  TypeDetails
    end.

good_props([], []) ->
    true;
good_props([], Extra) ->
    {extra_props, Extra};
good_props(Missing, []) ->
    {missing_props, Missing};
good_props(Missing, Extra) ->
    {bad_props, {missing, Missing}, {extra, Extra}}.

invalid_props(TypeName, Props, S) ->
    %% properties are invalid if they include modifications to claimant or active property
    HasClaimant = false =/= lists:keyfind(claimant, 1, Props),
    HasActive = case lists:keyfind(active, 1, Props) of
                    false -> false;
                    {active, Active} ->
                        %% if included, cannot modify state of active
                        not (Active =:= type_active(TypeName, S))
                end,
    HasClaimant orelse HasActive.

%% Generators

type_name(#state{types=Types}) ->
    ExistingTypes = [Name || {Name, _, _} <- Types],
    ExistingGen = [ oneof(ExistingTypes) || length(ExistingTypes) > 0 ],
    oneof([new_type_name() | ExistingGen]).


existing_type_name(#state{types=Types}) ->
    ExistingTypes = [Name || {Name, _, _} <- Types],
    %% assumes Types is non-empty
    oneof(ExistingTypes).

new_type_name() ->
    ?SUCHTHAT(X, binary(10), X =/= <<"any">>).

bucket_name() ->
    binary(10).

props() ->
    fault_rate(1, 10, ?LET(Props, list(prop()), fault([immutable_core_prop() | Props], Props))).

prop() ->
    {a_prop_name(), a_prop_value()}.

a_prop_name() ->
    binary(10).

a_prop_value() ->
    bool().

immutable_core_prop() ->
    %% use a boolean for both values because thats what we use for other props,
    %% the value doesn't really matter, except for active 'false' is a valid value
    %% which we generate sometimes just to make sure
    oneof([{active, bool()}, {claimant, true}]).

%% @doc weight/2 - Distribution of calls
weight(_S, create_type) -> 3;
weight(_S, activate_type) -> 3;
weight(_S, type_status) -> 1;
weight(_S, get_type) -> 1;
weight(_S, fold) -> 1;
weight(_S, _Cmd) -> 1.

%% @doc the property
prop_btype_invariant() ->
    ?FORALL(Cmds, commands(?MODULE),
            aggregate(command_names(Cmds),
                      ?TRAPEXIT(
                         begin
                             {H, S, Res} =
                                 bucket_eqc_utils:per_test_setup([],
                                     fun() ->
                                             run_commands(?MODULE,Cmds)
                                     end),
                             pretty_commands(?MODULE, Cmds, {H, S, Res},
                                             Res == ok)
                         end
                        ))).

-endif.
