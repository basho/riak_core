%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Functions for manipulating bucket properties.
%% @type riak_core_bucketprops() = [{Propkey :: atom(), Propval :: term()}]

-module(riak_core_bucket).

-export([append_bucket_defaults/1,
         set_bucket/2,
         get_bucket/1,
         get_bucket/2,
         reset_bucket/1,
         get_buckets/1,
         bucket_nval_map/1,
         default_object_nval/0,
         merge_props/2,
         name/1,
         n_val/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @doc Add a list of defaults to global list of defaults for new
%%      buckets.  If any item is in Items is already set in the
%%      current defaults list, the new setting is omitted, and the old
%%      setting is kept.  Omitting the new setting is intended
%%      behavior, to allow settings from app.config to override any
%%      hard-coded values.
append_bucket_defaults(Items) when is_list(Items) ->
    OldDefaults = app_helper:get_env(riak_core, default_bucket_props, []),
    NewDefaults = merge_props(OldDefaults, Items),
    FixedDefaults = case riak_core:bucket_fixups() of
        [] -> NewDefaults;
        Fixups ->
            riak_core_ring_manager:run_fixups(Fixups, default, NewDefaults)
    end,
    application:set_env(riak_core, default_bucket_props, FixedDefaults),
    %% do a noop transform on the ring, to make the fixups re-run
    catch(riak_core_ring_manager:ring_trans(fun(Ring, _) ->
                    {new_ring, Ring} end, undefined)).


%% @spec set_bucket(riak_object:bucket(), BucketProps::riak_core_bucketprops()) -> ok
%% @doc Set the given BucketProps in Bucket.
set_bucket(Name, BucketProps0) ->
    case validate_props(BucketProps0, riak_core:bucket_validators(), []) of
        {ok, BucketProps} ->
            F = fun(Ring, _Args) ->
                        OldBucket = get_bucket(Name),
                        NewBucket = merge_props(BucketProps, OldBucket),
                        {new_ring, riak_core_ring:update_meta({bucket,Name},
                                                              NewBucket,
                                                              Ring)}
                end,
            {ok, _NewRing} = riak_core_ring_manager:ring_trans(F, undefined),
            ok;
        {error, Details} ->
            lager:error("Bucket validation failed ~p~n", [Details]),
            {error, Details}
    end.

%% @spec merge_props(list(), list()) -> list()
%% @doc Merge two sets of bucket props.  If duplicates exist, the
%%      entries in Overriding are chosen before those in Other.
merge_props(Overriding, Other) ->
    lists:ukeymerge(1, lists:ukeysort(1, Overriding),
                    lists:ukeysort(1, Other)).

%% @spec get_bucket(riak_object:bucket()) ->
%%         {ok, BucketProps :: riak_core_bucketprops()}
%% @doc Return the complete current list of properties for Bucket.
%% Properties include but are not limited to:
%% <pre>
%% n_val: how many replicas of objects in this bucket (default: 3)
%% allow_mult: can objects in this bucket have siblings? (default: false)
%% linkfun: a function returning a m/r FunTerm for link extraction
%% </pre>
%%
get_bucket(Name) ->
    Meta = riak_core_ring_manager:get_bucket_meta(Name),
    get_bucket_props(Name, Meta).

%% @spec get_bucket(Name, Ring::riak_core_ring:riak_core_ring()) ->
%%          BucketProps :: riak_core_bucketprops()
%% @private
get_bucket(Name, Ring) ->
    Meta = riak_core_ring:get_meta({bucket, Name}, Ring),
    get_bucket_props(Name, Meta).

get_bucket_props(Name, Meta) ->
    case Meta of
        undefined ->
            [{name, Name}
             |app_helper:get_env(riak_core, default_bucket_props)];
        {ok, Bucket} -> Bucket
    end.

%% @spec reset_bucket(binary()) -> ok
%% @doc Reset the bucket properties for Bucket to the default settings
reset_bucket(Bucket) ->
    F = fun(Ring, _Args) ->
                {new_ring, riak_core_ring:remove_meta({bucket, Bucket}, Ring)}
        end,
    {ok, _NewRing} = riak_core_ring_manager:ring_trans(F, undefined),
    ok.

%% @doc Get bucket properties `Props' for all the buckets in the given
%%      `Ring'
-spec get_buckets(riak_core_ring:riak_core_ring()) ->
                         Props::list().
get_buckets(Ring) ->
    Names = riak_core_ring:get_buckets(Ring),
    [get_bucket(Name, Ring) || Name <- Names].

%% @doc returns a proplist containing all buckets and their respective N values
-spec bucket_nval_map(riak_core_ring:riak_core_ring()) -> [{binary(),integer()}].
bucket_nval_map(Ring) ->
    [{riak_core_bucket:name(B), riak_core_bucket:n_val(B)} ||
        B <- riak_core_bucket:get_buckets(Ring)].

%% @doc returns the default n value for buckets that have not explicitly set the property
-spec default_object_nval() -> integer().
default_object_nval() ->
    riak_core_bucket:n_val(riak_core_config:default_bucket_props()).

%% @private
-spec validate_props(BucketProps::list({PropName::atom(), Value::any()}),
                     Validators::list(module()),
                     Errors::list({PropName::atom(), Error::atom()})) ->
                            {ok, BucketProps::list({PropName::atom(), Value::any()})} |
                            {error,  Errors::list({PropName::atom(), Error::atom()})}.
validate_props(BucketProps, [], []) ->
    {ok, BucketProps};
validate_props(_, [], Errors) ->
    {error, Errors};
validate_props(BucketProps0, [{_App, Validator}|T], Errors0) ->
    {BucketProps, Errors} = Validator:validate(BucketProps0),
    validate_props(BucketProps, T, lists:flatten([Errors|Errors0])).

name(BProps) ->
    proplists:get_value(name, BProps).

n_val(BProps) ->
    proplists:get_value(n_val, BProps).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

simple_set_test() ->
    application:load(riak_core),
    %% appending an empty list of defaults makes up for the fact that
    %% riak_core_app:start/2 is not called during eunit runs
    %% (that's where the usual defaults are set at startup),
    %% while also not adding any trash that might affect other tests
    append_bucket_defaults([]),
    riak_core_ring_events:start_link(),
    riak_core_ring_manager:start_link(test),
    ok = set_bucket(a_bucket,[{key,value}]),
    Bucket = get_bucket(a_bucket),
    riak_core_ring_manager:stop(),
    ?assertEqual(value, proplists:get_value(key, Bucket)).

-endif.
