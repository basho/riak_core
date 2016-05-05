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
-module(riak_core_bucket).

-export([append_bucket_defaults/1,
    set_bucket/2,
    get_bucket/1,
    get_bucket/2,
    reset_bucket/1,
    get_buckets/1,
    bucket_nval_map/1,
    default_object_nval/0,
    all_n/1,
    merge_props/2,
    name/1,
    n_val/1, get_value/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type property() :: {PropName::atom(), PropValue::any()}.
-type properties() :: [property()].

-type riak_core_ring() :: riak_core_ring:riak_core_ring().
-type bucket_type()  :: riak_core_bucket_type:bucket_type().
-type nval_set() :: ordsets:ordset(pos_integer()).
-type bucket() :: binary() | {bucket_type(), binary()}.

-export_type([property/0, properties/0, bucket/0, nval_set/0]).

-define(METADATA_PREFIX, {core, buckets}).

%% @doc Add a list of defaults to global list of defaults for new
%%      buckets.  If any item is in Items is already set in the
%%      current defaults list, the new setting is omitted, and the old
%%      setting is kept.  Omitting the new setting is intended
%%      behavior, to allow settings from app.config to override any
%%      hard-coded values.
append_bucket_defaults(Items) when is_list(Items) ->
    riak_core_bucket_props:append_defaults(Items).

%% @doc Set the given BucketProps in Bucket or {BucketType, Bucket}. If BucketType does not
%% exist, or is not active, {error, no_type} is returned.
-spec set_bucket(bucket(), [{atom(), any()}]) ->
                        ok | {error, no_type | [{atom(), atom()}]}.
set_bucket({<<"default">>, Name}, BucketProps) ->
    set_bucket(Name, BucketProps);
set_bucket({Type, _Name}=Bucket, BucketProps0) ->
    case riak_core_bucket_type:get(Type) of
        undefined -> 
            lager:error("Attempt to set properties of non-existent bucket type ~p", [Bucket]),
            {error, no_type};
        _ -> set_bucket(fun set_bucket_in_metadata/2, Bucket, BucketProps0)
    end;
set_bucket(Name, BucketProps0) ->
    set_bucket(fun set_bucket_in_ring/2, Name, BucketProps0).

set_bucket(StoreFun, Bucket, BucketProps0) ->
    OldBucket = get_bucket(Bucket),
    case riak_core_bucket_props:validate(update, Bucket, OldBucket, BucketProps0) of
        {ok, BucketProps} ->
            NewBucket = merge_props(BucketProps, OldBucket),
            StoreFun(Bucket, NewBucket);
        {error, Details} ->
            lager:error("Bucket properties validation failed ~p~n", [Details]),
            {error, Details}
    end.

set_bucket_in_metadata(Bucket, BucketMeta) ->
    riak_core_metadata:put(?METADATA_PREFIX, bucket_key(Bucket), BucketMeta).

set_bucket_in_ring(Bucket, BucketMeta) ->
    F = fun(Ring, _Args) ->
                {new_ring, riak_core_ring:update_meta(bucket_key(Bucket),
                                                      BucketMeta,
                                                      Ring)}
        end,
    {ok, _NewRing} = riak_core_ring_manager:ring_trans(F, undefined),
    ok.


%% @spec merge_props(list(), list()) -> list()
%% @doc Merge two sets of bucket props.  If duplicates exist, the
%%      entries in Overriding are chosen before those in Other.
merge_props(Overriding, Other) ->
    riak_core_bucket_props:merge(Overriding, Other).

%% @spec get_bucket(riak_object:bucket()) ->
%%         {ok, BucketProps :: riak_core_bucketprops()} | {error,  no_type}
%% @doc Return the complete current list of properties for Bucket.
%% Properties include but are not limited to:
%% <pre>
%% n_val: how many replicas of objects in this bucket (default: 3)
%% allow_mult: can objects in this bucket have siblings? (default: false)
%% linkfun: a function returning a m/r FunTerm for link extraction
%% </pre>
%%
get_bucket({<<"default">>, Name}) ->
    get_bucket(Name);
get_bucket({Type, _Name}=Bucket) ->
    TypeMeta = riak_core_bucket_type:get(Type),
    BucketMeta = riak_core_metadata:get(?METADATA_PREFIX, bucket_key(Bucket),
                                        [{resolver, fun riak_core_bucket_props:resolve/2}]),
    case merge_type_props(TypeMeta, BucketMeta) of
        {error, _}=Error -> 
            lager:error("~p getting properties of bucket ~p",[Error,Bucket]),
            Error;
        Props -> [{name, Bucket} | Props]
    end;
get_bucket(Name) ->
    Meta = riak_core_ring_manager:get_bucket_meta(Name),
    get_bucket_props(Name, Meta).

%% @spec get_bucket(Name, Ring::riak_core_ring()) ->
%%          BucketProps :: riak_core_bucketprops()
%% @private
get_bucket({<<"default">>, Name}, Ring) ->
    get_bucket(Name, Ring);
get_bucket({_Type, _Name}=Bucket, _Ring) ->
    %% non-default type buckets are not stored in the ring, so just ignore it
    get_bucket(Bucket);
get_bucket(Bucket, Ring) ->
    Meta = riak_core_ring:get_meta(bucket_key(Bucket), Ring),
    get_bucket_props(Bucket, Meta).

get_bucket_props(Name, undefined) ->
    [{name, Name} | riak_core_bucket_props:defaults()];
get_bucket_props(_Name, {ok, Bucket}) ->
    Bucket.

merge_type_props(undefined, _) ->
    {error, no_type};
merge_type_props(TypeMeta, undefined) when is_list(TypeMeta) ->
    TypeMeta;
merge_type_props(TypeMeta, BucketMeta) when is_list(TypeMeta) andalso
                                            is_list(BucketMeta) ->
    merge_props(BucketMeta, TypeMeta).

%% @spec reset_bucket(binary()) -> ok
%% @doc Reset the bucket properties for Bucket to the settings
%% inherited from its Bucket Type
reset_bucket({<<"default">>, Name}) ->
    reset_bucket(Name);
reset_bucket({_Type, _Name}=Bucket) ->
    riak_core_metadata:delete(?METADATA_PREFIX, bucket_key(Bucket));
reset_bucket(Bucket) ->
    F = fun(Ring, _Args) ->
                {new_ring, riak_core_ring:remove_meta(bucket_key(Bucket), Ring)}
        end,
    {ok, _NewRing} = riak_core_ring_manager:ring_trans(F, undefined),
    ok.

%% @doc Get bucket properties `Props' for all the buckets in the given
%%      `Ring' and stored in metadata
-spec get_buckets(riak_core_ring()) ->
                         Props::list().
get_buckets(Ring) ->
    RingNames = riak_core_ring:get_buckets(Ring),
    RingBuckets = [get_bucket(Name, Ring) || Name <- RingNames],
    MetadataBuckets = riak_core_metadata:fold(fun({_, undefined}, Acc) ->
                                                      Acc;
                                                 ({_Key, Props}, Acc) ->
                                                      [Props | Acc]
                                              end,
                                              [], ?METADATA_PREFIX,
                                              [{resolver, fun riak_core_bucket_props:resolve/2},
                                               {default, undefined}]),
    RingBuckets ++ MetadataBuckets.

%% @doc returns a proplist containing all buckets and their respective N values
-spec bucket_nval_map(riak_core_ring()) -> [{binary(),integer()}].
bucket_nval_map(Ring) ->
    [{riak_core_bucket:name(B), riak_core_bucket:n_val(B)} ||
        B <- riak_core_bucket:get_buckets(Ring)].

%% @doc returns the default n value for buckets that have not explicitly set the property
-spec default_object_nval() -> integer().
default_object_nval() ->
    riak_core_bucket:n_val(riak_core_bucket_props:defaults()).

-spec all_n(riak_core_ring()) -> [pos_integer(),...].
all_n(Ring) ->
    BucketNVals = bucket_nvals(Ring),
    BucketTypeNVals = riak_core_bucket_type:all_n(),
    ordsets:to_list(
        ordsets:union(BucketNVals, BucketTypeNVals)
    ).

%% @private
-spec bucket_nvals(riak_core_ring()) -> nval_set().
bucket_nvals(Ring) ->
    BucketNs = bucket_nval_map(Ring),
    DefaultN = default_object_nval(),
    AllN = lists:foldl(fun({_, N}, Acc) ->
                               ordsets:add_element(N, Acc)
                       end, [DefaultN], BucketNs),
    AllN.


name(BProps) ->
    get_value(name, BProps).

n_val(BProps) ->
    get_value(n_val, BProps).

% a slighly faster version of proplists:get_value
-spec get_value(atom(), properties()) -> any().
get_value(Key, Proplist) ->
    case lists:keyfind(Key, 1, Proplist) of
        {Key, Value} -> Value;
        _ -> undefined
    end.

bucket_key({<<"default">>, Name}) ->
    bucket_key(Name);
bucket_key({_Type, _Name}=Bucket) ->
    Bucket;
bucket_key(Name) ->
    {bucket, Name}.

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
