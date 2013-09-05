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
-module(riak_core_bucket_type).

-export([create/2,
         update/2,
         get/1,
         iterator/0,
         itr_next/1,
         itr_done/1,
         itr_value/1]).

-type bucket_type()       :: binary().
-type bucket_type_props() :: [{term(), term()}].

-define(KEY_PREFIX, {core, bucket_types}).
-define(DEFAULT_TYPE, <<"default">>).

%% @doc Create a new bucket type. Properties provided will be merged with default
%% bucket properties set in config. "Creation" as implemented is subject to concurrent
%% write issues, so for now this is more of a semantic name. This will be addressed
%% when proper support is added to cluster metadata
-spec create(bucket_type(), bucket_type_props()) -> ok | {error, term()}.
create(?DEFAULT_TYPE, _Props) ->
    {error, already_exists};
create(BucketType, Props) when is_binary(BucketType) ->
    %% TODO: this doesn't actually give us much guaruntees
    case ?MODULE:get(BucketType) of
        undefined -> update(BucketType, Props);
        _ -> {error, already_exists}
    end.

%% @doc Update an existing bucket type. See comments in create/1 for caveats.
-spec update(bucket_type(), bucket_type_props()) -> ok | {error, term()}.
update(?DEFAULT_TYPE, _Props) ->
    {error, no_default_update}; %% default props are in the app.config
update(BucketType, Props0) when is_binary(BucketType)->
    case riak_core_bucket_props:validate(Props0) of
        {ok, Props} ->
            OldProps = get(BucketType, riak_core_bucket_props:defaults()),
            NewProps = riak_core_bucket_props:merge(Props, OldProps),
            %% TODO: but what if DNE? not much "update" guarantee
            riak_core_metadata:put(?KEY_PREFIX, BucketType, NewProps);
        {error, Details} ->
            lager:error("Bucket Type properties validation failed ~p~n", [Details]),
            {error, Details}
    end.

%% @doc Return the properties associated with the given bucket type
-spec get(bucket_type()) -> undefined | bucket_type_props().
get(BucketType) when is_binary(BucketType) ->
    get(BucketType, undefined).

%% @private
get(BucketType, Default) ->
    riak_core_metadata:get(?KEY_PREFIX,
                           BucketType,
                           [{default, Default}, {resolver, fun riak_core_bucket_props:resolve/2}]).

%% @doc Return an iterator that can be used to walk iterate through all existing bucket types
-spec iterator() -> riak_core_metadata:iterator().
iterator() ->
    %% we don't allow deletion (yet) so we should never need the default but we provide one anyway
    riak_core_metadata:iterator(?KEY_PREFIX, [{default, undefined},
                                              {resolver, fun riak_core_bucket_props:resolve/2}]).

%% @doc Advance the iterator to the next bucket type. itr_done/1 should always be called
%% before this function
-spec itr_next(riak_core_metadata:iterator()) ->
                      riak_core_metadata:iterator().
itr_next(It) ->
    riak_core_metadata:itr_next(It).

%% @doc Returns true if there are no more bucket types to iterate over
-spec itr_done(riak_core_metadata:iterator()) -> boolean().
itr_done(It) ->
    riak_core_metadata:itr_done(It).

%% @doc Returns the type and properties that the iterator points too. Any siblings,
%% are resolved at this time. itr_done/1 should be checked before calling this function.
-spec itr_value(riak_core_metadata:iterator()) ->
                       {bucket_type(), bucket_type_props()}.
itr_value(It) ->
    {BucketType, Props} = riak_core_metadata:itr_key_values(It),
    {BucketType, Props}.
