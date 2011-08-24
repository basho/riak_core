%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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
-module(bucket_fixup_test).

-include_lib("eunit/include/eunit.hrl").

-export([fixup/2]).

fixup(_Name, Props) ->
    {ok, [{test, 1}|Props]}.

fixup_test() ->
    application:set_env(riak_core,ring_creation_size, 4),
    application:set_env(riak_core, bucket_fixups, [{someapp, ?MODULE}]),
    Ring = riak_core_ring:update_meta({bucket,mybucket},
        [{foo, bar}],
        riak_core_ring:fresh()),
    riak_core_ring_manager:set_ring_global(Ring),
    %NewRing = riak_core_ring_manager:get_my_ring(),
    ?assertEqual([mybucket], riak_core_ring:get_buckets(Ring)),
    [ ?assertEqual(1, proplists:get_value(test,
                riak_core_bucket:get_bucket(Bucket))) ||
        Bucket <- riak_core_ring:get_buckets(Ring)],
    ok.

