%% -------------------------------------------------------------------
%%
%% riak_core_console_util: Core Riak Application
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(ring_math).

-export([hash_to_partition_id/2, partition_id_to_hash/2, hash_is_partition_boundary/2]).
-export_type([partition_id/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type partition_id() :: non_neg_integer(). %% Integer representing a partition: [0, ring_size)

-spec hash_to_partition_id(chash:index() | chash:index_as_int(),
                riak_core_ring:riak_core_ring() | pos_integer()) ->
                       partition_id().
hash_to_partition_id(CHashKey, Ring) when is_binary(CHashKey), is_tuple(Ring)->
    <<CHashInt:160/integer>> = CHashKey,
    hash_to_partition_id(CHashInt, Ring);
hash_to_partition_id(CHashKey, RingSize) when is_binary(CHashKey), is_integer(RingSize)->
    <<CHashInt:160/integer>> = CHashKey,
    hash_to_partition_id(CHashInt, RingSize);
hash_to_partition_id(CHashInt, Ring) when is_tuple(Ring) ->
    PartitionCount = riak_core_ring:num_partitions(Ring),
    hash_to_partition_id(CHashInt, PartitionCount);
hash_to_partition_id(CHashInt, _RingSize) when CHashInt < 0 ->
    throw(invalid_hash);
hash_to_partition_id(CHashInt, RingSize) ->
    CHashInt div chash:ring_increment(RingSize).

-spec partition_id_to_hash(partition_id(), riak_core_ring:riak_core_ring() | pos_integer()) ->
                       chash:index_as_int().
partition_id_to_hash(Id, Ring) when is_tuple(Ring) ->
    partition_id_to_hash(Id, riak_core_ring:num_partitions(Ring));
partition_id_to_hash(Id, RingSize) when Id < 0; Id >= RingSize ->
    throw(invalid_partition_id);
partition_id_to_hash(Id, RingSize) ->
    Id * chash:ring_increment(RingSize).


-spec hash_is_partition_boundary(chash:index() | chash:index_as_int(),
                                 riak_core_ring:riak_core_ring() | pos_integer()) ->
                                        boolean().
hash_is_partition_boundary(CHashKey, Ring) when is_binary(CHashKey), is_tuple(Ring)->
    <<CHashInt:160/integer>> = CHashKey,
    hash_is_partition_boundary(CHashInt, Ring);
hash_is_partition_boundary(CHashInt, Ring) when is_tuple(Ring) ->
    PartitionCount = riak_core_ring:num_partitions(Ring),
    hash_is_partition_boundary(CHashInt, PartitionCount);
hash_is_partition_boundary(CHashInt, _RingSize) when CHashInt < 0 ->
    throw(invalid_hash);
hash_is_partition_boundary(CHashInt, RingSize) when CHashInt rem RingSize =/= 0 ->
    false;
hash_is_partition_boundary(_CHashInt, _RingSize) ->
    true.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

%% Partition boundaries are reversable.
reverse_test() ->
    IntIndex = ring_math:partition_id_to_hash(31, 32),
    HashIndex = <<IntIndex:160>>,
    Ring = riak_core_ring:fresh(32, test_dummy),
    ?assertEqual(31, ring_math:hash_to_partition_id(HashIndex, 32)),
    ?assertEqual(0, ring_math:hash_to_partition_id(<<0:160>>, 32)),
    ?assertEqual(31, ring_math:hash_to_partition_id(HashIndex, Ring)),
    ?assertEqual(0, ring_math:hash_to_partition_id(<<0:160>>, Ring)).

%% Index values somewhere in the middle of a partition can be mapped
%% to partition IDs.
partition_test() ->
    IntIndex = ring_math:partition_id_to_hash(20, 32) +
        chash:ring_increment(32) div 3,
    HashIndex = <<IntIndex:160>>,
    Ring = riak_core_ring:fresh(32, test_dummy),
    ?assertEqual(20, ring_math:hash_to_partition_id(IntIndex, 32)),
    ?assertEqual(20, ring_math:hash_to_partition_id(HashIndex, 32)),
    ?assertEqual(20, ring_math:hash_to_partition_id(IntIndex, Ring)),
    ?assertEqual(20, ring_math:hash_to_partition_id(HashIndex, Ring)).

%% Index integers outside [0, ring_size) result in exceptions
throw_test() ->
    Ring = riak_core_ring:fresh(32, test_dummy),
    ?assertThrow(invalid_hash, ring_math:hash_to_partition_id(-1, 32)),
    ?assertThrow(invalid_partition_id, ring_math:partition_id_to_hash(-1, 32)),
    ?assertThrow(invalid_partition_id, ring_math:partition_id_to_hash(32, 32)),
    ?assertThrow(invalid_hash, ring_math:hash_is_partition_boundary(-5, 32)),
    ?assertThrow(invalid_hash, ring_math:hash_to_partition_id(-1, Ring)),
    ?assertThrow(invalid_partition_id, ring_math:partition_id_to_hash(-1, Ring)),
    ?assertThrow(invalid_partition_id, ring_math:partition_id_to_hash(32, Ring)),
    ?assertThrow(invalid_hash, ring_math:hash_is_partition_boundary(-5, Ring)).

-endif.
