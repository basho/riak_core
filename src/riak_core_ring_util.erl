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
-module(riak_core_ring_util).

-export([assign/2,
         check_ring/0,
         check_ring/1,
         check_ring/2,
         hash_to_partition_id/2,
         partition_id_to_hash/2,
         hash_is_partition_boundary/2]).

-export_type([partition_id/0]).

-ifdef(TEST).
-ifdef(EQC).
-export([prop_reverse/0, prop_boundaries/0, prop_monotonic/0]).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-type partition_id() :: non_neg_integer(). %% Integer representing a
                                           %% partition: [0, ring_size)

%% @doc Forcibly assign a partition to a specific node
assign(Partition, ToNode) ->
    F = fun(Ring, _) ->
                {new_ring, riak_core_ring:transfer_node(Partition, ToNode, Ring)}
        end,
    {ok, _NewRing} = riak_core_ring_manager:ring_trans(F, undefined),
    ok.

%% @doc Check the local ring for any preflists that do not satisfy n_val
check_ring() ->
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    check_ring(R).

check_ring(Ring) ->
    {ok, Props} = application:get_env(riak_core, default_bucket_props),
    {n_val, Nval} = lists:keyfind(n_val, 1, Props),
    check_ring(Ring, Nval).

%% @doc Check a ring for any preflists that do not satisfy n_val
check_ring(Ring, Nval) ->
    Preflists = riak_core_ring:all_preflists(Ring, Nval),
    lists:foldl(fun(PL,Acc) ->
                        PLNodes = lists:usort([Node || {_,Node} <- PL]),
                        case length(PLNodes) of
                            Nval ->
                                Acc;
                            _ ->
                                ordsets:add_element(PL, Acc)
                        end
                end, [], Preflists).

-spec hash_to_partition_id(chash:index() | chash:index_as_int(),
                riak_core_ring:riak_core_ring() | pos_integer()) ->
                       partition_id().
%% @doc Map a key hash (as binary or integer) to a partition ID [0, ring_size)
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

-spec partition_id_to_hash(partition_id(),
                           riak_core_ring:riak_core_ring() | pos_integer()) ->
                                  chash:index_as_int().
%% @doc Identify the first key hash (integer form) in a partition ID [0, ring_size)
partition_id_to_hash(Id, Ring) when is_tuple(Ring) ->
    partition_id_to_hash(Id, riak_core_ring:num_partitions(Ring));
partition_id_to_hash(Id, RingSize) when Id < 0; Id >= RingSize ->
    throw(invalid_partition_id);
partition_id_to_hash(Id, RingSize) ->
    Id * chash:ring_increment(RingSize).


-spec hash_is_partition_boundary(chash:index() | chash:index_as_int(),
                                 riak_core_ring:riak_core_ring() |
                                 pos_integer()) ->
                                        boolean().
%% @doc For user-facing tools, indicate whether a specified hash value
%% is a valid "boundary" value (first hash in some partition)
hash_is_partition_boundary(CHashKey, Ring) when is_binary(CHashKey), is_tuple(Ring)->
    <<CHashInt:160/integer>> = CHashKey,
    hash_is_partition_boundary(CHashInt, Ring);
hash_is_partition_boundary(CHashKey, RingSize) when is_binary(CHashKey) ->
    <<CHashInt:160/integer>> = CHashKey,
    hash_is_partition_boundary(CHashInt, RingSize);
hash_is_partition_boundary(CHashInt, Ring) when is_tuple(Ring) ->
    PartitionCount = riak_core_ring:num_partitions(Ring),
    hash_is_partition_boundary(CHashInt, PartitionCount);
hash_is_partition_boundary(CHashInt, _RingSize) when CHashInt < 0 ->
    throw(invalid_hash);
hash_is_partition_boundary(CHashInt, RingSize) ->
    CHashInt rem chash:ring_increment(RingSize) =:= 0.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

%% Partition boundaries are reversable.
reverse_test() ->
    IntIndex = riak_core_ring_util:partition_id_to_hash(31, 32),
    HashIndex = <<IntIndex:160>>,
    Ring = riak_core_ring:fresh(32, test_dummy),
    ?assertEqual(31, riak_core_ring_util:hash_to_partition_id(HashIndex, 32)),
    ?assertEqual(0, riak_core_ring_util:hash_to_partition_id(<<0:160>>, 32)),
    ?assertEqual(31, riak_core_ring_util:hash_to_partition_id(HashIndex, Ring)),
    ?assertEqual(0, riak_core_ring_util:hash_to_partition_id(<<0:160>>, Ring)).

%% Index values somewhere in the middle of a partition can be mapped
%% to partition IDs.
partition_test() ->
    IntIndex = riak_core_ring_util:partition_id_to_hash(20, 32) +
        chash:ring_increment(32) div 3,
    HashIndex = <<IntIndex:160>>,
    Ring = riak_core_ring:fresh(32, test_dummy),
    ?assertEqual(20, riak_core_ring_util:hash_to_partition_id(IntIndex, 32)),
    ?assertEqual(20, riak_core_ring_util:hash_to_partition_id(HashIndex, 32)),
    ?assertEqual(20, riak_core_ring_util:hash_to_partition_id(IntIndex, Ring)),
    ?assertEqual(20, riak_core_ring_util:hash_to_partition_id(HashIndex, Ring)).

%% Index values divisible by partition size are boundary values, others are not
boundary_test() ->
    BoundaryIndex = riak_core_ring_util:partition_id_to_hash(15, 32),
    Ring = riak_core_ring:fresh(32, test_dummy),
    ?assert(riak_core_ring_util:hash_is_partition_boundary(BoundaryIndex, Ring)),
    ?assert(riak_core_ring_util:hash_is_partition_boundary(<<BoundaryIndex:160>>, Ring)),
    ?assert(riak_core_ring_util:hash_is_partition_boundary(BoundaryIndex, 32)),
    ?assert(riak_core_ring_util:hash_is_partition_boundary(<<BoundaryIndex:160>>, 32)),
    ?assertNot(riak_core_ring_util:hash_is_partition_boundary(BoundaryIndex + 32, Ring)),
    ?assertNot(riak_core_ring_util:hash_is_partition_boundary(<<(BoundaryIndex + 32):160>>, Ring)),
    ?assertNot(riak_core_ring_util:hash_is_partition_boundary(BoundaryIndex + 32, 32)),
    ?assertNot(riak_core_ring_util:hash_is_partition_boundary(<<(BoundaryIndex + 32):160>>, 32)),
    ?assertNot(riak_core_ring_util:hash_is_partition_boundary(<<(BoundaryIndex + 1):160>>, 32)),
    ?assertNot(riak_core_ring_util:hash_is_partition_boundary(<<(BoundaryIndex - 1):160>>, 32)),
    ?assertNot(riak_core_ring_util:hash_is_partition_boundary(<<(BoundaryIndex + 2):160>>, 32)),
    ?assertNot(riak_core_ring_util:hash_is_partition_boundary(<<(BoundaryIndex + 10):160>>, 32)).


%% Index integers outside [0, ring_size) result in exceptions
throw_test() ->
    Ring = riak_core_ring:fresh(32, test_dummy),
    ?assertThrow(invalid_hash, riak_core_ring_util:hash_to_partition_id(-1, 32)),
    ?assertThrow(invalid_partition_id, riak_core_ring_util:partition_id_to_hash(-1, 32)),
    ?assertThrow(invalid_partition_id, riak_core_ring_util:partition_id_to_hash(32, 32)),
    ?assertThrow(invalid_hash, riak_core_ring_util:hash_is_partition_boundary(-5, 32)),
    ?assertThrow(invalid_hash, riak_core_ring_util:hash_to_partition_id(-1, Ring)),
    ?assertThrow(invalid_partition_id, riak_core_ring_util:partition_id_to_hash(-1, Ring)),
    ?assertThrow(invalid_partition_id, riak_core_ring_util:partition_id_to_hash(32, Ring)),
    ?assertThrow(invalid_hash, riak_core_ring_util:hash_is_partition_boundary(-5, Ring)).

-ifdef(EQC).

-define(HASHMAX, 1 bsl 160 - 1).
-define(RINGSIZEEXPMAX, 14).
-define(RINGSIZE(X), 1 bsl X). %% We'll generate powers of 2 with choose() and convert that to a ring size with this macro

%% Take a ring size, pick a partition ID, convert it to a hash and back to a partition ID and they should match
prop_reverse() ->
    ?FORALL(RingPower, choose(2, ?RINGSIZEEXPMAX),
            ?FORALL(PartitionId, choose(0, ?RINGSIZE(RingPower) - 1),
                    riak_core_ring_util:hash_to_partition_id(riak_core_ring_util:partition_id_to_hash(PartitionId, ?RINGSIZE(RingPower)), ?RINGSIZE(RingPower)) =:= PartitionId)).

%% Take a ring size, pick a hash value, convert it to a partition id and back to a hash. Will probably be different from original hash value but must be a partition boundary
prop_boundaries() ->
    ?FORALL(RingPower, choose(2, ?RINGSIZEEXPMAX),
            ?FORALL(HashValue, choose(0, ?HASHMAX),
                    riak_core_ring_util:hash_is_partition_boundary(
                      riak_core_ring_util:partition_id_to_hash(
                        riak_core_ring_util:hash_to_partition_id(HashValue, ?RINGSIZE(RingPower)), ?RINGSIZE(RingPower)), ?RINGSIZE(RingPower)) =:= true)).

%% For any given hash value, any larger hash value maps to a partition ID of greater or equal value.
prop_monotonic() ->
    ?FORALL(RingPower, choose(2, ?RINGSIZEEXPMAX),
            ?FORALL(HashValue, choose(0, ?HASHMAX - 1),
                    ?FORALL(GreaterHash, choose(HashValue + 1, ?HASHMAX),
                            riak_core_ring_util:hash_to_partition_id(HashValue, ?RINGSIZE(RingPower)) =< riak_core_ring_util:hash_to_partition_id(GreaterHash, ?RINGSIZE(RingPower))))).


-endif. % EQC
-endif. % TEST
