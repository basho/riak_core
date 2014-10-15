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
-export([prop_ids_are_boundaries/0, prop_reverse/0,
         prop_monotonic/0, prop_only_boundaries/0]).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-type partition_id() :: {partition_id, non_neg_integer()}.
%% Integer represents a value [0, ring_size)

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
                           pos_integer()) ->
                                  partition_id().
%% @doc Map a key hash (as binary or integer) to a partition ID [0, ring_size)
hash_to_partition_id(CHashKey, RingSize) when is_binary(CHashKey) ->
    <<CHashInt:160/integer>> = CHashKey,
    hash_to_partition_id(CHashInt, RingSize);
hash_to_partition_id(CHashInt, RingSize) ->
    {partition_id, CHashInt div chash:ring_increment(RingSize)}.

-spec partition_id_to_hash(pos_integer() | partition_id(), pos_integer()) ->
                                  chash:index_as_int().
%% @doc Identify the first key hash (integer form) in a partition ID [0, ring_size)
partition_id_to_hash({partition_id, Id}, RingSize) ->
    partition_id_to_hash(Id, RingSize);
partition_id_to_hash(Id, RingSize) ->
    Id * chash:ring_increment(RingSize).


-spec hash_is_partition_boundary(chash:index() | chash:index_as_int(),
                                 pos_integer()) ->
                                        boolean().
%% @doc For user-facing tools, indicate whether a specified hash value
%% is a valid "boundary" value (first hash in some partition)
hash_is_partition_boundary(CHashKey, RingSize) when is_binary(CHashKey) ->
    <<CHashInt:160/integer>> = CHashKey,
    hash_is_partition_boundary(CHashInt, RingSize);
hash_is_partition_boundary(CHashInt, RingSize) ->
    CHashInt rem chash:ring_increment(RingSize) =:= 0.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).
-define(TEST_TIME_SECS, 10).

-define(HASHMAX, 1 bsl 160 - 1).
-define(RINGSIZEEXPMAX, 14).
-define(RINGSIZE(X), (1 bsl X)). %% We'll generate powers of 2 with choose() and convert that to a ring size with this macro
-define(PARTITIONSIZE(X), ((1 bsl 160) div (X))).

ids_are_boundaries_test_() ->
    {timeout, ?TEST_TIME_SECS+5, [?_assert(test_ids_are_boundaries() =:= true)]}.

test_ids_are_boundaries() ->
    test_ids_are_boundaries(?TEST_TIME_SECS).

test_ids_are_boundaries(TestTimeSecs) ->
        eqc:quickcheck(eqc:testing_time(TestTimeSecs, ?QC_OUT(prop_ids_are_boundaries()))).

reverse_test_() ->
    {timeout, ?TEST_TIME_SECS+5, [?_assert(test_reverse() =:= true)]}.

test_reverse() ->
    test_reverse(?TEST_TIME_SECS).

test_reverse(TestTimeSecs) ->
        eqc:quickcheck(eqc:testing_time(TestTimeSecs, ?QC_OUT(prop_reverse()))).


monotonic_test_() ->
    {timeout, ?TEST_TIME_SECS+5, [?_assert(test_monotonic() =:= true)]}.

test_monotonic() ->
    test_monotonic(?TEST_TIME_SECS).

test_monotonic(TestTimeSecs) ->
        eqc:quickcheck(eqc:testing_time(TestTimeSecs, ?QC_OUT(prop_monotonic()))).


only_boundaries_test_() ->
    {timeout, ?TEST_TIME_SECS*5000, [?_assert(test_only_boundaries() =:= true)]}.

test_only_boundaries() ->
    test_only_boundaries(?TEST_TIME_SECS).

test_only_boundaries(_TestTimeSecs) ->
        eqc:quickcheck(eqc:numtests(10, ?QC_OUT(prop_only_boundaries()))). %% XXX: need way more than 10, performance problems

%% Partition IDs should map to hash values which are partition boundaries
prop_ids_are_boundaries() ->
    ?FORALL(RingPower, choose(2, ?RINGSIZEEXPMAX),
            ?FORALL(PartitionId, choose(0, ?RINGSIZE(RingPower) - 1),
                    riak_core_ring_util:hash_is_partition_boundary(
                      riak_core_ring_util:partition_id_to_hash(PartitionId,
                                                               ?RINGSIZE(RingPower)),
                      ?RINGSIZE(RingPower)) =:= true)).

%% Partition IDs should map to hash values which map back to the same partition IDs
prop_reverse() ->
    ?FORALL(RingPower, choose(2, ?RINGSIZEEXPMAX),
            ?FORALL(PartitionId, choose(0, ?RINGSIZE(RingPower) - 1),
                    riak_core_ring_util:hash_to_partition_id(
                      riak_core_ring_util:partition_id_to_hash(PartitionId,
                                                               ?RINGSIZE(RingPower)),
                      ?RINGSIZE(RingPower)) =:= {partition_id, PartitionId})).

%% For any given hash value, any larger hash value maps to a partition
%% ID of greater or equal value.
prop_monotonic() ->
    ?FORALL(RingPower, choose(2, ?RINGSIZEEXPMAX),
            ?FORALL(HashValue, choose(0, ?HASHMAX - 1),
                    ?FORALL(GreaterHash, choose(HashValue + 1, ?HASHMAX),
                            begin
                                {partition_id, LowerPartition} =
                                    riak_core_ring_util:hash_to_partition_id(HashValue,
                                                                             ?RINGSIZE(RingPower)),
                                {partition_id, GreaterPartition} =
                                    riak_core_ring_util:hash_to_partition_id(GreaterHash,
                                                                             ?RINGSIZE(RingPower)),
                                LowerPartition =< GreaterPartition
                            end
                           ))).


%% Hash values which are listed in the ring structure are boundary
%% values
boundary_helper(Hash, RingDict) ->
    orddict:is_key(Hash, RingDict).

find_near_boundaries(RingSize, PartitionSize) ->
    ?LET({Id, Offset}, {choose(1, RingSize-1), choose(-RingSize, RingSize)},
         Id * PartitionSize + Offset).


%% orddict gives faster lookups than proplist but probably need chashbin
ring_to_orddict({_RingSize, PropList}) ->
    orddict:from_list(PropList).

prop_only_boundaries() ->
    ?FORALL(RingPower, choose(2, ?RINGSIZEEXPMAX),
            ?FORALL({HashValue, RingDict},
                    {frequency([
                               {5, choose(0, ?HASHMAX)},
                               {2, find_near_boundaries(?RINGSIZE(RingPower),
                                                        ?PARTITIONSIZE(?RINGSIZE(RingPower)))}]),
                     ring_to_orddict(chash:fresh(?RINGSIZE(RingPower), dummy))},
                    boundary_helper(HashValue, RingDict) =:=
                        riak_core_ring_util:hash_is_partition_boundary(HashValue,
                                                                       ?RINGSIZE(RingPower)))).

-endif. % EQC
-endif. % TEST
