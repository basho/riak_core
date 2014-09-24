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

%% @doc riak_core_console_util provides helper functions for blah blah blah

-module(riak_core_console_util).

-export([idx_to_id/2, id_to_idx/2]).
-export_type([partition_id/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type partition_id() :: non_neg_integer(). %% Integer representing a partition: [0, ring_size)

-spec idx_to_id(chash:index() | chash:index_as_int(),
                riak_core_ring:riak_core_ring() | pos_integer()) ->
                       partition_id() | 'invalid_idx'.
idx_to_id(CHashKey, Ring) when is_binary(CHashKey), is_tuple(Ring)->
    <<CHashInt:160/integer>> = CHashKey,
    idx_to_id(CHashInt, Ring);
idx_to_id(CHashInt, Ring) when is_tuple(Ring) ->
    PartitionCount = riak_core_ring:num_partitions(Ring),
    idx_to_id(CHashInt, PartitionCount);
idx_to_id(CHashInt, RingSize) when CHashInt < 0; CHashInt rem RingSize =/= 0 ->
    invalid_idx;
idx_to_id(CHashInt, RingSize) ->
    CHashInt div chash:ring_increment(RingSize).

-spec id_to_idx(partition_id(), riak_core_ring:riak_core_ring() | pos_integer()) ->
                       chash:index_as_int() | 'invalid_id'.
id_to_idx(Id, Ring) when is_tuple(Ring) ->
    id_to_idx(Id, riak_core_ring:num_partitions(Ring));
id_to_idx(Id, RingSize) when Id < 0; Id >= RingSize ->
    invalid_id;
id_to_idx(Id, RingSize) ->
    Id * chash:ring_increment(RingSize).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-endif.
