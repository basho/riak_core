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

%% @type Integer representing a partition: [0, ring_size)
-type partition_id() :: non_neg_integer().

-spec idx_to_id(chash:index() | chash:index_as_int(),
                riak_core_ring:riak_core_ring()) ->
                       partition_id() | 'invalid_idx'.
idx_to_id(CHashKey, Ring) when is_binary(CHashKey) ->
    <<CHashInt:160/integer>> = CHashKey,
    idx_to_id(CHashInt, Ring);
idx_to_id(CHashInt, Ring) ->
    PartitionCount = riak_core_ring:num_partitions(Ring),
    idx_to_id_aux(CHashInt, PartitionCount).

%% @private
%% This is defined for human-readable output only. If we're given an
%% index that doesn't map exactly to the first key in a partition,
%% something has gone wrong, and return `invalid_idx'.
-spec idx_to_id_aux(chash:index_as_int(), pos_integer()) ->
                           partition_id() | 'invalid_idx'.
idx_to_id_aux(CHashInt, RingSize) when CHashInt rem RingSize =/= 0 ->
    invalid_idx;
idx_to_id_aux(CHashInt, RingSize) ->
    CHashInt div RingSize.

-spec id_to_idx(partition_id(), riak_core_ring:riak_core_ring()) ->
                       chash:index_as_int() | 'invalid_id'.
id_to_idx(Id, Ring) ->
    id_to_idx_aux(Id, riak_core_ring:num_partitions(Ring)).

%% @private
id_to_idx_aux(Id, RingSize) when Id >= RingSize ->
    invalid_id;
id_to_idx_aux(Id, RingSize) ->
    Id * chash:ring_increment(RingSize).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-endif.
