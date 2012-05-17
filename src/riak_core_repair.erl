%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_core_repair).
-export([gen_range/3,
         gen_range_fun/2,
         gen_range_map/3]).

-include("riak_core_vnode.hrl").

%% GTE = Greater Than or Equal
%% LTE = Less Than or Equal
-type range_wrap() :: {wrap, GTE::binary(), LTE::binary()}.
-type range_nowrap() :: {nowrap, GTE::binary(), LTE::binary()}.
-type hash_range() :: range_wrap() | range_nowrap().

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Generate the hash `Range' for a given `Target' partition and
%%      `NVal'.
%%
%% Note: The type of NVal should be pos_integer() but dialyzer says
%%       success typing is integer() and I don't have time for games.
-spec gen_range(partition(), riak_core_ring:riak_core_ring(), integer()) ->
                       hash_range().
gen_range(Target, Ring, NVal) ->
    Predecessors = chash:predecessors(<<Target:160/integer>>, Ring, NVal+1),
    [{FirstIdx, Node}|Rest] = Predecessors,
    Predecessors2 = [{FirstIdx-1, Node}|Rest],
    Predecessors3 = [<<I:160/integer>> || {I,_} <- Predecessors2],
    {A,B} = lists:splitwith(fun(E) -> E > <<0:160/integer>> end, Predecessors3),
    case B of
        [] ->
            %% In this case there is no "wrap" around the end of the
            %% ring so the range check it simply an inclusive
            %% inbetween.
            make_nowrap(A);

        [<<0:160/integer>>] ->
            %% In this case the 0th partition is first so no wrap
            %% actually occurred.
            make_nowrap(A ++ B);

        _ ->
            %% In this case there is a "wrap" around the end of the
            %% ring.  Either the key is greater than or equal the
            %% largest or smaller than or equal to the smallest.
            make_wrap(A, B)
    end.


%% @doc Generate the function that will return the hash range for a
%%      given `Bucket'.
-spec gen_range_fun(list(), hash_range()) -> function().
gen_range_fun(RangeMap, Default) ->
    fun(Bucket) ->
            case lists:keyfind(Bucket, 1, RangeMap) of
                false -> Default;
                {_, Val} -> Val
            end
    end.

%% @doc Generate the map from bucket `B' to hash `Range' that a key
%%      must fall into to be included for repair on the `Target'
%%      partition.
-spec gen_range_map(partition(), chash:chash(), list()) ->
                           [{B::riak_object:bucket(), Range::hash_range()}].
gen_range_map(Target, Ring, NValMap) ->
    [{I, gen_range(Target, Ring, N)} || {I, N} <- NValMap, N > 1].


%% ===================================================================
%% Internal
%% ===================================================================

%% @private
%%
%% @doc Make the `nowrap' tuple representing the range a key hash must
%% fall into.
-spec make_nowrap(list()) -> range_nowrap().
make_nowrap(RingSlice) ->
    Slice2 = lists:sort(RingSlice),
    GTE = hd(Slice2),
    LTE = lists:last(Slice2),
    {nowrap, GTE, LTE}.

%% @private
%%
%% @doc Make `wrap' tuple representing the two ranges, relative to 0,
%% that a key hash must fall into.
%%
%% `RingSliceAfter' contains entries after the wrap around the 0th
%% partition.  `RingSliceBefore' are entries before wrap and the
%% including the wrap at partition 0.
-spec make_wrap(list(), list()) -> range_wrap().
make_wrap(RingSliceAfter, RingSliceBefore) ->
            LTE = hd(RingSliceAfter),
            %% know first element of RingSliceBefore is 0
            Before2 = tl(RingSliceBefore),
            GTE = lists:last(Before2),
            {wrap, GTE, LTE}.

