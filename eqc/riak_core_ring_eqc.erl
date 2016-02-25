%% -------------------------------------------------------------------
%%
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

-module(riak_core_ring_eqc).

-ifdef(EQC).
-export([prop_future_index/0]).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TEST_ITERATIONS, 10000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).


eqc_test_() ->
    {inparallel,
     [{spawn,
       [{setup,
         fun() -> ok end,
         fun(_) -> ok end,
         [
          %% Run the quickcheck tests
          {timeout, 60000, % timeout is in msec
           %% Indicate the number of test iterations for each property here
           ?_assertEqual(true,
                         quickcheck(numtests(?TEST_ITERATIONS,
                                             ?QC_OUT(prop_future_index()))))
          }]}]}]}.



prop_future_index() ->
    ?FORALL({CHashKey, OrigIdx, TargetIdx, N, Pos, Ring}=TransferItem, resize_item(),
            ?WHENFAIL(prop_future_index_failed(TransferItem),
                      collect(N =/= undefined andalso Pos >= N,
                              begin
                                  {Time, Val} = timer:tc(riak_core_ring, future_index,
                                                         [CHashKey, OrigIdx, N, Ring]),
                                  measure(future_index_usec, Time, TargetIdx =:= Val)
                              end))).

resize_item() ->
    %% RingSize - Starting Ring Size
    %% GrowthFactor - >1 expanding, <1 shrinking
    %% IndexStart - first partition in preflist for key
    %% Pos - position of source partition in preflist
    %% N - optional known N-value for the key
    ?LET({RingSize, GrowthF},
         {current_size(), growth_factor()},
         ?LET({IndexStart, Pos, N},
              {index_in_current_ring(RingSize),
               replica_position(RingSize),
               check_nval(RingSize, GrowthF)},
              begin
                  Ring0 = riak_core_ring:fresh(RingSize, node()),
                  Ring1 = riak_core_ring:resize(Ring0,trunc(GrowthF * RingSize)),
                  Ring = riak_core_ring:set_pending_resize(Ring1, Ring0),
                  CHashKey = <<(IndexStart-1):160/integer>>,
                  Preflist = riak_core_ring:preflist(CHashKey, Ring0),
                  FuturePreflist = riak_core_ring:preflist(CHashKey, Ring1),
                  {SourceIdx, _} = lists:nth(Pos+1, Preflist),

                  %% account for case where position is greater than
                  %% future ring size or possbly known N-value
                  %% (shrinking) we shouldn't have a target index in
                  %% that case since a transfer to it would be invalid
                  case Pos >= riak_core_ring:num_partitions(Ring1) orelse
                      (N =/= undefined andalso Pos >= N) of
                      true -> TargetIdx = undefined;
                      false -> {TargetIdx, _} = lists:nth(Pos+1, FuturePreflist)
                  end,
                  {CHashKey, SourceIdx, TargetIdx, N, Pos, Ring}
              end)).

index_in_current_ring(RingSize) ->
    elements([I || {I,_} <- riak_core_ring:all_owners(riak_core_ring:fresh(RingSize, node()))]).

current_size() ->
    elements([16, 32, 64, 128]).

growth_factor() ->
    oneof([0.25, 0.5, 2, 4]).


replica_position(RingSize) ->
    %% use a max position that could be invalid for shrinking (greater than future size)
    %% purposefully to generate negative cases
    Max = RingSize,
    choose(0, (Max - 1)).

check_nval(RingSize, GrowthF) ->
    case GrowthF > 1 of
        %% while expanding the n-value doesn't matter
        true -> undefined;
        %% while shrinking provide a "realistic" n-value of the key
        false -> choose(1, trunc((RingSize * GrowthF) / 2) - 1)
    end.


prop_future_index_failed({CHashKey, OrigIdx, TargetIdx, NValCheck, _, R}) ->
    <<CHashInt:160/integer>> = CHashKey,
    FoundTarget = riak_core_ring:future_index(CHashKey, OrigIdx, NValCheck, R),
    io:format("key: ~p~nsource: ~p~ncurrsize: ~p~nfuturesize: ~p~nexpected: ~p~nactual: ~p~n",
              [CHashInt, OrigIdx,
               riak_core_ring:num_partitions(R), riak_core_ring:future_num_partitions(R),
               TargetIdx, FoundTarget]).

-endif.
