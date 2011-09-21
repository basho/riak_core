%% -------------------------------------------------------------------
%%
%% Copyright (c) 2011 Basho Technologies, Inc.  All Rights Reserved.
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

%% Based on an earlier edition provided by Greg Nelson and Ryan Zezeski:
%% https://gist.github.com/992317

-module(claim_simulation).
-compile(export_all).

%%-define(SIMULATE,1).
-ifdef(SIMULATE).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(node(N), list_to_atom(lists:flatten(io_lib:format("riak@node~w",[N])))).
-define(get(K, PL, D), proplists:get_value(K, PL, D)).

basic_test_() ->
    {timeout, 60000, [fun basic_default/0,
                      fun basic_new/0]}.

basic_default() ->
    Opts = [{suffix, "_default"},
            {wc_mf, {riak_core_claim, default_wants_claim}},
            {cc_mf, {riak_core_claim, default_choose_claim}},
            {target_n_val, 4},
            {ring_size, 32},
            {node_count, 8},
            {node_capacity, 24}
           ],
    run(Opts).

basic_new() ->
    Opts = [{suffix, "_new"},
            {wc_mf, {riak_core_new_claim, new_wants_claim}},
            {cc_mf, {riak_core_new_claim, new_choose_claim}},
            {target_n_val, 4},
            {ring_size, 32},
            {node_count, 8},
            {node_capacity, 24}
           ],
    run(Opts).

run(Opts) ->
    application:load(riak_core),

    WCMod = ?get(wc_mf, Opts, {riak_core_claim, default_wants_claim}),
    CCMod = ?get(cc_mf, Opts, {riak_core_claim, default_choose_claim}),
    TargetN = ?get(target_n_val, Opts, 4),
    Suffix = ?get(suffix, Opts, ""),

    application:set_env(riak_core, wants_claim_fun, WCMod),
    application:set_env(riak_core, choose_claim_fun, CCMod),
    application:set_env(riak_core, target_n_val, TargetN),

    RingSize = ?get(ring_size, Opts, 2048),
    NodeCount = ?get(node_count, Opts, 100),
    NodeCapacity = ?get(node_capacity, Opts, 24), %% in TB

    Ring1 = riak_core_ring:fresh(RingSize, 'riak@node1'),
    {Rings, _} = lists:mapfoldl(
                   fun(N, Prev) ->
                           Node = ?node(N),
                           R = riak_core_ring:add_member(Node, Prev, Node),
                           Next =
                               riak_core_gossip:claim_until_balanced(R,
                                                                     Node),
                           {Next, Next}
                   end, Ring1, lists:seq(2, NodeCount)),

    Owners1 = riak_core_ring:all_owners(Ring1),
    Owners = lists:map(fun riak_core_ring:all_owners/1, Rings),

    {Movers, _} =
        lists:mapfoldl(
          fun(Curr, Prev) ->
                  Sum = length(lists:filter(fun not_same_node/1,
                                            lists:zip(Prev, Curr))),
                  {Sum, Curr}
          end, Owners1, Owners),

    MeetTargetN = [riak_core_claim:meets_target_n(R, TargetN) || R <- Rings],

    FName = io_lib:format("/tmp/rings_~w_~w~s.txt",
                          [RingSize, NodeCount, Suffix]),
    {ok, Out} = file:open(FName, [write]),
    [print_info(Out, O, N, M, lists:nth(N - 1, MeetTargetN),
                RingSize, NodeCapacity)
     || {O, M, N} <- lists:zip3(Owners, Movers, lists:seq(2, NodeCount))],
    lists:foreach(fun(RingOut) ->
                          riak_core_ring:pretty_print(RingOut,
                                                      [{out, Out},
                                                       {target_n, TargetN}])
                  end, [Ring1|Rings]).

not_same_node({{P,N}, {P,N}}) -> false;
not_same_node({{P,_N}, {P,_M}}) -> true.

print_info(Out, Owners, NodeCount, PartitionsMoved, MeetsTargetN,
           RingSize, NodeCapacity) ->
    Expect = round(RingSize / NodeCount),
    ActualPercent = 100 * (PartitionsMoved / RingSize),
    Terabytes = 0.8 * NodeCount * NodeCapacity * (PartitionsMoved / RingSize),
    %% Terabytes = Total Amount of Data * Fraction Moved

    F = fun({_,Node}, Acc) ->
                dict:update_counter(Node, 1, Acc)
        end,
    C = lists:keysort(1, dict:to_list(lists:foldl(F, dict:new(), Owners))),
    io:format(Out,
              "add node ~p, expect=~p, actual=~p, "
              "percent=~p, terabytes=~p, meets_target_n=~p~n"
              "counts: ~p~n~n",
              [NodeCount, Expect, PartitionsMoved, ActualPercent,
               Terabytes, MeetsTargetN,C]).

-endif.
-endif.
