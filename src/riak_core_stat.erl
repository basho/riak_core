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

-module(riak_core_stat).

%% API
-export([get_stats/0, update/1, register_stats/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(APP, riak_core).

%% @spec get_stats() -> proplist()
%% @doc Get the current aggregation of stats.
get_stats() ->
    produce_stats().

%% @spec update(term()) -> ok
%% @doc Update the given stat.
update(rejected_handoffs) ->
    folsom_metrics:notify_existing_metric({?APP, rejected_handoffs}, {inc, 1}, counter);

update(handoff_timeouts) ->
    folsom_metrics:notify_existing_metric({?APP, handoff_timeouts}, {inc, 1}, counter);

update(ignored_gossip) ->
    folsom_metrics:notify_existing_metric({?APP, ignored_gossip_total}, {inc, 1}, counter);

update(gossip_received) ->
    folsom_metrics:notify_existing_metric({?APP, gossip_received}, 1, meter);

update(rings_reconciled) ->
    folsom_metrics:notify_existing_metric({?APP, rings_reconciled}, 1, meter);
    
update(converge_timer_begin) ->
    folsom_metrics:notify_existing_metric({?APP, converge_delay}, timer_start, duration);
update(converge_timer_end) ->
    folsom_metrics:notify_existing_metric({?APP, converge_delay}, timer_end, duration);

update(rebalance_timer_begin) ->
    folsom_metrics:notify_existing_metric({?APP, rebalance_delay}, timer_start, duration);
update(rebalance_timer_end) ->
    folsom_metrics:notify_existing_metric({?APP, rebalance_delay}, timer_end, duration).

register_stats() ->
    [register_stat({?APP, Name}, Type) || {Name, Type} <- stats()].

%% private
stats() ->
    [{ignored_gossip_total, counter},
     {rings_reconciled, meter},
     {gossip_received, meter},
     {rejected_handoffs, counter},
     {handoff_timeouts, counter},
     {converge_delay, duration},
     {rebalance_delay, duration}].

register_stat(Name, counter) ->
    folsom_metrics:new_counter(Name);
register_stat(Name, meter) ->
    folsom_metrics:new_meter(Name);
register_stat(Name, duration) ->
    folsom_metrics:new_duration(Name).

% @spec produce_stats(state(), integer()) -> proplist()
%% @doc Produce a proplist-formatted view of the current aggregation
%%      of stats.
produce_stats() ->
    lists:append([gossip_stats(),
                  vnodeq_stats()]).

gossip_stats() ->
    lists:flatten([backwards_compat(Stat, Type, folsom_metrics:get_metric_value({?APP, Stat})) ||
                      {Stat, Type} <- stats(), Stat /= riak_core_rejected_handoffs]).

backwards_compat(rings_reconciled, meter, Stats) ->
    [{rings_reconciled_total, proplists:get_value(count, Stats)},
    {rings_reconciled, trunc(proplists:get_value(one, Stats))}];
backwards_compat(gossip_received, meter, Stats) ->
    {gossip_received, trunc(proplists:get_value(one, Stats))};
backwards_compat(Name, counter, Stats) ->
    {Name, Stats};
backwards_compat(Name, duration, Stats) ->
    [{join(Name, min), trunc(proplists:get_value(min, Stats))},
     {join(Name, max), trunc(proplists:get_value(max, Stats))},
     {join(Name, mean), trunc(proplists:get_value(arithmetic_mean, Stats))},
     {join(Name, last), proplists:get_value(last, Stats)}].

join(Atom1, Atom2) ->
    Bin1 = atom_to_binary(Atom1, latin1),
    Bin2 = atom_to_binary(Atom2, latin1),
    binary_to_atom(<<Bin1/binary, $_, Bin2/binary>>, latin1).
    
%% Provide aggregate stats for vnode queues.  Compute instantaneously for now,
%% may need to cache if stats are called heavily (multiple times per seconds)
vnodeq_stats() ->
    VnodesInfo = [{Service, element(2, erlang:process_info(Pid, message_queue_len))} ||
                     {Service, _Index, Pid} <- riak_core_vnode_manager:all_vnodes()],
    ServiceInfo = lists:foldl(fun({S,MQL}, A) ->
                                      orddict:append_list(S, [MQL], A)
                              end, orddict:new(), VnodesInfo),
    lists:flatten([vnodeq_aggregate(S, MQLs) || {S, MQLs} <- ServiceInfo]).

vnodeq_aggregate(_Service, []) ->
    []; % no vnodes, no stats
vnodeq_aggregate(Service, MQLs0) ->
    MQLs = lists:sort(MQLs0),
    Len = length(MQLs),
    Total = lists:sum(MQLs),
    Mean = Total div Len,
    Median = case (Len rem 2) of
                 0 -> % even number, average middle two
                     (lists:nth(Len div 2, MQLs) +
                      lists:nth(Len div 2 + 1, MQLs)) div 2;
                 1 ->
                     lists:nth(Len div 2 + 1, MQLs)
             end,
    [{vnodeq_atom(Service, <<"s_running">>), Len},
     {vnodeq_atom(Service, <<"q_min">>), lists:nth(1, MQLs)},
     {vnodeq_atom(Service, <<"q_median">>), Median},
     {vnodeq_atom(Service, <<"q_mean">>), Mean},
     {vnodeq_atom(Service, <<"q_max">>), lists:nth(Len, MQLs)},
     {vnodeq_atom(Service, <<"q_total">>), Total}].

vnodeq_atom(Service, Desc) ->
    binary_to_atom(<<(atom_to_binary(Service, latin1))/binary, Desc/binary>>, latin1).


-ifdef(TEST).

%% Check vnodeq aggregation function
vnodeq_aggregate_empty_test() ->
    ?assertEqual([], vnodeq_aggregate(service_vnode, [])).

vnodeq_aggregate_odd1_test() ->
    ?assertEqual([{service_vnodes_running, 1},
                  {service_vnodeq_min, 10},
                  {service_vnodeq_median, 10},
                  {service_vnodeq_mean, 10},
                  {service_vnodeq_max, 10},
                  {service_vnodeq_total, 10}],
                 vnodeq_aggregate(service_vnode, [10])).

vnodeq_aggregate_odd3_test() ->
    ?assertEqual([{service_vnodes_running, 3},
                  {service_vnodeq_min, 1},
                  {service_vnodeq_median, 2},
                  {service_vnodeq_mean, 2},
                  {service_vnodeq_max, 3},
                  {service_vnodeq_total, 6}],
                 vnodeq_aggregate(service_vnode, [1, 2, 3])).

vnodeq_aggregate_odd5_test() ->
    ?assertEqual([{service_vnodes_running, 5},
                  {service_vnodeq_min, 0},
                  {service_vnodeq_median, 1},
                  {service_vnodeq_mean, 2},
                  {service_vnodeq_max, 5},
                  {service_vnodeq_total, 10}],
                 vnodeq_aggregate(service_vnode, [1, 0, 5, 0, 4])).

vnodeq_aggregate_even2_test() ->
    ?assertEqual([{service_vnodes_running, 2},
                  {service_vnodeq_min, 10},
                  {service_vnodeq_median, 15},
                  {service_vnodeq_mean, 15},
                  {service_vnodeq_max, 20},
                  {service_vnodeq_total, 30}],
                 vnodeq_aggregate(service_vnode, [10, 20])).

vnodeq_aggregate_even4_test() ->
    ?assertEqual([{service_vnodes_running, 4},
                  {service_vnodeq_min, 0},
                  {service_vnodeq_median, 5},
                  {service_vnodeq_mean, 7},
                  {service_vnodeq_max, 20},
                  {service_vnodeq_total, 30}],
                 vnodeq_aggregate(service_vnode, [0, 10, 0, 20])).

-endif.
