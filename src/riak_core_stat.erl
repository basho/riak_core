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
-export([get_stats/0, get_stats/1, update/1]).

%% Metrics API
-export([stat_specs/0]).

-define(APP, riak_core).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec stat_specs() -> riak_core_metric:stat_specs().
stat_specs() ->
    [{ignored_gossip_total, [{type, counter}]},
     {rings_reconciled_total, [{type, counter}]},
     {rejected_handoffs, [{type, counter}]},
     {gossip_received, [{type, meter}]},
     {rings_reconciled, [{type, meter}]},
     {converge_delay, [{type, duration}]},
     {rebalance_delay, [{type, duration}]}
    ].

%% @spec get_stats() -> proplist()
%% @doc Get the current aggregation of stats.
get_stats() ->
    produce_stats().

get_stats(_Moment) ->
    produce_stats().

%% @spec update(term()) -> ok
%% @doc Update the given stat.
update(Stat) ->
    update(Stat, slide:moment()).

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

%% @doc Update the given stat in State, returning a new State.
-spec update(Stat::term(), integer()) -> ok.
update(converge_timer_begin, _Moment) ->
    riak_core_metric_duration:start(?APP, converge_delay);
update(converge_timer_end, _Moment) ->
    riak_core_metric_duration:stop(?APP, converge_delay);
update(rebalance_timer_begin, _Moment) ->
    riak_core_metric_duration:start(?APP, rebalance_delay);
update(rebalance_timer_end, _Moment) ->
    riak_core_metric_duration:stop(?APP, rebalance_delay);
update(rejected_handoffs, _Moment) ->
    riak_core_metric_counter:increment(?APP, rejected_handoffs);
update(ignored_gossip, _Moment) ->
    riak_core_metric_counter:increment(?APP, ignored_gossip_total);
update(gossip_received, Moment) ->
    riak_core_metric_meter:increment(?APP, gossip_received, Moment);
update(rings_reconciled, Moment) ->
    riak_core_metric_meter:increment(?APP, rings_reconciled, Moment),
    riak_core_metric_counter:increment(?APP, rings_reconciled_total);
update(_, _) ->
    ok.

%% @spec produce_stats(state(), integer()) -> proplist()
%% @doc Produce a proplist-formatted view of the current aggregation
%%      of stats.
produce_stats() ->
    lists:append([gossip_stats(),
                  vnodeq_stats()]).

%% @spec gossip_stats(integer()) -> proplist()
%% @doc Get the gossip stats proplist.
gossip_stats() ->
    CDelay = riak_core_metric_duration:cumulative(?APP, converge_delay),
    RDelay = riak_core_metric_duration:cumulative(?APP, rebalance_delay),
    [{ignored_gossip_total, riak_core_metric_counter:total(?APP, ignored_gossip_total)},
     {rings_reconciled_total, riak_core_metric_counter:total(?APP, rings_reconciled_total)},
     {rings_reconciled, riak_core_metric_meter:minute(?APP, rings_reconciled)},
     {gossip_received, riak_core_metric_meter:minute(?APP, gossip_received)},
     {converge_delay_min,  proplists:get_value(min,  CDelay)},
     {converge_delay_max,  proplists:get_value(max, CDelay)},
     {converge_delay_mean, proplists:get_value(mean, CDelay)},
     {converge_delay_last, proplists:get_value(last, CDelay)},
     {rebalance_delay_min,  proplists:get_value(min, RDelay)},
     {rebalance_delay_max,  proplists:get_value(max, RDelay)},
     {rebalance_delay_mean, proplists:get_value(mean, RDelay)},
     {rebalance_delay_last, proplists:get_value(last, RDelay)}].


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
