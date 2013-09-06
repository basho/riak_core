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

-behaviour(gen_server).

%% API
-export([start_link/0, get_stats/0, update/1,
         register_stats/0, produce_stats/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).

-define(APP, riak_core).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register_stats() ->
    [(catch folsom_metrics:delete_metric({?APP, Name})) || {Name, _Type} <- stats()],
    [register_stat({?APP, Name}, Type) || {Name, Type} <- stats()],
    riak_core_stat_cache:register_app(?APP, {?MODULE, produce_stats, []}).

%% @spec get_stats() -> proplist()
%% @doc Get the current aggregation of stats.
get_stats() ->
    case riak_core_stat_cache:get_stats(?APP) of
        {ok, Stats, _TS} ->
            Stats;
        Error -> Error
    end.

update(Arg) ->
    gen_server:cast(?SERVER, {update, Arg}).

% @spec produce_stats(state(), integer()) -> proplist()
%% @doc Produce a proplist-formatted view of the current aggregation
%%      of stats.
produce_stats() ->
    lists:append([gossip_stats(),
                  vnodeq_stats()]).

%% gen_server

init([]) ->
    register_stats(),
    {ok, ok}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({update, Arg}, State) ->
    update1(Arg),
    {noreply, State};
handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @spec update(term()) -> ok
%% @doc Update the given stat.
update1(rejected_handoffs) ->
    folsom_metrics:notify_existing_metric({?APP, rejected_handoffs}, {inc, 1}, counter);

update1(handoff_timeouts) ->
    folsom_metrics:notify_existing_metric({?APP, handoff_timeouts}, {inc, 1}, counter);

update1(ignored_gossip) ->
    folsom_metrics:notify_existing_metric({?APP, ignored_gossip_total}, {inc, 1}, counter);

update1(gossip_received) ->
    folsom_metrics:notify_existing_metric({?APP, gossip_received}, 1, spiral);

update1(rings_reconciled) ->
    folsom_metrics:notify_existing_metric({?APP, rings_reconciled}, 1, spiral);

update1(dropped_vnode_requests) ->
    folsom_metrics:notify_existing_metric({?APP, dropped_vnode_requests_total}, {inc, 1}, counter);

update1(converge_timer_begin) ->
    folsom_metrics:notify_existing_metric({?APP, converge_delay}, timer_start, duration);
update1(converge_timer_end) ->
    folsom_metrics:notify_existing_metric({?APP, converge_delay}, timer_end, duration);

update1(rebalance_timer_begin) ->
    folsom_metrics:notify_existing_metric({?APP, rebalance_delay}, timer_start, duration);
update1(rebalance_timer_end) ->
    folsom_metrics:notify_existing_metric({?APP, rebalance_delay}, timer_end, duration).

%% private
stats() ->
    [{ignored_gossip_total, counter},
     {rings_reconciled, spiral},
     {gossip_received, spiral},
     {rejected_handoffs, counter},
     {handoff_timeouts, counter},
     {dropped_vnode_requests_total, counter},
     {converge_delay, duration},
     {rebalance_delay, duration}].

register_stat(Name, counter) ->
    folsom_metrics:new_counter(Name);
register_stat(Name, spiral) ->
    folsom_metrics:new_spiral(Name);
register_stat(Name, duration) ->
    folsom_metrics:new_duration(Name).

gossip_stats() ->
    lists:flatten([backwards_compat(Stat, Type, riak_core_stat_q:calc_stat({{?APP, Stat}, Type})) ||
                      {Stat, Type} <- stats(), Stat /= riak_core_rejected_handoffs]).

backwards_compat(Name, Type, unavailable) when Type =/= counter ->
    backwards_compat(Name, Type, []);
backwards_compat(rings_reconciled, spiral, Stats) ->
    [{rings_reconciled_total, proplists:get_value(count, Stats, unavailable)},
    {rings_reconciled, safe_trunc(proplists:get_value(one, Stats, unavailable))}];
backwards_compat(gossip_received, spiral, Stats) ->
    {gossip_received, safe_trunc(proplists:get_value(one, Stats, unavailable))};
backwards_compat(Name, counter, Stats) ->
    {Name, Stats};
backwards_compat(Name, duration, Stats) ->
    [{join(Name, min), safe_trunc(proplists:get_value(min, Stats, unavailable))},
     {join(Name, max), safe_trunc(proplists:get_value(max, Stats, unavailable))},
     {join(Name, mean), safe_trunc(proplists:get_value(arithmetic_mean, Stats, unavailable))},
     {join(Name, last), proplists:get_value(last, Stats, unavailable)}].

join(Atom1, Atom2) ->
    Bin1 = atom_to_binary(Atom1, latin1),
    Bin2 = atom_to_binary(Atom2, latin1),
    binary_to_atom(<<Bin1/binary, $_, Bin2/binary>>, latin1).

safe_trunc(N) when is_number(N) ->
    trunc(N);
safe_trunc(X) ->
    X.

%% Provide aggregate stats for vnode queues.  Compute instantaneously for now,
%% may need to cache if stats are called heavily (multiple times per seconds)
vnodeq_stats() ->
    VnodesInfo = [{Service, vnodeq_len(Pid)} ||
                     {Service, _Index, Pid} <- riak_core_vnode_manager:all_vnodes()],
    ServiceInfo = lists:foldl(fun({S,MQL}, A) ->
                                      orddict:append_list(S, [MQL], A)
                              end, orddict:new(), VnodesInfo),
    lists:flatten([vnodeq_aggregate(S, MQLs) || {S, MQLs} <- ServiceInfo]).

vnodeq_len(Pid) ->
    try
        element(2, erlang:process_info(Pid, message_queue_len))
    catch _:_ ->
            0
    end.

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
