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
-export([
    start_link/0, get_stats/0, get_stats/1, get_value/1,
    get_stats_info/0, get_stat/1, update/1,
    register_stats/0, vnodeq_stats/0,
	  register_stats/2,
	  register_vnode_stats/3, unregister_vnode_stats/2,
	  vnodeq_stats/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-compile({parse_transform, riak_core_stat_xform}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).

-define(APP, riak_core).
-define(PFX, riak_core_stat_admin:prefix()).


start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register_stats() ->
    register_stats(common, system_stats()),
    register_stats(?APP, stats()).

%% @spec register_stats(App, Stats) -> ok
%% @doc (Re-)Register a list of metrics for App.
register_stats(App, Stats) ->
  riak_core_stat_admin:register(App, Stats).

register_vnode_stats(Module, Index, Pid) ->
  F = fun vnodeq_atom/2,
  Stat1 =  {[vnodes_running, Module],
    { function, exometer, select_count,
      [[{ {[vnodeq, Module, '_'], '_', '_'},
        [], [true] }]], match, value }, [],
    [{aliases, [{value, F(Module, <<"s_running">>)}]}]},

  Stat2 = {[vnodeq, Module],
    {function, riak_core_stat, vnodeq_stats, [Module],
      histogram, [mean,median,min,max,total]},[],
    [{aliases, [{mean  , F(Module, <<"q_mean">>)},
      {median, F(Module, <<"q_median">>)},
      {min   , F(Module, <<"q_min">>)},
      {max   , F(Module, <<"q_max">>)},
      {total , F(Module, <<"q_total">>)}]}]},

  Stat3 = {[vnodeq, Module, Index],
    function, [{ arg, {erlang, process_info, [Pid, message_queue_len],
      match, {'_', value} }}]},

  RegisterStats = [Stat1, Stat2, Stat3],
  register_stats(?APP, RegisterStats).

unregister_vnode_stats(Module, Index) ->
  riak_core_stat_admin:unregister(Module, Index, vnodeq, ?APP).


%% @spec get_stats() -> proplist()
%% @doc Get the current aggregation of stats.
get_stats() ->
    get_stats(?APP).

get_stats(App) ->
  riak_core_stat_admin:get_app_stats(App).

get_stats_info() ->
  riak_core_stat_admin:get_stats_info(?APP).

get_stat(Arg) ->
  get_value([?PFX, ?APP | Arg]).

get_value(Arg) ->
  riak_core_stat_admin:get_stat_value(Arg).

update(Arg) ->
    gen_server:cast(?SERVER, {update, Arg}).

%% gen_server

init([]) ->
    register_stats(),
    {ok, ok}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({update, {worker_pool, vnode_pool}}, State) ->
    exometer_update([?PFX, ?APP, vnode, worker_pool], 1, counter),
    {noreply, State};
handle_cast({update, {worker_pool, Pool}}, State) ->
    exometer_update([?PFX, ?APP, node, worker_pool, Pool], 1, counter),
    {noreply, State};
handle_cast({update, Arg}, State) ->
    exometer_update([?PFX, ?APP, update_metric(Arg)], update_value(Arg), update_type(Arg)),
    {noreply, State};
handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

exometer_update(Name, Value, Type) ->
    case riak_core_stat_admin:update(Name, Value, Type) of
        {error, not_found} ->
            lager:debug("~p not found on update.", [Name]);
        ok ->
            ok
    end.

update_metric(converge_timer_begin ) -> converge_delay;
update_metric(converge_timer_end   ) -> converge_delay;
update_metric(rebalance_timer_begin) -> rebalance_delay;
update_metric(rebalance_timer_end  ) -> rebalance_delay;
update_metric(Arg) -> Arg.


update_value(converge_timer_begin ) -> timer_start;
update_value(rebalance_timer_begin) -> timer_start;
update_value(converge_timer_end   ) -> timer_end;
update_value(rebalance_timer_end  ) -> timer_end;
update_value(_) -> 1.

update_type(converge_timer_begin ) -> duration;
update_type(converge_timer_end   ) -> duration;
update_type(rebalance_timer_begin) -> duration;
update_type(rebalance_timer_end  ) -> duration;
update_type(_) -> '_'.

%% private
stats() ->
    [{ignored_gossip_total, counter, [], [{value, ignored_gossip_total}]},
     {rings_reconciled, spiral, [], [{count, rings_reconciled_total},
                                     {one, rings_reconciled}]},
     {ring_creation_size,
      {function, app_helper, get_env, [riak_core, ring_creation_size],
       match, value}, [], [{value, ring_creation_size}]},
     {gossip_received, spiral, [], [{one, gossip_received}]},
     {rejected_handoffs, counter, [], [{value, rejected_handoffs}]},
     {handoff_timeouts, counter, [], [{value, handoff_timeouts}]},
     {dropped_vnode_requests, counter, [], [{value, dropped_vnode_requests_total}]},
     {converge_delay, duration, [], [{mean, converge_delay_mean},
                                     {min, converge_delay_min},
                                     {max, converge_delay_max},
                                     {last, converge_delay_last}]},
     {rebalance_delay, duration, [], [{min, rebalance_delay_min},
                                      {max, rebalance_delay_max},
                                      {mean, rebalance_delay_mean},
                                      {last, rebalance_delay_last}]} |  nwp_stats()].

nwp_stats() ->
    [ {[vnode, worker_pool], counter, [], [{value, vnode_worker_pool_total}]},
      {[node, worker_pool, unregistered], counter, [], [{value, node_worker_pool_unregistered_total}]} |
      [nwp_stat(Pool) || Pool <- riak_core_node_worker_pool:pools()]].

nwp_stat(Pool) ->
    {[node, worker_pool, Pool], counter, [], [{value, nwp_name_atom(Pool)}]}.

system_stats() ->
    [
     {cpu_stats, cpu, [{sample_interval, 5000}], [{nprocs, cpu_nprocs},
                                                  {avg1  , cpu_avg1},
                                                  {avg5  , cpu_avg5},
                                                  {avg15 , cpu_avg15}]},
     {mem_stats, {function, memsup, get_memory_data, [], match, {total, allocated, '_'}},
      [], [{total, mem_total},
           {allocated, mem_allocated}]},
     {memory_stats, {function, erlang, memory, [], proplist, [total, processes, processes_used,
                                                              system, atom, atom_used, binary,
                                                              code, ets]},
      [], [{total         , memory_total},
           {processes     , memory_processes},
           {processes_used, memory_processes_used},
           {system        , memory_system},
           {atom          , memory_atom},
           {atom_used     , memory_atom_used},
           {binary        , memory_binary},
           {code          , memory_code},
           {ets           , memory_ets}]}
    ].

%% Provide aggregate stats for vnode queues.  Compute instantaneously for now,
%% may need to cache if stats are called heavily (multiple times per seconds)
vnodeq_stats() ->
    VnodesInfo = [{Service, vnodeq_len(Pid)} ||
                     {Service, _Index, Pid} <- riak_core_vnode_manager:all_vnodes()],
    ServiceInfo = lists:foldl(fun({S,MQL}, A) ->
                                      orddict:append_list(S, [MQL], A)
                              end, orddict:new(), VnodesInfo),
    lists:flatten([vnodeq_aggregate(S, MQLs) || {S, MQLs} <- ServiceInfo]).

vnodeq_stats(Mod) ->
    [vnodeq_len(Pid) || {_, _, Pid} <- riak_core_vnode_manager:all_vnodes(Mod)].

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
    [{[?PFX, riak_core, vnodeq_atom(Service,<<"s_running">>)], [{value, Len}]},
     {[?PFX, riak_core, vnodeq_atom(Service,<<"q">>)],
      [{min, lists:nth(1, MQLs)}, {median, Median}, {mean, Mean},
       {max, lists:nth(Len, MQLs)}, {total, Total}]}].

vnodeq_atom(Service, Desc) ->
    binary_to_atom(<<(atom_to_binary(Service, latin1))/binary, Desc/binary>>, latin1).

nwp_name_atom(Atom) ->
    binary_to_atom(<< <<"node_worker_pool_">>/binary,
                      (atom_to_binary(Atom, latin1))/binary,
                      <<"_total">>/binary>>, latin1).


-ifdef(TEST).

nwp_name_to_atom_test() ->
    ?assertEqual(node_worker_pool_af1_pool_total, nwp_name_atom(af1_pool)).

%% Check vnodeq aggregation function
vnodeq_aggregate_empty_test() ->
    ?assertEqual([], vnodeq_aggregate(service_vnode, [])).

vnodeq_aggregate_odd1_test() ->
    ?assertEqual([{[?PFX, riak_core, service_vnodes_running], [{value, 1}]},
                  {[?PFX, riak_core, service_vnodeq],
		   [{min, 10}, {median, 10}, {mean, 10}, {max, 10}, {total, 10}]}],
                 vnodeq_aggregate(service_vnode, [10])).

vnodeq_aggregate_odd3_test() ->
    ?assertEqual([{[?PFX, riak_core, service_vnodes_running], [{value, 3}]},
                  {[?PFX, riak_core, service_vnodeq],
		   [{min, 1}, {median, 2}, {mean, 2}, {max, 3}, {total, 6}]}],
                 vnodeq_aggregate(service_vnode, [1, 2, 3])).

vnodeq_aggregate_odd5_test() ->
    ?assertEqual([{[?PFX, riak_core, service_vnodes_running], [{value, 5}]},
                  {[?PFX, riak_core, service_vnodeq],
		   [{min, 0}, {median, 1}, {mean, 2}, {max, 5}, {total, 10}]}],
                 vnodeq_aggregate(service_vnode, [1, 0, 5, 0, 4])).

vnodeq_aggregate_even2_test() ->
    ?assertEqual([{[?PFX, riak_core, service_vnodes_running], [{value, 2}]},
                  {[?PFX, riak_core, service_vnodeq],
		   [{min, 10}, {median, 15}, {mean, 15}, {max, 20}, {total, 30}]}],
                 vnodeq_aggregate(service_vnode, [10, 20])).

vnodeq_aggregate_even4_test() ->
    ?assertEqual([{[?PFX, riak_core, service_vnodes_running], [{value, 4}]},
                  {[?PFX, riak_core, service_vnodeq],
		   [{min, 0}, {median, 5}, {mean, 7}, {max, 20}, {total, 30}]}],
                 vnodeq_aggregate(service_vnode, [0, 10, 0, 20])).

-endif.
