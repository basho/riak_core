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
-export([start_link/0, get_stats/0, get_stats/1, update/1,
         register_stats/0, vnodeq_stats/0,
	 register_stats/2,
	 register_vnode_stats/3, unregister_vnode_stats/2,
	 vnodeq_stats/1,
	 prefix/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-compile({parse_transform, riak_core_stat_xform}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).

-define(APP, riak_core).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register_stats() ->
    register_stats(common, system_stats()),
    register_stats(?APP, stats()).

%% @spec register_stats(App, Stats) -> ok
%% @doc (Re-)Register a list of metrics for App.
register_stats(App, Stats) ->
    P = prefix(),
    lists:foreach(fun(Stat) ->
			  register_stat(P, App, Stat)
		  end, Stats).

register_stat(P, App, Stat) ->
    {Name, Type, Opts, Aliases} =
        case Stat of
            {N, T}         -> {N, T, [], []};
            {N, T, Os}     -> {N, T, Os, []};
            {N, T, Os, As} -> {N, T, Os, As}
        end,
    StatName = stat_name(P, App, Name),
    exometer:re_register(StatName, Type, Opts),
    lists:foreach(
      fun({DP, Alias}) ->
              exometer_alias:new(Alias, StatName, DP)
      end, Aliases).

register_vnode_stats(Module, Index, Pid) ->
    P = prefix(),
    exometer:ensure([P, ?APP, vnodes_running, Module],
		    { function, exometer, select_count,
		      [[{ {[P, ?APP, vnodeq, Module, '_'], '_', '_'},
			  [], [true] }]], match, value },
                    [{aliases, [{value, vnodeq_atom(Module, <<"s_running">>)}]}]),
    exometer:ensure([P, ?APP, vnodeq, Module],
		    {function, riak_core_stat, vnodeq_stats, [Module],
		     histogram, [mean,median,min,max,total]},
                    [{aliases, [{mean  , vnodeq_atom(Module, <<"q_mean">>)},
				{median, vnodeq_atom(Module, <<"q_median">>)},
				{min   , vnodeq_atom(Module, <<"q_min">>)},
				{max   , vnodeq_atom(Module, <<"q_max">>)},
				{total , vnodeq_atom(Module, <<"q_total">>)}]}
		    ]),
    exometer:re_register(
      [P, ?APP, vnodeq, Module, Index],
      function, [{ arg, {erlang, process_info, [Pid, message_queue_len],
                                match, {'_', value} }}]).

unregister_vnode_stats(Module, Index) ->
    exometer:delete([riak_core_stat:prefix(), ?APP, vnodeq, Module, Index]).

stat_name(P, App, N) when is_atom(N) ->
    stat_name_([P, App, N]);
stat_name(P, App, N) when is_list(N) ->
    stat_name_([P, App | N]).

stat_name_([P, [] | Rest]) -> [P | Rest];
stat_name_(N) -> N.


%% @spec get_stats() -> proplist()
%% @doc Get the current aggregation of stats.
get_stats() ->
    get_stats(?APP).

get_stats(App) ->
    P = prefix(),
    exometer:get_values([P, App]).

update(Arg) ->
    gen_server:cast(?SERVER, {update, Arg}).

prefix() ->
    app_helper:get_env(riak_core, stat_prefix, riak).

%% gen_server

init([]) ->
    register_stats(),
    {ok, ok}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({update, Arg}, State) ->
    case exometer:update([prefix(), ?APP, update_metric(Arg)], update_value(Arg)) of
        {error, not_found} ->
            lager:debug("~p not found on update.", [Arg]);
        ok ->
            ok
    end,
    {noreply, State};
handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

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
                                      {last, rebalance_delay_last}]}].

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
    P = prefix(),
    [{[P, riak_core, vnodeq_atom(Service,<<"s_running">>)], [{value, Len}]},
     {[P, riak_core, vnodeq_atom(Service,<<"q">>)],
      [{min, lists:nth(1, MQLs)}, {median, Median}, {mean, Mean},
       {max, lists:nth(Len, MQLs)}, {total, Total}]}].

vnodeq_atom(Service, Desc) ->
    binary_to_atom(<<(atom_to_binary(Service, latin1))/binary, Desc/binary>>, latin1).


-ifdef(TEST).

%% Check vnodeq aggregation function
vnodeq_aggregate_empty_test() ->
    ?assertEqual([], vnodeq_aggregate(service_vnode, [])).

vnodeq_aggregate_odd1_test() ->
    P = prefix(),
    ?assertEqual([{[P, riak_core, service_vnodes_running], [{value, 1}]},
                  {[P, riak_core, service_vnodeq],
		   [{min, 10}, {median, 10}, {mean, 10}, {max, 10}, {total, 10}]}],
                 vnodeq_aggregate(service_vnode, [10])).

vnodeq_aggregate_odd3_test() ->
    P = prefix(),
    ?assertEqual([{[P, riak_core, service_vnodes_running], [{value, 3}]},
                  {[P, riak_core, service_vnodeq],
		   [{min, 1}, {median, 2}, {mean, 2}, {max, 3}, {total, 6}]}],
                 vnodeq_aggregate(service_vnode, [1, 2, 3])).

vnodeq_aggregate_odd5_test() ->
    P = prefix(),
    ?assertEqual([{[P, riak_core, service_vnodes_running], [{value, 5}]},
                  {[P, riak_core, service_vnodeq],
		   [{min, 0}, {median, 1}, {mean, 2}, {max, 5}, {total, 10}]}],
                 vnodeq_aggregate(service_vnode, [1, 0, 5, 0, 4])).

vnodeq_aggregate_even2_test() ->
    P = prefix(),
    ?assertEqual([{[P, riak_core, service_vnodes_running], [{value, 2}]},
                  {[P, riak_core, service_vnodeq],
		   [{min, 10}, {median, 15}, {mean, 15}, {max, 20}, {total, 30}]}],
                 vnodeq_aggregate(service_vnode, [10, 20])).

vnodeq_aggregate_even4_test() ->
    P = prefix(),
    ?assertEqual([{[P, riak_core, service_vnodes_running], [{value, 4}]},
                  {[P, riak_core, service_vnodeq],
		   [{min, 0}, {median, 5}, {mean, 7}, {max, 20}, {total, 30}]}],
                 vnodeq_aggregate(service_vnode, [0, 10, 0, 20])).

-endif.
