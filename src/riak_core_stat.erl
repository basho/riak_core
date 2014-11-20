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
         register_stats/0, produce_stats/0, vnodeq_stats/0,
	 register_stats/2,
	 register_vnode_stats/3, unregister_vnode_stats/2,
	 vnodeq_stats/1,
	 legacy_stat_map/0,
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
    {Name, Type, Opts} = case Stat of
			     {N, T}     -> {N, T, []};
			     {N, T, Os} -> {N, T, Os}
			 end,
    exometer:re_register(stat_name(P,App,Name), Type, Opts).

register_vnode_stats(Module, Index, Pid) ->
    P = prefix(),
    exometer:ensure([P, ?APP, vnodes_running, Module],
		    { function, exometer, select_count,
		      [[{ {[P, ?APP, vnodeq, Module, '_'], '_', '_'},
			  [], [true] }]], value, [value] }, []),
    exometer:ensure([P, ?APP, vnodeq, Module],
		    {function, riak_core_stat, vnodeq_stats, [Module],
		     histogram, default}, []),
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

%% @spec produce_stats() -> proplist()
%% @doc Produce a proplist-formatted view of the current aggregation
%%      of stats.
produce_stats() ->
    lists:append([gossip_stats(),
                  vnodeq_stats()]).

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
    [{ignored_gossip_total, counter},
     {rings_reconciled, spiral},
     {ring_creation_size,
      {function, app_helper, get_env, [riak_core, ring_creation_size],
       match, value}},
     {gossip_received, spiral},
     {rejected_handoffs, counter},
     {handoff_timeouts, counter},
     {dropped_vnode_requests_total, counter},
     {converge_delay, duration},
     {rebalance_delay, duration}].

system_stats() ->
    [{cpu_stats, cpu, [{sample_interval, 5000}]}].


gossip_stats() ->
    lists:flatten([backwards_compat(Stat, Type, riak_core_stat_q:calc_stat({{?APP, Stat}, Type})) ||
                      {Stat, Type} <- stats(), Stat /= riak_core_rejected_handoffs]).

backwards_compat(Name, Type, unavailable) when Type =/= counter ->
    backwards_compat(Name, Type, []);
backwards_compat(Name, Type, {error,not_found}) when Type =/= counter ->
    backwards_compat(Name, Type, []);
backwards_compat(rings_reconciled, spiral, Stats) ->
    [{rings_reconciled_total, proplists:get_value(count, Stats, unavailable)},
    {rings_reconciled, safe_trunc(proplists:get_value(one, Stats, unavailable))}];
backwards_compat(gossip_received, spiral, Stats) ->
    {gossip_received, safe_trunc(proplists:get_value(one, Stats, unavailable))};
backwards_compat(Name, counter, Stats) ->
    {Name, proplists:get_value(value, Stats)};
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


legacy_stat_map() ->
    [
     {[riak_kv,object,merge], [{one  , object_merge},
                               {count, object_merge_total}]},
     {[riak_kv,object,map,merge], [{one  , object_map_merge},
                                   {count, object_map_merge_total}]},
     {[riak_kv,object,merge,time], [{mean  , object_merge_time_mean},
                                    {median, object_merge_time_median},
                                    {95    , object_merge_time_95},
                                    {99    , object_merge_time_99},
                                    {max   , object_merge_time_100}]},
     {[riak_kv,vnode,map,update,time], [{mean  , vnode_map_update_time_mean},
                                        {median, vnode_map_update_time_median},
                                        {95    , vnode_map_update_time_95},
                                        {99    , vnode_map_update_time_99},
                                        {max   , vnode_map_update_time_100}]},
     {[riak_kv,vnode,gets], [{one  , vnode_gets},
                             {count, vnode_gets_total}]},
     {[riak_kv,vnode,puts], [{one  , vnode_puts},
                             {count, vnode_puts_total}]},
     {[riak_kv,vnode,index,reads], [{one  , vnode_index_reads},
                                    {count, vnode_index_reads_total}]},
     {[riak_kv,vnode,index,refreshes], [{one  ,vnode_index_refreshes},
                                        {count, vnode_index_refreshes_total}]},
     {[riak_kv,vnode,index,writes], [{one  , vnode_index_writes},
                                     {count, vnode_index_writes_total}]},
     {[riak_kv,vnode,index,writes,postings], [{one  , vnode_index_writes_postings},
                                              {count, vnode_index_writes_postings_total}]},
     {[riak_kv,vnode,index,deletes], [{one  , vnode_index_deletes},
                                      {count, vnode_index_deletes_total}]},
     {[riak_kv,vnode,index,deletes,postings], [{one  , vnode_index_deletes_postings},
                                               {count, vnode_index_deletes_postings_total}]},
     {[riak_kv,vnode,backend,leveldb,read_block_error],
      [{value, leveldb_read_block_error}]},
     {[riak_kv,object,map,merge,time], [{mean  , object_map_merge_time_mean},
                                        {median, object_map_merge_time_median},
                                        {95    , object_map_merge_time_95},
                                        {99    , object_map_merge_time_99},
                                        {max   , object_map_merge_time_100}]},
     {[riak_kv,node,gets], [{one  , node_gets},
                            {count, node_gets_total}]},
     {[riak_kv,node,gets,siblings], [{mean  , node_get_fsm_siblings_mean},
                                     {median, node_get_fsm_siblings_median},
                                     {95    , node_get_fsm_siblings_95},
                                     {99    , node_get_fsm_siblings_99},
                                     {max   , node_get_fsm_siblings_100}]},
     {[riak_kv,node,gets,set,siblings], [{mean  , node_get_fsm_set_siblings_mean},
                                         {median, node_get_fsm_set_siblings_median},
                                         {95    , node_get_fsm_set_siblings_95},
                                         {99    , node_get_fsm_set_siblings_99},
                                         {max   , node_get_fsm_set_siblings_100}]},
     {[riak_kv,node,gets,objsize], [{mean  , node_get_fsm_objsize_mean},
                                    {median, node_get_fsm_objsize_median},
                                    {95    , node_get_fsm_objsize_95},
                                    {99    , node_get_fsm_objsize_99},
                                    {max   , node_get_fsm_objsize_100}]},
     {[riak_kv,node,gets,set,objsize], [{mean  , node_get_fsm_set_objsize_mean},
                                        {median, node_get_fsm_set_objsize_median},
                                        {95    , node_get_fsm_set_objsize_95},
                                        {99    , node_get_fsm_set_objsize_99},
                                        {max   , node_get_fsm_set_objsize_100}]},
     {[riak_kv,node,gets,time], [{mean  , node_get_fsm_time_mean},
                                 {median, node_get_fsm_time_median},
                                 {95    , node_get_fsm_time_95},
                                 {99    , node_get_fsm_time_99},
                                 {max   , node_get_fsm_time_100}]},
     {[riak_kv,node,gets,map,siblings], [{mean  , node_get_fsm_map_siblings_mean},
                                         {median, node_get_fsm_map_siblings_median},
                                         {95    , node_get_fsm_map_siblings_95},
                                         {99    , node_get_fsm_map_siblings_99},
                                         {max   , node_get_fsm_map_siblings_100}]},
     {[riak_kv,node,gets,map,objsize], [{mean  , node_get_fsm_map_objsize_mean},
                                        {median, node_get_fsm_map_objsize_median},
                                        {95    , node_get_fsm_map_objsize_95},
                                        {99    , node_get_fsm_map_objsize_99},
                                        {max   , node_get_fsm_map_objsize_100}]},
     {[riak_kv,node,gets,map,time], [{mean  , node_get_fsm_map_time_mean},
                                     {median, node_get_fsm_map_time_median},
                                     {95    , node_get_fsm_map_time_95},
                                     {99    , node_get_fsm_map_time_99},
                                     {max   , node_get_fsm_map_time_100}]},
     {[riak_kv,node,gets,set,time], [{mean  , node_get_fsm_set_time_mean},
                                     {median, node_get_fsm_set_time_median},
                                     {95    , node_get_fsm_set_time_95},
                                     {99    , node_get_fsm_set_time_99},
                                     {max   , node_get_fsm_set_time_100}]},
     {[riak_kv,node,gets,set], [{one  , node_gets_set},
                                {count, node_gets_set_total}]},
     {[riak_kv,node,puts], [{one, node_puts},
                            {count, node_puts_total}]},
     {[riak_kv,node,puts,time], [{mean  , node_put_fsm_time_mean},
                                 {median, node_put_fsm_time_median},
                                 {95    , node_put_fsm_time_95},
                                 {99    , node_put_fsm_time_99},
                                 {max   , node_put_fsm_time_100}]},
     {[riak_kv,node,gets,counter,objsize], [{mean  , node_get_fsm_counter_objsize_mean},
                                            {median, node_get_fsm_counter_objsize_median},
                                            {95    , node_get_fsm_counter_objsize_95},
                                            {99    , node_get_fsm_counter_objsize_99},
                                            {max   , node_get_fsm_counter_objsize_100}]},
     {[riak_kv,node,gets,read_repairs], [{one, read_repairs},
                                         {count, read_repairs_total}]},
     {[riak_kv,node,gets,skipped_read_repairs], [{one, skipped_read_repairs},
						 {count, skipped_read_repairs_total}]},
     {[riak_kv,node,gets,counter], [{one  , node_gets_counter},
                                    {count, node_gets_counter_total}]},
     {[riak_kv,node,gets,counter,siblings], [{mean  , node_get_fsm_counter_siblings_mean},
                                             {median, node_get_fsm_counter_siblings_median},
                                             {95    , node_get_fsm_counter_siblings_95},
                                             {99    , node_get_fsm_counter_siblings_99},
                                             {max   , node_get_fsm_counter_siblings_100}]},
     {[riak_kv,node,gets,counter,time], [{mean  , node_get_fsm_counter_time_mean},
                                         {median, node_get_fsm_counter_time_median},
                                         {95    , node_get_fsm_counter_time_95},
                                         {99    , node_get_fsm_counter_time_99},
                                         {max   , node_get_fsm_counter_time_100}]},
     {[riak_kv,node,puts,coord_redirs], [{value,coord_redirs_total}]},
     {[riak_kv,mapper_count], [{value, executing_mappers}]},
     {[riak_kv,object,counter,merge], [{one, object_counter_merge},
                                       {count, object_counter_merge_total}]},
     {[riak_kv,object,counter,merge,time], [{mean  , object_counter_merge_time_mean},
                                            {median, object_counter_merge_time_median},
                                            {95    , object_counter_merge_time_95},
                                            {99    , object_counter_merge_time_99},
                                            {max   , object_counter_merge_time_100}]},
     {[riak_kv,object,set,merge], [{one  , object_set_merge},
                                   {count, object_set_merge_total}]},
     {[riak_kv,object,set,merge,time], [{mean  , object_set_merge_time_mean},
                                        {median, object_set_merge_time_median},
                                        {95    , object_set_merge_time_95},
                                        {99    , object_set_merge_time_99},
                                        {max   , object_set_merge_time_100}]},
     {[riak_kv,precommit_fail], [{value, precommit_fail}]},
     {[riak_kv,postcommit_fail], [{value, postcommit_fail}]},
     {[riak_kv,index,fsm,create], [{one, index_fsm_create}]},
     {[riak_kv,index,fsm,create,error], [{one, index_fsm_create_error}]},
     {[riak_kv,index,fsm,active], [{value, index_fsm_active}]},
     {[riak_kv,list,fsm,active], [{value, list_fsm_active}]},
     {[riak_kv,consistent,gets], [{one, consistent_gets},
                                  {count, consistent_gets_total}]},
     {[riak_kv,consistent,gets,objsize], [{mean  , consistent_get_objsize_mean},
                                          {median, consistent_get_objsize_median},
                                          {95    , consistent_get_objsize_95},
                                          {99    , consistent_get_objsize_99},
                                          {max   , consistent_get_objsize_100}]},
     {[riak_kv,consistent,gets,time], [{mean  , consistent_get_time_mean},
                                       {median, consistent_get_time_median},
                                       {95    , consistent_get_time_95},
                                       {99    , consistent_get_time_99},
                                       {max   , consistent_get_time_100}]},
     {[riak_kv,consistent,puts], [{one, consistent_puts},
                                  {count, consistent_puts_total}]},
     {[riak_kv,consistent,puts,objsize], [{mean  , consistent_put_objsize_mean},
                                          {median, consistent_put_objsize_median},
                                          {95    , consistent_put_objsize_95},
                                          {99    , consistent_put_objsize_99},
                                          {max   , consistent_put_objsize_100}]},
     {[riak_kv,consistent,puts,time], [{mean  , consistent_put_time_mean},
                                       {median, consistent_put_time_median},
                                       {95    , consistent_put_time_95},
                                       {99    , consistent_put_time_99},
                                       {max   , consistent_put_time_100}]},
     {[riak_api,pbc_connects,active], [{value, pbc_active}]},
     {[riak_api,pbc_connects], [{one, pbc_connects},
                                {count, pbc_connects_total}]},
     {[riak_kv,get_fsm,sidejob], [{usage, node_get_fsm_active},
                                  {usage_60s, node_get_fsm_active_60s},
                                  {in_rate, node_get_fsm_in_rate},
                                  {out_rate, node_get_fsm_out_rate},
                                  {rejected, node_get_fsm_rejected},
                                  {rejected_60s, node_get_fsm_rejected_60s},
                                  {rejected_total, node_get_fsm_rejected_total}]},
     {[riak_kv,put_fsm,sidejob], [{usage, node_put_fsm_active},
                                  {usage_60s, node_put_fsm_active_60s},
                                  {in_rate, node_put_fsm_in_rate},
                                  {out_rate, node_put_fsm_out_rate},
                                  {rejected, node_put_fsm_rejected},
                                  {rejected_60s, node_put_fsm_rejected_60s},
                                  {rejected_total, node_put_fsm_rejected_total}]},
     {[riak_kv,node,puts,counter,time], [{mean  , node_put_fsm_counter_time_mean},
					 {median, node_put_fsm_counter_time_median},
					 {95    , node_put_fsm_counter_time_95},
					 {99    , node_put_fsm_counter_time_99},
					 {max   , node_put_fsm_counter_time_100}]},
     {[riak_kv,node,puts,set,time], [{mean  , node_put_fsm_set_time_mean},
				     {median, node_put_fsm_set_time_median},
				     {95    , node_put_fsm_set_time_95},
				     {99    , node_put_fsm_set_time_99},
				     {max   , node_put_fsm_set_time_100}]},
     {[riak_kv,node,puts,counter], [{one  , node_puts_counter},
                                    {count, node_puts_counter_total}]},
     {[riak_kv,node,puts,set], [{one  , node_puts_set},
                                {count, node_puts_set_total}]},
     {[riak_kv,node,gets,map], [{one  , node_gets_map},
                                {count, node_gets_map_total}]},
     {[riak_kv,node,gets,counter,read_repairs], [{one  , read_repairs_counter},
                                                 {count, read_repairs_counter_total}]},
     {[riak_kv,node,puts,map], [{one  , node_puts_map},
                                {count, node_puts_map_total}]},
     {[riak_kv,node,puts,map,time], [{mean  , node_put_fsm_map_time_mean},
                                     {median, node_put_fsm_map_time_median},
                                     {95    , node_put_fsm_map_time_95},
                                     {99    , node_put_fsm_map_time_99},
                                     {max   , node_put_fsm_map_time_100}]},
     {[riak_kv,node,gets,set,read_repairs], [{one  , read_repairs_set},
                                             {count, read_repairs_set_total}]},
     {[riak_kv,node,gets,map,read_repairs,map], [{one  , read_repairs_map},
                                                 {count, read_repairs_map_total}]},
     {[riak_kv,node,gets,read_repairs,primary,notfound],
      [{one  , read_repairs_primary_notfound_one},
       {count, read_repairs_primary_notfound_count}]},
     {[riak_kv,node,gets,read_repairs,primary,outofdate],
      [{one  , read_repairs_primary_outofdate_one},
       {count, read_repairs_primary_outofdate_count}]},
     {[riak_kv,list,fsm,create], [{one  , list_fsm_create},
                                  {count, list_fsm_create_total}]},
     {[riak_kv,list,fsm,create,error], [{one  , list_fsm_create_error},
                                        {count, list_fsm_create_error_total}]},
     {[riak_kv,counter,actor_count], [{mean  , counter_actor_counts_mean},
                                      {median, counter_actor_counts_median},
                                      {95    , counter_actor_counts_95},
                                      {99    , counter_actor_counts_99},
                                      {max   , counter_actor_counts_100}]},
     {[riak_kv,set,actor_count], [{mean  , set_actor_counts_mean},
                                  {median, set_actor_counts_median},
                                  {95    , set_actor_counts_95},
                                  {99    , set_actor_counts_99},
                                  {max   , set_actor_counts_100}]},
     {[riak_kv,map,actor_count], [{mean  , map_actor_counts_mean},
                                  {median, map_actor_counts_median},
                                  {95    , map_actor_counts_95},
                                  {99    , map_actor_counts_99},
                                  {max   , map_actor_counts_100}]},
     {[riak_kv,late_put_fsm_coordinator_ack], [{value, late_put_fsm_coordinator_ack}]},
     {[riak_kv,vnode,counter,update], [{one  , vnode_counter_update},
                                       {count, vnode_counter_update_total}]},
     {[riak_kv,vnode,counter,update,time], [{mean  , vnode_counter_update_time_mean},
                                            {median, vnode_counter_update_time_median},
                                            {95    , vnode_counter_update_time_95},
                                            {99    , vnode_counter_update_time_99},
                                            {max   , vnode_counter_update_time_100}]},
     {[riak_kv,vnode,map,update],  [{one  , vnode_map_update},
                                    {count, vnode_map_update_total}]},
     {[riak_kv,vnode,set,update], [{one  , vnode_set_update},
                                   {count, vnode_set_update_total}]},
     {[riak_kv,vnode,set,update,time], [{mean  , vnode_set_update_time_mean},
                                        {median, vnode_set_update_time_median},
                                        {95    , vnode_set_update_time_95},
                                        {99    , vnode_set_update_time_99},
                                        {max   , vnode_set_update_time_100}]},
     {[riak_kv,ring_stats], [{ring_members       , ring_members},
                             {ring_num_partitions, ring_num_partitions},
                             {ring_ownership     , ring_ownership}]},
     {[riak_core,ring_creation_size], [{value, ring_creation_size}]},
     {[riak_kv,storage_backend], [{value, storage_backend}]},
     {[common,cpu_stats], [{nprocs, cpu_nprocs},
                           {avg1  , cpu_avg1},
                           {avg5  , cpu_avg5},
                           {avg15 , cpu_avg15}]},
     {[riak_core_stat_ts], [{value, riak_core_stat_ts}]},
     {[riak_kv_stat_ts]  , [{value, riak_kv_stat_ts}]},
     {[riak_pipe_stat_ts], [{value, riak_pipe_stat_ts}]},
     {[yokozuna,index,fail], [{count,search_index_fail_count},
                              {one  ,search_index_fail_one}]},
     {[yokozuna,index,latency], [{95    , search_index_latency_95},
                                 {99    , search_index_latency_99},
                                 {999   , search_index_latency_999},
                                 {max   , search_index_latency_max},
                                 {median, search_index_latency_median},
                                 {min   , search_index_latency_min}]},
     {[yokozuna,index,throughput], [{count, search_index_throughput_count},
                                    {one  , search_index_throughtput_one}]},
     {[yokozuna,'query',fail], [{count, search_search_query_fail_count},
                                {one  , search_query_fail_one}]},
     {[yokozuna,'query',latency], [{95    , search_query_latency_95},
                                   {99    , search_query_latency_99},
                                   {999   , search_query_latency_999},
                                   {max   , search_query_latency_max},
                                   {median, search_query_latency_median},
                                   {min   , search_query_latency_min}]},
     {[yokozuna,'query',throughput], [{count,search_query_throughput_count},
                                      {one  ,search_query_throughput_one}]}
    ].

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
