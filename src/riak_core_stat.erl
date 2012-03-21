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
-behaviour(gen_server2).

%% API
-export([start_link/0, get_stats/0, get_stats/1, update/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(cuml, {count =  0 :: integer(),
               min        :: integer(),
               max   = -1 :: integer(),
               mean  =  0 :: integer(),
               last       :: integer()}).

-record(state, {
          ignored_gossip_total   :: integer(),
          rings_reconciled_total :: integer(),
          rejected_handoffs      :: integer(),
          handoff_timeouts       :: integer(),
          gossip_received        :: spiraltime:spiral(),
          rings_reconciled       :: spiraltime:spiral(),
          converge_epoch         :: calendar:t_now(),
          converge_delay         :: #cuml{},
          rebalance_epoch        :: calendar:t_now(),
          rebalance_delay        :: #cuml{}
         }).

%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Start the server.
start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @spec get_stats() -> proplist()
%% @doc Get the current aggregation of stats.
get_stats() ->
    get_stats(slide:moment()).

get_stats(Moment) ->
    gen_server2:call(?MODULE, {get_stats, Moment}, infinity).

%% @spec update(term()) -> ok
%% @doc Update the given stat.
update(Stat) ->
    gen_server2:cast(?MODULE, {update, Stat, slide:moment()}).

%% @private
init([]) ->
    %% Removing the slide directory here would conflict with riak_kv_stat.
    %% We will need to resolve if we ever use slide metrics in this module.
    %%
    %% process_flag(trap_exit, true),
    %% remove_slide_private_dirs(),

    {ok, #state{ignored_gossip_total=0,
                rings_reconciled_total=0,
                rejected_handoffs=0,
                handoff_timeouts=0,
                gossip_received=spiraltime:fresh(),
                rings_reconciled=spiraltime:fresh(),
                converge_delay=#cuml{},
                rebalance_delay=#cuml{}
               }}.

%% @private
handle_call({get_stats, Moment}, _From, State) ->
    {reply, produce_stats(State, Moment), State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%% @private
handle_cast({update, Stat, Moment}, State) ->
    {noreply, update(Stat, Moment, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

%% @doc Update the given stat in State, returning a new State.
-spec update(Stat::term(), integer(), #state{}) -> #state{}.
update(converge_timer_begin, _Moment, State) ->
    State#state{converge_epoch=erlang:now()};
update(converge_timer_end, _Moment, State=#state{converge_epoch=undefined}) ->
    State;
update(converge_timer_end, _Moment, State=#state{converge_epoch=T0}) ->
    Duration = timer:now_diff(erlang:now(), T0),
    update_cumulative(#state.converge_delay, Duration,
                      State#state{converge_epoch=undefined});

update(rebalance_timer_begin, _Moment, State) ->
    State#state{rebalance_epoch=erlang:now()};
update(rebalance_timer_end, _Moment, State=#state{rebalance_epoch=undefined}) ->
    State;
update(rebalance_timer_end, _Moment, State=#state{rebalance_epoch=T0}) ->
    Duration = timer:now_diff(erlang:now(), T0),
    update_cumulative(#state.rebalance_delay, Duration,
                      State#state{rebalance_epoch=undefined});

update(rejected_handoffs, _Moment, State) ->
    int_incr(#state.rejected_handoffs, State);

update(handoff_timeouts, _Moment, State) ->
    int_incr(#state.handoff_timeouts, State);

update(ignored_gossip, _Moment, State) ->
    int_incr(#state.ignored_gossip_total, State);

update(gossip_received, Moment, State) ->
    spiral_incr(#state.gossip_received, Moment, State);

update(rings_reconciled, Moment, State) ->
    spiral_incr(#state.rings_reconciled, Moment,
                int_incr(#state.rings_reconciled_total, State));

update(_, _, State) ->
    State.

%% @spec spiral_incr(integer(), integer(), state()) -> state()
%% @doc Increment the value of a spiraltime structure at a given
%%      position of the State tuple.
spiral_incr(Elt, Moment, State) ->
    setelement(Elt, State,
               spiraltime:incr(1, Moment, element(Elt, State))).

%% @doc Increment the value at the given position of the State tuple.
int_incr(Elt, State) ->
    int_incr(Elt, 1, State).
int_incr(Elt, Amount, State) ->
    setelement(Elt, State, element(Elt, State) + Amount).

%% @doc Add a value to a set, updating the cumulative min/max/mean
update_cumulative(Elt, Value, State) ->
    #cuml{count=N, min=Min, max=Max, mean=Mean} = element(Elt, State),
    Min2 = erlang:min(Min, Value),
    Max2 = erlang:max(Max, Value),
    Mean2 = ((N * Mean) + Value) div (N+1),
    Stat2 = #cuml{count=N+1, min=Min2, max=Max2, mean=Mean2, last=Value},
    setelement(Elt, State, Stat2).

%% @spec produce_stats(state(), integer()) -> proplist()
%% @doc Produce a proplist-formatted view of the current aggregation
%%      of stats.
produce_stats(State, Moment) ->
    lists:append([gossip_stats(Moment, State),
                  vnodeq_stats()]).

%% @spec spiral_minute(integer(), integer(), state()) -> integer()
%% @doc Get the count of events in the last minute from the spiraltime
%%      structure at the given element of the state tuple.
spiral_minute(_Moment, Elt, State) ->
    {_,Count} = spiraltime:rep_minute(element(Elt, State)),
    Count.

%% @spec gossip_stats(integer(), state()) -> proplist()
%% @doc Get the gossip stats proplist.
gossip_stats(Moment, State=#state{converge_delay=CDelay,
                                  rebalance_delay=RDelay}) ->

    [{ignored_gossip_total, State#state.ignored_gossip_total},
     {rings_reconciled_total, State#state.rings_reconciled_total},
     {rings_reconciled, spiral_minute(Moment, #state.rings_reconciled, State)},
     {gossip_received, spiral_minute(Moment, #state.gossip_received, State)},
     {handoff_timeouts, State#state.handoff_timeouts},
     {converge_delay_min,  CDelay#cuml.min},
     {converge_delay_max,  CDelay#cuml.max},
     {converge_delay_mean, CDelay#cuml.mean},
     {converge_delay_last, CDelay#cuml.last},
     {rebalance_delay_min,  RDelay#cuml.min},
     {rebalance_delay_max,  RDelay#cuml.max},
     {rebalance_delay_mean, RDelay#cuml.mean},
     {rebalance_delay_last, RDelay#cuml.last}].


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
