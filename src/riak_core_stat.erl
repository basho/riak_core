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

-record(cuml, {count =  0 :: integer(),
               min        :: integer(),
               max   = -1 :: integer(),
               mean  =  0 :: integer(),
               last       :: integer()}).

-record(state, {
          ignored_gossip_total   :: integer(),
          rings_reconciled_total :: integer(),
          gossip_received  :: spiraltime:spiral(),
          rings_reconciled :: spiraltime:spiral(),
          converge_epoch  :: calendar:t_now(),
          converge_delay  :: #cuml{},
          rebalance_epoch :: calendar:t_now(),
          rebalance_delay :: #cuml{}
         }).

%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Start the server.  Also start the os_mon application, if it's
%%      not already running.
start_link() ->
    case application:start(os_mon) of
        ok -> ok;
        {error, {already_started, os_mon}} -> ok
    %% die if os_mon doesn't start
    end,
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
    lists:append([gossip_stats(Moment, State)]).

%% @spec spiral_minute(integer(), integer(), state()) -> integer()
%% @doc Get the count of events in the last minute from the spiraltime
%%      structure at the given element of the state tuple.
spiral_minute(_Moment, Elt, State) ->
    {_,Count} = spiraltime:rep_minute(element(Elt, State)),
    Count.

%% @spec node_stats(integer(), state()) -> proplist()
%% @doc Get the gossip stats proplist.
gossip_stats(Moment, State=#state{converge_delay=CDelay,
                                  rebalance_delay=RDelay}) ->

    [{ignored_gossip_total, State#state.ignored_gossip_total},
     {rings_reconciled_total, State#state.rings_reconciled_total},
     {rings_reconciled, spiral_minute(Moment, #state.rings_reconciled, State)},
     {gossip_received, spiral_minute(Moment, #state.gossip_received, State)},
     {converge_delay_min,  CDelay#cuml.min},
     {converge_delay_max,  CDelay#cuml.max},
     {converge_delay_mean, CDelay#cuml.mean},
     {converge_delay_last, CDelay#cuml.last},
     {rebalance_delay_min,  RDelay#cuml.min},
     {rebalance_delay_max,  RDelay#cuml.max},
     {rebalance_delay_mean, RDelay#cuml.mean},
     {rebalance_delay_last, RDelay#cuml.last}].
