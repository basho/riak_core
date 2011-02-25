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
-module(riak_core_tracer).

-behaviour(gen_server).
-include_lib("riak_kv/include/riak_kv_vnode.hrl").

%% API
-export([start_link/0,
         reset/0,
         filter/2,
         collect/0, collect/1, collect/2,
         results/0]).
-export([log_get_fsm/0,
         log_put_fsm/0]).
-export([test_get/0,
         test_put/0,
         test_get_put/0,
         test_all_events/1]).

-export([all_events/1, get_fsm_events/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, {trace=[],
                filters=[],
                mfs=[],
                stop_tref}).

%%===================================================================
%% API
%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

reset() ->
    gen_server:call(?SERVER, reset).

%% Set up a filter on trace messages to the list of [{M, F}]s.  The
%% tracer function should return a list of events to log
filter(MFs, Filter) ->
    gen_server:call(?SERVER, {filter, MFs, Filter}).

%% Collect traces
collect() ->
    collect(100).

collect(Duration) ->
    collect(Duration, nodes()).

collect(Duration, Nodes) ->
    gen_server:call(?SERVER, {collect, Duration,  Nodes}).

%% Return the trace
results() ->
    gen_server:call(?SERVER, results).

all_events({trace, Pid, call, {M,F,A}}) ->
    [{node(Pid), {M,F,A}}].

%%===================================================================
%% Pre-canned examples - tracing of riak_kv, but not compile time
%% coupling.
%%===================================================================

get_fsm_events({trace, _Pid, call,
                {riak_kv_get_fsm,start,
                 [ReqId,B,K,_R,_T,_ReplyTo]}}) ->
    [{get_start, ReqId, {B, K}}];
get_fsm_events({trace, _Pid, call,
                {riak_kv_get_fsm, client_reply,
                 [Reply, State]}}) ->
    [{get_finished, element(8, State), Reply}].

log_get_fsm() ->
    riak_core_tracer:filter([{riak_kv_get_fsm, start},
                   {riak_kv_get_fsm, client_reply}],
                  fun get_fsm_events/1).

put_fsm_events({trace, _Pid, call,
               {riak_kv_put_fsm,start,
                [ReqId,Obj, _W, _DW,_T,_ReplyTo,[]]}}) ->
    [{put_start, ReqId, Obj}];
put_fsm_events({trace, _Pid, call,
                {riak_kv_put_fsm, send_reply,
                 [_Client, ReqId, Reply]}}) ->
    [{put_finished, ReqId, Reply}].

log_put_fsm() ->
    riak_core_tracer:filter([{riak_kv_put_fsm, start},
                   {riak_kv_put_fsm, send_reply}],
                  fun put_fsm_events/1).
 
%% Some functions to execute
test_get() ->
    riak_core_tracer:start_link(),
    riak_core_tracer:reset(),
    log_get_fsm(),
    riak_core_tracer:collect(5000),
    {ok, C} = riak:local_client(),
    C:get(<<"b">>,<<"k1">>),
    C:get(<<"b">>,<<"k2">>),
    C:get(<<"b">>,<<"k3">>),
    timer:sleep(100),
    riak_core_tracer:results().

test_put() ->
    riak_core_tracer:start_link(),
    riak_core_tracer:reset(),
    log_put_fsm(),
    riak_core_tracer:collect(5000),
    {ok, C} = riak:local_client(),
    C:put(riak_object:new(<<"b">>,<<"k1">>,<<"v1">>)),
    C:put(riak_object:new(<<"b">>,<<"k2">>,<<"v2">>)),
    C:put(riak_object:new(<<"b">>,<<"k3">>,<<"v3">>)),
    timer:sleep(100),
    riak_core_tracer:results().

test_get_put() ->
    riak_core_tracer:start_link(),
    riak_core_tracer:reset(),
    log_get_fsm(),
    riak_core_tracer:collect(5000),
    {ok, C} = riak:local_client(),
    C:get(<<"b">>,<<"k1">>),
    C:put(riak_object:new(<<"b">>,<<"k1">>,<<"v1">>)),
    C:get(<<"b">>,<<"k2">>),
    C:put(riak_object:new(<<"b">>,<<"k1">>,<<"v1">>)),
    timer:sleep(100),
    riak_core_tracer:results().


test_all_events(Ms) ->
    riak_core_tracer:start_link(),
    riak_core_tracer:reset(),
    riak_core_tracer:filter(Ms, fun all_events/1),
    riak_core_tracer:collect(5000).

%%===================================================================
%% gen_server callbacks
%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call(reset, _From, State) ->
    cancel_timer(State#state.stop_tref),
    {reply, ok, #state{}};
handle_call({filter, MFs, Filter}, _From, State) ->
    {reply, ok, State#state{mfs=MFs ++ State#state.mfs,
                            filters = [Filter | State#state.filters]}};
handle_call({collect, Duration, Nodes}, _From, State) ->
    cancel_timer(State#state.stop_tref),
    Tref = timer:send_after(Duration, stop),
    dbg:tracer(process, {fun (Msg, Pid) ->
                                 %io:format("~p ! ~p\n", [Pid, Msg]),
                                 Entries = lists:flatten(
                                             [begin
                                                  case catch F(Msg) of
                                                      {'EXIT', _} ->
                                                          [];
                                                      E ->
                                                          E
                                                  end
                                              end || F <- State#state.filters]),
                                 case Entries of
                                     [] ->
                                         ok;
                                     _ ->
                                         {Mega, Secs, Micro} = now(),
                                         Ts = 1000000 * (1000000 * Mega + Secs) + Micro,
                                         TsEntries = [{Ts, E} || E <- Entries],
                                         gen_server:call(Pid,  {traces, TsEntries})
                                 end,
                                 Pid
                         end, self()}),
    dbg:p(all, call),
    [{ok, N} = dbg:n(N) || N <- Nodes],
    add_tracers(State#state.mfs),
    {reply, ok, State#state{trace=[], stop_tref = Tref}};
handle_call({traces, Entries}, _From, State) ->
    {reply, ok, State#state{trace=Entries ++ State#state.trace}};
handle_call(results, _From, State) ->
    case lists:sort(State#state.trace) of
        [] ->
            R = [];
        STrace ->
            {MinTs,_} = hd(STrace),
            R = zero_ts(MinTs, STrace, [])
    end,
    {reply, R, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(stop, State) ->
    dbg:stop_clear(),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
     {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

add_tracers([]) ->
    ok;
add_tracers([{M, F} | Rest]) ->
    dbg:tpl(M, F, [{'_',[],[{message,{return_trace}}]}]),
     add_tracers(Rest);
add_tracers([M | Rest]) ->
    dbg:tpl(M,[{'_',[],[{message,{return_trace}}]}]),
    add_tracers(Rest).

cancel_timer(undefined) ->
    ok;
cancel_timer(Tref) ->
    catch timer:cancel(Tref),
    receive
        stop ->
            ok
    after
        0 ->
            ok
    end.

zero_ts(_Offset, [], Acc) ->
    lists:reverse(Acc);
zero_ts(Offset, [{Ts,Trace}|Rest], Acc) ->
    zero_ts(Offset, Rest, [{Ts - Offset, Trace} | Acc]).

