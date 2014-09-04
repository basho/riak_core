%% -------------------------------------------------------------------
%% riak_core_stat_calc_proc: a process that caches the value of a calculated
%%  folsom stat. Purpose is it add more fine grained caching to stat calculation
%%  and not to use a single process for stat calculation.
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

%% riak_core_stat_calc_proc: a cache and concurrency control.
%%        there is no point in calculating the value of a stat if it
%%        is already being done. Some histogram stats with many readings can
%%        take seconds to calculate. This process caches a value for N (default 5)
%%        seconds, and also spawns a process to calculate the stat's value
%%        when needed. Any request for the value will either get the cached
%%        value, spawn a process to calculate, if none is running, or wait for
%%        the last spawned process to return.
%%        The idea / execution is as per riak_core_stat_cache, except more
%%        fine grained. This will replace that in future releases.

-module(riak_core_stat_calc_proc).

-behaviour(gen_server).

%% API
-export([start_link/1, value/1,
         stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @doc Cache item time to live in seconds
-define(TTL, 5).

-record(state, {stat, %% the stat name this proc manages
                value, %% the current value (if calculated) of this stat
                timestamp, %% the time the value was calculated
                ttl,    %% How long, in seconds, to cache the stat
                active, %% Pid of a process that is calculating the stat value
                awaiting=[] %% list of processes waiting for a result
               }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Stat) ->
    gen_server:start_link(?MODULE, [Stat], []).

value(Pid) ->
    gen_server:call(Pid, value, infinity).

stop(Pid) ->
    gen_server:cast(Pid, stop).

%%% gen server

init([Stat]) ->
    process_flag(trap_exit, true),
    TTL = app_helper:get_env(riak_core, stat_cache_ttl, ?TTL),
    {ok, #state{stat=Stat, ttl=TTL}}.

handle_call(value, From, State0=#state{active=Active0, awaiting=Awaiting0,
                                       stat=Stat, ttl=TTL, timestamp=TS, value=Value}) ->
    Reply = case cache_get(TS, TTL) of
                No when No == miss; No == stale ->
                    {Active, Awaiting} = maybe_get_stat(Stat, From, Active0, Awaiting0 ),
                    {noreply, State0#state{active=Active, awaiting=Awaiting}, 
                     timer:seconds(10)};
                hit ->
                    {reply, Value, State0}
            end,
    Reply;
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({value, Value, TS}, State=#state{awaiting=Awaiting, 
                                             value=OldValue}) ->
    case Value of
        {error, Reason} ->
            lager:debug("stat calc failed: ~p ~p", [Reason]),
            Reply = maybe_tag_stale(OldValue),
            _ = [gen_server:reply(From, Reply) || From <- Awaiting],
            %% update the timestamp so as not to flood the failing 
            %% process with update requests
            {noreply, State#state{timestamp=TS, active=undefined, 
                                  awaiting=[], value = Reply}};
        _Else ->
            _ = [gen_server:reply(From, Value) || From <- Awaiting],
            {noreply, State#state{value=Value, timestamp=TS, 
                                  active=undefined, awaiting=[]}}
    end;
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%% don't let a crashing stat calc crash the server
handle_info({'EXIT', FromPid, Reason}, State=#state{active=FromPid, awaiting=Awaiting}) when Reason /= normal ->
    _ = [gen_server:reply(From, {error, Reason}) || From <- Awaiting],
    {noreply, State#state{active=undefined, awaiting=[]}};
handle_info({'EXIT', _FromPid, Reason}, State=#state{active=undefined, 
                                                    awaiting=[]}) 
  when Reason /= normal ->
    %% catch intentional kills due to timeout here.
    {noreply, State};
handle_info(timeout, State=#state{active=Pid, awaiting=Awaiting, value=Value}) ->
    %% kill the pid, causing the above clause to be processed
    lager:debug("killed delinquent stats process ~p", [Pid]),
    exit(Pid, kill),
    %% let the cache get staler, tag so people can detect
    _ = [gen_server:reply(From, maybe_tag_stale(Value)) || From <- Awaiting],
    {noreply, State#state{active=undefined, awaiting=[]}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal
cache_get(undefined, _TTL) ->
    miss;
cache_get(TS, TTL) ->
    check_freshness(TS, TTL).

check_freshness(TStamp, TTL) ->
    case (TStamp + TTL) > folsom_utils:now_epoch() of
        true ->
             hit;
        false ->
            stale
    end.

maybe_get_stat(Stat, From, undefined, Awaiting) ->
    %% if a process for  getting stat value is not underway start one
    Pid = do_calc_stat(Stat),
    maybe_get_stat(Stat, From, Pid, Awaiting);
maybe_get_stat(_Stat, From, Pid, Awaiting) ->
    {Pid, [From|Awaiting]}.

do_calc_stat(Stat) ->
    ServerPid = self(),
    spawn_link(
      fun() ->
              StatVal = riak_core_stat_q:calc_stat(Stat),
              gen_server:cast(ServerPid, {value, StatVal, folsom_utils:now_epoch()}) end
     ).

maybe_tag_stale(Value) ->
    case Value of
        <<"stale:",_/binary>> ->
            Value;
        _ ->
            V = io_lib:format("stale: ~p", [Value]),
            iolist_to_binary(V)
    end.

