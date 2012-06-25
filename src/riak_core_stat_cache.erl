%% -------------------------------------------------------------------
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

-module(riak_core_stat_cache).

-behaviour(gen_server).

-author('Russell Brown <russelldb@basho.com>').

%% API
-export([start_link/0, get_stats/1, register_app/2, register_app/3,
        stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).
%% @doc Cache item time to live in seconds
-define(TTL, 5).
-define(ENOTREG(App), {error, {not_registered, App}}).

-record(state, {tab, active=orddict:new(), apps=orddict:new()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register_app(App, {M, F, A}) ->
    TTL = app_helper:get_env(riak_core, stat_cache_ttl, ?TTL),
    register_app(App, {M, F, A}, TTL).

register_app(App, {M, F, A}, TTL) ->
    gen_server:call(?SERVER, {register, App, {M, F, A}, TTL}).

get_stats(App) ->
    gen_server:call(?SERVER, {get_stats, App}, infinity).

stop() ->
    gen_server:cast(?SERVER, stop).

%%% gen server

init([]) ->
    Tab = ets:new(?MODULE, [protected, set, named_table]),
    {ok, #state{tab=Tab}}.

handle_call({register, App, {Mod, Fun, Args}, TTL}, _From, State0=#state{apps=Apps0}) ->
    Apps = case registered(App, Apps0) of
               false ->
                   folsom_metrics:new_histogram({?MODULE, Mod}),
                   folsom_metrics:new_meter({?MODULE, App}),
                   orddict:store(App, {Mod, Fun, Args, TTL}, Apps0);
               {true, _} ->
                   Apps0
           end,
    {reply, ok, State0#state{apps=Apps}};
handle_call({get_stats, App}, From, State0=#state{apps=Apps, active=Active0, tab=Tab}) ->
    Reply = case registered(App, Apps) of
                false ->
                    {reply, ?ENOTREG(App), State0};
                {true, {M, F, A, TTL}} ->
                    case cache_get(App, Tab, TTL) of
                        No when No == miss; No == stale ->
                            Active = maybe_get_stats(App, From, Active0, {M, F, A}),
                            {noreply, State0#state{active=Active}};
                        {hit, Stats, TS} ->
                            {reply, {ok, Stats, TS}, State0}
                    end
            end,
    Reply;
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({stats, App, Stats, TS}, State0=#state{tab=Tab, active=Active}) ->
    ets:insert(Tab, {App, TS, Stats}),
    State = case orddict:find(App, Active) of
                {ok, Awaiting} ->
                    [gen_server:reply(From, {ok, Stats, TS}) || From <- Awaiting],
                    State0#state{active=orddict:erase(App, Active)};
                error ->
                    State0
            end,
    {noreply, State};
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal
registered(App, Apps) ->
    registered(orddict:find(App, Apps)).

registered(error) ->
    false;
registered({ok, Val}) ->
    {true, Val}.

cache_get(App, Tab, TTL) ->
    Res = case ets:lookup(Tab, App) of
              [] ->
                  miss;
              [Hit] ->
                  check_freshness(Hit, TTL)
          end,
    Res.

check_freshness({_App, TStamp, Stats}, TTL) ->
    case (TStamp + TTL) > folsom_utils:now_epoch() of
        true ->
             {hit, Stats, TStamp};
        false ->
            stale
    end.

maybe_get_stats(App, From, Active, {M, F, A}) ->
    %% if a get stats is not under way start one
    Awaiting = case orddict:find(App, Active) of
                   error ->
                       do_get_stats(App, {M, F, A}),
                       [From];
                   {ok, Froms} ->
                       [From|Froms]
               end,
    orddict:store(App, Awaiting, Active).

do_get_stats(App, {M, F, A}) ->
    spawn_link(fun() ->
                       Stats = folsom_metrics:histogram_timed_update({?MODULE, M}, M, F, A),
                       folsom_metrics:notify_existing_metric({?MODULE, App}, 1, meter),
                       gen_server:cast(?MODULE, {stats, App, Stats, folsom_utils:now_epoch()}) end).

-ifdef(TEST).

-define(MOCKS, [folsom_utils, riak_core_stat, riak_kv_stat]).
-define(STATS, [{stat1, 0, stat2, 1, stat3, 2}]).

cache_test_() ->
    {setup,
     fun() ->
             folsom:start(),
             [meck:new(Mock, [passthrough]) || Mock <- ?MOCKS],
             riak_core_stat_cache:start_link()
     end,
     fun(_) ->
             folsom:stop(),
             [meck:unload(Mock) || Mock <- ?MOCKS],
             riak_core_stat_cache:stop()
     end,

     [{"Register with the cache",
      fun register/0},
      {"Get cached value",
       fun get_cached/0},
      {"Expired cache, re-calculate",
       fun get_expired/0},
      {"Only a single process can calculate stats",
       fun serialize_calls/0}
     ]}.

register() ->
    [meck:expect(M, produce_stats, fun() -> ?STATS end)
     || M <- [riak_core_stat, riak_kv_stat]],
    Now = tick(1000, 0),
    riak_core_stat_cache:register_app(riak_core, {riak_core_stat, produce_stats, []}, 5),
    riak_core_stat_cache:register_app(riak_kv, {riak_kv_stat, produce_stats, []}, 5),
    NonSuch = riak_core_stat_cache:get_stats(nonsuch),
    ?assertEqual({ok, ?STATS, Now}, riak_core_stat_cache:get_stats(riak_core)),
    ?assertEqual({ok, ?STATS, Now}, riak_core_stat_cache:get_stats(riak_kv)),
    ?assertEqual(?ENOTREG(nonsuch), NonSuch),
    %% and check the cache has the correct values
    [?assertEqual([{App, Now, ?STATS}], ets:lookup(riak_core_stat_cache, App))
     || App <- [riak_core, riak_kv]],
    %% and that a meter and histogram has been registered for all registered modules
    [?assertEqual([{{?MODULE, M}, [{type, histogram}]}], folsom_metrics:get_metric_info({?MODULE, M}))
        || M <- [riak_core_stat, riak_kv_stat]],
    [?assertEqual([{{?MODULE, App}, [{type, meter}]}], folsom_metrics:get_metric_info({?MODULE, App}))
     || App <- [riak_core, riak_kv]].

get_cached() ->
    Now = tick(1000, 0),
    [?assertEqual({ok, ?STATS, Now}, riak_core_stat_cache:get_stats(riak_core))
     || _ <- lists:seq(1, 20)],
    ?assertEqual(1, meck:num_calls(riak_core_stat, produce_stats, [])).

get_expired() ->
    Now = tick(1000, ?TTL+?TTL),
    [?assertEqual({ok, ?STATS, Now}, riak_core_stat_cache:get_stats(riak_core))
     || _ <- lists:seq(1, 20)],
    ?assertEqual(2, meck:num_calls(riak_core_stat, produce_stats, [])).

serialize_calls() ->
    %% many processes can call get stats at once
    %% they should not block the server
    %% but only one call to calculate stats should result
    %% the calling processes should block until they get a response
    %% call get_stats for kv from many processes at the same time
    %% check that they are blocked
    %% call get stats for core to show the server is not blocked
    %% return from the kv call and show a) all have same result
    %% b) only one call to produce_stats
    Procs = 20,
    Now = tick(2000, 0),
    meck:expect(riak_kv_stat, produce_stats, fun() -> register(blocked, self()), receive release -> ?STATS  end end),
    Coordinator = self(),
    Collector  = spawn_link(fun() -> collect_results(Coordinator, [], Procs) end),
    Pids = [spawn_link(fun() -> Stats = riak_core_stat_cache:get_stats(riak_kv), Collector ! {res, Stats} end) || _ <- lists:seq(1, Procs)],
    ?assertEqual({ok, ?STATS, Now}, riak_core_stat_cache:get_stats(riak_core)),
    [?assertEqual({status, waiting}, process_info(Pid, status)) || Pid <- Pids],

    timer:sleep(100), %% time for register

    blocked ! release,

    Results = receive
                  R -> R
              after
                  1000 ->
                        ?assert(false)
              end,

    [?assertEqual(undefined, process_info(Pid)) || Pid <- Pids],
    ?assertEqual(Procs, length(Results)),
    [?assertEqual({ok, ?STATS, Now}, Res) || Res <- Results],
    ?assertEqual(2, meck:num_calls(riak_kv_stat, produce_stats, [])).

tick(Moment, IncrBy) ->
    meck:expect(folsom_utils, now_epoch, fun() -> Moment + IncrBy end),
    Moment+IncrBy.

collect_results(Pid, Results, 0) ->
    Pid ! Results;
collect_results(Pid, Results, Procs) ->
    receive
        {res, Stats} ->
            collect_results(Pid, [Stats|Results], Procs-1)
    end.

-endif.
