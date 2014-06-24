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

%% @doc fetches stats for registered modules and stores them
%% in an ets backed cache.
%% Only ever allows one process at a time to calculate stats.
%% Will always serve the stats that are in the cache.
%% Adds a stat `{stat_mod_ts, timestamp()}' to the stats returned
%% from `get_stats/1' which is the time those stats were calculated.

-module(riak_core_stat_cache).

-behaviour(gen_server).

%% API
-export([start_link/0, get_stats/1, register_app/2, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type registered_app() :: MFA::mfa().

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).
-define(ENOTREG(App), {error, {not_registered, App}}).
-define(DEFAULT_REG(Mod), {Mod, produce_stats, []}).

-record(state, {active=orddict:new(), apps=orddict:new()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register_app(App, {M, F, A}) ->
    gen_server:call(?SERVER, {register, App, {M, F, A}}, infinity).

get_stats(App) ->
    case gen_server:call(?SERVER, {get_stats_mfa, App}) of
	{ok, MFA} ->
	    do_get_stats(App, MFA);
	Error ->
	    Error
    end.

stop() ->
    gen_server:cast(?SERVER, stop).

%%% gen server

init([]) ->
    process_flag(trap_exit, true),
    %% re-register mods, if this is a restart after a crash
    RegisteredMods = lists:foldl(fun({App, Mod}, Registered) ->
                                         register_mod(App, ?DEFAULT_REG(Mod), Registered) end,
                                 orddict:new(),
                                 riak_core:stat_mods()),
    {ok, #state{apps=orddict:from_list(RegisteredMods)}}.

handle_call({register, App, MFA}, _From, State0=#state{apps=Apps0}) ->
    Apps = case registered(App, Apps0) of
               false ->
                   register_mod(App, MFA, Apps0);
               {true, _} ->
                   Apps0
           end,
    {reply, ok, State0#state{apps=Apps}};
handle_call({get_stats_mfa, App}, _From, State0=#state{apps=Apps}) ->
    case registered(App, Apps) of
	false ->
	    {reply, ?ENOTREG(App), State0};
	{true, MFA} ->
	    {reply, {ok, MFA}, State0}
    end;
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

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

-spec register_mod(atom(), registered_app(), orddict:orddict()) -> orddict:orddict().
register_mod(App, {Mod, _, _} = MFA, Apps0) ->
    P = riak_core_stat:prefix(),
    exometer:new([P, ?MODULE, Mod], histogram),
    exometer:new([P, ?MODULE, App], meter),
    orddict:store(App, MFA, Apps0).

registered(App, Apps) ->
    registered(orddict:find(App, Apps)).

registered(error) ->
    false;
registered({ok, Val}) ->
    {true, Val}.

do_get_stats(App, {M, F, A}) ->
    P = riak_core_stat:prefix(),
    Stats = histogram_timed_update([P, ?MODULE, M], M, F, A),
    exometer:update([P, ?MODULE, App], 1),
    Stats.

histogram_timed_update(Name, M, F, A) ->
    {Time, Value} = timer:tc(M, F, A),
    exometer:update(Name, Time),
    Value.

-ifdef(TEST).

-define(MOCKS, [folsom_utils, riak_core_stat, riak_kv_stat]).
-define(STATS, [{stat1, 0}, {stat2, 1}, {stat3, 2}]).

register() ->
    [meck:expect(M, produce_stats, fun() -> ?STATS end)
     || M <- [riak_core_stat, riak_kv_stat]],
    Now = tick(1000, 0),
    riak_core_stat_cache:register_app(riak_core, {riak_core_stat, produce_stats, []}, 5),
    riak_core_stat_cache:register_app(riak_kv, {riak_kv_stat, produce_stats, []}, 5),
    NonSuch = riak_core_stat_cache:get_stats(nonsuch),

    ?assertEqual(?ENOTREG(nonsuch), NonSuch),

    %% and that a meter and histogram has been registered for all registered modules
    [?assertEqual([{{?MODULE, M}, [{type, histogram}]}], folsom_metrics:get_metric_info({?MODULE, M}))
        || M <- [riak_core_stat, riak_kv_stat]],
    [?assertEqual([{{?MODULE, App}, [{type, meter}]}], folsom_metrics:get_metric_info({?MODULE, App}))
     || App <- [riak_core, riak_kv]].

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
    %% But ONLY in the case that the cache is empty. At any other time,
    %% that cached answer should be returned.
    Procs = 20,
    Then = 1000,
    Now = tick(2000, 0),
    meck:expect(riak_kv_stat, produce_stats, fun() -> register(blocked, self()), receive release -> ?STATS  end end),
    Coordinator = self(),
    Collector  = spawn_link(fun() -> collect_results(Coordinator, [], Procs) end),
    Pids = [spawn_link(fun() -> Stats = riak_core_stat_cache:get_stats(riak_kv), Collector ! {res, Stats} end) || _ <- lists:seq(1, Procs)],
    ?assertEqual({ok, cached(riak_core, Then), Then}, riak_core_stat_cache:get_stats(riak_core)),
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
    [?assertEqual({ok, cached(riak_kv, Now), Now}, Res) || Res <- Results],
    ?assertEqual(2, meck:num_calls(riak_kv_stat, produce_stats, [])).

crasher() ->
    Pid = whereis(riak_core_stat_cache),
    Then = tick(1000, 0),
    %% Now = tick(10000, 0),
    meck:expect(riak_core_stat, produce_stats, fun() ->
                                                       ?STATS end),
    meck:expect(riak_kv_stat, produce_stats, fun() -> erlang:error(boom)  end),
    ?assertMatch({error, {boom, _Stack}}, riak_core_stat_cache:get_stats(riak_kv)),
    ?assertEqual(Pid, whereis(riak_core_stat_cache)),
    ?assertEqual({ok, cached(riak_core, Then), Then}, riak_core_stat_cache:get_stats(riak_core)).

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
