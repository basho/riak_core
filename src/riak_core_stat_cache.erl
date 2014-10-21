%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-export([start_link/0,
	 stop/0]).

-export([register_app/2,
	 get_stats/1]).

-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3]).

-record(st, {apps = []}).

register_app(App, ProduceMFA) ->
    gen_server:call(?MODULE, {register, App, ProduceMFA}),
    ok.

%% This function should not be called by code that's been adapted to
%% exometer. Thus, it must return the old format, and assume that the
%%
get_stats(App) ->
    T1 = os:timestamp(),
    Result = case get_app(App) of
		 {_, {M, F, A}} ->
		     apply(M, F, A);
		 false ->
		     []
	     end,
    T2 = os:timestamp(),
    {ok, Result, timer:now_diff(T2, T1)}.

%% The gen_server mainly exists as a stub, since legacy test suites expect
%% riak_core_stat_cache:start_link() to return {ok, Pid}. It also keeps track
%% of the produce_stats() callbacks for each registered app.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    ok.

init(_) -> {ok, #st{}}.

handle_call({register, App, MFA}, _, #st{apps = Apps} = S) ->
    {reply, ok, S#st{apps = lists:keystore(App, 1, Apps, {App, MFA})}};
handle_call({get_app, App}, _, #st{apps = Apps} = S) ->
    {reply, lists:keyfind(App, 1, Apps), S};
handle_call(_, _, S) ->
    {reply, {error,not_supported}, S}.

handle_cast(_, S) -> {noreply, S}.
handle_info(_, S) -> {noreply, S}.
terminate(_, _) -> ok.
code_change(_, S, _) -> {ok, S}.

%% -- end gen_server

get_app(App) ->
    gen_server:call(?MODULE, {get_app, App}).
