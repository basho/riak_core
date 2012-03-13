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

%% @doc An histogram . Wraps slide
-module(riak_core_metric_histogram).

-behaviour(riak_core_metric).

-export([new/0, value/2, value/3,  update/2]).

-export([increment/4, minute/7, sum/2]).

increment(App, Stat, Reading, Moment) ->
    riak_core_metric_proc:update(App, Stat, {Reading, Moment}).

minute(App, Stat, Moment, Min, Max, Bins, RoundingMode) ->
    riak_core_metric_proc:value(App, Stat, {Moment, Min, Max, Bins, RoundingMode}).

sum(App, Stat) ->
    riak_core_metric_proc:value(App, Stat).
    
%% Behaviour
new() ->
    slide:fresh().

value(Name, Slide) ->
    {Name, slide:sum(Slide)}.

value({Moment, Min, Max, Bins, RoundingMode}, Name,  Slide) ->
    {Name, slide:mean_and_nines(Slide, Moment, Min, Max, Bins, RoundingMode)}.

update({Reading, Moment}, Slide) ->
    slide:update(Slide, Reading, Moment).
