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

%% @doc A Meter. Wraps spiraltime
-module(riak_core_metric_meter).

-behaviour(riak_core_metric).

%% Behaviour API
-export([new/0, value/2, value/3, update/2]).

%% Usage API
-export([increment/3, increment/4, minute/2]).

increment(App, Stat, Amount, Moment) ->
    riak_core_metric_proc:update(App, Stat, {Amount, Moment}).

increment(App, Stat, Moment) ->
    increment(App, Stat, 1, Moment).

minute(App, Stat) ->
    riak_core_metric_proc:value(App, Stat).

new() ->
    spiraltime:fresh().

value(Name, Meter) ->
    {_Moment, Count} = spiraltime:rep_minute(Meter),
    {Name, Count}.

value(_Spec, Name, Meter) ->
    value(Name, Meter).

update({Amount, Moment}, Meter) ->
    spiraltime:incr(Amount, Moment, Meter).
