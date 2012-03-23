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

%% @doc update the count for the App Stat by Amount for the
%% given moment.
-spec increment(atom(), atom(), integer(), integer()) -> ok.
increment(App, Stat, Amount, Moment) ->
    riak_core_metric_proc:update(App, Stat, {Amount, Moment}).

%% @doc update the count for the App Stat by 1 for the
%% given moment.
-spec increment(atom(), atom(), integer()) -> ok.
increment(App, Stat, Moment) ->
    increment(App, Stat, 1, Moment).

%% @doc display the number of entries in the last minute
%% for App Stat.
-spec minute(atom(), atom()) -> {atom(), integer()}.
minute(App, Stat) ->
    riak_core_metric_proc:value(App, Stat).

%% @doc create a new meter.
-spec new() -> spiraltime:spiral().
new() ->
    spiraltime:fresh().

%% @doc format the number of entries in the last minute as
%% {name, count}.
-spec value(atom(), spiraltime:spiral()) ->
                   {atom(), integer()}.
value(Name, Meter) ->
    {_Moment, Count} = spiraltime:rep_minute(Meter),
    {Name, Count}.

%% @doc format the number of entries in the last minute as
%% {name, count}.
-spec value(_, atom(), spiraltime:spiral()) ->
                   {atom(), integer()}.
value(_Spec, Name, Meter) ->
    value(Name, Meter).

%% @doc update the entry for the given Momen by Amout,
%%  in the given Meter
-spec update({integer(), integer()}, spiraltime:spiral()) ->
                    spiraltime:spiral().
update({Amount, Moment}, Meter) ->
    spiraltime:incr(Amount, Moment, Meter).
