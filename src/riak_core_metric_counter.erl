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

%% @doc A counter. Wraps, erm, an integer()
-module(riak_core_metric_counter).

-behaviour(riak_core_metric).

-export([new/0, value/2, value/3,  update/2]).

-export([increment/2, increment/3, decrement/2, decrement/3, total/2]).

increment(App, Stat, Amount) ->
    riak_core_metric_proc:update(App, Stat, Amount).

increment(App, Stat) ->
    increment(App, Stat, 1).

decrement(App, Stat, Amount) ->
    riak_core_metric_proc:update(App, Stat, {decrement, Amount}).

decrement(App, Stat) ->
    decrement(App, Stat, 1).

total(App, Stat) ->
    riak_core_metric_proc:value(App, Stat).

%% Behaviour
new() ->
    0.

value(Name, Counter) ->
    {Name, Counter}.

value(_, Name, Counter) ->
    value(Name, Counter).

update({decrement, Amount}, 0) when is_integer(Amount) ->
    0;
update({decrement, Amount}, Counter) when is_integer(Amount) ->
    Counter - Amount;
update(Amount, Counter) when is_integer(Amount) ->
    Counter + Amount.
