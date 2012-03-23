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

%% @doc A counter. Wraps an integer()
-module(riak_core_metric_counter).

-behaviour(riak_core_metric).

-export([new/0, value/2, value/3,  update/2]).

-export([increment/2, increment/3, decrement/2, decrement/3, total/2]).

%% @doc increments the counter for App named Stat
%% by Amount.
-spec increment(atom(), atom(), integer()) ->
                       ok.
increment(App, Stat, Amount) ->
    riak_core_metric_proc:update(App, Stat, Amount).

%% @doc increment the counter for App named Stat
%% by 1.
-spec increment(atom(), atom()) ->
                       ok.
increment(App, Stat) ->
    increment(App, Stat, 1).

%% @doc decrement the counter for App named Stat
%% by Amount.
-spec decrement(atom(), atom(), integer()) ->
                       ok.
decrement(App, Stat, Amount) ->
    riak_core_metric_proc:update(App, Stat, {decrement, Amount}).

%% @doc decrement the counter for App named Stat
%% by 1.
-spec decrement(atom(), atom()) -> ok.
decrement(App, Stat) ->
    decrement(App, Stat, 1).

%% @doc return the counter for App named Stat's
%% current value.
-spec total(atom(), atom()) -> non_neg_integer().
total(App, Stat) ->
    riak_core_metric_proc:value(App, Stat).

%% Behaviour
-spec new() -> 0.
new() ->
    0.

-spec value(atom(), non_neg_integer()) ->
                   {atom(), non_neg_integer()}.
value(Name, Counter) ->
    {Name, Counter}.

-spec value(_, atom(), non_neg_integer()) ->
                   {atom(), non_neg_integer()}.
value(_, Name, Counter) ->
    value(Name, Counter).

-spec update({atom(), integer()}, non_neg_integer()) ->
                    non_neg_integer();
            (integer(), non_neg_integer()) -> non_neg_integer().
update({decrement, Amount}, Counter) when is_integer(Amount) ->
    erlang:max(Counter - Amount, 0);
update(Amount, Counter) when is_integer(Amount) ->
    erlang:max(Counter + Amount, 0).
