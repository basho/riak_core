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

%% @doc tracks a duration, min, max, last, mean
-module(riak_core_metric_duration).

-behaviour(riak_core_metric).

%% Behaviour API
-export([new/0, value/2, value/3,  update/2]).

%% Public API
-export([start/2, stop/2, cumulative/2]).

-record(cuml, {count =  0 :: integer(),
               min        :: integer(),
               max   = -1 :: integer(),
               mean  =  0 :: integer(),
               last       :: integer(),
               start      :: calendar:t_now()}).

start(App, Stat) ->
    riak_core_metric_proc:update(App, Stat, start).

stop(App, Stat) ->
    riak_core_metric_proc:update(App, Stat, stop).

cumulative(App, Stat) ->
    riak_core_metric_proc:value(App, Stat).

new() ->
    #cuml{}.

value(Name, Dur) ->
    display(Name, to_proplist(Dur), [count, min, max, mean, last], []).

value(Fields, Name, Dur) ->
    display(Name, to_proplist(Dur), Fields, []).

update(start, Dur) ->
    Dur#cuml{start=erlang:now()};
update(stop, #cuml{count=N, min=Min, max=Max, mean=Mean, start=T0}) ->
    Duration = timer:now_diff(erlang:now(), T0),
    Min2 = erlang:min(Min, Duration),
    Max2 = erlang:max(Max, Duration),
    Mean2 = ((N * Mean) + Duration) div (N+1),
    #cuml{count=N+1, min=Min2, max=Max2, mean=Mean2, last=Duration, start=undefined}.

%% internal
display(_Stat, _Cuml, [], Acc) ->
    lists:reverse(Acc);
display(Stat, Cuml, [Field|Rest], Acc) ->
    Name = riak_core_metric:join_as_atom([Stat, '_', Field]),
    Value = proplists:get_value(Field, Cuml),
    display(Stat, Cuml, Rest, [{Name, Value}|Acc]).

to_proplist(Cuml) when is_record(Cuml, cuml) ->
    lists:zip(record_info(fields, cuml), tl(tuple_to_list(Cuml))).
