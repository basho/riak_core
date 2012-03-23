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

-export_type([display_spec/0]).

-record(cuml, {count =  0 :: integer(),
               min        :: integer(),
               max   = -1 :: integer(),
               mean  =  0 :: integer(),
               last       :: integer(),
               start      :: calendar:t_now()}).

-type display() :: [{field(), integer()}].
-type field() :: count | min | max | mean | last | start.
-type display_spec() :: [field()].

%% @doc start timing the duration
-spec start(atom(), atom()) -> ok.
start(App, Stat) ->
    riak_core_metric_proc:update(App, Stat, start).

%% @doc stop timing the duration
-spec stop(atom(), atom()) -> ok.
stop(App, Stat) ->
    riak_core_metric_proc:update(App, Stat, stop).

%% @doc get display the current value of
%% the duration Stat. Value is
%% returned as a proplist
-spec cumulative(atom(), atom()) ->
                        display().
cumulative(App, Stat) ->
    riak_core_metric_proc:value(App, Stat).

-spec new() -> #cuml{}.
new() ->
    #cuml{}.

%% @doc format Dur as a proplist with
%% default fields count, min, max, mean and last
-spec value(atom(), #cuml{}) ->
                  display().
value(Name, Dur) ->
    display(Name, to_proplist(Dur), [count, min, max, mean, last], []).

%% @doc fromat Dur as a proplist
%% Fields is a list of values to display
%% picked from count, min, max, mean, last
-spec value(display_spec(), atom(), #cuml{}) ->
                   display().
value(Fields, Name, Dur) ->
    display(Name, to_proplist(Dur), Fields, []).

-spec update(start, #cuml{}) -> #cuml{};
            (stop, #cuml{}) -> #cuml{}.
update(start, Dur) ->
    Dur#cuml{start=erlang:now()};
update(stop, #cuml{count=N, min=Min, max=Max, mean=Mean, start=T0}) ->
    Duration = timer:now_diff(erlang:now(), T0),
    Min2 = erlang:min(Min, Duration),
    Max2 = erlang:max(Max, Duration),
    Mean2 = ((N * Mean) + Duration) div (N+1),
    #cuml{count=N+1, min=Min2, max=Max2, mean=Mean2, last=Duration, start=undefined}.

%% internal
-spec display(atom(), display(), [field()], display()) ->
                     display().
display(_Stat, _Cuml, [], Acc) ->
    lists:reverse(Acc);
display(Stat, Cuml, [Field|Rest], Acc) ->
    Name = riak_core_metric:join_as_atom([Stat, '_', Field]),
    Value = proplists:get_value(Field, Cuml),
    display(Stat, Cuml, Rest, [{Name, Value}|Acc]).

-spec to_proplist(#cuml{}) -> display().
to_proplist(Cuml) when is_record(Cuml, cuml) ->
    lists:zip(record_info(fields, cuml), tl(tuple_to_list(Cuml))).
