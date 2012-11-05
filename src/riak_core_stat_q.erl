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

-module(riak_core_stat_q).

-compile(export_all).

-type path() :: [atom()].
-type stats() :: [stat()].
-type stat() :: {stat_name(), stat_value()}.
-type stat_name() :: tuple().
-type stat_value() :: integer() | [tuple()].

-spec get_stats(path()) -> stats().
get_stats(Path) ->
    %% get all the stats that are at Path
    NamesNTypes = names_and_types(Path),
    calculate_stats(NamesNTypes).

names_and_types(Path) ->
    Guards = guards_from_path(Path),
    ets:select(folsom, [{{'$1','$2'}, Guards,['$_']}]).

guards_from_path(Path) ->
    SizeGuard = size_guard(length(Path)),
    %% Going to reverse it is why this way around
    Guards = [SizeGuard, {is_tuple, '$1'}],
    add_guards(Path, Guards, 1).

add_guards([], Guards, _Cnt) ->
    lists:reverse(Guards);
add_guards(['_'|Path], Guards, Cnt) ->
    add_guards(Path, Guards, Cnt+1);
add_guards([Elem|Path], Guards, Cnt) ->
   add_guards(Path, [guard(Elem, Cnt) | Guards], Cnt+1).

guard(Elem, Cnt) when is_atom(Elem), Cnt > 0 ->
    {'==', {element, Cnt, '$1'}, Elem}.

-spec size_guard(pos_integer()) -> tuple().
size_guard(N) ->
    {'>=', {size, '$1'}, N}.

calculate_stats(NamesAndTypes) ->
    [{Name, get_stat(Stat)} || {Name, _Type}=Stat <- NamesAndTypes].

%% Create/use a cache/calculation process
get_stat(Stat) ->
    Pid = riak_core_stat_calc_sup:calc_proc(Stat),
    riak_core_stat_calc_proc:value(Pid).

%% BAD uses internl knowledge of folsom metrics record
calc_stat({Name, {metric, _Tags, gauge, _HistLen}}) ->
    GuageVal = folsom_metrics:get_metric_value(Name),
    calc_guage(GuageVal);
calc_stat({Name, {metric, _Tags, histogram, _HistLen}}) ->
    folsom_metrics:get_histogram_statistics(Name);
calc_stat({Name, {metric, _Tags, _Type, _HistLen}}) ->
    folsom_metrics:get_metric_value(Name).

%% some crazy people put funs in folsom gauges
%% so that they can have a consistent interface
%% to access stats from disperate sources
calc_guage(Val) when is_function(Val) ->
    Val();
calc_guage(Val) ->
    Val.
