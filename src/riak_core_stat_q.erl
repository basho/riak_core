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

%% @doc riak_core_stat_q is an interface to query folsom stats
%%      To use, call `get_stats/1' with a query `Path'.
%%      A `Path' is a list of atoms | binaries. The module creates a set
%%      of `ets:select/1' guards, one for each element in `Path'
%%      For each stat that has a key that matches `Path' we calculate the
%%      current value and return it. This module makes use of
%%     `riak_core_stat_calc_proc'
%%      to cache and limit stat calculations.

-module(riak_core_stat_q).

-compile(export_all).

-export_type([path/0,
              stat_name/0]).

-type path() :: [] | [atom()|binary()].
-type stats() :: [stat()].
-type stat() :: {stat_name(), stat_value()}.
-type stat_name() :: tuple().
-type stat_value() :: integer() | [tuple()].

%% @doc To allow for namespacing, and adding richer dimensions, stats
%% are named with a tuple key. The key (like `{riak_kv, node, gets}' or
%% `{riak_kv, vnode, puts, time}') can
%% be seen as an hierarchical path. With `riak_kv' at the root and
%% the other elements as branches / leaves.
%% This module allows us to get only the stats at and below a particular key.
%% `Path' is a list of atoms or the empty list.
%% an example path might be `[riak_kv]' which will return every
%% stat that has `riak_kv' in the first element of its key tuple.
%% You may use the atom '_' at any point
%% in `Path' as a wild card.
-spec get_stats(path()) -> stats().
get_stats(Path) ->
    %% get all the stats that are at Path
    NamesNTypes = names_and_types(Path),
    calculate_stats(NamesNTypes).

%% @doc queries folsom's metrics table for stats that match our path
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

guard(Elem, Cnt) when Cnt > 0 ->
    {'==', {element, Cnt, '$1'}, Elem}.

-spec size_guard(pos_integer()) -> tuple().
size_guard(N) ->
    {'>=', {size, '$1'}, N}.

calculate_stats(NamesAndTypes) ->
    [{Name, get_stat({Name, Type})} || {Name, {metric, _, Type, _}} <- NamesAndTypes].

%% Create/lookup a cache/calculation process
get_stat(Stat) ->
    Pid = riak_core_stat_calc_sup:calc_proc(Stat),
    riak_core_stat_calc_proc:value(Pid).

throw_folsom_error({error, _, _} = Err) ->
    throw(Err);
throw_folsom_error(Other) -> Other.

%% Encapsulate getting a stat value from folsom.
%%
%% If for any reason we can't get a stats value
%% return 'unavailable'.
%% @TODO experience shows that once a stat is
%% broken it stays that way. Should we delete
%% stats that are broken?
calc_stat({Name, gauge}) ->
    try
        GaugeVal = throw_folsom_error(folsom_metrics:get_metric_value(Name)),
        calc_gauge(GaugeVal)
    catch ErrClass:ErrReason ->
            log_error(Name, ErrClass, ErrReason),
            unavailable
    end;
calc_stat({Name, histogram}) ->
    try
        throw_folsom_error(folsom_metrics:get_histogram_statistics(Name))
    catch ErrClass:ErrReason ->
            log_error(Name, ErrClass, ErrReason),
            unavailable
    end;
calc_stat({Name, _Type}) ->
    try throw_folsom_error(folsom_metrics:get_metric_value(Name))
    catch ErrClass:ErrReason ->
            log_error(Name, ErrClass, ErrReason),
            unavailable
    end.

log_error(StatName, ErrClass, ErrReason) ->
    lager:warning("Failed to calculate stat ~p with ~p:~p", [StatName, ErrClass, ErrReason]).

%% some crazy people put funs in folsom gauges
%% so that they can have a consistent interface
%% to access stats from disperate sources
calc_gauge({function, Mod, Fun}) ->
    Mod:Fun();
calc_gauge(Val) ->
    Val.
