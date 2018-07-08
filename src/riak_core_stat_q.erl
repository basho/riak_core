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
%%      current value and return it.

-module(riak_core_stat_q).

-export([get_stats/1, calculate_stats/1, get_stat/1, calc_stat/1,
         stat_return/1, log_error/3, calc_gauge/1]).

-export_type([path/0,
              stat_name/0]).

-type path() :: [] | [atom()|binary()].
-type stats() :: [stat()].
-type stat() :: {stat_name(), stat_value()}.
-type stat_name() :: list().
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
    exometer:get_values(Path).
    %% %% get all the stats that are at Path
    %% calculate_stats(exometer:select(
    %%                     [{ {Path ++ '_','_',enabled}, [], ['$_'] }])).

calculate_stats(NamesAndTypes) ->
    [{Name, get_stat(Name)} || {Name, _, _} <- NamesAndTypes].

%% Create/lookup a cache/calculation process
get_stat(Stat) ->
    exometer:get_value(Stat).

%% Encapsulate getting a stat value from exometer.
%%
%% If for any reason we can't get a stats value
%% return 'unavailable'.
%% @TODO experience shows that once a stat is
%% broken it stays that way. Should we delete
%% stats that are broken?
calc_stat({Name, _Type}) when is_tuple(Name) ->
    stat_return(exometer:get_value([riak_core_stat:prefix()|tuple_to_list(Name)]));
calc_stat({[_|_] = Name, _Type}) ->
    stat_return(exometer:get_value([riak_core_stat:prefix()|Name])).

stat_return({error,not_found}) -> unavailable;
stat_return({ok, Value}) -> Value.

log_error(StatName, ErrClass, ErrReason) ->
    lager:warning("Failed to calculate stat ~p with ~p:~p", [StatName, ErrClass, ErrReason]).

%% some crazy people put funs in gauges (exometer has a 'function' metric)
%% so that they can have a consistent interface
%% to access stats from disperate sources
calc_gauge({function, Mod, Fun}) ->
    Mod:Fun();
calc_gauge(Val) ->
    Val.
