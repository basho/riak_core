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

%% @doc An histogram . Wraps slide.
-module(riak_core_metric_histogram).

-behaviour(riak_core_metric).

-export([new/0, value/2, value/3,  update/2]).

-export([increment/4]).

-export_type([display_spec/0]).

-type display()       :: [{field(), integer()}].
-type field()         :: count | mean | median | '95' | '99' | '100'.
-type display_spec()  :: [ args() | fields() | prefix() ].
-type args()          :: {args, {Min::integer(), Max::integer(), Bins::integer(),
                          RoundingMode:: up | down}}.
-type fields()        :: {fields, [fields()]}.
-type prefix()        :: {prefix, atom() | string() | binary() | integer()}.

%% @doc update histogram for App Stat with Reading for the given
%%      Moment.
-spec increment(atom(), atom(), integer(), integer()) -> ok.
increment(App, Stat, Reading, Moment) ->
    riak_core_metric_proc:update(App, Stat, {Reading, Moment}).

%% Behaviour
%% @doc a new, fresh histogram
-spec new() -> slide:slide().
new() ->
    slide:fresh().

%% @doc Sum of readings from now to 'window size' seconds ago.
%%      Returns total number of readings and the sum of those
%%      readings.
-spec value(atom(), slide:slide()) ->
                   {atom(), {non_neg_integer(), number()}}.
value(Name, Slide) ->
    {Name, slide:sum(Slide)}.

%% @doc returns the fields of the histogram defined in the
%%      display spec. Use the 'args' in the display spec
%%      to produce results.
%% @see slide:mean_and_nines/6
-spec value(display_spec(), atom(), slide:slide()) ->
                   display().
value(DisplaySpec, Name,  Slide) ->
    {Min, Max, Bins, RoundingMode} = proplists:get_value(args, DisplaySpec),
    Fields = proplists:get_value(fields, DisplaySpec),
    Prefix = proplists:get_value(prefix, DisplaySpec),
    Res = slide:mean_and_nines(Slide, slide:moment(), Min, Max, Bins, RoundingMode),
    PL = to_proplist(Res),
    FieldPrefix = field_prefix(Prefix, Name),
    display(FieldPrefix, Fields, PL, []).

%% @doc update histogram with Reading for given Moment
-spec update({integer(), integer()}, slide:slide()) ->
                    slide:slide().
update({Reading, Moment}, Slide) ->
    slide:update(Slide, Reading, Moment).

%% Internal
%% @doc transform the out put of slide:mean_and_nines/6
%% to a proplist
-spec to_proplist({integer(), integer(),
                   {integer(), integer(), integer(), integer()}}) ->
                         display().
to_proplist({Cnt, Mean, {Median, NineFive, NineNine, Max}}) ->
    [{count, Cnt},
     {mean, Mean},
     {median, Median},
     {'95', NineFive},
     {'99', NineNine},
     {'100', Max}].

%% @doc add a prefix Prefix_ to the given Field
-spec field_prefix(atom(), field()) ->
                          atom().
field_prefix(undefined, Name) ->
    Name;
field_prefix(Prefix, Name) ->
    riak_core_metric:join_as_atom([Prefix, '_', Name]).

%% @doc produce a proplist containing only specified fields
-spec display(atom(), [field()], display(), display()) ->
                     display().
display(_Prefix, [], _Stat, Acc) ->
    lists:reverse(Acc);
display(Prefix, [Field|Rest], Stats, Acc) ->
    Name = riak_core_metric:join_as_atom([Prefix, '_',  Field]),
    Item = {Name, proplists:get_value(Field, Stats)},
    display(Prefix, Rest, Stats, [Item|Acc]).
