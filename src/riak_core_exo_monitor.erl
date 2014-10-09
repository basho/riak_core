%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
%%
%%   This Source Code Form is subject to the terms of the Mozilla Public
%%   License, v. 2.0. If a copy of the MPL was not distributed with this
%%   file, You can obtain one at http://mozilla.org/MPL/2.0/.
%%
%% -------------------------------------------------------------------
%%
%% @doc Legacy mapping module folsom metrics to exometer
%%
%% @end
-module(riak_core_exo_monitor).
-behaviour(exometer_folsom_monitor).
-behaviour(exometer_entry).

-export([copy_folsom/3]).
-export([behaviour/0,
	 delete/3,
	 get_datapoints/3,
	 get_value/4,
	 new/3,
	 reset/3,
	 sample/3,
	 setopts/3,
	 update/4]).

behaviour() ->
    entry.

copy_folsom(Name, Type, Opts) when is_tuple(Name) ->
    Prefix = riak_core_stat:prefix(),
    {[Prefix|tuple_to_list(Name)], ad_hoc, [{folsom_name, Name},
					    {module, ?MODULE},
					    {type, Type}
					    | options(Type, Opts)]};
copy_folsom(_, _, _) ->
    false.

new(N, _, Opts) ->
    {ok, {proplists:get_value(type, Opts, unknown),
	  proplists:get_value(folsom_name, Opts, N)}}.

update(_, Value, counter, {_, Name}) ->
    folsom_metrics:notify_existing_metric(Name, {inc,Value}, counter);
update(_, Value, Type, {_, Name}) ->
    folsom_metrics:notify_existing_metric(Name, Value, Type).

reset(_, _, _) ->
    {error, unsupported}.

get_value(_, Type, {_, Name}, DPs) ->
    exometer_folsom:get_value(Name, Type, [], DPs).

sample(_, _, _) ->
    {error, unsupported}.

setopts(_, _, _) ->
    ok.

delete(_, _, _) ->
    {error, unsupported}.

get_datapoints(Name, Type, _) ->
    exometer_folsom:get_datapoints(Name, Type, []).

options(history, [Size]) ->
    [{size, Size}];
options(histogram, [SampleType, SampleSize, Alpha]) ->
    [{sample_type, SampleType},
     {sample_size, SampleSize},
     {alpha, Alpha}];
options(duration    , [SampleType, SampleSize, Alpha]) ->
    [{sample_type, SampleType},
     {sample_size, SampleSize},
     {alpha, Alpha}];
options(meter_reader, []) -> [];
options(spiral      , []) -> [];
options(meter       , []) -> [];
options(gauge       , []) -> [];
options(counter     , []) -> [].
