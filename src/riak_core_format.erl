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

%% @doc Functions for formatting data.

-module(riak_core_format).
-export([fmt/2,
         human_size_fmt/2,
         human_time_fmt/2]).

%% @doc Created a string `Str' based on the format string `FmtStr' and
%%      list of args `Args'.
-spec fmt(string(), list()) -> Str::string().
fmt(FmtStr, Args) ->
    lists:flatten(io_lib:format(FmtStr, Args)).

%% @doc Create a human friendly string `Str' for number of bytes
%%      `Bytes' and format based on format string `Fmt'.
-spec human_size_fmt(string(), non_neg_integer()) -> Str::string().
human_size_fmt(Fmt, Bytes) ->
    Fmt2 = Fmt ++ " ~s",
    {Value, Units} = human_size(Bytes, ["B","KB","MB","GB","TB","PB"]),
    fmt(Fmt2, [Value, Units]).

%% @doc Create a human friendly string `Str' for the given time in
%%      microseconds `Micros'.  Format according to format string
%%      `Fmt'.
-spec human_time_fmt(string(), non_neg_integer()) -> Str::string().
human_time_fmt(Fmt, Micros) ->
    Fmt2 = Fmt ++ " ~s",
    {Value, Units} = human_time(Micros),
    fmt(Fmt2, [Value, Units]).

%%%===================================================================
%%% Private
%%%===================================================================

%% @private
%%
%% @doc Formats a byte size into a human-readable size with units.
%%      Thanks StackOverflow:
%%      http://stackoverflow.com/questions/2163691/simpler-way-to-format-bytesize-in-a-human-readable-way
-spec human_size(non_neg_integer(), list()) -> iolist().
human_size(S, [_|[_|_] = L]) when S >= 1024 -> human_size(S/1024, L);
human_size(S, [M|_]) ->
    {float(S), M}.

%% @private
%%
%% @doc Given a number of `Micros' returns a human friendly time
%%      duration in the form of `{Value, Units}'.
-spec human_time(non_neg_integer()) -> {Value::number(), Units::string()}.
human_time(Micros) ->
    human_time(Micros, {1000, "us"}, [{1000, "ms"}, {1000, "s"}, {60, "min"}, {60, "hr"}, {24, "d"}]).

-spec human_time(non_neg_integer(), {pos_integer(), string()},
                 [{pos_integer(), string()}]) ->
                        {number(), string()}.
human_time(T, {Divisor, Unit}, Units) when T < Divisor orelse Units == [] ->
    {float(T), Unit};
human_time(T, {Divisor, _}, [Next|Units]) ->
    human_time(T / Divisor, Next, Units).
