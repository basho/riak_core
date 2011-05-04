%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc A set of sliding windows for recording N-per-second running stats.
%%
%% This keeps stats per second for the last minute.
%%
%% See git commit history for versions of this module which keep stats
%% for more than 1 minute.

-module(spiraltime).
-author('Justin Sheehy <justin@basho.com>').
-export([fresh/0,fresh/1,n/0,incr/2,incr/3,
         rep_second/1,rep_minute/1,
         test_spiraltime/0]).

%% @type moment() = integer().
%% This is a number of seconds, as produced by
%% calendar:datetime_to_gregorian_seconds(calendar:universal_time())

%% @type count() = integer().
%% The number of entries recorded in some time period.

-record(spiral, {moment :: integer(),
                 seconds :: [integer()],
                 seconds_len :: integer()
                }).

n() ->
    calendar:datetime_to_gregorian_seconds(calendar:universal_time()).

%% @doc Create an empty spiral with which to begin recording entries.
%% @spec fresh() -> spiral()
fresh() ->
    fresh(n()).

%% @doc Create an empty spiral with which to begin recording entries.
%% @spec fresh(moment()) -> spiral()
fresh(Moment) ->
    #spiral{moment=Moment,
            seconds=[0 || _ <- lists:seq(1,60)],
            seconds_len = 60
           }.

%% @doc Produce the number of entries recorded in the last second.
%% @spec rep_second(spiral()) -> {moment(), count()}
rep_second(Spiral) ->
    {Spiral#spiral.moment, hd(Spiral#spiral.seconds)}.

%% @doc Produce the number of entries recorded in the last minute.
%% @spec rep_minute(spiral()) -> {moment(), count()}
rep_minute(Spiral) ->
    {Minute,_} = lists:split(60,Spiral#spiral.seconds),
    {Spiral#spiral.moment, lists:sum(Minute)}.

%% @doc Add N to the counter of events, as recently as possible.
%% @spec incr(count(), spiral()) -> spiral()
incr(N, Spiral) -> incr(N,n(),Spiral).

%% @doc Add N to the counter of events occurring at Moment.
%% @spec incr(count(), moment(), spiral()) -> spiral()
incr(N, Moment, Spiral) when Spiral#spiral.moment =:= Moment ->
    % common case -- updates for "now"
    Spiral#spiral{seconds=bump(N, Spiral#spiral.seconds)};
incr(_N, Moment, Spiral) when Spiral#spiral.moment - Moment >= 60 ->
    Spiral; % updates more than a minute old are dropped! whee!
incr(N, Moment, Spiral) when Moment =< Spiral#spiral.moment ->
    % increment in recent past
    {Front,Back} = lists:split(Spiral#spiral.moment - Moment,
                               Spiral#spiral.seconds),
    Spiral#spiral{seconds=Front ++ bump(N, Back)};
incr(N, Moment, Spiral) when Moment - Spiral#spiral.moment > 60 ->
    % increment in far future - we can drop all the history
    Fresh = fresh(Moment),
    Fresh#spiral{seconds=bump(N, Fresh#spiral.seconds)};
incr(N, Moment, Spiral) ->
    % increment in near future
    Num = Moment - Spiral#spiral.moment,
    Trimmed = drop_tail(Num, Spiral),
    PaddedSeconds = push_zeros(Num, Trimmed#spiral.seconds),
    PaddedSecondsLength = Num + Trimmed#spiral.seconds_len,
    Trimmed#spiral{moment = Moment,
                   seconds = bump(N, PaddedSeconds),
                   seconds_len = PaddedSecondsLength}.

%% @doc Add N to item at the front of the list
%% @spec bump(integer(), [integer()]) -> [integer()]
bump(N, [H|T]) ->
    [H + N | T].

%% @doc Add N to item at the front of the list
%% @spec bump(integer(), [integer()]) -> [integer()]
push_zeros(0, Seconds) ->
    Seconds;
push_zeros(Num, Seconds) ->
    push_zeros(Num - 1, [0|Seconds]).

%% @doc drops trailing seconds if necessary
%% @spec drop_tail(integer(), spiral()) -> spiral()
drop_tail(Num, Spiral) when Num + Spiral#spiral.seconds_len =< 120 ->
    Spiral;
drop_tail(Num, Spiral) ->
    NewLength = 60 - Num,
    {Keep, _Past} = lists:split(0, NewLength),
    Spiral#spiral{seconds=Keep, seconds_len=NewLength}.

test_spiraltime() ->
    Start = n(),
    S0 = fresh(Start),

    S1 = incr(17, Start, S0),

    % test updates just now
    {Start, 17} = rep_second(S1),
    {Start, 17} = rep_minute(S1),

    % test updates within a minute from now
    lists:foreach(fun(N) ->
        PlusN = Start+N,
        S2a = incr(3, PlusN, S1),
        {PlusN, 3} = rep_second(S2a),
        {PlusN, 20} = rep_minute(S2a),
        S2b = incr(3, Start - N, S1),
        {Start, 17} = rep_second(S2b),
        {Start, 20} = rep_minute(S2b)
    end, lists:seq(1, 59)),

    % test updates more than a minute from now
    lists:foreach(fun(N) ->
        PlusN = Start+N,
        S2a = incr(3, PlusN, S1),
        {PlusN, 3} = rep_second(S2a),
        {PlusN, 3} = rep_minute(S2a),
        S2b = incr(3, Start - N, S1),
        {Start, 17} = rep_second(S2b),
        {Start, 17} = rep_minute(S2b),
        ok
    end, lists:seq(60, 200)),

    true.
