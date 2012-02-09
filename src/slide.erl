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

%% @doc Keep track of thing in a sliding time window.  The idea here
%%      is that you have some reading to take several times.
%%      Occasionally, you want to compute some aggregation of those
%%      readings for the last N seconds.
%%
%%      For example, you might read the weight of cars passing a point
%%      in the road.  You want to compute some statistics every hour.
%%      You could:
%%
%%      %% create a new slide, with an hour window
%%      T0 = slide:fresh(60*60)
%%
%%      %% update it every time a car passes
%%      T1 = slide:update(T0, Weight, slide:moment())
%%
%%      %% eventually ask for stats
%%      {NumberOfCars, TotalWeight} = slide:sum(TN, slide:moment())
%%      {NumberOfCars, AverageWeight} = slide:mean(TN, slide:moment())
%%      {NumberOfCars, {MedianWeight,
%%                      NinetyFivePercentWeight,
%%                      NinetyNinePercentWeight,
%%                      HeaviestWeight} = slide:nines(TN, slide:moment())

-module(slide).

-export([fresh/0, fresh/1, fresh/2]).
-export([update/2, update/3, moment/0]).
-export([sum/1, sum/2, sum/3]).
-export([mean/1, mean/2, mean/3]).
-export([nines/1, nines/2, nines/3]).
-export([mean_and_nines/2, mean_and_nines/6]).
-export([private_dir/0, sync/1]).

-include_lib("kernel/include/file.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DIR, "/tmp/riak/slide-data"). % SLF TODO: need pkg-specific data dir handling
-define(REC_BYTES, 12).          % 4 + (size(term_to_binary(4000000000)) = 8)

-record(slide, {
          oldest,   %% oldest timestamp here
          window,   %% window to which to trim
          trigger,  %% age at which to trigger pruning
          dir,      %% directory for data
          readings_fh, %% filehandle for current moment's readings
          readings_m %% moment associated with readings_fh
         }).

%% @spec fresh() -> slide()
%% @equiv fresh(60)
fresh() -> fresh(60).

%% @spec fresh(integer()) -> slide()
%% @equiv fresh(Window, Window)
fresh(Window) -> fresh(Window, Window).

%% @spec fresh(integer(), integer()) -> slide()
%% @doc Create an empty slide for tracking Window-seconds worth of
%%      readings, and pruning those readings after Trigger seconds.
fresh(Window, Trigger) when Trigger >= Window ->
    {A,B,C} = now(),
    Dir = lists:flatten(io_lib:format("~s/~p.~p.~p", [private_dir(), A, B, C])),
    {ok, parent, Dir} = {filelib:ensure_dir(Dir), parent, Dir},
    {ok, Dir} = {file:make_dir(Dir), Dir},
    #slide{window=Window, trigger=Trigger, dir=Dir}.

%% @spec moment() -> integer()
%% @doc Get the current time in seconds.
moment() ->
    calendar:datetime_to_gregorian_seconds(calendar:local_time()).

%% @spec update(slide(), term()) -> slide()
%% @equiv update(S, Reading, moment())
update(S, Reading) -> update(S, Reading, moment()).

%% @spec update(slide(), term(), integer()) -> slide()
%% @doc Store a new reading.  The current list of readings will be
%%      pruned if Moment is as new as or newer than the most recent
%%      reading stored, and more than Trigger seconds newer than the
%%      oldest reading stored.
update(S0=#slide{oldest=Oldest,dir=Dir,readings_m=RdMoment,readings_fh=FH},
       Reading0, Moment) ->
    S1 = if Moment == RdMoment ->
                 S0;
            true ->
                 catch file:close(FH),
                 File = integer_to_list(Moment rem S0#slide.window),
                 {ok, FH2} = file:open(filename:join(Dir, File),
                                       file_write_options()),
                 S0#slide{readings_m = Moment,
                          readings_fh = FH2,
                          oldest = if Oldest == undefined ->
                                           Moment;
                                      true ->
                                           Oldest
                                   end}
         end,
    Reading = if Reading0 < 4000000000 -> Reading0;
                 true                  -> 4000000000
              end,
    %% 4 bytes len header + 8 bytes ...
    Bin = pad_bin(term_to_binary(Reading), 8),
    ok = file:write(S1#slide.readings_fh, [<<8:32>>, Bin]),
    S1.

%% @spec sum(slide()) -> {Count::integer(), Sum::integer()}
%% @doc Sum of readings from now through Window seconds ago.  Return is
%%      number of readings in the range and the sum of those readings.
sum(Slide) -> sum(Slide, moment()).

%% @spec sum(slide(), integer()) -> {Count::integer(), Sum::integer()}
%% @doc Sum of readings from Moment through Window seconds before Moment.
%%      Return is number of readings in the range and the sum of those
%%      readings.
sum(Slide, Moment) -> sum(Slide, Moment, Slide#slide.window).

%% @spec sum(slide(), integer(), integer()) ->
%%          {Count::integer(), Sum::integer()}
%% @doc Sum of readings from Moment through Seconds seconds before
%%      Moment.  Return is number of readings in the range and the sum
%%      of those readings.
sum(#slide{dir=Dir}, Moment, Seconds) ->
    Cutoff = Moment-Seconds,
    Names = filelib:wildcard("*", Dir),
    ToScan = [Name || Name <- Names, list_to_integer(Name) >= 0],
    Blobs = [element(2, file:read_file(filename:join(Dir, Name))) ||
                        Name <- ToScan],
    %% histo_experiment(Blobs),
    sum_blobs(Blobs, Moment, Cutoff).

private_dir() ->
    case application:get_env(riak_core, slide_private_dir) of
        undefined ->
            Root = case application:get_env(riak_core, platform_data_dir) of
                       undefined -> ?DIR;
                       {ok, X} -> filename:join([X, "slide-data"])
                   end,
            filename:join([Root, os:getpid()]);
        {ok, Dir} ->
            Dir
    end.

sync(_S) ->
    todo.

mean_and_nines(Slide, Moment) ->
    mean_and_nines(Slide, Moment, 0, 5000000, 20000, down).

mean_and_nines(#slide{dir=Dir, window = Window}, _Moment, HistMin, HistMax, HistBins, RoundingMode) ->
    Now = moment(),
    Names = filelib:wildcard("*", Dir),
    ModTime = fun(Name) ->
                      {ok, FI} = file:read_file_info(filename:join(Dir, Name)),
                      calendar:datetime_to_gregorian_seconds(FI#file_info.mtime)
              end,
    ToScan = [Name || Name <- Names,
                      Now - ModTime(Name) =< Window],
    Blobs = [element(2, file:read_file(filename:join(Dir, Name))) ||
                        Name <- ToScan],
    compute_quantiles(Blobs, HistMin, HistMax, HistBins, RoundingMode).

compute_quantiles(Blobs, HistMin, HistMax, HistBins, RoundingMode) ->
    {H, Count} = compute_quantiles(
                   Blobs, basho_stats_histogram:new(HistMin, HistMax, HistBins), 0),
    {_Min, Mean, Max, _Var, _SDev} = basho_stats_histogram:summary_stats(H),
    P50 = basho_stats_histogram:quantile(0.50, H),
    P95 = basho_stats_histogram:quantile(0.95, H),
    P99 = basho_stats_histogram:quantile(0.99, H),

    %% RoundingMode allows the caller to decide whether to round up or
    %% down to the nearest integer. This is useful in cases where we
    %% measure very small, but non-zero integer values where rounding
    %% down would give a zero rather than a one.

    %% The calls to erlang:min/N exist because the histogram estimates
    %% percentiles. Depending on the sample size or distribution, it
    %% is possible that the estimated percentile is larger than the
    %% max, which is foolish. If that happens, then we ignore the
    %% estimate and use the value of max instead.
    case RoundingMode of
        up ->
            RMax = my_ceil(Max),
            {Count, my_ceil(Mean), {
                              erlang:min(my_ceil(P50), RMax),
                              erlang:min(my_ceil(P95), RMax),
                              erlang:min(my_ceil(P99), RMax),
                              erlang:min(my_ceil(Max), RMax)
                            }};
        _ -> %% 'down'
            RMax = my_trunc(Max),
            {Count, my_trunc(Mean), {
                              erlang:min(my_trunc(P50), RMax),
                              erlang:min(my_trunc(P95), RMax),
                              erlang:min(my_trunc(P99), RMax),
                              erlang:min(my_trunc(Max), RMax)
                            }}
    end.

compute_quantiles([Blob|Blobs], H, Count) ->
    Ns = [binary_to_term(Bin) || <<_Hdr:32, Bin:8/binary>> <= Blob],
    H2 = basho_stats_histogram:update_all(Ns, H),
    compute_quantiles(Blobs, H2, Count + length(Ns));
compute_quantiles([], H, Count) ->
    {H, Count}.

my_trunc(X) when is_atom(X) ->
    0;
my_trunc(N) ->
    trunc(N).

my_ceil(X) when is_atom(X) ->
    0;
my_ceil(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T;
        Pos when Pos > 0 -> T + 1;
        _ -> T
    end.

%% @spec mean(slide()) -> {Count::integer(), Mean::number()}
%% @doc Mean of readings from now through Window seconds ago.  Return is
%%      number of readings in the range and the mean of those readings.
mean(Slide) -> mean(Slide, moment()).

%% @spec mean(slide(), integer()) -> {Count::integer(), Mean::number()}
%% @doc Mean of readings from Moment through Window seconds before Moment.
%%      Return is number of readings in the range and the mean of those
%%      readings.
mean(Slide, Moment) -> mean(Slide, Moment, Slide#slide.window).

%% @spec mean(slide(), integer(), integer()) ->
%%          {Count::integer(), Mean::number()}
%% @doc Mean of readings from Moment through Seconds seconds before
%%      Moment.  Return is number of readings in the range and the mean
%%      of those readings.
mean(S, Moment, Seconds) ->
    case sum(S, Moment, Seconds) of
        {0, _}       -> {0, undefined};
        {Count, Sum} -> {Count, Sum/Count}
    end.

%% @spec nines(slide()) ->
%%         {Count::integer(), {Median::number(), NinetyFive::number(),
%%                             NinetyNine::number(), Hundred::number()}}
%% @doc Median, 95%, 99%, and 100% readings from now through Window
%%  seconds ago.  Return is number of readings in the range and the
%%  nines of those readings.
nines(Slide) -> nines(Slide, moment()).

%% @spec nines(slide(), integer()) ->
%%         {Count::integer(), {Median::number(), NinetyFive::number(),
%%                             NinetyNine::number(), Hundred::number()}}
%% @doc Median, 95%, 99%, and 100% readings from Moment through Window
%%      seconds before Moment.  Return is number of readings in the
%%      range and the nines of those readings.
nines(Slide, Moment) -> nines(Slide, Moment, Slide#slide.window).

%% @spec nines(slide(), integer(), integer()) ->
%%         {Count::integer(), {Median::number(), NinetyFive::number(),
%%                             NinetyNine::number(), Hundred::number()}}
%% @doc Median, 95%, 99%, and 100% readings from Moment through
%%      Seconds seconds before Moment.  Return is number of readings
%%      in the range and the nines of those readings.
nines(#slide{dir=Dir}, Moment, Seconds) ->
    _Cutoff = Moment-Seconds,
    Names = filelib:wildcard("*", Dir),
    ToScan = [Name || Name <- Names, list_to_integer(Name) >= 0],
    OutFile = filename:join(Dir, "-42"),
    Opts = [], %%[{no_files, 64}],
    ok = file_sorter:sort([filename:join(Dir, Name) || Name <- ToScan],
                          OutFile, Opts),
    {ok, FI} = file:read_file_info(OutFile),
    case FI#file_info.size of
        0 ->
            {0, {undefined, undefined, undefined, undefined}};
        Size ->
            Count = (Size div ?REC_BYTES) - 1,
            {Count,
             {read_word_at(mochinum:int_ceil(Count*0.50) * ?REC_BYTES, OutFile),
              read_word_at(mochinum:int_ceil(Count*0.95) * ?REC_BYTES, OutFile),
              read_word_at(mochinum:int_ceil(Count*0.99) * ?REC_BYTES, OutFile),
              read_word_at(Count * ?REC_BYTES, OutFile)}}
    end.

read_word_at(Offset, File) ->
    {ok, FH} = file:open(File, [read, raw, binary]),
    {ok, Bin} = file:pread(FH, Offset + 4, ?REC_BYTES - 4), % 4 = header to skip
    binary_to_term(Bin).

%% Using accumulator func args avoids the garbage creation by
%% lists:foldl's need to create 2-tuples to manage accumulator.

sum_blobs(Blobs, Moment, Cutoff) ->
    sum_blobs2(Blobs, Moment, Cutoff, 0, 0).

sum_blobs2([], _Moment, _Cutoff, TCount, TSum) ->
    {TCount, TSum};
sum_blobs2([Blob|Blobs], Moment, Cutoff, TCount, TSum) ->
    {Count, Sum} = sum_ints(
                     [binary_to_term(Bin) || <<_Hdr:32, Bin:8/binary>> <= Blob],
                     0, 0),
    sum_blobs2(Blobs, Moment, Cutoff, TCount + Count, TSum + Sum).

%% Dunno if this is any faster/slower than lists:sum/1 + erlang:length/1.

sum_ints([I|Is], Count, Sum) ->
    sum_ints(Is, Count + 1, Sum + I);
sum_ints([], Count, Sum) ->
    {Count, Sum}.

pad_bin(Bin, Size) when size(Bin) == Size ->
    Bin;
pad_bin(Bin, Size) ->
    Bits = (Size - size(Bin)) * 8,
    <<Bin/binary, 0:Bits>>.

%%
%% Test
%%

setup_eunit_proc_dict() ->
    erlang:put({?MODULE, eunit}, true).

file_write_options() ->
    case erlang:get({?MODULE, eunit}) of
        true ->
            [write, raw, binary];
        _ ->
            [write, raw, binary, delayed_write]
    end.

-ifdef(TEST).

auto_prune_test() ->
    S0 = slide:fresh(10),
    S1 = slide:update(S0, 5, 3),
    S1b = idle_time_passing(S1, 4, 13),
    S2 = slide:update(S1b, 6, 14),
    S2b = idle_time_passing(S2, 15, 15),
    ?assertEqual(6, element(2, slide:sum(S2b, 15, 10))).

sum_test() ->
    setup_eunit_proc_dict(),
    S0 = slide:fresh(10),
    ?assertEqual({0, 0}, % no points, sum = 0
                 slide:sum(S0, 9, 10)),
    S1 = slide:update(S0, 3, 1),
    ?assertEqual({1, 3}, % one point, sum = 3
                 slide:sum(S1, 9, 10)),
    S2 = slide:update(S1, 5, 5),
    ?assertEqual({2, 8}, % two points, sum = 8
                 slide:sum(S2, 9, 10)),
    S3 = slide:update(S2, 7, 5),
    ?assertEqual({3, 15}, % three points (two concurrent), sum = 15
                 slide:sum(S3, 9, 10)),
    S3b = idle_time_passing(S3, 6, 13),
    S4 = slide:update(S3b, 11, 14),
    ?assertEqual(23, % ignoring first reading, sum = 23
                 element(2, slide:sum(S4, 14, 10))),
    S4b = idle_time_passing(S4, 15, 18),
    ?assertEqual(11, % shifted window
                 element(2, slide:sum(S4b, 18, 10))),
    S4c = idle_time_passing(S4b, 19, 21),
    S5 = slide:update(S4c, 13, 22),
    ?assertEqual(24, % shifted window
                 element(2, slide:sum(S5, 22, 10))).

idle_time_passing(Slide, StartMoment, EndMoment) ->
    lists:foldl(fun(Moment, S) -> slide:update(S, 0, Moment) end,
                Slide, lists:seq(StartMoment, EndMoment)).

mean_test() ->
    setup_eunit_proc_dict(),
    S0 = slide:fresh(10),
    ?assertEqual({0, undefined}, % no points, no average
                 slide:mean(S0)),
    S1 = slide:update(S0, 3, 1),
    ?assertEqual({1, 3.0}, % one point, avg = 3
                 slide:mean(S1, 9, 10)),
    S2 = slide:update(S1, 5, 5),
    ?assertEqual({2, 4.0}, % two points, avg = 4
                  slide:mean(S2, 9, 10)),
    S3 = slide:update(S2, 7, 5),
    ?assertEqual({3, 5.0}, % three points (two concurrent), avg = 5
                  slide:mean(S3, 9, 10)),
    S3b = idle_time_passing(S3, 6, 13),
    S4 = slide:update(S3b, 11, 14),
    ?assertEqual(23/11, % ignoring first reading, avg =
                 element(2, slide:mean(S4, 14, 10))),
    S4b = idle_time_passing(S4, 15, 18),
    ?assertEqual(11/10, % shifted window
                 element(2, slide:mean(S4b, 18, 10))),
    S4c = idle_time_passing(S4b, 19, 21),
    S5 = slide:update(S4c, 13, 22),
    ?assertEqual(24/10, % shifted window
                 element(2, slide:mean(S5, 22, 10))).

mean_and_nines_test() ->
    setup_eunit_proc_dict(),
    PushReadings = fun(S, Readings) ->
                           lists:foldl(
                             fun({R,T}, A) ->
                                     slide:update(A, R, T)
                             end,
                             S, Readings)
                   end,
    S0 = slide:fresh(10),
    ?assertEqual({0, {undefined, undefined, undefined, undefined}},
                 slide:nines(S0)),
    S1 = PushReadings(S0, [ {R*100, 1} || R <- lists:seq(1, 10) ]),
    %% lists:sum([X*100 || X <- lists:seq(1,10)]) / 10 -> 550
    ?assertEqual({10, 550, {500, 958, 991, 1000}},
                 slide:mean_and_nines(S1, 10)),
    S2 = PushReadings(S1, [ {R*100, 2} || R <- lists:seq(11, 20) ]),
    %% lists:sum([X*100 || X <- lists:seq(1,20)]) / 20 -> 1050
    ?assertEqual({20, 1050, {1000, 1916, 1983, 2000}},
                 slide:mean_and_nines(S2, 10)),
    S3 = PushReadings(S2, [ {R*100, 3} || R <- lists:seq(21, 100) ]),
    %% lists:sum([X*100 || X <- lists:seq(1,100)]) / 100 -> 5050
    ?assertEqual({100, 5050, {5000, 9500, 9916, 10000}},
                 slide:mean_and_nines(S3, 10)),
    S4 = idle_time_passing(S3, 4, 11),          % 8 samples
    %% lists:sum([X*100 || X <- lists:seq(11,100)]) / (90+8) -> 5096.9
    ?assertEqual({98, 5096, {5125, 9512, 9918, 10000}},
                 slide:mean_and_nines(S4, 11)).

private_dir_test() ->
    %% Capture the initial state
    Pid = os:getpid(),
    OldSlide = application:get_env(riak_core, slide_private_dir),
    OldPlatform = application:get_env(riak_core, platform_data_dir),

    %% When slide_private_dir is set, use that.
    application:set_env(riak_core, slide_private_dir, "foo"),
    ?assertEqual("foo", private_dir()),

    %% When slide_private_dir is unset, but platform_data_dir is set,
    %% use a subdirectory of the platform_data_dir.
    application:unset_env(riak_core, slide_private_dir),
    application:set_env(riak_core, platform_data_dir, "./data"),
    ?assertEqual("./data/slide-data/" ++ Pid, private_dir()),

    %% When neither slide_private_dir nor platform_data_dir is set,
    %% use the hardcoded path.
    application:unset_env(riak_core, slide_private_dir),
    application:unset_env(riak_core, platform_data_dir),
    ?assertEqual(?DIR ++ "/" ++ Pid, private_dir()),

    %% Cleanup after ourselves
    case OldSlide of
        {ok, S} ->
            application:set_env(riak_core, slide_private_dir, S);
        _ -> ok
    end,
    case OldPlatform of
        {ok, P} ->
            application:set_env(riak_core, platform_data_dir, P);
        _ -> ok
    end.

-endif. %TEST
