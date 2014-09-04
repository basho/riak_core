%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
%% @doc Erlang-triggerable DTrace probe support
%% 
%% In an ideal world, this module would live in a repo that would be
%% easily sharable across multiple Basho projects.  The tricky bit for
%% this would be trying to make generic the
%% `application:get_env(riak_core, dtrace_support)' call that's
%% currently in the `riak_kv_dtrace:dtrace/1' function.  But we'll
%% wait for another day, I think.
%%
%% The purpose of this module is to reduce the overhead of DTrace (and
%% SystemTap) probes when those probes are: 1. not supported by the VM,
%% or 2. disabled by application configuration.  #1 is the bigger
%% problem: a single call to the code loader can take several
%% milliseconds.  #2 is useful in the case that we want to try to reduce
%% the overhead of adding these probes even further by avoiding the NIF
%% call entirely.
%%
%% SLF's MacBook Pro tests
%%
%% without cover, with R14B04 + DTrace:
%%
%% timeit_naive                 average  2236.306 usec/call over     500.0 calls
%% timeit_mochiglobal           average     0.509 usec/call over  225000.0 calls
%% timeit_best OFF (fastest)    average     0.051 usec/call over  225000.0 calls
%% timeit_best ON -init         average     1.027 usec/call over  225000.0 calls
%% timeit_best ON +init         average     0.202 usec/call over  225000.0 calls
%%
%% with cover, with R14B04 + DTrace:
%%
%% timeit_naive                 average  2286.202 usec/call over     500.0 calls
%% timeit_mochiglobal           average     1.255 usec/call over  225000.0 calls
%% timeit_best OFF (fastest)    average     1.162 usec/call over  225000.0 calls
%% timeit_best ON -init         average     2.207 usec/call over  225000.0 calls
%% timeit_best ON +init         average     1.303 usec/call over  225000.0 calls

-module(riak_core_dtrace).

-export([dtrace/1, dtrace/3, dtrace/4, dtrace/6]).
-export([enabled/0, put_tag/1]).
-export([timeit0/1, timeit_mg/1, timeit_best/1]).   % debugging/testing only

-define(MAGIC, '**DTRACE*SUPPORT**').
-define(DTRACE_TAG_KEY, '**DTRACE*TAG*KEY**').

dtrace(ArgList) ->
    case get(?MAGIC) of
        undefined ->
            case application:get_env(riak_core, dtrace_support) of
                {ok, true} ->
                    case string:to_float(erlang:system_info(version)) of
                        {5.8, _} ->
                            %% R14B04
                            put(?MAGIC, dtrace),
                            if ArgList == enabled_check -> true;
                               true                     -> dtrace(ArgList)
                            end;
                        {Num, _} when Num > 5.8 ->
                            %% R15B or higher, though dyntrace option
                            %% was first available in R15B01.
                            put(?MAGIC, dyntrace),
                            if ArgList == enabled_check -> true;
                               true                     -> dtrace(ArgList)
                            end;
                        _ ->
                            put(?MAGIC, unsupported),
                            false
                    end;
                _ ->
                    put(?MAGIC, unsupported),
                    false
            end;
        dyntrace ->
            if ArgList == enabled_check -> true;
               true                     -> erlang:apply(dyntrace, p, ArgList)
            end;
        dtrace ->
            if ArgList == enabled_check -> true;
               true                     -> erlang:apply(dtrace, p, ArgList)
            end;
        _ ->
            false
    end.

%% NOTE: We do a get() first to avoid the list concat ++ if we don't need it.

dtrace(Int0, Ints, Strings) when is_integer(Int0) ->
    case get(?MAGIC) of
        unsupported ->
            false;
        _ ->
            dtrace([Int0] ++ Ints ++ Strings)
    end.

dtrace(Int0, Ints, String0, Strings) when is_integer(Int0) ->
    case get(?MAGIC) of
        unsupported ->
            false;
        _ ->
            dtrace([Int0] ++ Ints ++ [String0] ++ Strings)
    end.

%% NOTE: Due to use of ?MODULE, we may have cases where the type
%%       of String0 is an atom and not a string/iodata.

dtrace(Int0, Int1, Ints, String0, String1, Strings)
  when is_integer(Int0), is_integer(Int1) ->
    case get(?MAGIC) of
        unsupported ->
            false;
        _ ->
            S0 = if is_atom(String0) -> erlang:atom_to_binary(String0, latin1);
                    true             -> String0
                 end,
            dtrace([Int0, Int1] ++ Ints ++ [S0, String1] ++ Strings)
    end.

enabled() ->
    dtrace(enabled_check).

put_tag(Tag) ->
    case enabled() of
        true ->
            FTag = iolist_to_binary(Tag),
            put(?DTRACE_TAG_KEY, FTag),
            dyntrace:put_tag(FTag);
        false ->
            ok
    end.

timeit0(ArgList) ->
    try
        erlang:apply(dyntrace, p, ArgList)
    catch
        error:undef ->
            try
                erlang:apply(dtrace, p, ArgList)
            catch
                error:undef ->
                    false
            end
    end.

timeit_mg(ArgList) ->
    case riak_core_mochiglobal:get(?MAGIC) of
        undefined ->
            case application:get_env(riak_core, dtrace_support) of
                {ok, true} ->
                    case string:to_float(erlang:system_info(version)) of
                        {5.8, _} ->
                            %% R14B04
                            riak_core_mochiglobal:put(?MAGIC, dtrace),
                            timeit_mg(ArgList);
                        {Num, _} when Num > 5.8 ->
                            %% R15B or higher, though dyntrace option
                            %% was first available in R15B01.
                            riak_core_mochiglobal:put(?MAGIC, dyntrace),
                            timeit_mg(ArgList);
                        _ ->
                            riak_core_mochiglobal:put(?MAGIC, unsupported),
                            false
                    end;
                _ ->
                    riak_core_mochiglobal:put(?MAGIC, unsupported),
                    false
            end;
        dyntrace ->
            erlang:apply(dyntrace, p, ArgList);
        dtrace ->
            erlang:apply(dtrace, p, ArgList);
        unsupported ->
            false
    end.

timeit_best(ArgList) ->
    dtrace(ArgList).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

timeit_naive_test() ->
    test_common("timeit_naive",
                fun() ->
                        [timer:tc(?MODULE, timeit0, [[42]]) ||
                            _ <- lists:seq(1,501)]
                end).

-define(REPS, 225000).

timeit_mochiglobal_test() ->
    riak_core_mochiglobal:delete(?MAGIC),
    Reps = lists:seq(1, ?REPS),
    test_common("timeit_mochiglobal",
                fun() ->
                        Start = os:timestamp(),
                        [?MODULE:timeit_mg([42]) || _ <- Reps],
                        [unused, {timer:now_diff(os:timestamp(), Start), unused}]
                end,
                ?REPS),
    riak_core_mochiglobal:delete(?MAGIC).

timeit_best_off_test() ->
    timeit_best_common("timeit_best OFF (fastest)", false).

timeit_best_onfalse_test() ->
    application:set_env(riak_core, dtrace_support, true),
    timeit_best_common("timeit_best ON -init", false),
    application:unset_env(riak_core, dtrace_support).

timeit_best_ontrue_test() ->
    %% NOTE: This test must be run *last* because it's really
    %%       difficult to undo the dtrace/dyntrace init.
    application:set_env(riak_core, dtrace_support, true),
    timeit_best_common("timeit_best ON +init", true),
    application:unset_env(riak_core, dtrace_support).

timeit_best_common(Label, InitTheDTraceStuff_p) ->
    case re:run(erlang:system_info(system_version), "dtrace|systemtap") of
        nomatch ->
            io:format(user, "Skipping timeit_best_on test: no "
                      "DTrace/SystemTap is available\n", []);
        _ ->
            if InitTheDTraceStuff_p ->
                    catch dtrace:init(),
                    catch dyntrace:p();
               true ->
                    ok
            end,
            Reps = lists:seq(1, ?REPS),
            test_common(Label,
                        fun() ->
                                Start = os:timestamp(),
                                [?MODULE:timeit_best([42]) || _ <- Reps],
                                %% X = lists:usort([?MODULE:timeit_best([42]) || _ <- Reps]), io:format(user, "Label ~s out ~p\n", [Label, X]),
                                [unused, {timer:now_diff(os:timestamp(), Start), unused}]
                        end,
                        ?REPS)
    end.

test_common(Label, Fun) ->
    test_common(Label, Fun, 1).

test_common(Label, Fun, Multiplier) ->
    erase(?MAGIC),
    [_|Xs] = Fun(),
    NumXs = length(Xs) * Multiplier,
    AvgUSec = lists:sum([X || {X, _} <- Xs]) / NumXs,
    io:format(user, "~-28s average ~9.3f usec/call over ~9.1f calls\n",
              [Label, AvgUSec, NumXs/1]).

last_test() ->
    %% THIS SHOULD BE THE LAST TEST.
    io:format(user, "NOTE: cover analysis will skew results.  Run without "
              "cover for true timings.\n", []).

-endif. % TEST
