%% NOTE: Coordinate ?DTRACE_TAG_KEY with riak_core_dtrace.erl
-define(DTRACE_TAG_KEY, '**DTRACE*TAG*KEY**').

%% Erlang tracing-related funnel functions for DTrace/SystemTap.  When
%% using Redbug or other Erlang tracing framework, trace these
%% functions.  Include these here to avoid copy/pasting to every
%% module.

dtrace(_BKey, Category, Ints, Strings) ->
    riak_core_dtrace:dtrace(Category, Ints, Strings).

%% Internal functions, not so interesting for Erlang tracing.

dtrace_int(Category, Ints, Strings) ->
    dtrace(get(?DTRACE_TAG_KEY), Category, Ints, Strings).
