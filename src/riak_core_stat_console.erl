%%%-------------------------------------------------------------------
%%% @doc
%%% Commands for "riak admin stat ___ ...." call into this module from
%%% riak_core_console. the purpose of these console commands is to display
%%% to the user all the information they want about an entry or entries,
%%% then the stats can be configured/updated in the metadata/exometer directly.
%%% @end
%%%-------------------------------------------------------------------
-module(riak_core_stat_console).

-include_lib("riak_core/include/riak_core_stat.hrl").

%% API
-export([
    show_stat/1,
    show_stat_0/1,
    stat_info/1,
    disable_stat_0/1,
    status_change/2,
    reset_stat/1,
    enable_metadata/1
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec(show_stat(data()) -> value()).
%% @doc
%% riak admin stat show <entry>/type=(type())/status=(enabled|disabled|*)/[dps].
%% Show enabled or disabled stats
%% when using riak-admin stat show riak.** enabled stats will show by default
%%
%% otherwise use: riak-admin stat show <entry>/status=* | disabled
%% @end
show_stat(Arg) ->
    [{Stats, MatchSpec, DP}] = data_sanitise(Arg),
    NewStats =
        case DP of
            default -> find_entries(MatchSpec, Stats);
            _ ->       find_entries(MatchSpec, Stats, DP)
        end,
    print_stats(NewStats).

-spec(show_stat_0(data()) -> value()).
%% @doc
%% Check which stats in exometer are not updating (only checks enabled)
%% @end
show_stat_0(Arg) ->
    [{Stats, MatchSpec, _DP}] = data_sanitise(Arg),
    Entries = lists:map(fun
                            ({Stat,        _Status}) ->         Stat;
                            ({Stat, _Type, _Status}) ->         Stat;
                            ({Stat, _Type, _Status, _DPs}) ->   Stat
                        end,
        find_entries(MatchSpec, Stats)),
    NotUpdating = not_updating(Entries),
    print_stats(NotUpdating).

-spec(stat_info(data()) -> value()).
%% @doc
%% Returns all the stats information
%% @end
stat_info([]) ->
    print_stats([],[]);
stat_info(Arg) ->
    {Attrs, RestArg} = pick_info_attrs(Arg),
    [{Stats, MatchSpec, _DP}] = data_sanitise(RestArg),
%%    [{Stats, MatchSpec, _DP}] = data_sanitise(Arg),
    Found = lists:map(fun
                            ({Stat,        _Status}) ->       {Stat, find_stat_info(Stat, Attrs)};
                            ({Stat, _Type, _Status}) ->       {Stat, find_stat_info(Stat, Attrs)};
                            ({Stat, _Type, _Status, _DPs}) -> {Stat, find_stat_info(Stat, Attrs)}
                        end,
        find_entries(MatchSpec, Stats)),

    print_stats(Found, []).
%%    infotest(Arg).

infotest(Arg) ->
    {T0,V0} = timer:tc(fun infotestor/1, Arg),
    {TN,VN} = timer:tc(fun infotestnew/1,Arg),
    io:fwrite("Test results:~n"),
    io:fwrite("Original Test : ~p~n", [T0]),
    io:fwrite("New Test      : ~p~n", [TN]),
    V0s = term_to_binary(V0),
    VNs = term_to_binary(VN),
    SV0 = erlang:size(V0s),
    SVN = erlang:size(VNs),
    io:fwrite("Original Size Final  : ~p~n", [SV0]),
    io:fwrite("New Size Final       : ~p~n", [SVN]).

infotestor(Arg) ->
    V1 = term_to_binary(Arg),
    V2 = erlang:size(V1),
    io:fwrite("Original Size Arg  : ~p~n", [V2]),

    {Attr, Rest} = pick_info_attrs(Arg),
    V3 = term_to_binary(Rest),
    V4 = erlang:size(V3),
    io:fwrite("Original Size Rest : ~p~n", [V4]),

    [{Stats, Match, DP}] = data_sanitise(Rest),
    V5 = term_to_binary([{Stats,Match,DP}]),
    V6 = erlang:size(V5),
    io:fwrite("Original Size data_: ~p~n", [V6]),

    Found = lists:map(fun
                          ({Stat,        _Status}) ->       Stat;
                          ({Stat, _Type, _Status}) ->       Stat;
                          ({Stat, _Type, _Status, _DPs}) -> Stat
                      end,
        find_entries(Match, Stats)),
    V7 = term_to_binary(Found),
    V8 = erlang:size(V7),
    io:fwrite("Original Size Found: ~p~n", [V8]),

    print_(Found, Attr).

infotestnew(Arg) ->
    V1 = term_to_binary(Arg),
    V2 = erlang:size(V1),
    io:fwrite("New Size Arg  : ~p~n", [V2]),

    {Attr, Rest} = pick_info_attrs(Arg),
    V3 = term_to_binary(Rest),
    V4 = erlang:size(V3),
    io:fwrite("New Size Rest : ~p~n", [V4]),

    [{Stats, Match, DP}] = data_sanitise(Rest),
    V5 = term_to_binary([{Stats,Match,DP}]),
    V6 = erlang:size(V5),
    io:fwrite("New Size data_: ~p~n", [V6]),

    Found = lists:map(fun
                          ({Stat,        _Status}) ->       {Stat, find_stat_info(Stat, Attr)};
                          ({Stat, _Type, _Status}) ->       {Stat, find_stat_info(Stat, Attr)};
                          ({Stat, _Type, _Status, _DPs}) -> {Stat, find_stat_info(Stat, Attr)}
                      end,
        find_entries(Match, Stats)),
    V7 = term_to_binary(Found),
    V8 = erlang:size(V7),
    io:fwrite("New Size Found: ~p~n", [V8]),

    print_(Found, []).


print_([],_) ->
    io:fwrite("No matching stats (test)~n");
print_(Stats, []) ->
    lists:foreach(fun
                      ({Stat, Info}) ->
                          io:fwrite("~p : ~p~n", [Stat,Info])
                          end, Stats);
print_(Stats, Attr) ->
    lists:foreach(
        fun
            ({N,_,_}) ->
                print_info_1(N, Attr);
            ({N,_}) ->
                print_info_1(N, Attr);
            (N) ->
                print_info_1(N, Attr)
        end, Stats
    ).

get_value(_, disabled, _) ->
    disabled;
get_value(E, _Status, DPs) ->
    case get_datapoint(E, DPs) of
        {ok, V} -> V;
        {error, _} -> unavailable
    end.

get_datapoint(E, DPs) ->
    riak_core_stat_exometer:get_datapoint(E,DPs).


% used to print the entire stat information
print_info_1(N, [A | Attrs]) ->
    Hdr = lists:flatten(io_lib:fwrite("~p: ~n", [N])),
    Pad = lists:duplicate(length(Hdr), $\s),
    Info = get_info(core, N),
    Status = proplists:get_value(status, Info, enabled),
    Body = [case proplists:get_value(A, Info) of
                undefined -> [];
                Other -> io_lib:fwrite("~w = ~p~n", [A, Other])
            end
        | lists:map(fun(value) ->
            io_lib:fwrite(Pad ++ "~w = ~p~n",
                [value, get_value(N, Status, default)]);
            (Ax) ->
                case proplists:get_value(Ax, Info) of
                    undefined -> [];
                    Other -> io_lib:fwrite(Pad ++ "~w = ~p~n",
                        [Ax, Other])
                end
                    end, Attrs)],
    io:put_chars([Hdr, Body]).


get_info(Name, Info) ->
    case riak_core_stat_coordinator:get_info(Name, Info) of
        undefined ->
            [];
        Other ->
            Other
    end.




-spec(disable_stat_0(data()) -> ok).
%% @doc
%% Similar to the function above, but will disable all the stats that
%% are not updating
%% @end
disable_stat_0(Arg) ->
    [{Stats, MatchSpec, _DP}] = data_sanitise(Arg),
    Entries = lists:map(fun
                            ({Stat,        _Status}) ->         Stat;
                            ({Stat, _Type, _Status}) ->         Stat;
                            ({Stat, _Type, _Status, _DPs}) ->   Stat
                        end,
        find_entries(MatchSpec, Stats)),
    NotUpdating = not_updating(Entries),
    DisableTheseStats =
        lists:map(fun({Name, _V}) ->
            {Name, {status, disabled}}
                  end, NotUpdating),
    change_status(DisableTheseStats).

-spec(status_change(data(), status()) -> ok).
%% @doc
%% change the status of the stat (in metadata and) in exometer
%% @end
status_change(Arg, ToStatus) ->
    Entries = % if disabling lots of stats, pull out only enabled ones
    case ToStatus of
        enabled  ->
            [{Stats, MatchSpec, _DP}] = data_sanitise(Arg, '_', disabled),
            find_entries(MatchSpec, Stats);
        disabled ->
            [{Stats, MatchSpec, _DP}] = data_sanitise(Arg, '_', enabled),
            find_entries(MatchSpec, Stats)
    end,
    change_status([{Stat, {status, ToStatus}} || {Stat, _Status} <- Entries]).

-spec(reset_stat(data()) -> ok).
%% @doc
%% resets the stats in metadata and exometer and tells metadata that the stat
%% has been reset
%% @end
reset_stat(Arg) ->
    [{Stats, MatchSpec, _DP}] = data_sanitise(Arg),
    Entries = lists:map(fun
                            ({Stat,        _Status}) ->         Stat;
                            ({Stat, _Type, _Status}) ->         Stat;
                            ({Stat, _Type, _Status, _DPs}) ->   Stat
                        end,
        find_entries(MatchSpec, Stats)),
    reset_stats(Entries).

-spec(enable_metadata(data()) -> ok).
%% @doc
%% enabling the metadata allows the stats configuration and the stats values to
%% be persisted, disabling the metadata returns riak to its original functionality
%% of only using the exometer functions. Enabling and disabling the metadata occurs
%% here, directing the stats and function work occurs in the riak_stat_coordinator
%% @end
enable_metadata(Arg) ->
    Truth = ?IS_ENABLED(?META_ENABLED),
    case data_sanitise(Arg) of
        Truth ->
            io:fwrite("Metadata-enabled already set to ~s~n", [Arg]);
        Bool when Bool == true; Bool == false ->
            case Bool of
                true ->
                    riak_core_stat_coordinator:reload_metadata(
                       riak_core_stat_exometer:find_entries([riak | '_'])),
                    app_helper:get_env(riak_core, ?META_ENABLED, Bool);
                false ->
                    app_helper:get_env(riak_core, ?META_ENABLED, Bool)
            end;
        Other ->
            io:fwrite("Wrong argument entered: ~p~n", [Other])
    end.


%%%===================================================================
%%% Admin API
%%%===================================================================

data_sanitise(Arg, Type, Status) ->
    riak_core_stat_data:data_sanitise(Arg, Type, Status).
data_sanitise(Arg) ->
    riak_core_stat_data:data_sanitise(Arg).

print_stats(Entries) ->
    print_stats(Entries, []).
print_stats(Entries, Attributes) ->
    riak_core_stat_data:print(Entries, Attributes).

%%%===================================================================
%%% Coordinator API
%%%===================================================================

find_entries(MatchSpec, StatNames) ->
    find_entries(MatchSpec, StatNames, []).
find_entries(MatchSpec, StatNames, DP) ->
    riak_core_stat_coordinator:find_entries(MatchSpec, StatNames, DP).

not_updating(StatNames) ->
    riak_core_stat_coordinator:find_static_stats(StatNames).

find_stat_info(Stats, Info) ->
    riak_core_stat_coordinator:find_stats_info(Stats, Info).

change_status(Stats) ->
    riak_core_stat_coordinator:change_status(Stats).

reset_stats(Name) ->
    riak_core_stat_coordinator:reset_stat(Name).


%%%===================================================================
%%% Helper functions
%%%===================================================================

-spec(pick_info_attrs(data()) -> value()).
%% @doc get list of attrs to print @end
pick_info_attrs(Arg) ->
    case lists:foldr(
        fun("-name", {As, Ps}) -> {[name | As], Ps};
            ("-type", {As, Ps}) -> {[type | As], Ps};
            ("-module", {As, Ps}) -> {[module | As], Ps};
            ("-value", {As, Ps}) -> {[value | As], Ps};
            ("-cache", {As, Ps}) -> {[cache | As], Ps};
            ("-status", {As, Ps}) -> {[status | As], Ps};
            ("-timestamp", {As, Ps}) -> {[timestamp | As], Ps};
            ("-options", {As, Ps}) -> {[options | As], Ps};
            (P, {As, Ps}) -> {As, [P | Ps]}
        end, {[], []}, split_arg(Arg)) of
        {[], Rest} ->
            {[name, type, module, value, cache, status, timestamp, options], Rest};
        Other ->
            Other
    end.

split_arg(Str) ->
    re:split(Str, "\\s", [{return, list}]).