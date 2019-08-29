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
stat_info(Arg) ->
    {Attrs, RestArg} = pick_info_attrs(Arg),
    [{Stats, MatchSpec, _DP}] = data_sanitise(RestArg),
    [{Stats, MatchSpec, _DP}] = data_sanitise(Arg),
    Found = lists:map(fun
                            ({Stat,        _Status}) ->       {Stat, find_stat_info(Stat, Attrs)};
                            ({Stat, _Type, _Status}) ->       {Stat, find_stat_info(Stat, Attrs)};
                            ({Stat, _Type, _Status, _DPs}) -> {Stat, find_stat_info(Stat, Attrs)}
                        end,
        find_entries(MatchSpec, Stats)),
    print_stats(Found).

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
    [{Stats, MatchSpec, _DP}] = data_sanitise(Arg, '_', ToStatus),
    Entries = % if disabling lots of stats, pull out only enabled ones
    case ToStatus of
        enabled  -> find_entries(MatchSpec, Stats);
        disabled -> find_entries(MatchSpec, Stats)
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

split_arg([Str]) ->
    re:split(Str, "\\s", [{return, list}]).

