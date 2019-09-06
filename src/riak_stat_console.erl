%%%-------------------------------------------------------------------
%%% @doc
%%% Commands for "riak admin stat ___ ...." call into this module from
%%% riak_core_console. the purpose of these console commands is to display
%%% to the user all the information they want about an entry or entries,
%%% then the stats can be configured/updated in the metadata/exometer directly.
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_console).
-include_lib("riak_core/include/riak_stat.hrl").

%% API
-export([
    show_stat/1,
    show_stat_0/1,
    disable_stat_0/1,
    stat_info/1,
    status_change/2,
    reset_stat/1,
    enable_metadata/1
]).

%% Additional API
-export([
    data_sanitise/1,
    data_sanitise/2,
    data_sanitise/3,
    data_sanitise/4
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%%===================================================================
%%% API
%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% riak admin stat show <entry>/type=(type())/status=(enabled|disabled|*)/[dps].
%% Show enabled or disabled stats
%% when using riak-admin stat show riak.** enabled stats will show by default
%%
%% otherwise use: riak-admin stat show <entry>/status=* | disabled
%% @end
%%%-------------------------------------------------------------------
-spec(show_stat(arg()) -> statslist()).
show_stat(Arg) ->
    {Stats,Status,Type,DPs} = data_sanitise(Arg),
    NewStats =
        case DPs of
            default -> find_entries(Stats, Status, Type);
            _       -> find_entries(Stats, Status, Type, DPs)
        end,
    print(stats,NewStats).

%%%-------------------------------------------------------------------
%% @doc
%% Check which stats in exometer are not updating (only checks enabled)
%% @end
%%%-------------------------------------------------------------------
-spec(show_stat_0(data()) -> value()).
show_stat_0(Arg) ->
    {Stats,Status,Type,DPs} = data_sanitise(Arg),
    Entries = lists:map(fun
                            ({Stat,        _Status}) ->         Stat;
                            ({Stat, _Type, _Status}) ->         Stat;
                            ({Stat, _Type, _Status, _DPs}) ->   Stat
                        end,
        find_entries(Stats,Status,Type,DPs)),
    NotUpdating = not_updating(Entries),
    print(stats,NotUpdating).

%%%-------------------------------------------------------------------
%% @doc
%% Returns all the stats information
%% @end
%%%-------------------------------------------------------------------
-spec(stat_info(data()) -> value()).
stat_info([]) ->
    print([]);
stat_info(Arg) ->
    {Attrs, RestArg} = pick_info_attrs(Arg),
    {Stats,Status,Type,DPs} = data_sanitise(RestArg),
    Found = lists:map(fun
                          ({Stat,        _Status}) ->       {Stat, find_stat_info(Stat, Attrs)};
                          ({Stat, _Type, _Status}) ->       {Stat, find_stat_info(Stat, Attrs)};
                          ({Stat, _Type, _Status, _DPs}) -> {Stat, find_stat_info(Stat, Attrs)}
                      end,
        find_entries(Stats,Status,Type,DPs)),
    print(stats,Found).


%%%-------------------------------------------------------------------
%% @doc
%% Similar to the function above, but will disable all the stats that
%% are not updating
%% @end
%%%-------------------------------------------------------------------
-spec(disable_stat_0(data()) -> ok).
disable_stat_0(Arg) ->
    {Stats,Status,Type,DPs} = data_sanitise(Arg),
    Entries = lists:map(fun
                            ({Stat,        _Status}) ->         Stat;
                            ({Stat, _Type, _Status}) ->         Stat;
                            ({Stat, _Type, _Status, _DPs}) ->   Stat
                        end,
        find_entries(Stats,Status,Type,DPs)),
    NotUpdating = not_updating(Entries),
    DisableTheseStats =
        lists:map(fun({Name, _V}) ->
            {Name, {status, disabled}}
                  end, NotUpdating),
    change_status(DisableTheseStats).



%%%-------------------------------------------------------------------
%% @doc
%% change the status of the stat (in metadata and) in exometer
%% @end
%%%-------------------------------------------------------------------
-spec(status_change(data(), status()) -> ok).
status_change(Arg, ToStatus) ->
    Entries = % if disabling lots of stats, pull out only enabled ones
    case ToStatus of
        enabled  ->
            {Stats,Status,Type,DPs} = data_sanitise(Arg, '_', disabled),
            find_entries(Stats,Status,Type,DPs);
        disabled ->
            {Stats,Status,Type,DPs} = data_sanitise(Arg, '_', enabled),
            find_entries(Stats,Status,Type,DPs)
    end,
    change_status([{Stat, {status, ToStatus}} || {Stat, _Status} <- Entries]).



%%%-------------------------------------------------------------------
%% @doc
%% resets the stats in metadata and exometer and tells metadata that the stat
%% has been reset
%% @end
%%%-------------------------------------------------------------------
-spec(reset_stat(data()) -> ok).
reset_stat(Arg) ->
    {Stats,Status,Type,DPs} = data_sanitise(Arg),
    Entries = lists:map(fun
                            ({Stat,        _Status}) ->         Stat;
                            ({Stat, _Type, _Status}) ->         Stat;
                            ({Stat, _Type, _Status, _DPs}) ->   Stat
                        end,
        find_entries(Stats,Status,Type,DPs)),
    reset_stats(Entries).


%%%-------------------------------------------------------------------
%% @doc
%% enabling the metadata allows the stats configuration and the stats values to
%% be persisted, disabling the metadata returns riak to its original functionality
%% of only using the exometer functions. Enabling and disabling the metadata occurs
%% here, directing the stats and function work occurs in the riak_stat_coordinator
%% @end
%%%-------------------------------------------------------------------
-spec(enable_metadata(data()) -> ok).
enable_metadata(Arg) ->
    Truth = ?IS_ENABLED(?METADATA_ENABLED),
    case data_sanitise(Arg) of
        Truth ->
            print("Metadata-enabled already set to ~s~n", [Arg]);
        Bool when Bool == true; Bool == false ->
            case Bool of
                true ->
                    riak_stat_mgr:reload_metadata(
                        riak_stat_exom:find_entries([riak | '_'])),
                    app_helper:get_env(riak_core, ?METADATA_ENABLED, Bool);
                false ->
                    app_helper:get_env(riak_core, ?METADATA_ENABLED, Bool)
            end;
        Other ->
            print("Wrong argument entered: ~p~n", [Other])
    end.

%%%===================================================================
%%% Helper API
%%%===================================================================

data_sanitise(Arg) ->
    riak_stat_data:data_sanitise(Arg).
data_sanitise(Arg, TypeOrStatus) ->
    riak_stat_data:data_sanitise(Arg, TypeOrStatus).
data_sanitise(Arg, Type, Status) ->
    riak_stat_data:data_sanitise(Arg, Type, Status).
data_sanitise(Arg, Type, Status, DPs) ->
    riak_stat_data:data_sanitise(Arg, Type, Status, DPs).


%%%-------------------------------------------------------------------

find_entries(Stats, Status, Type) ->
    find_entries(Stats, Status, Type, []).

find_entries(Stats, Status, Type, DPs) ->
    riak_stat_mgr:find_entries(Stats, Status, Type, DPs).

%%%-------------------------------------------------------------------

find_stat_info(Stats, Info) ->
    %% todo: change this so the stat info is pulled out during print
    riak_stat_mgr:find_stats_info(Stats, Info).


%%%-------------------------------------------------------------------

not_updating(Stats) ->
    riak_stat_mgr:find_static_stats(Stats).


change_status(Stats) ->
    riak_stat_mgr:change_status(Stats).


reset_stats(Name) ->
    riak_stat_mgr:reset_stat(Name).



%%%-------------------------------------------------------------------

print(Arg) ->
    print(Arg, []).

print(stats,Stats) ->
    print(Stats,stats);
print(Stats,Attr) ->
    riak_stat_data:print(Stats,Attr).

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

-ifdef(TEST).

-endif.
