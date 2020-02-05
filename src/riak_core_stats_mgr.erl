%%%-------------------------------------------------------------------
%%% @doc
%%% This module is for the administration of the stats within
%%% riak_core and other stats, in other riak apps.
%%%
%%% These _stat modules call into this module to REGISTER, READ,
%%% UPDATE, DELETE or RESET the stats.
%%% @end
%%%-------------------------------------------------------------------
-module(riak_core_stats_mgr).
-include("riak_stat.hrl").

%% API
-export([
    register/2,
    get_stats/1,
    find_entries/2,
    stats/0,
    app_stats/1,
    get_value/1,
    get_info/1,
    update/3,
    unregister/1,
    reset/1
]).


-define(STATCACHE, app_helper:get_env(riak_core,exometer_cache,
    {cache,5000})).

%%%===================================================================
%%% API
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%%% For registration the stats call into this module to become an
%%% uniform format in a list to be "re-registered" in the metadata
%%% and in exometer. Unless the metadata is disabled (it is enabled
%%% by default), the stats are sent to both to be registered. The
%%% purpose of the metadata is to allow persistence of the stats
%%% registration and configuration - whether the stat is
%%% enabled/disabled/unregistered;
%%% Exometer does not persist the stats information.
%%% Metadata does not persist the stats values.
%% @end
%%%-------------------------------------------------------------------
-spec(register(app(), listofstats()) -> ok | error()).
register(App, Stats) ->
    lists:foreach(fun
                      ({Name,Type})                -> register_(App,Name,Type,[],[]);
                      ({Name,Type,Option})         -> register_(App,Name,Type,Option,[]);
                      ({Name,Type,Option,Aliases}) -> register_(App,Name,Type,Option,Aliases)
                  end, Stats).

register_(App, Name, Type, Options, Aliases) ->
    StatName   = stat_name(prefix(),App,Name),
    NewOptions = add_cache(Options),
    register_stats({StatName, Type, NewOptions, Aliases}).

%% All stats in App are appended onto the prefix
stat_name(Prefix, App, Name) when is_atom(Name) ->
    stat_name_([Prefix, App, Name]);
stat_name(Prefix, App, Name) when is_list(Name) ->
    stat_name_([Prefix, App | Name]).

stat_name_([P, [] | Rest]) -> [P | Rest];
stat_name_(N) -> N.

add_cache(Options) ->
    %% look for cache value in stat, if undefined - add cache option
    case proplists:get_value(cache, Options) of
        undefined -> [?STATCACHE | Options];
        _ -> Options %% Otherwise return the Options as is
    end.

-spec(register_stats(StatInfo :: tuple_stat()) -> ok | error()).
register_stats(StatInfo) ->
    DefFun = fun register_both/4,
    ExoFun = fun register_exom/1,
    case maybe_meta(DefFun, StatInfo) of
        false     -> ExoFun(StatInfo);
        Otherwise -> Otherwise
    end.

register_both(Stat,Type,Options,Aliases) ->
    register_both({Stat,Type,Options,Aliases}).
register_both(StatInfo) ->
    case register_meta(StatInfo) of
        [] -> ok; %% stat is deleted or recorded as unregistered
        NewOpts ->
            {Name, Type, _Opts, Aliases} = StatInfo,
            register_exom({Name, Type, NewOpts, Aliases})
    end.

register_meta(StatInfo) ->
    riak_core_metadata_stats:register(StatInfo).

register_exom(StatInfo) ->
    riak_core_exometer:register(StatInfo).

%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% Give a path to a stat such as : [riak,riak_kv,node,gets,time]
%% to retrieve the stats' as : [{Name, Type, Status}]
%% @end
%%%-------------------------------------------------------------------
-spec(get_stats(metricname()) -> nts_stats()).
get_stats(Path) ->
    find_entries(Path, '_').
find_entries(Stats, Status) ->
    find_entries(Stats, Status, '_', []).
%%%-------------------------------------------------------------------
%%% @doc
%%% look for the stat in @see legacy_search first, if it is not the
%%% alias of a metric then check in the metadata, the metadata takes
%%% slightly longer than exometer to retrieve (0.01 millis) but it
%%% reduces the traffic to exometer, if the metadata is disabled it
%%% will look in exometer anyway, or if it cannot find it.
%%% @end
%%%-------------------------------------------------------------------
-spec(find_entries(metrics(),status(),type(),datapoint()) ->
                                                        listofstats()).
find_entries(Stats, Status, Type, DPs) ->
    MFun = fun find_entries_meta/1,
    EFun = fun find_entries_exom/1,
    %% check in legacy search first, as it takes less time to perform
    case legacy_search(Stats, Status, Type) of
        [] ->
            case maybe_meta(MFun, {Stats, Status, Type, DPs}) of
                false ->
                    EFun(Stats, Status, Type, DPs);
                Return ->
                    Return
            end;
        [NewStats] ->  NewStats; %% alias' search
        Otherwise -> Otherwise   %% legacy search
    end.

find_entries_meta({Stats, Status, Type, DPs}) ->
    case riak_core_metadata_stats:find_entries(Stats, Status, Type) of
        [] -> %% it is not registered or "unregistered"
            find_entries_exom({Stats, Status, Type, DPs});
        {error, _Reason} ->
            find_entries_exom({Stats, Status, Type, DPs});
        [undefined] ->
            find_entries_exom({Stats, Status, Type, DPs});
        NewStats ->
            {NewStats,DPs}
    end.

find_entries_exom({Stats, Status, Type, DPs}) ->
    MS = make_exo_ms(Stats, Status, Type),
    case exo_select(MS) of
        [] ->
            try riak_core_exometer:find_entries(Stats, Status) of
                NewStats ->
                    {NewStats, DPs}
            catch _:_ -> {[],[]}
            end;
        NewStats ->
            {NewStats,DPs}
    end.

make_exo_ms(Stats,Status,Type) ->
    [{{Stat,Type,Status},[],['$_']} || Stat <- Stats].


exo_select(MatchSpec) ->
    riak_core_exometer:select(MatchSpec).

make_re(Stat) ->
    repl(split_pattern(Stat, [])).

repl([single|T]) ->
    <<"[^_]*", (repl(T))/binary>>;
repl([double|T]) ->
    <<".*", (repl(T))/binary>>;
repl([H|T]) ->
    <<H/binary, (repl(T))/binary>>;
repl([]) ->
    <<>>.

split_pattern(<<>>, Acc) ->
    lists:reverse(Acc);
split_pattern(<<"**", T/binary>>, Acc) ->
    split_pattern(T, [double|Acc]);
split_pattern(<<"*", T/binary>>, Acc) ->
    split_pattern(T, [single|Acc]);
split_pattern(B, Acc) ->
    case binary:match(B, <<"*">>) of
        {Pos,_} ->
            <<Bef:Pos/binary, Rest/binary>> = B,
            split_pattern(Rest, [Bef|Acc]);
        nomatch ->
            lists:reverse([B|Acc])
    end.

legacy_search_cont(Re, Status, Type) ->
    Found = riak_stat_exom:aliases(regexp_foldl, [Re]),
    lists:foldl(
        fun({Entry, DPs}, Acc) ->
            case match_type(Entry, Type) of
                true ->
                    DPnames = [D || {D,_}<- DPs],
                    case get_datapoint(Entry, DPnames) of
                        Values when is_list(Values) ->
                            [{Entry, zip_values(Values, DPs)} | Acc];
                        disabled when Status == '_';
                            Status == disabled ->
                            [{Entry, zip_disabled(DPs)} | Acc];
                        _ ->
                            [{Entry, [{D, undefined}
                                || D <- DPnames]} | Acc]
                    end;
                false ->
                    Acc
            end
        end, [], orddict:to_list(Found)).

match_type(_, '_') ->
    true;
match_type(Name, T) ->
    T == get_info(Name, type).

zip_values([{D, V} | T], DPs) ->
    {_, N} = lists:keyfind(D, 1, DPs),
    [{D, V, N} | zip_values(T, DPs)];
zip_values([], _) ->
    [].

zip_disabled(DPs) ->
    [{D, disabled, N} || {D, N} <- DPs].

get_datapoint(Entry, DPs) ->
    case riak_core_exometer:get_datapoint(Entry, DPs) of
        {ok, V} -> V;
        _ -> unavailable
    end.
get_info(Name, Info) ->
    riak_core_exometer:get_info(Name, Info).

%%%-------------------------------------------------------------------
%%% @doc
%%% legacy search looks for the alias in exometer_alias to return the
%%% stat name and its value.
%%% @end
%%%-------------------------------------------------------------------
-spec(legacy_search(metrics(), status(), type()) -> listofstats()).
legacy_search(Stats, Status, Type) ->
    lists:flatten(
        lists:map(fun(S) ->
            legacy_search_(S, Status, Type)
                  end, Stats)).

legacy_search_(Stat, Status, Type) ->
    try re:run(Stat, "\\.",[]) of
        {match, _} -> %% wrong format, should not match
            [];
        nomatch ->
            Re = <<"^", (make_re(Stat))/binary, "$">>,
            [{Stat, legacy_search_cont(Re, Status, Type)}]
    catch _:_ ->
        find_through_alias(Stat,Status,Type)
    end.

%%%-------------------------------------------------------------------
%% @doc
%% like legacy search we use the alias of the stat to find it's entry
%% and datapoint
%% @end
%%%-------------------------------------------------------------------
-spec(find_through_alias(metrics(),status(),type()) -> error() | ok).
find_through_alias([Alias],Status,Type) when is_atom(Alias)->
    case riak_core_exometer:resolve(Alias) of
        error -> [];
        {Entry,DP} ->
            case find_entries_exom([Entry],Status,Type,[DP]) of
                [] -> [];
                NewStat ->
                    NewStat
            end
    end;
find_through_alias(_Aliases,_Status,_Type) ->
    [].

%%%-------------------------------------------------------------------
%% @doc (Shell function - not used internally).
%% Print all the stats for riak stored in exometer/metadata
%% @end
%%%-------------------------------------------------------------------
-spec(stats() -> print()).
stats() -> print(get_stats([[prefix()|'_']])).

%%%-------------------------------------------------------------------
%% @doc (Shell function - not used internally).
%% Print all the stats for an app stored in exometer/metadata
%% @end
%%%-------------------------------------------------------------------
-spec(app_stats(app()) -> print()).
app_stats(App) -> print(get_stats([[prefix(),App|'_']])).

%%%-------------------------------------------------------------------
%% @doc (Shell function - not used internally)
%% get the value of the stat from exometer (returns enabled metrics)
%% @end
%%%-------------------------------------------------------------------
-spec(get_value(listofstats()) -> print()).
get_value(Arg) -> print(riak_core_exometer:get_values(Arg)).

%%%-------------------------------------------------------------------
%% @doc (Shell function - not used internally)
%% get the info of the stat/app-stats from exometer
%% (enabled metrics only)
%% @end
%%%-------------------------------------------------------------------
-spec(get_info(app() | metrics()) -> print()).
get_info(Arg) when is_atom(Arg) ->
    get_info([prefix(),Arg |'_']); %% assumed arg is the app (atom)
get_info(Arg) ->
    {Stats, _DPs} = get_stats([Arg]),
    print([{Stat, stat_info(Stat)} || {Stat,_T,_S} <- Stats]).

stat_info(Stat) ->
    riak_core_exometer:get_info(Stat, ?INFOSTAT).

%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% For updating the stats, each of the stats module will call into
%% this module to update the stat in exometer by IncrVal given, all
%% update function calls call into the update_or_create/3 function
%% in exometer. Meaning if the stat has been deleted but is still
%% hitting a function that updates it, it will create a basic
%% (unspecific) metric for that stat so the update values are stored
%% somewhere. This means if the stat is to be unregistered/deleted,
%% its existence should be removed from the stats code, otherwise it
%% is to be disabled.
%% @end
%%%-------------------------------------------------------------------
-spec(update(metricname(), incrvalue(), type()) -> ok | error()).
update(Name, Inc, Type) ->
    ok = riak_core_exometer:update(Name, Inc, Type).

%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% unregister a stat from the metadata leaves it's status as
%% {status, unregistered}, and deletes the metric from exometer
%% @end
%%%-------------------------------------------------------------------
-spec(unregister({app(),Idx :: term(),type(),app()} | metricname()) ->
    ok | error()).
unregister({Mod, Idx, Type, App}) -> %% specific to riak_core vnode
    unregister_vnode(App, Mod, Idx, Type);
unregister(StatName) -> %% generic
    unregister_stat(StatName).

unregister_vnode(App, Mod, Idx, Type) ->
    Prefix = prefix(),
    unregister_stat(Prefix, App, Type, Mod, Idx).

unregister_stat(StatName) ->
    unreg_stats(StatName).
unregister_stat(Prefix, App, Type, [Op, time], Index) ->
    unreg_stats([Prefix, App, Type, Op, time, Index]);
unregister_stat(Prefix, App, Type, Mod, Index) ->
    unreg_stats([Prefix, App, Type, Mod, Index]).

unreg_stats(StatName) ->
    Fun = fun unregister_in_both/1,
    case maybe_meta(Fun, StatName) of
        false ->
            unregister_in_exometer(StatName);
        Ans ->
            Ans
    end.

unregister_in_both(StatName) ->
    unregister_in_metadata(StatName),
    unregister_in_exometer(StatName).

unregister_in_metadata(StatName) ->
    riak_core_metadata_stats:unregister(StatName).

unregister_in_exometer(StatName) ->
    riak_core_exometer:unregister(StatName).

%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% Resetting the stats; change the values back to 0, and it's reset
%% counter in the metadata and in the exometer are updated +1
%% (enabled stats only)
%% @end
%%%-------------------------------------------------------------------
-spec(reset(metricname()) -> ok | error()).
reset([]) -> io:fwrite("No Stats found~n");
reset(StatName) ->
    Fun = fun reset_in_both/1,
    case maybe_meta(Fun, StatName) of
        false ->
            reset_exom_stat(StatName);
        Ans ->
            Ans
    end.

reset_in_both(StatName) ->
    reset_meta_stat(StatName),
    reset_exom_stat(StatName).

reset_meta_stat(Arg) ->
    riak_core_metadata_stats:reset_stat(Arg).

reset_exom_stat(Arg) ->
    riak_core_exometer:reset_stat(Arg).

%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% change status in metadata and then in exometer if metadata
%% is enabled.
%% @end
%%%-------------------------------------------------------------------
-spec(change_status(n_s_stats()) -> ok | error()).
change_status([]) ->
    io:format("No stats need changing~n");
change_status(StatsList) ->
    case maybe_meta(fun change_both_status/1, StatsList) of
        false ->
            change_exom_status(StatsList);
        Other ->
            Other
    end.

change_both_status(StatsList) ->
    change_meta_status(StatsList),
    change_exom_status(StatsList).

change_meta_status(Arg) ->
    riak_core_metadata_stats:change_status(Arg).

change_exom_status(Arg) ->
    riak_core_exometer:change_status(Arg).


%%%===================================================================
%%% Internal API
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%% standard prefix for all stats inside riak, all modules call
%% into this function for the prefix.
%% @end
%%%-------------------------------------------------------------------
-spec(prefix() -> prefix()).
prefix() -> app_helper:get_env(riak_stat, prefix, riak).

%%%-------------------------------------------------------------------

-spec(print(Entries :: (nts_stats() %% [{Name,  Type,Status}]
                        | n_v_stats() %% [{Name, Value/Values}]
                        | n_i_stats() %% [{Name,  Information}]
                        | stat_value())) -> ok).
print(Entries) -> riak_core_console:print(Entries).

%%%-------------------------------------------------------------------

maybe_meta(Fun, Args) ->
    riak_core_metadata_stats:maybe_meta(Fun,Args).

