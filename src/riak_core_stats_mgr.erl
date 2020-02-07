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
    find_entries/4,
    exometer_find_entries/2,
    stats/0,
    app_stats/1,
    get_datapoint/2,
    get_value/1,
    get_values/1,
    get_info/1, get_info/2,
    aliases_prefix_foldl/0,
    alias/1,
    update/3,
    unregister/1,
    reset/1,
    find_stats_info/2,
    change_status/1,
    timestamp/0
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
    riak_core_stat_persist:register(StatInfo).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% NB : DP = DataPoint
%%      Alias : Name of a datapoint of a Stat.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

register_exom({StatName, Type, Opts, Aliases}) ->
    exometer:re_register(StatName, Type ,Opts),
    lists:foreach(fun
                      ({DP,Alias}) ->
                          exometer_alias:new(Alias,StatName,DP)
                  end,Aliases).

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
    case riak_core_stat_persist:find_entries(Stats, Status, Type) of
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
    case select(MS) of
        [] ->
            try exometer_find_entries(Stats, Status) of
                NewStats ->
                    {NewStats, DPs}
            catch _:_ -> {[],[]}
            end;
        NewStats ->
            {NewStats,DPs}
    end.

make_exo_ms(Stats,Status,Type) ->
    [{{Stat,Type,Status},[],['$_']} || Stat <- Stats].

%%%-------------------------------------------------------------------
%% @doc
%% Find the stat in exometer using pattern :: ets:match_spec()
%% @end
%%%-------------------------------------------------------------------
-spec(select(pattern()) -> stat_value()).
select(MatchSpec) ->
    exometer:select(MatchSpec).

%%%-------------------------------------------------------------------
%% @doc
%% Use @see exometer:find_entries to get the name, type and status of
%% a stat given, fo all the stats that match the Status given put into
%% a list to be returned
%% @end
%%%-------------------------------------------------------------------
-spec(exometer_find_entries(metricname(), status()) -> listofstats()).
exometer_find_entries(Stats, Status) ->
    [lists:foldl(fun
                     ({Name, Type, EStatus}, Found)
                         when EStatus == Status orelse Status == '_' ->
                         [{Name, Type, Status} | Found];
                     (_, Found) -> Found % Different status
                 end,[], exometer_find_entries(Stat)) || Stat <- Stats].

exometer_find_entries(Stat) ->
    exometer:find_entries(Stat).

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
    Found = aliases_regexp_foldr([Re]),
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


aliases_regexp_foldr([N]) ->
    exometer_alias:regexp_foldr(N,alias_fun(),orddict:new()).

aliases_prefix_foldl() ->
    exometer_alias:prefix_foldl(<<>>,alias_fun(),orddict:new()).

-spec(alias(Group :: orddict:orddict()) -> ok | acc()).
alias(Group) ->
    lists:keysort(
        1,
        lists:foldl(
            fun({K, DPs}, Acc) ->
                case get_datapoint(K, [D || {D, _} <- DPs]) of
                    {ok, Vs} when is_list(Vs) ->
                        lists:foldr(fun({D, V}, Acc1) ->
                            {_, N} = lists:keyfind(D, 1, DPs),
                            [{N, V} | Acc1]
                                    end, Acc, Vs);
                    Other ->
                        Val = case Other of
                                  {ok, disabled} -> undefined;
                                  _ -> 0
                              end,
                        lists:foldr(fun({_, N}, Acc1) ->
                            [{N, Val} | Acc1]
                                    end, Acc, DPs)
                end
            end, [], orddict:to_list(Group))).

alias_fun() ->
    fun(Alias, Entry, DP, Acc) ->
        orddict:append(Entry, {DP, Alias}, Acc)
    end.

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
    case exometer:get_value(Entry, DPs) of
        {ok, V} -> V;
        _ -> unavailable
    end.
get_info(Name, Info) ->
    exometer:info(Name, Info).

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
    case exometer_alias:resolve(Alias) of
        error -> [];
        {Entry,DP} ->
            case find_entries_exom({[Entry],Status,Type,[DP]}) of
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
stats() -> get_stats([[prefix()|'_']]).

%%%-------------------------------------------------------------------
%% @doc (Shell function - not used internally).
%% Print all the stats for an app stored in exometer/metadata
%% @end
%%%-------------------------------------------------------------------
-spec(app_stats(app()) -> print()).
app_stats(App) -> get_stats([[prefix(),App|'_']]).

%%%-------------------------------------------------------------------
%% @doc
%% The Path is the start or full name of the stat(s) you wish to find,
%% i.e. [riak,riak_kv|'_'] as a path will return stats with those to
%% elements in their path. and uses exometer:find_entries
%% @end
%%%-------------------------------------------------------------------
-spec(get_values(listofstats()) -> print()).
get_values(Arg) -> exometer:get_values(Arg).

get_value(Name) ->
    exometer:get_value(Name).
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
    exometer:info(Stat, ?INFOSTAT).

%%%===================================================================

-spec(find_stats_info(metricname(), datapoints()) -> listofstats()).
find_stats_info(Stats, Info) when is_atom(Info) ->
    find_stats_info(Stats, [Info]);
find_stats_info(Stat, Info) when is_list(Info) ->
    lists:foldl(fun(DP, Acc) ->
        case get_datapoint(Stat, DP) of
            {ok, [{DP, _Error}]} ->
                Acc;
            {ok, Value} ->
                [{DP, Value} | Acc];
            _ -> Acc
        end
                end, [], Info).


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
update(Name, Val, Type) ->
    update(Name, Val, Type, []).
update(Name, Val, Type, Opts) ->
    exometer:update_or_create(Name, Val,Type, Opts).

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
    riak_core_stat_persist:unregister(StatName).

unregister_in_exometer(StatName) ->
    exometer:delete(StatName).

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
    lists:flatten(riak_core_stat_persist:change_status(Arg)).

change_exom_status(Stats)  when is_list(Stats) ->
    lists:flatten(
        [change_exom_status(Stat,Status)||{Stat,Status} <- Stats]);
change_exom_status({Stat, Status}) ->
    change_exom_status(Stat, Status).
change_exom_status(Stat, Status) ->
    set_opts(Stat, [{status,Status}]),
    {Stat,Status}.

set_opts(StatName, Opts) ->
    exometer:setopts(StatName, Opts).


-spec(reset(statname()) -> ok).
reset(Stat) ->
    exometer:reset(Stat).

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
    riak_core_stat_persist:maybe_meta(Fun,Args).

%%%-------------------------------------------------------------------
%% @doc Returns the timestamp to put in the stat entry  @end
%%%-------------------------------------------------------------------
-spec(timestamp() -> timestamp()).
timestamp() ->
    exometer_util:timestamp().

