%%%-------------------------------------------------------------------
%%% @doc
%%% The middleman between exometer and metadata and the rest of the app,
%%% any information needed from exometer or metadata goes through the
%%% manager
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_mgr).
-include_lib("riak_core/include/riak_stat.hrl").

%% Main API
-export([
    maybe_meta/2,
    reload_metadata/1]).

%% Registration API
-export([
    register/1]).

%% Reading API
-export([
    find_entries/4,
    find_stats_info/2,
    find_static_stats/1,
    aggregate/2]).

%% Updating API
-export([
    change_status/1
]).

%% Deleting/Resetting API
-export([
    reset_stat/1,
    unregister/1
]).

%% Profile API
-export([
    save_profile/1,
    load_profile/1,
    delete_profile/1,
    reset_profile/0,
    get_profiles/0,
    get_loaded_profile/0]).

%% Specific to manager Macros:

-define(DISABLED_METADATA,  io:fwrite("Metadata is Disabled~n")).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%%===================================================================
%%% Main API
%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% Check the apps env for the status of the metadata, the persistence
%% of stat configuration can be disabled as a fail safe, or in case
%% the stats configuration doesn't need to be semi-permanent for a
%% length of time (i.e. testing)
%% @end
%%%-------------------------------------------------------------------
-spec(maybe_meta(function(), arguments()) -> false | ok | error() | arg()).
maybe_meta(Function, Arguments) ->
    case ?IS_ENABLED(?METADATA_ENABLED) of
        false -> ?DISABLED_METADATA, false; %% it's disabled
        true  -> case Arguments of          %% it's enabled (default)
                     []        -> Function();
                     {U,D}     -> Function(U,D);
                     {U,D,T}   -> Function(U,D,T);
                     {U,D,T,C} -> Function(U,D,T,C);
                     U -> Function(U)
                 end
    end.


reload_metadata(Stats) ->
    change_meta_status(Stats).


%%%===================================================================
%%% Registration API
%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% register in metadata and pull out the status,
%% and send that status to exometer
%% @end
%%%-------------------------------------------------------------------
-spec(register(statinfo()) -> ok | error()).
register(StatInfo) ->
    DefFun = fun register_both/1,
    ExoFun = fun register_exom/1,
    case maybe_meta(DefFun, StatInfo) of
        false     -> ExoFun(StatInfo);
        Otherwise -> Otherwise
    end.

register_both(StatInfo) ->
    case register_meta(StatInfo) of
        [] -> ok; %% stat is deleted or recorded as unregistered in meta
        NewOpts -> {Name, Type, _Opts, Aliases} = StatInfo,
            register_exom({Name, Type, NewOpts, Aliases})
    end.

register_meta(StatInfo) ->
    riak_stat_meta:register(StatInfo).

register_exom(StatInfo) ->
    riak_stat_exom:register(StatInfo).

%%%===================================================================
%%% Reading Stats API
%%%===================================================================

%%%-------------------------------------------------------------------
%%% @doc
%%% look for the stat in @see legacy_search first, if it is not the
%%% alias of a metric then check in the metadata, the metadata takes
%%% slightly longer than exometer to retrieve (0.01 millis) but it
%%% reduces the traffic to exometer, if the metadata is disabled it
%%% will look in exometer anyway, or if it cannot find it.
%%% @end
%%%-------------------------------------------------------------------
-spec(find_entries(statslist(), status(),type(),datapoint()) -> statslist()).
find_entries(Stats,Status,Type,DPs) ->
    MFun = fun find_entries_meta/4,
    EFun = fun find_entries_exom/4,
    %% check in legacy search first, as it takes less time to perform
    case legacy_search(Stats, Status, Type) of
        [] ->
            case maybe_meta(MFun, {Stats, Status, Type, DPs}) of
                false ->
                    EFun(Stats, Status, Type, DPs);
                Return ->
                    Return
            end;
        Stats ->
            {Stats, DPs}
    end.

%%%-------------------------------------------------------------------
%%% @doc
%%% legacy search looks for the alias in exomter_alias to return the
%%% stat name and its value.
%%% @end
%%%-------------------------------------------------------------------
-spec(legacy_search(statslist(), status(), type()) -> statslist()).
legacy_search(Stats, Status, Type) ->
    lists:map(fun(S) ->
        legacy_search_(S, Status, Type)
              end, Stats).

legacy_search_(Stat, Status, Type) ->
    case re:run(Stat, "\\.",[]) of
        {match, _} -> %% wrong format, does not match
            [];
        nomatch ->
            Re = <<"^", (make_re(Stat))/binary, "$">>,
                [{Stat, legacy_search_cont(Re, Status, Type)}]
    end.

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
                        {ok, Values} when is_list(Values) ->
                            [{Entry, zip_values(Values, DPs)} | Acc];
                        {ok, disabled} when Status == '_';
                            Status == disabled ->
                            [{Entry, zip_disabled(DPs)} | Acc];
                        _ ->
                            [{Entry, [{D, undefined} || D <- DPnames]} | Acc]
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
    riak_stat_exom:get_datapoint(Entry, DPs).

get_info(Name, Info) ->
    riak_stat_exom:get_info(Name, Info).

%%%-------------------------------------------------------------------

find_entries_meta(Stats, Status, Type, DPs) ->
    case riak_stat_meta:find_entries(Stats, Status, Type, DPs) of
        [] -> %% it is not registered or "unregistered"
            find_entries_exom(Stats, Status, Type, DPs);
        {error, _Reason} ->
            find_entries_exom(Stats, Status, Type, DPs);
        Stats when DPs == []->
            Stats;
        Stats ->
            find_entries_aliases(Stats)
    end.

find_entries_aliases(Stats) ->
    lists:foldl(fun
                    ({Name, Type, Status, Aliases}, Acc) ->
                        DPs =
                [riak_stat_exom:find_alias(Alias) || Alias <- Aliases],
                        [{Name, Type, Status, DPs} | Acc];
                    (Other, Acc) ->
                        [Other | Acc]
                end, [], Stats).

find_entries_exom(Stats, Status, Type, DPs) ->
    MS = make_exo_ms(Stats, Status, Type),
    case exo_select(MS) of
        [] ->
            {riak_stat_exom:find_entries(Stats, Status),DPs};
        Stats ->
            {Stats, DPs}
    end.

exo_select(MatchSpec) ->
    riak_stat_exom:select(MatchSpec).

make_exo_ms(Stats,Status,Type) ->
    [{{S,Type,'_'},[{'=:=','$status',Status}],['$_']} || S <- Stats].

%%%-------------------------------------------------------------------

find_stats_info(Stats, Info) ->
    riak_stat_exom:find_stats_info(Stats, Info).

%%%-------------------------------------------------------------------
%% @doc
%% find all the stats in exometer with the value = 0
%% @end
%%%-------------------------------------------------------------------
-spec(find_static_stats(stats()) -> status()).
find_static_stats(Stats) ->
    case riak_stat_exom:find_static_stats(Stats) of
        [] ->
            [];
        {error, _Reason} ->
            [];
        Stats ->
            Stats
    end.

-spec(aggregate(pattern(), datapoint()) -> stats()).
aggregate(Pattern, DPs) ->
    riak_stat_exom:aggregate(Pattern, DPs).



%%%===================================================================
%%% Updating Stats API
%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% change status in metadata and then in exometer if metadata]
%% is enabled.
%% @end
%%%-------------------------------------------------------------------
-spec(change_status(stats()) -> ok | error()).
change_status([]) ->
    io:format("Error, no stats~n");
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
    riak_stat_meta:change_status(Arg).

change_exom_status(Arg) ->
    riak_stat_exom:change_status(Arg).

%%%===================================================================
%%% Deleting/Resetting Stats API
%%%===================================================================


%%%-------------------------------------------------------------------
%% @doc
%% reset the stat in exometer and in the metadata
%% @end
%%%-------------------------------------------------------------------
-spec(reset_stat(statname()) -> ok).
reset_stat(StatName) ->
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
    riak_stat_meta:reset_stat(Arg).

reset_exom_stat(Arg) ->
    riak_stat_exom:reset_stat(Arg).

%%%-------------------------------------------------------------------


%%%-------------------------------------------------------------------
%% @doc
%% set status to unregister in metadata, and delete
%% in exometer
%% @end
%%%-------------------------------------------------------------------
-spec(unregister(statname()) -> ok | error()).
unregister(StatName) ->
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
    riak_stat_meta:unregister(StatName).

unregister_in_exometer(StatName) ->
    riak_stat_exom:unregister(StatName).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%% Profile API %%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%-------------------------------------------------------------------
%%% @doc
%%% Before continuing - the API checks if the metadata is enabled
%%% If it isn't it returns "Metadata is disabled"
%%% If it is enabled it sends the data to the fun in the metadata
%%% @end
%%%-------------------------------------------------------------------

save_profile(Profile) ->
    maybe_meta(fun riak_stat_meta:save_profile/1, Profile).

load_profile(Profile) ->
    maybe_meta(fun riak_stat_meta:load_profile/1, Profile).

delete_profile(Profile) ->
    maybe_meta(fun riak_stat_meta:delete_profile/1, Profile).

reset_profile() ->
    maybe_meta(fun riak_stat_meta:reset_profile/0, []).

get_profiles() ->
    maybe_meta(fun riak_stat_meta:get_profiles/0, []).

get_loaded_profile() ->
    maybe_meta(fun riak_stat_meta:get_loaded_profile/0, []).

-ifdef(TEST).

-endif.
