%%%-------------------------------------------------------------------
%%% @doc
%%% The middleman between exometer and metadata and the rest of the app,
%%% any information needed from exometer or metadata goes through the
%%% coordinator
%%% @end
%%%-------------------------------------------------------------------
-module(riak_core_stat_coordinator).

-include_lib("riak_core/include/riak_core_stat.hrl").

%% Console API
-export([
    find_entries/3,
    find_static_stats/1,
    find_stats_info/2,
    change_status/1,
    reset_stat/1
]).

%% Profile API
-export([
    save_profile/1,
    load_profile/1,
    delete_profile/1,
    reset_profile/0,
    get_profiles/0,
    get_loaded_profile/0
]).

%% Admin API
-export([
    get_values/1,
    register/1,
    update/3,
    unregister/1,
    aggregate/2
]).

%% Metadata API
-export([
    reload_metadata/1,
    check_status/1
]).

%% Exometer API
-export([
    get_info/2,
    get_datapoint/2,
    alias/1,
    aliases/1,
    aliases/2
]).

-define(DISABLED_META_RESPONSE, io:fwrite("Metadata is Disabled~n")).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%% Console API %%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec(find_entries(pattern(), stats(), datapoint()) -> stats()).
%% @doc
%% Find the entries in exometer if the metadata is disabled, return
%% the stat name and its status : {Stat, Status}
%% @end
find_entries(MatchSpec, Stats, DP) ->
    [{{_Name, Type, Status}, _G, _O}] = MatchSpec,
    %% pull the Matchspec contains all info
    case maybe_meta(fun find_entries_meta/4, {Stats, Status, Type, DP}) of
        false -> find_entries_exom(Stats, Status, MatchSpec, DP);
        NewStats -> NewStats
    end.

find_entries_meta(Stats, Status, Type, DP) ->
    case riak_core_stat_metadata:find_entries(Stats, Status, Type, DP) of
        [] -> find_entries_exom(Stats, Status, Type, DP);
        {error, _Reason} -> find_entries_exom(Stats, Status, Type, DP);
        NewStats ->
            find_entries_aliases(NewStats)
    end.


find_entries_exom(Stats, Status, Type, []) ->
    find_entries_exom(Stats, Status, Type);
find_entries_exom(Stats, Status, Type, DP) ->
    Stats = find_entries_exom(Stats, Status, Type),
    lists:map(fun
                  ({Stat, Type0, Status0}) ->
                      DPS = [find_entries_dps(Stat, D, []) || D <- DP],
                      case lists:flatten(DPS) of
                          [] -> {Stat, Type0, Status0};
                          DPO -> {Stat, Type0, Status0, DPO}
                      end;
                  ({Stat, Status0}) ->
                      DPS = [find_entries_dps(Stat, D, []) || D <- DP],
                      case lists:flatten(DPS) of
                          [] -> {Stat, Status0};
                          DPO -> {Stat, Status0, DPO}
                      end
              end, Stats).
find_entries_exom(Stats, Status, Type) when is_atom(Type) ->
%%  io:format("find_entries_exom~n"),
%%  case legacy_search(Stats, Type, Status) of
%%    [] -> exo_select(Stats, Status, Type);
%%    Entries -> Entries
%%  end,
    case exo_select(Stats, Status, Type) of
        [] -> legacy_search(Stats, Type, Status);
        Entries -> Entries
    end;
find_entries_exom(Stats, Status, MS) ->
%%  case legacy_search(Stats, Status) of
%%    [] -> exo_select(MS);
%%    OtherWise -> OtherWise
%%  end.
    case exo_select(Stats, Status, MS) of
        [] -> legacy_search(Stats, MS, Status);
        Entries -> Entries
    end.

find_entries_dps(Stat, DP, Default) ->
    case get_datapoint(Stat, DP) of
        {ok, Ans} -> Ans;
        {error, _} -> Default
    end.

find_entries_aliases(Stats) ->
    lists:foldl(fun
                    ({Name, Type, Status, Aliases}, Acc) ->
                        DPS = [riak_core_stat_exometer:find_alias(Alias) || Alias <- Aliases],
                        [{Name, Type, Status, DPS} | Acc];

                    (Other, Acc) ->
                        [Other | Acc]


                end, [], Stats).


-spec(legacy_search(stats(), type(), status()) -> stats()).
%% @doc
%% legacy code to find the stat and its status, in case it isn't
%% found in metadata/exometer
%% @end
%%legacy_search(Stats, Status) ->
%%%%  io:format("legacy search stats: ~p, Status: ~p~n", [Stats, Status]),
%%%  [Stats2] = lists:flatten(Stats),
%%%  Stats1 = atom_to_binary(Stats, latin1),
%%  case legacy_search_1(Stats,'_', Status) of
%%    false -> [];
%%    [false] -> [];
%%    Stats0 -> Stats0
%%  end.

%%legacy_search_1(Stats, Type, Status) ->
%%  lists:map(fun
%%	(Stat) when is_atom(Stat)->
%%    Stat0 = atom_to_binary(Stat, latin1),
%%    io:format("stat: ~p~n", [Stat0]),
%%    legacy_search(Stat0, Type, Status)
%%            end, Stats).
legacy_search(Stat, Type, Status) ->
    try case re:run(Stat, "\\.", []) of
            {match, _} ->
                [];
            nomatch ->
                Re = <<"^", (make_re(Stat))/binary, "$">>,
                {Stat, legacy_search_(Re, Type, Status)};
            {error, _Reason} -> false;
            _ ->
                []
        end
    catch _:_ ->
        []
    end.

make_re(S) ->
    repl(split_pattern(S, [])).

repl([single | T]) ->
    <<"[^_]*", (repl(T))/binary>>;
repl([double | T]) ->
    <<".*", (repl(T))/binary>>;
repl([H | T]) ->
    <<H/binary, (repl(T))/binary>>;
repl([]) ->
    <<>>.

split_pattern(<<>>, Acc) ->
    lists:reverse(Acc);
split_pattern(<<"**", T/binary>>, Acc) ->
    split_pattern(T, [double | Acc]);
split_pattern(<<"*", T/binary>>, Acc) ->
    split_pattern(T, [single | Acc]);
split_pattern(B, Acc) ->
    case binary:match(B, <<"*">>) of
        {Pos, _} ->
            <<Bef:Pos/binary, Rest/binary>> = B,
            split_pattern(Rest, [Bef | Acc]);
        nomatch ->
            lists:reverse([B | Acc])
    end.

legacy_search_(N, Type, Status) ->
    Found = aliases(regexp_foldr, [N]),
    lists:foldr(
        fun({Entry, DPs}, Acc) ->
            case match_type(Entry, Type) of
                true ->
                    DPnames = [D || {D, _} <- DPs],
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


%%%----------------------------------------------------------------%%%

-spec(find_static_stats(stats()) -> status()).
%% @doc
%% find all the stats in exometer with the value = 0
%% @end
find_static_stats(Stats) ->
    case riak_core_stat_exometer:find_static_stats(Stats) of
        [] ->
            [];
        {error, _Reason} ->
            [];
        Stats ->
            Stats
    end.

%%%----------------------------------------------------------------%%%

-spec(find_stats_info(stats(), info() | list()) -> stats()).
%% @doc
%% find the information of a stat from the information given
%% @end
find_stats_info(Stats, Info) ->
    riak_core_stat_exometer:find_stats_info(Stats, Info).

%%%----------------------------------------------------------------%%%

-spec(change_status(stats()) -> ok | error()).
%% @doc
%% change status in metadata and then in exometer if metadata]
%% is enabled.
%% @end
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

%%%----------------------------------------------------------------%%%

-spec(reset_stat(statname()) -> ok).
%% @doc
%% reset the stat in exometer and in the metadata
%% @end
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
    maybe_meta(fun riak_core_stat_metadata:save_profile/1, Profile).

load_profile(Profile) ->
    maybe_meta(fun riak_core_stat_metadata:load_profile/1, Profile).

delete_profile(Profile) ->
    maybe_meta(fun riak_core_stat_metadata:delete_profile/1, Profile).

reset_profile() ->
    maybe_meta(fun riak_core_stat_metadata:reset_profile/0, []).

get_profiles() ->
    maybe_meta(fun riak_core_stat_metadata:get_profiles/0, []).

get_loaded_profile() ->
    maybe_meta(fun riak_core_stat_metadata:get_loaded_profile/0, []).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Admin API %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec(get_values(arg()) -> stats()).
%% @doc get the values from exometer of path given @end
get_values(Arg) ->
    [{Stats, _MS, _DPS}] = riak_core_stat_data:data_sanitise(Arg),
    lists:map(fun(Path) ->
        riak_core_stat_exometer:get_values(Path)
              end, Stats).

%%%----------------------------------------------------------------%%%


-spec(register(data()) -> ok | error()).
%% @doc
%% register in metadata and pull out the status,
%% and send that status to exometer
%% @end
register({Stat, Type, Opts, Aliases} = Arg) ->
%%  io:format("riak_stat_coordinator:register(~p)~n", [Arg]),
    Fun = fun register_in_both/4,
    case maybe_meta(Fun, Arg) of
        false ->
%%      io:format("riak_stat_coordinatoe:register_in_exometer(Arg)~n"),
            register_in_exometer(Stat, Type, Opts, Aliases);
        Ans ->
            Ans
    end.

register_in_both(Stat, Type, Opts, Aliases) ->
    case register_in_metadata({Stat, Type, Opts, Aliases}) of
        [] -> ok;
        NewOpts -> register_in_exometer(Stat, Type, NewOpts, Aliases)
    end.

%%%----------------------------------------------------------------%%%

-spec(update(statname(), incrvalue(), type()) -> ok).
%% @doc
%% update unless disabled or unregistered
%% @end
update(Name, Inc, Type) ->
    Fun = fun check_in_meta/1,
    case maybe_meta(Fun, Name) of
        false ->
            update_exom(Name, Inc, Type);
        ok -> ok;
        _ -> update_exom(Name, Inc, Type)
    end.

%%%----------------------------------------------------------------%%%

-spec(unregister(statname()) -> ok | error()).
%% @doc
%% set status to unregister in metadata, and delete
%% in exometer
%% @end
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

%%%----------------------------------------------------------------%%%

-spec(aggregate(pattern(), datapoint()) -> stats()).
aggregate(P, DP) ->
    riak_core_stat_exometer:aggregate(P, DP).


%%%===================================================================
%%% Metadata API
%%%===================================================================

reload_metadata(Stats) ->
    change_meta_status(Stats).


maybe_meta(Fun, Args) ->
    case ?IS_ENABLED(?META_ENABLED) of
        true ->
            case Args of
                [] ->
                    Fun();
                {One, Two} ->
                    Fun(One, Two);
                {One, Two, Three} ->
                    Fun(One, Two, Three);
                {One, Two, Three, Four} ->
                    Fun(One, Two, Three, Four);
                One ->
                    Fun(One)
            end;
        false ->
            ?DISABLED_META_RESPONSE,
            false
    end.

register_in_metadata(StatInfo) ->
    riak_core_stat_metadata:register_stat(StatInfo).

check_in_meta(Name) ->
    riak_core_stat_metadata:check_meta(Name).

check_status(Stat) ->
    riak_core_stat_metadata:check_status(Stat).

change_meta_status(Arg) ->
    riak_core_stat_metadata:change_status(Arg).

unregister_in_metadata(StatName) ->
    riak_core_stat_metadata:unregister(StatName).

reset_meta_stat(Arg) ->
    riak_core_stat_metadata:reset_stat(Arg).


%%%===================================================================
%%% Exometer API
%%%===================================================================

get_info(Name, Info) ->
    riak_core_stat_exometer:info(Name, Info).

get_datapoint(Name, DP) ->
    riak_core_stat_exometer:get_datapoint(Name, DP).

exo_select(Stats, Status, Type) ->
    exo_select([{{Stat, Type, '_'}, [{'==', '$status', Status}], ['$_']} || Stat <- Stats]).

exo_select(Arg) ->
    case riak_core_stat_exometer:select_stat(Arg) of
        [] -> [];
        false ->  [];
        Other ->  Other
    end.



alias(Arg) ->
    riak_core_stat_exometer:alias(Arg).

aliases(Arg, Value) ->
    riak_core_stat_exometer:aliases(Arg, Value).
aliases({Arg, Value}) ->
    riak_core_stat_exometer:aliases(Arg, Value).

register_in_exometer(StatName, Type, Opts, Aliases) ->
    riak_core_stat_exometer:register_stat(StatName, Type, Opts, Aliases).

unregister_in_exometer(StatName) ->
    riak_core_stat_exometer:unregister_stat(StatName).

change_exom_status(Arg) ->
    riak_core_stat_exometer:change_status(Arg).

reset_exom_stat(Arg) ->
    riak_core_stat_exometer:reset_stat(Arg).

update_exom(Name, IncrBy, Type) ->
    riak_core_stat_exometer:update_or_create(Name, IncrBy, Type).

%%%===================================================================

