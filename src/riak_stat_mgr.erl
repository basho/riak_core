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
    read_stats/4,
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

-ifdef(EUNIT).
%%-ifdef(DEBUG).
-export([
    return/2,
    register_both/1]).
%%-endif.
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
        [] -> ok; %% stat is deleted or recorded as unregistered in meta
        NewOpts -> {Name, Type, _Opts, Aliases} = StatInfo,
            register_exom({Name, Type, NewOpts, Aliases})
    end.

%%register_meta(Stat,Type,Options,Aliases) ->
%%    register_meta({Stat,Type,Options,Aliases}).
register_meta(StatInfo) ->
    riak_stat_meta:register(StatInfo).

register_exom(StatInfo) ->
    riak_stat_exom:register(StatInfo).

%%%===================================================================
%%% Reading Stats API
%%%===================================================================

read_stats(Stats,Status,Type,DPs) ->
    case legacy_search(Stats, Status, Type) of
        [] ->
            case read_exo_stats(Stats, Status, Type) of
                [] ->
                    find_entries_meta(Stats,Status,Type,DPs);
                Ans -> Ans
            end;
        Stat ->
            Stat
    end.

read_exo_stats(Stats, Status, Type) ->
    MS = make_exo_ms(Stats, Status, Type),
    riak_stat_exom:select(MS).



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
    %% todo: at this point check for the stat name, if it is {legacy,Stat},
    % then look in legacy search
    %% check in legacy search first, as it takes less time to perform
    case legacy_search(Stats, Status, Type) of
        [] ->
            case maybe_meta(MFun, {Stats, Status, Type, DPs}) of
                [] ->
                    EFun(Stats, Status, Type, DPs);
                Return ->
                    Return
            end;
        NewStats ->
            print_stats(NewStats, DPs)
    end.

%%%-------------------------------------------------------------------
%%% @doc
%%% legacy search looks for the alias in exomter_alias to return the
%%% stat name and its value.
%%% @end
%%%-------------------------------------------------------------------
-spec(legacy_search(statslist(), status(), type()) -> statslist()).
legacy_search(Stats, Status, Type) ->
    lists:flatten(
        lists:map(fun(S) ->
        legacy_search_(S, Status, Type)
              end, Stats)).

legacy_search_(Stat, Status, Type) ->
    try re:run(Stat, "\\.",[]) of %% todo: remove this, it was here for types before
        {match, _} -> %% wrong format, does not match
            [];
        nomatch ->
            Re = <<"^", (make_re(Stat))/binary, "$">>,
                [{Stat, legacy_search_cont(Re, Status, Type)}]
    catch _:_ ->
        []
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
                        Values when is_list(Values) ->
                            [{Entry, zip_values(Values, DPs)} | Acc];
                        disabled when Status == '_';
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
    case riak_stat_exom:get_datapoint(Entry, DPs) of
        {ok, V} -> V;
        _ -> unavailable
    end.
get_info(Name, Info) ->
    riak_stat_exom:get_info(Name, Info).

%%%-------------------------------------------------------------------

find_entries_meta(Stats, Status, Type, DPs) -> %% todo: why is it breaking here?
    case riak_stat_meta:find_entries(Stats, Status, Type) of
        [] -> %% it is not registered or "unregistered"
            find_entries_exom(Stats, Status, Type, DPs);
        {error, _Reason} ->
            find_entries_exom(Stats, Status, Type, DPs);
        [undefined] ->
            find_entries_exom(Stats, Status, Type, DPs);
        NewStats ->
            print_stats(NewStats,DPs)
    end.

find_entries_aliases(Stats) ->
    lists:map(fun
                    ({Name, _Type, _Status, Aliases}) ->
                        DPs =
                [riak_stat_exom:find_alias(Alias) || Alias <- Aliases],
                        io:fwrite("A ~p : ~p~n",[Name, DPs]);
%%                        {Name, Type, Status, DPs};
                    (_Other) ->
                        []
                end, Stats).

find_entries_exom(Stats, Status, Type, DPs) ->
    MS = make_exo_ms(Stats, Status, Type),
    case exo_select(MS) of
        [] ->
            try riak_stat_exom:find_entries(Stats, Status) of
                NewStats -> print_stats(NewStats, DPs)
            catch _:_ ->
                print_stats([],[])
            end;
        NewStats ->
            print_stats(NewStats,DPs)

    end.

exo_select(MatchSpec) ->
    riak_stat_exom:select(MatchSpec).

make_exo_ms(Stats,Status,Type) ->
    [{{Stat,Type,Status},[],['$_']} || Stat <- Stats].


print_stats([], _) ->
    io:fwrite("No Matching Stats~n");
print_stats(NewStats,DPs) ->
    lists:map(fun
                  ({N,_S})    when DPs == []->  get_value(N);
                  ({N,_S})    ->                find_stats_info(N,DPs);

                  ({N,_T,_S}) when DPs == [] -> get_value(N);
                  ({N,_T,_S}) ->                find_stats_info(N,DPs);

                  %% legacy pattern
                  (Legacy) ->
                      lists:map(fun
                                    ({LP,[]}) ->
                                        io:fwrite(
                                            "== ~s (Legacy pattern): No matching stats ==~n", [LP]);
                                    ({LP, Matches}) ->
                                        io:fwrite("== ~s (Legacy pattern): ==~n", [LP]),
                                        [[io:fwrite("~p: ~p (~p/~p)~n", [N, V, E, DP])
                                            || {DP, V, N} <- DPs] || {E, DPs} <- Matches];
                                    (_) ->
                                        io:fwrite("0~n"),
                                       []
                                end, Legacy)
              end,NewStats).

%%%-------------------------------------------------------------------

get_value(N) ->
    case riak_stat_exom:get_value(N) of
        {ok,Val} ->
%%            io:fwrite("~p : ",[N]),
            lists:map(fun({_,{error,_}}) -> [];
                (D) -> io:fwrite("1~p : ~p~n",[N,D])
                end, Val);
        {error, _} -> io:format("2"),[]
    end.
%%    {ok, Val} = riak_stat_exom:get_value(N),
%%    Val.

find_stats_info(Stats, Info) ->
    case riak_stat_exom:get_datapoint(Stats, Info) of
        [] -> [];
        {ok, V} -> lists:map(fun
                                 ([]) -> [];
                                 ({_DP, undefined}) -> [];
                                 ({_DP, {error,_}}) -> [];
                                 (DP) ->
                                     io:fwrite("3~p : ~p~n", [Stats, DP])
                             end, V);
        {error,_} -> get_info_2_electric_boogaloo(Stats, Info)
    end.

get_info_2_electric_boogaloo(N,Attrs) ->
    lists:flatten(io_lib:fwrite("~p: ", [N])),
    lists:map(fun
                  (undefined) -> [];
                  ([]) -> [];
                  ({_,{error,_ }}) -> [];
                  (A) -> io:fwrite("~p~n",[A])
                  end, [riak_stat_exom:get_info(N,Attrs)]).
%%    case [riak_stat_exom:get_info(N,I) || I <- Attrs] of
%%        [] -> [];
%%        undefined -> [];
%%        NAttrs -> io:fwrite("~p~n",[NAttrs])
%%    end.
%%    Pad = lists:duplicate(length(Hdr), $\s),
%%    Info = exometer:get_info(N),
%%    Status = proplists:get_value(status, Info, enabled),
%%    Body = [io_lib:fwrite("~w = ~p~n", [A, proplists:get_value(A, Info,undefined)])
%%        | lists:map(fun(value) ->
%%            io_lib:fwrite(Pad ++ "~w = ~p~n",
%%                [value, get_datapoint(N, default)]);
%%            (Ax) ->
%%                io_lib:fwrite(Pad ++ "~w = ~p~n",
%%                    [Ax, proplists:get_value(Ax, Info, undefined)])
%%                    end, Attrs)],
%%    io:put_chars([Hdr, Body]).

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

-ifdef(EUNIT).
%%-ifdef(DEBUG).
-include_lib("eunit/include/eunit.hrl").


-define(new(Mod),                   meck:new(Mod)).
-define(expect(Mod,Fun,Func),       meck:expect(Mod,Fun,Func)).
-define(expect(Mod,Fun,Val,Func),   meck:expect(Mod,Fun,Val,Func)).
-define(unload(Mod),                meck:unload(Mod)).

-define(setup(Fun),        {setup,    fun setup/0,          fun cleanup/1, Fun}).
-define(foreach(Funs),     {foreach,  fun setup/0,          fun cleanup/1, Funs}).

-define(setuptest(Desc, Test), {Desc, ?setup(fun(_) -> Test end)}).

-define(TestApps,     [riak_stat,riak_test,riak_core,riak_kv,riak_repl,riak_pipe]).
-define(TestCaches,   [{cache,6000},{cache,7000},{cache,8000},{cache,9000},{cache,0}]).
-define(TestStatuses, [{status,disabled},{status,enabled}]).
-define(TestName,     [stat,counter,active,list,pb,node,metadata,exometer]).
-define(TestTypes,    [histogram, gauge, spiral, counter, duration]).

-define(HistoAlias,   ['mean','max','99','95','median']).
-define(SpiralAlias,  ['one','count']).
-define(DuratAlias,   ['mean','max','last','min']).

-define(TestStatNum, 1000).

-define(STAT,                  stats).
-define(STATPFX,              {?STAT, ?NODEID}).
-define(NODEID,                term_to_binary(node())).

setup() ->
%%    catch(?unload(riak_stat)),
%%    catch(?unload(riak_stat_exom)),
%%    catch(?unload(riak_stat_meta)),
%%    catch(?unload(riak_core_metadata)),
%%    catch(?unload(riak_core_metadata_manager)),
%%    catch(?unload(riak_stat_data)),
%%    catch(?unload(riak_stat_mgr)),
%%    catch(?unload(riak_stat_console)),
%%    ?new(riak_stat),
%%    ?new(riak_stat_exom),
%%    ?new(riak_stat_meta),
%%    ?new(riak_core_metadata),
%%    ?new(riak_core_metadata_manager),
%%    ?new(riak_stat_data),
%%    ?new(riak_stat_mgr),
%%    ?new(riak_stat_console),
%%
%%    ?expect(riak_core_metadata_manager, data_root, fun(_) -> "data" end)
    lager:start(),
    {ok,Pid} = riak_core_metadata_manager:start_link(),
    {ok,Pid2} = riak_core_metadata_hashtree:start_link(),
%%    {ok, Pids} = riak_core_sup:start_link(),
%%    Pids = riak_core_app:start(normal,ok),
    Pids = [Pid,Pid2],
    exometer:start(),
    lists:map(fun(_) ->
        Stat = stat_generator(),
        riak_stat_mgr:register_both(Stat)
              end,
        lists:seq(1, rand:uniform(?TestStatNum))),
    [Pids].

cleanup(_Pids) ->
%%    process_flag(trap_exit,true),
%%    catch(?unload(riak_stat)),
%%    catch(?unload(riak_stat_exom)),
%%    catch(?unload(riak_stat_meta)),
%%    catch(?unload(riak_core_metadata)),
%%    catch(?unload(riak_stat_data)),
%%    catch(?unload(riak_stat_mgr)),
%%    catch(?unload(riak_stat_console)),
%%    process_flag(trap_exit,false),
%%    riak_core_metadata_manager:terminate(normal,stop),
%%    [supervisor:terminate_child(riak_core_sup, Pid) || Pid <- Pids],
    exometer:stop().


types() ->
    pick_rand_(?TestTypes,gauge).

options() ->
    Cache  = pick_rand_(?TestCaches,[]),
    Status = pick_rand_(?TestStatuses,[]),
    lists:flatten([Cache,Status]).

aliases(Stat,Type) ->
    case Type of
        histogram ->
            [alias(Stat,Alias) || Alias <- ?HistoAlias];
        gauge ->
            [];
        spiral ->
            [alias(Stat,Alias) || Alias <- ?SpiralAlias];
        counter ->
            [alias(Stat,value)];
        duration ->
            [alias(Stat,Alias) || Alias <- ?DuratAlias];
        _ -> []
    end.

alias(Stat,Alias) ->
    Pre = lists:map(fun(S) -> atom_to_list(S) end, Stat),
    Pref = lists:join("_", Pre),
    Prefi = lists:concat(Pref++"_"++atom_to_list(Alias)),
    Prefix = list_to_atom(Prefi),
    {Alias, Prefix}.


pick_rand_([],Def) ->
    pick_rand_([error],Def);
pick_rand_(List, Def) ->
    Num  = length(List),
    Elem = rand:uniform(Num),
    case lists:nth(Elem, List) of
        ok -> [];
        [] -> [];
        _ -> Def
    end.


stat_generator() ->
    Prefix = riak,
    RandomApp = pick_rand_(?TestApps,riak_stat),
    RandomName = [pick_rand_(?TestName,stat) || _ <- lists:seq(1,rand:uniform(length(?TestName)))],
    Stat = [Prefix, RandomApp | RandomName],
    RandomType = types(),
    RandomOptions = options(),
    RandomAliases = aliases(Stat,RandomType),
    {Stat, RandomType, RandomOptions, RandomAliases}.


find_entries_test_() ->
    ?setuptest("Test what is the quickest in the find_entries",
        [
            {"[riak_stat|'_'],enabled", fun test_find_entries/0},
            {"[riak_stat|'_'],enabled,spiral", fun test_find_entries_spiral/0},
            {"[riak_stat|'_'],enabled,[max,mean]", fun test_find_entries_dps/0},
            {"[riak_stat|'_'],disabled", fun test_find_entries_dis/0},
            {"[riak_stat|'_'],[one]", fun test_find_entries_onedp/0},
            {"[riak_stat|'_'],histogram,[mean]", fun test_find_entries_hist_dp/0}
        ]).

return(Test,[T1,V1,T2,V2,T3,V3,T4,V4]) ->
    FormatString = "T~p : ~p    V~p: ~p",
    ?debugFmt("Test: ~s", [Test]),
    ?debugFmt(FormatString, [1,T1,1,V1]),
    ?debugFmt(FormatString, [2,T2,2,V2]),
    ?debugFmt(FormatString, [3,T3,3,V3]),
    ?debugFmt(FormatString++"~n", [4,T4,4,V4]).

test_find_entries() ->
    StatPath = [riak|'_'],
    EStatPath = [riak],
    Status = enabled,
    {T1,V1} = timer:tc(fun find_entries_meta_print/4, [StatPath,Status,'_',[]]),
    {T2,V2} = timer:tc(fun find_entries_meta_return/4, [StatPath,Status,'_',[]]),
    {T3,V3} = timer:tc(fun find_entries_exom_print/4, [EStatPath,Status,'_',[]]),
    {T4,V4} = timer:tc(fun find_entries_exom_return/4, [EStatPath,Status,'_',[]]),
    return("riak_stat|'_', enabled",[T1,V1,T2,V2,T3,V3,T4,V4]).

test_find_entries_spiral() ->
    StatPath = [riak|'_'],
    EStatPath = [riak],
    Status = enabled,
    Type = spiral,
    {T1,V1} = timer:tc(fun find_entries_meta_print/4, [StatPath,Status,Type,[]]),
    {T2,V2} = timer:tc(fun find_entries_meta_return/4, [StatPath,Status,Type,[]]),
    {T3,V3} = timer:tc(fun find_entries_exom_print/4, [EStatPath,Status,Type,[]]),
    {T4,V4} = timer:tc(fun find_entries_exom_return/4, [EStatPath,Status,Type,[]]),
    return("riak_stat|'_', enabled, spiral",[T1,V1,T2,V2,T3,V3,T4,V4]).

test_find_entries_dps() ->
    StatPath = [riak|'_'],
    EStatPath = [riak],
    Status = enabled,
    DPs = [max,mean],
    {T1,V1} = timer:tc(fun find_entries_meta_print/4, [StatPath,Status,'_',DPs]),
    {T2,V2} = timer:tc(fun find_entries_meta_return/4, [StatPath,Status,'_',DPs]),
    {T3,V3} = timer:tc(fun find_entries_exom_print/4, [EStatPath,Status,'_',DPs]),
    {T4,V4} = timer:tc(fun find_entries_exom_return/4, [EStatPath,Status,'_',DPs]),
    return("riak_stat|'_', enabled,[max,mean]",[T1,V1,T2,V2,T3,V3,T4,V4]).

test_find_entries_dis() ->
    StatPath = [riak|'_'],
    EStatPath = [riak],
    Status = disabled,
    {T1,V1} = timer:tc(fun find_entries_meta_print/4, [StatPath,Status,'_',[]]),
    {T2,V2} = timer:tc(fun find_entries_meta_return/4, [StatPath,Status,'_',[]]),
    {T3,V3} = timer:tc(fun find_entries_exom_print/4, [EStatPath,Status,'_',[]]),
    {T4,V4} = timer:tc(fun find_entries_exom_return/4, [EStatPath,Status,'_',[]]),
    return("riak_stat|'_', disabled",[T1,V1,T2,V2,T3,V3,T4,V4]).

test_find_entries_onedp() ->
    StatPath = [riak|'_'],
    EStatPath = [riak],
    Status = enabled,
    DPs = [one],
    {T1,V1} = timer:tc(fun find_entries_meta_print/4, [StatPath,Status,'_',DPs]),
    {T2,V2} = timer:tc(fun find_entries_meta_return/4, [StatPath,Status,'_',DPs]),
    {T3,V3} = timer:tc(fun find_entries_exom_print/4, [EStatPath,Status,'_',DPs]),
    {T4,V4} = timer:tc(fun find_entries_exom_return/4, [EStatPath,Status,'_',DPs]),
    return("riak_stat|'_', enabled,[one]",[T1,V1,T2,V2,T3,V3,T4,V4]).

test_find_entries_hist_dp() ->
    StatPath = [riak|'_'],
    EStatPath = [riak],
    Status = enabled,
    Type = histogram,
    DPs = [mean],
    {T1,V1} = timer:tc(fun find_entries_meta_print/4, [StatPath,Status,Type,DPs]),
    {T2,V2} = timer:tc(fun find_entries_meta_return/4, [StatPath,Status,Type,DPs]),
    {T3,V3} = timer:tc(fun find_entries_exom_print/4, [EStatPath,Status,Type,DPs]),
    {T4,V4} = timer:tc(fun find_entries_exom_return/4, [EStatPath,Status,Type,DPs]),
    return("riak_stat|'_', enabled,histo,[mean]",[T1,V1,T2,V2,T3,V3,T4,V4]).



find_entries_meta_print(StatPath,Status0,'_',[]) ->
    {Stats,_}=
        riak_core_metadata:fold(fun
                                    ({Name, [{MStatus, _T, _O, _A}]}, {Acc, Status})
                                        when Status == '_' orelse Status == MStatus ->
                                        io:fwrite("~p : ~p~n",[Name, MStatus]),
                                        {[Name|Acc],Status};
                                    ({Name, [{MStatus, _T, _O}]}, {Acc, Status})
                                        when Status == '_' orelse Status == MStatus ->
                                        io:fwrite("~p : ~p~n",[Name, MStatus]),
                                        {[Name|Acc],Status};
                                    ({Name, [{MStatus, _T}]}, {Acc, Status})
                                        when Status == '_' orelse Status == MStatus ->
                                        io:fwrite("~p : ~p~n",[Name, MStatus]),
                                        {[Name|Acc],Status};
                                    (_Other, {Acc, Status}) ->
                                        {Acc, Status}
                                end, {[], Status0}, ?STATPFX, [{match, StatPath}]),
    length(Stats);
find_entries_meta_print(StatPath,Status0,Type0,[]) ->
    {Stats,_,_}=
        riak_core_metadata:fold(fun
                                    ({Name, [{MStatus, MType, _O, _A}]}, {Acc, Status, Type})
                                        when MType == Type
                                        andalso (Status == '_' orelse MStatus == Status) ->
                                        io:fwrite("~p, ~p, ~p~n",[Name, MType, MStatus]),
                                        {[Name|Acc],Status,Type};
                                    ({Name, [{MStatus, MType, _O}]}, {Acc, Status, Type})
                                        when MType == Type
                                        andalso (Status == '_' orelse MStatus == Status) ->
                                        io:fwrite("~p, ~p, ~p~n",[Name, MType, MStatus]),
                                        {[Name|Acc],Status,Type};
                                    ({Name, [{MStatus, MType}]}, {Acc, Status, Type})
                                        when MType == Type
                                        andalso (Status == '_' orelse MStatus == Status) ->
                                        io:fwrite("~p, ~p, ~p~n",[Name, MType, MStatus]),
                                        {[Name|Acc],Status,Type};
                                    (_Other, {Acc, Status, Type}) ->
                                        {Acc, Status, Type}
                                end, {[], Status0, Type0}, ?STATPFX, [{match, StatPath}]),
    length(Stats);
find_entries_meta_print(StatPath,Status0,'_',DPs0) ->
    {Stats,_,_}=
        riak_core_metadata:fold(fun
                                    ({Name, [{MStatus, MType, _O, MAliases}]}, {Acc, Status, DPs})
                                        when Status == '_' orelse MStatus == Status
                                        andalso MAliases =/= [] ->
                                        Result = riak_stat_meta:dp_get(DPs, MAliases),
                                        Final = find_entries_aliases({Name, MType,MStatus,Result}),
                                        io:format("~p, ~p, ~p : ~p~n", [Name, MType,MStatus,
                                            [DP || {_N,_T,_S,DP} <- Final]]),
                                        {[Name|Acc],Status,DPs};
                                    ({Name, [{MStatus, MType, MOpts}]}, {Acc, Status, DPs})
                                        when (Status == '_' orelse MStatus == Status) ->
                                        MAliases = proplists:get_value(aliases, MOpts, []),
                                        Result = riak_stat_meta:dp_get(DPs, MAliases),
                                        Final = find_entries_aliases({Name, MType,MStatus,Result}),
                                        io:format("~p, ~p, ~p : ~p~n", [Name, MType,MStatus,
                                            [DP || {_N,_T,_S,DP} <- Final]]),
                                        {[Name|Acc],Status,DPs};
                                    (_Other, {Acc, Status, DPs}) ->
                                        {Acc, Status, DPs}
                                end, {[], Status0, DPs0}, ?STATPFX, [{match, StatPath}]),
    length(Stats);
find_entries_meta_print(StatPath,Status0,Type0,DPs0) ->
    {Stats,_,_,_}=
        riak_core_metadata:fold(fun
                                ({Name, [{MStatus, MType, _O, MAliases}]}, {Acc, Status, Type, DPs})
                                    when (Type == '_' orelse MType == Type)
                                    andalso (Status == '_' orelse MStatus == Status)
                                    andalso MAliases =/= [] ->
                                    Result = riak_stat_meta:dp_get(DPs, MAliases),
                                    Final = find_entries_aliases({Name, MType,MStatus,Result}),
                                    io:format("~p, ~p, ~p : ~p~n", [Name, MType,MStatus,
                                        [DP || {_N,_T,_S,DP} <- Final]]),
                                    {[Name|Acc],Status,Type,DPs};
                                ({Name, [{MStatus, MType, MOpts}]}, {Acc, Status, Type, DPs})
                                    when (Type == '_' orelse MType == Type)
                                    andalso (Status == '_' orelse MStatus == Status) ->
                                    MAliases = proplists:get_value(aliases, MOpts, []),
                                    Result = riak_stat_meta:dp_get(DPs, MAliases),
                                    Final = find_entries_aliases({Name, MType,MStatus,Result}),
                                    io:format("~p, ~p, ~p : ~p~n", [Name, MType,MStatus,
                                        [DP || {_N,_T,_S,DP} <- Final]]),
                                    {[Name|Acc],Status,Type,DPs};
                                (_Other, {Acc, Status,Type, DPs}) ->
                                    {Acc, Status,Type, DPs}
                            end, {[], Status0, Type0, DPs0}, ?STATPFX, [{match, StatPath}]),
    length(Stats).


find_entries_meta_return(StatPath,Status0,'_',[]) ->
    {Stats, _Status} =
        riak_core_metadata:fold(fun
                                    ({Name, [{MStatus, _T, _O, _A}]}, {Acc, Status})
                                        when Status == '_' orelse Status == MStatus ->
                                        {[{Name, MStatus} | Acc], Status};

                                    ({Name, [{MStatus, _T, _O}]}, {Acc, Status})
                                        when Status == '_' orelse Status == MStatus ->
                                        {[{Name, MStatus} | Acc], Status};

                                    ({Name, [{MStatus, _T}]}, {Acc, Status})
                                        when Status == '_' orelse Status == MStatus ->
                                        {[{Name, MStatus} | Acc], Status};

                                    (_Other, {Acc, Status}) ->
                                        {Acc, Status}
                                end, {[], Status0}, ?STATPFX, [{match, StatPath}]),
    [io:fwrite("~p : ~p~n", [St, Sta]) || {St, Sta} <- Stats],
    length(Stats);
find_entries_meta_return(StatPath,Status0,Type0,[]) ->
    {Stats, _Status, _Type} =
        riak_core_metadata:fold(fun
                                    ({Name, [{MStatus, MType, _O, _A}]}, {Acc, Status, Type})
                                        when MType == Type
                                        andalso (Status == '_' orelse MStatus == Status) ->
                                        {[{Name, MType, MStatus} | Acc], Status, Type};

                                    ({Name, [{MStatus, MType, _O}]}, {Acc, Status, Type})
                                        when MType == Type
                                        andalso (Status == '_' orelse MStatus == Status) ->
                                        {[{Name, MType, MStatus} | Acc], Status, Type};

                                    ({Name, [{MStatus, MType}]}, {Acc, Status, Type})
                                        when MType == Type
                                        andalso (Status == '_' orelse MStatus == Status) ->
                                        {[{Name, MType, MStatus} | Acc], Status, Type};

                                    (_Other, {Acc, Status, Type}) ->
                                        {Acc, Status, Type}
                                end, {[], Status0, Type0}, ?STATPFX, [{match, StatPath}]),
    [io:fwrite("~p, ~p, ~p~n", [St,T, Sta]) || {St,T, Sta} <- Stats],
    length(Stats);
find_entries_meta_return(StatPath,Status0,'_',DPs0) ->
    {Stats, _Status, _DPs} =
        riak_core_metadata:fold(fun
                                    ({Name, [{MStatus, MType, _O, MAliases}]}, {Acc, Status, DPs})
                                        when Status == '_' orelse MStatus == Status
                                        andalso MAliases =/= [] ->
                                        Result = riak_stat_meta:dp_get(DPs, MAliases),
                                        case lists:flatten(Result) of
                                            [] ->
                                                {Acc, Status, DPs};
                                            Aliases ->
                                                {[{Name, MType, MStatus, Aliases} | Acc], Status, DPs}
                                        end;

                                    ({Name, [{MStatus, MType, MOpts}]}, {Acc, Status, DPs})
                                        when (Status == '_' orelse MStatus == Status) ->
                                        MAliases = proplists:get_value(aliases, MOpts, []),
                                        Result = riak_stat_meta:dp_get(DPs, MAliases),
                                        case lists:flatten(Result) of
                                            [] ->
                                                {Acc, Status, DPs};
                                            Aliases ->
                                                {[{Name, MType, MStatus, Aliases} | Acc], Status, DPs}
                                        end;

                                    (_Other, {Acc, Status, DPs}) ->
                                        {Acc, Status, DPs}
                                end, {[], Status0, DPs0}, ?STATPFX, [{match, StatPath}]),
    [io:fwrite("~p, ~p, ~p : ~p~n", [St,T, Sta,D]) || {St,T, Sta,D} <- Stats],
    length(Stats);
find_entries_meta_return(StatPath,Status0,Type0,DPs0) ->
    {Stats, _Status, _Type, _DPs} =
        riak_core_metadata:fold(fun
                                    ({Name, [{MStatus, MType, _O, MAliases}]}, {Acc, Status, Type, DPs})
                                        when (Type == '_' orelse MType == Type)
                                        andalso (Status == '_' orelse MStatus == Status)
                                        andalso MAliases =/= [] ->
                                        Result = riak_stat_meta:dp_get(DPs, MAliases),
                                        case lists:flatten(Result) of
                                            [] ->
                                                {Acc, Status, Type, DPs};
                                            Aliases ->
                                                {[{Name, MType, MStatus, Aliases} | Acc], Status, Type, DPs}
                                        end;

                                    ({Name, [{MStatus, MType, MOpts}]}, {Acc, Status, Type, DPs})
                                        when (Type == '_' orelse MType == Type)
                                        andalso (Status == '_' orelse MStatus == Status) ->
                                        MAliases = proplists:get_value(aliases, MOpts, []),
                                        Result = riak_stat_meta:dp_get(DPs, MAliases),
                                        case lists:flatten(Result) of
                                            [] ->
                                                {Acc, Status, Type, DPs};
                                            Aliases ->
                                                {[{Name, MType, MStatus, Aliases} | Acc], Status, Type, DPs}
                                        end;

                                    (_Other, {Acc, Status, DPs}) ->
                                        {Acc, Status, DPs}

                                end, {[], Status0, Type0, DPs0}, ?STATPFX, [{match, StatPath}]),
    [io:fwrite("~p, ~p, ~p : ~p~n", [St,T, Sta,D]) || {St,T, Sta,D} <- Stats],
    length(Stats).

find_entries_exom_print(StatPath,Status,'_',[]) ->
     Stats=
         lists:map(fun
                      ({Name, _EType, EStatus})
                          when EStatus == Status orelse Status == '_' ->
                          io:fwrite("~p :~p~n", [Name, EStatus]),
                            Name;
                      (_Other) ->
                          []
                  end, exometer:find_entries(StatPath)),
    length(lists:flatten(Stats));
find_entries_exom_print(StatPath,Status,Type,[]) ->
    Stats=
        lists:map(fun
                  ({Name, EType, EStatus})
                      when (EStatus == Status andalso EType == Type)
                      orelse (Status == '_' andalso EType == Type) ->
                      io:fwrite("~p, ~p, ~p~n", [Name, EType, EStatus]),
                      Name;
                  (_Other) ->
                      []
              end, exometer:find_entries(StatPath)),
    length(lists:flatten(Stats));
find_entries_exom_print(StatPath,Status,'_',DPs) ->
    Stats=
        lists:map(fun
                        ({Name, EType, EStatus})
                            when EStatus == Status orelse Status == '_' ->
                            Al = [exometer_alias:reverse_map(Name, DP) ||
                                DP <- DPs],
                            case Al of
                                [] -> [];
                                Other ->
                                    New =
                                        lists:map(fun
                                                      ({D1,{ok,Val}}) -> {D1,Val}
                                                  end,
                                            [{D,exometer_alias:get_value(Alias)}|| {Alias,_,D} <- Other]),
                                    io:fwrite("~p, ~p, ~p : ~p~n", [Name,EType,EStatus,New]),
                                    Name
                            end;
                        (_Other) ->
                           []
                    end,exometer:find_entries(StatPath)),
    length(lists:flatten(Stats));
find_entries_exom_print(StatPath,Status,Type,DPs) ->
    Stats=
        lists:map(fun
                    ({Name, EType, EStatus})
                        when (EStatus == Status andalso EType == Type)
                        orelse (Status == '_' andalso EType == Type) ->
                        Al = [exometer_alias:reverse_map(Name, DP) ||
                            DP <- DPs],
                        case Al of
                            [] -> [];
                            Other ->
                                New =
                                    lists:map(fun
                                                  ({D1,{ok,Val}}) -> {D1,Val}
                                              end,
                                        [{D,exometer_alias:get_value(Alias)}|| {Alias,_,D} <- Other]),
                                io:fwrite("~p, ~p, ~p : ~p~n", [Name,EType,EStatus,New]),
                                Name
                        end;
                    (_Other) ->
                        []
                end,exometer:find_entries(StatPath)),
    length(lists:flatten(Stats)).

find_entries_exom_return(StatPath,Status,'_',[]) ->
    Stats =
        lists:foldl(fun
                        ({Name, _EType, EStatus},Acc)
                            when EStatus == Status orelse Status == '_' ->
                            [{Name, EStatus}|Acc];
                        (_Other, Acc) ->
                            Acc
                    end,[],exometer:find_entries(StatPath)),
    [io:fwrite("~p : ~p ~n", [N,S]) || {N,S} <- Stats],
    length(Stats);
find_entries_exom_return(StatPath,Status,Type,[]) ->
    Stats =
        lists:foldl(fun
                        ({Name, EType, EStatus},Acc)
                            when (EStatus == Status andalso EType == Type)
                            orelse (Status == '_' andalso EType == Type) ->
                            [{Name, EType, EStatus}|Acc];
                        (_Other, Acc) ->
                            Acc
                    end,[],exometer:find_entries(StatPath)),
    [io:fwrite("~p, ~p, ~p ~n", [N,T,S]) || {N,T,S} <- Stats],
    length(Stats);
find_entries_exom_return(StatPath,Status,'_',DPs) ->
    Stats =
        lists:foldl(fun
                        ({Name, EType, EStatus},Acc)
                            when EStatus == Status orelse Status == '_' ->
                            Al = [exometer_alias:reverse_map(Name, DP) ||
                                DP <- DPs],
                            case Al of
                                [] -> Acc;
                                Other ->
                                    New =
                                        lists:map(fun
                                                      ({D1,{ok,Val}}) -> {D1,Val}
                                                  end,
                                            [{D,exometer_alias:get_value(Alias)}|| {Alias,_,D} <- Other]),
                                    [{Name,EType,EStatus,New}|Acc]
                            end;
                        (_Other, Acc) ->
                            Acc
                    end,[],exometer:find_entries(StatPath)),
    [io:fwrite("~p, ~p, ~p : ~p~n", [N,T,S,A]) || {N,T,S,A} <- Stats],
    length(Stats);
find_entries_exom_return(StatPath,Status,Type,DPs) ->
   Stats =
    lists:foldl(fun
                  ({Name, EType, EStatus},Acc)
                      when (EStatus == Status andalso EType == Type)
                      orelse (Status == '_' andalso EType == Type) ->
                      Al = [exometer_alias:reverse_map(Name, DP) ||
                          DP <- DPs],
                      case Al of
                          [] -> Acc;
                          Other ->
                              New =
                              lists:map(fun
                                            ({D1,{ok,Val}}) -> {D1,Val}
                                        end,
                                  [{D,exometer_alias:get_value(Alias)}|| {Alias,_,D} <- Other]),
                              [{Name,EType,EStatus,New}|Acc]
                      end;
                    (_Other, Acc) ->
                        Acc
              end,[],exometer:find_entries(StatPath)),
    [io:fwrite("~p, ~p, ~p : ~p~n", [N,T,S,A]) || {N,T,S,A} <- Stats],
    length(Stats).

-endif.
%%-endif.
