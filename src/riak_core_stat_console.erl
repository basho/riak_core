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
    show_stat_test/0,
    show_stat/1,
    show_stat_0/1,
    stat_info/1,
    disable_stat_0/1,
    status_change/2,
    reset_stat/1,
    enable_metadata/1
]).

-define(STATUS, enabled). %% default status
-define(TYPE,   '_').     %% default type
-define(DPs,    default). %% default Datapoints

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
show_stat(_Arg) ->
%%    NewStats = find_entries(print, Arg),
%%
%%    {Stats, MatchSpec, DP} = data_sanitise(Arg),
%%    NewStats =
%%        case DP of
%%            default -> find_entries(MatchSpec, Stats);
%%            _ ->       find_entries(MatchSpec, Stats, DP)
%%        end,
%%    print_stats(NewStats),
    show_stat_test().

show_stat_test() ->
    Arg1 = [riak|'_'],
    Arg2 = [riak,riak_kv|'_'],
    Arg3 = [riak,riak_kv,node|'_'],
    Arg4 = [riak,riak_kv,node,gets|'_'],
    Arg5 = [riak,riak_kv,node,gets],
    Arg6 = [node_gets],
    Arg7 = node_gets,

    DPs  = [mean,max],
    Type = '_',
    Status = enabled,
%%    [io:format("~p~n", [A]) || A <- [Arg1,Arg2,Arg3,Arg4,Arg5,Arg6,Arg7]],
    %% metadata

    FM1 = fun find_mentries/3,
    FM2 = fun find_mentries/4,
    FX1 = fun find_xentries/3,
    FX2 = fun find_xentries/4,
    FL1 = fun find_lentries/3,
    FL2 = fun find_lentries/4,
%%    FF1 = fun find_new_xentries/3,
%%    FF2 = fun find_new_xentries/4,
    A1 = [Arg1, Status, Type],
    A1d = [Arg1, Status, Type, DPs],
    A2 = [Arg2, Status, Type],
    A2d = [Arg2, Status, Type, DPs],
    A3 = [Arg3, Status, Type],
    A3d = [Arg3, Status, Type, DPs],
    A4 = [Arg4, Status, Type],
    A4d = [Arg4, Status, Type, DPs],
    A5 = [Arg5, Status, Type],
    A5d = [Arg5, Status, Type, DPs],
    A6 = [Arg6, Status, Type],
    A6d = [Arg6, Status, Type, DPs],
    A7 = [Arg7, Status, Type],
    A7d = [Arg7, Status, Type, DPs],


    {T1ma,V1ma} = timer:tc(FM1, A1), %% noDPs
    {T1mb,V1mb} = timer:tc(FM2, A1d), %% DPs
%%    io:format("test1~n"),
    {T2ma,V2ma} = timer:tc(FM1, A2),
    {T2mb,V2mb} = timer:tc(FM2, A2d),
%%    io:format("test2~n"),
    {T3ma,V3ma} = timer:tc(FM1, A3),
    {T3mb,V3mb} = timer:tc(FM2, A3d),
%%    io:format("test3~n"),
    {T4ma,V4ma} = timer:tc(FM1, A4),
    {T4mb,V4mb} = timer:tc(FM2, A4d),
%%    io:format("test4~n"),
    {T5ma,V5ma} = timer:tc(FM1, A5),
    {T5mb,V5mb} = timer:tc(FM2, A5d),
%%    io:format("test5~n"),
    {T6ma,V6ma} = timer:tc(FM1, A6),
    {T6mb,V6mb} = timer:tc(FM2, A6d),
%%    io:format("test6~n"),
    {T7ma,V7ma} = timer:tc(FM1, A7),
    {T7mb,V7mb} = timer:tc(FM2, A7d),
%%    io:format("test7~n"),

    %% exometer

    {T1xa,V1xa} = timer:tc(FX1, A1), %% noDPs
    {T1xb,V1xb} = timer:tc(FX2, A1d), %% DPs
%%    io:format("test1x~n"),
    {T2xa,V2xa} = timer:tc(FX1, A2),
    {T2xb,V2xb} = timer:tc(FX2, A2d),
%%    io:format("test2x~n"),
    {T3xa,V3xa} = timer:tc(FX1, A3),
    {T3xb,V3xb} = timer:tc(FX2, A3d),
%%    io:format("test3x~n"),
    {T4xa,V4xa} = timer:tc(FX1, A4),
    {T4xb,V4xb} = timer:tc(FX2, A4d),
%%    io:format("test4x~n"),
    {T5xa,V5xa} = timer:tc(FX1, A5),
    {T5xb,V5xb} = timer:tc(FX2, A5d),
%%    io:format("test5x~n"),
    {T6xa,V6xa} = timer:tc(FX1, A6),
    {T6xb,V6xb} = timer:tc(FX2, A6d),
%%    io:format("test6x~n"),
    {T7xa,V7xa} = timer:tc(FX1, A7),
    {T7xb,V7xb} = timer:tc(FX2, A7d),
%%    io:format("test7x~n"),

    %% legacy_search


    {T1la,V1la} = timer:tc(FL1, A1), %% noDPs
    {T1lb,V1lb} = timer:tc(FL2, A1d), %% DPs
%%    io:format("test1l~n"),
    {T2la,V2la} = timer:tc(FL1, A2),
    {T2lb,V2lb} = timer:tc(FL2, A2d),
%%    io:format("test2l~n"),
    {T3la,V3la} = timer:tc(FL1, A3),
    {T3lb,V3lb} = timer:tc(FL2, A3d),
%%    io:format("test3l~n"),
    {T4la,V4la} = timer:tc(FL1, A4),
    {T4lb,V4lb} = timer:tc(FL2, A4d),
%%    io:format("test4l~n"),
    {T5la,V5la} = timer:tc(FL1, A5),
    {T5lb,V5lb} = timer:tc(FL2, A5d),
%%    io:format("test5l~n"),
    {T6la,V6la} = timer:tc(FL1, A6),
    {T6lb,V6lb} = timer:tc(FL2, A6d),
%%    io:format("test6l~n"),
    {T7la,V7la} = timer:tc(FL1, A7),
    {T7lb,V7lb} = timer:tc(FL2, A7d),
%%    io:format("test7l~n"),

    %% new exometer


    Arg1List = {Arg1,
        T1mb,length(V1mb),T1ma,length(V1ma),
        T1xb,length(V1xb),T1xa,length(V1xa),
        T1lb,length(V1lb),T1la,length(V1la)
        },
    Arg2List = {Arg2,
        T2mb,length(V2mb),T2ma,length(V2ma),
        T2xb,length(V2xb),T2xa,length(V2xa),
        T2lb,length(V2lb),T2la,length(V2la)
        },
    Arg3List = {Arg3,
        T3mb,length(V3mb),T3ma,length(V3ma),
        T3xb,length(V3xb),T3xa,length(V3xa),
        T3lb,length(V3lb),T3la,length(V3la)
        },
    Arg4List = {Arg4,
        T4mb,length(V4mb),T4ma,length(V4ma),
        T4xb,length(V4xb),T4xa,length(V4xa),
        T4lb,length(V4lb),T4la,length(V4la)
        },
    Arg5List = {Arg5,
        T5mb,length(V5mb),T5ma,length(V5ma),
        T5xb,length(V5xb),T5xa,length(V5xa),
        T5lb,length(V5lb),T5la,length(V5la)
        },
    Arg6List = {Arg6,
        T6mb,length(V6mb),T6ma,length(V6ma),
        T6xb,length(V6xb),T6xa,length(V6xa),
        T6lb,length(V6lb),T6la,length(V6la)
        },
    Arg7List = {Arg7,
        T7mb,length(V7mb),T7ma,length(V7ma),
        T7xb,length(V7xb),T7xa,length(V7xa),
        T7lb,length(V7lb),T7la,length(V7la)
        },

    ArgList = [
        Arg1List,
        Arg2List,
        Arg3List,
        Arg4List,
        Arg5List,
        Arg6List,
        Arg7List
        ],

    [io:format(
        "Arg: ~p~n
        Meta(DP) : ~p (~p)    Meta(NoDP) : ~p (~p)~n
        Exom(DP) : ~p (~p)    Exom(NoDP) : ~p (~p)~n
        Lega(DP) : ~p (~p)    Lega(NoDP) : ~p (~p)~n~n",
        [Arg,MDP,MDPNUM,MNDP,MNDPNUM,
            EDP,EDPNUM,ENDP,ENDPNUM,
            LDP,LDPNUM,LNDP,LNDPNUM]) ||
        {Arg,MDP,MDPNUM,MNDP,MNDPNUM,
        EDP,EDPNUM,ENDP,ENDPNUM,
        LDP,LDPNUM,LNDP,LNDPNUM} <- ArgList],

    ok.

find_mentries(Stat, Status, Type) ->
    find_mentries(Stat, Status, Type, []).

find_mentries(Stat, Status, Type, DP) ->
%%    [].
    Entries = riak_core_stat_metadata:find_entries([Stat], Status, Type, DP),
    lists:map(fun
                  ({N, St, T, A}) ->
                      An =
                          [{DPa, exometer_alias:get_value(Al)}
                              || {DPa, Al} <- A],
                              {N, St, T, An};
                  (O) -> O
              end, Entries).


find_xentries(Arg, Status, Type) ->
    MS = makems(Arg, Status, Type),
%%    io:format("find_xentries/3~n"),
    exometer:select(MS).

find_xentries(Arg, Status, Type, DP) ->
    Entries = find_xentries(Arg, Status, Type),
%%    io:format("find_xentries/4~n"),
    lists:map(fun
                  ({N, T, S}) ->
        {N, T, S, DP}
              end, Entries).

%%find_new_xentries(Arg, Status, Type) ->
%%    Stats = exometer:find_entries(Arg),
%%    lists:foldl(fun
%%                  ({N,T,S},A) when Status == S andalso (T == Type orelse Type == '_') ->
%%                      [{N,S}|A];
%%                  (_O, A) ->
%%                      A
%%                end, [],Stats).
%%
%%find_new_xentries(Arg, Status, Type,DP) ->
%%    Stats = find_new_xentries(Arg, Status, Type),
%%    lists:map(fun
%%                  ({N,T,S}) -> {N,T,S,DP};
%%                  (O) -> O
%%              end, Stats).
%%
%%find_lentries(S,T,St) when is_atom(S) ->
%%    try atom_to_binary(S, latin1) of
%%        Arg ->
%%            try find_lentries(Arg, T, St) of
%%                Yes -> Yes
%%            catch _:_ ->
%%                find_lentries([Arg],T,St)
%%            end
%%    catch _:_ ->
%%        NArg = atom_to_list(S),
%%        find_lentries(NArg, T, St)
%%    end;
find_lentries(S, Type, Status) when is_atom(S) ->
%%    case exometer_alias:get_value(S) of
%%        {error, _Reason} ->
%%            [];
%%        {ok, Value} -> [{S,Type,Status, Value}]
%%    end
    find_lentries_(S,Type,Status);
find_lentries(S, Type, Status) ->
    try
    case re:run(S, "\\.", []) of
        {match,_} ->
            [];
        nomatch ->
            Re = <<"^", (make_re(S))/binary, "$">>,
            [{S, find_lentries_(Re, Type, Status)}];
        _ ->
            []
    end
    catch error:_ -> [] end.

make_re(S) ->
    repl(split_pattern(S, [])).


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

find_lentries_(N, Type, Status) ->
    N1 = [atom_to_list(N)],
    Found = exometer_alias:regexp_foldr(N1, fun(Alias, Entry, DP, Acc) ->
        orddict:append(Entry, {DP,Alias}, Acc)
                                           end, orddict:new()),
    lists:foldr(
        fun({Entry, DPs}, Acc) ->
            case match_type(Entry, Type) of
                true ->
                    DPnames = [D || {D,_} <- DPs],
                    case exometer:get_value(Entry, DPnames) of
                        {ok, Values} when is_list(Values) ->
                            [{Entry, zip_values(Values, DPs)} | Acc];
                        {ok, disabled} when Status=='_';
                            Status==disabled ->
                            [{Entry, zip_disabled(DPs)} | Acc];
                        _ ->
                            [{Entry, [{D,undefined} || D <- DPnames]}|Acc]
                    end;
                false ->
                    Acc
            end
        end, [], orddict:to_list(Found)).

find_lentries(N, Type, Status, DP) ->
    Stats = find_lentries(N, Type, Status),
    lists:map(fun
                  ({E, T, St}) ->
                      {E, T, St, get_value(E, Status, DP)};
                  ({E,S}) ->
                      {E,S,get_value(E,Status,DP)};
                  (O) ->
                      O
              end, Stats).

match_type(_, '_') ->
    true;
match_type(Name, T) ->
    T == exometer:info(Name, type).

zip_values([{D,V}|T], DPs) ->
    {_,N} = lists:keyfind(D, 1, DPs),
    [{D,V,N}|zip_values(T, DPs)];
zip_values([], _) ->
    [].

zip_disabled(DPs) ->
    [{D,disabled,N} || {D,N} <- DPs].


get_value(_, disabled, _) ->
    disabled;
get_value(E, _Status, DPs) ->
    case get_datapoint(E, DPs) of
        {ok, V} -> V;
        {error, _} -> unavailable
    end.

get_datapoint(E, DPs) ->
    riak_core_stat_exometer:get_datapoint(E,DPs).


makems(Arg, Status, Type) ->
    [{{Arg, Type, '_'}, [{'=:=','$status',Status}], ['$_']}].


-spec(show_stat_0(data()) -> value()).
%% @doc
%% Check which stats in exometer are not updating (only checks enabled)
%% @end
show_stat_0(Arg) ->
    {Stats, MatchSpec, _DP} = data_sanitise(Arg),
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
    {Stats, MatchSpec, _DP} = data_sanitise(RestArg),
%%    [{Stats, MatchSpec, _DP}] = data_sanitise(Arg),
    Found = lists:map(fun
                            ({Stat,        _Status}) ->       {Stat, find_stat_info(Stat, Attrs)};
                            ({Stat, _Type, _Status}) ->       {Stat, find_stat_info(Stat, Attrs)};
                            ({Stat, _Type, _Status, _DPs}) -> {Stat, find_stat_info(Stat, Attrs)}
                        end,
        find_entries(MatchSpec, Stats)),

    print_stats(Found, []).



-spec(disable_stat_0(data()) -> ok).
%% @doc
%% Similar to the function above, but will disable all the stats that
%% are not updating
%% @end
disable_stat_0(Arg) ->
    {Stats, MatchSpec, _DP} = data_sanitise(Arg),
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
    {Stats, MatchSpec, _DP} = data_sanitise(Arg),
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

%%find_entries(Format, Arg) ->
%%    riak_core_stat_coordinator:find_entries(Format, Arg,?STATUS,?TYPE,?DPs).

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
%%
%%    io:fwrite("=========================================================~n"),
%%    io:fwrite("Test for Quickest out of Meta, Exometer and Legacy_search~n"),
%%    io:fwrite("=========================================================~n"),
%%
%%    io:fwrite("Arg : ~p             No Datapoints,~n", [Arg1]),
%%    io:fwrite("Meta: ~p             Time: ~p,~n", [length(V1ma), T1ma]),
%%    io:fwrite("Exom: ~p             Time: ~p,~n", [length(V1xa), T1xa]),
%%    io:fwrite("lega: ~p             Time: ~p,~n", [length(V1la), T1la]),
%%
%%    io:fwrite("Arg : ~p             No Datapoints,~n", [Arg2]),
%%    io:fwrite("Meta: ~p             Time: ~p,~n", [length(V2ma), T2ma]),
%%    io:fwrite("Exom: ~p             Time: ~p,~n", [length(V2xa), T2xa]),
%%    io:fwrite("lega: ~p             Time: ~p,~n", [length(V2la), T2la]),
%%
%%    io:fwrite("Arg : ~p             No Datapoints,~n", [Arg3]),
%%    io:fwrite("Meta: ~p             Time: ~p,~n", [length(V3ma), T3ma]),
%%    io:fwrite("Exom: ~p             Time: ~p,~n", [length(V3xa), T3xa]),
%%    io:fwrite("lega: ~p             Time: ~p,~n", [length(V3la), T3la]),
%%
%%    io:fwrite("Arg : ~p             No Datapoints,~n", [Arg4]),
%%    io:fwrite("Meta: ~p             Time: ~p,~n", [length(V4ma), T4ma]),
%%    io:fwrite("Exom: ~p             Time: ~p,~n", [length(V4xa), T4xa]),
%%    io:fwrite("lega: ~p             Time: ~p,~n", [length(V4la), T4la]),
%%
%%    io:fwrite("Arg : ~p             No Datapoints,~n", [Arg5]),
%%    io:fwrite("Meta: ~p             Time: ~p,~n", [length(V5ma), T5ma]),
%%    io:fwrite("Exom: ~p             Time: ~p,~n", [length(V5xa), T5xa]),
%%    io:fwrite("lega: ~p             Time: ~p,~n", [length(V5la), T5la]),
%%
%%    io:fwrite("Arg : ~p             No Datapoints,~n", [Arg6]),
%%    io:fwrite("Meta: ~p             Time: ~p,~n", [length(V6ma), T6ma]),
%%    io:fwrite("Exom: ~p             Time: ~p,~n", [length(V6xa), T6xa]),
%%    io:fwrite("lega: ~p             Time: ~p,~n", [length(V6la), T6la]),
%%
%%    io:fwrite("Arg : ~p             No Datapoints,~n", [Arg7]),
%%    io:fwrite("Meta: ~p             Time: ~p,~n", [length(V7ma), T7ma]),
%%    io:fwrite("Exom: ~p             Time: ~p,~n", [length(V7xa), T7xa]),
%%    io:fwrite("lega: ~p             Time: ~p,~n", [length(V7la), T7la]),
%%
%%    %% With DPs:
%%
%%    io:fwrite("Arg : ~p             Datapoints,~n", [Arg1]),
%%    io:fwrite("Meta: ~p             Time: ~p,~n", [length(V1mb), T1mb]),
%%    io:fwrite("Exom: ~p             Time: ~p,~n", [length(V1xb), T1xb]),
%%    io:fwrite("lega: ~p             Time: ~p,~n", [length(V1lb), T1lb]),
%%
%%    io:fwrite("Arg : ~p             Datapoints,~n", [Arg2]),
%%    io:fwrite("Meta: ~p             Time: ~p,~n", [length(V2mb), T2mb]),
%%    io:fwrite("Exom: ~p             Time: ~p,~n", [length(V2xb), T2xb]),
%%    io:fwrite("lega: ~p             Time: ~p,~n", [length(V2lb), T2lb]),
%%
%%    io:fwrite("Arg : ~p             Datapoints,~n", [Arg3]),
%%    io:fwrite("Meta: ~p             Time: ~p,~n", [length(V3mb), T3mb]),
%%    io:fwrite("Exom: ~p             Time: ~p,~n", [length(V3xb), T3xb]),
%%    io:fwrite("lega: ~p             Time: ~p,~n", [length(V3lb), T3lb]),
%%
%%    io:fwrite("Arg : ~p             Datapoints,~n", [Arg4]),
%%    io:fwrite("Meta: ~p             Time: ~p,~n", [length(V4mb), T4mb]),
%%    io:fwrite("Exom: ~p             Time: ~p,~n", [length(V4xb), T4xb]),
%%    io:fwrite("lega: ~p             Time: ~p,~n", [length(V4lb), T4lb]),
%%
%%    io:fwrite("Arg : ~p             Datapoints,~n", [Arg5]),
%%    io:fwrite("Meta: ~p             Time: ~p,~n", [length(V5mb), T5mb]),
%%    io:fwrite("Exom: ~p             Time: ~p,~n", [length(V5xb), T5xb]),
%%    io:fwrite("lega: ~p             Time: ~p,~n", [length(V5lb), T5lb]),
%%
%%    io:fwrite("Arg : ~p             Datapoints,~n", [Arg6]),
%%    io:fwrite("Meta: ~p             Time: ~p,~n", [length(V6mb), T6mb]),
%%    io:fwrite("Exom: ~p             Time: ~p,~n", [length(V6xb), T6xb]),
%%    io:fwrite("lega: ~p             Time: ~p,~n", [length(V6lb), T6lb]),
%%
%%    io:fwrite("Arg : ~p             Datapoints,~n", [Arg7]),
%%    io:fwrite("Meta: ~p             Time: ~p,~n", [length(V7mb), T7mb]),
%%    io:fwrite("Exom: ~p             Time: ~p,~n", [length(V7xb), T7xb]),
%%    io:fwrite("lega: ~p             Time: ~p,~n", [length(V7lb), T7lb]),
%%
%%    io:fwrite("=========================================================~n"),
%%    io:fwrite("Test for Quickest for the arguments provided  (no DPs)   ~n"),
%%    io:fwrite("=========================================================~n"),
%%
%%    io:fwrite("---------------------------------------------------------~n"),
%%    io:fwrite("|    Metadata     |     Exometer     |      legacy      |~n"),
%%    io:fwrite("---------------------------------------------------------~n"),
%%    io:fwrite("|    ~p      |     ~p      |      ~p     |~n", [T1ma, T1xa, T1la]),
%%    io:fwrite("|    ~p      |     ~p      |      ~p     |~n", [T2ma, T2xa, T2la]),
%%    io:fwrite("|    ~p       |     ~p      |      ~p     |~n", [T3ma, T3xa, T3la]),
%%    io:fwrite("|    ~p       |     ~p      |      ~p     |~n", [T4ma, T4xa, T4la]),
%%    io:fwrite("|    ~p        |     ~p      |      ~p     |~n", [T5ma, T5xa, T5la]),
%%    io:fwrite("|    ~p         |     ~p      |      ~p     |~n", [T6ma, T6xa, T6la]),
%%    io:fwrite("|    ~p        |     ~p      |      ~p     |~n", [T7ma, T7xa, T7la]),
%%    io:fwrite("---------------------------------------------------------~n"),
%%
%%    io:fwrite("=========================================================~n"),
%%    io:fwrite("Test for Quickest for the arguments provided  (DPs)   ~n"),
%%    io:fwrite("=========================================================~n"),
%%
%%    io:fwrite("---------------------------------------------------------~n"),
%%    io:fwrite("|    Metadata     |     Exometer     |      legacy      |~n"),
%%    io:fwrite("---------------------------------------------------------~n"),
%%    io:fwrite("|    ~p        |     ~p      |      ~p     |~n", [T1mb, T1xb, T1lb]),
%%    io:fwrite("|    ~p        |     ~p      |      ~p     |~n", [T2mb, T2xb, T2lb]),
%%    io:fwrite("|    ~p         |     ~p      |      ~p     |~n", [T3mb, T3xb, T3lb]),
%%    io:fwrite("|    ~p         |     ~p      |      ~p     |~n", [T4mb, T4xb, T4lb]),
%%    io:fwrite("|    ~p          |     ~p      |      ~p     |~n", [T5mb, T5xb, T5lb]),
%%    io:fwrite("|    ~p           |     ~p      |      ~p     |~n", [T6mb, T6xb, T6lb]),
%%    io:fwrite("|    ~p          |     ~p      |      ~p     |~n", [T7mb, T7xb, T7lb]),
%%    io:fwrite("---------------------------------------------------------~n"),


%%    infotest(Arg).

%%infotest(Arg) ->
%%    {T0,V0} = timer:tc(fun infotestor/1, Arg),
%%    {TN,VN} = timer:tc(fun infotestnew/1,Arg),
%%    io:fwrite("Test results:~n"),
%%    io:fwrite("Original Test : ~p~n", [T0]),
%%    io:fwrite("New Test      : ~p~n", [TN]),
%%    V0s = term_to_binary(V0),
%%    VNs = term_to_binary(VN),
%%    SV0 = erlang:size(V0s),
%%    SVN = erlang:size(VNs),
%%    io:fwrite("Original Size Final  : ~p~n", [SV0]),
%%    io:fwrite("New Size Final       : ~p~n", [SVN]).
%%
%%infotestor(Arg) ->
%%    V1 = term_to_binary(Arg),
%%    V2 = erlang:size(V1),
%%    io:fwrite("Original Size Arg  : ~p~n", [V2]),
%%
%%    {Attr, Rest} = pick_info_attrs(Arg),
%%    V3 = term_to_binary(Rest),
%%    V4 = erlang:size(V3),
%%    io:fwrite("Original Size Rest : ~p~n", [V4]),
%%
%%    [{Stats, Match, DP}] = data_sanitise(Rest),
%%    V5 = term_to_binary([{Stats,Match,DP}]),
%%    V6 = erlang:size(V5),
%%    io:fwrite("Original Size data_: ~p~n", [V6]),
%%
%%    Found = lists:map(fun
%%                          ({Stat,        _Status}) ->       Stat;
%%                          ({Stat, _Type, _Status}) ->       Stat;
%%                          ({Stat, _Type, _Status, _DPs}) -> Stat
%%                      end,
%%        find_entries(Match, Stats)),
%%    V7 = term_to_binary(Found),
%%    V8 = erlang:size(V7),
%%    io:fwrite("Original Size Found: ~p~n", [V8]),
%%
%%    print_(Found, Attr).
%%
%%infotestnew(Arg) ->
%%    V1 = term_to_binary(Arg),
%%    V2 = erlang:size(V1),
%%    io:fwrite("New Size Arg  : ~p~n", [V2]),
%%
%%    {Attr, Rest} = pick_info_attrs(Arg),
%%    V3 = term_to_binary(Rest),
%%    V4 = erlang:size(V3),
%%    io:fwrite("New Size Rest : ~p~n", [V4]),
%%
%%    [{Stats, Match, DP}] = data_sanitise(Rest),
%%    V5 = term_to_binary([{Stats,Match,DP}]),
%%    V6 = erlang:size(V5),
%%    io:fwrite("New Size data_: ~p~n", [V6]),
%%
%%    Found = lists:map(fun
%%                          ({Stat,        _Status}) ->       {Stat, find_stat_info(Stat, Attr)};
%%                          ({Stat, _Type, _Status}) ->       {Stat, find_stat_info(Stat, Attr)};
%%                          ({Stat, _Type, _Status, _DPs}) -> {Stat, find_stat_info(Stat, Attr)}
%%                      end,
%%        find_entries(Match, Stats)),
%%    V7 = term_to_binary(Found),
%%    V8 = erlang:size(V7),
%%    io:fwrite("New Size Found: ~p~n", [V8]),
%%
%%    print_(Found, []).


%%print_([],_) ->
%%    io:fwrite("No matching stats (test)~n");
%%print_(Stats, []) ->
%%    lists:foreach(fun
%%                      ({Stat, Info}) ->
%%                          io:fwrite("~p : ~p~n", [Stat,Info])
%%                          end, Stats);
%%print_(Stats, Attr) ->
%%    lists:foreach(
%%        fun
%%            ({N,_,_}) ->
%%                print_info_1(N, Attr);
%%            ({N,_}) ->
%%                print_info_1(N, Attr);
%%            (N) ->
%%                print_info_1(N, Attr)
%%        end, Stats
%%    ).

%%
%%
%%% used to print the entire stat information
%%print_info_1(N, [A | Attrs]) ->
%%    Hdr = lists:flatten(io_lib:fwrite("~p: ~n", [N])),
%%    Pad = lists:duplicate(length(Hdr), $\s),
%%    Info = get_info(core, N),
%%    Status = proplists:get_value(status, Info, enabled),
%%    Body = [case proplists:get_value(A, Info) of
%%                undefined -> [];
%%                Other -> io_lib:fwrite("~w = ~p~n", [A, Other])
%%            end
%%        | lists:map(fun(value) ->
%%            io_lib:fwrite(Pad ++ "~w = ~p~n",
%%                [value, get_value(N, Status, default)]);
%%            (Ax) ->
%%                case proplists:get_value(Ax, Info) of
%%                    undefined -> [];
%%                    Other -> io_lib:fwrite(Pad ++ "~w = ~p~n",
%%                        [Ax, Other])
%%                end
%%                    end, Attrs)],
%%    io:put_chars([Hdr, Body]).

%%
%%get_info(Name, Info) ->
%%    case riak_core_stat_coordinator:get_info(Name, Info) of
%%        undefined ->
%%            [];
%%        Other ->
%%            Other
%%    end.

