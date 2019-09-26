%%%-------------------------------------------------------------------
%%% @doc
%%% Commands for "riak admin stat ___ ...." call into this module from
%%% riak_core_console. the purpose of these console commands is to
%%% display to the user all the information they want about an entry
%%% or entries, then the stats can be configured/updated in the
%%% metadata/exometer directly.
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
    print/1,
    print/2
]).

-define(STATUS, enabled). %% default status
-define(TYPE,   '_').     %% default type
-define(DPs,    default). %% default Datapoints

%%%===================================================================
%%% API
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%% riak admin stat show
%%      <entry>/type=(type())/status=(enabled|disabled|*)/[dps].
%% Show enabled or disabled stats
%% when using riak-admin stat show riak.** enabled stats will
%% show by default
%%
%% otherwise use: riak-admin stat show <entry>/status=* | disabled
%% @end
%%%-------------------------------------------------------------------
-spec(show_stat(arg()) -> statslist()).
show_stat(Arg) ->
    print(find_entries(data_sanitise(Arg))).

%%%-------------------------------------------------------------------
%% @doc
%% Check which stats in exometer are not updating (checks enabled)
%% @end
%%%-------------------------------------------------------------------
-spec(show_stat_0(data()) -> value()).
show_stat_0(Arg) ->
    {Stats,_Status,Type,DPs}=data_sanitise(Arg),
    print({[find_entries({Stats,enabled,Type,DPs})],show_0}).

%%%-------------------------------------------------------------------
%% @doc
%% Returns all the stats information
%% @end
%%%-------------------------------------------------------------------
-spec(stat_info(data()) -> value()).
stat_info(Arg) ->
    {Attrs, RestArg} = pick_info_attrs(Arg),
    {Stat,Type,Status,_DPS} = data_sanitise(RestArg),
    print(find_entries({Stat,Type,Status,Attrs})).

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
            {?INFOSTAT, Rest};
        Other ->
            Other
    end.

split_arg(Str) ->
    re:split(Str, "\\s", [{return, list}]).

%%%-------------------------------------------------------------------
%% @doc
%% Similar to the function above, but will disable all the stats that
%% are not updating
%% @end
%%%-------------------------------------------------------------------
-spec(disable_stat_0(data()) -> ok).
disable_stat_0(Arg) ->
    {Stats,_Status,Type,DPs} = data_sanitise(Arg),
    print({[find_entries({Stats,enabled,Type,DPs})],disable_0}).

%%%-------------------------------------------------------------------
%% @doc
%% change the status of the stat (in metadata and) in exometer
%% @end
%%%-------------------------------------------------------------------
-spec(status_change(data(), status()) -> ok).
status_change(Arg, ToStatus) ->
    {Entries,_DP} = % if disabling stats, pull only enabled ones
    case ToStatus of
        enabled  -> find_entries(data_sanitise(Arg, '_', disabled));
        disabled -> find_entries(data_sanitise(Arg, '_', enabled))
    end,
    change_status([{Stat, {status, ToStatus}} ||
        {Stat,_,_} <- Entries]).

%%%-------------------------------------------------------------------
%% @doc
%% resets the stats in metadata and exometer and tells metadata that
%% the stat has been reset
%% @end
%%%-------------------------------------------------------------------
-spec(reset_stat(data()) -> ok).
reset_stat(Arg) ->
    {Found, _DPs} = find_entries(data_sanitise(Arg)),
    reset_stats([N || {N,_,_} <-Found]).

%%%-------------------------------------------------------------------
%% @doc
%% enabling the metadata allows the stats configuration and the stats
%% values to be persisted, disabling the metadata returns riak to its
%% original functionality of only using the exometer functions.
%% Enabling and disabling the metadata occurs here, directing the
%% stats and function work occurs in the riak_stat_coordinator
%% @end
%%%-------------------------------------------------------------------
-spec(enable_metadata(data()) -> ok).
enable_metadata(Arg) ->
    Truth = ?IS_ENABLED(?METADATA_ENABLED),
    {A,_,_,_} = data_sanitise(Arg),
    [C] = lists:flatten(A),
    case C of
        Truth ->
            print("Metadata-enabled already set to ~s~n", [Arg]);
        Bool ->
            case Bool of
                true ->
                    riak_stat_mgr:reload_metadata(
                        riak_stat_exom:find_entries([riak])),
                    application:set_env(riak_core,
                        ?METADATA_ENABLED, Bool);
                false ->
                    application:set_env(riak_core,
                        ?METADATA_ENABLED, Bool);
                Other ->
                    print("Wrong argument entered: ~p~n", [Other])
            end
    end.

%%%===================================================================
%%% Helper API
%%%===================================================================
%%%-------------------------------------------------------------------
%%% @doc
%%% Arguments coming in from the console or _stat modules arrive at
%%% this function, data is transformed into a metrics name and type
%%% status/datapoints if they have been given.
%%% @end
%%%-------------------------------------------------------------------
-spec(data_sanitise(data()) -> sanitised()).
data_sanitise(Arg) ->
    parse_stat_entry(check_args(Arg), ?TYPE, ?STATUS, ?DPs).

%% separate Status from Type with fun of same arity
data_sanitise(Arg, Status)
    when   Status == enabled
    orelse Status == disabled
    orelse Status == '_' ->
    parse_stat_entry(check_args(Arg), ?TYPE, Status, ?DPs);

data_sanitise(Arg, Type) ->
    parse_stat_entry(check_args(Arg), Type, ?STATUS, ?DPs).

data_sanitise(Arg, Type, Status) ->
    parse_stat_entry(check_args(Arg), Type, Status, ?DPs).
%%%-------------------------------------------------------------------

check_args([]) ->
    print([]);
check_args([Args]) when is_atom(Args) ->
    check_args(atom_to_binary(Args, latin1));
check_args([Args]) when is_list(Args) ->
    check_args(list_to_binary(Args));
check_args([Args]) when is_binary(Args) ->
    [Args];

check_args(Args) when is_atom(Args) ->
    check_args(atom_to_binary(Args, latin1));
check_args(Args) when is_list(Args) ->
    check_args(lists:map(fun
                             (A) when is_atom(A) ->
                                 atom_to_binary(A, latin1);
                             (A) when is_list(A) ->
                                 list_to_binary(A);
                             (A) when is_binary(A) ->
                                 A
                         end, Args));
check_args(Args) when is_binary(Args) ->
    [Args];
check_args(_) ->
    print("Illegal Argument Type ~n"), [].
%%%-------------------------------------------------------------------

parse_stat_entry(BinArgs, Type, Status, DPs) ->
    [Bin | Args] = re:split(BinArgs, "/"), %% separate type etc...
    {NewType, NewStatus, NewDPs} =
        type_status_and_dps(Args, Type, Status, DPs),
    StatName = statname(Bin),
    {StatName, NewStatus, NewType, NewDPs}.

%% legacy code \/
type_status_and_dps([], Type, Status, DPs) ->
    {Type, Status, DPs};
type_status_and_dps([<<"type=", T/binary>> | Rest], _T,Status,DPs) ->
    NewType =
        case T of
            <<"*">> -> '_';
            _ ->
                try binary_to_existing_atom(T, latin1)
                catch error:_ -> T
                end
        end,
    type_status_and_dps(Rest, NewType, Status, DPs);
type_status_and_dps([<<"status=", S/binary>> | Rest],Type,_St,DPs) ->
    NewStatus =
        case S of
            <<"*">> -> '_';
            <<"enabled">>  -> enabled;
            <<"disabled">> -> disabled
        end,
    type_status_and_dps(Rest, Type, NewStatus, DPs);
type_status_and_dps([DPsBin | Rest], Type, Status, DPs) ->
    Atoms =
        lists:map(fun(D) ->
            try binary_to_existing_atom(D, latin1) of
                DP -> DP
            catch _:_ ->
                io:fwrite("Illegal datapoint name~n"),[]
            end
                  end, re:split(DPsBin,",")),
    NewDPs = merge(lists:flatten(Atoms),DPs),
    type_status_and_dps(Rest, Type, Status, NewDPs).

merge([_ | _] = DPs, default) ->
    DPs;
merge([H | T], DPs) ->
    case lists:member(H, DPs) of
        true -> merge(T, DPs);
        false -> merge(T, DPs ++ [H])
    end;
merge([], DPs) ->
    DPs.

statname([]) ->
    [riak_stat:prefix()] ++ '_' ;
statname("*") ->
    statname([]);
statname("["++_ = Expr) -> %% legacy code?
    case erl_scan:string(ensure_trailing_dot(Expr)) of
        {ok, Toks, _} ->
            case erl_parse:parse_exprs(Toks) of
                {ok, [Abst]} -> partial_eval(Abst);
                Error -> print("Parse error in ~p for ~p~n",
                             [Expr, Error]), []
            end;
        Error -> print("Scan Error in ~p for ~p~n",
                        [Expr, Error]), []
    end;
statname(Arg) when is_binary(Arg) ->
    Parts = re:split(Arg, "\\.", [{return,list}]),
    replace_parts(Parts);

statname(_) ->
    print("Illegal Argument Type in riak_stat_data:statname~n").

ensure_trailing_dot(Str) ->
    case lists:reverse(Str) of
        "." ++ _ ->
            Str;
        _ ->
            Str ++ "."
    end.

partial_eval({cons, _, H, T}) ->
    [partial_eval(H) | partial_eval(T)];
partial_eval({tuple, _, Elems}) ->
    list_to_tuple([partial_eval(E) || E <- Elems]);
partial_eval({op, _, '++', L1, L2}) ->
    partial_eval(L1) ++ partial_eval(L2);
partial_eval(X) ->
    erl_parse:normalise(X).

replace_parts(Parts) ->
    case split(Parts, "**", []) of
        {_, []} ->
            [replace_parts_1(Parts)];
        {Before, After} ->
            Head = replace_parts_1(Before),
            Tail = replace_parts_1(After),
            [Head ++ Pad ++ Tail || Pad <- pads()]
        %% case of "**" in between elements in metric name
    end.

split([H | T], H, Acc) ->
    {lists:reverse(Acc), T};
split([H | T], X, Acc) ->
    split(T, X, [H | Acc]);
split([], _, Acc) ->
    {lists:reverse(Acc), []}.

replace_parts_1([H | T]) ->
    R = replace_part(H),
    case T of
        '_' -> '_';
        "**" -> [R] ++ '_';
        ["**"] -> [R] ++ '_'; %% [riak|'_']
        _ -> [R | replace_parts_1(T)]
    end;
replace_parts_1([]) ->
    [].

replace_part(H) ->
    case H of
        '_' -> '_';
        "*" -> '_';
        "'" ++ _ ->
            case erl_scan:string(H) of
                {ok, [{atom, _, A}], _} ->
                    A;
                Error ->
                    lager:error("Cannot replace part: ~p~n",
                        [Error])
            end;
        [C | _] when C >= $0, C =< $9 ->
            try list_to_integer(H)
            catch
                error:_ -> list_to_atom(H)
            end;
        _ -> list_to_atom(H)
    end.

pads() ->
    [lists:duplicate(N, '_') || N <- lists:seq(1,10)].

%%%-------------------------------------------------------------------
%% @doc
%% Find_entries for the stat show/show-0/info, each one will use
%% find_entries to print a stats information. specific for show-0 and
%% different for info, stat show is the generic base in which it was
%% created
%% @end
%%%-------------------------------------------------------------------
-spec(find_entries(statname(),status(),type(),datapoint()) -> stats()).
find_entries({Stat,Status,Type,DPs}) ->
    find_entries(Stat,Status,Type,DPs).
find_entries(Stats,Status,Type,default) ->
    find_entries(Stats,Status,Type,[]);
find_entries(Stats,Status,Type,DPs) ->
    riak_stat_mgr:find_entries(Stats,Status,Type,DPs).

%%%-------------------------------------------------------------------

change_status(Stats) ->
    riak_stat_mgr:change_status(Stats).

reset_stats(Name) ->
    riak_stat_mgr:reset_stat(Name).

%%%-------------------------------------------------------------------
%% @doc
%% Print stats is generic, and used by both stat show and stat info,
%% Stat info includes all the attributes that will be printed whereas
%% stat show will pass in an empty list into the Attributes field.
%% @end
%%%-------------------------------------------------------------------
print(undefined) ->
    print([]);
print([undefined]) ->
    print([]);
print({Stats,DPs}) ->
    print(Stats,DPs);
print(Arg) ->
    print(Arg, []).

print([], _) ->
    io:fwrite("No Matching Stats~n");
print(NewStats,DPs) ->
    lists:map(fun
          ({Names, _NDPs}) when DPs == disable_0 ->
              case lists:flatten([not_0(N, dis) ||
                  {N, _, _} <- Names]) of

                  [] -> print([]);
                  _V -> ok
              end;
          ({Names, NDPs}) when DPs == show_0 ->
              case lists:flatten([not_0(N, NDPs) ||
                  {N, _, _} <- Names]) of

                  [] -> print([]);
                  V -> io:fwrite("~p: ~p~n", [Names, V])
              end;

          ({N, _T, _S}) when DPs == [] -> get_value(N);
          ({N, _T, _S}) ->  find_stats_info(N, DPs);

          %% legacy pattern
          (Legacy) ->
              lists:map(fun
                            ({LP, []}) ->
                                io:fwrite(
                                    "== ~s (Legacy pattern): No matching stats ==~n",
                                    [LP]);
                            ({LP, Matches}) ->
                                io:fwrite("== ~s (Legacy pattern): ==~n",
                                    [LP]),
                                [[io:fwrite("~p: ~p (~p/~p)~n",
                                    [N, V, E, DP]) ||
                                    {DP, V, N} <- LDPs] ||
                                    {E, LDPs} <- Matches];
                            (_) ->
                                []
                        end, Legacy)
      end, NewStats).

not_0(StatName,dis) ->
    case not_0_(StatName,[]) of
        [] -> [];
        _Vals -> change_status([{StatName,{status,disabled}}])
    end;
not_0(StatName,DPs) ->
    not_0_(StatName,DPs).

not_0_(Stat, _DPs) ->
    case riak_stat_exom:get_value(Stat) of
        {ok, V} ->
            lists:foldl(fun
                            ({Va,0},Acc) -> [{Va,0}|Acc];
                            ({Va,[]},Acc) ->[{Va,0}|Acc];
                            ({_Va,{error,_}},Acc)->Acc;
                            (_, Acc) -> Acc
                        end, [], V);
        _ -> []
    end.

%%%-------------------------------------------------------------------

get_value(N) ->
    case riak_stat_exom:get_value(N) of
        {ok,Val} ->
            case lists:foldl(fun
                          ({_,{error,_}},A) -> A;
                          (D,A) ->
                              [D|A]
                      end, [],Val) of
                [] -> [];
                R ->  io:fwrite("~p : ~p ~n",[N,R])
            end;
        {error, _} -> []
    end.

find_stats_info(Stats, Info) ->
    case riak_stat_exom:get_datapoint(Stats, Info) of
        [] -> [];
        {ok, V} ->
            case lists:foldl(fun
                          ([],A) -> A;
                          ({_DP, undefined},A) -> A;
                          ({_DP, {error,_}},A) -> A;
                          (DP,A) -> [DP|A]
                      end, [], V) of
                [] -> [];
                O  -> io:fwrite("~p : ~p~n",[Stats,O])
            end;
        {error,_} -> get_info_2_electric_boogaloo(Stats, Info)
    end.

get_info_2_electric_boogaloo(N,Attrs) ->
    case lists:foldl(fun
                  (undefined,A) -> A;
                  ([],A) -> A;
                  ({_,{error,_ }},A) -> A;
                  (D,A) -> [D|A]
              end, [],[riak_stat_exom:get_info(N,Attrs)]) of
        [] -> [];
        O  -> io:fwrite("~p : ~p~n",[N,O])
    end.


%%%===================================================================
%%%===================================================================



-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([data_sanitise_test/0]).

-define(setup(Fun),        {setup,  fun setup/0,fun cleanup/1, Fun}).
-define(foreach(Funs),     {foreach,fun setup/0,fun cleanup/1, Funs}).
-define(setuptest(Desc, Test), {Desc, ?setup(fun(_) -> Test end)}).

-define(new(Mod),                   meck:new(Mod)).
-define(unload(Mod),                meck:unload(Mod)).


setup() ->
    ?unload(riak_stat_data),
    ?new(riak_stat_data).

cleanup(_Pid) ->
    catch?unload(riak_stat_data),
    ok.

data_sanitise_test() ->
    ?setuptest("Data Sanitise test",
    [{"riak.**",                         fun tests_riak_star_star/0},
     {"riak.riak_kv.**",                 fun tests_riak_kv_star_star/0},
     {"riak.riak_kv.*",                  fun tests_riak_kv_star/0},
     {"riak.riak_kv.node.*",             fun tests_riak_kv_node_star/0},
     {"node_gets",                       fun tests_node_gets/0},
     {"riak.riak_kv.node.gets/max",      fun tests_node_gets_dp/0},
     {"riak.riak_kv.**/type=spiral",     fun tests_riak_kv_type_spiral/0},
     {"riak.riak_kv.**/status=disabled", fun tests_riak_kv_status_dis/0},
     {"true",                            fun tests_true/0}        ]).

tests_riak_star_star() ->
    {Name,_T,_S,_DP} =
        data_sanitise(["riak.**"]),
    ?assertEqual([riak|'_'],Name).

tests_riak_kv_star_star() ->
    {Name,_T,_S,_DP} =
        data_sanitise(["riak.riak_kv.**"]),
    ?assertEqual([riak,riak_kv|'_'],Name).

tests_riak_kv_star() ->
    {Name,_T,_S,_DP} =
        data_sanitise(["riak.riak_kv.*"]),
    ?assertEqual(Name, [riak,riak_kv,'_']).

tests_riak_kv_node_star() ->
    {Name,_T,_S,_DP} =
        data_sanitise(["riak.riak_kv.node.*"]),
    ?assertEqual(Name, [riak,riak_kv,node,'_']).

tests_node_gets() ->
    {Name,_T,_S,_DP} =
        data_sanitise(["node_gets"]),
    ?assertEqual([node_gets],Name).

tests_node_gets_dp() ->
    {Name,_T,_S,DPs} =
        data_sanitise(["riak.riak_kv.node.gets/max"]),
    ?assertEqual(Name, [riak,riak_kv,node,gets]),
    ?assertEqual([max],DPs).

tests_riak_kv_type_spiral() ->
    {Name,Type,_St,_DP} =
        data_sanitise(["riak.riak_kv.**/type=spiral"]),
    ?assertEqual(Name, [riak,riak_kv|'_']),
    ?assertEqual(Type, spiral).

tests_riak_kv_status_dis() ->
    {Name,_Type,Status,_DP} =
        data_sanitise(["riak.riak_kv.**/status=disabled"]),
    ?assertEqual(Name, [riak,riak_kv|'_']),
    ?assertEqual(Status, disabled).

tests_true() ->
    {Arg,_t,_S,_D} =
        data_sanitise(["true"]),
    ?assertEqual(Arg, [true]).

-endif.
