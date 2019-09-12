%%%-------------------------------------------------------------------
%%% @doc
%%% Parsing data for profiles and console commands to the format used
%%% in the metadata and exometer.
%%%
%%% Console and profile commands input:
%%% [<<"riak.riak_kv.**">>] and [<<"test-profile-name">>]
%%%
%%% Parse the Data into an atom or list format.
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_data).
-include_lib("riak_core/include/riak_stat.hrl").

%% API
-export([
    data_sanitise/1,
    data_sanitise/2,
    data_sanitise/3,
    data_sanitise/4,
    print/1,
    print/2]).

-define(STATUS, enabled). %% default status
-define(TYPE,   '_').     %% default type
-define(DPs,    default). %% default Datapoints


%%%===================================================================
%%% API
%%%===================================================================

%% todo: make sure that if there is a legacy_stat come in it returns as
%% a legacy stat, as in {legacy,Stat} is whats returned.

%%%-------------------------------------------------------------------
%%% @doc
%%% Arguments coming in from the console or _stat modules arrive at
%%% this function, the data is transformed into a metrics name and type
%%% status/datapoints if they have been given.
%%% @end
%%%-------------------------------------------------------------------
-spec(data_sanitise(data()) -> sanitised()).
data_sanitise(Arg) ->
    data_sanitise(check_args(Arg), ?TYPE, ?STATUS, ?DPs).

data_sanitise(Arg, Status) when Status == enabled
    orelse Status == disabled orelse Status == '_' ->
    data_sanitise(check_args(Arg), ?TYPE, Status, ?DPs);
data_sanitise(Arg, Type) ->
    data_sanitise(check_args(Arg), Type, ?STATUS, ?DPs).

data_sanitise(Arg, Type, Status) ->
    data_sanitise(check_args(Arg), Type, Status, ?DPs).

data_sanitise(BinArgs, Type, Status, DPs) ->
    [Bin | Args] = re:split(BinArgs, "/"), %% separate type etc...
    {NewType, NewStatus, NewDPs} =
        type_status_and_dps(Args, Type, Status, DPs),
    StatName = statname(Bin),
    {StatName, NewStatus, NewType, NewDPs}.

type_status_and_dps([], Type, Status, DPs) ->
    {Type, Status, DPs};
type_status_and_dps([<<"type=", T/binary>> | Rest], _Type, Status, DPs) ->
    NewType =
        case T of
            <<"*">> -> '_';
            _ ->
                try binary_to_existing_atom(T, latin1)
                catch error:_ -> T
                end
        end,
    type_status_and_dps(Rest, NewType, Status, DPs);
type_status_and_dps([<<"status=", S/binary>> | Rest], Type, _Status, DPs) ->
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
    io:format("H: ~p, T: ~p, DPs : ~p~n",[H,T,DPs]),
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
                Error -> print("Parse error in ~p for ~p~n", [Expr, Error]), []
            end;
        Error -> print("Scan Error in ~p for ~p~n", [Expr, Error]), []
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
        ["**"] -> [R] ++ '_'; %% [riak,riak_kv|'_']
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
                    lager:error("Cannot replace part: ~p~n", [Error])
            end;
        [C | _] when C >= $0, C =< $9 ->
            try list_to_integer(H)
            catch
                error:_ -> list_to_atom(H)
            end;
        _ -> list_to_atom(H)
    end.

pads() ->
    [lists:duplicate(N, '_') || N <- lists:seq(1,15)].

%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% Print stats is generic, and used by both stat show and stat info,
%% Stat info includes all the attributes that will be printed whereas
%% stat show will pass in an empty list into the Attributes field.
%% @end
%%%-------------------------------------------------------------------
-spec(print(data(), attr() | term()) -> print()).
print([]) ->
    print("No Argument given~n",[]);
print(Elem) when is_list(Elem) ->
    print(Elem, []);


print(_) ->
    ok.


print([],_) ->
    io:fwrite("No Matching Stats~n");
%%print(Stats, DPs) ->
%%    lists:map(fun
%%                  ({Name, _Type, _Status}) when DPs == [] ->
%%                      io:fwrite("~p : ~p~n", [Name, get_values(Name)]);
%%                  ({Name, _Type, _Status}) ->
%%                      io:fwrite("~p : ~p~n",[Name, get_dps(Name, DPs)])
%%              end, Stats);
print(Stats, stats) ->
    lists:map(fun
                  ({N, _T, S}) ->
                      io:fwrite("{~p, ~p}~n", [N,S]);
                  ({Stat,Status}) when Status == enabled;
                      Status == disabled ->
                      io:format("~p : ~p~n",[Stat,Status]);
                  ({Stat, DPs}) ->
                      io:format("catrchallw1 ~p~n",[DPs]),

                      print_stat([Stat],DPs);
                  (Stat) ->
                      io:format("catrchallw2~n"),
                      print_stat([Stat],[])
              end, Stats);
print(Elem, [])   when is_list(Elem) ->
    io:fwrite(Elem);
print(Elem, Args) when is_list(Elem) ->
    io:fwrite(Elem, Args);


print(_i,_u) ->
    ok.

get_dps(Name, DataPoints) ->
    lists:map(fun
                  (D) ->
                      {Name, riak_stat_exom:get_datapoint(Name, D)}
              end, DataPoints).

get_values(Name) ->
    riak_stat_exom:get_values(Name).

print_stat(Stats, []) ->
    lists:map(fun
                  ({Stat,Type,Status,DPs}) ->
                      io:fwrite("{~p,~p,~p} : ~p~n",
                          [Stat,Type,Status,DPs]);
                  ({Stat, Type, Status}) ->
                      io:fwrite("{~p,~p,~p}~n",
                          [Stat,Type,Status]);
                  ({Stat,Status}) ->
                      io:fwrite("{~p,~p}~n",
                          [Stat,Status]);
                  (Stat) ->
                      io:fwrite("~p~n",[Stat])
              end, Stats);
print_stat(Stats, DPs) ->
    lists:map(fun
                  ({Stat,Type,Status}) ->
                      io:fwrite("{~p,~p,~p}: ~p~n",
                          [Stat,Type,Status,DPs]);
                  ({Stat,Status}) ->
                      io:fwrite("{~p,~p}: ~p~n",
                          [Stat,Status,DPs]);
                  (Stat) ->
                      io:fwrite("~p~n",[Stat])
              end, Stats).

%%%===================================================================
%%% Helper API
%%%===================================================================

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
%%
%%get_datapoint(Entry, DPs) ->
%%    riak_stat_exom:get_datapoint(Entry,DPs).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(setup(Fun),        {setup,    fun setup/0,          fun cleanup/1, Fun}).
-define(foreach(Funs),     {foreach,  fun setup/0,          fun cleanup/1, Funs}).
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
        [
            {"riak.**",                         fun tests_riak_star_star/0},
            {"riak.riak_kv.**",                 fun tests_riak_kv_star_star/0},
            {"riak.riak_kv.*",                  fun tests_riak_kv_star/0},
            {"riak.riak_kv.node.*",             fun tests_riak_kv_node_star/0},
            {"node_gets",                       fun tests_node_gets/0},
            {"riak.riak_kv.node.gets/max",      fun tests_node_gets_dp/0},
            {"riak.riak_kv.**/type=spiral",     fun tests_riak_kv_type_spiral/0},
            {"riak.riak_kv.**/status=disabled", fun tests_riak_kv_status_dis/0},
            {"true",                            fun tests_true/0}
        ]).

tests_riak_star_star() ->
    {Name,_T,_S,_DP} = data_sanitise(["riak.**"]),
    ?assertEqual([riak|'_'],Name).

tests_riak_kv_star_star() ->
    {Name,_T,_S,_DP} = data_sanitise(["riak.riak_kv.**"]),
    ?assertEqual([riak,riak_kv|'_'],Name).

tests_riak_kv_star() ->
    {Name,_T,_S,_DP} = data_sanitise(["riak.riak_kv.*"]),
    ?assertEqual(Name, [riak,riak_kv,'_']).

tests_riak_kv_node_star() ->
    {Name,_T,_S,_DP} = data_sanitise(["riak.riak_kv.node.*"]),
    ?assertEqual(Name, [riak,riak_kv,node,'_']).

tests_node_gets() ->
    {Name,_T,_S,_DP} = data_sanitise(["node_gets"]),
    ?assertEqual([node_gets],Name).

tests_node_gets_dp() ->
    {Name,_T,_S,DPs} = data_sanitise(["riak.riak_kv.node.gets/max"]),
    ?assertEqual(Name, [riak,riak_kv,node,gets]),
    ?assertEqual([max],DPs).

tests_riak_kv_type_spiral() ->
    {Name,Type,_St,_DP} = data_sanitise(["riak.riak_kv.**/type=spiral"]),
    ?assertEqual(Name, [riak,riak_kv|'_']),
    ?assertEqual(Type, spiral).

tests_riak_kv_status_dis() ->
    {Name,_Type,Status,_DP} = data_sanitise(["riak.riak_kv.**/status=disabled"]),
    ?assertEqual(Name, [riak,riak_kv|'_']),
    ?assertEqual(Status, disabled).

tests_true() ->
    {Arg,_t,_S,_D} = data_sanitise(["true"]),
    ?assertEqual(Arg, [true]).



-endif.
