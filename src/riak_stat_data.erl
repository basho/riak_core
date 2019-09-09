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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%%===================================================================
%%% API
%%%===================================================================

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
    NewDPs = merge(
        [binary_to_existing_atom(D, latin1) || D <- re:split(DPsBin, ",")],
        DPs), %% datapoints are separated by ","
    type_status_and_dps(Rest, Type, Status, NewDPs).

merge([_ | _] = DPs, []) ->
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
    io_lib:fwrite("No Matching Stats~n");
print(Stats, stats) ->
    lists:map(fun
                  ({N, _T, S}) ->
                      io:fwrite("{~p, ~p}~n", [N,S]);
                  ({Stat, DPs}) ->
                      print_stat(Stat,DPs)
              end, Stats);
print(Elem, [])   when is_list(Elem) ->
    io_lib:fwrite(Elem);
print(Elem, Args) when is_list(Elem) ->
    io_lib:fwrite(Elem, Args);


print(_i,_u) ->
    ok.

print_stat(Stats, []) ->
    lists:map(fun
                  ({Stat, Type, Status}) ->
                      io_lib:fwrite("{~p,~p,~p}~n",
                          [Stat,Type,Status])
              end, Stats);
print_stat(Stats, DPs) ->
    lists:map(fun
                  ({Stat,Type,Status}) ->
                      io_lib:fwrite("{~p,~p,~p}: ~p~n",
                          [Stat,Type,Status,DPs])
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

-endif.
