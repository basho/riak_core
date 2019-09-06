%%%-------------------------------------------------------------------
%%% @doc
%%% Parsing data for profiles and console commands to the format used
%%% in the metadata and exometer.
%%%
%%% Console and profile commands input:
%%% [<<"riak.riak_kv.**">>] and [<<"test-profile-name">>]
%%%
%%% Additional Endpoint input data:
%%% [<<"udp.8080">>]
%%%
%%% Parse the Data into an atom or list format.
%%% @end
%%%-------------------------------------------------------------------
-module(riak_core_stat_data).

-include_lib("riak_core/include/riak_core_stat.hrl").

-export([
  data_sanitise/1,
    data_sanitise/4,
  print/2
]).

%% API
-export([
  format_time/1,
  utc_milliseconds_time/0,
  exo_timestamp/0,
  sanitise_data/1
]).

-define(SECS_MILLISECOND_DIVISOR,1000).
-define(MILLISECONDS_MICROSECONDS_DIVISOR,1000).

%%%-------------------------------------------------------------------
-spec(data_sanitise(data(),status(),type(),datapoint()) -> data()).
%% @doc
%% this is for data coming in from the console of format [<<"X">>]
%% or ["riak...."] etc.,
%% this one transforms the arg given into usable arguments in admin
%% console and metadata. Taken from legacy code 2013-2014
%% @end
%%data_sanitise(Arg, Status, Type, DPs) ->
%%    BinArgs = check_args(Arg),
%%    [Bin | Args] = re:split(BinArgs, "/"),
%%    %% Arguments are separated with "/"
%%    {NewType, NewStatus, NewDPs} =
%%        type_status_and_dps(Args, Type, Status, DPs),
%%    {Bin, NewStatus, NewType, NewDPs}.
%%
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




data_sanitise(Arg) ->
    BinArg = check_args(Arg),
    DefaultType     = '_',
    DefaultStatus   = enabled,
    DefaultDPs      = [],
    data_sanitise_(BinArg, DefaultType, DefaultStatus, DefaultDPs).

data_sanitise(Arg, Status, Type, DPs) ->
    data_sanitise_(check_args(Arg), Status, Type, DPs).

data_sanitise_(BinArg, Type, Status, DPs) ->
    [Bin | Arg] = re:split(BinArg, "/"),
%%     with "riak admin stat show ..." the separation of type, status and datapoints
%%     is with "/" -> riak.**/type=duration/status=*/mean,max
    {NewType, NewStatus, NewDps} = type_status_and_dps(Arg, Type, Status, DPs),
%%    {Stats, ExoMatchSpec, DatPs} =
        stats_and_matchspecs(Bin, NewType, NewStatus, NewDps).



stats_and_matchspecs([], Type, Status, DPs) -> % basic
    {[?PFX]++'_',
        [{{[?PFX]++'_', Type, '_'}, [{'=:=', '$status', Status}], ['$_']}], DPs};
stats_and_matchspecs("*", Type, Status, DPs) ->
    stats_and_matchspecs([], Type, Status, DPs);
stats_and_matchspecs("["++_ = Expr, _Type, _Status, _DPs) ->
    case erl_scan:string(ensure_trailing_dot(Expr)) of
        {ok, Toks, _} ->
            case erl_parse:parse_exprs(Toks) of
                {ok, [Abst]} -> partial_eval(Abst);
                Error -> print("Parse error in ~p for ~p~n", [Expr, Error]), []
            end;
        Error -> print("Scan Error in ~p for ~p~n", [Expr, Error]), []
    end;
stats_and_matchspecs(Data, Type, Status, DPs) when is_atom(Status)->
    Parts = re:split(Data, "\\.", [{return, list}]),
    Heads = replace_parts(Parts),
    {Heads,[{{H, Type, Status}, [], ['$_']} || H <- Heads], DPs};
stats_and_matchspecs(_Stat, _Type, Status, _DP) ->
    print("(Illegal status : ~p~n", [Status]).


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



partial_eval({cons, _, H, T}) ->
    [partial_eval(H) | partial_eval(T)];
partial_eval({tuple, _, Elems}) ->
    list_to_tuple([partial_eval(E) || E <- Elems]);
partial_eval({op, _, '++', L1, L2}) ->
    partial_eval(L1) ++ partial_eval(L2);
partial_eval(X) ->
    erl_parse:normalise(X).


ensure_trailing_dot(Str) ->
    case lists:reverse(Str) of
        "." ++ _ ->
            Str;
        _ ->
            Str ++ "."
    end.

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
    print(illegal).

print([]) ->
    io:fwrite("No Argument given~n");
print(illegal) ->
    io:fwrite("Illegal Argument Type given ~n").





-spec(print(data(), attr() | term()) -> print()).
%% @doc
%% Print stats is generic, and used by both stat show and stat info,
%% Stat info includes all the attributes that will be printed whereas stat show
%% will pass in an empty list into the Attributes field.
%% @end
print([], _) ->
    io:fwrite("No matching stats~n");
print({[{LP, []}], _}, _) ->
    io:fwrite("== ~s (Legacy pattern): No matching stats ==~n", [LP]);
print({[{LP, Matches}], _}, []) ->
    io:fwrite("== ~s (Legacy pattern): ==~n", [LP]),
    [[io:fwrite("~p: ~p (~p/~p)~n", [N, V, E, DP])
        || {DP, V, N} <- DPs] || {E, DPs} <- Matches];
print({[{LP, Matches}], _}, Attrs) ->
    io:fwrite("== ~s (Legacy pattern): ==~n", [LP]),
    lists:foreach(
        fun({N, _}) ->
            print_info_1(N, Attrs)
        end, Matches);
print({[], _}, _) ->
    io_lib:fwrite("No matching stats~n", []);
print({Entry, DP}, _) when is_atom(DP) ->
    io:fwrite("~p: ~p~n", [Entry, DP]);
print({Entries, DPs}, []) ->
    io:fwrite("~p:~n", [Entries]),
    [io:fwrite("~p: ~p~n", [DP, Data])
        || {DP, Data} <- DPs, Data =/= undefined, Data =/= []];
print({Entries, _}, Attrs) -> %% todo make this work please, remove print_info_1
    lists:foreach(
        fun
            ({N,_,_,_}) ->
                print_info_1(N, Attrs);
            ({N, _, _}) ->
                print_info_1(N, Attrs);
            ({N, _}) ->
                print_info_1(N, Attrs);
            (N) ->
                print_info_1(N, [])
        end, Entries);
print(Entries, []) when is_list(Entries) ->
    lists:map(fun
                  (Ent) when is_atom(Ent) ->
                      print({Entries, []}, []);
                  ({Stat, Status}) when is_atom(Status) ->
                      print({Stat, Status}, []);
                  (Ent) ->
                      print(Ent, [name, status])
              end, Entries);
print(Data, Att) ->
    io:fwrite("~p:~p~n", [Data, Att]).
%%    print({Data, []}, Att).

get_value(_, disabled, _) ->
    disabled;
get_value(E, _Status, DPs) ->
    case get_datapoint(E, DPs) of
        {ok, V} -> V;
        {error, _} -> unavailable
    end.

get_datapoint(E, DPs) ->
  riak_core_stat_exometer:get_datapoint(E,DPs).


% used to print the entire stat information
print_info_1(N, [A | Attrs]) ->
  Hdr = lists:flatten(io_lib:fwrite("~p: ~n", [N])),
  Pad = lists:duplicate(length(Hdr), $\s),
  Info = get_info(core, N),
  Status = proplists:get_value(status, Info, enabled),
  Body = [case proplists:get_value(A, Info) of
            undefined -> [];
            Other -> io_lib:fwrite("~w = ~p~n", [A, Other])
          end
    | lists:map(fun(value) ->
      io_lib:fwrite(Pad ++ "~w = ~p~n",
        [value, get_value(N, Status, default)]);
      (Ax) ->
        case proplists:get_value(Ax, Info) of
          undefined -> [];
          Other -> io_lib:fwrite(Pad ++ "~w = ~p~n",
            [Ax, Other])
        end
                end, Attrs)],
  io:put_chars([Hdr, Body]).


get_info(Name, Info) ->
  case riak_core_stat_coordinator:get_info(Name, Info) of
    undefined ->
      [];
    Other ->
      Other
  end.

%%--------------------------------------------------------------------

-spec(sanitise_data(arg()) -> sanitised_data()).
%% @doc
%% Sanitise the data coming in, into the necessary arguments for setting
%% up an endpoint, This is specific to the endpoint functions
%% @end
sanitise_data(Arg) ->
  [Opts | Stats] = break_up(Arg, "/"),
  List = break_up(Opts, "\\s"),
  NewStats =
    case Stats of
      [] -> ['_'];
      Data -> Data
    end,
  {sanitise_data_(List), NewStats}.

break_up(Arg, Str) ->
  re:split(Arg, Str, []).

sanitise_data_(Arg) ->
  sanitise_data_(Arg, ?MONITOR_STATS_PORT, ?INSTANCE, ?MONITOR_SERVER).
sanitise_data_([<<"port=", Po/binary>> | Rest], Port, Instance, Sip) ->
  NewPort =
    case binary_to_integer(Po) of
      {error, _reason} -> Port;
      Int -> Int
    end,
  sanitise_data_(Rest, NewPort, Instance, Sip);
sanitise_data_([<<"instance=", I/binary>> | Rest], Port, _Instance, Sip) ->
  NewInstance = binary_to_list(I),
  sanitise_data_(Rest, Port, NewInstance, Sip);
sanitise_data_([<<"sip=", S/binary>> | Rest], Port, Instance, _Sip) ->
  NewIP = re:split(S, "\\s", [{return, list}]),
  sanitise_data_(Rest, Port, Instance, NewIP);
sanitise_data_([], Port, Instance, Sip) ->
  {Port, Instance, Sip}.



%%--------------------------------------------------------------------
-spec(format_time(erlang:timestamp()) -> string()).
%% @doc
%% For a given timestamp, returns the string representation in the following
%% format, YYYY.MM.ddTHH:mm:ss.SSSZ the time is in UTC.
%% @end
format_time({ _, _, MicroSeconds} = Now) ->
  {{Year, Month, Day},{ Hour, Min, Sec}} = calendar:now_to_universal_time(Now),
  lists:flatten(io_lib:format("~.4.0w-~.2.0w-~.2.0wT~.2.0w:~.2.0w:~.2.0w.~.3.0wZ",
    [Year, Month, Day, Hour, Min, Sec, MicroSeconds div ?MILLISECONDS_MICROSECONDS_DIVISOR])).

-define(MEGASECS_MILLISECOND_DIVISOR,1000000000).

%%--------------------------------------------------------------------
-spec(utc_milliseconds_time() -> term()).
%% @doc
%% Gets the utc time in milliseconds.
%% @end
utc_milliseconds_time() ->
  {Mega, Sec, Micro} = os:timestamp(),
  (Mega * ?MEGASECS_MILLISECOND_DIVISOR) + (Sec * ?SECS_MILLISECOND_DIVISOR) + (Micro div ?MILLISECONDS_MICROSECONDS_DIVISOR).

%%--------------------------------------------------------------------
-spec(exo_timestamp() -> term()).
%% @doc
%% timestamp format used with exometer_histogram and exometer_slide
%% @end
exo_timestamp() ->
  riak_core_stat_exometer:timestamp().
