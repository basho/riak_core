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
  data_sanitise/3,
  print/2
]).

-spec(data_sanitise(data()) -> data()).
%% @doc
%% this is for data coming in from the console of format [<<"X">>],
%% this one transforms the arg given into usable arguments in admin
%% console and metadata. Taken from legacy code 2013-2014
%% @end
data_sanitise(Data) ->
  data_sanitise(Data, '_', enabled).
data_sanitise([Da | Ta], Type, Status) when is_list(Da) ->
  Dat = list_to_binary(Da),
  A = lists:map(fun(Stat) -> list_to_binary(Stat) end, Ta),
  data_sanitise([Dat | A], Type, Status);
data_sanitise([Da | Ta], Type, Status) when is_atom(Da)  ->
  Data = lists:map(fun
                     ("**") -> <<"**">>;
                     ("*")  -> <<"*">>;
                     (D) -> atom_to_binary(D, latin1)
                   end, [Da|Ta]),
  data_sanitise(Data, Type, Status);
data_sanitise(Data, Type, Status) when is_binary(Data) ->
  data_sanitise([Data], Type, Status);
data_sanitise(Data, Type, Status) when is_atom(Data) ->
  data_sanitise([atom_to_binary(Data, latin1)], Type, Status);
data_sanitise(Data, Type, Status) when is_list(Data) ->
%%    io:format("Data in sanitise :~p~n", [Data]),
  lists:map(fun(D) ->
    data_sanitise_(D, Type, Status)
            end, Data).


data_sanitise_(Data, Type, Status) ->
  [Stat | Est] = re:split(Data, "/"),
  % [<<"riak.riak_kv.*.gets.**">> | <<"status=*">>]
  io:format("Data : ~p -> Stat : ~p, Est : ~p~n", [Data, Stat, Est]),
  {Type, Status, DPs} = type_status_and_dps(Est, Type, Status, default),
  io:format("Type : ~p -> Status : ~p, DPs : ~p~n", [Type, Status, DPs]),
  {Names, MatchSpecs} = stat_entries(Stat, Type, Status),
  io:format("Name : ~p, Matchspec : ~p, Dps : ~p~n", [Names, MatchSpecs, DPs]),
  {Names, MatchSpecs, DPs}.


%% @doc
%% when /status=*, /type=* or /datapoints is given it can be extracted out
%% @end
type_status_and_dps([<<"type=", T/binary>> | Rest], _Type, Status, DPs) ->
%%    io:format("types typestatusnaddps~n"),
  NewType =
    case T of
      <<"*">> -> '_';
      _ ->
        try binary_to_existing_atom(T, latin1)
        catch error:_ ->T
        end
    end, type_status_and_dps(Rest, NewType, Status, DPs);
type_status_and_dps([<<"status=", S/binary>> | Rest], Type, _Status, DPs) ->
%%    io:format("status typesatusanddps~n"),
  NewStatus =
    case S of
      <<"*">> -> '_';
      <<"enabled">> ->  enabled;
      <<"disabled">> -> disabled
    end, type_status_and_dps(Rest, Type, NewStatus, DPs);
type_status_and_dps([DPsBin | Rest], Type, Status, DPs) ->
%%    io:format("dps typestatusanddps~n"),
  NewDPs = merge(
    [binary_to_existing_atom(D, latin1) || D <- re:split(DPsBin, ",")],
    DPs),
  type_status_and_dps(Rest, Type, Status, NewDPs);
type_status_and_dps([], Type, Status, DPs) ->
%%    io:format("empty typestatusanddps~n"),
  {Type, Status, DPs}.

merge([_ | _] = DPs, default) ->
  DPs;
merge([H | T], DPs) ->
  case lists:member(H, DPs) of
    true -> merge(T, DPs);
    false -> merge(T, DPs ++ [H])
  end;
merge([], DPs) ->
  DPs.

%% @doc
%% Input is a name of a stat(s) in binary
%% output is the name of a stat or several in list form
%% [<<"riak,riak_kv,*.gets.**">>] -> [riak,riak_kv,'_',gets,'_','_',...]
%% @end
stat_entries([], Type, Status) ->
  {[],[{{[?PFX]++'_', Type, '_'}, [{'=:=', '$status', Status}], ['$_']}]};
stat_entries("*", Type, Status) ->
  stat_entries([], Type, Status);
stat_entries("[" ++ _ = Expr, _Type, _Status) ->
  case erl_scan:string(ensure_trailing_dot(Expr)) of
    {ok, Toks, _} ->
      case erl_parse:parse_exprs(Toks) of
        {ok, [Abst]} ->
          partial_eval(Abst);
        Error ->
          io:fwrite("(Parse error for ~p: ~p~n", [Expr, Error]),
          []
      end;
    ScanErr ->
      io:fwrite("(Scan error for ~p: ~p~n", [Expr, ScanErr]),
      []
  end; %% legacy Code
stat_entries(Data, Type, Status) when is_atom(Status) ->
%%    io:format("stat_entries ----- Data : ~p ~n", [Data]),
  Parts = re:split(Data, "\\.", [{return, list}]),
  io:format("stat_entries -----Parts : ~p ~n", [Parts]),
  Heads = replace_parts(Parts),
  io:format("stat_entries -----Heads : ~p ~n", [Heads]),
  {Heads,[{{H, Type, Status}, [], ['$_']} || H <- Heads]};
stat_entries(_Stat, _Type, Status) ->
%%    io:format("stat_entries illegal~n"),
  io:fwrite("(Illegal status : ~p~n", [Status]).

replace_parts(Parts) ->
%%    io:format("replace parts: ~p~n", [Parts]),
  case split(Parts, "**", []) of
    {_, []} ->
      io:format("replace_parts {_,[]}~n"),

      [replace_parts_1(Parts)];
    {Before, After} ->
      io:format("replace_parts {Before,After}~n"),
      io:format("Before:~p, After:~p~n", [Before, After]),

      Head = replace_parts_1(Before),
      Tail = replace_parts_1(After),
      [Head ++ Pad ++ Tail || Pad <- pads()]
  end.

split([H | T], H, Acc) ->
  {lists:reverse(Acc), T};
split([H | T], X, Acc) ->
  split(T, X, [H | Acc]);
split([], _, Acc) ->
%%    io:format("split~n"),

  {lists:reverse(Acc), []}.


replace_parts_1([H | T]) ->
%%    io:format("replace parts again [~p | ~p]~n", [H,T]),
  R = replace_part(H),
  case T of
    '_' -> '_';
    "**" -> [R] ++ ['_'];
    ["**"] -> [R] ++ ['_'];
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
    || {DP, Data} <- DPs];
print({Entries, _}, Attrs) ->
  lists:foreach(
    fun({N, _, _}) ->
      print_info_1(N, Attrs)
    end, Entries);
print(Entries, []) when is_list(Entries) ->
  lists:map(fun
              (Ent) when is_atom(Ent) ->
                print({Entries, []}, []);
              ({Stat, Status}) when is_atom(Status) ->
                print({Stat, Status}, stat);
              (Ent) ->
                print(Ent, [name, status])
            end, Entries);
print(Data, Att) ->
  print({[{Data, [], []}], []}, Att).

get_value(_, disabled, _) ->
  disabled;
get_value(E, _Status, DPs) ->
  case get_datapoint(E, DPs) of
    {ok, V} -> V;
    {error, _} -> unavailable
  end.

get_datapoint(E, DPs) ->
  riak_stat_coordinator:get_datapoint(E,DPs).


% used to print the entire stat information
print_info_1(N, [A | Attrs]) ->
  Hdr = lists:flatten(io_lib:fwrite("~p: ", [N])),
  Pad = lists:duplicate(length(Hdr), $\s),
  Info = get_info(core, N),
  Status = proplists:get_value(status, Info, enabled),
  Body = [io_lib:fwrite("~w = ~p~n", [A, proplists:get_value(A, Info)])
    | lists:map(fun(value) ->
      io_lib:fwrite(Pad ++ "~w = ~p~n",
        [value, get_value(N, Status, default)]);
      (Ax) ->
        io_lib:fwrite(Pad ++ "~w = ~p~n",
          [Ax, proplists:get_value(Ax, Info)])
                end, Attrs)],
  io:put_chars([Hdr, Body]).


get_info(Name, Info) ->
  case riak_stat_coordinator:get_info(Name, Info) of
    undefined ->
      [];
    Other ->
      Other
  end.

%% API
-export([
  format_time/1,
  utc_milliseconds_time/0,
  exo_timestamp/0,
  sanitise_data/1
]).

-define(MEGASECS_MILLISECOND_DIVISOR,1000000000).
-define(SECS_MILLISECOND_DIVISOR,1000).
-define(MILLISECONDS_MICROSECONDS_DIVISOR,1000).


%%--------------------------------------------------------------------
-spec(sanitise_data(arg()) -> sanitised_data()).
%% @doc
%% Sanitise the data coming in, into the necessary arguments for setting
%% up an endpoint
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
  riak_stat:timestamp().
