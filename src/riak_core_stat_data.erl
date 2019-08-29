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


-spec(data_sanitise(data()) -> data()).
%% @doc
%% this is for data coming in from the console of format [<<"X">>]
%% or ["riak...."] etc.,
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
  lists:map(fun(D) ->
    data_sanitise_(D, Type, Status)
            end, Data).
%% convert Data to binary in a list if it isn't already, to pull out
%% additional arguments, such as type and status.


data_sanitise_(Data, Type, Status) ->
  [Stat | Est] = re:split(Data, "/"),
    %% When pulling out additional arguments they are separated by "/"
    %% i.e. riak.riak_kv.*.gets.**/status=* ->
    %% [<<"riak.riak_kv.*.gets.**">> | <<"status=*">>]
  {NewType, NewStatus, DPs} = type_status_and_dps(Est, Type, Status, default),
  {Names, MatchSpecs} = stat_entries(Stat, NewType, NewStatus),
  {Names, MatchSpecs, DPs}.


%% @doc
%% when /status=*, /type=* or /datapoints is given it can be extracted out
%% @end
type_status_and_dps([<<"type=", T/binary>> | Rest], _Type, Status, DPs) ->
  NewType =
    case T of
      <<"*">> -> '_'; %% type does not matter, is also the default
      _ ->
        try binary_to_existing_atom(T, latin1)
        catch error:_ ->T
        end
    end, type_status_and_dps(Rest, NewType, Status, DPs);
type_status_and_dps([<<"status=", S/binary>> | Rest], Type, _Status, DPs) ->
  NewStatus =
    case S of
      <<"*">> -> '_'; %% status does not matter,
      <<"enabled">> ->  enabled; %% enabled is the default
      <<"disabled">> -> disabled
    end, type_status_and_dps(Rest, Type, NewStatus, DPs);
type_status_and_dps([DPsBin | Rest], Type, Status, DPs) ->
  NewDPs = merge(
    [binary_to_existing_atom(D, latin1) || D <- re:split(DPsBin, ",")],
    DPs), %% datapoints are separated by ","
  type_status_and_dps(Rest, Type, Status, NewDPs);
type_status_and_dps([], Type, Status, DPs) ->
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
    %% match_spec to match the metrics saved in exometer, is the pattern used in
    %% exometer:select
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
  Parts = re:split(Data, "\\.", [{return, list}]),
  Heads = replace_parts(Parts),
  {Heads,[{{H, Type, Status}, [], ['$_']} || H <- Heads]};
stat_entries(_Stat, _Type, Status) ->
  io:fwrite("(Illegal status : ~p~n", [Status]).

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
        || {DP, Data} <- DPs, Data =/= undefined, Data =/= []];
print({Entries, _}, Attrs) -> %% todo make this work please, remove print_info_1
    lists:foreach(
        fun
            ({N,_,_,_}) ->
                print(N, Attrs);
            ({N, _, _}) ->
                print(N, Attrs);
            ({N, _}) ->
                print(N, Attrs)
%%            (N) ->
%%                print(N, [])
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
