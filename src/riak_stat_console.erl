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
    stat_metadata/1
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
%% Show enabled or disabled stats
%% when using riak-admin stat show riak.** enabled stats will
%% show by default
%% @end
%%%-------------------------------------------------------------------
-spec(show_stat(consolearg()) -> print()).
show_stat(Arg) ->
    print(find_entries(data_sanitise(Arg))).

%%%-------------------------------------------------------------------
%% @doc
%% Check which stats in exometer are not updating (checks enabled)
%% @end
%%%-------------------------------------------------------------------
-spec(show_stat_0(consolearg()) -> print()).
show_stat_0(Arg) ->
    {Stats,_Status,Type,DPs}=data_sanitise(Arg),
    print({[find_entries({Stats,enabled,Type,DPs})],show_0}).

%%%-------------------------------------------------------------------
%% @doc
%% Returns all the stats information
%% @end
%%%-------------------------------------------------------------------
-spec(stat_info(consolearg()) -> print()).
stat_info(Arg) ->
    {Attrs, RestArg} = pick_info_attrs(Arg),
    {Stat,Type,Status,_DPS} = data_sanitise(RestArg),
    print(find_entries({Stat,Type,Status,Attrs})).

-spec(pick_info_attrs(consolearg()) -> {attributes(), consolearg()}).
%% @doc get list of attrs to print @end
pick_info_attrs(Arg) ->
    case lists:foldr(
        fun("name",     {As, Ps}) -> {[name      | As], Ps};
           ("type",     {As, Ps}) -> {[type      | As], Ps};
           ("module",   {As, Ps}) -> {[module    | As], Ps};
           ("value",    {As, Ps}) -> {[value     | As], Ps};
           ("cache",    {As, Ps}) -> {[cache     | As], Ps};
           ("status",   {As, Ps}) -> {[status    | As], Ps};
           ("timestamp",{As, Ps}) -> {[timestamp | As], Ps};
           ("options",  {As, Ps}) -> {[options   | As], Ps};
           (P,           {As, Ps}) -> {As, [P | Ps]}
        end, {[], []}, split_arg(Arg)) of
        {[], Rest} ->          %% If no arguments given
            {?INFOSTAT, Rest}; %% use all, and return arg
        Other ->
            lager:info("Other : ~p",[Other]),
            Other %% Otherwise = {[name,type...],["riak.**"]}
    end.

split_arg(Str) ->
    %% separate the argument by the "-"
    %% i.e. ["riak.** -type -status"] -> ["riak.**","type","status"]
                %% Spaces are concatenated in riak-admin
    re:split(Str, "\\-", [{return, list}]).

%%%-------------------------------------------------------------------
%% @doc
%% Similar to @see show_stat_0, but will disable all the stats that
%% are not updating
%% @end
%%%-------------------------------------------------------------------
-spec(disable_stat_0(consolearg()) -> print()).
disable_stat_0(Arg) ->
    {Stats,_Status,Type,DPs} = data_sanitise(Arg),
    print({[find_entries({Stats,enabled,Type,DPs})],disable_0}).

%%%-------------------------------------------------------------------
%% @doc
%% change the status of the stat (in metadata and) in exometer
%% @end
%%%-------------------------------------------------------------------
-spec(status_change(consolearg(), status()) -> print()).
status_change(Arg, ToStatus) ->
    {Entries,_DP} = % if disabling stats, pull only enabled ones &vv
    case ToStatus of
        enabled  -> find_entries(data_sanitise(Arg, '_', disabled));
        disabled -> find_entries(data_sanitise(Arg, '_', enabled))
    end,
    change_status([{Stat, ToStatus} || {Stat,_,_} <- Entries]).

%%%-------------------------------------------------------------------
%% @doc
%% resets the stats in metadata and exometer and tells metadata that
%% the stat has been reset
%% @end
%%%-------------------------------------------------------------------
-spec(reset_stat(consolearg()) -> ok).
reset_stat(Arg) ->
    {Found, _DPs} = find_entries(data_sanitise(Arg)),
    reset_stats([N || {N,_,_} <-Found]).

%%%-------------------------------------------------------------------
%% @doc
%% enabling the metadata allows the stats configuration to be
%% persisted, disabling the metadata returns riak to its
%% original functionality of only using the exometer functions.
%% Enabling and disabling the metadata occurs here, directing the
%% stats and function work occurs in the riak_stat_mgr
%% @end
%%%-------------------------------------------------------------------
-define(Metadata_Enabled, ?IS_ENABLED(?METADATA_ENABLED)).

-spec(stat_metadata(Argument :: boolean() | status) -> print()).
stat_metadata(Argument) -> stat_metadata(Argument, ?Metadata_Enabled).

-spec(stat_metadata(ToStatus :: boolean() | status,
                    CurrentStatus :: true | false) -> print()).
%% Enable the metadata when it is already enabled:
stat_metadata(true, true) ->
    print_response("Metadata already enabled~n");
%% Enable the metadata when it is disabled:
stat_metadata(true, false) ->
    riak_stat_mgr:reload_metadata(),
    set_metadata(true),
    print_response("Metadata enabled~n");
%% Disabled the metadata when it is enabled:
stat_metadata(false, true) ->
    set_metadata(false),
    print_response("Metadata disabled~n");
%% Disable the metadata when it is already disabled:
stat_metadata(false, false) ->
    print_response("Metadata already disabled~n");
%% Find the status of the metadata
stat_metadata(status, true)  -> print_response("Metadata Is enabled~n");
stat_metadata(status, false) -> print_response("Metadata Is disabled~n").

%- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
set_metadata(Boolean) ->
    application:set_env(riak_core, ?METADATA_ENABLED, Boolean).

print_response(String) -> print_response(String,[]).
print_response(String, Args) -> io:fwrite(String, Args).


%%%===================================================================
%%% Helper API
%%%===================================================================
%%%-------------------------------------------------------------------
%%% @doc
%%% Arguments coming in from the console arrive at this function, data
%%% is transformed into a metrics name and type status/datapoints if
%%% they have been given.
%%% @end
%%%-------------------------------------------------------------------
-spec(data_sanitise(consolearg()) -> sanitised_stat()).
data_sanitise(Arg) ->
    parse_stat_entry(check_args(Arg), ?TYPE, ?STATUS, ?DPs).

%% separate Status from Type with function of same arity
data_sanitise(Arg, Status)
    when   Status == enabled
    orelse Status == disabled
    orelse Status == '_' ->
    parse_stat_entry(check_args(Arg), ?TYPE, Status, ?DPs);
%% If doesn't match the clause above, it must be the Type given.
data_sanitise(Arg, Type) ->
    parse_stat_entry(check_args(Arg), Type, ?STATUS, ?DPs).

data_sanitise(Arg, Type, Status) ->
    parse_stat_entry(check_args(Arg), Type,  Status, ?DPs).
%%%-------------------------------------------------------------------

%% @doc make sure all Args are Binaries in a list: [<<Args>>]
%% sanitised for parse_stat_entry @end
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
    [Bin | Args] = re:split(BinArgs, "/"), %% separate /type=*.. etc...
    {NewType, NewStatus, NewDPs} =
    %% pull the type, status and datapoints from the argument given
    %% if nothing was given then the default is returned.
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
    %% creates a list of the datapoints given from the command line.
        lists:map(fun(D) ->
            try binary_to_existing_atom(D, latin1) of
                DP -> DP
            catch _:_ ->
                io:fwrite("Illegal datapoint name~n"),[]
            end
                  end, re:split(DPsBin,",")),
    NewDPs = merge(lists:flatten(Atoms),DPs),
    type_status_and_dps(Rest, Type, Status, NewDPs).

%% @doc If 'default' then
%% it will be given as 'default' to exometer anyway
merge([_ | _] = DPs, default) ->
    DPs;
%% Otherwise checks if the H from Arg is a member of the DPs list,
%% and will skip if they are already in there. @end
merge([H | T], DPs) ->
    case lists:member(H, DPs) of
        true -> merge(T, DPs);
        false -> merge(T, DPs ++ [H])
    end;
merge([], DPs) ->
    DPs.

%% @doc creates a path for the stat name @end
statname([]) ->
    [?Prefix] ++ '_' ;
statname("*") ->
    statname([]);
statname("["++_ = Expr) ->
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
        %% a Pad ('_') is added in between the terms given
        %% in the arg, up to 10 times, and inserted into the
        %% list; as in:
        %% [[riak,'_',time],
        %% [riak,'_','_',time],
        %% [riak,'_','_','_',time],...];
        %% If the Argument: riak.**.time was given.
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
-spec(find_entries(sanitised_stat()) -> listofstats()).
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
-spec(print(atom() | string() | list()
         | {listofstats(),datapoints()}) -> print()).
print(undefined)   -> print([]);
print([undefined]) -> print([]);
print({Stats,DPs}) -> print(Stats,DPs);
print(Arg)         -> print(Arg, []).

print([], _) ->
    io:fwrite("No Matching Stats~n");
print(NewStats,Args) ->
    lists:map(fun
          ({Names,_NDPs}) when Args == disable_0 ->
              fun_0_stats(Names,disable_0);

          ({Names, NDPs}) when Args == show_0 ->
              fun_0_stats(Names,NDPs);

          ({Names, Values}) when is_list(Names) ->
              print_if_0(Names, Values);

          ({N,_T,_S}) when Args == [] -> get_value(N);
          ({N, T, S}) ->  find_stats_info({N,T,S}, Args);

          %% legacy pattern
          (Legacy) -> legacy_map(Legacy)
      end, NewStats).

legacy_map(Legacy) ->
    lists:map(fun
                  ({LP,[]}) ->
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
              end, Legacy).

fun_0_stats(Stats,DPs) ->
    case lists:flatten([print_if_0(Name,DPs) || {Name,_,_} <- Stats]) of
        [] -> print([]);
        _ -> ok
    end.

print_if_0(StatName,disable_0) ->
    case not_0(StatName,[]) of
        [] -> [];
        _Vals ->
            change_status([{StatName,disabled}]),
            print_stat_args(StatName, disabled)
    end;
print_if_0(StatName, DPs) ->
    Values = not_0(StatName,DPs),
    print_stat_args(StatName, Values).

not_0(Stat, _DPs) ->
    case riak_stat_exom:get_value(Stat) of
        {ok, V} -> fold_0_values(V);
        _ -> []
    end.

fold_0_values(Values) ->
    lists:foldl(fun
                    ({Value,0}, Acc) -> [{Value,0}|Acc];
                    ({Value,[]},Acc) -> [{Value,0}|Acc];
                    (__________,Acc) -> Acc
                end, [],Values).

%%%-------------------------------------------------------------------

get_value(Name) ->
    Values = riak_stat_exom:get_value(Name),
    Folded = fold_values(Values),
    print_stat_args(Name, Folded).

find_stats_info({Name,Type,Status}, Info) ->
    Values = riak_stat_exom:get_datapoint(Name, Info),
    Folded = fold_values(Values),
    print_stat_args({Name,Type,Status,Info},Folded).

get_info_2(Statname,Attrs) ->
    fold_values([riak_stat_exom:get_info(Statname,Attrs)]).

print_stat_args(_StatName, []) -> [];
print_stat_args(StatName, disabled) ->
    io:fwrite("~p : disabled", [StatName]);
print_stat_args({StatName,Type,Status,Info},{error,_}) ->
    NewValues = get_info_2(StatName,Info),
    NewArgs = replace_type_and_status(Type,Status,NewValues),
    print_stat_args(StatName,NewArgs);
print_stat_args({Statname, Type, Status,_Info}, Args) ->
    NewArgs = replace_type_and_status(Type,Status,Args),
    print_stat_args(Statname,NewArgs);
print_stat_args(StatName, Args) ->
    io:fwrite("~p : ~p~n",[StatName,Args]).

fold_values([]) -> [];
fold_values(Values) when is_list(Values) ->
    lists:foldl(fun
                    (undefined,A)           -> A;
                    ([], A)                 -> A;
                    ({type,_},A)            -> [{type,undef}|A];
                    ({status,_},A)          -> [{status,undef}|A];
                    ({_,undefined},A)       -> A;
                    ({_,{error,_}},A)       -> A;
                    ({ms_since_reset,_V},A) -> A;
                    (DP,A)                  -> [DP|A]
                end, [], Values);
fold_values({ok,Values}) -> fold_values(Values);
fold_values(Values) -> Values.

replace_type_and_status(Type,Status,List) ->
    NewList = lists:keyreplace(type,1,List,{type,Type}),
    lists:keyreplace(status,1,NewList, {status,Status}).
