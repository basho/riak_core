%%%-------------------------------------------------------------------
%%% @doc
%%% Commands for "riak admin stat ___ ...." call into this module from
%%% riak_core_console. the purpose of these console commands is to display
%%% to the user all the information they want about an entry or entries,
%%% then the stats can be configured/updated in the metadata/exometer directly.
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
    data_sanitise/4
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%%===================================================================
%%% API
%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% riak admin stat show <entry>/type=(type())/status=(enabled|disabled|*)/[dps].
%% Show enabled or disabled stats
%% when using riak-admin stat show riak.** enabled stats will show by default
%%
%% otherwise use: riak-admin stat show <entry>/status=* | disabled
%% @end
%%%-------------------------------------------------------------------
-spec(show_stat(arg()) -> statslist()).
show_stat(Arg) ->
    print_stats(find_entries(data_sanitise(Arg))).

%%%-------------------------------------------------------------------
%% @doc
%% Check which stats in exometer are not updating (only checks enabled)
%% @end
%%%-------------------------------------------------------------------
-spec(show_stat_0(data()) -> value()).
show_stat_0(Arg) ->
    {Stats,_Status,Type,DPs}=data_sanitise(Arg),
    print_stats(find_entries({Stats,enabled,Type,DPs}),[]).

%%%-------------------------------------------------------------------
%% @doc
%% Returns all the stats information
%% @end
%%%-------------------------------------------------------------------
-spec(stat_info(data()) -> value()).
stat_info(Arg) ->
    %% todo: work out how to put info into the find_entries
    {Attrs, RestArg} = pick_info_attrs(Arg),
%%    {Stats,Status,Type,DPs} = data_sanitise(RestArg),
%%    Found = lists:map(fun
%%                          ({Stat,        _Status}) ->       {Stat, find_stat_info(Stat, Attrs)};
%%                          ({Stat, _Type, _Status}) ->       {Stat, find_stat_info(Stat, Attrs)};
%%                          ({Stat, _Type, _Status, _DPs}) -> {Stat, find_stat_info(Stat, Attrs)}
%%                      end,
    {S,T,ST,_DPS} = data_sanitise(RestArg),
        find_entries({S,T,ST,Attrs}).
%%    print(Found).

%%%-------------------------------------------------------------------
%% @doc
%% Similar to the function above, but will disable all the stats that
%% are not updating
%% @end
%%%-------------------------------------------------------------------
-spec(disable_stat_0(data()) -> ok).
disable_stat_0(Arg) ->
%%    {Stats,Status,Type,DPs} = data_sanitise(Arg),
%%    Entries = lists:map(fun
%%                            ({Stat,        _Status}) ->         Stat;
%%                            ({Stat, _Type, _Status}) ->         Stat;
%%                            ({Stat, _Type, _Status, _DPs}) ->   Stat
%%                        end,
%%        find_entries(data_sanitise(Arg))),
    NotUpdating = not_updating(find_entries(data_sanitise(Arg))),
%%    DisableTheseStats =
%%        lists:map(fun({Name, _V}) ->
%%            {Name, {status, disabled}}
%%                  end, NotUpdating),
    change_status([{Name,{status,disabled}} || {Name,_V} <- NotUpdating]).



%%%-------------------------------------------------------------------
%% @doc
%% change the status of the stat (in metadata and) in exometer
%% @end
%%%-------------------------------------------------------------------
-spec(status_change(data(), status()) -> ok).
status_change(Arg, ToStatus) ->
    Entries = % if disabling lots of stats, pull out only enabled ones
    case ToStatus of
        enabled  ->
%%            {Stats,Status,Type,DPs} = data_sanitise(Arg, '_', disabled),
            find_entries(data_sanitise(Arg, '_', disabled));
        disabled ->
%%            {Stats,Status,Type,DPs} = data_sanitise(Arg, '_', enabled),
            find_entries(data_sanitise(Arg, '_', enabled))
    end,
    change_status([{Stat, {status, ToStatus}} || {Stat, _Status} <- Entries]).



%%%-------------------------------------------------------------------
%% @doc
%% resets the stats in metadata and exometer and tells metadata that the stat
%% has been reset
%% @end
%%%-------------------------------------------------------------------
-spec(reset_stat(data()) -> ok).
reset_stat(Arg) ->
%%    {Stats,Status,Type,DPs} = data_sanitise(Arg),
    Entries = lists:map(fun
                            ({Stat,        _Status}) ->         Stat;
                            ({Stat, _Type, _Status}) ->         Stat;
                            ({Stat, _Type, _Status, _DPs}) ->   Stat
                        end,
        find_entries(data_sanitise(Arg))),
    reset_stats(Entries).


%%%-------------------------------------------------------------------
%% @doc
%% enabling the metadata allows the stats configuration and the stats values to
%% be persisted, disabling the metadata returns riak to its original functionality
%% of only using the exometer functions. Enabling and disabling the metadata occurs
%% here, directing the stats and function work occurs in the riak_stat_coordinator
%% @end
%%%-------------------------------------------------------------------
-spec(enable_metadata(data()) -> ok).
enable_metadata(Arg) ->
    Truth = ?IS_ENABLED(?METADATA_ENABLED),
    case data_sanitise(Arg) of
        Truth ->
            print("Metadata-enabled already set to ~s~n", [Arg]);
        Bool when Bool == true; Bool == false ->
            case Bool of
                true ->
                    riak_stat_mgr:reload_metadata(
                        riak_stat_exom:find_entries([riak | '_'])),
                    app_helper:get_env(riak_core, ?METADATA_ENABLED, Bool);
                false ->
                    app_helper:get_env(riak_core, ?METADATA_ENABLED, Bool)
            end;
        Other ->
            print("Wrong argument entered: ~p~n", [Other])
    end.

%%%===================================================================
%%% Helper API
%%%===================================================================

data_sanitise(Arg) ->
    riak_stat_data:data_sanitise(Arg).
data_sanitise(Arg, TypeOrStatus) ->
    riak_stat_data:data_sanitise(Arg, TypeOrStatus).
data_sanitise(Arg, Type, Status) ->
    riak_stat_data:data_sanitise(Arg, Type, Status).
data_sanitise(Arg, Type, Status, DPs) ->
    riak_stat_data:data_sanitise(Arg, Type, Status, DPs).

%%%-------------------------------------------------------------------
%% @doc
%% Find_entries for the stat show/show-0/info, each one will use
%% find_entries to print a stats information. specific for show-0 and
%% different for info, stat show is the generic base in which it was
%% created
%% @end
%%%-------------------------------------------------------------------
-spec(find_entries(statname(),status(),type(),datapoint()) -> statslist()).
find_entries({Stat,Status,Type,DPs}) ->
    find_entries(Stat,Status,Type,DPs).
find_entries(Stats,Status,Type,default) ->
    find_entries(Stats,Status,Type,[]);
find_entries(Stats,Status,Type,DPs) ->
    riak_stat_mgr:find_entries(Stats,Status,Type,DPs).


%%%-------------------------------------------------------------------

find_stat_info(Stats, Info) ->
    riak_stat_mgr:find_stats_info(Stats, Info).


%%%-------------------------------------------------------------------

not_updating({Stats,_Status,Type,DPs}) ->
%%    [not_0(N,[]) || {N,_T,_S} <- find_entries(Stats,enabled,Type,DPs)].
    lists:foldl(fun
                    ({N, _, _}, Acc) ->     not_0(N, Acc);
                    (N, Acc) ->             not_0(N, Acc)
              end,[],find_entries(Stats,enabled,Type,DPs)).

not_0(StatName,Acc) ->
    case riak_stat_exom:get_datapoint(StatName,value) of
        {value, 0} -> [{StatName,0}|Acc];
        {value,[]} -> [{StatName,[]}|Acc];
        {value, _} -> Acc;
        _Otherwise -> Acc
    end.


print_stats({Stats,DPs}) ->
    print_stats(Stats,DPs).
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
                                    ({{NewStats,DPs},[]}) ->
                                        %% not legacy, but will be used in show-0
                                        ok;
                                    ({LP,[]}) ->
                                        io:fwrite(
                                            "== ~s (Legacy pattern): No matching stats ==~n", [LP]);
                                    ({LP, Matches}) ->
                                        io:fwrite("== ~s (Legacy pattern): ==~n", [LP]),
                                        [[io:fwrite("~p: ~p (~p/~p)~n", [N, V, E, DP])
                                            || {DP, V, N} <- DPs] || {E, DPs} <- Matches];
                                    (_) ->
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





change_status(Stats) ->
    riak_stat_mgr:change_status(Stats).


reset_stats(Name) ->
    riak_stat_mgr:reset_stat(Name).


%%%-------------------------------------------------------------------

print(undefined) ->
    print([]);
print([undefined]) ->
    print([]);
print(Arg) ->
    print(Arg, []).
print(Stats, default) ->
    print(Stats, []);
print(stats,Stats) ->
    print(Stats,stats);
print(Stats,Attr) ->
    riak_stat_data:print(Stats,Attr).

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

-ifdef(TEST).

-endif.

%%%-------------------------------------------------------------------

%%read_stats(Stats, Status) ->
%%    read_stats(Stats,Status, '_').
%%read_stats(Stats, Status, Type,DPs) ->
%%    riak_stat_mgr:read_stats(Stats,Status,Type,DPs).


%%%-------------------------------------------------------------------

%%find_entries(Arg) ->
%%    {Stat, Type, Status, DPs} = data_sanitise(Arg),
%%    case Stat of
%%        "[" ++ _ ->
%%            {find_entries_extra(Stat), DPs};
%%        _ ->
%%            case legacy_search(Stat, Type, Status) of
%%                false ->
%%                    find_entries_(Stat, Type, Status, DPs);
%%                Stats ->
%%                    Stats
%%            end
%%    end.
%%
%%find_entries_(Stat, Type, Status, default) ->
%%    find_entries_(Stat, Type, Status);
%%find_entries_(Stat,Type, Status, DPs) ->
%%    Stats = find_entries_(Stat, Type, Status),
%%    lists:map(fun
%%                  ({Name, _Type, _Status}) when DPs == []->
%%                      io:fwrite("~p : ~p~n",
%%                          [Name, riak_stat_exom:get_values(Name)]);
%%                  ({Name, _Type, _Status}) ->
%%                      io:fwrite("~p : ~p~n",
%%                          [Name, riak_stat_exom:get_datapoint(Name, DPs)])
%%              end, Stats).
%%find_entries_(Stat, Type, Status) ->
%%    MS = ms_stat_entry(Stat, Type, Status),
%%    riak_stat_exom:select(MS).
%%
%%ms_stat_entry([], Type, Status) ->
%%    {{[riak_stat:prefix()]++'_', Type, '_'}, [{'=:=','$status',Status}], ['$_']};
%%ms_stat_entry("*", Type, Status) ->
%%    find_entries_([], Type, Status);
%%ms_stat_entry(Stat, Type, Status) when Status == '_'
%%    orelse Status == disabled orelse Status == endabled ->
%%    [{{Stat, Type, Status},[],['$_']}];
%%ms_stat_entry(_Stat,_Type,Status) ->
%%    io:fwrite("Illegal Status Type: ~p~n",[Status]).

%%find_entries_extra(Expr) ->
%%    case erl_scan:string(ensure_trailing_dot(Expr)) of
%%        {ok,Tokens,_} ->
%%            case erl_parse:parse_exprs(Tokens) of
%%                {ok, [Abstract]} ->
%%                    partial_eval(Abstract);
%%                Error ->
%%                    lager:debug("Parse Error in find_entries ~p:~p~n",
%%                    [Expr, Error]), []
%%            end;
%%        ScanError ->
%%            lager:error("Scan Error in find_entries for ~p:~p~n",
%%                [Expr, ScanError])
%%%%    end.
%%
%%ensure_trailing_dot(Str) ->
%%    case lists:reverse(Str) of
%%        "." ++ _ ->
%%            Str;
%%        _ ->
%%            Str ++ "."
%%    end.
%%
%%partial_eval({cons,_,H,T}) ->
%%    [partial_eval(H) | partial_eval(T)];
%%partial_eval({tuple,_,Elems}) ->
%%    list_to_tuple([partial_eval(E) || E <- Elems]);
%%partial_eval({op,_,'++',L1,L2}) ->
%%    partial_eval(L1) ++ partial_eval(L2);
%%partial_eval(X) ->
%%    erl_parse:normalise(X).


%% todo: create a find_entries for the show, show-0 and info, include
%% todo: a function for stats and info and show-0 .,

% todo: make it generic but specific for these, i.e. show and show-0 will have
% todo: similar arguments to be printed.
%
% todo: basically make a simple and generic print(Elem, Args),
% then for each stat take its type, status and datapoints similar to how its
% done in riak_stat_meta and instead of returning a stat that matches those points
%
% print them out
%
% then do a separate function for enable/disable/reset/disable-0 to go to the
% mgr to have its status change and it done in both metadata (if enabled) and in the
% exometer.,
%
% A good point is should we be checking if the metadata is enabled/disabled when
% changing the status, I think it shouldnt matter, if the metadata is causing an error
% have lager:debug return the error and just return ok, that way the metadata does not
% need to  be "reloaded" when it is loaded up.
% enabling and disabling stats does not account for the most of the traffic so it
% should not be too much of an issue.
%
% for disable-0 it might be best if it is down to exometer first.
%
% The order for find_entries: legacy -> exom -> meta.
% the order for enabling/disabling/reset/disable-0 : exom -> meta.
%
% todo:
% do the data_sanitise in this module, to remove the riak_stat_data module basically
%
% do the print function in this module as well or have an atom get passed through to riak_stat
% exom to print if needing printing or returned.
%
% so for functions that need the stats printed it would be like find_entries(print,etc..)
% and if the entries are needed for other things then it would be find_entries(return,etc...)
%
% then we can do the same for metadata, i.e. when the status is enabled and disabled we
% could print the enabled and disabled stats with their status, in the same way
% as with exometer find_entries(print, etc...) and find_entries(return, etc....)
%
% todo:
%
% make a select_replace function in riak_core_metadata, it takes the MS, and replaces a value
%
% see how it is compared to the iterator, as in can it be iterated over and the status changed
%
% i..e
%
% fold(fun({Stat,[{Status,Type,Opts,Aliases}]} when Status == disabled ->
%               {Stat,[{NewStatus,Type,Opts,Aliases}]} end, ?STATPFX).
%
% legacy search should be in here instead of mgr then?
%
% todo: see if we actually need the mgr. It does the communication between the meta
% and exometer, so it is useful in that way. but if we already have riak_stat_exom
% and riak_stat_meta then maybe its best to just remove the one module as mgr, it is
% actually useful, for the other modules to call to instead of this one just replacing the
% riak_core_console...



%%    {Stats,Status,Type,DPs} = data_sanitise(Arg),
%%    NewStats =
%%        case DPs of
%%            default -> find_entries(Stats, Status, Type);
%%            _       -> find_entries(Stats, Status, Type, DPs)
%%        end,
%%    NewStats = find_entries({Stats,Status,Type,DPs}),
%%    print(NewStats,DPs).
%%    print(


%%    {Stats,Status,Type,DPs} = data_sanitise(Arg),
%%    Entries = lists:map(fun
%%                            ({Stat,        _Status}) ->         Stat;
%%                            ({Stat, _Type, _Status}) ->         Stat;
%%                            ({Stat, _Type, _Status, _DPs}) ->   Stat
%%                        end,
%%        find_entries(data_sanitise(Arg))),
%%    NotUpdating = not_updating(data_sanitise(Arg)),