%%%-------------------------------------------------------------------
%%% @doc
%%% The middleman between exometer and metadata and the rest of the app,
%%% any information needed from exometer or metadata goes through the
%%% coordinator
%%% @end
%%%-------------------------------------------------------------------
-module(riak_core_stat_coordinator).

-include_lib("riak_core/include/riak_core_stat.hrl").

%% Console API
-export([
  find_entries/2,
  find_static_stats/1,
  find_stats_info/2,
  change_status/1,
  reset_stat/1
]).

%% Profile API
-export([
  save_profile/1,
  load_profile/1,
  delete_profile/1,
  reset_profile/0,
  get_profiles/0,
  get_loaded_profile/0
]).

-export([
  register/1,
  update/3,
  unregister/1,
  aggregate/2
]).

%% Metadata API
-export([
  reload_metadata/1,
  check_status/1
]).

%% Exometer API
-export([
  get_info/2,
  get_datapoint/2,
  select/1,
  alias/1,
  aliases/1,
  aliases/2,
  get_values/1
]).

-define(DISABLED_META_RESPONSE, io:fwrite("Metadata is Disabled~n")).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%% Console API %%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec(find_entries(stats(), status()) -> stats()).
%% @doc
%% Find the entries in exometer if the metadata is disabled, return
%% the stat name and its status : {Stat, Status}
%% @end
find_entries(Stats, Status) ->
  case maybe_meta(fun find_entries_meta/2, {Stats, Status}) of
    false ->
      find_entries_exom(Stats, Status);
    Stats ->
      Stats
  end.

find_entries_meta(Stats, Status) ->
  case riak_core_stat_metadata:find_entries(Stats, Status) of
    [] ->
      legacy_search(Stats, Status);
    {error, _Reason} ->
      legacy_search(Stats, Status);
    Entries ->
      Entries
  end.

find_entries_exom(Stats, Status) ->
  case legacy_search(Stats, Status) of
    [] ->
      find_entries(Stats, Status);
    Entries ->
      Entries
  end.

-spec(legacy_search(stats(), status()) -> stats()).
%% @doc
%% legacy code to find the stat and its status, in case it isn't
%% found in metadata/exometer
%% @end
legacy_search(Stats, Status) ->
  case the_legacy_search(Stats, Status) of
    false ->
      [];
    Stats ->
      Stats
  end.

the_legacy_search(Stats, Status) ->
  lists:map(fun(Stat) ->
    legacy_search(Stat, '_', Status)
            end, Stats).
legacy_search(Stat, Type, Status) ->
  case re:run(Stat, "\\.", []) of
    {match, _} ->
      false;
    nomatch ->
      Re = <<"^", (make_re(Stat))/binary, "$">>,
      [{Stat, legacy_search_(Re, Type, Status)}]
  end.

make_re(S) ->
  repl(split_pattern(S, [])).

repl([single | T]) ->
  <<"[^_]*", (repl(T))/binary>>;
repl([double | T]) ->
  <<".*", (repl(T))/binary>>;
repl([H | T]) ->
  <<H/binary, (repl(T))/binary>>;
repl([]) ->
  <<>>.

split_pattern(<<>>, Acc) ->
  lists:reverse(Acc);
split_pattern(<<"**", T/binary>>, Acc) ->
  split_pattern(T, [double | Acc]);
split_pattern(<<"*", T/binary>>, Acc) ->
  split_pattern(T, [single | Acc]);
split_pattern(B, Acc) ->
  case binary:match(B, <<"*">>) of
    {Pos, _} ->
      <<Bef:Pos/binary, Rest/binary>> = B,
      split_pattern(Rest, [Bef | Acc]);
    nomatch ->
      lists:reverse([B | Acc])
  end.

legacy_search_(N, Type, Status) ->
  Found = aliases(regexp_foldr, [N]),
  lists:foldr(
    fun({Entry, DPs}, Acc) ->
      case match_type(Entry, Type) of
        true ->
          DPnames = [D || {D, _} <- DPs],
          case get_datapoint(Entry, DPnames) of
            {ok, Values} when is_list(Values) ->
              [{Entry, zip_values(Values, DPs)} | Acc];
            {ok, disabled} when Status == '_';
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


%%%----------------------------------------------------------------%%%

-spec(find_static_stats(stats()) -> status()).
%% @doc
%% find all the stats in exometer with the value = 0
%% @end
find_static_stats(Stats) ->
  case riak_core_stat_exometer:find_static_stats(Stats) of
    [] ->
      [];
    {error, _Reason} ->
      [];
    Stats ->
      Stats
  end.

%%%----------------------------------------------------------------%%%

-spec(find_stats_info(stats(), info() | list()) -> stats()).
%% @doc
%% find the information of a stat from the information given
%% @end
find_stats_info(Stats, Info) ->
  riak_core_stat_exometer:find_stats_info(Stats, Info).

%%%----------------------------------------------------------------%%%

-spec(change_status(stats()) -> ok | error()).
%% @doc
%% change status in metadata and then in exometer if metadata]
%% is enabled.
%% @end
change_status(StatsList) ->
  case maybe_meta(fun change_both_status/1, StatsList) of
    false  ->
      change_exom_status(StatsList);
    Other ->
      Other
  end.

change_both_status(StatsList) ->
  change_meta_status(StatsList),
  change_exom_status(StatsList).

%%%----------------------------------------------------------------%%%

-spec(reset_stat(statname()) -> ok).
%% @doc
%% reset the stat in exometer and in the metadata
%% @end
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

select(Arg) ->
  case maybe_meta(fun met_select/1, Arg) of
    false  ->
      exo_select(Arg);
    Stats ->
      Stats
  end.

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
  maybe_meta(fun riak_core_stat_metadata:save_profile/1, Profile).

load_profile(Profile) ->
  maybe_meta(fun riak_core_stat_metadata:load_profile/1, Profile).

delete_profile(Profile) ->
  maybe_meta(fun riak_core_stat_metadata:delete_profile/1, Profile).

reset_profile() ->
  maybe_meta(fun riak_core_stat_metadata:reset_profile/0, []).

get_profiles() ->
  maybe_meta(fun riak_core_stat_metadata:get_profiles/0, []).

get_loaded_profile() ->
  maybe_meta(fun riak_core_stat_metadata:get_loaded_profile/0, []).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Admin API %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec(register(data()) -> ok | error()).
%% @doc
%% register in metadata and pull out the status,
%% and send that status to exometer
%% @end
register({Stat, Type, Opts, Aliases} = Arg) ->
  io:format("riak_stat_coordinator:register(~p)~n", [Arg]),
  Fun = fun register_in_both/4,
  case maybe_meta(Fun, Arg) of
    false  ->
      io:format("riak_stat_coordinatoe:register_in_exometer(Arg)~n"),
      register_in_exometer(Stat, Type, Opts, Aliases);
    Ans ->
      Ans
  end.

register_in_both(Stat, Type, Opts, Aliases) ->
  case register_in_metadata({Stat, Type, Opts, Aliases}) of
    [] -> ok;
    NewOpts -> register_in_exometer(Stat, Type, NewOpts, Aliases)
  end.

%%%----------------------------------------------------------------%%%

-spec(update(statname(), incrvalue(), type()) -> ok).
%% @doc
%% update unless disabled or unregistered
%% @end
update(Name, Inc, Type) ->
  Fun = fun check_in_meta/1,
  case maybe_meta(Fun, Name) of
    false  ->
      update_exom(Name, Inc, Type);
    ok -> ok;
    _ -> update_exom(Name, Inc, Type)
  end.

%%%----------------------------------------------------------------%%%

-spec(unregister(statname()) -> ok | error()).
%% @doc
%% set status to unregister in metadata, and delete
%% in exometer
%% @end
unregister(StatName) ->
  Fun = fun unregister_in_both/1,
  case maybe_meta(Fun, StatName) of
    false  ->
      unregister_in_exometer(StatName);
    Ans ->
      Ans
  end.

unregister_in_both(StatName) ->
  unregister_in_metadata(StatName),
  unregister_in_exometer(StatName).

%%%----------------------------------------------------------------%%%

-spec(aggregate(pattern(), datapoint()) -> stats()).
aggregate(P, DP) ->
  riak_core_stat_exometer:aggregate(P, DP).


%%%===================================================================
%%% Metadata API
%%%===================================================================

reload_metadata(Stats) ->
  change_meta_status(Stats).


maybe_meta(Fun, Args) ->
  case ?IS_ENABLED(?META_ENABLED) of
    true ->
      case Args of
        []                      -> Fun();
        {One, Two}              -> Fun(One, Two);
        {One, Two, Three}       -> Fun(One, Two, Three);
        {One, Two, Three, Four} -> Fun(One, Two, Three, Four);
        One        -> Fun(One)
      end;
    false ->
      ?DISABLED_META_RESPONSE,
      false
  end.

register_in_metadata(StatInfo) ->
  riak_core_stat_metadata:register_stat(StatInfo).

check_in_meta(Name) ->
  riak_core_stat_metadata:check_meta(Name).

check_status(Stat) ->
  riak_core_stat_metadata:check_status(Stat).

change_meta_status(Arg) ->
  riak_core_stat_metadata:change_status(Arg).

unregister_in_metadata(StatName) ->
  riak_core_stat_metadata:unregister(StatName).

reset_meta_stat(Arg) ->
  riak_core_stat_metadata:reset_stat(Arg).

met_select(Arg) ->
  riak_core_stat_metadata:select(Arg).


%%%===================================================================
%%% Exometer API
%%%===================================================================

get_info(Name, Info) ->
  riak_core_stat_exometer:info(Name, Info).

get_datapoint(Name, DP) ->
  riak_core_stat_exometer:get_datapoint(Name, DP).

exo_select(Arg) ->
  riak_core_stat_exometer:select_stat(Arg).

alias(Arg) ->
  riak_core_stat_exometer:alias(Arg).

aliases(Arg, Value) ->
  riak_core_stat_exometer:aliases(Arg, Value).
aliases({Arg, Value}) ->
  riak_core_stat_exometer:aliases(Arg, Value).

register_in_exometer(StatName, Type, Opts, Aliases) ->
  io:format("riak_stat_coordinator:register_in_exometer(~p)~n", [StatName]),
  riak_core_stat_exometer:register_stat(StatName, Type, Opts, Aliases).

unregister_in_exometer(StatName) ->
  riak_core_stat_exometer:unregister_stat(StatName).

change_exom_status(Arg) ->
  riak_core_stat_exometer:change_status(Arg).

reset_exom_stat(Arg) ->
  riak_core_stat_exometer:reset_stat(Arg).

update_exom(Name, IncrBy, Type) ->
  riak_core_stat_exometer:update_or_create(Name, IncrBy, Type).

%%%===================================================================

get_values(Arg) ->
  {Stats, _MS, _DPS} = riak_core_stat_admin:data_sanitise(Arg),
  lists:map(fun(Path) ->
    riak_core_stat_exometer:get_values(Path)
            end, Stats).
