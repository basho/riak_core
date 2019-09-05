%%%-------------------------------------------------------------------
%%% @doc
%%% Exometer Man, the Manager for all things exometer, any function calls
%%% to exometer go through here.
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_exom).
-include_lib("riak_core/include/riak_stat.hrl").

%% Registration API
-export([register/1]).

%% Read API
-export([
    get_values/1,
    get_info/2]).

%% Update API
-export([
    update/3,
    update/4]).


%%%===================================================================
%%% Registration API
%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% Registers all stats, using  exometer:re_register/3, any stat that is
%% re_registered overwrites the previous entry, works the same as
%% exometer:new/3 except it wont return an error if the stat already
%% is registered.
%% @end
%%%-------------------------------------------------------------------
-spec(register(statinfo()) -> ok | error()).
register({StatName, Type, Opts, Aliases}) ->
    register(StatName, Type, Opts, Aliases).
register(StatName, Type, Opts, Aliases) ->
    re_register(StatName, Type, Opts),
    lists:foreach(fun
                      ({DP,Alias}) ->
                          aliases(new,{Alias,StatName,DP})
                  end,Aliases).

re_register(StatName, Type, Opts) ->
    exometer:re_register(StatName, Type ,Opts).

%%%-------------------------------------------------------------------
%% @doc
%% goes to exometer_alias and performs the type of alias function specified
%% @end
%%%-------------------------------------------------------------------
-spec(aliases(aliastype(), list()) -> ok | acc() | error()).
aliases(new, [Alias,StatName,DP]) ->
    exometer_alias:new(Alias,StatName,DP);
aliases(prefix_foldl,[]) ->
    exometer_alias:prefix_foldl(<<>>,alias_fun(),orddict:new());
aliases(regexp_foldr,[N]) ->
    exometer_alias:regexp_foldr(N,alias_fun(),orddict:new()).

alias_fun() ->
    fun(Alias, Entry, DP, Acc) ->
        orddict:append(Entry, {DP, Alias}, Acc)
    end.

%%%===================================================================
%%% Reading Stats API
%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% The Path is the start or full name of the stat(s) you wish to find,
%% i.e. [riak,riak_kv] as a path will return stats with those to elements
%% in their path. and uses exometer:find_entries and above function
%% @end
%%%-------------------------------------------------------------------
-spec(get_values(arg()) -> exo_value() | error()).
get_values(Path) ->
    exometer:get_values(Path).

%%%-------------------------------------------------------------------
%% @doc
%% find information about a stat on a specific item
%% @end
%%%-------------------------------------------------------------------
-spec(get_info(statname(), info()) -> value()).
get_info(Stat, Info) ->
    exometer:info(Stat, Info).

%%%===================================================================
%%% Updating Stats API
%%%===================================================================


%%%-------------------------------------------------------------------
%% @doc
%% Updates the stat, if the stat does not exist it will create a
%% crude version of the metric
%% @end
%%%-------------------------------------------------------------------
-spec(update(metricname(),arg(),type(),options()) -> ok).
update(Name, Val, Type) ->
    update(Name, Val, Type, []).
update(Name, Val, Type, Opts) ->
    exometer:update_or_create(Name, Val,Type, Opts).

