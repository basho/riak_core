%%%-------------------------------------------------------------------
%%% @doc
%%% The middleman between exometer and metadata and the rest of the app,
%%% any information needed from exometer or metadata goes through the
%%% manager
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_mgr).
-include_lib("riak_core/include/riak_stat.hrl").

%% Main API
-export([maybe_meta/2]).

%% Registration API
-export([register/1]).

%% Profile API
-export([
    save_profile/1,
    load_profile/1,
    delete_profile/1,
    reset_profile/0,
    get_profiles/0,
    get_loaded_profile/0]).

%% Specific to manager Macros:

-define(IS_ENABLED(Arg),    app_helper:get_env(riak_core,Arg,true)).
-define(METADATA_ENABLED,   metadata_enabled).
-define(DISABLED_METADATA,  io:fwrite("Metadata is Disabled~n")).

%%%===================================================================
%%% Main API
%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% Check the apps env for the status of the metadata, the persistence
%% of stat configuration can be disabled as a fail safe, or in case
%% the stats configuration doesn't need to be semi-permanent for a
%% length of time (i.e. testing)
%% @end
%%%-------------------------------------------------------------------
-spec(maybe_meta(function(), arguments()) -> false | ok | error() | arg()).
maybe_meta(Function, Arguments) ->
    case ?IS_ENABLED(?METADATA_ENABLED) of
        false -> ?DISABLED_METADATA, false; %% it's disabled
        true  -> case Arguments of          %% it's enabled (default)
                     []        -> Function();
                     {U,D}     -> Function(U,D);
                     {U,D,T}   -> Function(U,D,T);
                     {U,D,T,C} -> Function(U,D,T,C);
                     U -> Function(U)
                 end
    end.

%%%===================================================================
%%% Registration API
%%%===================================================================

%%%-------------------------------------------------------------------
%% @doc
%% register in metadata and pull out the status,
%% and send that status to exometer
%% @end
%%%-------------------------------------------------------------------
-spec(register(statinfo()) -> ok | error()).
register(StatInfo) ->
    DefFun = fun register_both/1,
    ExoFun = fun register_exom/1,
    case maybe_meta(DefFun, StatInfo) of
        false     -> ExoFun(StatInfo);
        Otherwise -> Otherwise
    end.

register_both(StatInfo) ->
    case register_meta(StatInfo) of
        [] -> ok; %% stat is deleted or recorded as unregistered in meta
        NewOpts -> {Name, Type, _Opts, Aliases} = StatInfo,
            register_exom({Name, Type, NewOpts, Aliases})
    end.

register_meta(StatInfo) ->
    riak_stat_meta:register(StatInfo).

register_exom(StatInfo) ->
    riak_stat_exom:register(StatInfo).

%%%===================================================================
%%% Reading Stats API
%%%===================================================================

find_entries(MS,Stats,Status) ->
    ok.

aggregate(Pattern, DPs) ->
    ok.

%%%===================================================================
%%% Deleting/Resetting Stats API
%%%===================================================================

unregister(Stat) ->
    ok.

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
    maybe_meta(fun riak_stat_meta:save_profile/1, Profile).

load_profile(Profile) ->
    maybe_meta(fun riak_stat_meta:load_profile/1, Profile).

delete_profile(Profile) ->
    maybe_meta(fun riak_stat_meta:delete_profile/1, Profile).

reset_profile() ->
    maybe_meta(fun riak_stat_meta:reset_profile/0, []).

get_profiles() ->
    maybe_meta(fun riak_stat_meta:get_profiles/0, []).

get_loaded_profile() ->
    maybe_meta(fun riak_stat_meta:get_loaded_profile/0, []).
