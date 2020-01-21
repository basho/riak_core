%%%-------------------------------------------------------------------
%%% @doc
%%% riak_stat_meta is the middle-man for stats and
%%% riak_core_metadata. All information that needs to go into or out
%%% of the metadata for riak_stat will go through this module.
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_meta).
-include_lib("riak_core/include/riak_stat.hrl").
-include_lib("riak_core/include/riak_core_metadata.hrl").

%% Registration API
-export([
    %% STATS:
    get/2, get/3,
    get_all/1,
    put/3, put/4,
    register/1,
    register/4,
    find_entries/3,
    change_status/1,
    change_status/2,
    reset_stat/1,
    reset_resets/0,
    unregister/1,

    %% PROFILES:
    save_profile/1,
    load_profile/1,
    delete_profile/1,
    reset_profile/0
]).

%% Stats are on per node basis
-define(STAT,                  stats).
-define(STATPFX,              {?STAT, ?NODEID}).
-define(STATKEY(StatName),    {?STATPFX, StatName}).

%% Profiles are Globally shared
-define(PROFPFX,              {profiles, list}).
-define(PROFILEKEY(Profile),  {?PROFPFX, Profile}).
-define(LOADEDPFX,            {profiles, loaded}).
-define(LOADEDKEY,             ?NODEID).
-define(LOADEDPKEY,           {?LOADEDPFX, ?LOADEDKEY}).

-define(STATMAP,               #{status  => enabled,
                                 type    => undefined,
                                 options => [{resets,0}],
                                 aliases => []}).

%%%===================================================================
%%% Metadata API
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%% Pulls out information from riak_core_metadata
%% @end
%%%-------------------------------------------------------------------
-spec(get(metadata_prefix(), metadata_key()) ->
                                    metadata_value() | undefined).
get(Prefix, Key) ->
    get(Prefix, Key, []).
get(Prefix, Key, Opts) ->
    riak_core_metadata:get(Prefix, Key, Opts).

%%%-------------------------------------------------------------------
%% @doc
%% Give a Prefix for anything in the metadata and get a list of all the
%% data stored under that prefix
%% @end
%%%-------------------------------------------------------------------
-spec(get_all(metadata_prefix()) -> metadata_value()).
get_all(Prefix) ->
    riak_core_metadata:to_list(Prefix).

%%%-------------------------------------------------------------------
%% @doc
%% Put the data into the metadata,
%% @end
%%%-------------------------------------------------------------------
-spec(put(metadata_prefix(), metadata_key(),
    metadata_value() | metadata_modifier(), options()) -> ok).
put(Prefix, Key, Value) ->
    put(Prefix, Key, Value, []).
put(Prefix, Key, Value, Opts) ->
    riak_core_metadata:put(Prefix, Key, Value, Opts).

%%%-------------------------------------------------------------------
%% @doc
%% deleting the key from the metadata replaces values with tombstone
%% @end
%%%-------------------------------------------------------------------
-spec(delete(metadata_prefix(), metadata_key()) -> ok).
delete(Prefix, Key) ->
    riak_core_metadata:delete(Prefix, Key).


%%%===================================================================
%%% Main API
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%% Use riak_core_metadata:fold/4 to fold over the path in the
%% metadata and pull out the stats that match the STATUS and TYPE.
%% @end
%%%-------------------------------------------------------------------
-spec(find_entries(metrics(),status(),type()) -> listofstats()).
find_entries(Stats,Status,Type) ->
    lists:flatten(
        lists:map(
              fun(Stat) -> fold(Stat,Status,Type) end, Stats)).

%%%-------------------------------------------------------------------
%% @doc
%% Pass the StatName into the fold, to match ({match,Stat}) the objects
%% stored in the metadata, in the Prefix: @see :  ?STATPFX
%%
%% Guard is passed in with the Accumulator to pattern match to, in
%% order to return the stats needed.
%%%-------------------------------------------------------------------
-spec(fold(metricname(),status(),(type() | '_')) -> acc()).
%%%-------------------------------------------------------------------
%% Returns the same as exometer:find_entries ->
%%          [{Name,Type,Status}|...]
%% @end
%%%-------------------------------------------------------------------
fold(Stat, Status0, Type0) ->
    {Stats, Status0, Type0} =
        riak_core_metadata:fold(
            fun({Name, [#{status := MStatus, type := MType}]},
                    {Acc, Status, Type})
                when    (Status == MStatus  orelse Status == '_')
                andalso (MType  == Type     orelse Type   == '_')->
                    {[{Name, MType, MStatus}|Acc],Status,Type};

                (_Other, {Acc, Status, Type}) ->
                    {Acc, Status, Type}
            end,
                {[], Status0, Type0}, ?STATPFX, [{match, Stat}]),
    Stats.

%%%-------------------------------------------------------------------
%% @doc
%% Checks the metadata for the pkey provided
%% returns [] | Value
%% @end
%%%-------------------------------------------------------------------
-spec(check_meta(metadata_pkey()) -> metadata_value()).
check_meta(Stat) when is_list(Stat) ->
    check_meta(?STATKEY(Stat));
check_meta({Prefix, Key}) ->
    case get(Prefix, Key) of
        undefined -> % Not found, return empty list
            [];
        Value ->
            case find_unregister_status(Value) of
                false        -> Value;
                unregistered -> unregistered;
                Other        -> Other
            end
    end.

find_unregister_status('$deleted')                -> unregistered;
find_unregister_status(#{status := unregistered}) -> unregistered;
find_unregister_status(Map) when is_map(Map)      -> Map;
find_unregister_status(_)                         -> false.

%%%-------------------------------------------------------------------
%% @doc
%% In the case where one list should take precedent, which is most
%% likely the case when registering in both exometer and metadata, the
%% options hardcoded into the stats may change, or the primary kv for
%% stats' statuses switches, in every case, there must be an alpha.
%%
%% For this, the lists are compared, and when a difference is found
%% (i.e. the stat tuple is not in the betalist, but is in the alphalist)
%% it means that the alpha stat, with the key-value needs to be
%% returned in order to Keep order in the stats' configuration
%% @end
%%%-------------------------------------------------------------------
-spec(the_alpha_stat(Alpha :: list(), Beta :: list()) -> list()).
the_alpha_stat(Alpha, Beta) ->
    {_LeftOvers, AlphaStatList} =
        lists:foldl(fun
                        (AlphaStat, {BetaAcc, TheAlphaStats}) ->
                            %% is the Alpha stat also in Beta?
                            case lists:member(AlphaStat, BetaAcc) of
                                true ->
                                    %% nothing to be done.
                                    {BetaAcc,TheAlphaStats};
                                false ->
                                    {AKey, _O} = AlphaStat,
                                    {lists:keydelete(AKey,1,BetaAcc),
                                        [AlphaStat|TheAlphaStats]}
                            end
                    end, {Beta, []}, Alpha),
    AlphaStatList.

%%%-------------------------------------------------------------------

find_all_entries() ->
    [{Name, Status} ||
        {Name,_Type, Status} <- find_entries([[riak|'_']], '_', '_')].

%%%===================================================================
%%% Registration API
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%% Checks if the stat is already registered in the metadata, if not it
%% registers it, and pulls out the options for the status and sends it
%% back to go into exometer
%% @end
%%%-------------------------------------------------------------------
-spec(register(tuple_stat()) -> options()).
register({StatName, Type, Opts, Aliases}) ->
    register(StatName, Type, Opts, Aliases).
register(StatName,Type, Opts, Aliases) ->
    case check_meta(?STATKEY(StatName)) of
        [] -> %% Cannot find
            {Status, MOpts} = find_status(fresh, Opts),
            re_register(StatName,{Status,Type,MOpts,Aliases}),
            [{status,Status}|MOpts];
        unregistered -> []; %% return empty list for options

        MapValue = #{options := MOptions,status := MStatus} ->
            {Status, NewOpts} =
                find_status(re_reg,{MOptions,MStatus,Opts}),
            re_register(StatName,MapValue#{status=>Status,
                options => NewOpts}),
        NewOpts;
        _ -> lager:debug(
            "riak_stat_meta:register(StatInfo) ->
            Could not register stat:~n{~p,[{~p,~p,~p,~p}]}~n",
            [StatName,undefined,Type,Opts,Aliases]),
            []
    end.

%%%-------------------------------------------------------------------
%% @doc
%% Find the status of a stat coming into the metadata.
%% If it is the first time registering then 'fresh' is the one hit,
%% and the status is pulled out of the Options() given, if no
%% status can be found, assume enabled - like exometer.
%%
%% Otherwise it is being re-registered and the status in the metadata
%% will take precedence as the status is persisted.
%% @end
%%%-------------------------------------------------------------------
-type find_status_type() :: fresh | re_reg.
-spec(find_status(find_status_type(), options()) ->
                                     {status(),options()}).
find_status(fresh, Opts) ->
    case proplists:get_value(status,Opts) of
        undefined -> {enabled, Opts};
        Status    -> {{status,Status},  Opts}
    end;
find_status(re_reg, {MetaOpts, MStatus, InOpts}) ->
    case proplists:get_value(status, InOpts) of
        undefined ->
            {MStatus,
                the_alpha_opts([{status,MStatus}|MetaOpts], InOpts)};
        _Status ->
            {MStatus,
                the_alpha_opts([{status,MStatus}|MetaOpts], InOpts)}
    end.

%%%-------------------------------------------------------------------
%% @doc
%% Combining Metadata's Options -> Replacing the current tuples in the
%% incoming options with the new tuples in the metadata, all the other
%% options that are not stored in the metadata are ignored
%%
%% The main options stored in the metadata are the same options that
%% are given on . @see : the_alpha_stat
%% @end
%%%-------------------------------------------------------------------
the_alpha_opts(MetadataOptions, IncomingOptions) ->
    lists:foldl(
        fun
            ({Option,Value},Acc) ->
                case lists:keyfind(Option, 1,Acc) of
                    false -> [{Option,Value}|Acc];
                    {Option,_OtherVal} ->
                        lists:keyreplace(Option,1,Acc,{Option,Value})
                end
        end, IncomingOptions, MetadataOptions).

re_register(StatName,{Status,Type,Options,Aliases}) ->
    StatMap = ?STATMAP,
    Value = StatMap#{
        status => Status,
        type => Type,
        options => Options,
        aliases => Aliases},
    re_register(StatName,Value);
re_register(StatName, Value) -> %% ok
    put(?STATPFX, StatName, Value).

%%%===================================================================
%%% Updating API
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%% Changes the status of stats in the metadata
%% @end
%%%-------------------------------------------------------------------
-spec(change_status(metadata_key(), status()) -> ok | acc()).
change_status(Stats) when is_list(Stats) ->
    [change_status(Stat,Status)||{Stat,Status} <- Stats];
change_status({StatName, Status}) ->
    change_status(StatName, Status).
change_status(Statname, ToStatus) ->
    case check_meta(?STATKEY(Statname)) of
        []           -> []; %% doesn't exist
        unregistered -> []; %% unregistered
        MapValue ->
            put(?STATPFX,Statname,MapValue#{status=>ToStatus})
    end.

%%%-------------------------------------------------------------------
%% @doc
%% Setting the options in the metadata manually, such as
%% resets etc...
%% @end
%%%-------------------------------------------------------------------
-spec(set_options(metadata_key(), options()) -> ok).
set_options(StatInfo, NewOpts) when is_list(NewOpts) ->
    lists:foreach(fun({Key, NewVal}) ->
                        set_options(StatInfo, {Key, NewVal})
                  end, NewOpts);
set_options({Statname, {Status, Type, Opts, Aliases}}, {Key, NewVal}) ->
    NewOpts = lists:keyreplace(Key, 1, Opts, {Key, NewVal}),
    set_options(Statname, {Status, Type, NewOpts, Aliases});
set_options(StatName, {Status, Type, NewOpts, Aliases}) ->
    re_register(StatName, {Status, Type, NewOpts, Aliases}).


%%%===================================================================
%%% Deleting/Resetting Stats API
%%%===================================================================
%%%-------------------------------------------------------------------
%% @doc
%% reset the stat in exometer and notify metadata of its reset
%% @end
%%%-------------------------------------------------------------------
-spec(reset_stat(metadata_key()) -> ok | error()).
reset_stat(Statname) ->
    case check_meta(?STATKEY(Statname)) of
        [] -> ok; %% doesn't exist
        unregistered -> {error, unregistered};
        MapValue = #{status := enabled,options := Options} ->
            NewOpts = resets(Options),
            put(?STATPFX,Statname,MapValue#{options => NewOpts});
        _Otherwise -> ok %% If the stat is disabled it shouldn't reset
    end.

resets(Options) ->
    case proplists:get_value(resets,Options,0) of
        0 -> [{resets,1}|Options];
        V -> lists:keyreplace(resets,1,Options,{resets,reset_inc(V)})
    end.

reset_inc(Count) -> Count + 1.

%%%-------------------------------------------------------------------
%% @doc
%% Eventually the rest counter will get too high, and for every single
%% stat its a bit much, reset the reset count in the metadata. for a
%% fresh stat
%% @end
%%%-------------------------------------------------------------------
-spec(reset_resets() -> ok).
reset_resets() ->
    lists:foreach(fun({Stat, _Val}) ->
        #{status := Status,
            type := Type,
            options := Options,
            aliases := Aliases} = check_meta(?STATKEY(Stat)),
        set_options({Stat, {Status,Type,Options,Aliases}}, {resets, 0})
                  end, get_all(?STATPFX)).

%%%-------------------------------------------------------------------
%% @doc
%% Marks the stats as unregistered, that way when a node is restarted
%% and registers the stats it will be ignored
%% @end
%%%-------------------------------------------------------------------
-spec(unregister(metadata_key()) -> ok).
unregister(Statname) ->
    case check_meta(?STATKEY(Statname)) of
        MapValue = #{status := Status} when Status =/= unregistered ->
            %% Stat exists, re-register with unregistered - "status"
            put(Statname,MapValue#{status=>unregistered});
        _ -> ok
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%% Profile API %%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%-------------------------------------------------------------------
%% @doc
%% Take the stats and their status out of the metadata for the current
%% node and save it into the metadata as a profile - works on per node
%% @end
%%%-------------------------------------------------------------------
-spec(save_profile(profilename()) -> ok | error()).
save_profile(ProfileName) ->
    put(?PROFPFX, ProfileName, find_all_entries()),
    io:fwrite("Profile: ~s Saved~n", [profile_name(ProfileName)]).

%%%-------------------------------------------------------------------
%% @doc
%% Find the profile in the metadata and pull out stats to change them.
%% It will compare the current stats with the profile stats and will
%% change the ones that need changing to prevent errors/less expense
%% @end
%%%-------------------------------------------------------------------
-spec(load_profile(profilename()) -> ok | error()).
load_profile(ProfileName) ->
    case check_meta(?PROFILEKEY(ProfileName)) of
        {error, Reason} ->
            io:format("Error : ~p~n",[Reason]);
        [] ->
            io:format("Error: Profile does not Exist~n");
        ProfileStats ->
            CurrentStats = find_all_entries(),
            ToChange = the_alpha_stat(ProfileStats, CurrentStats),
            %% delete stats that are already enabled/disabled, any
            %% duplicates with different statuses will be replaced
            %% with the profile one
            change_stat_list_to_status(ToChange),
            put(?LOADEDPFX, ?LOADEDKEY, ProfileName),
            io:format("Loaded Profile: ~s~n",[profile_name(ProfileName)])

        %% the reason a profile is not checked to see if it is already
        %% loaded is because it is easier to "reload" an already loaded
        %% profile in the case the stats configuration is changed,
        %% rather than "unloading" the profile and reloading it to
        %% change many stats statuses unnecessarily

        %% consequentially, save -> load -> change -> load again
        %% would mean no stats would change if the profile is already
        %% loaded

    end.

change_stat_list_to_status(StatusList) ->
    riak_stat_mgr:change_status(StatusList).

profile_name(ProfileName) ->
    string:join(ProfileName," ").

%%%-------------------------------------------------------------------
%% @doc
%% Deletes the profile from the metadata, however currently the
%% metadata returns a tombstone for the profile, it can be overwritten
%% when a new profile is made of the same name, and in the profile
%% gen_server the name of the profile is "unregistered" so it can not
%% be reloaded again after deletion
%% @end
%%%-------------------------------------------------------------------
-spec(delete_profile(profilename()) -> ok).
delete_profile(ProfileName) ->
    case check_meta(?LOADEDPKEY) of
        ProfileName ->
            put(?LOADEDPFX, ?LOADEDKEY, ["none"]),
            delete(?PROFPFX, ProfileName),
            io:format("Profile Deleted : ~s~n",[profile_name(ProfileName)]);
        %% Load "none" in case the profile deleted is the one currently
        %% loaded. Does not change the status of the stats however.
        _Other ->
            case check_meta(?PROFILEKEY(ProfileName)) of
                [] ->
                    io:format("Error : Profile does not Exist~n"),
                    no_profile;
                _ ->
                    delete(?PROFPFX, ProfileName),
                    io:format("Profile Deleted : ~s~n",
                        [profile_name(ProfileName)])
        %% Otherwise the profile is found and deleted
            end
    end.

%%%-------------------------------------------------------------------
%% @doc
%% resets the profile by enabling all the stats, pulling out all the
%% stats that are disabled in the metadata and then changing them to
%% enabled in both the metadata and exometer
%% @end
%%%-------------------------------------------------------------------
-spec(reset_profile() -> ok | error()).
reset_profile() ->
    CurrentStats = find_all_entries(),
    put(?LOADEDPFX, ?LOADEDKEY, ["none"]),
    change_stats_from(CurrentStats, disabled),
    io:format("All Stats set to 'enabled'~n").

%% @doc change only disabled to enabled and vice versa @end
change_stats_from(Stats, Status) ->
    change_stat_list_to_status(
        lists:foldl(fun
                        ({Stat,CStatus},Acc) when CStatus == Status ->
                            NewStatus = reverse_status(CStatus),
                            [{Stat,NewStatus}|Acc];
                        (_, Acc) -> Acc
                    end, [], Stats)).

reverse_status(enabled) -> disabled;
reverse_status(disabled) -> enabled.