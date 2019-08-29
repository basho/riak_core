%%%-------------------------------------------------------------------
%%% @doc
%%% riak_core_stat_metadata is the middle-man for stat and
%%% riak_core_metadata. All information that needs to go into or out
%%% of the metadata will always go through this module.
%%%
%%% Profile Prefix: {profiles, list}
%%% Loaded Prefix:  {profiles, loaded}
%%% Stats Prefix:   {stats,    nodeid()}
%%%
%%% Profile metadata-pkey: {{profiles, list}, [<<"profile-name">>]}
%%% Profile metadata-val : [{Stat, {status, Status},...]
%%%
%%% Loaded metadata-pkey : {{profiles, loaded}, nodeid()}
%%% Loaded metadata-val  : [<<"profile-name">>]
%%%
%%% Stats metadata-pkey: {{stats, nodeid()}, [riak,riak_kv,...]}
%%% Stats metadata-val : {enabled, spiral, [{resets,...},{vclock,...}], [Aliases]}
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(riak_core_stat_metadata).

-include_lib("riak_core/include/riak_core_stat.hrl").
-include_lib("riak_core/include/riak_core_metadata.hrl").

%% Basic API
-export([
    get/2,
    get/3,
    put/3,
    put/4,
    get_all/1,
    delete/2,
    find_entries/4,
    dp_get/2,
    get_dps/2
]).

%% API
-export([
    check_meta/1,
    check_status/1,
    change_status/1,
    change_status/2,
    set_options/2
]).

%% Admin API
-export([
    register_stat/4,
    register_stat/1,
    unregister/1,
    reset_stat/1,
    reset_resets/0
]).

%% Profile API
-export([
    get_profiles/0,
    save_profile/1,
    load_profile/1,
    delete_profile/1,
    reset_profile/0,
    get_loaded_profile/0
]).

%% Stats are on per node basis
-define(STAT,                  stats).
-define(STATPFX,              {?STAT, ?NODEID}).
-define(STATKEY(StatName),    {?STATPFX, StatName}).
-define(NODEID,                term_to_binary(node())).

%% Profiles are Globally shared
-define(PROF,                  profiles).
-define(PROFID,                list).
-define(PROFPFX,              {?PROF, ?PROFID}).
-define(PROFILEKEY(Profile),  {?PROFPFX, Profile}).
-define(LOADEDPFX,            {?PROF, loaded}).
-define(LOADEDKEY,             ?NODEID).
-define(LOADEDPKEY,           {?LOADEDPFX, ?LOADEDKEY}).

%%%===================================================================
%%% Basic API
%%%===================================================================

-spec(get(metadata_prefix(), metadata_key()) -> metadata_value() | undefined).
%% @doc
%% Get the data from the riak_core_metadata, If not Opts are passed then an empty
%% list is given and the defaults are set in the riak_core_metadata.
%% it's possible to do a select pattern in the options under the form:
%%      {match, ets:match_spec}
%% Which is pulled out in riak_core_metadata and used in an ets:select,
%% @end
get(Prefix, Key) ->
  get(Prefix, Key, []).
get(Prefix, Key, Opts) ->
  riak_core_metadata:get(Prefix, Key, Opts).

-spec(put(metadata_prefix(), metadata_key(), metadata_value() | metadata_modifier(), options()) -> ok).
%% @doc
%% put the data into the metadata, options contain the {match, Match_spec}
%% @end
put(Prefix, Key, Value) ->
  put(Prefix, Key, Value, []).
put(Prefix, Key, Value, Opts) ->
  riak_core_metadata:put(Prefix, Key, Value, Opts).

-spec(get_all(metadata_prefix()) -> metadata_value()).
%% @doc
%% Give a Prefix for anything in the metadata and get a list of all the
%% data stored under that prefix
%% @end
get_all(Prefix) ->
  riak_core_metadata:to_list(Prefix).

-spec(delete(metadata_prefix(), metadata_key()) -> ok).
%% @doc
%% deleting the key from the metadata replaces values with tombstone
%% @end
delete(Prefix, Key) ->
  riak_core_metadata:delete(Prefix, Key).


-spec(find_entries(stats(), status(), type(), datapoint()) -> stats()).
%% @doc
%% Use an ets:select to find the stats in the metadata of the same status as
%% the one given, default at riak_core_console level - is enabled.
%% @end
find_entries(Stats, Status, Type, DP) ->
  lists:flatten(
  lists:map(fun(Stat) ->
    fold(Stat, Status, Type, DP)
            end, Stats)).

-spec(fold(statname(),(enabled | disabled | '_'),(type() | '_'), (datapoint() | [])) -> acc()).
%% @doc
%% Using riak_core_metadata the statname(s) is passed in a tuple: {match,Name} which will
%% return the objects that match in the metadata in order to fold through in the iterator, this
%% iterates over the ?STATPFX : {stats,term_to_binary(node())}, and fold over the objects returned
%% and depending on the Status, Type or DataPoints (DP) requested, it will be guarded and then returned
%% in the accumulator.
%%
%% Some objects have a tuple 3 or 2 value, and some of the tuple-3 values store the aliases in the
%% Options (O), encompasses any possible Value that may have the aliases stored in a different place.
%%
%% The Aliases are the names of the DPs for those specific stats, if the stat does not have any aliases
%% for the data points requested it will not be returned.
%% @end
fold(Stat, Status0, Type0, DP0) ->
  {Acc1, _Status, _Type, _DP} =
  riak_core_metadata:fold(fun
                          %%%%%%% 4 - tuple
                          %%% The Type and DataPoints are not given so only the name and status is to be returned:
                            ({Name, [{MStatus,_Type,_Opts,_Aliases}]},{Acc,Status,'_',[]})
                              when Status == '_' orelse MStatus == Status -> %% Status :: enabled | disabled | '_'
                              {[{Name, MStatus}|Acc],Status,'_',[]};

                          %%% The DataPoints are not given so the Name, Status and Type needs to be returned:
                            ({Name, [{MStatus,MType,_Opts,_Aliases}]},{Acc,Status,Type,[]})
                              when (Type == '_' orelse MType == Type)
                              andalso (Status == '_' orelse MStatus == Status) ->
                              {[{Name,MType,MStatus}|Acc],Status,Type,[]};

                            ({Name, [{MStatus,MType,_MOpts,MAliases}]},{Acc,Status,Type,DPs})
                              when (Type == '_' orelse MType == Type)
                              andalso (Status == '_' orelse MStatus == Status)
                              andalso MAliases =/= [] -> %% aliases aren't stored in options and isn't empty

                              Result = riak_core_stat_metadata:dp_get(DPs,MAliases),

                              case lists:flatten(Result) of
                                [] -> {Acc, Status,Type,DPs};
                                Aliases -> {[{Name,MType,MStatus,Aliases}|Acc],Status,Type,DPs}
                              end;


                            %%%%%%% 3 - tuple
                            %%% The Type and DataPoints are not given so only the name and status is to be returned:
                            ({Name, [{MStatus,_Type,_Opts}]},{Acc,Status,'_',[]})
                              when Status == '_' orelse MStatus == Status -> %% Status :: enabled | disabled | '_'
                              {[{Name, MStatus}|Acc],Status,'_',[]};

                            %%% The DataPoints are not given so the Name, Status and Type needs to be returned:
                            ({Name, [{MStatus,MType,_Opts}]},{Acc,Status,Type,[]})
                              when (Type == '_' orelse MType == Type)
                              andalso (Status == '_' orelse MStatus == Status) ->
                              {[{Name,MType,MStatus}|Acc],Status,Type,[]};

                            ({Name, [{MStatus,MType,MOpts}]},{Acc,Status,Type,DPs})
                              when (Type == '_' orelse MType == Type)
                              andalso (Status == '_' orelse MStatus == Status) ->

                              MAliases = proplists:get_value(aliases,MOpts,[]),
                              Result = riak_core_stat_metadata:dp_get(DPs,MAliases),

                              case lists:flatten(Result) of
                                [] -> {Acc, Status,Type,DPs};
                                Aliases -> {[{Name,MType,MStatus,Aliases}|Acc],Status,Type,DPs}
                              end;

                            %%%%%%% 2 - tuple
                            %%% The Type and DataPoints are not given so only the name and status is to be returned:
                            ({Name, [{MStatus,_Type}]},{Acc,Status,'_',[]})
                              when Status == '_' orelse MStatus == Status -> %% Status :: enabled | disabled | '_'
                              {[{Name, MStatus}|Acc],Status,'_',[]};

                            %%% The DataPoints are not in this stat, Status and Type needs to be returned:
                            ({Name, [{MStatus,MType}]},{Acc, Status, Type, DP})
                              when (Type == '_' orelse MType == Type)
                              andalso (Status == '_' orelse MStatus == Status)  ->
                              {[{Name, MType, MStatus}|Acc], Status, Type, DP};


                            %% Otherwise
                            (_Other, {Acc, Status, Type, DP}) ->
                              {Acc, Status, Type, DP}

                                end, {[], Status0, Type0, DP0}, ?STATPFX, [{match, Stat}]), Acc1.

dp_get(DPs, Aliases) ->
  lists:foldl(fun
                ({_,[]},Ac) -> Ac;
                (Valid, Ac) -> [Valid|Ac]
              end,[],[riak_core_stat_metadata:get_dps(DP,Aliases) || DP <- DPs]).

get_dps(DP, Aliases) ->
  case proplists:get_value(DP, Aliases, []) of
    [] -> [];
    V -> {DP,V}
  end.

find_all_entries() ->
  Stats = get_all(?STATPFX),
  [{Name, {status, Status}} || {Name, Status} <- find_entries(Stats, '_', '_',[])].

%%%===================================================================
%%% Profile API
%%%===================================================================

-spec(save_profile(profilename()) -> ok | error()).
%% @doc
%% Take the stats and their status out of the metadata for the current
%% node and save it into the metadata as a profile - works on per node
%% @end
save_profile(ProfileName) ->
    put(?PROFPFX, ProfileName, find_all_entries()).

-spec(load_profile(profilename()) -> ok | error()).
%% @doc
%% Find the profile in the metadata and pull out stats to change them.
%% It will compare the current stats with the profile stats and will
%% change the ones that need changing to prevent errors/less expense
%% @end
load_profile(ProfileName) ->
  case check_meta(?PROFILEKEY(ProfileName)) of
    {error, Reason} ->
      {error, Reason};
    ProfileStats ->
      CurrentStats = find_all_entries(),
      ToChange = the_alpha_stat(ProfileStats, CurrentStats),
      %% delete stats that are already enabled/disabled, any duplicates
      %% with different statuses will be replaced with the profile one
      change_stat_list_to_status(ToChange), %% todo: use the metadata fold to change the stats
      put(?LOADEDPFX, ?LOADEDKEY, ProfileName)
  end.

-spec(the_alpha_stat(Alpha :: list(), Beta :: list()) -> term()).
%% @doc
%% In the case where one list should take precedent, which is most
%% likely the case when registering in both exometer and metadata, the options
%% hardcoded into the stats may change, or the primary kv for stats statuses
%% switches, in every case, there must be an alpha.
%% @end
the_alpha_stat(Alpha, Beta) ->
% The keys are sorted first with ukeysort which deletes duplicates, then merged
% so any key with the same stat name that is both enabled and disabled returns
% only the enabled option, where it is enabled in the alpha.
  AlphaList = the_alpha_map(Alpha),
  BetaList = the_alpha_map(Beta),
  {_Nout, ListtoChange} =
    lists:foldr(fun(Stat, {List1, List2}) -> %% todo: revamp this fold
      case lists:member(Stat, List1) of
        true ->
          {List1, List2};
        false ->
          {List1, [Stat | List2]}
      end
                end, {BetaList, []}, AlphaList),
  ListtoChange.
% The stats must fight, to become the alpha

the_alpha_map(A_B) ->
  lists:map(fun
              ({Stat, {Atom, Val}}) -> {Stat, {Atom, Val}};
              ({Stat, Val})         -> {Stat, {atom, Val}};
              ([]) -> []
            end, A_B).


change_stat_list_to_status(StatusList) -> %% todo: change the name of this function to some generic
  riak_core_stat_coordinator:change_status(StatusList).


-spec(delete_profile(profilename()) -> ok).
%% @doc
%% Deletes the profile from the metadata, however currently the metadata
%% returns a tombstone for the profile, it can be overwritten when a new profile
%% is made of the same name, and in the profile gen_server the name of the
%% profile is "unregistered" so it can not be reloaded again after deletion
%% @end
delete_profile(ProfileName) ->
  case check_meta(?LOADEDPKEY) of
    ProfileName -> %% make this a guard instead of a pattern match
      put(?LOADEDPFX, ?LOADEDKEY, [<<"none">>]),
      delete(?PROFPFX, ProfileName);
    _ ->
      delete(?PROFPFX, ProfileName)
  end.


-spec(reset_profile() -> ok | error()).
%% @doc
%% resets the profile by enabling all the stats, pulling out all the stats that
%% are disabled in the metadata and then changing them to enabled in both the
%% metadata and exometer
%% @end
reset_profile() ->
  CurrentStats =
  put(?LOADEDPFX, ?LOADEDKEY, [<<"none">>]),
  change_stats_from(CurrentStats, disabled).
% change from disabled to enabled


change_stats_from(Stats, Status) ->
  change_stat_list_to_status( %% todo: make this using metadata fold
    lists:foldl(fun
                  ({Stat, {status, St}}, Acc) when St == Status ->
                    NewSt =
                      case Status of
                        enabled -> disabled;
                        disabled -> enabled
                      end,
                    [{Stat, {status, NewSt}} | Acc];
                  ({_Stat, {status, St}}, Acc) when St =/= Status ->
                    Acc
                end, [], Stats)).


-spec(get_profiles() -> metadata_value()).
%% @doc
%% returns a list of the profile names stored in the metadata
%% @end
get_profiles() ->
  get_all(?PROFPFX).

-spec(get_loaded_profile() -> profilename()).
%% @doc
%% get the profile that is loaded in the metadata
%% @end
get_loaded_profile() ->
  get(?LOADEDPFX, ?LOADEDKEY).


%%%===================================================================
%%% API
%%%===================================================================

-spec(check_meta(metadata_pkey()) -> metadata_value()).
%% @doc
%% Checks the metadata for the pkey provided
%% returns [] | Value
%% @end
check_meta(Stat) when is_list(Stat) ->
  check_meta(?STATKEY(Stat));
check_meta({Prefix, Key}) ->
  case get(Prefix, Key) of
    undefined -> % Not found, return empty list
      [];
    Value ->
      case find_unregister_status(Key, Value) of
        false ->
          Value;
        unregistered -> unregistered;
        _            -> Value
      end
  end.

find_unregister_status(_K, '$deleted') ->
  unregistered;
find_unregister_status(_SN, {Status, _T, _Opts, _A}) ->
  Status; % enabled | disabled =/= unregistered
find_unregister_status(_PN, _Stats) ->
  false.

%%%%%%%%%% READING OPTS %%%%%%%%%%%%

-spec(check_status(metadata_key()) -> metadata_value() | error()).
%% @doc
%% Returns the status of the stat saved in the metadata
%% @end
check_status(StatName) ->
  case check_meta(?STATKEY(StatName)) of
    {Status, _Type, _Opts, _Aliases} ->
      {StatName, {status, Status}};
    _ ->
      {error, no_stat}
  end.

-spec(change_status(metadata_key(), status()) -> ok | acc()).
%% @doc
%% Changes the status in the metadata
%% @end
change_status(Stats) when is_list(Stats) ->
  lists:foldl(fun
                ({Stat, {status, Status}}, Acc) ->
                  [change_status(Stat, Status) | Acc];
                ({Stat, Status}, Acc) ->
                  [change_status(Stat, Status) | Acc]
              end, [], Stats);
change_status({StatName, Status}) ->
  change_status(StatName, Status).
change_status(Statname, ToStatus) ->
  case check_meta(?STATKEY(Statname)) of
    [] ->
      [];
    unregistered ->
      [];
    {_Status, Type, Opts, Aliases} ->
      put(?STATPFX, Statname, {ToStatus, Type, Opts, Aliases})
  end.

%%%%%%%%%% SET OPTIONS %%%%%%%%%%%%%

-spec(set_options(metadata_key(), options()) -> ok).
%% @doc
%% Setting the options in the metadata manually, such as
%% resets etc...
%% @end
set_options(StatInfo, NewOpts) when is_list(NewOpts) ->
  lists:foreach(fun({Key, NewVal}) ->
    set_options(StatInfo, {Key, NewVal})
                end, NewOpts);
set_options({Statname, {Status, Type, Opts, Aliases}}, {Key, NewVal}) ->
  NewOpts = lists:keyreplace(Key, 1, Opts, {Key, NewVal}),
  NewOpts2 = fresh_clock(NewOpts),
  set_options(Statname, {Status, Type, NewOpts2, Aliases});
set_options(StatName, {Status, Type, NewOpts, Aliases}) ->
  re_register_stat(StatName, {Status, Type, NewOpts, Aliases}).

fresh_clock(Opts) ->
  case lists:keysearch(vclock, 1, Opts) of
    false ->
      [{vclock, clock_fresh(?NODEID, 0)} | Opts];
    {value, {vclock, [{Node, {Count, _VC}}]}} ->
      lists:keyreplace(vclock, 1, Opts, {vclock, clock_fresh(Node, Count)});
    _ ->
      [{vclock, clock_fresh(?NODEID, 0)} | Opts]
  end.

clock_fresh(Node, Count) ->
  vclock:fresh(Node, vc_inc(Count)).
vc_inc(Count) -> Count + 1.

%%%===================================================================
%%% Admin API
%%%===================================================================


%%%%%%%%%%%% REGISTERING %%%%%%%%%%%%

register_stat({StatName, Type, Opts, Aliases}) ->
  register_stat(StatName, Type, Opts, Aliases).
-spec(register_stat(metadata_key(), type(), options(), aliases()) -> ok | options()).
%% @doc
%% Checks if the stat is already registered in the metadata, if not it
%% registers it, and pulls out the options for the status and sends it
%% back to go into exometer
%% @end
register_stat(StatName, Type, Opts, Aliases) ->
  case check_meta(?STATKEY(StatName)) of % check registration
    [] -> % if not registered return default Opts
      {Status, MetaOpts} = find_status(fresh, Opts),
      re_register_stat(StatName, {Status, Type, [{vclock, vclock:fresh(?NODEID, 1)} | MetaOpts], Aliases}),
      Opts;
    unregistered -> [];
    {MStatus, Type, MetaOpts, Aliases} -> % if registered
      {Status, NewMetaOptions, NewOpts} = find_status(re_reg, {Opts, MStatus, MetaOpts}),
      re_register_stat(StatName, {Status, Type, NewMetaOptions, Aliases}),
      NewOpts;
    _ ->
      lager:debug("riak_stat_meta_mgr:register_stat --
            Could not register the stat:~n{{~p,~p},~p,{~p,~p,~p}}~n",
        [?NODEID, ?STAT, StatName, Type, Opts, Aliases])
  end.

re_register_stat(StatName, StatValue) -> % returns -> ok.
  put(?STATPFX, StatName, StatValue).

%% @doc
%% The Finds the option for status in the metaopts, for first time registration
%% should return false, in which case the options given are returned.
%% else the Status from the metadata takes precedent and is returned ontop of the
%% opts given
%% @end
find_status(fresh, Opts) ->
  case proplists:get_value(status, Opts) of
    undefined -> {enabled, Opts}; % default is enabled, with original opts in
    Status    -> {Status, lists:keydelete(status, 1, Opts)} % set status and add options w/o status
  end;
find_status(re_reg, {Opts, MStatus, MOpts}) ->
  case proplists:get_value(status, Opts) of
    undefined ->
      {MStatus, the_alpha_stat(MOpts, Opts), [{status, MStatus} | Opts]};
    _Status    ->
      {MStatus, the_alpha_stat(MOpts, Opts), lists:keyreplace(status, 1, Opts, {status, MStatus})}
  end.

%%%%%%%%%% UNREGISTERING %%%%%%%%%%%%

-spec(unregister(metadata_key()) -> ok).
%% @doc
%% Marks the stats as unregistered, that way when a node is restarted and registers the
%% stats it will ignore stats that are marked unregistered
%% @end
unregister(Statname) ->
  case check_meta(?STATKEY(Statname)) of
    {_Status, Type, MetaOpts, Aliases} ->
      re_register_stat(Statname, {unregistered, Type, MetaOpts, Aliases});
    _ -> ok
  end.

%%%%%%%%% RESETTING %%%%%%%%%%%

-spec(reset_stat(metadata_key()) -> ok | error()).
%% @doc
%% reset the stat in exometer and notify metadata of its reset
%% @end
reset_stat(Statname) ->
  case check_meta(?STATKEY(Statname)) of
    [] -> ok;
    unregistered -> {error, unregistered};
    {_Status, Type, Opts, Aliases} ->
      Resets= proplists:get_value(resets, Opts),
      Options = [{resets, reset_inc(Resets)}],
      set_options({Statname, {enabled, Type, Opts, Aliases}}, Options)
  end.

reset_inc(Count) -> Count + 1.

-spec(reset_resets() -> ok).
%% @doc
%% sometimes the reset count just gets too high, and for every single
%% stat its a bit much
%% @end
reset_resets() ->
  lists:foreach(fun({Stat, _Val}) ->
    {Status, Type, Opts, Aliases} = check_meta(?STATKEY(Stat)),
    set_options({Stat, {Status, Type, Opts, Aliases}}, {resets, 0})
                end, get_all(?STATPFX)).


%% for 4-tuple value, Where Status matches and the Type doesn't matter
%%                            ({Name, [{MStatus,_Type,_O,Aliases}]},{Acc, Status, '_', DP}) when MStatus == Status; Status == '_' ->
%%                                case lists:flatten([{D,proplists:get_value(D,Aliases,[])} || D <- DP]) of
%%                                  {_,[]} -> {Acc, Status, '_', DP};
%%                                  Alia -> {[{Name, [], MStatus, Alia}|Acc], Status, '_', DP}
%%                                end;
%% return the aliases if the DPs for them are given, otherwise -> []


%% for 4-tuple value, Where Status and Type matches (are requested)
%%                            ({Name, [{MStatus,MType,_O,Aliases}]},{Acc, Status, Type, DP})
%%                              when MType == Type andalso (MStatus == Status orelse Status == '_')  ->
%%
%%                              case lists:flatten([{D,proplists:get_value(D,Aliases,[])} || D <- DP]) of
%%                                {_,[]} -> {Acc, Status, Type, DP};
%%                                Alia -> {[{Name, MType, MStatus, Alia}|Acc], Status, '_', DP}
%%                              end;


%% for 3-tuple value, Where Status matches and the Type doesn't matter
%%                            ({Name, [{MStatus,_Type,O}]},{Acc, Status, '_', DP}) when MStatus == Status; Status == '_' ->
%%
%%                              Value = proplists:get_value(aliases,O, []),
%%                                case lists:flatten([{D,proplists:get_value(D,Value,[])} || D <- DP]) of
%%                                  {_,[]} -> {Acc, Status, '_', DP};
%%                                  Alia -> {[{Name, [], MStatus, Alia}|Acc], Status, '_', DP}
%%                                end;


%% for 3-tuple value, Where Status and Type matches
%%                            ({Name, [{MStatus,MType,O}]},{Acc, Status, Type, DP})
%%                              when MType == Type andalso (MStatus == Status orelse Status == '_')  ->
%%
%%                              Value = proplists:get_value(aliases,O, []),
%%                              case lists:flatten([{D,proplists:get_value(D,Value,[])} || D <- DP]) of
%%                                {_,[]} -> {Acc, Status, Type, DP};
%%                                Alia -> {[{Name, MType, MStatus, Alia}|Acc], Status, Type, DP}
%%                              end;


%%                            %% for 2-tuple value, Where Status matches and Type don't matter
%%                            ({Name, [{MStatus,_Type}]},{Acc, Status, '_', DP}) when MStatus == Status; Status == '_' ->
%%
%%                              {[{Name, [], MStatus, []}|Acc], Status, '_', DP}; %% aliases can't be retrieved, return []


%%                            %% for 2-tuple value, Where Status and Type both match
%%                            ({Name, [{MStatus,MType}]},{Acc, Status, Type, DP})%
%%                              when MType == Type andalso (MStatus == Status orelse Status == '_')  ->
%%
%%                              {[{Name, MType, MStatus, []}|Acc], Status, Type, DP};



