%%%-------------------------------------------------------------------
%%% @doc
%%% All things profile are in this gen_server, all calls from riak_stat
%%% arrive here, are manipulated and sent to riak_stat_coordinator to
%%% end up in the metadata.
%%% Sends data to exometer or metadata via riak_stat_coordinator
%%%
%%% save-profile <entry> ->
%%%     Saves the current stats and their status as a value to the
%%%     key: <entry>, in the metadata
%%%
%%% load-profile <entry> ->
%%%     Loads a profile that is saved in the metadata with the key
%%%     <entry>, pulls the stats and their status out of the metadata
%%%     and sends to riak_stat_coordinator to change the statuses
%%%
%%% delete-profile <entry> ->
%%%     Delete a profile <entry> from the metadata, the metadata leaves
%%%     the profile with a tombstone
%%%
%%% reset-profile ->
%%%     unloads the current profile and changes all the stats back to
%%%     enabled, no entry needed.
%%%
%%% pull_profiles() ->
%%%     Pulls the list of all the profiles saved in the metadata and
%%%     their stats
%%%
%%% when pretty printing profile names -> [<<"profile-one">>] use ~s token
%%%
%%% NOTE :
%%%     When creating a profile, you can make changes and then overwrite
%%%     the profile with the new stat configuration. However if the profile
%%%     is saved and then loaded, upon re-write the new profile is different
%%%     but still enabled as loaded, therefore it can not be loaded again
%%%     until it is deleted and created again, or the profile is reset
%%%
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(riak_core_stat_profiles).

-behaviour(gen_server).
-include_lib("riak_core/include/riak_core_stat.hrl").

%% API
-export([
  save_profile/1,
  load_profile/1,
  delete_profile/1,
  reset_profile/0,
  pull_profiles/0
]).

%% gen_server callbacks
-export([
  start_link/0,
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-define(SERVER, ?MODULE).
-define(timestamp, os:timestamp()).
-define(NODEID, riak_core_nodeid:get()).

-record(state, {
  profile = none, %% currently loaded profile
  profilelist     %% tableId for profile ETS table
}).

%%%===================================================================
%%% API
%%%===================================================================
%% -------------------------------------------------------------------
%% @doc
%% Profile names come in "as is" from riak_core_metadata, that is in
%% the format [<<"profile-name">>], meaning that saving a profile in riak
%% attach will work for any format as a name, as it is just the key, but
%% it cannot be loaded from console unless the format is of the same form
%% @end
%% -------------------------------------------------------------------


-spec(save_profile(profilename()) -> response()).
%% @doc
%% Data comes in already formatted unless nothing is entered, then the
%% response is 'no_data'.
%% Saves the profile name in the metadata with all the current stats and
%% their status as the value
%% multiple saves of the same name overwrites the profile
%% @end
save_profile(ProfileName) ->
  gen_server:call(?SERVER, {save, ProfileName}).

-spec(load_profile(profilename()) -> response()).
%% @doc
%% load a profile saved in the metadata, If the profile is not there it
%% will return {error, no_profile}, if nothing was entered it will return
%% the statement below. Stats will be pulled out of the metadata and
%% compared to the current status of the stats, It will only enable the
%% disabled and disable the enabled stats etc, any unregistered stats that
%% are saved in the profile will do nothing.
%% the return will be 'ok'
%% @end
load_profile(ProfileName) ->
  gen_server:call(?SERVER, {load, ProfileName}).

-spec(delete_profile(profilename()) -> response()).
%% @doc
%% deletes the profile from the metadata and all its values but it does
%% not affect the status of the stats, metadata does not remove an entry
%% but does replace the data with a tombstone
%% @end
delete_profile(ProfileName) ->
  gen_server:call(?SERVER, {delete, ProfileName}).

-spec(reset_profile() -> response()).
%% @doc
%% resets the profile so no profile is loaded and will enable all the
%% disabled stats. it returns a ok if everything went ok, or
%% {error, Reason}
%% @end
reset_profile() ->
  gen_server:call(?SERVER, reset).


%%--------------------------------------------------------------------
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  io:format("riak_stat_profiles:start_link()~n"),
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec(init(Args :: term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  io:format("riak_stat_profiles:init([])~n"),
%%    Profiles = pull_profiles(), % Pull out already saved profiles in the metadata
  Tid =                       % create ets for profiles
  ets:new(profiles, [
    set,
    protected,
    named_table,
    {keypos, 1},
    {write_concurrency, true},
    {read_concurrency, true}
  ]),
  io:format("riak_stat_profiles:init() -> ets:new = ~p~n", [Tid]),
  Loaded = last_loaded_profile(),
  io:format("riak_stat_profiles:init() -> loaded_profile = ~p~n", [Loaded]),
  case pull_profiles() of
    false -> ok;
    Profiles ->
      Ets = ets:insert(Tid, Profiles),
      io:format("riak_stat_profiles:init() -> pull_profile: ~p~n", [Profiles]),
      io:format("riak_stat_profiles:init() -> ets:insert = ~p~n", [Ets])
  end,
  case Loaded of %% load last profile that was loaded
    [<<"none">>] -> [];
    false -> ok;
    Other ->
%%            io:format("Other: ~p~n", [Other]),
      gen_server:call(?SERVER, {load, Other})
  end,

  {ok, #state{profilelist = Tid}}.

%%--------------------------------------------------------------------

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_call({_Fun, []}, _From, State) ->
  {reply, {error, no_data}, State};
handle_call({_Fun, [<<>>]}, _From, State) ->
  {reply, {error, no_data}, State};
handle_call({save, Arg}, _From, State = #state{profilelist = ProfileList}) ->
  ets:insert(ProfileList, {Arg, ?timestamp}),
  save_profile_(Arg),
  {reply, ok, State};
handle_call({load, Arg}, _From, State =
  #state{profile = Profile, profilelist = ProfileList}) ->
  {Reply, NewState} =
    case Profile == Arg of
      true ->
        {io:format("~s already loaded~n", [Profile]), State};
      false ->
        case ets:lookup(ProfileList, Arg) of
          []              -> {{error, no_profile}, State};
          {Name, _time} -> {load_profile_(Name), State#state{profile = Name}}
        end
    end,
  {reply, Reply, NewState};
handle_call({delete, Arg}, _From, State =
  #state{profile = Profile, profilelist = ProfileList}) ->
  NewState =
    case Profile == Arg of
      true -> State#state{profile = none};
      false -> State
    end,
  Reply =
    case ets:lookup(ProfileList, Arg) of
      []              -> {error, no_profile};
      {Name, _time} -> delete_profile_(Name)
    end,
  {reply, Reply, NewState};
handle_call(reset, _From, State) ->
  Reply = reset_profile_(),
  NewState = State#state{profile = none},
  {reply, Reply, NewState};

handle_call({Request, _Arg}, _From, State) ->
  {reply, {error, Request}, State};
handle_call(Request, _From, State) ->
  {reply, {error, Request}, State}.

%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).

handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------

-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).

handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------

-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------

-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Coordinator API
%%%===================================================================

save_profile_(ProfileName) ->
  riak_core_stat_coordinator:save_profile(ProfileName).

load_profile_(ProfileName) ->
  riak_core_stat_coordinator:load_profile(ProfileName).

delete_profile_(ProfileName) ->
  riak_core_stat_coordinator:delete_profile(ProfileName).

reset_profile_() ->
  riak_core_stat_coordinator:reset_profile().

pull_profiles() ->
  riak_core_stat_coordinator:get_profiles().

last_loaded_profile() ->
  riak_core_stat_coordinator:get_loaded_profile().


%%%-------------------------------------------------------------------