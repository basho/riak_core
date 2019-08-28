%%%-------------------------------------------------------------------
%%% @doc
%%% Profiles are the "saved setup of stat configuration", basically, if
%%% the current statuses of the stats are in a state you would like to repeat
%%% either for testing or default preferences they can be saved into the
%%% metadata and gossiped to all nodes. Thus allowing a profile for one node to
%%% be mimic'ed on all nodes in the cluster.
%%%
%%% This means - unfortunately, trying to save the nodes setup (if the
%%% state of the stats status is different on all or some nodes) at once
%%% with the same name, the LWW and the nodes individual setup could be
%%% overwritten, all setups are treated as a globally registered setup that
%%% can be enabled on one or all nodes in a cluster. i.e. saving a clusters
%%% setup should be done on a per node basis
%%%
%%% save-profile <entry> ->
%%%     Saves the current stats and their status as a value to the
%%%     key: <entry>, in the metadata
%%%
%%% load-profile <entry> ->
%%%     Loads a profile that is saved in the metadata with the key
%%%     <entry>, pulls the stats and their status out of the metadata
%%%     and sends to riak_core_stat_coordinator to change the statuses
%%%
%%% delete-profile <entry> ->
%%%     Delete a profile <entry> from the metadata, the metadata leaves
%%%     the profile with a tombstone, does not effect the current configuration
%%%     if it is the currently loaded profile
%%%
%%% reset-profile ->
%%%     unloads the current profile and changes all the stats back to
%%%     enabled, no entry needed. Does not delete any profiles
%%%
%%% pull_profiles() ->
%%%     Pulls the list of all the profiles saved in the metadata and
%%%     their [{Stat, {status, Status}}] list
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
%%%     for example:
%%% N1: riak admin stat save-profile test-profile
%%%         -- saves the profile and the current setup in metadata --
%%% N2: riak admin stat load-profile test-profile
%%%         -- loads the profile that is saved, is recognised as the current profile --
%%% N1: riak admin stat disable riak.riak_kv.**
%%% N1: riak admin stat save-profile test-profile
%%%         -- changes the setup of the stats and rewrites the current profile saved --
%%% N2: riak admin stat load-profile test-profile
%%%     > "profile already loaded"
%%%         -- even though the setup has changed this profile is currently loaded in the
%%%             old configuration, it will not change the stats to the new save unless
%%%             it is unloaded and reloaded again --.
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
-define(NODEID, term_to_binary(node())).

-record(state, {
    profile = none, %% currently loaded profile
    profiletable    %% tableId for profile ETS table
}).

%%%===================================================================
%%% API
%%%===================================================================
%% -------------------------------------------------------------------
%% @doc
%% Profile names come in "as is" from riak_core_console, that is in
%% the format [<<"profile-name">>] or ["profile-name"],
%% meaning that saving a profile in the shell will work for any format
%% as a name, as it is just the key, but it cannot be loaded from console
%% unless the format is of the same form. i.e. saving it from console
%% saves it in binary form, saving from shell can be of any format, if
%% it is to be loaded from console then it must be of the same input format
%% entered from "riak admin stat load-profile ...."
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
    case check_args(ProfileName) of
        ok -> ok;
        Args -> print(gen_server:call(?SERVER, {save, Args}), Args++" saved")
    end.



-spec(load_profile(profilename()) -> response()).
%% @doc
%% load a profile saved in the metadata, If the profile is not there it
%% will return {error, no_profile}, if nothing was entered it will return
%% the statement below. Stats will be pulled out of the metadata and
%% compared to the current status of the stats, It will only enable the
%% disabled stats and disable the enabled stats etc, any unregistered stats that
%% are saved in the profile will do nothing - will be ignored.
%% the return will be 'ok' | {error, Reason}
%% @end
load_profile(ProfileName) ->
    case check_args(ProfileName) of
        ok -> ok;
        Args -> print(gen_server:call(?SERVER, {load, Args}), Args++" loaded")
    end.


-spec(delete_profile(profilename()) -> response()).
%% @doc
%% deletes the profile from the metadata and all its values but it does
%% not affect the status of the stats, metadata does not remove an entry
%% but does replace the data with a tombstone
%% @end
delete_profile(ProfileName) ->
    case check_args(ProfileName) of
        ok -> ok;
        Args -> print(gen_server:call(?SERVER, {delete, Args}), Args++" deleted")
    end.

-spec(reset_profile() -> response()).
%% @doc
%% resets the profile so no profile is loaded and will enable all the
%% disabled stats. it returns 'ok' if everything went ok, or
%% {error, Reason}
%% @end
reset_profile() ->
    gen_server:call(?SERVER, reset).


%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([]) ->
    Tid =
    ets:new(profiles, [
        set,
        protected,
        named_table,
        {keypos, 1},
        {write_concurrency, true},
        {read_concurrency, true}
    ]),
    %% the ets table is a process kept alive by this gen_server, to keep track of profiles
    case pull_profiles() of
        false -> ok;
        Profiles ->
            ets:insert(Tid, Profiles)
    end,
    case last_loaded_profile() of %% load last profile that was loaded
        [<<"none">>] -> ok;
        false -> ok;
        undefined -> ok;
        Other ->
            gen_server:call(?SERVER, {load, Other})
    end,

    {ok, #state{profiletable = Tid}}.

%%--------------------------------------------------------------------

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).

handle_call({save, Arg}, _From, State = #state{profiletable = ProfileList}) ->

    ets:insert(ProfileList, {Arg, ?timestamp}),

    {reply, save_profile_(Arg), State};


handle_call({load, Arg}, _From, State =
    #state{profile = Profile, profiletable  = ProfileList}) ->
    {Reply, NewState} =
        case Profile == Arg of %% check if the current loaded is same as argument given
            true ->
                {io_lib:format("~s already loaded~n", [Profile]), State};
            false ->
                case ets:lookup(ProfileList, Arg) of
                    [] -> {{error, no_profile}, State};
                    {Name, _time} -> {load_profile_(Name), State#state{profile = Name}};
                    Other -> {load_profile_(Other), State#state{profile = Other}}
                end
        end,
    {reply, Reply, NewState};
handle_call({delete, Arg}, _From, State =
    #state{profile = Profile, profiletable = ProfileList}) ->
    NewState =
        case Profile == Arg of
            true -> State#state{profile = none};
            false -> State
        end,
    Reply =
        case ets:lookup(ProfileList, Arg) of
            [] -> {error, no_profile};
            {Name, _time} -> delete_profile_(Name);
            Other -> {delete_profile_(Other), State#state{profile = Other}}

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

%%%===================================================================
%%% helper API
%%%===================================================================

print(ok,Args) ->
    print("~p~n", [Args]);
print({error, Reason},Args) ->
    print("Error in ~p because ~p~n", [Args, Reason]);
print([],_) ->
    io:format("Error: Wrong Argument Format entered~n");
print(String, Args) ->
    io:format(String, Args).


check_args([]) ->
    print([],[]);
check_args([Args]) ->
    Args;
check_args(_) ->
    check_args([]).