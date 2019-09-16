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
-module(riak_stat_profiles).
-include_lib("riak_core/include/riak_stat.hrl").

%% API
-export([
    save_profile/1,
    load_profile/1,
    delete_profile/1,
    reset_profile/0]).


-define(timestamp, os:timestamp()).
-define(NODEID, term_to_binary(node())).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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

%% -------------------------------------------------------------------
%% @doc
%% Data comes in already formatted unless nothing is entered, then the
%% response is 'no_data'.
%% Saves the profile name in the metadata with all the current stats and
%% their status as the value
%% multiple saves of the same name overwrites the profile
%% @end
%% -------------------------------------------------------------------
-spec(save_profile(profilename()) -> ok | error()).
save_profile(ProfileName) ->
    case check_args(ProfileName) of
        ok -> ok;
        _ -> riak_stat_mgr:save_profile(ProfileName)
    end.

%% -------------------------------------------------------------------
%% @doc
%% load a profile saved in the metadata, If the profile is not there it
%% will return {error, no_profile}, if nothing was entered it will return
%% the statement below. Stats will be pulled out of the metadata and
%% compared to the current status of the stats, It will only enable the
%% disabled stats and disable the enabled stats etc, any unregistered stats that
%% are saved in the profile will do nothing - will be ignored.
%% the return will be 'ok' | {error, Reason}
%% @end
%% -------------------------------------------------------------------
-spec(load_profile(profilename()) -> ok | error()).
load_profile(ProfileName) ->
    case check_args(ProfileName) of
        ok -> ok;
        _ -> riak_stat_mgr:load_profile(ProfileName)
    end.

%% -------------------------------------------------------------------
%% @doc
%% deletes the profile from the metadata and all its values but it does
%% not affect the status of the stats, metadata does not remove an entry
%% but does replace the data with a tombstone
%% @end
%% -------------------------------------------------------------------
-spec(delete_profile(profilename()) -> ok | error()).
delete_profile(ProfileName) ->
    case check_args(ProfileName) of
        ok -> ok;
        _ -> riak_stat_mgr:delete_profile(ProfileName)
    end.

%% -------------------------------------------------------------------
%% @doc
%% resets the profile so no profile is loaded and will enable all the
%% disabled stats. it returns 'ok' if everything went ok, or
%% {error, Reason}
%% @end
%% -------------------------------------------------------------------
-spec(reset_profile() -> ok | error()).
reset_profile() ->
    riak_stat_mgr:reset_profile().


%%%===================================================================
%%% Internal functions
%%%===================================================================

print(ok,Args) ->
    print("~s~n", [Args]);
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

-ifdef(TEST).

-endif.
