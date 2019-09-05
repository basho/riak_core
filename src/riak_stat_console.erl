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
-export([]).


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
    ok.
