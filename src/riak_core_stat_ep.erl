%%%-------------------------------------------------------------------
%%% @doc
%%% a part of riak_stat, as part of stat_management
%%% it sets up the pushing of stats to a UDP endpoint or the body produced
%%% when stats are requested through HTTP
%%% Named ep, as it only deals with the retrieval of stats on
%%% a surface layer to an endpoint, no stat manipulation is made
%%% @end
%%%-------------------------------------------------------------------
-module(riak_core_stat_ep).
-include_lib("riak_core/include/riak_core_stat.hrl").

%% Exports
-export([
  setup/1,
  setdown/1
]).

-spec(setup(arg()) -> ok).
%% @doc
%% the default operation of this function is to start up the pushing and
%% polling of stats from exometer to the UDP endpoint "set-up".
%% The ability to pass in an argument gives the added layer of functionality to
%% choose the endpoint details quicker and easier, passing in arguments is optional.
%% @end
setup(Arg) ->
  riak_core_stat_console:setup(Arg).

-spec(setdown(arg()) -> ok).
%% @doc
%% Kill the udp servers currently running and pushing stats to an endpoint.
%% @end
setdown(Arg) ->
  riak_core_stat_console:setdown(Arg).
