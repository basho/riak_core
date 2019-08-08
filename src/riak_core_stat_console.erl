%%%-------------------------------------------------------------------
%%% @doc
%%% calls from riak_core_console are directed to this module to
%%% enable/disable or read stats from exometer/metadata
%%%
%%% calls from endpoint to this module to retrieve stats
%%% for a UDP or HTTP endpoint
%%% @end
%%%-------------------------------------------------------------------
-module(riak_core_stat_console).

-include_lib("riak_core/include/riak_core_stat.hrl").

%% API
-export([
  show_stat/1,
  show_stat_0/1,
  stat_info/1,
  disable_stat_0/1,
  status_change/2,
  reset_stat/1,
  enable_metadata/1
]).

%% Endpoint API
-export([
  setup/1,
  setdown/0,
  get_host/1,
  get_stats/0
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec(show_stat(data()) -> value()).
%% @doc
%% Show enabled or disabled stats
%% when using riak-admin stat show riak.** enabled stats will show by default
%%
%% otherwise use: riak-admin stat show <entry>/status=* | disabled
%% @end
show_stat(Arg) ->
  [{_S, MatchSpec, DP}] = data_sanitise(Arg),
  Entries = select_entries(MatchSpec),
  Stats =
    case DP of
      default -> [{Entry, Status} || {Entry, _, Status} <- Entries];
      _ -> [find_stat_info(Entry, DP) || {Entry, _, _} <- Entries]
    end,
  print_stats(Stats).

-spec(show_stat_0(data()) -> value()).
%% @doc
%% Check which stats in exometer are not updating, only checks enabled
%% @end
show_stat_0(Arg) ->
  [{_Stats, MatchSpec, _DP}] = data_sanitise(Arg),
  Entries = [Entry || {Entry, _,_} <- select_entries(MatchSpec)],
  NotUpdating = not_updating(Entries),
  print_stats(NotUpdating).

-spec(stat_info(data()) -> value()).
%% @doc
%% Returns all the stats information
%% @end
stat_info(Arg) ->
  {Attrs, RestArg} = pick_info_attrs(Arg),
  [{Stats, _MatchSpec, _DP}] = data_sanitise(RestArg),
  Entries = find_entries(Stats, enabled),
  Found = [find_stat_info(Entry, Attrs) || {Entry, _} <- Entries],
  print_stats(Found).

-spec(disable_stat_0(data()) -> ok).
%% @doc
%% Similar to the function above, but will disable all the stats that
%% are not updating
%% @end
disable_stat_0(Arg) ->
  [{_S, MatchSpec, _DP}] = data_sanitise(Arg),
  Entries = [Entry || {Entry, _,_} <- select_entries(MatchSpec)],
  NotUpdating = not_updating(Entries),
  DisableTheseStats =
    lists:map(fun({Name, _V}) ->
      {Name, {status, disabled}}
              end, NotUpdating),
  change_status(DisableTheseStats).

-spec(status_change(data(), status()) -> ok).
%% @doc
%% change the status of the stat (in metadata and) in exometer
%% @end
status_change(Arg, ToStatus) ->
  [{Stats, _MatchSpec, _DP}] = data_sanitise(Arg),
  Entries = % if disabling lots of stats, pull out only enabled ones
  case ToStatus of
    enabled -> find_entries(Stats, disabled);
    disabled -> find_entries(Stats, enabled)
  end,
  change_status([{Stat, {status, Status}} || {Stat, Status} <- Entries]).

-spec(reset_stat(data()) -> ok).
%% @doc
%% resets the stats in metadata and exometer and tells metadata that the stat
%% has been reset
%% @end
reset_stat(Arg) ->
  [{Stats, _MatchSpec, _DP}] = data_sanitise(Arg),
  reset_stats([Entry || {Entry, _} <- find_entries(Stats, enabled)]).

-spec(enable_metadata(data()) -> ok).
%% @doc
%% enabling the metadata allows the stats configuration and the stats values to
%% be persisted, disabling the metadata returns riak to its original functionality
%% of only using the exometer functions. Enabling and disabling the metadata occurs
%% here, directing the stats and function work occurs in the riak_stat_coordinator
%% @end
enable_metadata(Arg) ->
  Truth = ?IS_ENABLED(?META_ENABLED),
  case data_sanitise(Arg) of
    Truth ->
      io:fwrite("Metadata-enabled already set to ~s~n", [Arg]);
    Bool when Bool == true; Bool == false ->
      case Bool of
        true ->
          riak_core_stat_coordinator:reload_metadata(get_stats()),
          app_helper:get_env(riak_core, ?META_ENABLED, Bool);
        false ->
          app_helper:get_env(riak_core, ?META_ENABLED, Bool)
      end;
    Other ->
      io:fwrite("Wrong argument entered: ~p~n", [Other])
  end.


%%%===================================================================
%%% Admin API
%%%===================================================================

data_sanitise(Arg) ->
  riak_core_stat_admin:data_sanitise(Arg).

print_stats(Entries) ->
  print_stats(Entries, []).
print_stats(Entries, Attributes) ->
  riak_core_stat_admin:print(Entries, Attributes).

%%%===================================================================
%%% Coordinator API
%%%===================================================================

find_entries(StatNames, Status) ->
  riak_core_stat_coordinator:find_entries(StatNames, Status).

select_entries(MS) ->
  riak_core_stat_coordinator:select(MS).

not_updating(StatNames) ->
  riak_core_stat_coordinator:find_static_stats(StatNames).

find_stat_info(Stats, Info) ->
  riak_core_stat_coordinator:find_stats_info(Stats, Info).

change_status(Stats) ->
  riak_core_stat_coordinator:change_status(Stats).

reset_stats(Name) ->
  riak_core_stat_coordinator:reset_stat(Name).


%%%===================================================================
%%% Helper functions
%%%===================================================================

-spec(pick_info_attrs(data()) -> value()).
%% @doc get list of attrs to print @end
pick_info_attrs(Arg) ->
  case lists:foldr(
    fun ("-name", {As, Ps}) -> {[name | As], Ps};
      ("-type", {As, Ps}) -> {[type | As], Ps};
      ("-module", {As, Ps}) -> {[module | As], Ps};
      ("-value", {As, Ps}) -> {[value | As], Ps};
      ("-cache", {As, Ps}) -> {[cache | As], Ps};
      ("-status", {As, Ps}) -> {[status | As], Ps};
      ("-timestamp", {As, Ps}) -> {[timestamp | As], Ps};
      ("-options", {As, Ps}) -> {[options | As], Ps};
      (P, {As, Ps}) -> {As, [P | Ps]}
    end, {[], []}, split_arg(Arg)) of
    {[], Rest} ->
      {[name, type, module, value, cache, status, timestamp, options], Rest};
    Other ->
      Other
  end.

split_arg([Str]) ->
  re:split(Str, "\\s", [{return, list}]).

-type hostarg()     :: atom().

%%-------------------------------------------------------------------
-spec(setup(arg()) -> ok).
%% @doc
%% sanitise the argument passed in to retrieve the data from console,
%% passing in information is optional as all the defaults are pulled out from
%% the sys.config.
%% similar to other stats functions the stats can be passed in to only poll
%% those stats, however, only the enabled stats will return with values
%%
%% If the server is already setup and polling stats, setting up another will
%% "restart" the gen_server.
%% @end
setup(Arg) ->
  {{Port, Instance, Sip}, STATSorPROFILES}    =     sanitise_data(Arg),
  start_server(riak_core_stat_latency, {{Port, Instance, Sip}, STATSorPROFILES}).

sanitise_data([<<>>]) -> sanitise_data([]);
sanitise_data(Arg) -> riak_core_stat_data:sanitise_data(Arg).

start_server(Child, Arg) ->
%%  case get_child() of
%%    {error, Reason} ->
%%      lager:error("Couldn't find Children because: ~p~n", [Reason]);
%%    ChildRef ->
%%      Ids = [Id || {Id, _Child, _Type, _Mods} <- ChildRef],
%%      terminate_server(Ids),
      riak_core_stats_sup:start_server(Child, Arg).
%%  end.


%%-------------------------------------------------------------------
-spec(setdown(arg()) -> ok).
%% @doc
%% Stop the pushing of stats by taking the setup of the udp gen_server
%% down
%% @end
setdown() ->
  setdown([]).
setdown(_Arg) ->
  terminate_server().

%%terminate_server(Arg) ->/.
%%  riak_core_stats_sup:stop_server(Arg).
terminate_server() ->
  riak_core_stats_sup:stop_server(riak_core_stat_latency).

%%-------------------------------------------------------------------
-spec(get_host(hostarg()) -> {socket(), server_ip() | server(), port()}).
%% @doc
%% get the host details of the udp_socket or http request details, similar to the state
%% in a gen_server but kept in an ets - table to preserve it longer that the udp
%% gen_server, information is pulled out like a last known request
%% @end
get_host(Info) ->
  case ets:lookup(?ENDPOINTTABLE, Info) of
    [{_, {MonitorServer, undefined, MonitorPort, Socket}}] ->
      {Socket, MonitorServer, MonitorPort};
    [{_, {_MonitorServer, MonitorServerIp, MonitorPort, Socket}}] ->
      {Socket, MonitorServerIp, MonitorPort};
    [] ->
      {error, no_info}
  end.

-spec(get_stats() -> stats()).
%% @doc
%% retrieve all the stats out of riak_kv_status
%% todo: improve the stat collection method
%% @end
get_stats() ->
  case application:get_env(riak_core, endpoint_defaults_type, classic) of
    classic ->
      riak_kv_status:get_stats(web);
    beta ->
      riak_core_stat_coordinator:get_values(['_'])
  end.