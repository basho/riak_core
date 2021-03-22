-module(riak_core_location).

%% API
-export([get_node_location/2,
         has_location_set_in_cluster/1,
         stripe_nodes_by_location/2,
         check_ring/1,
         check_ring/2,
         check_ring/3]).

-spec get_node_location(node(), dict:dict()) -> string() | undefined.
get_node_location(Node, Locations) ->
  case dict:find(Node, Locations) of
    error ->
      undefined;
    {ok, Location} ->
      Location
  end.

-spec has_location_set_in_cluster(dict:dict()) -> boolean().
has_location_set_in_cluster(NodesLocations) ->
  0 =/= dict:size(NodesLocations).

-spec get_location_nodes([node()|undefined], dict:dict()) -> dict:dict().
get_location_nodes(Nodes, Locations) ->
  lists:foldl(fun(undefined, Acc) ->
                   dict:append(undefined, undefined, Acc);
                 (Node, Acc) ->
                   NodeLocation = get_node_location(Node, Locations),
                   dict:append(NodeLocation, Node, Acc)
              end, dict:new(), Nodes).

%% Order nodes list by having a different location after each other
-spec stripe_nodes_by_location([node()|undefined], dict:dict()) ->
  [node()|undefined].
stripe_nodes_by_location(Nodes, NodesLocations) ->
  LocationsNodes = get_location_nodes(Nodes, NodesLocations),
  stripe_nodes_by_location(get_locations(LocationsNodes), LocationsNodes, []).

-spec get_locations(dict:dict()) -> [node()|undefined].
get_locations(LocationsNodes) ->
  LocationNodesSorted = sort_by_node_count(dict:to_list(LocationsNodes)),
  lists:map(fun({Location, _}) -> Location end, LocationNodesSorted).

-spec sort_by_node_count(list()) -> list().
sort_by_node_count(LocationsNodes) ->
  lists:sort(fun compare/2, LocationsNodes).

-spec compare({any(), list()}, {any(), list()}) -> boolean().
compare({_, NodesA}, {_, NodesB}) ->
  LengthA = length(NodesA),
  LengthB = length(NodesB),
  case LengthA =:= LengthB of
    true ->
      lists:last(NodesA) >= lists:last(NodesB);
    _ ->
      LengthA >= LengthB
  end.

-spec stripe_nodes_by_location([node()|undefined], dist:dict(), [node()|undefined]) ->
  [node()|undefined].
stripe_nodes_by_location([], _LocationsNodes, NewNodeList)  ->
  lists:reverse(NewNodeList);
stripe_nodes_by_location(Locations, LocationsNodes, NewNodeList) ->
  Nth = (length(NewNodeList) rem length(Locations)) + 1,
  CurrentLocation = lists:nth(Nth, Locations),
  case dict:find(CurrentLocation, LocationsNodes) of
    {ok, []} ->
      NewLocations = lists:delete(CurrentLocation, Locations),
      NewLocationsNodes = dict:erase(CurrentLocation, LocationsNodes),
      stripe_nodes_by_location(NewLocations, NewLocationsNodes, NewNodeList);
    {ok, [Node | RemNodes]} ->
      NewLocationNodes = dict:store(CurrentLocation, RemNodes, LocationsNodes),
      stripe_nodes_by_location(Locations, NewLocationNodes, [Node | NewNodeList])
  end.

-spec check_ring(riak_core_ring:riak_core_ring()) -> list().
check_ring(Ring) ->
  {ok, Props} = application:get_env(riak_core, default_bucket_props),
  check_ring(Ring, proplists:get_value(n_val, Props)).

-spec check_ring(riak_core_ring:riak_core_ring(), pos_integer()) -> list().
check_ring(Ring, Nval) ->
  check_ring(Ring, Nval, Nval).

-spec check_ring(riak_core_ring:riak_core_ring(), pos_integer(), pos_integer()) -> list().
check_ring(Ring, NVal, MinimumNumberOfDistinctLocations) ->
  Locations = riak_core_ring:get_nodes_locations(Ring),
  case has_location_set_in_cluster(Locations) of
    true ->
      check_ring(Ring, NVal, MinimumNumberOfDistinctLocations, Locations);
    _ ->
      [] % no location set, not needed to check
  end.

-spec check_ring(riak_core_ring:riak_core_ring(), pos_integer(), pos_integer(), dict:dict()) ->
  list().
check_ring(Ring, Nval, MinimumNumberOfDistinctLocations, Locations) ->
  lists:foldl(fun(PL, Acc) ->
                case length(get_unique_locations(PL, Locations)) < MinimumNumberOfDistinctLocations of
                  false -> Acc;
                  _ ->
                    Error = [{Idx, Node, get_node_location(Node, Locations)} || {Idx, Node} <- PL],
                    [Error | Acc]
                end
              end, [], riak_core_ring:all_preflists(Ring, Nval)).

-spec get_unique_locations(list(), dict:dict()) ->
  list().
get_unique_locations(PrefLists, Locations) ->
  lists:usort([get_node_location(Node, Locations) || {_, Node} <- PrefLists]).