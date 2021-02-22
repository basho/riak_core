-module(riak_core_location).

%% API
-export([get_node_location/2,
         has_location_set_in_cluster/1,
         stripe_nodes_by_location/2]).

-spec get_node_location(node(), dict:dict()) -> string() | unknown.
get_node_location(Node, Locations) ->
  case dict:find(Node, Locations) of
    error ->
      unknown;
    {ok, Location} ->
      Location
  end.

-spec has_location_set_in_cluster(dict:dict()) -> boolean().
has_location_set_in_cluster(NodesLocations) ->
  0 =/= dict:size(NodesLocations).

-spec get_location_nodes([node()|undefined], dict:dict()) -> dict:dict().
get_location_nodes(Nodes, Locations) ->
  lists:foldl(fun(undefined, Acc) ->
                   dict:append(unknown, undefined, Acc);
                 (Node, Acc) ->
                   NodeLocation = get_node_location(Node, Locations),
                   dict:append(NodeLocation, Node, Acc)
              end, dict:new(), Nodes).

%% Order nodes list by having a different location after each other
-spec stripe_nodes_by_location([node()|undefined], dict:dict()) ->
  [node()|undefined].
stripe_nodes_by_location(Nodes, NodesLocations) ->
  LocationsNodes = get_location_nodes(Nodes, NodesLocations),
  Locations = dict:fetch_keys(LocationsNodes),
  stripe_nodes_by_location(Locations, LocationsNodes, []).

-spec stripe_nodes_by_location([node()|undefined], dict:dict(), [node()|undefined]) ->
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
