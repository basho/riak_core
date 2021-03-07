%%% @author Peter Tihanyi <peter@systream.hu>
%%% @copyright (C) 2021, Peter Tihanyi
%%% @doc
%%%
%%% @end
-module(rack_awareness_test).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-define(RING_SIZES, [8, 64, 128, 256, 512]).
-define(NODE_COUNTS, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 20]).

ring_claim_test_() ->
    {setup,
     fun generate_rings/0,
     fun (_) -> ok end,
     fun(Rings) ->
       RingsWithLocation = lists:flatten(lists:map(fun add_loc_to_ring/1, Rings)),
       Threads = erlang:system_info(schedulers_online),
       {inparallel, Threads, [{get_test_desc(Ring),
                                fun() -> assert_vnode_location(claim(Ring)) end}
                              || Ring <- RingsWithLocation]}
     end
    }.

get_test_desc(Ring) ->
  lists:flatten(
    io_lib:format("ring_size: ~p node_count: ~p nodes with locations: ~p",
                [riak_core_ring:num_partitions(Ring),
                 length(riak_core_ring:all_members(Ring)),
                 dict:size(riak_core_ring:get_nodes_locations(Ring))])
  ).

add_loc_to_ring(Ring) ->
  Nodes = riak_core_ring:all_members(Ring),
  NodeCount = length(Nodes),
  Locations = generate_site_names(NodeCount),
  %Permutations = gen_loc_permutations(Locations, #{}, NodeCount),
  Permutations = gen_loc_permutations_opt(Locations, NodeCount),
  lists:map(fun(Permutation) ->
              {_, NewRing} =
                maps:fold(fun(Location, Count, {CNodes, CRing}) ->
                             do_set_locations(CNodes, Location, Count, CRing)
                          end, {Nodes, Ring}, Permutation),
              NewRing
            end, Permutations).

do_set_locations(Nodes, _Location, 0, Ring) ->
  {Nodes, Ring};
do_set_locations([Node | RemNodes], Location, Count, Ring) ->
  NewRing = riak_core_ring:set_node_location(Node, Location, Ring),
  do_set_locations(RemNodes, Location, Count-1, NewRing).

gen_loc_permutations_opt(Locations, Max) ->
  InitPermutationMap = init_permutation_map(Locations),
  HeadLoc = hd(Locations),
  gen_loc_permutations_opt(HeadLoc, Locations, InitPermutationMap, Max,
                           [InitPermutationMap]).

gen_loc_permutations_opt(_, [], _Permutation, _Max, Acc) ->
  Acc;
gen_loc_permutations_opt(TopLoc, [HeadLoc | RestLoc] = Locations, Permutation, Max,
                         Acc) ->
  case get_location_combination_count(Permutation) =:= Max of
    true ->
      NewPermutations = increase(Permutation, TopLoc, -1),
      TopLocCount = maps:get(TopLoc, NewPermutations),
      HeadLocCount = maps:get(HeadLoc, NewPermutations),
      case TopLocCount > HeadLocCount of
        true ->
          NewPermutations2 = increase(NewPermutations, HeadLoc, -1),
          gen_loc_permutations_opt(TopLoc, Locations, NewPermutations2, Max, Acc);
        _ ->
          gen_loc_permutations_opt(HeadLoc, RestLoc, NewPermutations, Max, Acc)
      end;
    _ ->
      NewPermutations = increase(Permutation, HeadLoc, 1),
      gen_loc_permutations_opt(TopLoc, Locations, NewPermutations, Max,
                               [NewPermutations | Acc])
  end.


init_permutation_map(Locations) ->
  lists:foldl(fun(LocName, Acc) -> Acc#{LocName => 0} end, #{}, Locations).

gen_loc_permutations([], Acc, _Max) ->
  Acc;
gen_loc_permutations(Loc, Acc, Max) when length(Loc) > 6 ->
  LocLength = length(Loc),
  NewLocations = lists:delete(lists:nth(rand:uniform(LocLength), Loc), Loc),
  gen_loc_permutations(NewLocations, Acc, Max);
gen_loc_permutations([LocationName | Rest] = _Full, Acc, Max) ->
  Result = [gen_loc_permutations(Rest, increase(Acc, LocationName, Add), Max) || Add <- lists:seq(0, Max)],
  lists:filter(fun(Item) ->
                 get_location_combination_count(Item) =< Max
               end, lists:flatten(Result)).

increase(State, LocationName, Add) ->
  State#{LocationName => maps:get(LocationName, State, 0)+Add}.

get_location_combination_count(Locations) ->
  maps:fold(fun(_, V, A) -> V+A end, 0, Locations).

assert_vnode_location(Ring) ->
  Locations = dict:to_list(riak_core_ring:get_nodes_locations(Ring)),
  assert_vnode_location(Ring, Locations).

assert_vnode_location(_Ring, []) ->
  true;
assert_vnode_location(Ring, Locations) ->
  CheckResult = check(Ring, Locations),
  case CheckResult of
    false ->
      Owners = riak_core_ring:all_owners(Ring),
      Members = riak_core_ring:all_members(Ring),
      Data = print_ring_partitions(Owners, Locations, ""),
      RSize = io_lib:format("Ring size: ~p~n", [length(Owners)]),
      LocationData = io_lib:format("Location data: ~p~n", [Locations]),
      MemberCount = io_lib:format("Nodes: ~p~n", [length(Members)]),
      RingErrors = io_lib:format("Check ring: ~p~n",
                                 [riak_core_location:check_ring(Ring, 3)]),
      NewData = ["\r\n *********************** \r\n", RSize, MemberCount,
                 LocationData, RingErrors, "\r\n" | Data],
      file:write_file("ring_debug.txt", NewData, [append, write]);
    _ ->
      ok
  end,
  ?assert(CheckResult).

check(Ring, Locations) ->
  Owners = riak_core_ring:all_owners(Ring),
  MemberCount = length(riak_core_ring:active_members(Ring)),
  NVal = 3,
  Rs = length(Owners),
  UniqueLocCount = length(get_unique_locations(Locations)),

  MaxError = get_max_error(NVal, Rs, MemberCount, UniqueLocCount),
  case riak_core_location:check_ring(Ring, NVal) of
    Errors when length(Errors) > MaxError ->
      io:format("Max error; ~p > ~p | ~p~n",
                [length(Errors), MaxError, {NVal, Rs, MemberCount,
                                                    UniqueLocCount}]),
      false;
    _ ->
      true
  end.

% @TODO better evaluation
get_max_error(NVal, RingSize, MemberCount, UniqueLocationCount)
  when UniqueLocationCount > NVal andalso
       MemberCount == UniqueLocationCount andalso
       RingSize rem MemberCount == 0 ->
  0;
get_max_error(NVal, _RingSize, MemberCount, UniqueLocationCount)
  when UniqueLocationCount > NVal andalso
       MemberCount == UniqueLocationCount ->
  NVal;
get_max_error(NVal, RingSize, MemberCount, UniqueLocationCount)
  when RingSize rem MemberCount == 0 -> % no tail violations
  MaxNodesOnSameLoc = (MemberCount - UniqueLocationCount)+1,
  case MaxNodesOnSameLoc > NVal of
    true ->
      RingSize;
    false  ->
      (RingSize - ((NVal rem MaxNodesOnSameLoc))+1)
  end;
get_max_error(_NVal, RingSize, _MemberCount, _UniqueLocationCount) ->
  RingSize.

get_unique_locations(Locations) ->
  lists:foldl(fun({_, ActualLoc}, Acc) ->
                case lists:member(ActualLoc, Acc) of
                  false ->
                    [ActualLoc | Acc];
                  _ ->
                    Acc
                end
              end, [], Locations).

print_ring_partitions(Owners, Locations) ->
  io:format(user, "~s", [print_ring_partitions(Owners, Locations, [])]).

print_ring_partitions([], _Locations, Acc) ->
  lists:reverse(Acc);
print_ring_partitions([{Idx, Node} | Rem], Locations, Acc) ->
  Location = proplists:get_value(Node, Locations, not_set),
  Line = io_lib:format("Node: ~p \t Loc: ~p \t idx: ~p ~n", [Node, Location, Idx]),
  print_ring_partitions(Rem, Locations, [Line | Acc]).

generate_rings() ->
  generate_rings(?RING_SIZES, ?NODE_COUNTS).

generate_rings(Sizes, NodeCounts) ->
  [do_generate_ring(S, generate_node_names(N)) ||
    S <- Sizes, N <- NodeCounts].

do_generate_ring(Size, ContributorNodes) when length(ContributorNodes) > Size ->
  do_generate_ring(Size, lists:sublist(ContributorNodes, 1, Size));
do_generate_ring(Size, ContributorNodes) ->
  [CNode] = generate_node_names(1),
  Ring = riak_core_ring:fresh(Size, CNode),
  NewRing = lists:foldl(fun(Node, CRing) ->
                          riak_core_ring:add_member(CNode, CRing, Node)
                        end, Ring, ContributorNodes),
  claim(NewRing).

claim(Ring) ->
  WantsClaimFun = {riak_core_claim, default_wants_claim},
  ChooseClaimFun = {riak_core_claim, default_choose_claim},
  riak_core_claim:claim(Ring, WantsClaimFun, ChooseClaimFun).

generate_site_names(Count) ->
  lists:map(fun(Name) -> binary_to_list(Name) end,
            tl(generate_name(<<"site_">>, Count+1))).

generate_node_names(Count) ->
  lists:map(fun(Name) -> binary_to_atom(<<Name/binary, "@test">>, utf8) end,
            generate_name(<<"node_">>, Count)).

generate_name(Prefix, Count) when Count =< 25 andalso Count >= 1 ->
  GenList = lists:seq(65, 65+Count-1),
  [<<Prefix/binary, (list_to_binary([C]))/binary>> || C <- GenList].
