%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(chashbin).
-export([create/1, to_chash/1, to_list/1, to_list_filter/2,
         responsible_index/2, responsible_position/2, index_owner/2,
         num_partitions/1]).
-export([iterator/2, exact_iterator/2, itr_value/1, itr_pop/2, itr_next/1, itr_next_while/2]).
-export_type([chashbin/0]).

%% 160 bits for hash, 16 bits for node id
-define(UNIT, 176).
-define(ENTRY, binary-unit:?UNIT).

-type owners_bin() :: <<_:_*?UNIT>>.
-type index()      :: chash:index_as_int().
-type pred_fun()   :: fun(({index(), node()}) -> boolean()).
-type chash_key()  :: index() | chash:index().

-record(chashbin, {size   :: pos_integer(),
                   owners :: owners_bin(),
                   nodes  :: tuple()}).

-type chashbin() :: #chashbin{}.

-record(iterator, {pos   :: non_neg_integer(),
                   start :: non_neg_integer(),
                   chbin :: chashbin()}).

-type iterator() :: #iterator{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Create a `chashbin' from the provided `chash'
-spec create(chash:chash()) -> chashbin().
create({Size, Owners}) ->
    Nodes1 = [Node || {_, Node} <- Owners],
    Nodes2 = lists:usort(Nodes1),
    Nodes3 = lists:zip(Nodes2, lists:seq(1, length(Nodes2))),
    Bin = create_bin(Owners, Nodes3, <<>>),
    #chashbin{size=Size,
              owners=Bin,
              nodes=list_to_tuple(Nodes2)}.

%% @doc Convert a `chashbin' back to a `chash'
-spec to_chash(chashbin()) -> chash:chash().
to_chash(CHBin=#chashbin{size=Size}) ->
    L = to_list(CHBin),
    {Size, L}.

%% @doc Convert a `chashbin' to a list of `{Index, Owner}' pairs
-spec to_list(chashbin()) -> [{index(), node()}].
to_list(#chashbin{owners=Bin, nodes=Nodes}) ->
    [{Idx, element(Id, Nodes)}
     || <<Idx:160/integer, Id:16/integer>> <= Bin].

%% @doc
%% Convert a `chashbin' to a list of `{Index, Owner}' pairs for
%% which `Pred({Index, Owner})' returns `true'
-spec to_list_filter(pred_fun(), chashbin()) -> [{index(), node()}].
to_list_filter(Pred, #chashbin{owners=Bin, nodes=Nodes}) ->
    [{Idx, element(Id,Nodes)}
     || <<Idx:160/integer, Id:16/integer>> <= Bin,
        Pred({Idx, element(Id,Nodes)})].

%% @doc Determine the ring index responsible for a given chash key
-spec responsible_index(chash_key(), chashbin()) -> index().
responsible_index(<<HashKey:160/integer>>, CHBin) ->
    responsible_index(HashKey, CHBin);
responsible_index(HashKey, #chashbin{size=Size}) ->
    Inc = chash:ring_increment(Size),
    (((HashKey div Inc) + 1) rem Size) * Inc.

%% @doc Determine the ring position responsible for a given chash key
-spec responsible_position(chash_key(), chashbin()) -> non_neg_integer().
responsible_position(<<HashKey:160/integer>>, CHBin) ->
    responsible_position(HashKey, CHBin);
responsible_position(HashKey, #chashbin{size=Size}) ->
    Inc = chash:ring_increment(Size),
    ((HashKey div Inc) + 1) rem Size.

%% @doc Return the node that owns the given index
-spec index_owner(index(), chashbin()) -> node().
index_owner(Idx, CHBin) ->
    case itr_value(exact_iterator(Idx, CHBin)) of
        {Idx, Owner} ->
            Owner;
        _ ->
            %% Match the behavior for riak_core_ring:index_owner/2
            exit({badmatch, false})
    end.

%% @doc Return the number of partitions in a given `chashbin'
-spec num_partitions(chashbin()) -> pos_integer().
num_partitions(#chashbin{size=Size}) ->
    Size.

%% ===================================================================
%% Public Iterator API
%% ===================================================================

%% @doc
%% Return an iterator pointing to the index responsible for the given chash key
-spec iterator(first | chash_key(), chashbin()) -> iterator().
iterator(first, CHBin) ->
    #iterator{pos=0,
              start=0,
              chbin=CHBin};
iterator(<<HashKey:160/integer>>, CHBin) ->
    iterator(HashKey, CHBin);
iterator(HashKey, CHBin) ->
    Pos = responsible_position(HashKey, CHBin),
    #iterator{pos=Pos,
              start=Pos,
              chbin=CHBin}.

%% @doc Return the `{Index, Owner}' pair pointed to by the iterator
-spec itr_value(iterator()) -> {index(), node()}.
itr_value(#iterator{pos=Pos, chbin=#chashbin{owners=Bin, nodes=Nodes}}) ->
    <<_:Pos/binary-unit:?UNIT, Idx:160/integer, Id:16/integer, _/binary>> = Bin,
    Owner = element(Id, Nodes),
    {Idx, Owner}.

%% @doc Advance the iterator by one ring position
-spec itr_next(iterator()) -> iterator() | done.
itr_next(Itr=#iterator{pos=Pos, start=Start, chbin=CHBin}) ->
    Pos2 = (Pos + 1) rem CHBin#chashbin.size,
    case Pos2 of
        Start ->
            done;
        _ ->
            Itr#iterator{pos=Pos2}
    end.

%% @doc
%% Advance the iterator `N' times, returning a list of the traversed
%% `{Index, Owner}' pairs as well as the new iterator state
-spec itr_pop(pos_integer(), iterator()) -> {[{index(), node()}], iterator()}.
itr_pop(N, Itr=#iterator{pos=Pos, chbin=CHBin}) ->
    #chashbin{size=Size, owners=Bin, nodes=Nodes} = CHBin,
    L =
        case Bin of
            <<_:Pos/?ENTRY, Bin2:N/?ENTRY, _/binary>> ->
                [{Idx, element(Id, Nodes)}
                 || <<Idx:160/integer, Id:16/integer>> <= Bin2];
            _ ->
                Left = (N + Pos) - Size,
                Skip = Pos - Left,
                <<Bin3:Left/?ENTRY, _:Skip/?ENTRY, Bin2/binary>> = Bin,
                L1 = [{Idx, element(Id, Nodes)}
                      || <<Idx:160/integer, Id:16/integer>> <= Bin2],
                L2 = [{Idx, element(Id, Nodes)}
                      || <<Idx:160/integer, Id:16/integer>> <= Bin3],
                L1 ++ L2
        end,
    Pos2 = (Pos + N) rem Size,
    Itr2 = Itr#iterator{pos=Pos2},
    {L, Itr2}.

%% @doc Advance the iterator while `Pred({Index, Owner})' returns `true'
-spec itr_next_while(pred_fun(), iterator()) -> iterator().
itr_next_while(Pred, Itr) ->
    case Pred(itr_value(Itr)) of
        false ->
            Itr;
        true ->
            itr_next_while(Pred, itr_next(Itr))
    end.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% Convert list of {Index, Owner} pairs into `chashbin' binary representation
-spec create_bin([{index(), node()}],
                 [{node(), pos_integer()}], binary()) -> owners_bin().
create_bin([], _, Bin) ->
    Bin;
create_bin([{Idx, Owner}|Owners], Nodes, Bin) ->
    {Owner, Id} = lists:keyfind(Owner, 1, Nodes),
    Bin2 = <<Bin/binary, Idx:160/integer, Id:16/integer>>,
    create_bin(Owners, Nodes, Bin2).

%% Convert ring index into ring position
index_position(<<Idx:160/integer>>, CHBin) ->
    index_position(Idx, CHBin);
index_position(Idx, #chashbin{size=Size}) ->
    Inc = chash:ring_increment(Size),
    (Idx div Inc) rem Size.

%% Return iterator pointing to the given index
exact_iterator(<<Idx:160/integer>>, CHBin) ->
    exact_iterator(Idx, CHBin);
exact_iterator(Idx, CHBin) ->
    Pos = index_position(Idx, CHBin),
    #iterator{pos=Pos, start=Pos, chbin=CHBin}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basic_test_() ->
    {spawn,
     {timeout, 120,
      fun() ->
              [basic_test_body(Size, NumNodes)
               || Size     <- [8, 16, 32, 64, 128],
                  NumNodes <- [1, 2, 3, 4, 5, 8, Size div 2, Size]]
      end
     }}.

basic_test_body(Size, NumNodes) ->
    RingTop = 1 bsl 160,
    Ring = riak_core_test_util:fake_ring(Size, NumNodes),
    CHash = riak_core_ring:chash(Ring),
    CHBin = create(CHash),
    Inc = chash:ring_increment(Size),
    Owners = riak_core_ring:all_owners(Ring),
    {Indices, _} = lists:unzip(Owners),
    HashKeys = [<<X:160/integer>> || X <- lists:seq(0, RingTop, Inc div 4)],
    ?assertEqual(CHash, to_chash(CHBin)),
    ?assertEqual(riak_core_ring:all_owners(Ring), to_list(CHBin)),
    ?assertEqual(riak_core_ring:num_partitions(Ring), num_partitions(CHBin)),
    [?assertEqual(riak_core_ring:index_owner(Ring, Idx), index_owner(Idx, CHBin))
     || Idx <- Indices],
    [begin
         ?assertEqual(riak_core_ring:responsible_index(HashKey, Ring),
                      responsible_index(HashKey, CHBin)),
         Itr = iterator(HashKey, CHBin),
         {Idx, Owner} = itr_value(Itr),
         ?assertEqual(responsible_index(HashKey, CHBin), Idx),
         ?assertEqual(index_owner(Idx, CHBin), Owner),
         ?assertEqual(lists:keyfind(Idx, 1, Owners), {Idx,Owner}),
         ItrTests = min(Size-1, 10),
         lists:foldl(fun(_, {LastIdx, LastItr}) ->
                             ExpectedIdx = (LastIdx + Inc) rem RingTop,
                             NextItr = itr_next(LastItr),
                             {NextIdx, _} = itr_value(NextItr),
                             ?assertEqual(ExpectedIdx, NextIdx),
                             {ExpectedIdx, NextItr}
                     end, {Idx, Itr}, lists:seq(1,ItrTests)),
         [begin
              ExpectedPL = lists:sublist(riak_core_ring:preflist(HashKey, Ring), N),
              {PL, Itr2} = itr_pop(N, Itr),
              ?assertEqual(ExpectedPL, PL),
              ExpectedIdx2 = (Idx + (Inc * N)) rem RingTop,
              {Idx2, _} = itr_value(Itr2),
              ?assertEqual(ExpectedIdx2, Idx2),
              ok
          end || N <- [1, 2, 3, 4, Size div 2, Size]],
         ok
     end || HashKey <- HashKeys],
    ok.

-endif.
