%% -------------------------------------------------------------------
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

%% @doc TODO

-module(hashtree_tree).

-export([new/2,
         destroy/1,
         insert/4,
         insert/5,
         update_snapshot/1,
         update_perform/1,
         local_compare/2,
         compare/4,
         top_hash/1,
         prefix_hash/2,
         get_bucket/4,
         key_hashes/3]).

-export_type([tree/0, tree_node/0, handler_fun/1, remote_fun/0]).

-record(hashtree_tree, {
          %% the identifier for this tree. TODO more description
          id         :: term(),

          %% directory where nodes are stored on disk
          data_root  :: file:name_all(),

          %% number of levels in the tree excluding leaves (height - 1)
          num_levels :: non_neg_integer(),
          
          %% ets table that holds hashtree nodes in the tree          
          nodes      :: ets:tab(),

          %% ets table that holds snapshot nodes
          snapshot   :: ets:tab(),

          %% set of dirty leaves
          dirty      :: gb_set()          
         }).

-define(ROOT, '$ht_root').
-define(NUM_LEVELS, 2).

-opaque tree()         :: #hashtree_tree{}.
-type prefix()         :: atom() | binary().
-type prefixes()       :: [prefix()].
-opaque tree_node()    :: prefixes() | ?ROOT.
-type prefix_diff()    :: {missing_prefix, local | remote, prefixes()}.
-type key_diffs()      :: {key_diffs, prefixes(),[{missing |
                                                   remote_missing |
                                                   different, binary()}]}.
-type diff()           :: prefix_diff() | key_diffs().
-opaque handler_fun(X) :: fun((diff(), X) -> X).
-opaque remote_fun()   :: fun((prefixes(),
                               {get_bucket, {integer(), integer()}} |
                               {key_hashses, integer()}) -> orddict:orddict()).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc TODO
-type new_opt_num_levels() :: {num_levels, non_neg_integer()}.
-type new_opt_data_dir()   :: {data_dir, file:name_all()}.
-type new_opt()            :: new_opt_num_levels() | new_opt_data_dir().
-type new_opts()           :: [new_opt()].
-spec new(term(), new_opts()) -> tree().
new(TreeId, Opts) ->
    NumLevels = proplists:get_value(num_levels, Opts, ?NUM_LEVELS),
    DataRoot  = data_root(Opts),
    Tree = #hashtree_tree{id = TreeId,
                          data_root = DataRoot,
                          num_levels = NumLevels,
                          %% table needs to be public to allow async update
                          nodes = ets:new(undefined, [public]),
                          snapshot = undefined,
                          dirty = gb_sets:new()},
    get_node(?ROOT, Tree),
    Tree.
    

%% @doc TODO
-spec destroy(tree()) -> ok.
destroy(Tree) ->
    ets:foldl(fun({_, Node}, _) ->
                      Node1 = hashtree:close(Node),
                      hashtree:destroy(Node1)
              end, undefined, Tree#hashtree_tree.nodes),
    catch ets:delete(Tree#hashtree_tree.nodes),
    ok.
    

%% @doc an alias for insert(Prefixes, Key, Hash, [], Tree)
-spec insert(prefixes(), binary(), binary(), tree()) -> tree() | {error, term()}.
insert(Prefixes, Key, Hash, Tree) ->
    insert(Prefixes, Key, Hash, [], Tree).

%% @doc TODO
-type insert_opt_if_missing() :: {if_missing, boolean()}.
-type insert_opt()            :: insert_opt_if_missing().
-type insert_opts()           :: [insert_opt()].
-spec insert(prefixes(), binary(), binary(), insert_opts(), tree()) -> tree() | {error, term()}.
insert(Prefixes, Key, Hash, Opts, Tree) ->
    NodeName = prefixes_to_node_name(Prefixes),
    case valid_prefixes(NodeName, Tree) of
        true ->
            insert_hash(Key, Hash, Opts, NodeName, Tree);
        false ->
            {error, bad_prefixes}
    end.

%% @doc TODO
-spec update_snapshot(tree()) -> {tree(), tree()}.
update_snapshot(Tree=#hashtree_tree{dirty=Dirty,nodes=Nodes}) ->
    FoldRes = gb_sets:fold(fun(DirtyName, Acc) ->
                                   DirtyKey = node_key(DirtyName, Tree),
                                   DirtyNode = lookup_node(DirtyName, Tree),
                                   {SnapNode, NewNode} = hashtree:update_snapshot(DirtyNode),
                                   [{{DirtyKey, SnapNode}, {DirtyKey, NewNode}} | Acc]
                           end, [], Dirty),
    {Snaps, NewNodes} = lists:unzip(FoldRes),
    Snapshot = ets:new(undefined, []),
    ets:insert(Snapshot, Snaps),
    ets:insert(Nodes, NewNodes),
    UpdatedTree = Tree#hashtree_tree{dirty=gb_sets:new()},
    SnapTree = UpdatedTree#hashtree_tree{snapshot=Snapshot},
    {SnapTree, UpdatedTree}.
            

%% @doc TODO
-spec update_perform(tree()) -> ok.
update_perform(Tree=#hashtree_tree{snapshot=Snapshot}) ->
    %% 2. look at original update node function from recovery
    %% 3. generic fold over ets (acc gb_set) and gb_set (acc gb_set)
    DirtyParents = ets:foldl(fun(DirtyKey, DirtyParents) ->
                                     update_dirty_leaves(DirtyKey, DirtyParents, Tree)
                             end,
                             gb_sets:new(), Snapshot),
    update_dirty_parents(DirtyParents, Tree),
    catch ets:delete(Snapshot),
    ok.

%% @doc TODO
%% TODO: spec
-spec local_compare(tree(), tree()) -> [diff()].
local_compare(T1, T2) ->
    RemoteFun = fun(Prefixes, {get_bucket, {Level, Bucket}}) ->
                        hashtree_tree:get_bucket(Prefixes, Level, Bucket, T2);                            
                   (Prefixes, {key_hashes, Segment}) ->
                        [{_, Hashes}] = hashtree_tree:key_hashes(Prefixes, Segment, T2),
                        Hashes
                end,
    HandlerFun = fun(Diff, Acc) -> Acc ++ [Diff] end,
    compare(T1, RemoteFun, HandlerFun, []).

%% @doc TODO
-spec compare(tree(), remote_fun(), handler_fun(X), X) -> X.
compare(LocalTree, RemoteFun, HandlerFun, HandlerAcc) ->    
    compare(?ROOT, 1, LocalTree, RemoteFun, HandlerFun, HandlerAcc).
                     
%% @doc TODO
-spec top_hash(tree()) -> undefined | binary().
top_hash(Tree) ->
    prefix_hash([], Tree).

%% @doc TODO
-spec prefix_hash(prefixes(), tree()) -> undefined | binary().
prefix_hash(Prefixes, Tree) ->
    NodeName = prefixes_to_node_name(Prefixes),
    case lookup_node(NodeName, Tree) of
        undefined -> undefined;
        Node -> extract_top_hash(hashtree:top_hash(Node))
    end.

%% @doc TODO
-spec get_bucket(tree_node(), integer(), integer(), tree()) -> orddict:orddict().
get_bucket(Prefixes, Level, Bucket, Tree) ->
    case lookup_node(prefixes_to_node_name(Prefixes), Tree) of
        undefined -> orddict:new();
        Node -> hashtree:get_bucket(Level, Bucket, Node)
    end.

%% @doc TODO
-spec key_hashes(tree_node(), integer(), tree()) -> [{integer(), orddict:orddict()}].
key_hashes(Prefixes, Segment, Tree) ->
    case lookup_node(prefixes_to_node_name(Prefixes), Tree) of
        undefined -> [{Segment, orddict:new()}];
        Node -> hashtree:key_hashes(Node, Segment)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
insert_hash(Key, Hash, Opts, NodeName, Tree) ->
    Node = get_node(NodeName, Tree),
    insert_hash(Key, Hash, Opts, NodeName, Node, Tree).

%% @private
insert_hash(Key, Hash, Opts, NodeName, Node, Tree=#hashtree_tree{dirty=Dirty}) ->
    Node2 = hashtree:insert(Key, Hash, Node, Opts),
    Dirty2 = gb_sets:add_element(NodeName, Dirty),
    set_node(NodeName, Node2, Tree),
    Tree#hashtree_tree{dirty=Dirty2}.

%% @private
update_dirty_leaves({DirtyKey, DirtyNode}, DirtyParents, Tree) ->
    update_dirty(node_key_to_name(DirtyKey), DirtyNode, DirtyParents, Tree).

%% @private
update_dirty_parents(DirtyParents, Tree) ->
    case gb_sets:is_empty(DirtyParents) of
        true -> ok;
        false ->
            NextDirty = gb_sets:fold(
                          fun(DirtyParent, DirtyAcc) ->
                                  DirtyNode = lookup_node(DirtyParent, Tree),
                                  {DirtySnap, DirtyNode2} = hashtree:update_snapshot(DirtyNode),
                                  NextDirty = update_dirty(DirtyParent, DirtySnap, DirtyAcc, Tree),
                                  set_node(DirtyParent, DirtyNode2, Tree),
                                  NextDirty
                          end, gb_sets:new(), DirtyParents),                
            update_dirty_parents(NextDirty, Tree)
    end.

%% @private
update_dirty(DirtyName, DirtyNode, NextDirty, Tree) ->
    hashtree:update_perform(DirtyNode),
    case parent_node(DirtyName, Tree) of
        undefined ->
            NextDirty;
        {ParentName, ParentNode} ->
            TopHash = extract_top_hash(hashtree:top_hash(DirtyNode)),
            ParentKey = to_parent_key(DirtyName),
            %% ignore returned tree b/c we are tracking dirty in this fold seperately
            insert_hash(ParentKey, TopHash, [], ParentName, ParentNode, Tree),
            gb_sets:add_element(ParentName, NextDirty)
    end.

%% @private
compare(NodeName, Level, LocalTree, RemoteFun, HandlerFun, HandlerAcc)
  when Level =:= LocalTree#hashtree_tree.num_levels + 1 ->
    Prefixes = node_name_to_prefixes(NodeName),
    LocalNode = lookup_node(NodeName, LocalTree),
    RemoteNode = fun(Action, Info) ->
                         RemoteFun(Prefixes, {Action, Info})
                 end,
    AccFun = fun(Diffs, CompareAcc) ->
                     Res = HandlerFun({key_diffs, Prefixes, Diffs},
                                      extract_compare_acc(CompareAcc, HandlerAcc)),
                     [{acc, Res}]
             end,
    CompareRes = hashtree:compare(LocalNode, RemoteNode, AccFun),
    extract_compare_acc(CompareRes, HandlerAcc);
compare(NodeName, Level, LocalTree, RemoteFun, HandlerFun, HandlerAcc) ->
    Prefixes = node_name_to_prefixes(NodeName),
    LocalNode = lookup_node(NodeName, LocalTree),
    RemoteNode = fun(Action, Info) ->
                         RemoteFun(Prefixes, {Action, Info})
                 end,
    AccFoldFun = fun({missing, NodeKey}, HandlerAcc2) ->
                         missing_prefix(NodeKey, local, HandlerFun, HandlerAcc2);
                    ({remote_missing, NodeKey}, HandlerAcc2) ->
                         missing_prefix(NodeKey, remote, HandlerFun, HandlerAcc2);
                    ({different, NodeKey}, HandlerAcc2) ->
                         compare(from_parent_key(NodeKey), Level+1, LocalTree,
                                 RemoteFun, HandlerFun, HandlerAcc2)
                 end,
    AccFun = fun(Diffs, CompareAcc) ->
                     Res = lists:foldl(AccFoldFun,
                                       extract_compare_acc(CompareAcc, HandlerAcc), Diffs),
                     [{acc, Res}]
             end,
    CompareRes = hashtree:compare(LocalNode, RemoteNode, AccFun),
    extract_compare_acc(CompareRes, HandlerAcc).


%% @private
missing_prefix(NodeKey, Type, HandlerFun, HandlerAcc) ->
    HandlerFun({missing_prefix, Type, node_name_to_prefixes(from_parent_key(NodeKey))},
               HandlerAcc).
%% @private                     
extract_compare_acc([], HandlerAcc) ->
    HandlerAcc;
extract_compare_acc([{acc, Acc}], _HandlerAcc) ->
    Acc.

%% @private
get_node(NodeName, Tree) ->
    Node = lookup_node(NodeName, Tree),
    get_node(NodeName, Node, Tree).

%% @private
get_node(NodeName, undefined, Tree) ->
    create_node(NodeName, Tree);
get_node(_NodeName, Node, _Tree) ->
    Node.

%% @private
lookup_node(NodeName, Tree=#hashtree_tree{nodes=Nodes}) ->
    NodeKey = node_key(NodeName, Tree),
    case ets:lookup(Nodes, NodeKey) of
        [] -> undefined;             
        [{NodeKey, Node}] -> Node
    end.

%% @private
create_node(NodeName, Tree) ->
    NodeId = node_id(NodeName, Tree),
    NodePath = node_path(NodeId, Tree),
    NumSegs = node_num_segs(NodeName),
    Width = node_width(NodeName),
    Opts = [{segment_path, NodePath}, {segments, NumSegs}, {width, Width}],
    Node = clean_node(NodeId, Opts),
    set_node(NodeName, Node, Tree).

%% @private
clean_node(NodeId, Opts) ->
    FakeNode = hashtree:new(NodeId, Opts),
    destroy_node(FakeNode),
    hashtree:new(NodeId, Opts).

%% @private
set_node(NodeName, Node, Tree) when is_list(NodeName) orelse NodeName =:= ?ROOT ->
    set_node(node_key(NodeName, Tree), Node, Tree);
set_node(NodeKey, Node, #hashtree_tree{nodes=Nodes}) when is_tuple(NodeKey) ->
    ets:insert(Nodes, [{NodeKey, Node}]),
    Node.

%% @private
destroy_node(Node) ->
    Node1 = hashtree:close(Node),
    hashtree:destroy(Node1).

%% @private
parent_node(?ROOT, _Tree) ->
    %% root has no parent
    undefined;
parent_node([_Single], Tree) ->
    %% parent of first level is the root
    {?ROOT, get_node(?ROOT, Tree)};
parent_node([_Prefix | Parent], Tree) ->
    %% parent of subsequent level is tail of node name
    {Parent, get_node(Parent, Tree)}.

%% @private
node_width(?ROOT) ->
    256;
node_width(NodeName) ->
    case length(NodeName) < 2 of
        true -> 512;
        false -> 1024
    end.

%% @private
node_num_segs(?ROOT) ->
    256 * 256;
node_num_segs(NodeName) ->
    case length(NodeName) < 2 of
        true -> 512 * 512;
        false -> 1024 * 1024
    end.

%% @private
node_path({_, <<NodeInt:176/integer>>}, #hashtree_tree{data_root=DataRoot}) ->
    NodeMD5 = riak_core_util:integer_to_list(NodeInt, 16),
    filename:join(DataRoot, NodeMD5).

%% @private
node_key(NodeName, #hashtree_tree{id=TreeId}) ->
    {TreeId, NodeName}.

%% @private
node_key_to_name({_TreeId, NodeName}) ->
    NodeName.

%% @private
node_id(?ROOT, #hashtree_tree{id=TreeId}) ->
    {TreeId, <<0:176/integer>>};
node_id(NodeName, #hashtree_tree{id=TreeId}) ->
    <<NodeMD5:128/integer>> = riak_core_util:md5(term_to_binary(NodeName)),
    {TreeId, <<NodeMD5:176/integer>>}.

%% @private
to_parent_key(NodeName) ->
    term_to_binary(NodeName).

%% @private
from_parent_key(NodeKey) ->
    binary_to_term(NodeKey).

%% @private
valid_prefixes(NodeName, #hashtree_tree{num_levels=NumLevels}) ->
    length(NodeName) =:= NumLevels.

%% @private
prefixes_to_node_name([]) ->
    ?ROOT;
prefixes_to_node_name(Prefixes) ->
    lists:reverse(Prefixes).

%% @private
node_name_to_prefixes(?ROOT) ->
    [];
node_name_to_prefixes(NodeName) ->
    lists:reverse(NodeName).

%% @private
extract_top_hash([]) ->
    undefined;
extract_top_hash([{0, Hash}]) ->
    Hash.

%% @private
data_root(Opts) ->
    case proplists:get_value(data_dir, Opts) of
        undefined ->
            Base = "/tmp/hashtree_tree",
            <<P:128/integer>> = riak_core_util:md5(term_to_binary(erlang:now())),
            filename:join(Base, riak_core_util:integer_to_list(P, 16));
        Root -> Root
    end.
    
