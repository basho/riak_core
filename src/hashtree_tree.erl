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

%% @doc This module implements a specialized hash tree that is used
%% primarily by cluster metadata's anti-entropy exchanges and by
%% metadata clients for determining when groups of metadata keys have
%% changed locally. The tree can be used, generally, for determining
%% the differences in groups of keys, or to find missing groups, between
%% two stores.
%%
%% Each node of the tree is itself a hash tree, specifically a {@link
%% hashtree}.  The tree has a fixed height but each node has a
%% variable amount of children. The height of the tree directly
%% corresponds to the number of prefixes supported by the tree. A list
%% of prefixes, or a "prefix list", represent a group of keys. Each
%% unique prefix list is a node in the tree. The leaves store hashes
%% for the individual keys in the segments of the node's {@link
%% hashtree}. The buckets of the leaves' hashtree provide an efficient
%% way of determining when keys in the segments differ between two
%% trees.  The tails of the prefix list are used to roll up groups
%% into parent groups. For example, the prefixes `[a, b]', `[a, c]',
%% `[d, e]' will be rolled up into parent groups `a', containing `c'
%% and `b', and `d', containing only 'e'. The parent group's node has
%% children corresponding to each child group. The top-hashes of the
%% child nodes are stored in the parent nodes' segments. The parent
%% nodes' buckets are used as an efficient method for determining when
%% child groups differ between two trees. The root node corresponds to
%% the empty list and it acts like any other node, storing hashes for
%% the first level of child groups. The top hash of the root node is
%% the top hash of the tree.
%%
%% The tree in the example above might store something like:
%%
%% node    parent   top-hash  segments
%% ---------------------------------------------------
%% root     none       1      [{a, 2}, {d, 3}]
%% [a]      root       2      [{b, 4}, {c, 5}]
%% [d]      root       3      [{e, 6}]
%% [a,b]    [a]        4      [{k1, 0}, {k2, 6}, ...]
%% [a,c]    [a]        5      [{k1, 1}, {k2, 4}, ...]
%% [d,e]    [d]        6      [{k1, 2}, {k2, 3}, ...]
%%
%%
%% When a key is inserted into the tree it is inserted into the leaf
%% corresponding to the given prefix list. The leaf and its parents
%% are not updated at this time. Instead the leaf is added to a dirty
%% set. The nodes are later updated in bulk.
%%
%% Updating the hashtree is a two step process. First, a snapshot of
%% the tree must be obtained. This prevents new writes from affecting
%% the update. Snapshotting the tree will snapshot each dirty
%% leaf. Since writes to nodes other than leaves only occur during
%% updates no snapshot is taken for them. Second, the tree is updated
%% using the snapshot. The update is performed by updating the {@link
%% hashtree} nodes at each level starting with the leaves. The top
%% hash of each node in a level is inserted into its parent node after
%% being updated. The list of dirty parents is then updated, moving up
%% the tree. Once the root is reached and has been updated the process
%% is complete. This process is designed to minimize the traversal of
%% the tree and ensure that each node is only updated once.
%%
%% The typical use for updating a tree is to compare it with another
%% recently updated tree. Comparison is done with the ``compare/4''
%% function.  Compare provides a sort of fold over the differences of
%% the tree allowing for callers to determine what to do with those
%% differences. In addition, the caller can accumulate a value, such
%% as the difference list or stats about differencces.
%%
%% The tree implemented in this module assumes that it will be managed
%% by a single process and that all calls will be made to it synchronously, with
%% a couple exceptions:
%%
%% 1. Updating a tree with a snapshot can be done in another process. The snapshot
%% must be taken by the owning process, synchronously.
%% 2. Comparing two trees may be done by a seperate process. Compares should should use
%% a snapshot and only be performed after an update.
%%
%% The nodes in this tree are backend by LevelDB, however, this is
%% most likely temporary and Cluster Metadata's use of the tree is
%% ephemeral. Trees are only meant to live for the lifetime of a
%% running node and are rebuilt on start.  To ensure the tree is fresh
%% each time, when nodes are created the backing LevelDB store is
%% opened, closed, and then re-opened to ensure any lingering files
%% are removed.  Additionally, the nodes themselves (references to
%% {@link hashtree}, are stored in {@link ets}.


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
          %% the identifier for this tree. used as part of the ids
          %% passed to hashtree.erl and in keys used to store nodes in
          %% the tree's ets tables.
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
-type handler_fun(X)   :: fun((diff(), X) -> X).
-type remote_fun()     :: fun((prefixes(),
                               {get_bucket, {integer(), integer()}} |
                               {key_hashses, integer()}) -> orddict:orddict()).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Creates a new hashtree.
%%
%% Takes the following options:
%%   * num_levels - the height of the tree excluding leaves. corresponds to the
%%                  length of the prefix list passed to {@link insert/5}.
%%   * data_dir   - the directory where the LevelDB instances for the nodes will
%%                  be stored.
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

%% @doc Destroys the tree cleaning up any used resources.
%% This deletes the LevelDB files for the nodes.
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

%% @doc Insert a hash into the tree. The length of `Prefixes' must
%% correspond to the height of the tree -- the value used for
%% `num_levels' when creating the tree. The hash is inserted into
%% a leaf of the tree and that leaf is marked as dirty. The tree is not
%% updated at this time. Future operations on the tree should used the
%% tree returend by this fucntion.
%%
%% Insert takes the following options:
%%   * if_missing - if `true' then the hash is only inserted into the tree
%%                  if the key is not already present. This is useful for
%%                  ensuring writes concurrent with building the tree
%%                  take precedence over older values. `false' is the default
%%                  value.
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

%% @doc Snapshot the tree for updating. The return tree should be
%% updated using {@link update_perform/1} and to perform future operations
%% on the tree
-spec update_snapshot(tree()) -> tree().
update_snapshot(Tree=#hashtree_tree{dirty=Dirty,nodes=Nodes,snapshot=Snapshot0}) ->
    catch ets:delete(Snapshot0),
    FoldRes = gb_sets:fold(fun(DirtyName, Acc) ->
                                   DirtyKey = node_key(DirtyName, Tree),
                                   Node = lookup_node(DirtyName, Tree),
                                   {DirtyNode, NewNode} = hashtree:update_snapshot(Node),
                                   [{{DirtyKey, DirtyNode}, {DirtyKey, NewNode}} | Acc]
                           end, [], Dirty),
    {Snaps, NewNodes} = lists:unzip(FoldRes),
    Snapshot = ets:new(undefined, []),
    ets:insert(Snapshot, Snaps),
    ets:insert(Nodes, NewNodes),
    Tree#hashtree_tree{dirty=gb_sets:new(),snapshot=Snapshot}.


%% @doc Update the tree with a snapshot obtained by {@link
%% update_snapshot/1}. This function may be called by a process other
%% than the one managing the tree.
-spec update_perform(tree()) -> ok.
update_perform(Tree=#hashtree_tree{snapshot=Snapshot}) ->
    DirtyParents = ets:foldl(fun(DirtyLeaf, DirtyParentsAcc) ->
                                     update_dirty_leaves(DirtyLeaf, DirtyParentsAcc, Tree)
                             end,
                             gb_sets:new(), Snapshot),
    update_dirty_parents(DirtyParents, Tree),
    catch ets:delete(Snapshot),
    ok.

%% @doc Compare two local trees. This function is primarily for
%% local debugging and testing.
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

%% @doc Compare a local and remote tree.  `RemoteFun' is used to
%% access the buckets and segments of nodes in the remote
%% tree. `HandlerFun' will be called for each difference found in the
%% tree. A difference is either a missing local or remote prefix, or a
%% list of key differences, which themselves signify different or
%% missing keys. `HandlerAcc' is passed to the first call of
%% `HandlerFun' and each subsequent call is passed the value returned
%% by the previous call. The return value of this function is the
%% return value from the last call to `HandlerFun'.
-spec compare(tree(), remote_fun(), handler_fun(X), X) -> X.
compare(LocalTree, RemoteFun, HandlerFun, HandlerAcc) ->
    compare(?ROOT, 1, LocalTree, RemoteFun, HandlerFun, HandlerAcc).

%% @doc Returns the top-hash of the tree. This is the top-hash of the
%% root node.
-spec top_hash(tree()) -> undefined | binary().
top_hash(Tree) ->
    prefix_hash([], Tree).

%% @doc Returns the top-hash of the node corresponding to the given
%% prefix list. The length of the prefix list can be less than or
%% equal to the height of the tree. If the tree has not been updated
%% or if the prefix list is not found or invalid, then `undefined' is
%% returned.  Otherwise the hash value from the most recent update is
%% returned.
-spec prefix_hash(prefixes(), tree()) -> undefined | binary().
prefix_hash(Prefixes, Tree) ->
    NodeName = prefixes_to_node_name(Prefixes),
    case lookup_node(NodeName, Tree) of
        undefined -> undefined;
        Node -> extract_top_hash(hashtree:top_hash(Node))
    end.

%% @doc Returns the {@link hashtree} buckets for a given node in the
%% tree. This is used primarily for accessing buckets of a remote tree
%% during compare.
-spec get_bucket(tree_node(), integer(), integer(), tree()) -> orddict:orddict().
get_bucket(Prefixes, Level, Bucket, Tree) ->
    case lookup_node(prefixes_to_node_name(Prefixes), Tree) of
        undefined -> orddict:new();
        Node -> hashtree:get_bucket(Level, Bucket, Node)
    end.

%% @doc Returns the {@link hashtree} segment hashes for a given node
%% in the tree.  This is used primarily for accessing key hashes of a
%% remote tree during compare.
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
    _ = set_node(NodeName, Node2, Tree),
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
                                  _ = set_node(DirtyParent, DirtyNode2, Tree),
                                  NextDirty
                          end, gb_sets:new(), DirtyParents),
            update_dirty_parents(NextDirty, Tree)
    end.

%% @private
update_dirty(DirtyName, DirtyNode, NextDirty, Tree) ->
    %% ignore returned tree b/c we are tracking dirty nodes in this fold seperately
    _ = hashtree:update_perform(DirtyNode),
    case parent_node(DirtyName, Tree) of
        undefined ->
            NextDirty;
        {ParentName, ParentNode} ->
            TopHash = extract_top_hash(hashtree:top_hash(DirtyNode)),
            ParentKey = to_parent_key(DirtyName),
            %% ignore returned tree b/c we are tracking dirty nodes in this fold seperately
            _ = insert_hash(ParentKey, TopHash, [], ParentName, ParentNode, Tree),
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
    CompareRes = hashtree:compare(LocalNode, RemoteNode, AccFun, []),
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
    CompareRes = hashtree:compare(LocalNode, RemoteNode, AccFun, []),
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
create_node(?ROOT, Tree) ->
    NodeId = node_id(?ROOT, Tree),
    NodePath = node_path(Tree),
    NumSegs = node_num_segs(?ROOT),
    Width = node_width(?ROOT),
    Opts = [{segment_path, NodePath}, {segments, NumSegs}, {width, Width}],
    %% destroy any data that previously existed because its lingering from
    %% a tree that was not properly destroyed
    ok = hashtree:destroy(NodePath),
    Node = hashtree:new(NodeId, Opts),
    set_node(?ROOT, Node, Tree);
create_node([], Tree) ->
    create_node(?ROOT, Tree);
create_node(NodeName, Tree) ->
    NodeId = node_id(NodeName, Tree),
    RootNode = get_node(?ROOT, Tree),
    NumSegs = node_num_segs(NodeName),
    Width = node_width(NodeName),
    Opts = [{segments, NumSegs}, {width, Width}],
    %% share segment store accross all nodes
    Node = hashtree:new(NodeId, RootNode, Opts),
    set_node(NodeName, Node, Tree).

%% @private
set_node(NodeName, Node, Tree) when is_list(NodeName) orelse NodeName =:= ?ROOT ->
    set_node(node_key(NodeName, Tree), Node, Tree);
set_node(NodeKey, Node, #hashtree_tree{nodes=Nodes}) when is_tuple(NodeKey) ->
    ets:insert(Nodes, [{NodeKey, Node}]),
    Node.

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
node_path(#hashtree_tree{data_root=DataRoot}) ->
    DataRoot.

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
