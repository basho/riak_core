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
-module(riak_core_metadata_hashtree).

-behaviour(gen_server).

%% API
-export([start_link/0,
         start_link/1,
         insert/2,
         insert/3,
         prefix_hash/1,
         get_bucket/4,
         key_hashes/3,
         lock/0,
         lock/1,
         lock/2,
         update/0,
         update/1,
         compare/3]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("riak_core_metadata.hrl").

-define(SERVER, ?MODULE).

-record(state, {
          %% the tree managed by this process
          tree  :: hashtree_tree:tree(),

          %% whether or not the tree has been built or a monitor ref
          %% if the tree is being built
          built :: boolean() | reference(),

          %% a monitor reference for a process that currently holds a
          %% lock on the tree. undefined otherwise
          lock  :: {internal | external, reference()} | undefined
         }).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Starts the process using {@link start_link/1}, passing in the
%% directory where other cluster metadata is stored in `platform_data_dir'
%% as the data root.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    PRoot = app_helper:get_env(riak_core, platform_data_dir),
    DataRoot = filename:join(PRoot, "cluster_meta/trees"),
    start_link(DataRoot).

%% @doc Starts a registered process that manages a {@link
%% hashtree_tree} for Cluster Metadata. Data for the tree is stored,
%% for the lifetime of the process (assuming it shutdowns gracefully),
%% in the directory `DataRoot'.
-spec start_link(file:filename()) -> {ok, pid()} | ignore | {error, term()}.
start_link(DataRoot) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [DataRoot], []).

%% @doc Same as insert(PKey, Hash, false).
-spec insert(metadata_pkey(), binary()) -> ok.
insert(PKey, Hash) ->
    insert(PKey, Hash, false).

%% @doc Insert a hash for a full-prefix and key into the tree
%% managed by the process. If `IfMissing' is `true' the hash is only
%% inserted into the tree if the key is not already present.
-spec insert(metadata_pkey(), binary(), boolean()) -> ok.
insert(PKey, Hash, IfMissing) ->
    gen_server:call(?SERVER, {insert, PKey, Hash, IfMissing}, infinity).

%% @doc Return the hash for the given prefix or full-prefix
-spec prefix_hash(metadata_prefix() | binary() | atom()) -> undefined | binary().
prefix_hash(Prefix) ->
    gen_server:call(?SERVER, {prefix_hash, Prefix}, infinity).

%% @doc Return the bucket for a node in the tree managed by this
%% process running on `Node'.
-spec get_bucket(node(), hashtree_tree:tree_node(),
                 non_neg_integer(), non_neg_integer()) -> orddict:orddict().
get_bucket(Node, Prefixes, Level, Bucket) ->
    gen_server:call({?SERVER, Node}, {get_bucket, Prefixes, Level, Bucket}, infinity).

%% @doc Return the key hashes for a node in the tree managed by this
%% process running on `Node'.
-spec key_hashes(node(), hashtree_tree:tree_node(), non_neg_integer()) -> orddict:orddict().
key_hashes(Node, Prefixes, Segment) ->
    gen_server:call({?SERVER, Node}, {key_hashes, Prefixes, Segment}, infinity).

%% @doc Locks the tree on this node for updating on behalf of the
%% calling process.
%% @see lock/2
-spec lock() -> ok | not_built | locked.
lock() ->
    lock(node()).

%% @doc Locks the tree on `Node' for updating on behalf of the calling
%% process.
%% @see lock/2
-spec lock(node()) -> ok | not_built | locked.
lock(Node) ->
    lock(Node, self()).

%% @doc Lock the tree for updating. This function must be called
%% before updating the tree with {@link update/0} or {@link
%% update/1}. If the tree is not built or already locked then the call
%% will fail and the appropriate atom is returned. Otherwise,
%% aqcuiring the lock succeeds and `ok' is returned.
-spec lock(node(), pid()) -> ok | not_built | locked.
lock(Node, Pid) ->
    gen_server:call({?SERVER, Node}, {lock, Pid}, infinity).

%% @doc Updates the tree on this node.
%% @see update/1
-spec update() -> ok | not_locked | not_built | ongoing_update.
update() ->
    update(node()).

%% @doc Updates the tree on `Node'. The tree must be locked using one
%% of the lock functions. If the tree is not locked or built the
%% update will not be started and the appropriate atom is
%% returned. Although this function should not be called without a
%% lock, if it is and the tree is being updated by the background tick
%% then `ongoing_update' is returned. If the tree is built and a lock
%% has been acquired then the update is started and `ok' is
%% returned. The update is performed asynchronously and does not block
%% the process that manages the tree (e.g. future inserts).
-spec update(node()) -> ok | not_locked | not_built | ongoing_update.
update(Node) ->
    gen_server:call({?SERVER, Node}, update, infinity).

%% @doc Compare the local tree managed by this process with the remote
%% tree also managed by a metadata hashtree process. `RemoteFun' is
%% used to access the buckets and segments of nodes in the remote tree
%% and should usually call {@link get_bucket/4} and {@link
%% key_hashes/3}. `HandlerFun' is used to process the differences
%% found between the two trees. `HandlerAcc' is passed to the first
%% invocation of `HandlerFun'. Subsequent calls are passed the return
%% value from the previous call.  This function returns the return
%% value from the last call to `HandlerFun'. {@link hashtree_tree} for
%% more details on `RemoteFun', `HandlerFun' and `HandlerAcc'.
-spec compare(hashtree_tree:remote_fun(), hashtree_tree:handler_fun(X), X) -> X.
compare(RemoteFun, HandlerFun, HandlerAcc) ->
    gen_server:call(?SERVER, {compare, RemoteFun, HandlerFun, HandlerAcc}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([DataRoot]) ->
    schedule_tick(),
    Tree = hashtree_tree:new(cluster_meta, [{data_dir, DataRoot}, {num_levels, 2}]),
    State = #state{tree=Tree,
                   built=false,
                   lock=undefined},
    State1 = build_async(State),
    {ok, State1}.

handle_call({compare, RemoteFun, HandlerFun, HandlerAcc}, From, State) ->
    maybe_compare_async(From, RemoteFun, HandlerFun, HandlerAcc, State),
    {noreply, State};
handle_call(update, From, State) ->
    State1 = maybe_external_update(From, State),
    {noreply, State1};
handle_call({lock, Pid}, _From, State) ->
    {Reply, State1} = maybe_external_lock(Pid, State),
    {reply, Reply, State1};
handle_call({get_bucket, Prefixes, Level, Bucket}, _From, State) ->
    Res = hashtree_tree:get_bucket(Prefixes, Level, Bucket, State#state.tree),
    {reply, Res, State};
handle_call({key_hashes, Prefixes, Segment}, _From, State) ->
    [{_, Res}] = hashtree_tree:key_hashes(Prefixes, Segment, State#state.tree),
    {reply, Res, State};
handle_call({prefix_hash, Prefix}, _From, State=#state{tree=Tree}) ->
    PrefixList = prefix_to_prefix_list(Prefix),
    PrefixHash = hashtree_tree:prefix_hash(PrefixList, Tree),
    {reply, PrefixHash, State};
handle_call({insert, PKey, Hash, IfMissing}, _From, State=#state{tree=Tree}) ->
    {Prefixes, Key} = prepare_pkey(PKey),
    Tree1 = hashtree_tree:insert(Prefixes, Key, Hash, [{if_missing, IfMissing}], Tree),
    {reply, ok, State#state{tree=Tree1}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', BuildRef, process, _Pid, normal}, State=#state{built=BuildRef}) ->
    State1 = build_done(State),
    {noreply, State1};
handle_info({'DOWN', BuildRef, process, _Pid, Reason}, State=#state{built=BuildRef}) ->
    lager:error("building tree failed: ~p", [Reason]),
    State1 = build_error(State),
    {noreply, State1};
handle_info({'DOWN', LockRef, process, _Pid, _Reason}, State=#state{lock={_, LockRef}}) ->
    State1 = release_lock(State),
    {noreply, State1};
handle_info(tick, State) ->
    schedule_tick(),
    State1 = maybe_build_async(State),
    State2 = maybe_update_async(State1),
    {noreply, State2}.

terminate(_Reason, State) ->
    hashtree_tree:destroy(State#state.tree),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
maybe_compare_async(From, RemoteFun, HandlerFun, HandlerAcc,
                    State=#state{built=true,lock={external,_}}) ->
    compare_async(From, RemoteFun, HandlerFun, HandlerAcc, State);
maybe_compare_async(From, _, _, HandlerAcc, _State) ->
    gen_server:reply(From, HandlerAcc).

%% @private
compare_async(From, RemoteFun, HandlerFun, HandlerAcc, #state{tree=Tree}) ->
    spawn_link(fun() ->
                       Res = hashtree_tree:compare(Tree, RemoteFun,
                                                   HandlerFun, HandlerAcc),
                       gen_server:reply(From, Res)
               end).

%% @private
maybe_external_update(From, State=#state{built=true,lock=undefined}) ->
    gen_server:reply(From, not_locked),
    State;
maybe_external_update(From, State=#state{built=true,lock={internal,_}}) ->
    gen_server:reply(From, ongoing_update),
    State;
maybe_external_update(From, State=#state{built=true,lock={external,_}}) ->
    update_async(From, false, State);
maybe_external_update(From, State) ->
    gen_server:reply(From, not_built),
    State.

%% @private
maybe_update_async(State=#state{built=true,lock=undefined}) ->
    update_async(State);
maybe_update_async(State) ->
    State.

%% @private
update_async(State) ->
    update_async(undefined, true, State).

%% @private
update_async(From, Lock, State=#state{tree=Tree}) ->
    Tree2 = hashtree_tree:update_snapshot(Tree),
    Pid = spawn_link(fun() ->
                             hashtree_tree:update_perform(Tree2),
                             case From of
                                 undefined -> ok;
                                 _ -> gen_server:reply(From, ok)
                             end
                     end),
    State1 = case Lock of
                 true -> lock(Pid, internal, State);
                 false -> State
             end,
    State1#state{tree=Tree2}.

%% @private
maybe_build_async(State=#state{built=false}) ->
    build_async(State);
maybe_build_async(State) ->
    State.

%% @private
build_async(State) ->
    {_Pid, Ref} = spawn_monitor(fun build/0),
    State#state{built=Ref}.

%% @private
build() ->
    PrefixIt = riak_core_metadata_manager:iterator(),
    build(PrefixIt).

%% @private
build(PrefixIt) ->
    case riak_core_metadata_manager:iterator_done(PrefixIt) of
        true ->
            riak_core_metadata_manager:iterator_close(PrefixIt);
        false ->
            Prefix = riak_core_metadata_manager:iterator_value(PrefixIt),
            ObjIt = riak_core_metadata_manager:iterator(Prefix, undefined),
            build(PrefixIt, ObjIt)
    end.

%% @private
build(PrefixIt, ObjIt) ->
    case riak_core_metadata_manager:iterator_done(ObjIt) of
        true ->
            riak_core_metadata_manager:iterator_close(ObjIt),
            build(riak_core_metadata_manager:iterate(PrefixIt));
        false ->
            FullPrefix = riak_core_metadata_manager:iterator_prefix(ObjIt),
            {Key, Obj} = riak_core_metadata_manager:iterator_value(ObjIt),
            Hash = riak_core_metadata_object:hash(Obj),
            %% insert only if missing to not clash w/ newer writes during build
            ?MODULE:insert({FullPrefix, Key}, Hash, true),
            build(PrefixIt, riak_core_metadata_manager:iterate(ObjIt))
    end.

%% @private
build_done(State) ->
    State#state{built=true}.

%% @private
build_error(State) ->
    State#state{built=false}.

%% @private
maybe_external_lock(Pid, State=#state{lock=undefined,built=true}) ->
    {ok, lock(Pid, external, State)};
maybe_external_lock(_Pid, State=#state{built=true}) ->
    {locked, State};
maybe_external_lock(_Pid, State) ->
    {not_built, State}.

%% @private
lock(Pid, Type, State) ->
    LockRef = monitor(process, Pid),
    State#state{lock={Type, LockRef}}.

%% @private
release_lock(State) ->
    State#state{lock=undefined}.

%% @private
prefix_to_prefix_list(Prefix) when is_binary(Prefix) or is_atom(Prefix) ->
    [Prefix];
prefix_to_prefix_list({Prefix, SubPrefix}) ->
    [Prefix,SubPrefix].

%% @private
prepare_pkey({FullPrefix, Key}) ->
    {prefix_to_prefix_list(FullPrefix), term_to_binary(Key)}.

%% @private
schedule_tick() ->
    TickMs = app_helper:get_env(riak_core, metadata_hashtree_timer, 10000),
    erlang:send_after(TickMs, ?MODULE, tick).
