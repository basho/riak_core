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
         prefix_hash/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("riak_core_metadata.hrl").

-define(SERVER, ?MODULE). 

-record(state, {
          %% the tree managed by this process
          tree  :: hashtree_tree:tree(),

          %% whether or not the tree has been built or a pid if the
          %% tree is being built
          built :: boolean() | pid(),

          %% a monitor reference for a process that currently holds a
          %% lock on the tree. undefined otherwise
          lock  :: reference() | undefined
         }).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc TODO
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    PRoot = app_helper:get_env(riak_core, platform_data_dir),
    DataRoot = filename:join(PRoot, "cluster_meta/trees"),    
    start_link(DataRoot).

%% @doc TODO
-spec start_link(file:filename()) -> {ok, pid()} | ignore | {error, term()}.
start_link(DataRoot) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [DataRoot], []).

%% @doc TODO
-spec insert(metadata_pkey(), binary()) -> ok.
insert(PKey, Hash) ->
    insert(PKey, Hash, false).


%% @doc TODO
-spec insert(metadata_pkey(), binary(), boolean()) -> ok.
insert(PKey, Hash, IfMissing) ->
    gen_server:call(?SERVER, {insert, PKey, Hash, IfMissing}).

%% @doc TODO
-spec prefix_hash(metadata_prefix() | binary() | atom()) -> undefined | binary().
prefix_hash(Prefix) ->
    gen_server:call(?SERVER, {prefix_hash, Prefix}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([DataRoot]) ->
    process_flag(trap_exit, true),
    schedule_tick(),
    Tree = hashtree_tree:new(cluster_meta, [{data_dir, DataRoot}, {num_levels, 2}]),
    State = #state{tree=Tree,
                   built=false,
                   lock=undefined},
    State1 = build_async(State),
    {ok, State1}.

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

handle_info({'EXIT', BuiltPid, normal}, State=#state{built=BuiltPid}) ->
    State1 = build_done(State),
    {noreply, State1};
handle_info({'EXIT', BuiltPid, _}, State=#state{built=BuiltPid}) ->
    State1 = build_error(State),
    {noreply, State1};
handle_info({'DOWN', LockRef, process, _Pid, _Reason}, State=#state{lock=LockRef}) ->
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
maybe_update_async(State=#state{built=true,lock=undefined}) ->
    update_async(State);
maybe_update_async(State) ->
    State.

%% @private
update_async(State=#state{tree=Tree}) ->
    {Snap, Tree2} = hashtree_tree:update_snapshot(Tree),
    Pid = spawn(fun() ->
                        hashtree_tree:update_perform(Snap)
                end),
    LockRef = monitor(process, Pid),
    State#state{tree=Tree2,lock=LockRef}.

%% @private
maybe_build_async(State=#state{built=false}) ->
    build_async(State);
maybe_build_async(State) ->
    State.

%% @private
build_async(State) ->
    Pid = spawn_link(fun build/0),
    State#state{built=Pid}.

%% @private
build() ->
    PrefixIt = riak_core_metadata_manager:iterator(),
    build(PrefixIt).

%% @private
build(PrefixIt) ->
    case riak_core_metadata_manager:iterator_done(PrefixIt) of
        true -> ok;
        false ->
            Prefix = riak_core_metadata_manager:iterator_value(PrefixIt),
            ObjIt = riak_core_metadata_manager:iterator(Prefix, undefined),
            build(PrefixIt, ObjIt)
    end.

%% @private
build(PrefixIt, ObjIt) ->
    case riak_core_metadata_manager:iterator_done(ObjIt) of
        true ->
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
release_lock(State) ->
    State#state{lock=undefined}.

%% @private
prefix_to_prefix_list(Prefix) when is_binary(Prefix) or is_atom(Prefix) ->
    [Prefix];
prefix_to_prefix_list({Prefix, SubPrefix}) ->
    [Prefix,SubPrefix].

%% @private
prepare_pkey({FulLPrefix, Key}) ->
    {prefix_to_prefix_list(FulLPrefix), term_to_binary(Key)}.

%% @private
schedule_tick() ->
    TickMs = app_helper:get_env(riak_core, metadata_hashtree_timer, 10000),
    erlang:send_after(TickMs, ?MODULE, tick).

