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
-module(riak_core_metadata_manager).

-behaviour(gen_server).
-behaviour(riak_core_broadcast_handler).

%% API
-export([start_link/0,
         start_link/1,
         get/1,
         put/3]).

%% riak_core_broadcast_handler callbacks
-export([broadcast_data/1,
         merge/2,
         is_stale/1,
         graft/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("riak_core_metadata.hrl").

-define(SERVER, ?MODULE).
-define(MM_DETS, mm_dets).
-define(MM_ETS, mm_ets).

-record(state, {serverid :: term()}).

-type mm_path_opt()     :: {data_dir, file:name_all()}.
-type mm_nodename_opt() :: {nodename, term()}.
-type mm_opt()          :: mm_path_opt() | mm_nodename_opt().
-type mm_opts()         :: [mm_opt()].

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    start_link([]).

%% @doc Start riak_core_metadadata_manager and link to calling process.
%%
%% The following options can be provided:
%%    * data_dir: the root directory to place cluster metadata files.
%%                if not provided this defaults to the `cluster_meta' directory in
%%                riak_core's `platform_data_dir'.
%%    * nodename: the node identifier (for use in logical clocks). defaults to node()
-spec start_link(mm_opts()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Opts], []).

%% @doc Reads the value for a prefixed key. If the value does not exist `undefined' is
%% returned. otherwise a Dotted Version Vector Set is returned. When reading the value
%% for a subsequent call to put/3 the context can be obtained using
%% riak_core_metadata_object:context/1. Values can obtained w/ riak_core_metadata_object:values/1.
-spec get(metadata_pkey()) -> metadata_object() | undefined.
get(PKey) ->
    gen_server:call(?SERVER, {get, PKey}).

%% @doc Sets the value of a prefixed key. The most recently read context (see get/1)
%% should be passed as the second argument to prevent unneccessary siblings.
-spec put(metadata_pkey(), metadata_context() | undefined, metadata_value()) -> metadata_object().
put(PKey, undefined, Value) ->
    %% nil is an empty version vector for dvvset
    put(PKey, [], Value);
put(PKey, Context, Value) ->
    gen_server:call(?SERVER, {put, PKey, Context, Value}).

%%%===================================================================
%%% riak_core_broadcast_handler callbacks
%%%===================================================================

%% @doc Deconstructs are broadcast that is sent using `riak_core_metadata_manager' as the
%% handling module returning the message id and payload.
-spec broadcast_data(metadata_broadcast()) -> {{metadata_pkey(), metadata_context()},
                                               metadata_object()}.
broadcast_data(#metadata_broadcast{pkey=Key, obj=Obj}) ->
    Context = riak_core_metadata_object:context(Obj),
    {{Key, Context}, Obj}.

%% @doc Merges a remote copy of a metadata record sent via broadcast w/ the local view
%% for the key contained in the message id. If the remote copy is causally older than
%% the current data stored then `false' is returned and no updates are merged. Otherwise,
%% the remote copy is merged (possibly generating siblings) and `true' is returned.
-spec merge({metadata_pkey(), metadata_context()}, metadata_object()) -> boolean().
merge({PKey, _Context}, Obj) ->
    gen_server:call(?SERVER, {merge, PKey, Obj}).

%% @doc Returns false if the update (or a causally newer update) has already been
%% received (stored locally).
-spec is_stale({metadata_pkey(), metadata_context()}) -> boolean().
is_stale({PKey, Context}) ->
    gen_server:call(?SERVER, {is_stale, PKey, Context}).

%% @doc returns the object associated with the given key and context (message id) if
%% the currently stored version has an equal context. otherwise stale is returned.
%% because it assumed that a grafted context can only be causally older than the local view
%% a stale response means there is another message that subsumes the grafted one
-spec graft({metadata_pkey(), metadata_context()}) ->
                   stale | {ok, metadata_object()} | {error, term()}.
graft({PKey, Context}) ->
    case ?MODULE:get(PKey) of
        undefined ->
            %% There would have to be a serious error in implementation to hit this case.
            %% Catch if here b/c it would be much harder to detect
            lager:error("object not found during graft for key: ~p", [PKey]),
            {error, {not_found, PKey}};
         Obj ->
            graft(Context, Obj)
    end.

graft(Context, Obj) ->
    case riak_core_metadata_object:equal_context(Context, Obj) of
        false ->
            %% when grafting the context will never be causally newer
            %% than what we have locally. Since its not equal, it must be
            %% an ancestor. Thus we've sent another, newer update that contains
            %% this context's information in addition to its own.  This graft
            %% is deemed stale
            stale;
        true ->
            {ok, Obj}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([mm_opts()]) -> {ok, #state{}} |
                           {ok, #state{}, non_neg_integer() | infinity} |
                           ignore |
                           {stop, term()}.
init([Opts]) ->
    case data_root(Opts) of
        undefined ->
            {stop, no_data_dir};
        DataRoot ->
            Nodename = proplists:get_value(nodename, Opts, node()),
            State = #state{serverid=Nodename},
            State2 = init_disk_storage(DataRoot, State),
            State3 = init_mem_storage(State2),
            {ok, State3}
    end.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
                         {reply, term(), #state{}} |
                         {reply, term(), #state{}, non_neg_integer()} |
                         {noreply, #state{}} |
                         {noreply, #state{}, non_neg_integer()} |
                         {stop, term(), term(), #state{}} |
                         {stop, term(), #state{}}.
handle_call({put, PKey, Context, Value}, _From, State) ->
    {Result, NewState} = read_modify_write(PKey, Context, Value, State),
    {reply, Result, NewState};
handle_call({merge, PKey, Obj}, _From, State) ->
    {Result, NewState} = read_merge_write(PKey, Obj, State),
    {reply, Result, NewState};
handle_call({get, PKey}, _From, State) ->
    Result = read(PKey, State),
    {reply, Result, State};
handle_call({is_stale, PKey, Context}, _From, State) ->
    Existing = read(PKey, State),
    IsStale = riak_core_metadata_object:is_stale(Context, Existing),
    {reply, IsStale, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}} |
                                       {noreply, #state{}, non_neg_integer()} |
                                       {stop, term(), #state{}}.
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
%% @doc Handling all non call/cast messages
-spec handle_info(term(), #state{}) -> {noreply, #state{}} |
                                       {noreply, #state{}, non_neg_integer()} |
                                       {stop, term(), #state{}}.
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok = dets:close(?MM_DETS).


%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
read_modify_write(PKey, Context, Value, State=#state{serverid=ServerId}) ->
    Existing = read(PKey, State),
    Modified = riak_core_metadata_object:modify(Existing, Context, Value, ServerId),
    store(PKey, Modified, State).

read_merge_write(PKey, Obj, State) ->
    Existing = read(PKey, State),
    case riak_core_metadata_object:reconcile(Obj, Existing) of
        false -> {false, State};
        {true, Reconciled} ->
            {_, NewState} = store(PKey, Reconciled, State),
            {true, NewState}
    end.

%% TODO: ets storage
store(PKey, Metadata, State) ->
    dets:insert(?MM_DETS, [{PKey, Metadata}]),
    {Metadata, State}.

%% TODO: add in ets lookup
read(Key, _State) ->
    case dets:lookup(?MM_DETS, Key) of
        [] -> undefined;
        [{Key, MetaRec}] -> MetaRec
    end.

init_disk_storage(DataRoot, State) ->
    DetsFile = dets_file(DataRoot),
    ok = filelib:ensure_dir(DetsFile),
    {ok, ?MM_DETS} = dets:open_file(?MM_DETS, [{file, DetsFile}]),
    State.

init_mem_storage(State) ->
    ?MM_ETS = ets:new(?MM_ETS, [named_table]),
%    dets:to_ets(?MM_DETS, ?MM_ETS),
    State.

dets_file(DataRoot) ->
    filename:join(DataRoot, "metadata").

data_root(Opts) ->
    case proplists:get_value(data_dir, Opts) of
        undefined -> default_data_root();
        Root -> Root
    end.

default_data_root() ->
    case application:get_env(riak_core, platform_data_dir) of
        {ok, PRoot} -> filename:join(PRoot, "cluster_meta");
        undefined -> undefined
    end.
