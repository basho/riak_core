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
         iterator/2,
         iterate/1,
         iterator_prefix/1,
         iterator_value/1,
         iterator_done/1,
         put/3]).

%% riak_core_broadcast_handler callbacks
-export([broadcast_data/1,
         merge/2,
         is_stale/1,
         graft/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([metadata_iterator/0]).

-include("riak_core_metadata.hrl").

-define(SERVER, ?MODULE).
-define(MANIFEST, cluster_meta_manifest).
-define(MANIFEST_FILENAME, "manifest.dets").

-record(state, {
          %% identifier used in logical clocks
          serverid   :: term(),

          %% where data files are stored
          data_root  :: file:filename(),

          %% an ets table holding references to per
          %% full-prefix ets tables
          ets_tabs   :: ets:tab()
         }).

-record(metadata_iterator, {
          prefix :: metadata_prefix(),
          match  :: term(),
          pos    :: term(),
          obj    :: {metadata_key(), metadata_object()},
          done   :: boolean(),
          tab    :: ets:tab()
         }).

-opaque metadata_iterator() :: #metadata_iterator{}.

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
get({{Prefix, SubPrefix}, _Key}=PKey) when (is_binary(Prefix) orelse is_atom(Prefix)) andalso
                                           (is_binary(SubPrefix) orelse is_atom(SubPrefix)) ->
    gen_server:call(?SERVER, {get, PKey}).

%% @doc Return an iterator for keys stored under a prefix. If KeyMatch is undefined then
%% all keys will may be visted by the iterator. Otherwise only keys matching KeyMatch will be
%% visited.
%%
%% KeyMatch can be either:
%%   * an erlang term - which will be matched exactly against a key
%%   * '_' - which is equivalent to undefined
%%   * an erlang tuple containing terms and '_' - if tuples are used as keys
%%   * this can be used to iterate over some subset of keys
-spec iterator(metadata_prefix() , term()) -> metadata_iterator().
iterator({Prefix, SubPrefix}=FullPrefix, KeyMatch)
  when (is_binary(Prefix) orelse is_atom(Prefix)) andalso
       (is_binary(SubPrefix) orelse is_atom(SubPrefix)) ->
    gen_server:call(?SERVER, {iterator, FullPrefix, KeyMatch}).


%% @doc advance the iterator by one key
-spec iterate(metadata_iterator()) -> metadata_iterator().
iterate(Iterator) ->
    gen_server:call(?SERVER, {iterate, Iterator}).

%% @doc return the full prefix being iterated by this iterator
-spec iterator_prefix(metadata_iterator()) -> metadata_prefix().
iterator_prefix(#metadata_iterator{prefix=Prefix}) -> Prefix.

%% @doc return the key and object pointed to by the iterator
-spec iterator_value(metadata_iterator()) -> {metadata_key(), metadata_object()}.
iterator_value(#metadata_iterator{obj=Obj}) -> Obj.

%% @doc returns true if there are no more keys to iterate over
-spec iterator_done(metadata_iterator()) -> boolean().
iterator_done(#metadata_iterator{done=Done}) -> Done.

%% @doc Sets the value of a prefixed key. The most recently read context (see get/1)
%% should be passed as the second argument to prevent unneccessary siblings.
-spec put(metadata_pkey(),
          metadata_context() | undefined,
          metadata_value() | metadata_modifier()) -> metadata_object().
put(PKey, undefined, ValueOrFun) ->
    %% nil is an empty version vector for dvvset
    put(PKey, [], ValueOrFun);
put({{Prefix, SubPrefix}, _Key}=PKey, Context, ValueOrFun)
  when (is_binary(Prefix) orelse is_atom(Prefix)) andalso
       (is_binary(SubPrefix) orelse is_atom(SubPrefix)) ->
    gen_server:call(?SERVER, {put, PKey, Context, ValueOrFun}).

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
            State = #state{serverid=Nodename,
                           data_root=DataRoot,
                           ets_tabs=new_ets_tab()},
            init_manifest(State),
            %% TODO: should do this out-of-band from startup so we don't block
            init_from_files(State),
            {ok, State}
    end.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
                         {reply, term(), #state{}} |
                         {reply, term(), #state{}, non_neg_integer()} |
                         {noreply, #state{}} |
                         {noreply, #state{}, non_neg_integer()} |
                         {stop, term(), term(), #state{}} |
                         {stop, term(), #state{}}.
handle_call({put, PKey, Context, ValueOrFun}, _From, State) ->
    {Result, NewState} = read_modify_write(PKey, Context, ValueOrFun, State),
    {reply, Result, NewState};
handle_call({merge, PKey, Obj}, _From, State) ->
    {Result, NewState} = read_merge_write(PKey, Obj, State),
    {reply, Result, NewState};
handle_call({get, PKey}, _From, State) ->
    Result = read(PKey, State),
    {reply, Result, State};
handle_call({iterator, FullPrefix, KeyMatch}, _From, State) ->
    Iterator = iterator(FullPrefix, KeyMatch, State),
    {reply, Iterator, State};
handle_call({iterate, Iterator}, _From, State) ->
    Next = next_iterator(Iterator),
    {reply, Next, State};
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
    close_dets_tabs(),
    close_manifest(),
    ok.


%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
iterator(FullPrefix, KeyMatch, State) ->
    case ets_tab(FullPrefix, State) of
        undefined -> empty_iterator(FullPrefix, KeyMatch, undefined);
        Tab -> new_iterator(FullPrefix, KeyMatch, Tab)
    end.

next_iterator(It=#metadata_iterator{done=true}) ->
    It;
next_iterator(It=#metadata_iterator{pos=Pos}) ->
    next_iterator(It, ets:match_object(Pos)).

next_iterator(It, '$end_of_table') ->
    It#metadata_iterator{done=true,
                         pos=undefined,
                         obj=undefined};
next_iterator(It, {[Next], Cont}) ->
    It#metadata_iterator{pos=Cont,
                         obj=Next}.

empty_iterator(FullPrefix, KeyMatch, Tab) ->
    #metadata_iterator{
                prefix=FullPrefix,
                match=KeyMatch,
                pos=undefined,
                obj=undefined,
                done=true,
                tab=Tab
               }.

new_iterator(FullPrefix, KeyMatch, Tab) ->
    ObjectMatch = iterator_match(KeyMatch),
    new_iterator(FullPrefix, KeyMatch, Tab, ets:match_object(Tab, ObjectMatch, 1)).

new_iterator(FullPrefix, KeyMatch, Tab, '$end_of_table') ->
    empty_iterator(FullPrefix, KeyMatch, Tab);
new_iterator(FullPrefix, KeyMatch, Tab, {[First], Cont}) ->
    #metadata_iterator{
              prefix=FullPrefix,
              match=KeyMatch,
              pos=Cont,
              obj=First,
              done=false,
              tab=Tab
             }.

iterator_match(undefined) ->
    '_';
iterator_match(KeyMatch) ->
    {KeyMatch, '_'}.

read_modify_write(PKey, Context, ValueOrFun, State=#state{serverid=ServerId}) ->
    Existing = read(PKey, State),
    Modified = riak_core_metadata_object:modify(Existing, Context, ValueOrFun, ServerId),
    store(PKey, Modified, State).

read_merge_write(PKey, Obj, State) ->
    Existing = read(PKey, State),
    case riak_core_metadata_object:reconcile(Obj, Existing) of
        false -> {false, State};
        {true, Reconciled} ->
            {_, NewState} = store(PKey, Reconciled, State),
            {true, NewState}
    end.

store({FullPrefix, Key}, Metadata, State) ->
    maybe_init_ets(FullPrefix, State),
    maybe_init_dets(FullPrefix, State),

    Objs = [{Key, Metadata}],
    ets:insert(ets_tab(FullPrefix, State), Objs),
    ok = dets_insert(dets_tabname(FullPrefix), Objs),
    {Metadata, State}.

read({FullPrefix, Key}, State=#state{}) ->
    case ets_tab(FullPrefix, State) of
        undefined -> undefined;
        TabId -> read(Key, TabId)
    end;
read(Key, TabId) ->
    case ets:lookup(TabId, Key) of
        [] -> undefined;
        [{Key, MetaRec}] -> MetaRec
    end.

init_manifest(State) ->
    ManifestFile = filename:join(State#state.data_root, ?MANIFEST_FILENAME),
    ok = filelib:ensure_dir(ManifestFile),
    {ok, ?MANIFEST} = dets:open_file(?MANIFEST, [{file, ManifestFile}]).

close_manifest() ->
    dets:close(?MANIFEST).

init_from_files(State) ->
    %% TODO: do this in parallel
    dets_fold_tabnames(fun init_from_file/2, State).

init_from_file(TabName, State) ->
    FullPrefix = dets_tabname_to_prefix(TabName),
    FileName = dets_file(State#state.data_root, FullPrefix),
    {ok, TabName} = dets:open_file(TabName, [{file, FileName}]),
    TabId = init_ets(FullPrefix, State),
    dets:to_ets(TabName, TabId),
    State.

ets_tab(FullPrefix, #state{ets_tabs=Tabs}) ->
    case ets:lookup(Tabs, FullPrefix) of
        [] -> undefined;
        [{FullPrefix, TabId}] -> TabId
    end.

maybe_init_ets(FullPrefix, State) ->
    case ets_tab(FullPrefix, State) of
        undefined -> init_ets(FullPrefix, State);
        _TabId -> ok
    end.

init_ets(FullPrefix, #state{ets_tabs=Tabs}) ->
    TabId = new_ets_tab(),
    ets:insert(Tabs, [{FullPrefix, TabId}]),
    TabId.

new_ets_tab() ->
    ets:new(undefined, []).

maybe_init_dets(FullPrefix, State) ->
    case dets:info(dets_tabname(FullPrefix)) of
        undefined -> init_dets(FullPrefix, State);
        _ -> State
    end.

init_dets(FullPrefix, #state{data_root=DataRoot}) ->
    TabName = dets_tabname(FullPrefix),
    FileName = dets_file(DataRoot, FullPrefix),
    {ok, TabName} = dets:open_file(TabName, [{file, FileName}]),
    dets_insert(?MANIFEST, [{FullPrefix, TabName, FileName}]).

close_dets_tabs() ->
    dets_fold_tabnames(fun close_dets_tab/2, undefined).

close_dets_tab(TabName, _Acc) ->
    dets:close(TabName).

dets_insert(TabName, Objs) ->
    ok = dets:insert(TabName, Objs),
    ok = dets:sync(TabName).

dets_tabname(FullPrefix) -> {?MODULE, FullPrefix}.
dets_tabname_to_prefix({?MODULE, FullPrefix}) ->  FullPrefix.

dets_file(DataRoot, FullPrefix) ->
    filename:join(DataRoot, dets_filename(FullPrefix)).

dets_filename({Prefix, SubPrefix}=FullPrefix) ->
    MD5Prefix = dets_filename_part(Prefix),
    MD5SubPrefix = dets_filename_part(SubPrefix),
    Trailer = dets_filename_trailer(FullPrefix),
    io_lib:format("~s-~s-~s.dets", [MD5Prefix, MD5SubPrefix, Trailer]).

dets_filename_part(Part) when is_atom(Part) ->
    dets_filename_part(list_to_binary(atom_to_list(Part)));
dets_filename_part(Part) when is_binary(Part) ->
    <<MD5Int:128/integer>> = riak_core_util:md5(Part),
    riak_core_util:integer_to_list(MD5Int, 16).

dets_filename_trailer(FullPrefix) ->
    [dets_filename_trailer_part(Part) || Part <- tuple_to_list(FullPrefix)].

dets_filename_trailer_part(Part) when is_atom(Part) ->
    "1";
dets_filename_trailer_part(Part) when is_binary(Part)->
    "0".

dets_fold_tabnames(Fun, Acc0) ->
    dets:foldl(fun({_FullPrefix, TabName, _FileName}, Acc) ->
                       Fun(TabName, Acc)
               end, Acc0, ?MANIFEST).

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
