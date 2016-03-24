-module(riak_core_aae_vnode).

-export([maybe_create_hashtrees/4,
         update_hashtree/4]).

-export([request_hashtree_pid/2,
         hashtree_pid/2,
         rehash/4]).

-define(DEFAULT_HASHTREE_TOKENS, 90).

-type preflist() :: [{Index::integer(), Node :: term()}].

%%%===================================================================
%%% Behaviour callbacks
%%%===================================================================

%% @doc aae_repair is called when the AAE system detectes a difference
%% the simplest method to handle this is causing a read-repair if the
%% system supports it. But the actual implemetation is left to the
%% vnode to handle whatever is best.
-callback aae_repair(Bucket::binary(), Key::term()) -> term().


%% @doc hash_object is called by the AAE subsyste to hash a object when the
%% tree first gets generated, a object needs to be hash or is inserted.
%% To AAE system does not care for the details as long as it returns a binary
%% and is deterministic in it's outcome. (see {@link riak_core_index_hashtree})
-callback hash_object({Bucket::binary(), Key::term()}, Obj::term()) -> binary().

%% @doc Returns the vnode master for this vnode type, that is the same
%% used when registering the vnode.
%% This function is required by the {@link riak_core_index_hashtree} to
%% send rehash requests to a vnode.

-callback master() -> Master::atom().


%%%===================================================================
%%% AAE Calls
%%%===================================================================

%% @doc This is a asyncronous command that needs to send a term in the form
%% `{ok, Hashtree::pid()}` or `{error, wrong_node}` to the process it was called
%% from.
%% It is required by the {@link riak_core_entropy_manager} to determin what
%% hashtree serves a partition on a given erlang node.
-spec request_hashtree_pid(_Master::atom(), Partition::non_neg_integer()) -> ok.
request_hashtree_pid(Master, Partition) ->
    ReqId = {hashtree_pid, Partition},
    riak_core_vnode_master:command({Partition, node()},
                                   {hashtree_pid, node()},
                                   {raw, ReqId, self()},
                                   Master).

%% @doc Returns the hashtree for the partiion of this service/vnode combination.
-spec hashtree_pid(_Master::atom(), Partition::non_neg_integer()) ->
                          {error, wrong_node} |
                          {ok, HashTree::pid()}.
hashtree_pid(Master, Partition) ->
    riak_core_vnode_master:sync_command({Partition, node()},
                                        {hashtree_pid, node()},
                                        Master,
                                        infinity).


%% Used by {@link riak_core_exchange_fsm} to force a vnode to update the hashtree
%% for repaired keys. Typically, repairing keys will trigger read repair that
%% will update the AAE hash in the write path. However, if the AAE tree is
%% divergent from the KV data, it is possible that AAE will try to repair keys
%% that do not have divergent KV replicas. In that case, read repair is never
%% triggered. Always rehashing keys after any attempt at repair ensures that
%% AAE does not try to repair the same non-divergent keys over and over.

-spec rehash(_Master::atom(), _Preflist::preflist(),
             _Bucket::binary(), _Key::binary()) -> ok.
rehash(Master, Preflist, Bucket, Key) ->
    riak_core_vnode_master:command(Preflist,
                                   {rehash, {Bucket, Key}},
                                   ignore,
                                   Master).

%%%===================================================================
%%% Utility functions
%%%===================================================================


%% @doc This function is a working example of how to implement hashtree
%% creation for a VNode, using this is recommended, it will need to be
%% called during the init process.
%% It also requires the calling vnode to implement a handle_info match on
%% `retry_create_hashtree` which will need to either call this function
%% again or do nothing if a valid hashtree already exists.
%% In addition to that the calling VNode will be set up to monitor the
%% created hashtree so it should listen for
%% `{'DOWN', _, _, Pid, _}` where Pid is the pid of the created hashtree
%% to recreate a new one if this should die.
-spec maybe_create_hashtrees(atom(), integer(), atom(), pid()|undefined) ->
                                    pid()|undefined.
maybe_create_hashtrees(Service, Index, VNode,  Last) ->
    maybe_create_hashtrees(riak_core_entropy_manager:enabled(), Service, Index,
                           VNode, Last).

-spec maybe_create_hashtrees(boolean(), atom(), integer(), atom(), pid()|undefined) ->
                                    pid()|undefined.
maybe_create_hashtrees(false, _Service, _Index, _VNode, Last) ->
    lager:debug("Hashtree not enabled."),
    Last;

maybe_create_hashtrees(true, Service, Index, VNode, Last) ->
    %% Only maintain a hashtree if a primary vnode
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    case riak_core_ring:vnode_type(Ring, Index) of
        primary ->
            %%{ok, ModCaps} = Mod:capabilities(ModState),
            %% Empty = case is_empty(State) of
            %%             {true, _}     -> true;
            %%             {false, _, _} -> false
            %% end,
            %%Opts = [vnode_empty || Empty],
            Opts = [],
            case riak_core_index_hashtree:start(Service, Index, self(),
                                                VNode, Opts) of
                {ok, Trees} ->
                    monitor(process, Trees),
                    Trees;
                Error ->
                    lager:info("~p/~p: unable to start index_hashtree: ~p",
                               [Service, Index, Error]),
                    erlang:send_after(1000, self(), retry_create_hashtree),
                    Last
            end;
        _ ->
            Last
    end.

%% @doc A Utility function that implements partially asyncronous updates
%% To the hashtree. It will allow up to `riak_core.anti_entropy_max_async`
%% asyncronous hashtree updates before requiering a syncronous update.
%% `riak_core.anti_entropy_max_async` if not set defaults to 90.
-spec update_hashtree(binary(), term(), binary(), pid()) -> ok.
update_hashtree(Bucket, Key, RObj, Trees) ->
    Items = [{object, {Bucket, Key}, RObj}],
    case get_hashtree_token() of
        true ->
            riak_core_index_hashtree:async_insert(Items, [], Trees),
            ok;
        false ->
            riak_core_index_hashtree:insert(Items, [], Trees),
            reset_hashtree_token(),
            ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec max_hashtree_tokens() -> pos_integer().
max_hashtree_tokens() ->
    app_helper:get_env(riak_core,
                       anti_entropy_max_async,
                       ?DEFAULT_HASHTREE_TOKENS).

get_hashtree_token() ->
    Tokens = get(hashtree_tokens),
    case Tokens of
        undefined ->
            put(hashtree_tokens, max_hashtree_tokens() - 1),
            true;
        N when N > 0 ->
            put(hashtree_tokens, Tokens - 1),
            true;
        _ ->
            false
    end.

reset_hashtree_token() ->
    put(hashtree_tokens, max_hashtree_tokens()).

%%%===================================================================
%%% Placehodlers for callback functions (to give you a idea how they look)
%%%===================================================================

%% @doc aae_repair is called when the AAE system detectes a difference
%% the simplest method to handle this is causing a read-repair if the
%% system supports it. But the actual implemetation is left to the
%% vnode to handle whatever is best.
%% -spec aae_repair(Bucket::binary(), Key::binary()) -> term().
%%aae_repair(_Bucket, _Key) ->
%%    aae_repair.


%% @doc hash_object is called by the AAE subsyste to hash a object when the
%% tree first gets generated, a object needs to be hash or is inserted.
%% To AAE system does not care for the details as long as it returns a binary
%% and is deterministic in it's outcome. (see {@link riak_core_index_hashtree})
%% -spec hash_object({Bucket::binary(), Key::binary()}, Obj::term()) -> binary().
%% hash_object(_BKey, _Obj) ->
%%    <<>>.

%% @doc Returns the vnode master for this vnode type, that is the same
%% used when registering the vnode.
%% This function is required by the {@link riak_core_index_hashtree} to
%% send rehash requests to a vnode.

%% -spec master() -> Master::atom().
%% master() ->
%%    some_other_strange_atom_form_master.
