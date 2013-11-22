-module(riak_core_aae_vnode).

-export([behaviour_info/1]).

-export([maybe_create_hashtrees/3,
         update_hashtree/4]).

-export([aae_repair/2,
         hash_object/2,
         request_hashtree_pid/1,
         hashtree_pid/1,
         master/0,
         rehash/3]).

-xref_ignore([aae_repair/2,
              hash_object/2,
              request_hashtree_pid/1,
              hashtree_pid/1,
              master/0,
              rehash/3]).

-define(DEFAULT_HASHTREE_TOKENS, 90).

-type preflist() :: [{Index::integer(), Node :: term()}].

-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
behaviour_info(callbacks) ->
    [{aae_repair, 2},
     {hash_object, 2},
     {request_hashtree_pid, 1},
     {hashtree_pid, 1},
     {master, 0},
     {rehash, 3}];

behaviour_info(_Other) ->
    undefined.

-spec maybe_create_hashtrees(atom(), integer(), pid()|undefined) ->
                                    {ok, pid()} |
                                    ignore |
                                    retry.
maybe_create_hashtrees(Service, Index, Last) ->
    maybe_create_hashtrees(riak_core_entropy_manager:enabled(), Service, Index,
                           Last).

-spec maybe_create_hashtrees(boolean(), atom(), integer(), pid()|undefined) ->
                                    pid()|undefined.
maybe_create_hashtrees(false, _Service, _Index, Last) ->
    lager:info("sniffle_dtrace: Hashtree not enabled."),
    Last;

maybe_create_hashtrees(true, Service, Index, Last) ->
    %% Only maintain a hashtree if a primary vnode
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    lager:debug("~p/~p: creating hashtree.", [Service, Index]),
    case riak_core_ring:vnode_type(Ring, Index) of
        primary ->
            RP = riak_core_util:responsible_preflists(Index),
            case riak_core_index_hashtree:start(Service, Index, RP, self(),
                                                ?MODULE) of
                {ok, Trees} ->
                    lager:debug("~p/~p: hashtree created: ~p.",
                                [Service, Index, Trees]),
                    monitor(process, Trees),
                    {ok, Trees};
                Error ->
                    lager:info("~p/~p: unable to start index_hashtree: ~p",
                               [Service, Index, Error]),
                    erlang:send_after(1000, self(), retry_create_hashtree),
                    Last
            end;
        _ ->
            lager:debug("~p/~p: not primary", [Service, Index]),
            Last
    end.

-spec update_hashtree(binary(), binary(), binary(), pid()) -> ok.
update_hashtree(Bucket, Key, Val, Trees) ->
    case get_hashtree_token() of
        true ->
            riak_core_index_hashtree:async_insert_object({Bucket, Key}, Val,
                                                         Trees),
            ok;
        false ->
            riak_core_index_hashtree:insert_object({Bucket, Key}, Val, Trees),
            reset_hashtree_token(),
            ok
    end.

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


%% @doc aae_repair is called when the AAE system detectes a difference
%% the simplest method to handle this is causing a read-repair if the
%% system supports it. But the actuall implemetation is left to the
%% vnode to handle whatever is best.
-spec aae_repair(Bucket::binary(), Key::binary()) -> term().
aae_repair(_Bucket, _Key) ->
    ok.


%% @doc hash_object is called to hash a object, how it does this is opaque
%% to the aae system as long as it returns a binary and is deterministic in
%% it's outcome.
-spec hash_object({Bucket::binary(), Key::binary()}, Obj::term()) -> binary().
hash_object(_BKey, _Obj) ->
    <<>>.


%% @doc This is a asyncronous command that needs to send a term in the form
%% `{ok, Hashtree::pid()}` or `{error, wrong_node}` to the process it was called
%% from.
-spec request_hashtree_pid(Partition::non_neg_integer()) -> ok.
request_hashtree_pid(_Partition) ->
    ok.

%% @doc Returns the hashtree for the partiion of this service/vnode combination.
-spec hashtree_pid(Partition::non_neg_integer()) ->
                          {error, wrong_node} |
                          {ok, HashTree::pid()}.
hashtree_pid(_Partition) ->
    {error, wrong_node}.

%% @doc Returns the vnode master for this vnode type, that is the same
%% used when registering the vnode.

-spec master() -> Master::atom().
master() ->
    ok.

%% Used by {@link riak_core_exchange_fsm} to force a vnode to update the hashtree
%% for repaired keys. Typically, repairing keys will trigger read repair that
%% will update the AAE hash in the write path. However, if the AAE tree is
%% divergent from the KV data, it is possible that AAE will try to repair keys
%% that do not have divergent KV replicas. In that case, read repair is never
%% triggered. Always rehashing keys after any attempt at repair ensures that
%% AAE does not try to repair the same non-divergent keys over and over.

-spec rehash(_Preflist::preflist(), _Bucket::binary(), _Key::binary()) -> ok.
rehash(_Preflist, _Bucket, _Key) ->
    ok.
