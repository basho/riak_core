# AAE


## Quickstart:

* Add the Entropy Manager to your apps supervisor tree.
* Add the `riak_core_aae_vnode` behavior to the vnode in question.
* Implement the functions required by the behavior.
* Implement the callback matches detailed in the VNode section.
* Makre sure that `init/1` creates a hashtree, see `maybe_create_hashtrees/2` as example how to do that.
* Make all data changing calls in your VNode also call `update_hashtree/3` (or something similar) or `riak_core_index_hashtree:delete`.
* Add the required config parameters.


## Entropy Manger (riak_core_entropy_manager)
One is started per vnode service, as part of the of the main apps supervisor. It needs the service name and the vnode module passed as arguments.

Here an example how to start it for the `snarl_user` service that is implemented in the `snarl_user_vnode`.

```erlang
    EntropyManagerUser =
        {snarl_user_entropy_manager,
         {riak_core_entropy_manager, start_link,
          [snarl_user, snarl_user_vnode]},
         permanent, 30000, worker, [riak_core_entropy_manager]},
```

The manager is responsible of coordinating the AAE tree build and exchange with other nodes.

## Entropy Info (riak_core_entropy_info)
A helper module to get information about the entropy system, nothing needs touching here.

## Exchange FSM (riak_core_exchange_fsm)
FSM to do the actual work of exchange.

## Index Hash Tree (riak_core_index_hashtree)
One of those is started for each combination of service/partition, it handles the hash tree and takes care of keeping track of the changes.

The VNode will need to take care of creating the hash tree for it's partition and also send updates on writes and deletes to keep the data in check. The hash tree will also fold over the entries for the first bootup.

## VNode (riak_core_aae_vnode)
`riak_core_aae_vnode` implements a behavior for all the functions that need to be implemented for this.

In addition to the functions described there the vnode needs to respond to the following calls to handle_info and command:

```erlang
% ...
handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    lager:debug("Fold on ~p", [State#state.partition]),
    Acc = fifo_db:fold(State#state.db, <<"user">>,
                       fun(K, V, O) ->
                               Fun({<<"user">>, K}, V, O)
                       end, Acc0),
    {reply, Acc, State};

% ...
handle_info(retry_create_hashtree, State=#state{hashtrees=undefined}) ->
    State2 = maybe_create_hashtrees(State),
    case State2#state.hashtrees of
        undefined ->
            ok;
        _ ->
            lager:info("riak_core/~p: successfully started index_hashtree on retry",
                       [State#state.partition])
    end,
    {ok, State2};
handle_info(retry_create_hashtree, State) ->
    {ok, State};
handle_info({'DOWN', _, _, Pid, _}, State=#state{hashtrees=Pid}) ->
    State2 = State#state{hashtrees=undefined},
    State3 = maybe_create_hashtrees(State2),
    {ok, State3};
handle_info({'DOWN', _, _, _, _}, State) ->
    {ok, State};
handle_info(_, State) ->
    {ok, State}.
```

An example implementation (taken from `riak_kv`) for internal utility functions used to create and maintain the hashtree:

```erlang
-define(DEFAULT_HASHTREE_TOKENS, 90).

-spec maybe_create_hashtrees(state()) -> state().
maybe_create_hashtrees(State) ->
    maybe_create_hashtrees(riak_core_entropy_manager:enabled(), State).

-spec maybe_create_hashtrees(boolean(), state()) -> state().
maybe_create_hashtrees(false, State) ->
    lager:info("snarl_user: Hashtree not enabled."),
    State;

maybe_create_hashtrees(true, State=#state{partition=Index}) ->
    %% Only maintain a hashtree if a primary vnode
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    lager:debug("snarl_user/~p: creating hashtree.", [Index]),
    case riak_core_ring:vnode_type(Ring, Index) of
        primary ->
            RP = riak_core_util:responsible_preflists(Index),
            case riak_core_index_hashtree:start(snarl_user, Index, RP, self(),
                                                ?MODULE) of
                {ok, Trees} ->
                    lager:debug("snarl_user/~p: hashtree created: ~p.", [Index, Trees]),
                    monitor(process, Trees),
                    State#state{hashtrees=Trees};
                Error ->
                    lager:info("snarl_user/~p: unable to start index_hashtree: ~p",
                               [Index, Error]),
                    erlang:send_after(1000, self(), retry_create_hashtree),
                    State#state{hashtrees=undefined}
            end;
        _ ->
            lager:debug("snarl_user/~p: not primary", [Index]),
            State
    end.

-spec update_hashtree(binary(), binary(), state()) -> ok.
update_hashtree(Key, Val, #state{hashtrees=Trees}) ->
    case get_hashtree_token() of
        true ->
            riak_core_index_hashtree:async_insert_object({<<"user">>, Key}, Val, Trees),
            ok;
        false ->
            riak_core_index_hashtree:insert_object({<<"user">>, Key}, Val, Trees),
            put(hashtree_tokens, max_hashtree_tokens()),
            ok
    end.

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

do_put(Key, Obj, State) ->
    fifo_db:put(State#state.db, <<"user">>, Key, Obj),
    update_hashtree(Key, term_to_binary(Obj), State).


-spec max_hashtree_tokens() -> pos_integer().
max_hashtree_tokens() ->
    app_helper:get_env(riak_core,
                       anti_entropy_max_async,
                       ?DEFAULT_HASHTREE_TOKENS).
```
