# AAE


## Quickstart:

* Add the Entropy Manager to your apps supervisor tree.
* Add the `riak_core_aae_vnode` behavior to the vnode in question. Implement the functions required by the behavior.
* Implement the callback matches detailed in the VNode section.
* Make sure that `init/1` creates a hashtree, see `maybe_create_hashtrees/2` as example how to do that.
* Make all data changing calls in your VNode also call `update_hashtree/3` (or something similar) or `riak_core_index_hashtree:delete`.
* Add the required config parameters.


## Entropy Manger (`riak_core_entropy_manager`)
One is started per vnode service, as part of the of the main apps supervisor. It needs the service name and the vnode module passed as arguments.

Here an example how to start it for the `snarl_user` service that is implemented in the `snarl_user_vnode`.

```erlang
    EntropyManagerUser = riak_core_entropy_manager:supervisor_spec(snarl_user, snarl_user_vnode),
```

The manager is responsible of coordinating the AAE tree build and exchange with other nodes.

## Entropy Info (`riak_core_entropy_info`)
A helper module to get information about the entropy system, nothing needs touching here.

## Exchange FSM (`riak_core_exchange_fsm`)
FSM to do the actual work of exchange.

## Index Hash Tree (`riak_core_index_hashtree`)
One of those is started for each combination of service/partition, it handles the hash tree and takes care of keeping track of the changes.

The VNode will need to take care of creating the hash tree for it's partition and also send updates on writes and deletes to keep the data in check. The hash tree will also fold over the entries for the first bootup.

## VNode (`riak_core_aae_vnode`)
`riak_core_aae_vnode` implements a behavior for all the functions that need to be implemented for this.

In addition to the functions described there the vnode needs to respond to the following calls to handle_info and command:

```erlang
% ...

%% Create a hahstree on init
init([Idx]) ->
    HT = riak_core_aae_vnode:maybe_create_hashtrees(?SERVICE, Idx, undefined),
    {ok, #state{dict = orddict:new(), hashtrees = HT, index = Idx}}.
% ...
%% This is used for rehashing the tree
handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    lager:debug("Fold on ~p", [State#state.partition]),
    Acc = orddict:fold(State#state.dict, fun(K, V, A) ->
    	 Fun({<<"bucket">>, K}, V, A)
      end, Acc0),
    {reply, Acc, State};

% ...

%% retry_create_hashtree is required by `riak_core_aae_vnode:maybe_create_hashtrees`
%% if the creation failed
handle_info(retry_create_hashtree, State=#state{hashtrees=undefined, index=Idx}) ->
    lager:debug("~p/~p retrying to create a hash tree.", [?SERVICE, Idx]),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(?SERVICE, Idx, undefined),
    {ok, State#state{hashtrees = HT}};
handle_info(retry_create_hashtree, State) ->
    {ok, State};
%% When the hashtree goes down we want to create a new one.
handle_info({'DOWN', _, _, Pid, _}, State=#state{hashtrees=Pid, index=Idx}) ->
    lager:debug("~p/~p hashtree ~p went down.", [?SERVICE, Idx, Pid]),
    erlang:send_after(1000, self(), retry_create_hashtree),
    {ok, State#state{hashtrees = undefined}};
handle_info({'DOWN', _, _, _, _}, State) ->
    {ok, State};
handle_info(_, State) ->
    {ok, State}.

%% Example write operation that uses `riak_core_aae_vnode:update_hashtree` to update
%% the hashtree.
do_put(Key, Obj, State) ->
    Dict1 = orddict:store(State#state.dict, Key, Obj),
    riak_core_aae_vnode:update_hashtree(<<"bucket">>, Key, term_to_binary(Obj),
                                        State#state.hashtrees),
    State#state{dict = Dict1}.

%% Example delete operation that uses `riak_core_index_hashtree:delete` to update
%% the hashtree.
do_delete(Key, State) ->
    Dict1 = orddict:erase(State#state.dict, Key),
    riak_core_index_hashtree:delete({<<"bucket">>, Key}, State#state.hashtrees),
    State#state{dict = Dict1}.

```

## Application (`*_app.erl`)

A ets table has to be created for aae to work adding the following line to the `*_app.erl` file (or anywhere really):

```erlang
    riak_core_entropy_info:create_table()
```