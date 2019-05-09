# Node Worker Pool


## Background

Riak Core has a pool of workers started by riak_core_vnode, based on configuration returned from the application vnode init code:

https://github.com/basho/riak_core/blob/2.1.9/src/riak_core_vnode.erl#L223-#L243

This provides a pool of workers per-vnode, for running async tasks.  Work is redirected to the runners by returning {async, Work, From, State} from a `Mod:handle_coverage/4` or `Mod:handle_command/3` request.

https://github.com/basho/riak_core/blob/2.1.9/src/riak_core_vnode.erl#L358-L362

https://github.com/basho/riak_core/blob/2.1.9/src/riak_core_vnode.erl#L394-L398

By default in riak, the vnode `vnode_worker_pool_size` is set to 10.  On an average riak cluster there is typically 8 to 15 vnodes per physical node, meaning there is o(100) vnode_workers that can be launched concurrently from these pools.  This means that it is easy to pass enough async work to the vnode pool to overwhelm resources in the cluster.  This may still be true even if the `vnode_worker_pool_size` is reduced to 1, and at these lower numbers multiple queries could become queued behind one long-running request.


## Feature Overview

The purpose of node worker pool feature is to provide  pool of workers that are shared amongst all vnodes on the node, and allow for potentially complex or expensive queries to be re-directed to node-wide pools rather than the vnode pool.  The node-wide pools allow for tighter management of resource contention, in that the pool sizes can be smaller than the count of vnodes on the node.

For some application it may be sufficient to have two pools available from each vnode - the `vnode_worker_pool` and the `node_worker_pool`.  However, some implementations may wish to have a more complete model of queues for [differentiated services](https://en.wikipedia.org/wiki/Differentiated_services) with:

- Expedited forwarding - i.e. use the almost unbounded vnode_worker_pool;

- Assured Forwarding 1 to 4 - 4 classes of pools of a fixed size which can be allocated to different task types;

- Best Effort - a narrow pool for any activity which is particularly expensive and can support arbitrary delays during busy periods.

The application can determine both the size of each pool, and which pool can be used.  If an attempt is made to use an undefined (or uninitiated) pool then that work should fallback to the `vnode_worker_pool`.


## Implementation

The `riak_core_vnode_worker_pool` is started and shutdown by the vnode process, as that vnode process starts up and shuts down.  The node_worker_pool cannot be tied to an individual vnode in the same way, so the node_worker_pool's supervisor is started directly through the main riak_core supervision tree:

https://github.com/martinsumner/riak_core/blob/mas-2.2.5-dscpworkerpool/src/riak_core_sup.erl#L82

This does not start any pools.  The responsibility for naming and starting pools lies with the application, which can start pools via:

https://github.com/martinsumner/riak_core/blob/mas-2.2.5-dscpworkerpool/src/riak_core_node_worker_pool_sup.erl#L44-L47

The arguments to use here are as with the `vnode_worker_pool`, except for the addition of `QueueType` which will be the name of the pool, under which the pool will be registered.  There are pre-defined types to use for the anticipated queueing strategies:

https://github.com/martinsumner/riak_core/blob/mas-2.2.5-dscpworkerpool/src/riak_core_node_worker_pool.erl#L29-L33

The following code snippet from `riak_kv_app.erl` shows how pools may be started:

```

    WorkerPools =
        case app_helper:get_env(riak_kv, worker_pool_strategy, none) of
            none ->
                [];
            single ->
                NWPS = app_helper:get_env(riak_kv, node_worker_pool_size),
                [{node_worker_pool, {riak_kv_worker, NWPS, [], [], node_worker_pool}}];
            dscp ->
                AF1 = app_helper:get_env(riak_kv, af1_worker_pool_size),
                AF2 = app_helper:get_env(riak_kv, af2_worker_pool_size),
                AF3 = app_helper:get_env(riak_kv, af3_worker_pool_size),
                AF4 = app_helper:get_env(riak_kv, af4_worker_pool_size),
                BE = app_helper:get_env(riak_kv, be_worker_pool_size),
                [{dscp_worker_pool, {riak_kv_worker, AF1, [], [], af1_pool}},
                    {dscp_worker_pool, {riak_kv_worker, AF2, [], [], af2_pool}},
                    {dscp_worker_pool, {riak_kv_worker, AF3, [], [], af3_pool}},
                    {dscp_worker_pool, {riak_kv_worker, AF4, [], [], af4_pool}},
                    {dscp_worker_pool, {riak_kv_worker, BE, [], [], be_pool}}]
        end,

....

    riak_core:register(riak_kv, [

                    ....

                ]

                ++ WorkerPools),
```

The implementation of both the `riak_core_node_worker_pool` and the `riak_core_vnode_worker_pool` is now based on a common behaviour - `riak_core_worker_pool`:

https://github.com/martinsumner/riak_core/blob/mas-2.2.5-dscpworkerpool/src/riak_core_worker_pool.erl

The primary difference in implementation is that `riak_core_node_worker_pool` must trap_exit on initialisation, as there is no closing vnode process to call shutdown_pool and neatly terminate the pool (with a wait for work to finish).

A new function `queue_work/4` is added to the `riak_core_vnode` to prompt work to be queued for a node_worker_pool:

https://github.com/martinsumner/riak_core/blob/mas-2.2.5-dscpworkerpool/src/riak_core_vnode.erl#L1092-L1105

This is triggered by a response to Mod:handle_coverage/4 or Mod:handle_command/3 of:

``{PoolName, Work, From, NewModState}``

https://github.com/martinsumner/riak_core/blob/mas-2.2.5-dscpworkerpool/src/riak_core_vnode.erl#L378-L386

If there is need to call for work to be queued directly from the application (e.g. using `riak_core_vnode:queue_work/4`), then the application should be aware of the vnode pool pid() to be used by `queue_work/4` as a fallback.  To receive this information onto ModState, the application may provide a `Mod:add_vnode_pool/2` function, which if present will be called by riak_core_vnode after the pool has been initialised:

https://github.com/martinsumner/riak_core/blob/mas-2.2.5-dscpworkerpool/src/riak_core_vnode.erl#L245-L254


## Snapshots Pre-Fold

Within `riak_kv` fold functions returned from backends for performing queries which were directed towards a worker_pool (such as 2i queries), were passed to the worker without a snapshot being taken.  When the worker in the pool ran the `Fold()`, at that point a snapshot would be taken.

This model works if there is unlimited capacity in the vnode worker pools, as it is likely that the fold functions across a coverage plan will be called reasonably close together, so as to present a roughly cluster-wide point-in-time view of the query.  However, with a constrained pool, a subset of the folds in the coverage plan may be delayed behind other work.  Therefore, it is preferable for async work which intends to use node_worker_pools to have had the snapshot taken prior to the fold function being returned from the vnode backend.

This is implemented within leveled as the SnapPreFold boolean which can be passed into query requests.  When SnapPrefold is `true`, the snapshot will be taken at the point the backend receives the request, and when the fold is eventually called by the worker in the pool, it will be based on that snapshot.  So variation in worker availability across node will not impact the "consistency" of the query results - the results will be based on a loosely correlated point in time (subject to race conditions to the head of the vnode message queue).

Some work has been done to implement prefold snapping in eleveldb, mainly by splitting up the existing fold API into two stages:

https://github.com/martinsumner/riak_kv/blob/mas-2.2.5-clusteraae/src/riak_kv_eleveldb_backend.erl#L388-L430.

To implement snap_prefold in Bitcask would probably require generating file links at the point the fold is closed, but no work has been done on that backend at present.
