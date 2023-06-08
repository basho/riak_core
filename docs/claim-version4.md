# Riak Core Claim Version 4

This post is about a new version of riak core's claim algorithm
[riak-core's](https://github.com/basho/riak_core).

An earlier post of [Russell](https://github.com/basho/riak_core/blob/develop/docs/claim-fixes.md) describes the present claim algorithm (version 2) in detail. That post is mainly about fixes performed to make things work with so called tail violations.

Recent enhancement of riak core in the form of [location awareness](https://github.com/basho/riak_core/blob/develop/docs/rack-awareness.md) have made it interesting to revisit and redesign the claim algorithm.

Riak is a replicated database. By default it stores three replicas of every key/value. The replication factor is called n-val meaning there are n replicas. If value has to be stored or retrieved, then the hash of that corresponds to one particular vnode in the ring. It is stored in that vnode and the n-1 next vnodes in the ring. When a vnode is unavailable (i.e. due to a failure on the node which hosts it), the node which owns the next vnode in the ring is required to start a fallback vnode as a replacement.

Therefore it is important to allocate the vnodes in the ring to the nodes in a cluster, such that n-val + 1 consecutive vnodes are not the same physical node. After all, if a physical node is down and the next vnode in the ring maps to the same physical node, then there is little redundancy.  

Taking this idea one step further, one may imagine a perfect ring with n-val replication, but two of the physical nodes are in the same "location", where location can be a rack or a data center. What if something happens that disconnects two consecutive physical nodes at once? Wouldn't it be nice if one could also take the location into consideration when placing the nodes in the ring, such that the each preflist in the ring conveniently spreads over locations?

We came up with a way to do so.

# Requirements

The solution for placing the nodes in a ring is performed by a so called claim algorithm. The user provides the nodes, the ring size (i.e. the number of vnodes) and the target n-val and from that
a mapping is returned that tries to fulfil the following:

1. the ring has exactly ring size elements.
2. all nodes occur approximately equally often: this is called _balanced_.
   (more precise for a given k, each node appears exactly k or k+1 times)
3. all n-val consecutive vnodes in the ring are in different locations.
4. all n-val consecutive anodes in the ring are on different nodes (which should follow if 3 is true).

It is expected that once a cluster is inititated the ring size and the target n-val will not normally change, hwoever the number of nodes in the cluster will be changed.

The first, second and fourth requirements cannot be relaxed. But the third one is not guaranteed to be possible at all. In fact, Bb using a SAT solver we identified
82 impossible configuration for ring size 16 and n-val, 2 or 3.  In case there is no solution possible, the algorithm is supposed to return a placement that fulfils the first two requirements and does some kind of best effort for the third.

In cases where the third requirement cannot be supported for a given n-val, it should also be possible have an n-val for locations that is less than the n-val for nodes, to create a relaxed condition which can then be met.


# Computing placements

We start of by presenting some examples to illustrate how the algorithm can be used.  The algorithm in [riak_core_claim_binring_alg](https://github.com/basho/riak_core/blob/develop/src/riak_core_claim_binring_alg.erl) is the core algorithm. You will most likely no use the API of this module directly, but by isolating the algorithm, development and testing becomes easier.

There are basically 2 API functions that matter: `solve` and `update`.  Central is the input __configuration__, presented as a list of integers, where each element in the list represents a location and the number represent the number of nodes in that location. For example, `[1,1,2]` represents 3 locations, (A, B, and C, say) such that the first and second location have 1 node and the third location has 2 nodes.

## Solve

Solving starts from a ring size, an n-val and a configuration and provides a binary representation of a ring with a placement that fits that configuration.  Consider the binary representation as an opaque type, because there is no need to inspect it. An API function "show" can be used to produce a string (ansi colour coded) from such a
binary ring (only exported for test and debug mode).

In _test_ mode or when you want to get a better understanding of the algorithm, solving the above [1,1,2] for ring size 16 and n-val 2 would be done as follows:

```
BinRing = riak_core_claim_binring_alg:solve(16, [1,1,2], 2).
io:format("~s\n", [riak_core_claim_binring_alg:show(BinRing, 2)]).
B1 C2 A1 C1 A1 C1 B1 C2 A1 C1 B1 C2 A1 C1 B1 C2 (0 violations)
```

The location names are alphabetically and the nodes are numbered. B1 is the first node in the second location.  By providing `show` also n-val it can return with `(0 violations)` given the
provided ring.

## Update

When Riak is running, then it has an existing placement of nodes and locations
in the ring. In that circumstance, one uses update to change the ring to a
new configuration.

```
Disclaimer:

We have only considered updating the configuration. It would work to update the n-val, but not the ring-size.
```

One can add a new location with new nodes, or add/remove nodes from existing locations. Again, a best-effort approach is provided.
In this best effort approach, the amount of transfers needed from one node to the other is kept into consideration.

### Adding a node to a location

For example, if we update the ring above
```
B1 C2 A1 C1 A1 C1 B1 C2 A1 C1 B1 C2 A1 C1 B1 C2
```
with one extra node in the second location:
```
BinRing1 = riak_core_claim_binring_alg:update(BinRing, [1,2,2], 2).
io:format("~s\n", [riak_core_claim_binring_alg:show(BinRing1, 2)]).
A1 B2 A1 B2 A1 C1 B1 C2 B1 C1 B1 C2 A1 C1 B2 C2 (0 violations)
```

Clearly, the new ring is of size 16 and is balanced (4 A1, 3 B1, 3 B2, 3 C1 and 3 C2).

It respects n-val 2, because no consecutive location is the same, not even when
we wrap around.

Another observation here is that 11 of the nodes have the same location in the ring. Clearly, some transfer is needed, but if we had used the `solve` approach to compute the new ring, we would have been presented with:
```
A1 B1 C1 A1 B2 C2 B1 C1 A1 B2 C2 B1 C1 A1 B2 C2
```
In which only 4 nodes have the same place in the ring. *Minimising the number of needed transfers* is the main reason for having the `update` function.

### Remove a node from a location (leave)

We can use the same update function to remove a node from a location, which in Riak terms is called a "leave". The node is removed from the ring data structure,
but the process of copying the data to create a new stable ring is a process that takes time, only after which the node is actually removed.  

Assume we want to remove the node we have just added above. In other words, we return to the initial configuration `[1, 1, 2]`:
```
BinRing2 = riak_core_claim_binring_alg:update(BinRing1, [1,1,2], 2).
io:format("~s\n", [riak_core_claim_binring_alg:show(BinRing2, 2)]).
B1 C2 A1 C1 A1 C1 B1 C2 B1 C1 B1 C2 A1 C1 A1 C2 (0 violations)
```
This does not give the same ring as the original placement, but close.  In order to minimise transfers, 12 nodes keep their position.

### Leave a location

In theory we can also add and leave nodes in one go. This is probably not something one would like to do in operation, but the algorithm allows it.

For example if we update the ring above by moving one of the single nodes to the other location with a single node:
```
NewBinRing = riak_core_claim_binring_alg:update(BinRing, [2,2], 2).
io:format("~s\n", [riak_core_claim_binring_alg:show(NewBinRing, 2)]).
B1 A2 B2 A1 B2 A1 B2 A2 B1 A1 B2 A2 B1 A1 B1 A2 (0 violations)
```
But that result is confusing, because now we have location A and B, but the intention was to keep location C and move a node from B to A (or alternatively from A to B).

We can patch the confusion in the [Embedding layer using this algorithm](#embedding-the-algorithm-in-riak-core) where we translate real node names and translations back and forth to these configurations. But that layer becomes easier if we actually state our intentions clearly and have a layer with zero nodes in the provided configuration:
```
NewBinRing = riak_core_claim_binring_alg:update(BinRing, [2,0,2], 2).
io:format("~s\n", [riak_core_claim_binring_alg:show(NewBinRing, 2)]).
A2 C2 A1 C1 A1 C1 A2 C2 A1 C1 A2 C2 A1 C1 A2 C2 (0 violations)
```
If we compare that to the original placement:
```
B1 C2 A1 C1 A1 C1 B1 C2 A1 C1 B1 C2 A1 C1 B1 C2
```
we see that the nodes in location C have not changed, but that B1 is replaced by A2.

# Embedding the algorithm in riak core

In Riak the claim algorithm is configurable via `wants_claim_fun` and `choose_claim_fun`. In order to run with this new algorithm, one should configure `choose_claim_fun` to `choose_claim_v4`. We do not use the wants function, but `riak_core_membership_claim` requires to have one, so use the default for version 2.

The main entry for claim is `riak_core_membership_claim:claim/1`. This in turn calls `riak_code_claim_swapping:choose_claim_v4/3`. This is just a wrapper to come to the real API, `riak_code_claim_swapping:claim/2` which takes the present ring and the n-val as input.

Riak always starts from an existing ring to compute the placement (in case of a new node, the initial consists of that same node at each position). Therefore, we start with an update... if however `update` cannot find a solution without violations, we fall back to `solve`.

### Mapping node/location names

The main work in `riak_code_claim_swapping` is to map the real node names and the real locations to the configurations we provide the algorithm.

A typical ring will contain node names as atoms and location names associated to those atoms. For example, one could have a ring of size 16 like this:
```
n2 n4 n1 n3 n1 n3 n2 n4 n1 n3 n2 n4 n1 n3 n2 n4
```
with the mapping `[{n1, loc1}, {n2, loc2}, {n3, loc3}, {n4, loc4}]`. We use this to create a list of tuples with location index and node index, something like:
```
[{2, 1}, {3, 2}, {1,1}, {3,1}, {1,1}, {3,1}, {2,1}, {3,2},
 {1, 1}, {3, 1}, {2,1}, {3,2}, {1,1}, {3,1}, {2,1}, {3,2}]
```
where the second integer is the index of the node in that location. This corresponds to:
```
B1 C2 A1 C1 A1 C1 B1 C2 A1 C1 B1 C2 A1 C1 B1 C2
```
With the function `riak_core_claim_binring_alg:from_list` we generate the ring in the binary form that the algorithm needs for the update function.

The update function now also wants the new location, then computes, as described above a new ring, which we translate back into a list of tuples via `riak_core_claim_binring_alg:to_list`.

The challenge is to make sure the right indices map to the right node names!  Because, what if we want to remove, say node `n3`. The configuration that we compute from the riak ring object, in which the action `leave n3` is present, is clearly `[1, 1, 1]`. When we run update, the computed ring is:
```
R1 = riak_core_claim_binring_alg:update(BinRing, [1,1,1], 2).
io:format("~s\n", [riak_core_claim_binring_alg:show(R1, 2)]).
C1 B1 A1 B1 A1 C1 A1 C1 A1 C1 B1 A1 B1 C1 B1 A1 (0 violations)
```
But it is easy to be mislead that C1 here is in fact `n4` and not `n3` as it was before. Our solution here is to compute the mapping function together with the binary ring
in such a way that leaving nodes have a higher index than nodes that do not leave. So, instead we use the mapping `[{loc1, [n1]}, {loc2, [n2]}, {loc3, [n4,n3]}]` to compute
the indexes for the binary ring, which then swaps `{3,1}` and `{3,2}`) and maps `n4` to C1:
```
[{2, 1}, {3, 1}, {1,1}, {3,2}, {1,1}, {3,2}, {2,1}, {3,1},
 {1, 1}, {3, 2}, {2,1}, {3,1}, {1,1}, {3,2}, {2,1}, {3,1}]
```
Using this ring in update, gives the resulting ring:
```
A1 C1 A1 B1 C1 A1 B1 C1 A1 C1 B1 A1 B1 A1 B1 C1 (0 violations)
```
which easily translates back to:
```
n1 n4 n1 n2 n4 n1 n2 n4 n1 n4 n2 n1 n2 n1 n2 n4
```
where `n3` is indeed removed. (This typical example unfortunately requires a lot of transfers).


# Legacy: no locations

The claim algorithms version 1 to 3 that have been used in Riak before, do not consider locations. There the goal is to just consider the n-val for nodes. The new algorithm also
supports that, such that if you have no locations, you can use this newer algorithm. In fact, you can just configure to use this new claim algorithm and run as usual. The module `riak_code_claim_swapping` checks whether you have defined locations and if not, it puts all the nodes in one location.

Effectively, the `solve` and `update` function are called with `{NVal, 1}` instead of `NVal` as argument, where the second element of the tuple is the location n-val.
```
BinRing = riak_core_claim_binring_alg:solve(16, [4], {2,1}).
io:format("~s\n", [riak_core_claim_binring_alg:show(BinRing, {2, 1})]).
A3 A1 A2 A4 A1 A2 A3 A4 A1 A2 A3 A4 A1 A2 A3 A4 (0 violations)
```

# Different target n_vals - nodes and locations

The default setting is to have a target_n_val of 4 for nodes, and a target_n_val of 3 for locations.  There will be some situations though, where a more optimal configuration would have been found by increasing the target_n_vals, and in particular matching the location and node n_val.  With higher n_vals there is a higher chance of an unsolveable configuration, and when the `riak admin cluster plan` function is called the operator would be notified of these violations.  In that case it will be possible to clear the plan, change these settings on the claimaint node (indicated with a `(C)` in the plan), and re-plan with alternative settings - to see if the outcome is preferable, perhaps as it reduces the number of required transfers.

The plan function under claim_v4 will always return the same answer with the same configuration.  So reverting any changes and re-planning will return to the original plan.

# Solvable configurations

As we see above, the algorithm may fail to satisfy the provided n-val. In fact, there are many configurations that are simply impossible to solve. Trivially when the number of locations is smaller than the n-val, etc. But excluding those trivial cases, we played with a SAT solver to find 82 *impossible configurations* with ring size 16 and n-val 2 or 3.

This resulted in some necessary requirements to be able to find a solution at all, which we use in [QuickCheck tests](../test/riak_core_claim_eqc.erl#L261) to avoid testing the wrong things.

Here we present some rules of thumb for good start configurations and typically more successful update configurations.

In general, a larger ring size is easier to solve than a small ring size. We simply have more play room to swap nodes to get to a solution. But note that it is more computation intensive when the ring size grows.

Distributing the nodes evenly over the location makes it more likely to find a solution. For a realistic example with ring size 512, n-val 4 and 4 locations which 3 nodes each,
we easily find a solution, similar when we put 2 nodes each in the 4 locations. But the configuration `[3,3,3,4]` has no solution. In that case it actually works to put the extra node in a different location.

In general, adding an extra location and having more locations than n-val makes it easier to find a solution. With ring size 512 and n-val 3 a solution for `[2, 2, 2, 2]` is quickly found, but the best-effort solution for `[3, 3, 3]` has 4 violations.  So, even if there are 9 nodes in the latter configuration and only 8 in the earlier, it is harder to find a placement.


# Conclusion

The new algorithm for node placement in a ring handles the case where location is an additional property to consider in a Riak installation. It is backward compatible with the situation in which no locations are considered at all.

The algorithm handles both addition of new nodes, in same or new locations, as well as nodes leaving the ring.

The algorithm has an inherent high complexity and can take a long time to come up with a solution. Since the algorithm is only used when planning a new configuration for a Riak installation, we find it acceptable that one needs to wait upto one or two minutes for a solution. In fact, one only needs to wait long when it is hard to find a solution. We provided some rules of thumb to provide configurations that are relatively easy to solve.

This algorithm will be released with the next version of Riak we create for NHS-Digital.
