# The Ring and Claim

## Summary

It is common when presenting Riak to throw in a picture of the ring and assume everyone understands what this means.

![](ibm_ring.png "IBM Presents the Ring")

However, the ring, and in particular the process by which nodes claim from the ring isn't necessarily obvious to everyone.  Perhaps sometimes even the speaker doesn't really understand what it all means.  

This is an attempt to explain the problems associated with claiming from a ring, examine the current implementations of claim within Riak and some issues with it, and then discuss areas for improvement.

## Overview of the Ring - Terminology

Some terminology which will help understand the basics of the ring in Riak:

- [Node](http://docs.basho.com/riak/kv/2.2.3/learn/glossary/#node) - a physical computer which will hold a subset of the data, and manage a subset of the load within Riak.  Normally, there should be [at least five nodes in a Riak cluster](http://basho.com/posts/technical/why-your-riak-cluster-should-have-at-least-five-nodes/).

- [Vnode](http://docs.basho.com/riak/kv/2.2.3/learn/glossary/#vnode) - the unit of concurrency in Riak, in the case of Riak KV an individual key-value store being orchestrated by Riak core.

- Ring-Size - the number of [partitions](http://docs.basho.com/riak/kv/2.2.3/learn/glossary/#partition) of the hashed key-space, which will then be mapped to vnodes.

- [Ring](http://docs.basho.com/riak/kv/2.2.3/learn/glossary/#ring) - an object which describes the partitioning of the key-space, the mapping between partitions (or vnodes) and physical nodes, and any planned or active changes to that configuration.

- Preflist - the set of primary vnodes that an object should be stored in if the key hashes to a given partition (normally that partition and the next two in the ring).

- Fallback - a vnode that will be used to store objects should a vnode in the preflist not be available.

## Overview of The Ring - the Basics

At design time of a Riak cluster, it is necessary to set the [size of the ring](http://docs.basho.com/riak/kv/2.2.3/configuring/basic/#ring-size) to a power of two.  Getting this right at the start is important, as although there exists the potential to resize a ring post go-live, it is difficult to do in a risk-free way under production load.  Currently, ring re-sizing is not a solved problem.  Guidance on how to size a ring in advance is [vague](http://docs.basho.com/riak/kv/2.2.3/setup/planning/cluster-capacity/#ring-size-number-of-partitions).  Currently, correctly setting the size of a ring to avoid resizing is not a solved problem.

The size of the ring simply defines the number of separate (and equal) partitions of the hash space there will be.  Each Bucket/Key combination will be hashed before storage, and hashed before fetching, and the outcome of that consistent hashing algorithm will determine the partition to which that Bucket/Key pair belongs.  Each partition has an ID, which is the floor hash value for that partition: to belong to a partition a Bucket/Key pair must hash to a value which is greater than the floor, and less than or equal to the floor of the next partition.  The "first" partition is ID 0, but this is only the first in the sense it has the lowest Partition ID - it has no special role in the ring.

Within a Riak cluster there are individual databases known as vnodes, where each vnode is associated with a single Partition ID.  The more partitions, the easier it is to spread load smoothly throughout larger and larger clusters.  The fewer partitions, the lower the overhead of running all the vnodes necessary to support each partition.  As the node count increases, so does the optimal number of vnodes.

Every bucket within Riak has an n-val which indicates how many vnodes each value should normally be stored in, and retrieved from.  When a key is to be stored or retrieved the Bucket/Key is hashed and then the operation will be performed against the vnode  with the Partition ID which the Key hashes to, plus the next n-1 vnodes in numerical order of Partition ID.  If one of these vnodes is known to be unavailable at commencement of the operation (Riak will gossip the status of vnodes around the cluster), then the next vnode in numerical order will also be used as a fallback (i.e. the node n partitions along from the one the key hashed to).  If more vnodes expected to be used are unavailable, more fallbacks will be used.  

If the database operation doesn't know the keys it is interested in, for example in a secondary index query, it must ask every nth vnode - where n is the n-val for the bucket related to that query.  If there is a n-val of three, that means there are three different coverage plans in a healthy cluster that could be used for the query, with the offset being chosen at random to determine which plan will be used.  To be explicit, there will be one coverage plan based on the 1st partition, the 4th partition, the 7th partition etc - and two more coverage plans where the partitions used are right shifted by 1 and 2 partitions respectively.

If a vnode is unavailable for a coverage query, using a fallback node is not a desirable answer.  Coverage queries are run with r=1 (only one vnode is checked for every part of the keyspace), so fallback nodes are unsafe as they only contain the data since the cluster change event occurred for some of those partitions.  So the coverage planner must assess the partitions which are not covered by the plan, and find other vnodes that can fill the role of the missing vnodes.  A partition filter will be passed along with the accumulating function for vnodes where querying the whole vnode would otherwise return duplicate data.

## Overview of the Ring - the Claiming problem

If we have a ring - which is really a list of partitions of a key space, which are mapped in turn to vnodes (individual key-value stores): the key question is how do we distribute these vnodes around the physical cluster?  

Riak has no specifically assigned roles (i.e. there is no master controlling node), so in theory distribution of vnodes can be managed by a process of nodes claiming the number of vnodes they want, and a consensus being reached within the cluster what the up-to-date set of claims are.  Different nodes can propose changes to the ring, and detection of conflicts between different proposals is managed using vector clocks.  In practice, such changes to claim are made through administrative action using cluster plans, and the node on which the cluster plan is made will make claims on the ring on behalf of the nodes involved in the change.

There are some properties of this claiming process which are required, and are listed here in decreasing order of obviousness:

- vnodes within n partitions of each other should not be mapped to the same physical nodes (so that writes in a healthy cluster are always safely distributed across different nodes);

- vnodes at the tail of the partition list, should not be mapped to the same physical nodes as vnodes at the front of the partition list (i.e. the above property must hold as the ring wraps around to the start);

- there should be an even distribution of vnodes across the physical nodes;

- transitions from one cluster state to another should minimise the number of transfers required;

- all coverage plans should be evenly spread across the physical nodes (so that expensive queries don't cause a subset of slow nodes);

- the above properties should be upheld if nodes are added one-by-one, or joined in bulk;

- vnodes should ideally be spaced as a far as possible, not just minimally;

- consideration needs to be given to multiplication of copysets in large clusters, and the potential for loss of data access on recovery from mass power failure.

Each of these properties will be examined in turn, and the explanation of those properties will hint at both how Riak may fail to meet some of these properties, and how that failure could be resolved.  The specific problems in Riak and the proposed solution will be explained in a later section.

### Proximity of vnodes

The mapping of vnodes to nodes needs to be spaced at least n-val partitions apart, as otherwise the writing of a bucket/key onto its primary partitions might not be written onto separate physical nodes.  

However, how far apart should the occurrence of a node in the partition list be spaced?  Clearly it cannot be more than m nodes apart, where m is the number of nodes.  It needs to be at least n-val apart, but n-val is set per bucket in Riak, and so there is no up-front agreement on what that n-val might be.  Also if nodes were spaced apart on the partition list just by n-val, when a single node failed, for some partitions an object may be written to a fallback vnode that is on the same physical node as a primary - and w=2 will not guarantee physical diversity under single node-failure scenarios.

To resolve this, Riak has a configuration parameter of [`target_n_val`](http://docs.basho.com/riak/kv/2.2.3/configuring/reference/) - which represents the target minimum spacing on the partition list for any given physical node.  This by default is set to 4, which allows for physical diversity (subject to sufficient nodes being joined) on n-vals up to 4, and also diversity under single failures for n-vals of up to 3.

Trivially we can achieve this in a partition list by simply allocating vnodes to nodes in sequence:

``| n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 ....``

So physical node 1 (n1) is allocated the "first" partition in the ring, n2 the second etc.

### Proximity of vnodes as the partition list wraps

In some cases, the number of nodes will be divisible by the ring_size, and the condition above will be sufficient to ensure diversity.  However, commonly the number of nodes will not divide evenly within the ring-size: for example if we have a ring-size of 32 and 5 physical nodes - the following distribution will be produced:

``| n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 |``

The list of allocations wraps around.  So now a bucket/key pair mapping to the 31st partition in the list will be stored on the 31st, 32nd and 1st partitions, which maps to | n1 | n2 | n1 | and so will not guarantee physical diversity.  Riak could respond to a pw=2 write, but in fact the write had only gone to one physical node.  

This wrap-around problem exists only if the remainder, when dividing the ring_size by the number of nodes, is smaller than the target_n_val.  This problem could be resolved (for non-trivial ring-sizes) by adding further nodes in sequence.  If we assume

`k = target_n_val - (ring_size mod node_count)`

then we can add k nodes in sequence to the tail of the sequence, and then remove each one of the added nodes from one of the 1st to kth sequences at the front of the list.  For the above example this would be:

| n1 | n2 | n3 ... ~~| n4 |~~ ... n5 | n1 | n2 ... ~~| n3 |~~ ... n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | ... ++ ... |n3 | n4 |

Which would give:

``| n1 | n2 | n3 | n5 | n1 | n2 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 |``

### Even distribution of vnodes

A Riak cluster will generally run at the pace of the slowest vnode.  The vnodes should be distributed as to minimise the maximum number of vnodes on any one given node, and ideally have an entirely even distribution of vnodes. So if there are 5 nodes, and a ring-size of 32:

- 3 nodes with 6 vnodes and 2 nodes with 7 vnodes is ideal;

- 4 nodes with 7 vnodes and 1 node with 4 vnodes is sub-optimal - isolating one node with less load will create 'one fast node' that should not have side effects beyond inefficiency;

- 4 nodes with 6 vnodes and 1 node with 8 vnodes is worse still - as it isolates one node with more load than the others, creating 'one slow node', a scenario with known side effects.

An uneven distribution of vnodes will be inefficient.  Isolating one node with more load will have side effects beyond inefficiency, especially where this leads to the vnodes on that node frequently going into an overload state.  It may be unavoidable for one node to have one more partition that all other nodes if the most even balance is desired (for example with a ring-size of 256 and 5 nodes).

### Minimising transfer counts

A simple sequential plan with handling of tail wrapping, as described above may resolve basic issues of distribution of vnodes and spacing between vnodes on a node.  However, it would require significant work on transition.  For example, the equivalent six node cluster would be distributed as such:

``| n1 | n2 | n3 | n5 | n6 | n1 | n2 | n4 | n5 | n6 | n1 | n2 | n3 | n4 | n5 | n6 | n1 | n2 | n3 | n4 | n5 | n6 | n1 | n2 | n3 | n4 | n5 | n6 | n1 | n2 | n3 | n4 |``

However transitioning from a 5-node cluster to a 6-node cluster would require at least <b>24</b> vnode transfers. As the new node only actually requires 5 vnodes to fulfill a balanced role within the cluster - this is a significant volume of excess work.  This problem is exacerbated by the need to schedule transfers so that both the balance of vnodes and the spacing of vnodes is not compromised during the transfer, and so the actual number of transfers required to safely transition may be much higher.

If only the minimum vnodes had been transferred, managing the transfer process is trivial i.e.

to transition from

``| n1 | n2 | n3 | n5 | n1 | n2 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 |``

to

``| n6 | n2 | n3 | n5 | n1 | n6 | n4 | n5 | n1 | n2 | n6 | n4 | n5 | n1 | n2 | n3 | n6 | n5 | n1 | n2 | n3 | n4 | n6 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 |``

would leave each node having either 5 or 6 vnodes, leave the spacing constraints in space, would require only <b>5</b> transfers without any specific scheduling of the transfers required to maintain safety guarantees.  

### Balanced coverage plans

In a six node cluster, with an even distribution of partitions of sequence, and tail wrap-around resolved - we have a ring arrangement like this:

``| n1 | n2 | n3 | n5 | n6 | n1 | n2 | n4 | n5 | n6 | n1 | n2 | n3 | n4 | n5 | n6 | n1 | n2 | n3 | n4 | n5 | n6 | n1 | n2 | n3 | n4 | n5 | n6 | n1 | n2 | n3 | n4 |``

The coverage plans will hit nodes as such:

Plan 1
- node 1 - 1
- node 2 - 1
- node 3 - 4
- node 4 - 0
- node 5 - 1
- node 6 - 4

Plan 2
- node 1 - 4
- node 2 - 1
- node 3 - 0
- node 4 - 5
- node 5 - 0
- node 6 - 1

Plan 3
- node 1 - 1
- node 2 - 4
- node 3 - 2
- node 4 - 0
- node 5 - 4
- node 6 - 0

The unbalanced coverage plans will lead to unbalanced resource pressures in the cluster, and in cases of long-running queries issues with multiple 'slow' nodes.

If when sequencing we divide the nodes into blocks of three, with a remainder, and then we allow the first node in each sequence to roll up and down the three nodes by one place at a time (except for the remainder at the tail of the sequence), we would have an alternative distribution:

``| n1 | n2 | n3 | n5 | n6 | n2 | n1 | n5 | n4 | n6 | n2 | n3 | n1 | n5 | n6 | n4 | n2 | n1 | n3 | n5 | n4 | n6 | n1 | n2 | n3 | n4 | n5 | n6 | n1 | n2 | n3 | n4 |``

The coverage plans will hit nodes as such:

Plan 1
- node 1 - 3
- node 2 - 0
- node 3 - 3
- node 4 - 1
- node 5 - 1
- node 6 - 3

Plan 2
- node 1 - 2
- node 2 - 3
- node 3 - 0
- node 4 - 2
- node 5 - 3
- node 6 - 1

Plan 3
- node 1 - 1
- node 2 - 3
- node 3 - 3
- node 4 - 2
- node 5 - 1
- node 6 - 1

This will provide for a more even distribution of coverage plans across nodes.  However, this is really only a problem where the number of nodes is a factor of three.  Even in the multiple of three case, although the coverage plans are individually unbalanced, they are collectively balanced; across multiple queries the load is still split across the cluster.  

This should not be a significant issue when scheduling lots of small coverage queries, but may have unpredictable impacts if scheduling larger coverage queries, such as when key-listing.  However, scheduling larger coverage queries on production systems breaches standard best practice guidance in Riak.

### Joining through cluster plans

Within Riak the joining, leaving and transferring of nodes may happen one node at a time, or may be requested in bulk.  So any algorithm must be able to cope with both scenarios.  

The bulk join could always be treated as a series of one-at-a-time changes (although note that in Riak this isn't strictly the case).  

It may also be considered that clusters generally have two states: with less nodes than target_n_val; with a node count greater than or equal to target_n_val.  Also transitioning between these states is a special case - so adding a node or set of nodes to prompt the transition needs to be optimised to create an initially balanced ring, and node additions beyond that transition should be optimised so as not to spoil the balance of the ring.

### Further spacing

``| n1 | n2 | n3 | n4 | n1 | n2 | n3 | n4 | n1 | n2 | n3 | n4 | n1 | n2 | n3 | n4 | n5 | n6 | n7 | n8 | n5 | n6 | n7 | n8 | n5 | n6 | n7 | n8 | n5 | n6 | n7 | n8 |``

In this scenario if two nodes were to fail (node 1 and node 2), the fallbacks elected would be split across the nodes so that the proportional activity on the remaining nodes will be:

- node 3 - 27.1%
- node 4 - 20.8%
- node 5 - 14.6%
- node 6 - 12.5%
- node 7 - 12.5%
- node 8 - 12.5%

With this arrangement, a clearly sub-optimal scenario is created on two node failures - as multiple preflists lost node diversity (i.e. they had fallbacks elected that were not on physically diverse nodes to surviving primaries), and there is a significant bias of load towards one particular node.

Ideally the spacing between partitions for a node should not just be `target_n_val` but further if possible.

### Copysets and the data loss problem

Failure scenarios in large clusters have potential weak-spots with random distributions of data across nodes - as articulated in the [Copysets paper by Asaf Cidon et al](http://web.stanford.edu/~cidon/materials/CR.pdf).  The paper sites evidence that of the order of 1% of nodes may not return after a sudden power loss event, and considers that if all three nodes used for storage are all concurrently impacted this will constitute a temporary data-loss event which may require expensive intervention to resolve.

Every cluster has a number of copysets, the copysets being unique sets of three nodes that hold any individual item of data.  If each data block is distributed across three separate nodes selected at random, then as the number of nodes grow, the number of copysets grows exponentially.  This means that at just 300 nodes, such a data loss event following a mass power outage becomes highly probable - at least one copyset is almost certainly impacted.

When using a ring structure, this is not however such a significant issue.  Assuming a need for even distribution of data, the number of copysets for any ring has a lower-bound of the number of nodes.  The lower-bound is achieved if the partitions are allocated to nodes sequentially.  So an allocation such as this

``| n1 | n2 | n3 | n4 | n5 | n6 | n7 | n8 | n1 | n2 | n3 | n4 | n5 | n6 | n7 | n8 | n1 | n2 | n3 | n4 | n5 | n6 | n7 | n8 | n1 | n2 | n3 | n4 | n5 | n6 | n7 | n8 |``

Would have only eight copysets: {n1, n2, n3}, {n2, n3, n4}, {n3, n4, n5}, {n6, n7, n8}, {n7, n8, n1} and {n8, n1, n2}.

However there is also an upper-bound, in that within the constraints of the ring the number of copysets cannot exceed the ring-size.  Standard cluster capacity planning guidance is for the ring-size to be approximately 10 x the size of the number of nodes: so the growth of the upper-bound progresses linearly with the number of nodes, and is generally only a single order of magnitude greater than the lower-bound.

So, using a ring, if there are 300 nodes, the lower-bound of copysets is 300, but the upper-bound regardless of the distribution algorithm would normally be 4096 (that being the standard ring-size for a cluster of that physical size).  So the probability of a data loss event on mass power outage is still < 1%, even if distribution was sufficiently random to hit the upper-bound.  This is an order of magnitude higher than the probability with a perfectly sequential allocation of partitions of nodes, but that difference is not significant until cluster sizes become much larger.

For much larger cluster sizes, some consideration may be required to limiting randomisation to stay within the recommend optimal number of copysets.  However, much larger cluster sizes are not currently a common use case for Riak.

## Riak Claim v2 and Upholding Claim properties

There are two non-deprecated claim algorithms in use in Riak today, the default (version 2), and a proposed alternative (version 3).  Both algorithms are "proven" through some property-based tests, which check for randomly chosen scenarios that properties should be upheld.

The properties verified are:
- The target_n_val is upheld, no node owns two partitions within the default target_n_val when the number of nodes is at least the target_n_val;
- No over-claiming, no transition leaves one node with less than ring_size div node_count partitions
- All partitions are assigned;
- Nodes with no claim are not assigned any vnodes.

However there are two significant issues with this property based testing:
- It assumes [one node is added at a time](https://github.com/basho/riak_core/blob/develop/src/riak_core_claim.erl#L1127-L1130), it does not test for bulk adding of nodes, in particular that bulk-adding which may occur when the cluster is first set-up (e.g. to transition from 1 node to 5 nodes);
- The list of properties is incomplete - no over-claiming does not prove balanced distribution, and there is no testing of balanced coverage plans, optimisation of spacing and minimisation of transition changes.

### An example - Building a 5 node Cluster

The ring is an Erlang record which can be passed around, until it is finalised and then gossiped to seek agreement for the completed transition.  A new node becomes a member of the ring when it is added to the record, but is only assigned partitions in the record when the ring and the node are passed to [choose_claim_v2](https://github.com/martinsumner/riak_core/blob/develop/src/riak_core_claim.erl#L271).  

If we look at a cluster which has a ring-size of 32, and is being expanded from 1 nodes to 5 nodes.  The property testing will add one node at a time to the ring record, and the call [choose_claim_v2](https://github.com/martinsumner/riak_core/blob/develop/src/riak_core_claim.erl#L271) every time a node has been added.  

Prior to the target_n_val being reached, the result of these iterations is irrelevant.  When the fourth node is added, the ring_size is now divisible by target_n_val - and then end outcome of that iteration will always be something of the form (regardless of how this is determined):

``| n1 | n2 | n3 | n4 | n1 | n2 | n3 | n4 | n1 | n2 | n3 | n4 | n1 | n2 | n3 | n4 | n1 | n2 | n3 | n4 | n1 | n2 | n3 | n4 | n1 | n2 | n3 | n4 | n1 | n2 | n3 | n4 |``

This conclusion will be reached as either the ring has been changed and [RingMeetsTargetN](https://github.com/martinsumner/riak_core/blob/develop/src/riak_core_claim.erl#L330) will evaluate to true, so that the [closing case clause](https://github.com/martinsumner/riak_core/blob/develop/src/riak_core_claim.erl#L331) will return the ring.  If not, RingMeetsTargetN will return false, and a [claim_rebalance_n/2](https://github.com/martinsumner/riak_core/blob/develop/src/riak_core_claim.erl#L337) will be called as the exit from the case clause - and this will always return a ring with the vnodes allocated to nodes in sequence.

The next node add will select vnodes to transition to node 5.  It will only select nodes that [do not violate](https://github.com/martinsumner/riak_core/blob/develop/src/riak_core_claim.erl#L323) the meeting of the target_n_val, so when the final case clause in choose_claim_v2 will be reached with the outcome:

``{RingChanged, EnoughNodes, RingMeetsTargetN} = {true, true, true}``

The modified ring which meets the target_n_val will then be used as the outcome of claim.  However, the input list to select partition is [ordered by the previous owner](https://github.com/martinsumner/riak_core/blob/develop/src/riak_core_claim.erl#L283).  The algorithm will select non-violating partitions from node 1, then node 2, then node 3 and then node 4.  The algorithm will move on from one node's list when the remaining indexes on the list are at 6 partitions (ring_size div node_count) - so giving up another node would make the donating node's contribution to the ring deficient for providing overall balance.  The algorithm will continue until the wants of node 5 have been satisfied (it wants 6 partitions).  

The effect of this algorithm is that it will take 2 nodes from node 1, 2 from node 2 and 2 nodes from node 3 - and at this stage it will stop taking nodes as the wants have been satisfied (and so no nodes will be taken from node 4).  The end outcome is a partition list like this:

``| n5 | n2 | n3 | n4 | n5 | n2 | n3 | n4 | n1 | n5 | n3 | n4 | n1 | n5 | n3 | n4 | n1 | n2 | n5 | n4 | n1 | n2 | n5 | n4 | n1 | n2 | n3 | n4 | n1 | n2 | n3 | n4 |``

This meets the target_n_val, but is unbalanced as the partitions are split between the nodes as follows:

- Node 1 - 6 partitions - 18.75%
- Node 2 - 6 partitions - 18.75%
- Node 3 - 6 partitions - 18.75%
- Node 4 - 8 partitions - 25.00%
- Node 5 - 6 partitions - 18.75%

So node 4 has 33.3% more partitions than any other node.

If as a user of Riak we build a 5 node cluster by adding one node at a time, the cluster will be built as tested in the property test - but this outcome violates a basic desirable property of ring-claiming, that it results in an even distribution of nodes.  The outcome isolates "one slow node" by focusing relatively-significant additional work in one location.

Although this is the tested scenario, this is not normally how 5 node clusters are built.  Normally, a user would setup five unclustered nodes, and then running a cluster plan to join four of the nodes to the first.  

The property testing code tests adding multiple nodes by adding them one node at a time to the ring, and then calling the choose_claim function after each addition, the actual code called during cluster planning works in a subtly different way.  The actual process adds all the nodes to be added as members to the ring first, and then the code loops over all the added nodes calling the choose_claim function each time.  So both the test code and real code loop over choose_claim for each added vnode, but in the test code there are no to-be added members on the ring when choose_claim is called each time, whereas in cluster planning all new nodes are added to the ring before the first call of choose_claim.  

There is a subtle difference between the process of adding one node in one plan and adding multiple nodes in one plan, and the property-based testing only confirms safe operation in the first case.

In the real-world cluster-planning scenario, the five nodes will be added to the ring, and choose_claim_v2 will be run four times, once for each joining node.  On the third loop, which adds the fourth node - the outcome of the algorithm will generally be:

``{RingChanged, EnoughNodes, RingMeetsTargetN} = {true, true, false}``

There is nothing about the picking algorithm used within choose_claim_v2 which will cause a cluster without enough nodes to transition to a cluster which meets `target_n_val` when the target_n_val'th node is added.

This state will prompt claim_rebalance_n/2 to be the [output of the loop](https://github.com/martinsumner/riak_core/blob/develop/src/riak_core_claim.erl#L337).  However, unlike the test scenario, the ring record passed to claim_rebalance_n will have all five nodes, not just the first four nodes - as with the cluster plan the ring had all the nodes added in-advance, not just prior to each individual call to choose_claim_v2.  The claim_rebalance_n function looks [at the members](https://github.com/martinsumner/riak_core/blob/develop/src/riak_core_claim.erl#L501) that have joined the ring (which is all five due to the cluster plan process, not just the four that have been used for iterations through choose_claim_v2).

So claim_rebalance_n will in this case output a simple striping of the partitions across the five nodes <b>with tail violations</b>:

``| n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 | n3 | n4 | n5 | n1 | n2 |``

The function choose_claim_v2 will then be called for a fifth time, for the fifth node.  However, node 5 has in this breaking ring all its wants satisfied - so the algorithm will [not make any ring changes](https://github.com/martinsumner/riak_core/blob/develop/src/riak_core_claim.erl#L324-L329).  Consequently `RingChangedM = false` at the final case clause, and claim_rebalance_n will be called.  The inputs to claim_rebalance_n are unchanged from the first call, and so this will produce the same result <b>with tail violations</b>.  

So the end outcome of transitioning from 1 node to 5 nodes in a real cluster will not meet the property of upholding a target_n_val, although the property-based testing will always claim this is upheld.  Further, even when an additional node is added to the cluster, the algorithm will only resolve once of these tail violations.  Only the addition of a seventh node will lead to a cluster upholding the target_n_val.

There is therefore no obvious and direct way to create a 5-node cluster in Riak with a 32-partition ring that has all the desired properties of a 5-node ring.  More dangerously if using a cluster plan to join all nodes at once, the consequent issue that some data will not be correctly dispersed across physical nodes will not be warned; although [application of best practice process](http://docs.basho.com/riak/kv/2.2.3/setup/upgrading/checklist/#confirming-configuration-with-riaknostic) before go-live should highlight the issue correctly.

Note, that a 5-node cluster is not a lonely exception.  There are similar issues starting a six node cluster with a ring-size of 128.  Starting a 5 node cluster with a ring-size of 128 through a cluster plan will not have any preflists onto split across nodes, but it will not meet the target_n_val - which is a more difficult issue to spot without manually drawing out the ring.

### Claim v2 - Evaluation

Evaluating the Claim v2 algorithm reveals the following weaknesses:

- Property-based testing doesn't correctly test the safety of clusters formed through cluster plans, and a [simple modification](https://github.com/basho/riak_core/compare/develop...russelldb:rdb/claim-eqc-borked) to the property test demonstrates this;

- The algorithm (and the test) don't check correctly for a truly even distribution of vnodes across nodes;

- The algorithm doesn't resolve tail violations when falling back to sequenced allocation of partitions;

- When adding one node at a time, the algorithm will not resolve all violations in the previous ring, even if such violations are resolvable.

- The algorithm doesn't consider the optimality of the outcome (could vnodes be further apart than target_n_val, could coverage query load be better balanced).

The Claimv2 algorithm does have the following positives:

- Adding one node at a time will never break the target_n_val;

- Once a cluster ring configuration supports the target_n_val, adding further nodes will never introduce target_n_val violations;

- The algorithm will produce a minimal number of transfers in the case of node addition.

## Riak Claim v3 and Upholding Claim properties

Riak has a v3 claim algorithm, which is not currently enabled by default.  It can however be enabled within a cluster, should issues be discovered with v2 claim.

The algorithm and its purpose is described briefly in the code:

> Claim V3 - unlike the v1/v2 algorithms, v3 treats claim as an optimization problem. In it's current form it creates a number of possible claim plans and evaluates them for violations, balance and diversity, choosing the 'best' plan.
>
> Violations are a count of how many partitions owned by the same node are within target-n of one another. Lower is better, 0 is desired if at all possible.
>
> Balance is a measure of the number of partitions owned versus the number of partitions wanted.  Want is supplied to the algorithm by the caller as a list of node/counts.  The score for deviation is the RMS of the difference between what the node wanted and what it has.  Lower is better, 0 if all wants are met.
>
> Diversity measures how often nodes are close to one another in the preference list.  The more diverse (spread of distances apart), the more evenly the responsibility for a failed node is spread across the cluster.  Diversity is calculated by working out the count of each distance for each node pair (currently distances are limited up to target N) and computing the RMS on that.  Lower diversity score is better, 0 if nodes are perfectly diverse.

The algorithm calculates the end distribution of partitions across nodes (by count) [up-front](https://github.com/basho/riak_core/blob/develop/src/riak_core_claim.erl#L386), without reference to the current distribution of vnodes.  These "wants" will produce a cluster with [a balanced distribution](https://github.com/basho/riak_core/blob/develop/src/riak_core_claim.erl#L775-L793) of vnodes.  For example if the outcome is a 5-node cluster with a ring-size of 32, the algorithm will allocate a target count to each node of 6 vnodes to 3 nodes, and 7 vnodes to 2 nodes, before beginning the process of proposing transfers.

The algorithm then examines the current ring, and looks for [violations](https://github.com/basho/riak_core/blob/develop/src/riak_core_claim.erl#L896-L904) and [overloads](https://github.com/basho/riak_core/blob/develop/src/riak_core_claim.erl#L908-L920).  Violations are partitions in breach of target_n_val, and where two partitions are too close together on one node, both those partitions in the pair are included in the list of violations.  Overloads are <b>all</b> of the partitions that belong to all of the nodes that own more partitions than their target ownership count.  In most cases when adding nodes to a relatively small cluster, this will be all the partitions.

[Two rounds of "takes" are attempted](https://github.com/basho/riak_core/blob/develop/src/riak_core_claim.erl#L883-L888).  Firstly, for each partition in violation, the partition is offered to a randomly-selected node which is capable (i.e. the node has spare capacity under its target count, and has no partition within target_n_val of the offered partition) of taking that violating partition.  This will create a new version of the ring.

The overloads are then calculated based on the view of the ring after resolving violations.  The same random take process is used then to distribute the overload indexes until no node is a "taker" - i.e. no node has spare capacity (unfulfilled wants), or there are no spare indexes left for any nodes to take.

This process of randomly selecting the violating and overloaded partitions, and distributing them to nodes that have capacity and would not cause a breach, will output a plan for a new ownership structure.  The claim_v3 algorithm will run multiple ([default 100](https://github.com/basho/riak_core/blob/develop/src/riak_core_claim.erl#L437)) iterations of this generating a new potential plan each time.  The plans are then scored on [diversity](ring_claim.md#diversity-scoring) and balance, although it is unclear as to how the outcome could end up unbalanced.  The best plan is chosen as the result.  The random factor in the takes is generated from a seed, and so the process if repeatable with that seed: given the same starting point and the same seed - the result is deterministic.  This determinism is used during the committing of a plan to validate that the current combination of code and ring-state (and the same seed) still returns the same plan, before deciding to commit the plan.  

The violation score of the best plan is checked before returning the proposal.  If the violation score is 0, that is the proposed plan.  If the best plan has a non-zero violation score, then a new ring arrangement is calculated using the [claim_diversify/3](https://github.com/basho/riak_core/blob/develop/src/riak_core_claim.erl#L471-L477), with the new ring arrangement ignoring all previous allocations.  

The claim_diversify function implements an algorithm that tries to assign each partition one at a time to a node that will not break the target_n_val.  If there is more than one eligible node a scoring algorithm is used to choose the next one.  The scoring is mysterious in its actual aim.  The algorithm looks at all pairs of nodes, and sees how many times those nodes are 1, 2, 3, and 4 partitions apart on the ring (or up to n nodes apart if the target_n_val is not 4), and prefers to find a scenario where pairs are all distances apart with equal frequency.  So if a pair of nodes are always only 1 partition apart, or always 4 partitions apart - then in both these scenarios this will score poorly.  The score is the sum of the scores for all pairs.  When selecting the node, the relative rolling score change is calculated for each selection, and the best option chosen at selection time, based on that rolling score.  There is no back-tracking to optimise the score over the complete run.

### Diversity Scoring

The end outcome of diversity scoring is that if there are a series of allocations all within the target_n_val it tends to prefer the most 'jumbled' of these scores (lower scores are considered better):

```
ScoreFun([a,b,c,d,e,f,a,b,c,d,e,f]), 4).
76.79999999999986

%% Swap the second and third partitions, and rebalance the sequences
%% by also swapping the same way the eight and ninth partitions

ScoreFun([a,c,b,d,e,f,a,c,b,d,e,f]), 4).
76.79999999999988

%% Swap back the eighth and ninth partitions, so only second and third
%% partitions vary from sequence

ScoreFun([a,c,b,d,e,f,a,b,c,d,e,f]), 4).
46.79999999999991

%% In addition, swap the fifth and sixth partitions

ScoreFun([a,c,b,d,f,e,a,b,c,d,e,f]), 4).
40.799999999999955

%% In addition swap the seventh and eighth partitions

ScoreFun([a,c,b,d,f,e,b,a,c,d,e,f]), 4).
38.59166666666669

%% In addition swap the first and last partitions

ScoreFun([f,c,b,d,f,e,b,a,c,d,e,a]), 4).
32.125
```
However, it may also prefer sequences that have a [risk of breach](ring_claim.md#further-spacing) on dual node failures, over those sequences that don't have such issues:

```
ScoreFun([a, b, c, d, e, f, g, h, a, b, c, d, e, f, g, h], 4).
109.71428571428542

%% The above will always write to three different physical nodes on any dual
%% node failure.
%%
%% But the example below will no longer write to three different nodes if
%% any two of {a, b, c, d} or any two of {e, f, g, h} fail.
%%
%% The worse sequence (from this perspective) scores better.

ScoreFun([a, b, c, d, a, b, c, d, e, f, g, h, e, f, g, h], 4).
66.0
```

This scoring anomaly may be related to the algorithm being constrained to consider only diversity within the target_n_val distance.  This reasoning behind this restriction is unclear.

The claim_diversify algorithm doesn't necessarily prevent preventable target_n_val violations.  It tries to avoid them, but does no back-tracking to resolve them when they occur, and simply logs at a debug level to notify of the breach.

Note that in the special case where the number of nodes is equal to the target_n_val, all vnodes will be allocated in sequence and no plans will be evaluated.  

### Operator Experience

The claim process is initiated by an operator submitting a cluster plan, and when calculations are complete the operator will be presented with the final proposal, unaware as to the reasons as to why it is considered to be the best proposal, or even if it meets the requested conditions (e.g. target_n_val).  The operator has a choice of either accepting the proposal or re-rolling the dice and request another proposal.  If the next proposal is considered by the operator to be "worse", they cannot revert to the previous proposal, they must keep requesting new proposals until they hit again a proposal they consider to be "optimal".

### Claim v3 - Evaluation

The version 3 claim algorithm is significantly more complex than the version 2 algorithm, and harder to use due to its non-deterministic nature.  It is not obvious that it solves all of the problems of the version 2 algorithm, in particular, when running the property-based tests with multiple (rather than one-by-one) node additions, property-based test failures occurred due to non-resolution of tail violations.

It should be less likely to result in an unbalanced distribution of partitions.  The randomisation and use of claim_diversify may resolve circumstances where symmetry of distribution has undesirable outcomes (such as unbalanced coverage plans).  However, improvements on version 2 are not guaranteed - and any improvements come at significant cost both in terms of both code complexity, and operator experience.

## Riak and Proposed Claim Improvements

To improve Riak claim, it would be preferable to make simple changes to claim_v2, and ensure those changes are well supported by property-based testing.  Although claim v3 represents the future direction expected by Basho for claim five years ago, it is now five years since it was developed without it being made a default feature of the database.  The complexity-related risks of moving to version 3, potentially outweigh the limited potential for benefits.  Version 3 should remain as an option to be used if version 2 returns sub-optimal results, but the recommendation here is to improve by fixing claim v2 rather than progressing a move to claim v3 .  

The suggested improvements for version 2 claim are:

- Add property-based testing for bulk, not just single, additions - note that such new property-based tests have already been proven to fail;

- Add property-based testing to check correctly for a genuine balance of vnode distribution following changes (e.g. the difference between the most populated node, and the least populated, should never exceed 1) - note that it is anticipated that such new property-based tests would fail;

- Alter the claim_rebalance_n algorithm so that it will attempt to auto-resolve tail violations by tail-addition and back-tracking to increase the remainder, where the number of nodes is at least target_n_val + 1;

- Change the selection of indexes to more strictly enforce the selection of a balanced output.

The first three stages are expected to be relatively simple, the final stage may also be simple.

## Some Further Thoughts

### Physical Promises at Run-Time

To the developer, Riak will provide promises with respect to the ring.  The developer can choose n, r, w, pw, pr, dw values to suit the application, but all these refer to vnodes and don't say anything directly about physical nodes.  The relationship between the ring-based promises and the physical outcome have to be assumed.

If as a developer the requirement of the database is that data has been made durable on two physical nodes before it is acknowledged, the way of being sure of this commitment is by setting primary writes (pw) to 2.  In this case, <b>if</b> the ring has been constructed correctly with a target_n_val of 4, then confirmation from two primary partitions is enough to be sure that the update has been acknowledged by two different physical nodes.

This may silently become untrue if the target_n_val commitment was not met during the building of the cluster.  More commonly, there may also be occasions when the data is sent to two physical nodes, but is not written to two primaries.  If two nodes fail, all partitions whose preflist include both those nodes, will now "fail" to acknowledge writes if pw is set to 2 - but if the target_n_val was met, the write will still almost certainly have been sent to two physical nodes.  Confusingly for the developer, at this stage there is no way of confirming whether or not the write did get to two nodes.  Replaying the write with pw=2 will continuously fail, reading the object with pr=2 will continuously fail, regardless of whether or not the object is actually on two physical nodes.  If the developer knows that only two nodes have failed, and that the target_n_val of 4 was met during the last cluster change, then the developer could assume that writing with dw=2 and pw=1 was safe.  However, the developer has no way of confirming this programmatically at run-time.

The abstract nature of ring commitments requires the developer to unnecessarily reduce availability to be sure that physical promises of diversity are kept.  Put coordinators, and GET finite state machines know more about physical diversity at run-time than the developer can assume at design-time - they know which nodes requests have been forwarded to, not just which vnodes.  It would be a potential improvement to have PUT and GET options where the developer could request for confirmation of a level of physical not logical diversity before acknowledgement.

### Availability Zones

Running Riak in a cloud environment is challenging where durability guarantees are required.  Riak write guarantees are based around the ring, and some assumptions can be made about how the ring-based commitments map to physical nodes: however in a cloud environment we can't now assume that different physical nodes have hardware diversity.  Corruption of data on one node in an Availability Zone may be coupled with an increased likelihood of corruption of data on another node within the same Availability Zone.  

If we want to make sure that data is safe in the cloud, the data needs to be written to two Availability Zones.  However, no assumptions can be made about this based on the current ring claim algorithm.

There are real issues with making riak_core Availability Zone aware:

- If the cloud environment supports only three Availability Zones then the wraparound problem at the tail of the ring is unavoidable for all ring-sizes - some preflists will only exist in two availability zones, and so even setting pw=2 will not guarantee physical diversity in a healthy cluster.

- There is significant additional cost and notable performance impact when splitting a Riak cluster across availability zones due to network charges and network latency.  This can be partially mitigated by [using HEAD requests](https://github.com/martinsumner/leveled/blob/master/docs/FUTURE.md#get_fsm---using-head) but this is not available in any production-ready backend.

- If the cloud environment supports only three Availability Zones any best-case diversity configuration would lead to coverage plans being exclusively focused within a single availability zone (as some cluster plans may involve significant data transfer, this may actually be advantageous).

It may be possible to make some commitments by combining claim changes with physical run-time promises - but there is no obvious answer to supporting Availability Zone intelligence when using distribution of data through a ring which is sized `2 ^ n`.

Given the constraints imposed by making the ring-size a power of 2, it is worth considering whether an alternative basis for ring-sizes could be used.  With a ring size which is a power of 2, the operations needed to allocate a binary hash value to a partition are computationally efficient, requiring only bit-shifting.  Efficiency is not restricted to power of 2 operations, determining the value of a large binary integer modular three [can also be done efficiently](https://math.stackexchange.com/a/979279).  It is unclear if there are deeper reasons as to why ring-sizes have been historically constrained to being a power of 2.

It might be possible to have ring sizes of the order `3 x 2 ^ n`, where the partition is based on both the range partitioning of the hash space, and the modular 3 of the hash.  This would give ring-sizes of 24, 48, 96, 192 etc, but as all these ring-sizes are divisible by three allocation of the ring round-robin across availability zones would now be possible.  This would lead to uneven distributions on failure, that may have beneficially side-effects (if a node fails only fallbacks within the same Availability Zone will be used).  This would also align coverage plans with Availability Zones.

Any such change to the ring-size would likely be a breaking change to a cluster, as ring-resizing is not a solved problem.  There would be no way of migrating a cluster to such a configuration, other than through cluster-to-cluster replication <b>not</b> using the proprietary Riak mechanisms - as the Riak replication mechanisms are coupled to consistency of ring-size between clusters.

Having an AZ-aware ring for a triple-AZ configuration is an area of potential interest that may warrant further research.
