There are two primary components to ring resizing operation. The
first, which is similar to cluster membership operations,
is determining what the future ring will look like once the operation
has completed. The second is facilitating that transition.

### The Claimant

Determining what the future ring will look like when resizing is
primarily about determining future ownership. Like normal cluster
operations this is the responsibility of `riak_core_claimant`.  Ring
resizing is also staged, planned, and committed via the claimant. The
claimant determines the ownership of the future ring and what
transfers will be necessary to safely make the transition. How the
future ring is determined is slightly different though, and is described
below.

If the ring is expanded all the existing partitions will be in bolth
the old and new ring. The new partitions are initially assigned to a
dummy node, before being assigned through claim. If the ring is
shrunk, some partitions will be removed. The claimant does this and
then runs the ring through claim, rebalancing it. Some existing
partitions may change owners as part of this process.

Like other ownership changes, after the claimant determines the future
ring it schedules transfers using the "next list" (a field in Riak's
gossip structure). However, some additional information is needed in
the case of resize operations. This is discussed in more detail below
in "Transfer Scheduling". The future ownership information also needs
to be carried in the gossip structure because unlike other ownership
changes the future ring cannot be calculated using only the next
list. This information is stored in the metadata dictionary that is
part of the gossip structure.

If the plan to resize the ring is committed, the claimant will not
install the new ring until all of the scheduled transfers have
completed. This is necessary to ensure a safe transition. This has the
side-effect that, with the exception of `force-replace`, other cluster
operations will be delayed until resizing completes. The claimant also
provides the ability to cancel while resizing is in-flight.

### Transfer Scheduling

To safely transition the cluster between different sized rings,
partitions must transfer their data not only to new owners of the
same partitions, but to different partitions. This is because when the
size of the ring changes so does every preflist. The figure below
illustrates this for a couple of bucket/key pairs.

![preflist](http://data.riakcs.net:8080/jrw-public/dynamic-ring/expand-preflist.png)

In the image on the left a key in the old ring is owned by N (3 in
this case) partitions, as expected. In the middle image, each
partition is divided in half to show, relatively, where the keys fall
within the keyspace owned by each partition. The image on the right
shows which partitions should own the key in the new ring.

![transfers](http://data.riakcs.net:8080/jrw-public/dynamic-ring/expand-preflist-transfers.png)

To safely transfer this key between the old and new ring there are
five necessary transfers (there would be a 6th, the top arrow in the
image above, but the owner of that portion of the keyspace is the same
node). The number of transfers is dependent on the growth factor (new
ring size / old ring size) and the largest N value in the
cluster. A similar process takes place when the ring is shrinking.

The first transfers are scheduled as part of the claimant operation
that determines ownership in the new ring. The claimant schedules a
single *resize operation* for each vnode in the existing ring. The
*resize operation* is an entry in the next list with a special value,
`$resize`, in the next owner field. It is intended to indicate the
existing vnode has several actual transfers to complete before it has
safely handed off all of its data. The claimant will also schedule a
single *resize transfer* for each index. The *resize transfer*
represents a single handoff of a portion of the keyspace between two
partitions, that may or may not have different owners (most likely
they will). The *resize transfer* that is scheduled depends on several
factors:

1. If the ring is shrinking and the partition will no longer exist in the
new ring, the transfer is scheduled to the owner of the first successor.
1. If the ring is expanding and the existing partition is not being
moved to a new owner, the transfer is scheduled to the owner for the
first predecessor.
1. If the ring is expanding and the existing partition is being moved
to a new owner, the transfer is scheduled to the new owner.

A *resize transfer* is similar to repair in that not all keys are
transferred. Since repair was added to Riak, the handoff subsystem has
had the ability to take a filter function which determines which keys
will be sent. This implementation augments that support to allow an
additional function to be provided, which is called when a key is not
sent. *Resize transfers* use this to determine when a key should be
sent to the partition it is transferring to and to determine what
partitions unsent keys need to be transferred to before the *resize
operation* can complete. This list of partitions is used to schedule
further *resize transfers*. This is performed for each *resize
transfer* that a partition takes part in, in order to ensure new
writes to buckets with a larger N value are not lost. Once all
scheduled *resize transfers* have completed and no new partitions are
discovered to transfer to, the *resize operation* for the partitions
has completed.

#### Determining Future Ownership

Given the hashed value of a key, the preflist for the hashed value is
calculated in bolth the current and resized ring. To determine
if a partition should transfer a given key to another, the position of
the source and target partition in the current and future preflist,
respectively, are compared. If the position is the same the key should
be sent. To determine which target partition the key should be
transferred to, in the event it should not be sent to the current
target, the partition at the same position in the future preflist as
the source partition in the current preflist is chosen. 

### Request Forwarding

Like ownership transfer, vnodes may forward requests to the new owner
during and after handoff (until the ownership change is
installed). But there are some differences. Ownership transfer
involves only a single new owner and always attempts to progress to the
future ring. Ring resizing can be canceled, and this requires some special
handling to not lose writes in this case.

Typical forwarding comes in two flavors: *explicit*
(`handle_handoff_command`) and *implicit*. Forwarding during resizing
only uses *explicit* forwarding. *Explicit* forwarding gives the local
vnode the option to effect a write locally before possibly choosing to
forward it on to the new owner. *Implicit* forwarding does not -- all
requests are immediately forwarded.  Since, the *resize operation* can
be canceled at any time, and since the time a vnode forwards during
the operation is significant, it is necessary to allow the local vnode
to perform the write before forwarding even after transferring all of
its data. Because of this assumption, this also means that reads can
be performed locally and not forwarded the entire time the ring is
resizing.

The following describes the reasoning for the above in more
detail. During ring resizing a vnode can be in one of four states:

1. *resize operation* for partition not yet started, ring resizing ongoing
1. *resize operation* ongoing, no *resize transfer* ongoing
1. *resize operation* ongoing, *resize transfer* ongoing
1. *resize operation* for partition complete, ring resizing ongoing

In each of these states, and based on several conditions, the vnode
will take one of two actions:

1. Handle request locally only, no forwarding -- `Mod:handle_command`
1. Option to handle locally + option to forward -- `Mod:handle_handoff_command`

The table below details the action taken by the vnode in a specific
state and based on the progress of the partition's *resize operation*:

<table>
<tr>
<th>State</td>
<th>Condition</td>
<th>Action</td>
<th>Notes</td>
</tr>
<tr>
<td>1</td>
<td>none</td>
<td>handle_command</td>
<td>Writes will be transferred to future owner via future resize transfer</td>
</tr>
<tr>
<td>2</td>
<td>Request for key in portion of keyspace not yet transferred</td>
<td>handle_command</td>
<td>Writes will be transferred to future owner via future resize transfer</td>
</tr>
<tr>
<td>2</td>
<td>Request for key in portion of keyspace already transferred</td>
<td>handle_handoff_command</td>
<td>Writes need be performed locally and remote, reads can be done locally</td>
</tr>
<tr>
<td>3</td>
<td>Request for key in portion of keyspace not yet transferred</td>
<td>handle_command</td>
<td>Writes will be transferred to future owner via future resize transfer</td>
</tr>
<tr>
<td>3</td>
<td>Request for key in portion of keyspace already transferred</td>
<td>handle_handoff_command</td>
<td>Writes need be performed locally and remote, reads can be done locally</td>
</tr>
<tr>
<td>3</td>
<td>Request for key in portion of keyspace being transferred</td>
<td>handle_handoff_command</td>
<td>Writes need be performed locally and remote, reads can be done locally</td>
</tr>
<tr>
<td>4</td>
<td>Request for key in portion of keyspace transferred</td>
<td>handle_handoff_command</td>
<td>Writes need be performed locally and remote, reads can be done locally</td>
</tr>
<tr>
<td>4</td>
<td>Request for key in portion of keyspace not transferred</td>
<td>handle_handoff_command</td>
<td>Occurs if max N val used by cluster changes during operation.
Writes need be performed locally and remote, reads can be done locally. This case is
not handled by the current implementation (the N-value should not be enlarged during resize)
</td>
</tr>
</table>

What partition to forward to is determined in the same manner that
*resize transfers* use to determine the proper target partition for an
unsent key.

All coverage requests are handled locally.

### Application Changes

There are three changes that must be made to any `riak_core` application
that wishes to support dynamic ring:

1. A function `object_info(term()) -> {undefined | binary(),binary()}`
must be defined. The argument to the function is key being transferred
during a *resize transfer* (the key passed to the handoff fold
function). The return value is a 2-tuple whose first element is either
undefined or the bucket the key is in and whose second element is the
hashed value of the key (e.g. value returned from
`riak_core_util:chash_key/1` in the case of `riak_kv`).
2. A function `request_hash(term()) -> undefined | binary()` must be
defined. The function takes the request being processed as an argument
and should return the hashed value of the key the request will act
upon or `undefined`. If `undefined` is returned, the request will never be
forwarded during ring resizing (`handle_command` will always be called).
3. A function `nval_map(riak_core_ring:riak_core_ring()) -> [{binary(), integer()}]`
must be defined. The function takes the current ring and should return a proplist
containing bucket/n-val pairs. For any buckets not included the default N value
will be used. This function is used when the ring is shrinking to ensure the N-value
isn't implicitly enlarged during the transition, it is not used during expansion.

One other minor change made to the vnode interface, regardless of
whether or not resizing is being used, is the first argument
of `handoff_starting/2` is now passed as `{HOType, {Idx, Node}}`.
`HOType` is one of `ownership_transfer`, `hinted_handoff` or
`resize_transfer`. The `Idx` being transferred to is always the index
of the current partition, except in the case of `resize_transfer`.
`Node` is the node being transferred to (this used to be the argument
passed into `handoff_starting/2`).

### Cleaning Up

To ensure data is not lost in the case the operation fails or is
aborted, after transfers complete the data is not immediately
removed. Data is removed after the resized ring is
installed. The claimant will schedule another special operation using
the "next list", `$delete`. This operation will be scheduled for any
partition that no longer exists in the new ring or that was moved during
resizing. The end result is that each of these partitions delete their data and
shut down.

The same will happen to new partitions when the ring is
expanded but the operation does not complete. Any data transferred to them
will be deleted since it will not be reachable until the operation
is resubmitted.
