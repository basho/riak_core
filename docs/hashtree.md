`hashtree.erl` implements a fixed-sized hash tree, avoiding any need
for rebalancing. The tree consists of a fixed number of on-disk
`segments` and a hash tree constructed over these `segments`. Each
level of the tree is grouped into buckets based on a fixed `tree
width`. Each hash at level `i` corresponds to the hash of a bucket of
hashes at level `i+1`. The following figure depicts a tree with 16
segments and a tree-width of 4:

![image](https://github.com/basho/riak_kv/raw/jdb-hashtree/docs/hashtree.png)

To insert a new `(key, hash)` pair, the key is hashed and mapped to
one of the segments. The `(key, hash)` pair is then stored in the
appropriate segment, which is an ordered `(key, hash)` dictionary. The
given segment is then marked as dirty. Whenever `update_tree` is
called, the hash for each dirty segment is re-computed, the
appropriate leaf node in the hash tree updated, and the hash tree is
updated bottom-up as necessary. Only paths along which hashes have
been changed are re-computed.

The current implementation uses LevelDB for the heavy lifting. Rather
than reading/writing the on-disk segments as a unit, `(key, hash)`
pairs are written to LevelDB as simple key-value pairs. The LevelDB
key written is the binary `<<$s, SegmentId:64/integer,
Key/binary>>`. Thus, inserting a new key-value hash is nothing more
than a single LevelDB write. Likewise, key-hash pairs for a segment
are laided on sequentially on-disk based on key sorting. An in-memory
bitvector is used to track dirty segments, although a `gb_sets` was
formerly used.

When updating the segment hashes, a LevelDB iterator is used to access
the segment keys in-order. The iterator seeks to the beginning of the
segment and then iterators through all of the key-hash pairs. As an
optimization, the iteration process is designed to read in multiple
segments when possible. For example, if the list of dirty segments was
`[1, 2, 3, 5, 6, 10]`, the code will seek an iterator to the beginning
of segment 1, iterator through all of its keys, compute the
appropriate segment 1 hash, then continue to traverse through segment
2 and segment 3's keys, updating those hashes as well. After segment
3, a new iterator will be created to seek to the beginning of segment
5, and handle both 5, and 6; and then a final iterator used to access
segment 10. This design works very well when constructing a new tree
from scratch. There's a phase of inserting a bunch of key-hash pairs
(all writes), followed by an in-order traversal of the LevelDB
database (all reads).

Trees are compared using standard hash tree approach, comparing the
hash at each level, and recursing to the next level down when
different. After reaching the leaf nodes, any differing hashes results
in a key exchange of the keys in the associated differing segments.

By default, the hash tree itself is entirely in-memory. However, the
code provides a `MEM_LEVEL` paramemter that specifics that levels
greater than the parameter should be stored on-disk instead. These
buckets are simply stored on disk in the same LevelDB structure as
`{$b, Level, Bucket} -> orddict(Key, Hash)}` objects.

The default settings use `1024*1024` segments with a tree width of
`1024`. Thus, the resulting tree is only 3 levels deep. And there
are only `1+1024+1024*1024` hashs stored in memory -- so, a few
MB per hash tree. Given `1024*1024` on-disk segments, and assuming
the code uniformly hashes keys to each segment, you end up with ~1000
keys per segment with a 1 billion key hash tree. Thus, a single key
difference would require 3 hash exchanges and a key exchange of
1000 keys to determine the differing key.
