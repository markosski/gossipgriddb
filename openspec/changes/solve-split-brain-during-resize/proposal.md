## Why

During cluster resizing, a new node might claim leadership of partitions that the previous leader still considers itself responsible for until the cluster configuration converges. This leads to a split-brain scenario where multiple nodes accept writes for the same partition, causing data inconsistency.

## What Changes

- Introduce a `locked_partitions` set for each node.
- **BREAKING**: Modify the write path to reject writes when a partition is in the local `locked_partitions` set.
- Implement a symmetrical lock handshake during cluster resizing:
  1. New leader adds partition to `locked_partitions` and gossips.
  2. Old leader sees the lock, stops serving local writes, and adds the partition to its own `locked_partitions`.
  3. New leader sees the acknowledgment (old leader's lock), removes the partition from its `locked_partitions`, and begins serving writes.
- Ensure reads remain available even when a partition is `LOCKED`.

## Non-Goals

- Implementation of a centralized consensus protocol (e.g., Raft).
- Structural changes to the underlying storage engines.
- Atomic multi-partition locking across node boundaries.
- Immediate strong consistency for reads during the lock window.

## Capabilities

### New Capabilities
- `partition-locking`: Mechanism to manage and gossip a `locked_partitions` set to coordinate leadership transitions.

### Modified Capabilities
- `cluster-resizing`: Update the resizing workflow to incorporate the lock handshake before finalizing the transition.
- `write-availability`: Add checks for `locked_partitions` before allowing write operations.

## Impact

- **Storage Layer**: Needs to track `locked_partitions`.
- **Gossip Protocol**: Must carry `locked_partitions` in metadata.
- **Web/API**: Write endpoints must return 503 errors when `LOCKED`.
- **Internal Cluster Logic**: The recalculation of leadership will trigger the locking phase.
