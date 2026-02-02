## 1. Foundation: Data Model (Independently Testable)

- [ ] 1.1 Add `locked_partitions` field (`HashSet<PartitionId>`) to `SimpleNode` struct
- [ ] 1.2 Update gossip serialization (Encode/Decode) for `locked_partitions`
- [ ] 1.3 Add unit tests verifying serialization/deserialization of `locked_partitions`

## 2. Foundation: Helper Methods (Pure Functions)

- [ ] 2.1 Implement `has_locked_partitions(&self) -> bool` in `Cluster`
- [ ] 2.2 Implement `is_partition_locked(&self, partition: PartitionId) -> bool` in `Cluster`
- [ ] 2.3 Implement `get_nodes_with_locked_partition(&self, partition: PartitionId) -> Vec<NodeId>` in `Cluster`
- [ ] 2.4 Add unit tests for all helper methods

## 3. Resize Guard (Standalone Feature)

- [ ] 3.1 Add `ResizeInProgress(String)` variant to `ClusterOperationError`
- [ ] 3.2 Add check at start of `Cluster::resize()` to reject if `has_locked_partitions()` returns true
- [ ] 3.3 Add unit test: resize rejected when any node has locked partitions
- [ ] 3.4 Add unit test: resize succeeds when no locks exist
- [ ] 3.5 Update resize API handler to return proper HTTP error for `ResizeInProgress`

## 4. Web API Gatekeeper (Standalone Feature)

- [ ] 4.1 Implement `PartitionLocked` variant in `WebError` for 503 responses
- [ ] 4.2 Update `decide_routing` to check if local partition is locked before routing locally
- [ ] 4.3 Modify write handlers (`handle_post_item`, `handle_remove_item`) to check locks and return 503
- [ ] 4.4 Add integration test: write to locked partition returns 503
- [ ] 4.5 Add integration test: read from locked partition succeeds

## 5. Lock Initiation (New Leader)

- [ ] 5.1 Update `assign_partition_leaders` to identify partitions where leadership is changing
- [ ] 5.2 For newly elected partitions, add them to local `locked_partitions` set
- [ ] 5.3 Tick HLC when modifying `locked_partitions`
- [ ] 5.4 Add unit test: new leader locks partitions it's elected to lead
- [ ] 5.5 Add integration test: verify locked partitions are gossiped to cluster

## 6. Lock Acknowledgment (Old Leader)

- [ ] 6.1 Implement gossip observation logic to detect when remote node locks a partition
- [ ] 6.2 If local node is current leader for that partition, add it to local `locked_partitions`
- [ ] 6.3 Tick HLC when acknowledging lock
- [ ] 6.4 Add unit test: old leader detects new leader's lock via gossip
- [ ] 6.5 Add integration test: old leader acknowledges lock within one gossip round

## 7. Lock Release (New Leader Opening)

- [ ] 7.1 Implement acknowledgment detection: check if all healthy assigned nodes have locked the partition
- [ ] 7.2 Implement force-unlock detection: check if old leader is `Disconnected`
- [ ] 7.3 Remove partition from `locked_partitions` when acknowledged or forced
- [ ] 7.4 Tick HLC when unlocking
- [ ] 7.5 Add unit test: new leader unlocks after acknowledgment
- [ ] 7.6 Add unit test: new leader force-unlocks after old leader disconnected (~10s)

## 8. Lock Cleanup (Old Leader)

- [ ] 8.1 Implement cleanup detection: check if new leader has removed partition from its `locked_partitions`
- [ ] 8.2 Remove partition from local `locked_partitions` once new leader has opened
- [ ] 8.3 Tick HLC when cleaning up
- [ ] 8.4 Add integration test: full handshake completes within expected time window

## 9. Persistence and Recovery

- [ ] 9.1 Update `NodeMetadata` struct to include `locked_partitions` field
- [ ] 9.2 Persist `locked_partitions` when node state is saved
- [ ] 9.3 Restore `locked_partitions` on node restart (persistent nodes only)
- [ ] 9.4 Verify ephemeral nodes initialize with empty `locked_partitions`
- [ ] 9.5 Add integration test: node restart during handshake resumes correctly

## 10. Edge Case Testing

- [ ] 10.1 Test: Consecutive resize rejected during handshake (edge case #1)
- [ ] 10.2 Test: New leader crashes mid-handshake, partition reassigned (edge case #2)
- [ ] 10.3 Test: Network partition during handshake, eventual convergence (edge case #3)
- [ ] 10.4 Test: False disconnection, force-unlock followed by old leader return (edge case #4)
- [ ] 10.5 Test: Multi-node concurrent join triggering multiple handshakes

## 11. Performance and Calibration

- [ ] 11.1 Measure handshake duration under normal conditions (baseline)
- [ ] 11.2 Measure handshake duration under slow gossip (degraded)
- [ ] 11.3 Verify no split-brain writes during lock window (correctness)
- [ ] 11.4 Calibrate gossip intervals if handshake duration is unacceptable
- [ ] 11.5 Add observability metrics for lock state and handshake duration
