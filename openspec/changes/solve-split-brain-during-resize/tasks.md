## 1. Data Model Updates

- [ ] 1.1 Add `locked_partitions` field (`HashSet<PartitionId>`) to `SimpleNode` struct
- [ ] 1.2 Update gossip serialization (Encode/Decode) for `locked_partitions`
- [ ] 1.3 Implement helper to check if a partition is "effectively locked" (locally or by expected previous leader)

## 2. Web API Gatekeeper

- [ ] 2.1 Update `decide_routing` to check local `locked_partitions` before routing locally
- [ ] 2.2 Implement `Locked` variant in `WebError` for 503 responses
- [ ] 2.3 Modify write handlers (`handle_post_item`, `handle_remove_item`) to return 503 if partition is in `locked_partitions`
- [ ] 2.4 Verify READ requests ignore the lock status and return available data

## 3. Leader Transition Logic (Part 1: The Lock & Acknowledge)

- [ ] 3.1 Update `assign_partition_leaders` to move newly elected partitions to `locked_partitions`
- [ ] 3.2 Implement observation logic: Detect when *any* remote node (as a new leader) is locking a partition
- [ ] 3.3 Create a "lock acknowledgment" trigger: Join the lock by adding the partition to local `locked_partitions` (and stop local writes if applicable)
- [ ] 3.4 Ensure the local node ticks its HLC whenever `locked_partitions` is modified

## 4. Leader Transition Logic (Part 2: The Acquisition)

- [ ] 4.1 Implement acknowledgment check: New leader waits for `locked_partitions(P)` from *all* healthy/assigned nodes in gossip
- [ ] 4.2 Add "Force Unlock" logic: Proceed to open if previous leader is `Disconnected` or no other node is healthy/assigned
- [ ] 4.3 Implement "Opening": Remove partition from `locked_partitions` once acknowledgment or disconnect is confirmed
- [ ] 4.4 Implement "Cleanup": Old leader removes partition from its `locked_partitions` once the new leader has opened it

## 5. Persistence and Recovery

- [ ] 5.1 Update `NodeMetadata` to persist `locked_partitions` state across restarts
- [ ] 5.2 Implement recovery logic to resume transitions (re-locking) after a node restart
- [ ] 5.3 Verify that an ephemeral node initializes with an empty `locked_partitions`

## 6. Verification and Calibration

- [ ] 6.1 Create an integration test simulating a multi-node resize with explicit delay in gossip processing
- [ ] 6.2 Verify that no split-brain writes occur during the window where both nodes see the lock
- [ ] 6.3 Test recovery when an old leader is killed mid-handshake
- [ ] 6.4 Calibrate gossip intervals to balance consistency window vs network overhead
