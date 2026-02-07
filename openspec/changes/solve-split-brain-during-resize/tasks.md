## 1. Foundation: Data Model

- [ ] 1.1 Add `importing_partitions` (HashSet<PartitionId>) to `SimpleNode` struct
- [ ] 1.2 Add `migrating_partitions` (HashSet<PartitionId>) to `SimpleNode` struct
- [ ] 1.3 Update gossip serialization for new fields
- [ ] 1.4 Add unit tests for serialization

## 2. Foundation: Migration State Logic

- [ ] 2.1 Implement `is_importing(&self, partition: PartitionId) -> bool`
- [ ] 2.2 Implement `is_migrating(&self, partition: PartitionId) -> bool`
- [ ] 2.3 Implement `mark_key_migrated(&self, partition: PartitionId, key: String)`
- [ ] 2.4 Implement `is_key_migrated(&self, partition: PartitionId, key: String) -> bool`

## 3. Web API: Request Forwarding & Dual Write

- [ ] 3.1 Implement `ForwardRequest` client logic
- [ ] 3.2 Update `handle_post_item` to detect `migrating` status
- [ ] 3.3 Implement "Dual Write" logic: Apply write locally, THEN forward to target.
- [ ] 3.4 Update `handle_get_item`: Serve locally (Source has data).
- [ ] 3.5 Ensure `WebError` handles forwarding failures gracefully (fallback to local success if target fails?).

## 4. Migration Process: Target Node (Receiver)

- [ ] 4.1 Update Topology Calculation: When taking new partition $P$, add to `importing_partitions`
- [ ] 4.2 Gossip `importing_partitions` immediately
- [ ] 4.3 Implement Sync Receiver: Endpoint to accept bulk key data from Source
- [ ] 4.4 Implement `finalize_import(partition)`: Remove from `importing`, become full leader

## 5. Migration Process: Source Node (Sender)

- [ ] 5.1 Gossip Listener: Detect when peer starts `importing` a partition we own
- [ ] 5.2 Transition Logic: Mark partition as `migrating`
- [ ] 5.3 Implement `StoreEngine::scan_partition` method for efficient iteration.
- [ ] 5.4 Implement "Finite Iterator" Scan:
    - [ ] Iterate all keys in partition using `scan_partition` (snapshot-like).
    - [ ] Batch keys and send to Target.
    - [ ] Do not loop for new keys (handled by Dual Write).
- [ ] 5.5 Trigger "Sync Complete" message when iterator finishes.
- [ ] 5.6 Cleanup: Drop `migrating` state and local data after Target confirms ownership.

## 6. Sync Protocol & Data Transfer

- [ ] 6.1 Define Sync API/Protocol (gRPC or internal HTTP endpoint)
- [ ] 6.2 Implement batching for efficient transfer
- [ ] 6.3 Implement retry logic for failed batches

## 7. Edge Cases & Recovery

- [ ] 7.1 Handle Target Node Disconnection: Source reverts `migrating` status
- [ ] 7.2 Handle Source Node Disconnection: Target force-claims (if data loss acceptable/replicated)
- [ ] 7.3 Handle Concurrent Resizes: Reject if any migration in progress

## 8. Testing

- [ ] 8.1 Unit Test: Topology calculation sets `importing`
- [ ] 8.2 Unit Test: Gossip update triggers `migrating` state on Source
- [ ] 8.3 Integration Test: Write to Source during migration (key not migrated) -> Success on Source
- [ ] 8.4 Integration Test: Write to Source during migration (key migrated) -> Forwarded to Target
- [ ] 8.5 Integration Test: Full migration flow (Start -> Sync -> Finish -> Topology Update)
