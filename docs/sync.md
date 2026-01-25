# Leadership and Data Synchronization

GossipGrid uses a decentralized leadership election mechanism and a pull-based synchronization protocol to ensure data consistency across the cluster.

## 1. Partition Leader Election

Leadership is determined per partition using a deterministic, state-aware priority system. Every node independently calculates leaders based on its current view of the cluster membership.

### Election Process
1.  **Ideal Node Calculation**: For each partition, an "ideal" node index is calculated as `partition_id % cluster_size`.
2.  **Scoring**: Each candidate node is assigned a composite score:
    -   **State Rank (Major Factor)**: Nodes in `Joined` state have higher priority (rank 0) than nodes in `JoinedSyncing` state (rank 1).
    -   **Proximity (Minor Factor)**: Circular distance from the candidate's node index to the "ideal" node index.
3.  **Selection**: The node with the lowest composite score becomes the leader.

### Stability and Transitions
-   **Seeding**: Existing leaders are given preference in Stage 1 of election to provide stability.
-   **State Promotion**: As nodes transition from `Syncing` to `Joined`, they may take over leadership if they have a better proximity score.
-   - **Write Safety**: While "paper leadership" may be held by a `Syncing` node to prevent deadlocks, the API rejects writes if the leader is not yet fully `Joined`.

## 2. Pull-Based Data Synchronization

Replicas are responsible for staying up-to-date with their partition leaders via a TCP-based synchronization protocol.

### Synchronization Flow
1.  **Request**: Replicas periodically poll their partition leaders for new data using `SyncRequest`. This request includes the last known Log Sequence Number (LSN) and file offset for each partition.
2.  **Stream**: The leader streams missing Write-Ahead Log (WAL) records from its local storage back to the replica.
3.  **Apply**: The replica appends these records to its own WAL and applies them to its local state.
4.  **Acknowledgement**: The replica sends a `SyncResponseAck` with its new LSN. The leader uses this to track replication progress.

### LSN Waiters
When a client performs a write, the leader can wait for replicas to catch up to a specific LSN (if requested) before acknowledging the write to the client, providing tunable consistency levels.