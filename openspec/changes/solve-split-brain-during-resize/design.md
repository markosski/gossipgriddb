## Context

The current `gossipgrid` implementation supports dynamic cluster resizing. However, when a new node joins and assumes leadership for partitions, there is a period of convergence where the old leader still holds data and serves requests while the new leader is technically responsible. This can lead to split-brain scenarios and data inconsistency if not handled carefully.

## Goals / Non-Goals

**Goals:**
- Eliminate split-brain scenarios during partition leadership transitions.
- **Maintain high write availability** during the transition (Live Migration).
- Ensure data consistency by forwarding requests for migrated keys to the new owner.
- Leverage the existing gossip protocol for coordination.

**Non-Goals:**
- Implementation of a centralized consensus protocol (e.g., Raft).
- Shared storage architecture (GossipGridDB is shared-nothing).
- Atomic multi-partition transactions across nodes.

## Decisions

### 1. Partition States
Nodes will track two new sets of partitions in their metadata:
- **`importing_partitions`** (Target Node): Partitions the node is currently receiving data for.
- **`migrating_partitions`** (Source Node): Partitions the node is currently sending data from.

### 2. Migration Handshake Protocol
The transition of leadership for partition $P$ from Node $A$ (Source) to Node $B$ (Target) follows this sequence:

1.  **Topology Calculation (Target)**: Node $B$ joins or resizes. It calculates that it *should* own partition $P$.
    -   $B$ adds $P$ to its `leading_partitions` (intent).
    -   $B$ adds $P$ to its `importing_partitions` (status).
    -   $B$ calculates topology but does *not* immediately serve writes as the sole authority until migration completes.
2.  **Acknowledgment (Source)**: Node $A$ (current leader of $P$) receives gossip from $B$.
    -   $A$ sees $B$ is importing $P$.
    -   $A$ marks $P$ as a `migrating_partition`.
    -   $A$ remains the *effective* leader for clients but enters "Migration Mode" for $P$.
3.  **Dual Write (Hot Standby)**:
    -   While in `migrating` state, the Source node **continues to accept writes**.
    -   It applies writes **locally** (updating its storage).
    -   It **concurrently forwards** the write to the Target node.
    -   This ensures that if the Target fails, the Source has the latest data and can seamlessly revert to exclusive ownership.
4.  **Background Sync (Finite Iterator)**:
    -   Source initiates a **Store Snapshot Scan** (iterating the in-memory map or storage engine directly), NOT the WAL.
    -   **Termination Condition**: The scan iterates the current key set **exactly once** (Finite Iterator). It does not retry or loop for new writes.
    -   New writes during the scan are handled by the Dual Write mechanism.
    -   Once the iterator finishes, the partition is fully synced.
5.  **Completion**:
    -   Source sends "Sync Complete" signal.
    -   Target promotes partition to "Owned", removes `importing`.
    -   Source sees Target is no longer `importing`, removes `migrating`, and drops ownership (deleting local data).

### 3. Request Routing (Smart Forwarding)
During the migration window (Phase 2 & 3), Node $A$ handles requests for $P$:

-   **Write Request**: Apply Dual Write (Local + Forward).
-   **Read Request**: Serve locally (since Source has full data until completion).

### 4. Safety & Failure Recovery (Revised with Replica C Analysis)

We evaluated two options for handling node failures during migration:
1.  **Option 1 (Dual Write - Chosen)**: Source handles all writes locally + forwards. Replica follows Source.
2.  **Option 2 (Dual Replication)**: Replica syncs from both Source and Target.

**Conclusion**: Option 1 is superior for simplicity and robustness. It ensures the Source (and its Replica) maintain a complete dataset until handoff, covering all single-node failure scenarios.

#### Scenario 1: Target Failure (Node B dies)
-   **Mechanism**: Source (A) writes locally, replicates to Replica (C), and forwards to Target (B).
-   **Failure**: Target (B) crashes mid-migration.
-   **Recovery**: Source (A) detects failure and cancels migration.
-   **Outcome**: Source (A) and Replica (C) both hold the complete dataset (Legacy + New Writes). **Zero Data Loss**.

#### Scenario 2: Source Failure (Node A dies)
-   **Mechanism**: Source (A) writes locally and replicates to Replica (C).
-   **Failure**: Source (A) crashes.
-   **Recovery**: Replica (C) detects failure and promotes itself to Leader of *Partition P*.
-   **Migration State**: Since C has full state (from A's replication stream), C simply restarts the migration to Target (B) from scratch (or resumes if state is persisted).
-   **Outcome**: Replica (C) holds the complete dataset. **Zero Data Loss**.

#### Scenario 3: Source & Replica Failure (Double Fault)
-   **Failure**: A and C both crash.
-   **Outcome**: Target (B) has partial data (New writes + Partial Backfill).
-   **Result**: Potential data loss (Legacy data lost). This is standard behavior for double-faults in single-replica systems.

### 5. Infinite Loop Prevention
-   **Finite Iterator Scan**: Migration time is proportional to dataset size, not traffic volume. The background sync iterates the snapshot once. New writes are handled by Dual Write.

### 3. Request Routing (Smart Forwarding)
During the migration window (Phase 2 & 3), Node $A$ handles requests for $P$:

-   **Write/Read Request for Key $K$**:
    -   $A$ checks if $K$ is in the "migrated set".
    -   **If Migrated**: Forward request to Node $B$.
    -   **If Not Migrated**: Process locally on Node $A$.
        -   *Note*: If a write occurs, $A$ must ensure it's synced to $B$ if the key was *just* migrated, or handle the race condition. Typically, if not migrated, write to $A$, then sync process picks it up (atomic check).

### 4. Dead Node Recovery
-   **Target Failure**: If $B$ disconnects while importing, $A$ sees this and reverts $P$ from `migrating` to normal ownership.
-   **Source Failure**: If $A$ disconnects while migrating, $B$ (and others) may need to treat $P$ as lost or rely on replicas. (Standard failure handling applies).

## Risks / Trade-offs

-   **Complexity**: Coordinating state between two nodes via gossip (eventual consistency) plus a direct sync channel is complex.
-   **Performance**: Forwarding requests adds latency.
-   **Memory**: Tracking "migrated keys" on Source requires memory (Bloom filter can mitigate, but has false positives; explicit set is exact but large).
    -   *Decision*: Use explicit tracking for now, or chunk-based tracking if possible.
-   **Race Conditions**: Handled by atomic local checks on Source.
