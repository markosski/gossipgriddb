## Context

The current `gossipgrid` implementation supports dynamic cluster resizing. However, when a new node joins and becomes a leader for partitions previously owned by another node, there is a period of convergence where both nodes may believe they are the leader. This "split-brain" window allows for inconsistent writes.

## Goals / Non-Goals

**Goals:**
- Eliminate the split-brain window during partition leadership transitions.
- Maintain read availability even when write operations are suspended.
- Leverage the existing gossip protocol for coordination.
- Ensure the new leader waits for old leader to acknowledge the lock before opening.

**Non-Goals:**
- Implementation of a centralized consensus protocol (e.g., Raft).
- Structural changes to the underlying storage engines.
- Atomic multi-partition locking.

## Decisions

### 1. Partition Lock Set in SimpleNode
Each node will advertise a `locked_partitions` set in its `SimpleNode` gossip data.
- **`locked_partitions`**: A set of Partition IDs that this node has currently suspended write operations for.

### 2. Symmetrical Lock Handshake
The transition of leadership for partition $P$ to Node $B$ (new leader) uses the `locked_partitions` set as a cluster-wide signal:

1. **Lock Initiation (B)**: Node $B$ identifies it is the new leader for $P$. It adds $P$ to its local `locked_partitions` and gossips this state. Node $B$ rejects writes for $P$.
2. **Acknowledgment (All Nodes)**: Every other node $N$ receives gossip showing `B.locked_partitions` contains $P$. 
   - Node $N$ immediately stops accepting writes for $P$ (if it previously considered itself a leader).
   - Node $N$ adds $P$ to its own local `locked_partitions` and gossips.
3. **Opening (B)**: Node $B$ receives gossip from all healthy nodes in the cluster showing they have $P$ in their `locked_partitions`.
   - Node $B$ now has proof that no other node will accept writes for $P$.
   - Node $B$ removes $P$ from its `locked_partitions` and begins serving writes.
4. **Cleanup (All Nodes)**: Other nodes see $P$ is no longer in `B.locked_partitions`. They remove $P$ from their own `locked_partitions`.

### 3. Dead Node Recovery (Force Unlock)
If Node $A$ (old leader) is marked as `Disconnected` in the gossip view, or if the cluster configuration indicates that Node $B$ is the only healthy candidate for leadership of $P$, Node $B$ can immediately transition $P$ to `OPEN`.
- **Rationale**: Prevents partitions from being stuck in `LOCKED` if the old leader crashes, or if Node $A$ has already gracefully surrendered leadership without a symmetrical lock (e.g., during a multi-step reconfiguration).

### 4. Distributed Partition Reassignment
All nodes independently run the same deterministic `assign_partition_leaders()` algorithm when topology changes occur (node join, disconnect, resize). There is no single coordinator node.
- **Deterministic Algorithm**: Uses rendezvous hashing with circular distance priority to ensure all nodes converge to the same leadership assignments.
- **Eventual Consistency**: Nodes converge through gossip propagation (~10 seconds for failure detection).

### 5. Concurrent Resize Prevention
To prevent complex multi-way handshake scenarios, `Cluster::resize()` will reject resize operations if any partition lock handshake is currently in progress.
- **Detection**: Check if any node has non-empty `locked_partitions` before proceeding with resize.
- **Error**: Return `ResizeInProgress` error to client, allowing retry after handshake completes.
- **Rationale**: Simplifies reasoning about handshake state and prevents cascading leadership changes.

## Risks / Trade-offs

- **[Risk] Partition stuck in LOCKED**: If Node $A$ is neither dead nor processing gossip correctly.
    - **Mitigation**: Standard gossip failure detection will eventually mark it disconnected (~10 seconds), triggering a force-unlock.
- **[Risk] Resize temporarily unavailable**: If locks exist during resize attempt.
    - **Mitigation**: Client can retry. Handshakes typically complete within 1-3 gossip rounds (seconds).
- **[Trade-off] Consistency vs Availability**: Write availability is suspended for the duration of the gossip round trip between $B \to A \to B$.

