# Gossip & HLC: Cluster Membership and Linearizability

This document provides a technical overview of how `gossipgrid` maintains cluster membership and ensures consistency using a Gossip mechanism coupled with Hybrid Logical Clocks (HLC).

## 1. Gossip Mechanism

Gossip is the primary mechanism for disseminating cluster state across all nodes. It operates on a peer-to-peer basis, ensuring that state eventually converges even in the presence of network partitions or transient failures.

### Message Types
- **GossipJoinRequest**: Sent by a `PreJoin` node to a seed peer to request entry into the cluster.
- **GossipJoinRequestAck**: Sent by a `Joined` node to confirm the entry, providing the `Cluster` configuration.
- **GossipMembershipMessage**: Periodically exchanged between `Joined` nodes. It contains:
  - **The sender's `SimpleNode` state**: Includes its address, HLC, and its current state (Joined, Syncing, etc.).
  - **Leading Partitions**: Part of the `SimpleNode` state, this lists all partitions for which the sender believes it is the leader.
  - **`other_peers`**: The sender's current view of all other nodes in the cluster.

### Propagation
Nodes periodically select a random subset of peers to send a `GossipMembershipMessage`. This ensures exponential spread of information throughout the cluster.

### Failure Detection
If a node has not received gossip from a peer for more than N seconds (`FAILURE_TIMEOUT`), it marks that node as `Disconnected`. This change is subsequently gossiped to other nodes, eventually leading to the peer being unassigned from its partitions and new leadership being elected.

---

## 2. Hybrid Logical Clock (HLC)

`gossipgrid` uses HLC to provide a total ordering of events without requiring perfectly synchronized physical clocks across nodes.

### Structure
An `HLC` consists of two components:
- `timestamp`: integer representing the physical wall-clock time (milliseconds).
- `counter`: logical counter used to order events that occur within the same physical millisecond.

### Causal Ordering (Merge & Tick)
The HLC is updated whenever a node performs a local event or receives a message:
1. **Local Event**: The clock "ticks." If the wall clock has moved forward, the `timestamp` is updated and the `counter` is reset. If the wall clock is behind or equal, the `counter` is incremented.
2. **Receiving a Message**: The local HLC is merged with the incoming HLC. The new `timestamp` is `max(local.timestamp, remote.timestamp, wall_clock)`. The `counter` is incremented based on which timestamp was chosen to ensure the new HLC is strictly greater than both the local and remote clocks.

#### Circumstances for Ticking
In the current implementation, a node's HLC is ticked in the following scenarios:
- **Data Modification**: When a leader receives a `POST` or `DELETE` request, it ticks and updates the **node's HLC**, and assigns this new version to the `Item`.
- **Membership & State Transitions**: The HLC is ticked on **any** meaningful change to the node's state (e.g., `Syncing` → `Joined`, `Hydrating` → `Syncing`) or the broader cluster membership (node join/leave/failure).
- **Conflict Resolution**: When receiving gossip that results in a change to the local cluster view, the node ticks its clock to propagate this new state.

This ensures **Lamport Causal Ordering**: if event A happens-before event B, then `HLC(A) < HLC(B)`.

---

## 3. Linearizability & Consistency

While Gossip provides eventual consistency, the addition of HLC allows `gossipgrid` to reason about the order of updates and provide stronger guarantees for specific operations.

### Membership Consistency
HLC acts as the authority for state transitions. In `update_cluster_membership`:
- If a node receives information about itself from another node, it only updates its local state if the incoming HLC is newer.
- For information about third-party nodes, the "Highest HLC Wins" rule applies. This prevents old membership updates (e.g., a delayed gossip packet saying a node is "Joined" when it is actually "Disconnected") from regressing the cluster state.

### Linearizability through Total Ordering
Because HLCs can be compared globally, they provide a total order of all state-changing events in the cluster. This is crucial for:
- **Last-Write-Wins (LWW) Conflict Resolution**: When two nodes update the same item, the one with the higher HLC is preserved. Since HLC captures causal relationships, a "later" update will always have a higher HLC than a "prior" update it was aware of.
- **Leadership Election**: Leadership is derived from the current membership view. HLC ensures that all nodes eventually see the same sequence of membership changes, leading them to converge on the same leadership decisions.

### Leadership Stability
To prevent flapping, the system favors existing leaders during reelection. A node remains the leader of a partition as long as it is still marked as `Joined` and no healthier or better-positioned node (based on the scoring algorithm) is available. Leadership only changes when the current leader is marked as `Disconnected`, or when a node in a more preferred state (e.g., `Joined` vs `JoinedSyncing`) becomes available.

---

## Summary of HLC Benefits
- **No Global Clock Sync**: Tolerates clock skew between nodes while maintaining causal ordering.
- **Total Ordering**: Allows deterministic resolution of concurrent updates.
- **Monotonicity**: Ensures that a node's view of the world only moves forward in logical time.
