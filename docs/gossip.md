# Gossip Protocol: Cluster Membership

GossipGrid uses an epidemic gossip protocol over UDP to maintain and disseminate cluster membership information.

## 1. Overview

The gossip protocol's primary responsibility is ensuring every node has a reasonably up-to-date view of the cluster state, including:
-   **Node Health**: Detecting which nodes are active, syncing, or disconnected.
-   **Leadership Claims**: Propagating which nodes believe they are leading which partitions.
-   **HLC Synchronization**: Keeping Hybrid Logical Clocks loosely synchronized.

## 2. Gossip Mechanism

### Dissemination
Every N seconds (`GOSSIP_INTERVAL`), each node selects a small number of random peers and sends them a `GossipMembershipMessage`.

### Message Content
A `GossipMembershipMessage` contains:
-   The sender's own `SimpleNode` state (HLC, status, address).
-   The sender's current view of all other peers in the cluster.
-   **Leading Partitions**: The set of partitions the sender has elected itself to lead.

### Receiving Gossip
When a node receives a gossip message:
1.  **HLC Merge**: It merges its local HLC with the sender's HLC.
2.  **State Update**: It compares the received information for each node against its local cache. Updates are only applied if the incoming HLC for a node is strictly greater than the stored HLC ("Highest HLC Wins").
3.  **Conflict Resolution**: This mechanism handles delayed or out-of-order packets by ensuring the logical clock always moves forward.

## 3. Node Lifecycle & Discovery

### Joining
New nodes start in a `PreJoin` state and send a `NodeJoinRequest` to a known seed node. The seed responds with a `NodeJoinAck` containing the full cluster configuration.

### Failure Detection
If a node has not received any communication from a peer for 15 seconds (`FAILURE_TIMEOUT`), it marks that peer as `Disconnected`. This state is then disseminated via subsequent gossip cycles.

## 4. Separation of Concerns

It is important to note that **data synchronization is NOT handled by gossip**. Gossip only manages membership and sync coordination (e.g., node health states and leadership roles). The actual data transfer and Log Sequence Number (LSN) tracking happens over TCP via the `sync` module.
