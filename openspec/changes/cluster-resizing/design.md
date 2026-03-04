## Context

GossipGrid currently supports a fixed-size cluster topology. On startup, `Cluster::new` takes a `cluster_size` and pre-computes partition assignments using a spread or rendezvous strategy. Nodes join via the `join_peer` gossip handshake (`NodeJoinRequest` → `NodeJoinAck`), which assigns them to the next unassigned slot via `assign_next()`. The cluster cannot grow or shrink at runtime — `cluster_size` is immutable after creation.

For graceful removal, there is no explicit "leave" protocol. When a node stops, it is eventually marked `Disconnected` via heartbeat failure detection (`FAILURE_TIMEOUT = 15s`). During this detection window, requests routed to the dead node will fail.

## Goals / Non-Goals

**Goals:**
- Implement a graceful "leave" mechanism so a departing node proactively notifies the cluster, enabling immediate routing updates without waiting for failure detection.
- Implement a graceful "join" mechanism to allow adding a node to the cluster that will take on leadership of existing partitions from other nodes and become a new replica for other partitions.
- Provide an administrative HTTP endpoint to trigger graceful leave (and, optionally, to initiate a resize for join).
- Ensure topology discovery and client routing adapt immediately to both join and leave transitions.
- No disruption to in-flight requests during either operation.

**Non-Goals:**
- Automated or metric-based auto-scaling.
- Complex data rebalancing beyond what the existing replication/sync mechanism provides.
- Changing the partition count at runtime (only node count changes).

## Decisions

### Decision 1: Dynamic `cluster_size` and Partition Reassignment on Join

**Choice:** When a new node joins, increment `cluster_size` and recompute partition assignments using the **rendezvous hashing** strategy (`generate_partition_assignments_rendezvous`). The new node gets assigned a fresh set of partitions and enters the standard `JoinedHydrating` → `JoinedSyncing` → `Joined` lifecycle.

**Rationale:** Rendezvous hashing assigns each partition by ranking all nodes via a per-partition hash. Adding node N+1 only displaces ~`1/(N+1)` of partitions — the theoretical minimum. This is critical for non-disruptive scaling. The current spread strategy (`(partition_id + r) % cluster_size`) reshuffles nearly every assignment on resize because the modulo changes globally.

**Alternatives Considered:**
- Spread strategy (current default) — rejected for resize because it causes ~78% partition movement when adding a node.
- Manual partition assignment by admin — rejected for complexity and error-proneness.
- Consistent hashing ring — would require a redesign of the partition model; over-engineering for the current system.

**Key concern:** Even with minimal movement, some partitions do migrate to the new node. Old assignments must be retained until the new node has fully synced the migrated partitions.

### Decision 2: Graceful Leave via Gossip Message

**Choice:** Introduce a `Leaving` node state and a corresponding gossip message. When a node receives an admin "leave" command, it transitions to `Leaving`, broadcasts this via gossip, and enters a drain period before shutting down.

**Rationale:** Proactive notification allows other nodes to immediately stop routing to the leaving node and begin leader re-election for its partitions. This eliminates the 15-second failure detection window.

**Alternatives Considered:**
- Relying on existing failure detection — rejected because of the unavailability window.

### Decision 3: Node State Machine Extensions

**Choice:** Extend `SimpleNodeState` and `AssignmentState` with new states:

- `SimpleNodeState::Leaving` — node has signaled intent to leave.
- `AssignmentState::Leaving { node }` — assignment-level state for a leaving node.

When a node is `Leaving`:
- It stops accepting new write requests.
- It continues serving reads until drain completes.
- It continues participating in gossip to propagate its `Leaving` status.
- After a configurable drain period, it fully shuts down.
- Other nodes immediately re-elect leaders for partitions previously led by the leaving node.

### Decision 4: Administrative API

**Choice:** Expose HTTP endpoints on each node:

- `POST /admin/cluster/leave` — triggers graceful leave for the receiving node.
- `POST /admin/cluster/resize` — (future) triggers a cluster resize to add a new node slot. A new node process started with `join_peer` would then fill this slot.

**Rationale:** HTTP is simple and integrates well with orchestration tools. The leave endpoint is node-local (you call it on the node that should leave). The resize/join flow is a two-step process: first resize the cluster config, then start a new node process.

## Risks / Trade-offs

- **[Risk] Partition reassignment disruption during join** → **Mitigation:** Use a two-phase approach: compute new assignments but keep old assignments active until the new node has synced. Only switch leadership after sync completes.
- **[Risk] Inconsistent cluster_size across nodes during resize** → **Mitigation:** The resize event (new `cluster_size`) propagates via gossip alongside the membership update. Nodes that haven't received the update yet continue operating with old assignments — the gossip protocol's HLC-based convergence ensures eventual consistency.
- **[Risk] Data loss on leave** → **Mitigation:** Flush all pending WAL syncs and in-flight replication acknowledgments before final shutdown.
- **[Risk] Split-brain during leave** → **Mitigation:** The leaving node continues gossiping its `Leaving` state for a brief drain period to ensure propagation before terminating.

## Open Questions

- Should the graceful leave require acknowledgment from a quorum before the node is allowed to shut down, or is a best-effort broadcast sufficient?
- When recomputing partition assignments after a resize, should we minimize partition movement (incremental rebalancing) or accept the full recomputation from the spread strategy?
- Should the resize operation be initiated from any node (cluster-wide), or only from the seed / a designated coordinator?
