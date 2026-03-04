## Why

When data volume or request demand increases, the cluster needs to expand to accommodate the load. Conversely, hardware replacement or scaling down requires removing nodes. This change enables administrators to add or remove nodes in a controlled, deliberate manner without introducing any disruption to ongoing cluster operations.

## What Changes

- Implement mechanisms to safely join a new node to an existing, running cluster without downtime.
- Implement mechanisms to gracefully remove a node from the cluster without data loss or request dropping.
- Provide administrative tools or API endpoints to trigger deliberate scaling operations.
- Ensure that the addition or removal of nodes propagates correctly and update routing organically.

**Non-goals:**
- Auto-scaling is explicitly out of scope; scaling must be deliberate and controlled by an admin.

## Capabilities

### New Capabilities
- `cluster-resizing`: Administrative capabilities for adding or removing nodes from the cluster gracefully.

### Modified Capabilities
- `topology-discovery`: Must support deliberate node joining and graceful leaving, distinct from node failures.
- `client-routing`: Must adapt to topology changes smoothly, routing new requests to new nodes and avoiding nodes that are gracefully leaving.

## Impact

- Core membership and gossip protocols to broadcast and handle "join" and "leave" intents.
- Topology management components to reflect the current active state vs joining/leaving state.
- Data synchronization mechanisms to handle moving data to new nodes or redistributing from leaving nodes (if applicable to the data model).
- Administrative APIs or CLI interfaces for initiating cluster resizing operations.
