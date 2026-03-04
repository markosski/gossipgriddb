## ADDED Requirements

### Requirement: Graceful Node Join
The system SHALL allow a new node to join a running cluster without disrupting ongoing operations. When a new node joins, the cluster MUST increment its cluster size and recompute partition assignments using rendezvous hashing to minimize partition movement.

#### Scenario: New node joins an active cluster
- **WHEN** a new node process starts and initiates a `join_peer` handshake with an existing cluster member
- **THEN** the cluster increments its cluster size, computes new partition assignments via rendezvous hashing, and the new node enters the `JoinedHydrating` → `JoinedSyncing` → `Joined` lifecycle

#### Scenario: Partition continuity during join
- **WHEN** a new node joins and partition assignments are recomputed
- **THEN** old partition assignments MUST remain active and serve requests until the new node has fully synced the migrated partitions

#### Scenario: Minimal partition disruption on join
- **WHEN** a node is added to a cluster of N nodes
- **THEN** approximately 1/(N+1) of partitions are reassigned to the new node, and all other partition assignments remain unchanged

### Requirement: Graceful Node Leave
The system SHALL allow a node to leave the cluster gracefully by proactively notifying other nodes, eliminating the failure detection delay window. The leaving node MUST transition through a `Leaving` state before shutdown.

#### Scenario: Admin-initiated graceful leave
- **WHEN** an administrator sends a leave command to a node via `POST /admin/cluster/leave`
- **THEN** the node transitions to `Leaving` state, broadcasts its intent via gossip, and enters a drain period before shutting down

#### Scenario: Write rejection during leave
- **WHEN** a node is in the `Leaving` state
- **THEN** it MUST stop accepting new write requests while continuing to serve read requests until the drain period completes

#### Scenario: Gossip propagation during leave
- **WHEN** a node is in the `Leaving` state
- **THEN** it MUST continue participating in gossip to propagate its `Leaving` status to all cluster members before terminating

#### Scenario: Leader re-election on leave
- **WHEN** a node transitions to `Leaving`
- **THEN** other nodes MUST immediately begin leader re-election for partitions previously led by the leaving node, without waiting for the failure detection timeout

#### Scenario: Data safety on leave
- **WHEN** a node is about to shut down after drain
- **THEN** it MUST flush all pending WAL syncs and in-flight replication acknowledgments before final shutdown

### Requirement: Administrative Cluster Resize API
The system SHALL expose HTTP endpoints on each node for administrators to trigger controlled scaling operations.

#### Scenario: Leave endpoint
- **WHEN** an administrator sends `POST /admin/cluster/leave` to a node
- **THEN** the receiving node initiates its graceful leave sequence

#### Scenario: Leave endpoint response
- **WHEN** a graceful leave is successfully initiated
- **THEN** the endpoint MUST return an acknowledgment with the node's updated state and the expected drain duration

### Requirement: Node State Extensions for Resizing
The system SHALL extend the node state machine to support `Leaving` states at both the node level and assignment level.

#### Scenario: Node-level Leaving state
- **WHEN** a node signals intent to leave
- **THEN** its `SimpleNodeState` MUST transition to `Leaving` and this state MUST be visible to all cluster participants via gossip

#### Scenario: Assignment-level Leaving state
- **WHEN** a node is in the `Leaving` state
- **THEN** all of its partition assignments MUST transition to `AssignmentState::Leaving { node }` so that other nodes can distinguish leaving assignments from active ones
