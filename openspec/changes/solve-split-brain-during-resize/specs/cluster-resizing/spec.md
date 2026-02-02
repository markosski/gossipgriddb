## ADDED Requirements

### Requirement: Locking Phase During Join
When a new node joins the cluster and claims leadership of existing partitions, it SHALL first add those partitions to its `locked_partitions` set. It SHALL NOT serve any writes for those partitions until it confirms that the previous leader has also marked them as `LOCKED`.

#### Scenario: New node joins and locks partitions
- **WHEN** a new node joins and identifies partitions to lead
- **THEN** it SHALL add them to `locked_partitions` immediately
- **AND** it SHALL NOT accept writes for those partitions
- **AND** it SHALL wait for acknowledgment (lock from previous leader) before opening

### Requirement: Lock Acknowledgment by Previous Leader
A node that receives gossip showing its owned partition is being locked by a new leader SHALL immediately stop serving writes and add that partition to its own `locked_partitions`.

#### Scenario: Previous leader acknowledges lock
- **WHEN** Node A (old leader) sees that Node B (new leader) has Partition 1 in `B.locked_partitions`
- **THEN** Node A SHALL cease all write operations for Partition 1
- **AND** Node A SHALL add Partition 1 to its own `locked_partitions` to signal acknowledgment

#### Scenario: New Leader Crashed Mid-Handshake
- **WHEN** Node B (new leader) locks P, but crashes before seeing Node A (old leader) acknowledgment
- **THEN** Node B will eventually be marked DISCONNECTED through gossip failure detection (~10 seconds)
- **AND** all nodes SHALL independently recalculate partition assignments using deterministic partition assignment logic (rendezvous hashing + circular distance priority)
- **AND IF** Node A regains leadership of P (no other viable candidate):
    - Node A SHALL remove P from `locked_partitions` and resume serving writes
- **AND IF** a new Node C is assigned leadership of P:
    - Node C SHALL initiate a fresh lock handshake with Node A
    - Node A SHALL acknowledge Node C's lock before C opens P for writes

### Requirement: Distributed Partition Reassignment
Partition leadership reassignment SHALL be performed independently by all nodes using deterministic algorithms. There SHALL be no single coordinator node.

#### Scenario: All nodes converge to same leadership view
- **WHEN** any node detects a topology change (node join, node disconnect, cluster resize)
- **THEN** that node SHALL run `assign_partition_leaders()` locally
- **AND** all other nodes SHALL independently run the same deterministic algorithm
- **AND** all nodes SHALL converge to the same partition leadership assignments (eventual consistency via gossip)
- **AND** the deterministic algorithm SHALL use rendezvous hashing with circular distance priority to ensure stability

### Requirement: Prevent Concurrent Resize Operations
A cluster resize operation SHALL be rejected if any partition lock handshake is currently in progress. This prevents cascading resize operations that could create complex multi-way handshake scenarios.

#### Scenario: Resize rejected when locks exist
- **WHEN** a resize operation is attempted
- **AND** any node in the cluster has non-empty `locked_partitions`
- **THEN** the resize SHALL be rejected with error "Cannot resize: partition leadership handshake in progress"
- **AND** the client MAY retry the resize after the handshake completes

#### Scenario: Resize succeeds when no locks exist
- **WHEN** a resize operation is attempted
- **AND** no node in the cluster has any locked partitions
- **THEN** the resize SHALL proceed normally
- **AND** new leadership assignments will trigger lock handshakes as needed