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
