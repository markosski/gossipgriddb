## ADDED Requirements

### Requirement: Partition State Tracking
The system SHALL maintain a set of `LOCKED` partitions for each node. A partition is considered `LOCKED` if it is present in the node's advertised `locked_partitions` set. Otherwise, it is effectively `OPEN`.

#### Scenario: Partition defaults to OPEN
- **WHEN** a partition is newly created or initialized
- **THEN** it SHALL NOT be present in any node's `locked_partitions` set

### Requirement: Gossip of Partition Lock State
Nodes SHALL include their `locked_partitions` set in every gossip message to propagate lock status across the cluster.

#### Scenario: Propagating LOCKED state
- **WHEN** a node adds a partition to its `locked_partitions`
- **THEN** it SHALL include this ID in its `SimpleNode` metadata for subsequent gossip cycles

### Requirement: Transition Acknowledgment
A new leader SHALL only remove a partition from its `locked_partitions` (opening it for writes) after it receives gossip showing that the previous leader has also added that partition to its own `locked_partitions`, or if the previous leader is determined to be unreachable.

#### Scenario: Successful Lock Handshake
- **WHEN** Node B (new leader) has Partition 1 in its `locked_partitions`
- **AND** Node B receives gossip from Node A (previous leader) showing Partition 1 in `A.locked_partitions`
- **THEN** Node B SHALL remove Partition 1 from its `locked_partitions` and begin serving writes

#### Scenario: Force Unlock on Node Failure
- **WHEN** Node B (new leader) has Partition 1 in its `locked_partitions`
- **AND** the previous leader (Node A) is marked as `Disconnected` in Node B's cluster view
- **OR** there are no other healthy nodes in the cluster that are assigned to Partition 1
- **THEN** Node B SHALL remove Partition 1 from its `locked_partitions` and begin serving writes
