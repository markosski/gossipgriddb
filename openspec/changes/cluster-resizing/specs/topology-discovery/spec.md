## MODIFIED Requirements

### Requirement: Topology Discovery
The smart client SHALL connect to a seed node on initialization to fetch the full cluster topology, including partition assignments, leader map, node addresses, and node states (including `Leaving` nodes).

#### Scenario: Successful initialization
- **WHEN** the client is initialized with a given seed node URL
- **THEN** it fetches the topology from the seed node and builds its internal routing table

#### Scenario: Seed node unavailable
- **WHEN** the client is initialized but the first seed node is unreachable
- **THEN** it falls back to the next available seed node to fetch the topology

#### Scenario: Topology update on node join
- **WHEN** a new node joins the cluster and partition assignments are recomputed
- **THEN** the topology discovery mechanism MUST propagate the updated assignments to all nodes via gossip, including the new cluster size

#### Scenario: Topology update on node leave
- **WHEN** a node transitions to the `Leaving` state
- **THEN** the topology discovery mechanism MUST propagate the `Leaving` status and trigger routing table updates across the cluster without waiting for failure detection

#### Scenario: Distinguishing leave from failure
- **WHEN** a node is in the `Leaving` state
- **THEN** the topology MUST distinguish this from a `Disconnected` node so that the cluster can react immediately rather than waiting for the failure detection timeout

## ADDED Requirements

### Requirement: Cluster Size Propagation
The topology discovery mechanism SHALL propagate cluster size changes via gossip alongside membership updates when a node joins or leaves.

#### Scenario: Cluster size incremented on join
- **WHEN** a new node joins the cluster
- **THEN** the updated cluster size MUST be propagated to all nodes via gossip, and nodes that receive the update MUST recompute their local partition assignment tables

#### Scenario: Convergence during resize
- **WHEN** the cluster size changes and not all nodes have received the update yet
- **THEN** nodes with stale cluster size MUST continue operating with old assignments, and HLC-based gossip convergence MUST ensure all nodes eventually receive the updated size
