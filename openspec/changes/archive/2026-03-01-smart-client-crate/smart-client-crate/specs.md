# Specifications: Smart Client Crate

## ADDED Requirements

### Requirement: Topology Discovery
The client SHALL connect to a seed node and fetch the full cluster topology on initialization.

#### Scenario: Initial connection
- **WHEN** the client is created with one or more seed node addresses
- **THEN** the client sends `GET /cluster/topology` to a seed node and receives the partition map, leader map, and node addresses.

#### Scenario: Seed node unavailable
- **WHEN** the primary seed node is unreachable
- **THEN** the client tries the next seed node in the list until one responds or all have been exhausted, at which point a connection error is returned.

---

### Requirement: Topology Endpoint
Each node SHALL expose an HTTP endpoint that returns the current cluster topology for client consumption.

#### Scenario: Topology request
- **WHEN** a `GET /cluster/topology` request is received
- **THEN** the node responds with a JSON body containing: `cluster_name`, `cluster_size`, `partition_count`, `replication_factor`, node list with addresses and web ports, partition assignments with states, and the partition leader map.

---

### Requirement: Client-Side Routing
The client SHALL hash partition keys and route requests directly to the correct node without server-side proxying.

#### Scenario: Write request routing
- **WHEN** the client submits a write or delete for a key
- **THEN** the client hashes the key using xxHash64 to determine the partition, looks up the leader for that partition, and sends the HTTP request directly to the leader node's web port.

#### Scenario: Read request routing
- **WHEN** the client submits a read for a key
- **THEN** the client hashes the key, and sends the request to any healthy replica that holds the partition (preferring replicas over leader to distribute load), falling back to the leader if no replica is available.

#### Scenario: Hashing consistency
- **GIVEN** the client uses `xxHash64` with seed `0` and computes `hash % partition_count`
- **THEN** the resulting partition ID is identical to the server's `Cluster::hash_key_inner` output for the same key.

---

### Requirement: Topology Heartbeat
The client SHALL maintain an up-to-date view of the cluster topology via periodic background refreshes.

#### Scenario: Periodic refresh
- **WHEN** the heartbeat interval elapses (default: 3 seconds)
- **THEN** the client sends `GET /cluster/topology` to a known healthy node and updates its local routing table with any changes to partition assignments or leaders.

#### Scenario: Heartbeat target failover
- **WHEN** the node used for the heartbeat request is unreachable
- **THEN** the client tries other known nodes from its topology cache until one responds, or marks the topology as stale if all nodes are unreachable.

---

### Requirement: Error Handling and Failover
The client SHALL handle node failures gracefully, with different strategies for reads vs. writes.

#### Scenario: Leader down during read
- **WHEN** a read request targets a partition whose leader is down
- **THEN** the client routes the read to a healthy replica for that partition.

#### Scenario: Leader down during write
- **WHEN** a write or delete targets a partition whose leader is unreachable or no leader is elected
- **THEN** the client returns a `LeaderUnavailable` error with the partition ID, indicating the caller should retry after the cluster re-elects a leader.

#### Scenario: Request retry on transient failure
- **WHEN** a request to the target node fails with a connection error
- **THEN** the client triggers an immediate topology refresh before retrying, and routes to the updated target if the topology has changed.
