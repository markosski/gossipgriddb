## ADDED Requirements

### Requirement: Topology Endpoint
The server SHALL expose an HTTP endpoint that returns the current ClusterMetadata, partition assignments, and leader map.

#### Scenario: Client requests topology
- **WHEN** a client makes a `GET` request to `/cluster/topology`
- **THEN** the server responds with a JSON representation of the current full cluster topology
