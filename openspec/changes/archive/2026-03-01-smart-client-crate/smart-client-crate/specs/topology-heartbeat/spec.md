## ADDED Requirements

### Requirement: Topology Heartbeat
The client SHALL run a background task to periodically refresh the local topology snapshot via heartbeat requests to a cluster node.

#### Scenario: Periodic refresh
- **WHEN** the heartbeat interval elapses
- **THEN** the client polls a cluster node and updates its internal routing table with any changes

#### Scenario: Failure during refresh
- **WHEN** the chosen cluster node fails to respond to a heartbeat
- **THEN** the client tries another known node to refresh the topology before marking it as stale
