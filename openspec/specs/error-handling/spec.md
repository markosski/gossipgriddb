## ADDED Requirements

### Requirement: Error Handling for Routing
The client SHALL gracefully handle node failures by falling back to replicas for reads and returning typed errors for writes when the leader is unavailable.

#### Scenario: Leader unavailable during write
- **WHEN** a client attempts to write but the known leader is unavailable
- **THEN** the client returns a typed error rather than routing the write to a non-leader replica

#### Scenario: Replica failure during read
- **WHEN** a client attempts to read from a replica and it fails
- **THEN** the client falls back to another replica or the leader to complete the read
