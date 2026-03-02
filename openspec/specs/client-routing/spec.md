## ADDED Requirements

### Requirement: Client Routing
The client SHALL route requests directly to the correct node based on the partition key without relying on intermediate proxy hops.

#### Scenario: Read operation routing
- **WHEN** a client performs a read operation (GET)
- **THEN** the request is routed directly to a known replica for that partition

#### Scenario: Write operation routing
- **WHEN** a client performs a write or delete operation
- **THEN** the request is routed directly to the known leader for that partition
