## ADDED Requirements

### Requirement: Topology Discovery
The smart client SHALL connect to a seed node on initialization to fetch the full cluster topology, including partition assignments, leader map, and node addresses.

#### Scenario: Successful initialization
- **WHEN** the client is initialized with a given seed node URL
- **THEN** it fetches the topology from the seed node and builds its internal routing table

#### Scenario: Seed node unavailable
- **WHEN** the client is initialized but the first seed node is unreachable
- **THEN** it falls back to the next available seed node to fetch the topology
