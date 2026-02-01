## ADDED Requirements

### Requirement: Write Rejection on Locked Partition
The system SHALL reject any write requests for a partition that is in the `LOCKED` state.

#### Scenario: Write to locked partition
- **WHEN** a client sends a write request for Partition 1
- **AND** Partition 1 is in the `LOCKED` state on the target node
- **THEN** the system SHALL return a `503 Service Unavailable` error (or equivalent "Partition Locked" error)
- **AND** the write SHALL NOT be persisted

### Requirement: Read Availability During Lock
The system SHALL continue to allow read requests for a partition even when it is in the `LOCKED` state.

#### Scenario: Read from locked partition
- **WHEN** a client sends a read request for Partition 1
- **AND** Partition 1 is in the `LOCKED` state
- **THEN** the system SHALL return the most recently available data for that partition
