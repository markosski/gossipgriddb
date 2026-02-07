## Why

Cluster resizing currently suffers from a "split-brain" window where ownership is ambiguous. The previous "Locking" proposal solved this but introduced downtime (503 errors). This new proposal introduces **Live Partition Migration** to ensure zero-downtime resizing by dynamically forwarding requests during data transfer.

## What Changes

- **New Node States**:
    - `importing_partitions` (on new owner)
    - `migrating_partitions` (on old owner)
- **Live Migration Protocol**:
    - Old owner keeps serving requests while data creates background.
    - Requests for *already migrated keys* are forwarded to the new owner.
    - Requests for *not-yet-migrated keys* are served locally.
- **Topological Handshake**:
    - Nodes use gossip to coordinate the start and end of migration.

## Capabilities

### New Capabilities
- `live-migration`: Seamless data movement between nodes.
- `request-forwarding`: Internal proxying or redirection for migrated keys.

### Modified Capabilities
- `cluster-resizing`: Now triggers migration instead of instant switch.

## Impact

- **Storage Layer**: Support for iterating keys and marking migration status.
- **Network**: New bandwidth usage for sync; increased latency for forwarded requests.
- **Availability**: Writes remain available throughout the process.
