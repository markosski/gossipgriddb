## Why

Currently, GossipGridDB supports writing individual items one at a time. This results in significant network overhead and latency when clients need to ingest large amounts of data. Introducing a batch write capability will allow clients to submit multiple items in a single request, drastically improving write throughput and overall ingestion performance.

## What Changes

- Add a new HTTP API endpoint to accept an array of items for batch ingestion.
- Enhance the Smart Client (`gossipgrid-client`) to provide a `put_batch` or similar batch application method.
- Update internal routing and storage logic to handle atomic or independent batch insertions, potentially routing different items to different partitions or routing the entire batch to the respective leaders.
- Update benchmarks or add new benchmarks to measure batch write performance.

## Capabilities

### New Capabilities
- `batch-write`: Defines the HTTP API and Smart Client interface for accepting and processing multiple items in a single request, along with expected routing and consensus behavior.

### Modified Capabilities

## Impact

- **HTTP API**: New batch endpoint will be exposed (e.g., `/items/batch` or similar, or modifying `/items/{partition_id}` to take an array).
- **Core Database Logic**: `gossipgrid/src/web/items.rs` and related item-handling modules will be updated to process multiple items efficiently.
- **Smart Client**: `gossipgrid-client/src/lib.rs` will gain a new method for batching.
- **Dependencies**: No new external dependencies are anticipated.
