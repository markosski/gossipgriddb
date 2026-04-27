## ADDED Requirements

### Requirement: Lock-Free Enqueueing of Write Requests
The system SHALL accept write requests via HTTP handlers by constructing a `WriteCommand` payload containing the items and a oneshot reply channel, and pushing it onto a Multi-Producer Single-Consumer (MPSC) bounded ring buffer channel. The HTTP handler MUST yield execution and await the oneshot channel response instead of executing direct synchronous IO.

#### Scenario: Submitting a single item write
- **WHEN** a client performs a `POST /items` request
- **THEN** the web handler pushes the payload to the ring buffer, suspends execution, and only returns an HTTP response once the oneshot channel receives the completion result (which depends on configured replication settings)

#### Scenario: Ring buffer reaches maximum capacity
- **WHEN** the volume of incoming write requests exceeds the configured MPSC capacity
- **THEN** incoming web handlers await available capacity (backpressure) without dropping requests or panicking

### Requirement: Dynamic Batch IO Processing
The system SHALL spawn a single dedicated background task (the "Writer") that consumes commands from the MPSC ring buffer. The Writer MUST dynamically drain the queue up to a maximum batch size and execute the entire batch as a single sequential transaction, calculating bulk HLC ticks, appending the batch to the WAL in one operation, inserting into the memory store, and finally responding to all oneshot channels.

#### Scenario: Writer processes a batch under high load
- **WHEN** the ring buffer contains multiple pending `WriteCommand`s
- **THEN** the Writer task drains the available items into a single contiguous batch, executes exactly one bulk `wal.append_batch` operation, inserts them sequentially into the store, and dispatches completion signals to all corresponding oneshot channels simultaneously
