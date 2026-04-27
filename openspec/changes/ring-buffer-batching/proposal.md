## Why

Currently, when handling write requests (`POST /items` or `POST /items/batch`), each HTTP worker thread independently takes read locks on the HLC, appends to the Write-Ahead Log (WAL), and writes to the data store. Under high load, this causes massive lock contention and thread context-switching overhead, creating a bottleneck that severely limits maximum throughput and increases tail latency. Transitioning to a ring buffer-based batching engine (Group Commit pattern) solves this by decoupling the HTTP threads from the IO path, enabling lock-free request enqueueing and single-threaded bulk IO processing, which significantly boosts overall system performance.

## What Changes

- Introduce a high-performance Multi-Producer Single-Consumer (MPSC) Ring Buffer.
- Refactor write handlers to push write requests (Command structs with `oneshot` reply channels) into the ring buffer instead of executing IO directly.
- Add a dedicated background consumer task (the "Writer") that pulls batches of requests from the ring buffer.
- The consumer will process HLC ticking, memory store insertion, and WAL batch appends sequentially for the entire batch in one bulk operation, then signal completion to all waiting HTTP requests.

## Capabilities

### New Capabilities

- `write-engine`: Internal architectural capability describing the new decoupled lock-free ring buffer write engine, ensuring strict serializability of writes while maximizing batch IO throughput.

### Modified Capabilities

(None. Client-facing API and `batch-write` requirements remain completely unchanged; this is an internal architectural enhancement.)

## Impact

- **Affected Code**: 
  - `gossipgrid/src/web/items.rs` (write-related endpoints will become lightweight producers).
  - `gossipgrid/src/node/state.rs` (IO insertion logic will be moved to the new consumer thread).
  - `gossipgrid/src/node/mod.rs` (initialization of the ring buffer and spawning the consumer task).
- **APIs**: Client-facing HTTP APIs remain unchanged. Internal routing may adjust slightly to accommodate the channel interface.
- **Dependencies**: May require adding a new dependency for the lock-free ring buffer (e.g., `crossbeam-queue` or `flume`), depending on benchmarking `tokio::sync::mpsc`.
