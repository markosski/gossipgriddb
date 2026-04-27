## Context

GossipGridDB currently handles write requests (`POST /items` and `POST /items/batch`) directly in the HTTP worker threads. Each HTTP request independently takes locks to compute the Hybrid Logical Clock (HLC) and sequentially writes to both the Write-Ahead Log (WAL) and the in-memory store. Under high load, this introduces significant lock contention, context switching overhead, and suboptimal disk IO, severely capping maximum throughput.

The proposed solution decouples the HTTP threads from the IO path using a lock-free Multi-Producer Single-Consumer (MPSC) Ring Buffer (Group Commit architecture). Web workers simply enqueue requests and sleep on a `oneshot` channel, while a single background task (the "Writer") drains the buffer, batches operations, and performs bulk IO.

## Goals / Non-Goals

**Goals:**
- Eliminate lock contention on the HLC, WAL, and Store for write operations.
- Batch disk IO flushes to maximize NVMe/SSD throughput.
- Drastically improve maximum write throughput (ops/sec).
- Reduce median and tail latency under heavy concurrent load.

**Non-Goals:**
- Do not change client-facing HTTP APIs.
- Do not change the overall cluster consensus or gossip mechanisms.
- Do not re-architect read operations (GET requests).

## Decisions

**Decision 1: Queue Implementation**
We will evaluate and select an MPSC queue for the ring buffer.
- *Alternative A: `tokio::sync::mpsc::channel`* - Standard, bounded, and async-native. We will start here.
- *Alternative B: `flume`* - Multi-producer, multi-consumer channel that often beats standard channels in high contention.
- *Alternative C: `crossbeam-queue::ArrayQueue`* - Extremely fast lock-free bounded queue, but requires managing thread waking/polling manually since it's not async-native.
- *Rationale:* We will default to `tokio::sync::mpsc` (bounded) initially to simplify async integration. It uses an internal linked-list ring structure. If profiling reveals it as a bottleneck, we will migrate to `flume`.

**Decision 2: The Command Envelope**
The ring buffer will carry a `WriteCommand` struct containing:
- The items to insert (e.g., `Vec<ItemEntry>`).
- A `tokio::sync::oneshot::Sender<Result<...>>` to respond to the HTTP task once the write (and optionally replication) is complete.

**Decision 3: The Writer Task Lifecycle (Dynamic Batching)**
- A new Tokio task will be spawned in `start_node` (e.g., `batch_writer_task`).
- It will block on `receiver.recv().await` to get the first command.
- Once awake, it will loop `receiver.try_recv()` to drain all immediately available items up to a hard batch limit (e.g., 5,000 items).
- It performs bulk HLC ticking, calls `insert_items_io_only` on the full batch, and then fires all oneshot senders to wake up the HTTP threads.

## Risks / Trade-offs

- **Memory Pressure from Enqueueing** → *Mitigation*: The MPSC channel will have a strict capacity limit. If full, the HTTP handlers will suspend and await capacity, providing natural backpressure to clients.
- **Replication Blocking Writer** → *Risk*: If the Writer task awaits `wait_for_sync` (replication acknowledgment) before processing the next batch, a slow replica could stall the entire node's write throughput.
  - *Mitigation/Trade-off*: Initially, we may keep the `wait_for_sync` step inside the Writer for simplicity, or immediately dispatch the wait to a separate replication coordination task so the Writer can continue local IO.

## Migration Plan

1. Define the `WriteCommand` and setup the MPSC channel in `start_node`.
2. Implement the `batch_writer_task` to consume from the channel.
3. Refactor `handle_post_item` and `handle_post_item_batch` to construct the command, send it over the channel, and await the oneshot receiver instead of executing IO.
4. Validate against all integration tests to ensure data durability and replication semantics are preserved.

## Open Questions

- Should the `wait_for_sync` replication step remain synchronously awaited by the Writer task, or should the Writer pass the oneshot sender to a secondary "Replication Waiter" task to allow the Writer to immediately process the next local IO batch?
