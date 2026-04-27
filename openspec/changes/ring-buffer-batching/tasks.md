## 1. Core Structures & Initialization

- [ ] 1.1 Define `WriteCommand` struct in `gossipgrid/src/node/mod.rs` or `state.rs` to wrap write requests and their `tokio::sync::oneshot::Sender` reply channels.
- [ ] 1.2 Initialize the `tokio::sync::mpsc::channel` (with a bounded capacity, e.g., 5,000) inside `start_node` and pass the sender to the web server state.

## 2. Batch Writer Task Implementation

- [ ] 2.1 Create the `batch_writer_task` async function that consumes `WriteCommand`s from the MPSC receiver.
- [ ] 2.2 Implement the dynamic batching loop: block on the first message, then `try_recv()` to drain all immediately available messages up to the batch limit.
- [ ] 2.3 Move the bulk HLC calculation, `insert_items_io_only` IO execution, and oneshot sender dispatch logic into the `batch_writer_task`.

## 3. Web Handler Refactoring

- [ ] 3.1 Refactor `handle_post_item` in `gossipgrid/src/web/items.rs` to construct a `WriteCommand`, push it to the MPSC channel, and await the oneshot reply.
- [ ] 3.2 Refactor `handle_post_item_batch` similarly to enqueue its items and await completion without blocking HTTP threads on IO.

## 4. Replication and Testing

- [ ] 4.1 Address `wait_for_sync` blocking: determine if it should remain synchronous in the Writer task or be offloaded to a separate waiter task.
- [ ] 4.2 Run unit tests and `cargo clippy` to ensure no regressions.
- [ ] 4.3 Execute integration tests (`RUST_LOG=info cargo test --test 'int_tests_*' -- --test-threads=4 --nocapture`) to validate throughput, durability, and cluster consensus stability with the new batching engine.
