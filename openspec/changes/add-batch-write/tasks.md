## 1. HTTP API (Server)

- [ ] 1.1 Update `gossipgrid/src/web/items.rs` to define the new HTTP `POST /items/batch` route in warp.
- [ ] 1.2 Implement the `handle_post_item_batch` function in `gossipgrid/src/web/items.rs`.
- [ ] 1.3 Inside `handle_post_item_batch`, group the incoming `Vec<ItemCreateUpdate>` by target leader node based on the partition key.
- [ ] 1.4 Write logic to process the local sub-batch via `JoinedNode::insert_items_io_only` and proxy the remote sub-batches to other nodes concurrently.
- [ ] 1.5 Consolidate the results (successes and errors) from all sub-batches and return a unified `ItemBatchResponseEnvelope` or equivalent.

## 2. Smart Client (Client)

- [ ] 2.1 Update `gossipgrid-client/src/types.rs` (or equivalent) to include `ItemBatchResponseEnvelope` for parsing responses.
- [ ] 2.2 Add a `put_batch(items: Vec<ItemCreateUpdate>)` method in `gossipgrid-client/src/lib.rs`.
- [ ] 2.3 Implement routing logic in `put_batch` to group items by partition leader using the cached topology.
- [ ] 2.4 Execute concurrent HTTP `POST /items/batch` requests for each target leader.
- [ ] 2.5 Aggregate the responses and return them to the caller.

## 3. Testing & Benchmarking

- [ ] 3.1 Write an integration test in `gossipgrid/tests/int_tests_sync_wait.rs` or a new file to verify batch single-node writes.
- [ ] 3.2 Write an integration test using the `gossipgrid-client` to verify `put_batch` routes correctly in a multi-node cluster.
- [ ] 3.3 Create a new benchmark `gossipgrid-client/benches/batch_latency_bench.rs` (or update an existing one) to measure batch write performance compared to single-item writes.
