## Context

Currently, the GossipGridDB HTTP API and Smart Client (`gossipgrid-client`) only support inserting items one at a time via a `POST /items/{partition_id}` endpoint. For use cases involving high throughput ingestion of logs, events, or sensor data, sending a separate HTTP request for each item introduces substantial network overhead, connection handling costs, and IO/consensus inefficiencies.

This design introduces a batch write mechanism to allow clients to send multiple items in a single HTTP request.

## Goals / Non-Goals

**Goals:**
- Provide a new HTTP API endpoint `POST /items` that accepts an array of item payloads.
- Update `gossipgrid/src/web/items.rs` to process an array of items.
- Extend `gossipgrid-client` with a `put_batch` method that intelligently routes batch requests.
- Ensure that batch insertion correctly triggers the underlying storage and replication mechanisms efficiently (batch IO and batched sync waiting where possible).
- Add integration benchmarks measuring batch write throughput.

**Non-Goals:**
- Cross-partition atomic transactions (all items in a batch must either succeed or fail as a single atomic unit across partitions). We will support batching for *efficiency*, but items routed to different partitions will be handled as independent sub-batches if the smart client splits them up, or handled on a best-effort basis if sent to a single API node. To keep it simple initially, the HTTP batch endpoint will execute the items, and if some fail, it will return a partial success/error response.

## Decisions

### 1. HTTP API Endpoint Design
**Decision:** Create a new `POST /items/batch` endpoint.
**Rationale:**
The existing `POST /items/{partition_id}` is heavily tied to a single partition key from the path. A batch request might contain items destined for *different* partitions. The payload for `POST /items/batch` will be an array of `ItemCreateUpdate` objects, which already include the `partition_key`, `range_key`, and `message`.

*Alternative considered:* Modifying `POST /items/{partition_id}` to accept an array. This forces all items in the batch to belong to the same partition, which is less flexible for the smart client and forces the client to group items by partition itself before sending.

### 2. Smart Client Routing
**Decision:** The `gossipgrid-client`'s `put_batch(items: Vec<ItemCreateUpdate>)` method will send the entire batch to any healthy node's `POST /items/batch` endpoint in a single request. The server-side handler is responsible for grouping items by leader and proxying remote sub-batches.
**Rationale:**
This keeps the client simple and avoids duplicating the partition-to-leader routing logic that already exists on the server. The server's `handle_post_item_batch` already groups items by leader, processes local items natively, and proxies remote sub-batches concurrently — so the client gains no correctness benefit from replicating that work. A single request also simplifies error handling and response aggregation on the client side. Client-side leader grouping remains a viable future optimization if proxy overhead becomes measurable.

### 3. Core Database Logic (`items.rs`)
**Decision:** Update `handle_post_item_batch` to process a `Vec<ItemCreateUpdate>`.
The HTTP `/items/batch` endpoint logic will group incoming items by their respective leader nodes.
- For items where the current node *is* the leader, it will prepare and commit them locally in a single IO batch (via `JoinedNode::insert_items_io_only`).
- For items where the current node is *not* the leader, it will act as a proxy, sending these sub-batches to the appropriate remote leaders concurrently.
- The handler will wait for all local insertions and all remote proxy responses to complete before assembling and returning a consolidated final response to the client.
**Rationale:**
This ensures the HTTP API provides a single cohesive response regardless of the cluster topology. `JoinedNode::insert_items_io_only` already handles vector batching, so local IO is extremely efficient.

## Risks / Trade-offs

- **[Risk] Partial Failures:** If a client sends a batch of 100 items spanning 3 partitions, and the node handling one of those partitions crashes during processing, the client will get a partial success.
  → **Mitigation:** The response envelopes need to clearly indicate which items succeeded and which failed. We will define an `ItemBatchResponseEnvelope` that can return lists of successful items and errors per item.

- **[Risk] Payload Size Limits:** A very large batch could exceed warp's default request body size limits, leading to HTTP 413 Payload Too Large errors.
  → **Mitigation:** We should document a recommended maximum batch size (e.g., 1000 items or 5MB) and ensure the warp filter is explicitly configured with a reasonable body size limit for the batch endpoint.

## Migration Plan

This is a fully backward-compatible change. Existing clients using single-item writes will continue to function normally. New clients can opt-in to use the new batch API.
