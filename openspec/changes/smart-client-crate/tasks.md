# Tasks: Smart Client Crate

## 1. Server-Side: Topology Endpoint

- [ ] 1.1 Add a `handle_get_topology` handler in `gossipgrid/src/web/` that reads `NodeState` and returns cluster topology as JSON (nodes, partition assignments, partition leaders, cluster metadata).
- [ ] 1.2 Add a `TopologyResponse` struct (with `Serialize`) capturing the response shape: `cluster_name`, `cluster_size`, `partition_count`, `replication_factor`, `nodes`, `partition_assignments`, `partition_leaders`.
- [ ] 1.3 Register `GET /cluster/topology` route in `web/server.rs`.

## 2. Client Crate Scaffolding

- [ ] 2.1 Create `gossipgrid-client/` crate directory with `Cargo.toml` (deps: `reqwest`, `tokio`, `twox-hash`, `serde`, `serde_json`, `thiserror`).
- [ ] 2.2 Add `gossipgrid-client` to the workspace `Cargo.toml` members.
- [ ] 2.3 Create module files: `lib.rs`, `topology.rs`, `routing.rs`, `error.rs`.

## 3. Error Types

- [ ] 3.1 Define `ClientError` enum in `error.rs`: `LeaderUnavailable`, `NoHealthyNodes`, `ConnectionFailed`, `TopologyStale`, `ServerError`.

## 4. Topology Discovery & Snapshot

- [ ] 4.1 Define `TopologySnapshot` struct in `topology.rs`: `partition_count`, `nodes` map, `partition_assignments`, `partition_leaders`.
- [ ] 4.2 Define `NodeInfo` struct: `address`, `web_port`, `state`.
- [ ] 4.3 Implement `TopologySnapshot::fetch(seed_url) -> Result<Self, ClientError>` that calls `GET /cluster/topology` and deserializes the response.
- [ ] 4.4 Implement seed node fallback: try each seed until one succeeds or return `NoHealthyNodes`.

## 5. Client-Side Routing

- [ ] 5.1 Implement `hash_key(partition_count, key) -> u16` in `routing.rs` using `twox-hash::XxHash64` with seed 0, matching server's `Cluster::hash_key_inner`.
- [ ] 5.2 Implement `resolve_target(snapshot, key, operation) -> Result<(String, u16), ClientError>` that returns `(address, web_port)` for the target node.
- [ ] 5.3 Route reads to any `Joined` replica holding the partition, falling back to leader.
- [ ] 5.4 Route writes/deletes to leader only; return `LeaderUnavailable` if leader is not `Joined`.

## 6. Background Heartbeat

- [ ] 6.1 Implement a `tokio::spawn` heartbeat task that periodically calls `TopologySnapshot::fetch` and swaps the `Arc<RwLock<TopologySnapshot>>`.
- [ ] 6.2 Support configurable heartbeat interval (default 3 seconds).
- [ ] 6.3 On heartbeat failure, try other known nodes before marking topology stale.
- [ ] 6.4 Wire up a shutdown signal (`tokio::sync::watch` or `CancellationToken`) to stop the heartbeat task.

## 7. Public API (GossipGridClient)

- [ ] 7.1 Implement `GossipGridClient::builder()` returning a `ClientBuilder` with `.seed_nodes()`, `.heartbeat_interval()`.
- [ ] 7.2 Implement `ClientBuilder::build() -> Result<GossipGridClient, ClientError>` that fetches initial topology and spawns heartbeat.
- [ ] 7.3 Implement `client.put(store_key, range_key, value)` — routes to leader, sends `POST /items`.
- [ ] 7.4 Implement `client.get(store_key, range_key)` — routes to replica/leader, sends `GET /items/{store_key}/{range_key}`.
- [ ] 7.5 Implement `client.get_by_partition(store_key)` — routes to replica/leader, sends `GET /items/{store_key}`.
- [ ] 7.6 Implement `client.delete(store_key, range_key)` — routes to leader, sends `DELETE /items/{store_key}/{range_key}`.
- [ ] 7.7 Implement on-error topology refresh: on `ConnectionFailed`, trigger immediate topology re-fetch before returning error.
- [ ] 7.8 Implement `client.shutdown()` to stop the heartbeat task.

## 8. Testing & Validation

- [ ] 8.1 Unit test: verify `hash_key` produces identical results to server's `Cluster::hash_key_inner` for a set of known keys.
- [ ] 8.2 Unit test: verify routing logic sends reads to replicas and writes to leaders given a mock topology.
- [ ] 8.3 Unit test: verify `LeaderUnavailable` is returned when leader node state is not `Joined`.
- [ ] 8.4 Integration test: spin up a 3-node ephemeral cluster, use the client to put/get/delete items, verify correct behavior.
- [ ] 8.5 Verify the `GET /cluster/topology` endpoint returns valid JSON with expected fields.
