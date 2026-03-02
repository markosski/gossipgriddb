# Design: Smart Client Crate

## Context
GossipGridDB's HTTP API allows any node to handle any request, proxying internally to the correct leader or replica via `decide_routing`. This works but adds an unnecessary network hop when the client doesn't know the topology. A "smart client" crate (`gossipgrid-client`) will maintain a local copy of the cluster topology and route requests directly to the correct node.

## Goals / Non-Goals

**Goals:**
- **Zero-proxy routing**: Client hashes keys and sends requests directly to the correct leader (writes) or replica (reads).
- **Automatic topology refresh**: Background heartbeat keeps the client's routing table current.
- **Graceful failover**: Reads fall back to replicas; writes return typed errors when leader is unavailable.
- **Minimal server-side impact**: Only a single new HTTP endpoint is needed on the server.
- **Idiomatic Rust API**: Async-first, builder pattern for configuration, compatible with `tokio` runtimes.

**Non-Goals:**
- **Connection pooling**: Out of scope; rely on `reqwest`'s built-in connection pool.
- **Client-side caching**: No query result caching in this iteration.
- **mTLS / auth**: Security layer deferred; assumes trusted network like the existing HTTP API.
- **Non-Rust clients**: This is a Rust-only library; other language bindings are future work.

## Decisions

### 1. Crate Structure
A new crate `gossipgrid-client` is added to the workspace at the project root (alongside `gossipgrid`). It is a standalone library with no compile-time dependency on the `gossipgrid` server crate — it communicates purely over HTTP and replicates the hashing logic independently.

```
Cargo.toml  (workspace: add "gossipgrid-client")
gossipgrid-client/
  Cargo.toml
  src/
    lib.rs          # Public API: GossipGridClient, ClientConfig
    topology.rs     # TopologySnapshot, topology refresh logic
    routing.rs      # Partition hashing + route decision
    error.rs        # ClientError enum
```

### 2. Topology Endpoint (Server-Side)
A new `GET /cluster/topology` endpoint is added to `gossipgrid/src/web/server.rs`. It returns a JSON response derived from the `NodeState`:

```json
{
  "cluster_name": "my_cluster",
  "cluster_size": 3,
  "partition_count": 9,
  "replication_factor": 2,
  "nodes": {
    "0": { "address": "127.0.0.1:4009", "web_port": 3001, "state": "Joined" },
    "1": { "address": "127.0.0.1:4010", "web_port": 3002, "state": "Joined" },
    "2": { "address": "127.0.0.1:4011", "web_port": 3003, "state": "JoinedSyncing" }
  },
  "partition_assignments": {
    "0": [0, 3, 6],
    "1": [1, 4, 7],
    "2": [2, 5, 8]
  },
  "partition_leaders": {
    "0": 0, "1": 1, "2": 2,
    "3": 0, "4": 1, "5": 2,
    "6": 0, "7": 1, "8": 2
  }
}
```

The handler reads from `Arc<RwLock<NodeState>>` (same pattern used by all existing web handlers) and constructs the response from `JoinedNode.cluster`.

### 3. Client-Side Hashing
The partition hashing must be **byte-identical** to the server's `Cluster::hash_key_inner`. The client replicates:

```rust
fn hash_key(partition_count: u16, key: &str) -> u16 {
    use std::hash::{Hash, Hasher};
    use twox_hash::XxHash64;
    let mut h = XxHash64::with_seed(0);
    key.hash(&mut h);
    (h.finish() % partition_count as u64) as u16
}
```

This uses the same `twox-hash` crate and `Hasher` trait implementation to ensure the `Hash` trait for `&str` produces identical bytes on both sides.

### 4. Route Decision Logic
The client mirrors the server's `decide_routing`:

| Operation | Target | Fallback |
|-----------|--------|----------|
| `GET` (read) | Any `Joined` replica for partition | Leader for partition |
| `POST` (write) | Leader for partition | Return `LeaderUnavailable` error |
| `DELETE` | Leader for partition | Return `LeaderUnavailable` error |

The client selects from its `TopologySnapshot`:
- For reads: iterate nodes assigned to the partition, pick a `Joined` non-leader node. If none available, fall back to the leader.
- For writes/deletes: look up `partition_leaders[partition_id]` → get the node → verify its state is `Joined`. If not, return error.

### 5. TopologySnapshot
The client maintains a `TopologySnapshot` behind an `Arc<RwLock<>>`:

```rust
struct TopologySnapshot {
    cluster_name: String,
    partition_count: u16,
    replication_factor: u8,
    nodes: HashMap<u8, NodeInfo>,           // node_index → (address, web_port, state)
    partition_assignments: HashMap<u8, Vec<u16>>, // node_index → partition IDs
    partition_leaders: HashMap<u16, u8>,     // partition_id → leader node_index
}
```

### 6. Background Heartbeat
A `tokio::spawn`ed task polls `GET /cluster/topology` on a configurable interval (default 3 seconds, matching the gossip interval). The heartbeat:
1. Picks a healthy node from the current topology (round-robin or random).
2. Sends the request with a short timeout (2 seconds).
3. On success: atomically swaps the `TopologySnapshot` behind the `RwLock`.
4. On failure: tries the next known node. After exhausting all nodes, marks topology as stale but keeps using the last known topology.

### 7. Public API
```rust
let client = GossipGridClient::builder()
    .seed_nodes(vec!["127.0.0.1:3001", "127.0.0.1:3002"])
    .heartbeat_interval(Duration::from_secs(3))
    .build()
    .await?;

// CRUD operations
client.put("store_key", "range_key", b"value").await?;
let item = client.get("store_key", "range_key").await?;
client.delete("store_key", "range_key").await?;

// Graceful shutdown stops the heartbeat task
client.shutdown().await;
```

The builder requires at least one seed node. On `build()`, the client connects to a seed, fetches topology, and spawns the heartbeat. All CRUD methods are `&self` (shared ownership via internal `Arc`s).

### 8. Error Handling Strategy
```rust
enum ClientError {
    LeaderUnavailable { partition_id: u16 },
    NoHealthyNodes,
    ConnectionFailed { address: String, source: reqwest::Error },
    TopologyStale,
    ServerError { status: u16, message: String },
}
```
- `LeaderUnavailable`: write/delete when leader is down or not yet elected.
- `NoHealthyNodes`: all seed nodes unreachable during initial connection.
- `ConnectionFailed`: specific node was unreachable (triggers topology refresh).
- `TopologyStale`: heartbeat has failed for multiple cycles.
- `ServerError`: the target node returned an HTTP error.

On any `ConnectionFailed`, the client triggers an immediate out-of-band topology refresh before returning the error, so the next request benefits from updated routing.

## Risks / Trade-offs

- **Hashing divergence**: If the server's hashing changes (algorithm or seed), the client will route incorrectly. Mitigation: the `twox-hash` crate version should be pinned in both crates, and a shared integration test should verify hash consistency.
- **Stale topology window**: Between heartbeat intervals, the client may route to a node that just went down. The existing server-side proxy handles this gracefully, so the worst case is a slightly delayed error, not data loss.
- **Topology endpoint security**: The endpoint exposes internal cluster structure. This is acceptable since the existing API has no auth, but should be addressed when auth is added.
- **reqwest dependency**: Adds a `reqwest` dependency to the client crate, which pulls in TLS libraries. If the user wants a minimal binary, a feature flag for TLS could be added later.
