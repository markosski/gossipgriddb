# Proposal: Smart Client Crate

## Summary
Introduce a new `gossipgrid-client` crate that provides a topology-aware Rust client for GossipGridDB. Instead of hitting any arbitrary node's HTTP API (which may proxy the request internally to the correct leader/replica), this client maintains a local copy of the cluster topology and routes requests directly to the appropriate node — eliminating unnecessary proxy hops and reducing latency.

## Problem Statement
The current HTTP API works correctly: any node can accept any request and will proxy it internally to the right destination via `decide_routing`. However, this means:
- **Extra network hops**: If a client happens to hit a node that doesn't own the partition, the request is proxied the actual leader or replica, doubling latency.
- **No client-side intelligence**: The client has no knowledge of the cluster topology, so it cannot make smart routing decisions.
- **No resilience to node failures**: If the node the client is connected to goes down, the client has no fallback — it must manually discover another node.

A smart client that maintains topology awareness can route requests directly to the correct node, handle failover transparently, and reduce load on intermediate nodes.

## What Changes
1. **New crate `gossipgrid-client`**: A standalone Rust library added to the workspace, providing a high-level async client API.
2. **Topology discovery**: On initialization, the client connects to any seed node's HTTP API to fetch the full cluster topology (partition assignments, leader map, node addresses).
3. **Client-side routing**: The client replicates the `hash_key` logic (xxHash64-based partition mapping) and `decide_routing` logic to route reads to replicas and writes/deletes to leaders — all without any proxy hop.
4. **Background heartbeat**: A background task periodically polls the cluster for updated topology (node joins/leaves, leader re-elections), keeping the client's routing table fresh.
5. **Error handling & failover**: When a leader is down, reads fall back to a healthy replica. Writes and deletes return an error indicating the partition leader is unavailable until a new leader is elected.
6. **New server-side topology endpoint**: A new HTTP endpoint (e.g., `GET /cluster/topology`) on every node that returns the current `ClusterMetadata`, partition assignments with states, and the leader map — providing the client with everything it needs to build its routing table.

## Capabilities

### New Capabilities
- `topology-discovery`: Connect to a seed node and fetch the full cluster topology including partition assignments, leader map, and node addresses.
- `client-routing`: Client-side partition key hashing and routing logic that mirrors the server's `decide_routing`, sending reads to replicas and writes to leaders directly.
- `topology-heartbeat`: Background task to periodically refresh the local topology snapshot via heartbeat requests to a cluster node.
- `error-handling`: Graceful handling of node failures — reads fall back to replicas; writes/deletes return a typed error when the partition leader is unavailable.
- `topology-endpoint`: Server-side HTTP endpoint exposing cluster topology for client consumption.

### Modified Capabilities
- None. This is a fully additive change with no modifications to existing server behavior.

## Impact
- **New crate**: `gossipgrid-client` (added to workspace `Cargo.toml`)
- **Modified crate**: `gossipgrid` (new topology HTTP endpoint in `web/`)
- **Structs**: New client configuration, topology snapshot, client error types
- **Traits**: Possible shared `PartitionRouter` trait if we want to share hashing logic between server and client
- **Dependencies**: `reqwest`, `tokio`, `twox-hash`, `serde`/`serde_json` (for the client crate)
