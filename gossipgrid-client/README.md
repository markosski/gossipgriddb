# gossipgrid-client

A topology-aware Rust client for [GossipGridDB](../README.md) that routes requests directly to the correct partition leader or replica, eliminating unnecessary proxy hops.

## Features

- **Smart routing** — hashes store keys to partitions and resolves the leader/replica, so every request goes straight to the right node.
- **Automatic topology refresh** — a background heartbeat keeps the local topology snapshot up to date.
- **Builder API** — configure seed nodes, heartbeat interval, and more before connecting.

## Quick Start

Add the dependency to your `Cargo.toml`:

```toml
[dependencies]
gossipgrid-client = { version = "*" }
```

> **Note:** `tokio` is pulled in transitively. If you need `#[tokio::main]` in your own crate, add `tokio` as a direct dependency (no extra features needed — gossipgrid-client already enables `rt` and `macros`).

Then use the client:

```rust
use gossipgrid_client::GossipGridClient;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the cluster via one or more seed nodes
    let client = GossipGridClient::builder()
        .seed_nodes(vec!["127.0.0.1:3001".to_string()])
        .heartbeat_interval(Duration::from_secs(3))
        .build()
        .await?;

    // Put an item
    client.put("my_store", "my_range", b"hello world").await?;

    // Get an item
    let item = client.get("my_store", "my_range").await?;
    println!("{item}");

    // Get all items in a partition (by store key only)
    let items = client.get_by_partition("my_store").await?;
    println!("{items}");

    // Delete an item
    client.delete("my_store", "my_range").await?;

    // Shutdown the client (stops the background heartbeat)
    client.shutdown().await;
    Ok(())
}
```

## Error Handling

All operations return `Result<serde_json::Value, ClientError>`. The error variants are:

| Variant | Description |
|---|---|
| `LeaderUnavailable` | The partition leader is down or not yet elected. Writes/deletes cannot proceed. |
| `NoHealthyNodes` | All seed/known nodes are unreachable. |
| `ConnectionFailed` | A specific node connection failed. Triggers an automatic topology refresh. |
| `TopologyStale` | The topology has not been refreshed for multiple heartbeat cycles. |
| `ServerError` | The server returned an HTTP error (includes status code and message). |

## Running Benchmarks

Benchmarks use [Criterion](https://docs.rs/criterion) and require a running GossipGridDB cluster on `localhost:3001`.

```bash
# Run all benchmarks
cargo bench -p gossipgrid-client

# Run a specific benchmark
cargo bench -p gossipgrid-client -- put_item
```

> **Note:** The benchmarks connect to a live cluster. Make sure at least one node is running before executing them.

## License

MIT OR Apache-2.0
