# gossipgrid-wal

Write-Ahead Log (WAL) implementation for GossipGrid.

This crate provides a structured WAL for recording state changes (Puts and Deletes) in a distributed database. It supports multiple partitions, LSN (Log Sequence Number) tracking, and asynchronous IO with `tokio`.

## Features

- **Partitioned Logs**: Each partition has its own WAL file for better concurrency and recovery.
- **Asynchronous IO**: Built on `tokio` for high-performance non-blocking disk operations.
- **LSN Tracking**: Every record is assigned an LSN for synchronization.
- **Watchers**: Subscribe to LSN updates for real-time replication.

## Usage

```rust
use gossipgrid_wal::{WalLocalFile, WalWriter, WalRecord, PartitionId};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wal = WalLocalFile::new(PathBuf::from("./wal_data"), false).await?;
    
    let record = WalRecord::Put {
        partition: PartitionId(1),
        key: b"my-key".to_vec(),
        value: b"my-value".to_vec(),
        hlc: 0,
    };
    
    let (lsn, offset) = wal.append(record).await?;
    println!("Appended record with LSN {} at offset {}", lsn, offset);
    
    wal.io_sync().await?;
    Ok(())
}
```

## Benchmarks

Two benchmark harnesses are included:

```bash
# Criterion microbenchmarks (append, serialization, read-back)
# Generates HTML reports in target/criterion/
cargo bench --package gossipgrid-wal --bench wal_micro

# End-to-end throughput harness (sustained writes, rotation, read-back)
cargo bench --package gossipgrid-wal --bench wal_throughput

# Customize throughput harness parameters
BENCH_RECORDS=100000 BENCH_VALUE_SIZE=4096 \
  cargo bench --package gossipgrid-wal --bench wal_throughput
```

## License

Apache-2.0
