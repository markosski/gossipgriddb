//! End-to-end throughput harness for `gossipgrid-wal`.
//!
//! This is a plain binary (harness = false) that prints a summary table.
//!
//! Run with:
//!   cargo bench --package gossipgrid-wal --bench wal_throughput

use gossipgrid_wal::{WalLocalFile, WalPartitionId, WalReader, WalRecord, WalWriter};
use std::time::{Duration, Instant};

const PARTITION: u16 = 1;

fn make_put(partition: WalPartitionId, value_size: usize) -> WalRecord {
    WalRecord::Put {
        partition,
        key: b"throughput-key".to_vec(),
        value: vec![0xCD; value_size],
        hlc: 1,
    }
}

// ---------------------------------------------------------------------------
// Individual benchmarks
// ---------------------------------------------------------------------------

struct BenchResult {
    name: &'static str,
    records: u64,
    bytes: u64,
    elapsed: Duration,
    extra: Option<String>,
}

impl BenchResult {
    fn ops_per_sec(&self) -> f64 {
        self.records as f64 / self.elapsed.as_secs_f64()
    }

    fn mb_per_sec(&self) -> f64 {
        self.bytes as f64 / self.elapsed.as_secs_f64() / (1024.0 * 1024.0)
    }
}

/// Sustained write throughput – default segment size (64 MB).
async fn bench_sustained_write(record_count: u64, value_size: usize) -> BenchResult {
    let tmp = tempfile::Builder::new()
        .prefix("tp_write")
        .tempdir()
        .unwrap();
    let wal = WalLocalFile::new(tmp.path().to_path_buf(), true)
        .await
        .unwrap();

    let partition = WalPartitionId::from(PARTITION);
    let rec = make_put(partition, value_size);
    let total_bytes = record_count * value_size as u64;

    let start = Instant::now();
    for _ in 0..record_count {
        wal.append(rec.clone()).await.unwrap();
    }
    wal.io_sync().await.unwrap();
    let elapsed = start.elapsed();

    BenchResult {
        name: "Sustained write (64 MB segments)",
        records: record_count,
        bytes: total_bytes,
        elapsed,
        extra: None,
    }
}

/// Write with a small segment size to force frequent rotation.
async fn bench_rotation_write(record_count: u64, value_size: usize) -> BenchResult {
    let segment_size: u64 = 256 * 1024; // 256 KB

    let tmp = tempfile::Builder::new()
        .prefix("tp_rotate")
        .tempdir()
        .unwrap();
    let wal = WalLocalFile::with_segment_size_max(tmp.path().to_path_buf(), true, segment_size)
        .await
        .unwrap();

    let partition = WalPartitionId::from(PARTITION);
    let rec = make_put(partition, value_size);
    let total_bytes = record_count * value_size as u64;

    let start = Instant::now();
    for _ in 0..record_count {
        wal.append(rec.clone()).await.unwrap();
    }
    wal.io_sync().await.unwrap();
    let elapsed = start.elapsed();

    // Count how many segment files were created.
    let seg_count = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .map(|e| e.file_name().to_string_lossy().ends_with(".wal"))
                .unwrap_or(false)
        })
        .count();

    BenchResult {
        name: "Write with rotation (256 KB segments)",
        records: record_count,
        bytes: total_bytes,
        elapsed,
        extra: Some(format!("{seg_count} segments created")),
    }
}

/// Read-back throughput over records written by `bench_sustained_write`.
async fn bench_read_back(record_count: u64, value_size: usize) -> BenchResult {
    let tmp = tempfile::Builder::new()
        .prefix("tp_read")
        .tempdir()
        .unwrap();
    let wal = WalLocalFile::new(tmp.path().to_path_buf(), true)
        .await
        .unwrap();

    let partition = WalPartitionId::from(PARTITION);
    let rec = make_put(partition, value_size);

    // Write phase (untimed).
    for _ in 0..record_count {
        wal.append(rec.clone()).await.unwrap();
    }
    wal.io_sync().await.unwrap();

    // Read phase (timed).
    let start = Instant::now();
    let iter = wal.stream_from(0, 0, partition, false).await;
    let mut count = 0u64;
    for item in iter {
        let _ = item.unwrap();
        count += 1;
    }
    let elapsed = start.elapsed();

    assert_eq!(count, record_count, "read-back record count mismatch");
    let total_bytes = count * value_size as u64;

    BenchResult {
        name: "Read-back (stream_from)",
        records: count,
        bytes: total_bytes,
        elapsed,
        extra: None,
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn print_table(results: &[BenchResult]) {
    let bar = "─".repeat(92);
    println!("\n┌{bar}┐");
    println!(
        "│ {:<40} {:>10} {:>10} {:>10} {:>14} │",
        "Benchmark", "Records", "MB/s", "ops/s", "Time"
    );
    println!("├{bar}┤");
    for r in results {
        println!(
            "│ {:<40} {:>10} {:>10.2} {:>10.0} {:>12.2?} │",
            r.name,
            r.records,
            r.mb_per_sec(),
            r.ops_per_sec(),
            r.elapsed,
        );
        if let Some(ref extra) = r.extra {
            println!("│   ↳ {:<85} │", extra);
        }
    }
    println!("└{bar}┘\n");
}

#[tokio::main]
async fn main() {
    let record_count: u64 = std::env::var("BENCH_RECORDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50_000);

    let value_size: usize = std::env::var("BENCH_VALUE_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1024);

    println!("WAL Throughput Benchmark");
    println!("  Records:    {record_count}");
    println!("  Value size: {value_size} bytes");
    println!();

    let mut results = Vec::new();

    results.push(bench_sustained_write(record_count, value_size).await);
    results.push(bench_rotation_write(record_count, value_size).await);
    results.push(bench_read_back(record_count, value_size).await);

    print_table(&results);
}
