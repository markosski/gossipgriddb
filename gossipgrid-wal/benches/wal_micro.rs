//! Criterion microbenchmarks for `gossipgrid-wal`.
//!
//! Run with:
//!   cargo bench --package gossipgrid-wal --bench wal_micro

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use gossipgrid_wal::{
    FramedWalRecord, WalLocalFile, WalPartitionId, WalReader, WalRecord, WalWriter,
};

/// Create a `WalRecord::Put` with a value of the given size.
fn make_put(partition: WalPartitionId, value_size: usize) -> WalRecord {
    WalRecord::Put {
        partition,
        key: b"bench-key".to_vec(),
        value: vec![0xAB; value_size],
        hlc: 1,
    }
}

/// Set up a fresh WAL in a temp directory with truncation enabled.
async fn setup_wal(name: &str) -> (WalLocalFile, tempfile::TempDir) {
    let tmp = tempfile::Builder::new()
        .prefix(name)
        .tempdir()
        .expect("failed to create tempdir");
    let wal = WalLocalFile::new(tmp.path().to_path_buf(), true)
        .await
        .expect("failed to create WAL");
    (wal, tmp)
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

/// Benchmark a single `append()` call for different payload sizes.
fn bench_append(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let sizes: &[(usize, &str)] = &[(128, "128B"), (1024, "1KB"), (4096, "4KB")];

    let mut group = c.benchmark_group("wal_append");

    for &(size, label) in sizes {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(label), &size, |b, &sz| {
            // Each iteration gets its own WAL so prior data doesn't influence I/O
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let (wal, _tmp) = setup_wal("bench_append").await;
                    let partition = WalPartitionId::from(1);
                    let rec = make_put(partition, sz);

                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        wal.append(rec.clone()).await.unwrap();
                    }
                    start.elapsed()
                })
            });
        });
    }

    group.finish();
}

/// Benchmark bincode encode + decode round-trip for a WalRecord.
fn bench_record_serialization(c: &mut Criterion) {
    let partition = WalPartitionId::from(1);
    let record = FramedWalRecord {
        lsn: 42,
        record: make_put(partition, 1024),
    };

    let mut group = c.benchmark_group("wal_serialization");

    group.bench_function("encode_1KB", |b| {
        b.iter(|| {
            bincode::encode_to_vec(&record, bincode::config::standard()).unwrap();
        });
    });

    let encoded = bincode::encode_to_vec(&record, bincode::config::standard()).unwrap();
    group.throughput(Throughput::Bytes(encoded.len() as u64));

    group.bench_function("decode_1KB", |b| {
        b.iter(|| {
            let _: (FramedWalRecord, _) =
                bincode::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        });
    });

    group.finish();
}

/// Benchmark reading back records via `stream_from`.
fn bench_stream_read(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let record_count: u64 = 1_000;
    let value_size: usize = 1024;

    // Pre-populate a WAL with records (outside the timed section).
    let tmp = tempfile::Builder::new()
        .prefix("bench_stream")
        .tempdir()
        .expect("failed to create tempdir");

    let wal = rt.block_on(async {
        let wal = WalLocalFile::new(tmp.path().to_path_buf(), true)
            .await
            .unwrap();
        let partition = WalPartitionId::from(1);
        for _ in 0..record_count {
            wal.append(make_put(partition, value_size)).await.unwrap();
        }
        wal.io_sync().await.unwrap();
        wal
    });

    let mut group = c.benchmark_group("wal_stream_read");
    group.throughput(Throughput::Elements(record_count));

    group.bench_function("read_1000x1KB", |b| {
        b.iter(|| {
            rt.block_on(async {
                let partition = WalPartitionId::from(1);
                let iter = wal.stream_from(0, 0, partition, false).await;
                let mut count = 0u64;
                for item in iter {
                    let _ = item.unwrap();
                    count += 1;
                }
                assert_eq!(count, record_count);
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_append,
    bench_record_serialization,
    bench_stream_read
);
criterion_main!(benches);
