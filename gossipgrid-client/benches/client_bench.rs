use criterion::{Criterion, criterion_group, criterion_main};
use gossipgrid_client::{GossipGridClient, ItemCreateUpdate, base64_encode};
use std::time::Duration;
use tokio::runtime::Runtime;

const SEED_NODE: &str = "127.0.0.1:3001";

fn build_client(rt: &Runtime) -> GossipGridClient {
    rt.block_on(async {
        GossipGridClient::builder()
            .seed_nodes(vec![SEED_NODE.to_string()])
            .heartbeat_interval(Duration::from_secs(30)) // Long interval to avoid interference
            .build()
            .await
            .expect("Failed to connect to cluster. Is a node running on localhost:4109?")
    })
}

fn bench_put(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let client = build_client(&rt);

    c.bench_function("put_item", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("bench_store:{i}");
            let range = format!("range:{i}");
            let value = format!("value_{i}");
            rt.block_on(async {
                let _ = client
                    .put(ItemCreateUpdate {
                        partition_key: key,
                        range_key: Some(range),
                        message: base64_encode(value.as_bytes()),
                    })
                    .await;
            });
            i += 1;
        });
    });

    rt.block_on(async { client.shutdown().await });
}

fn bench_get(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let client = build_client(&rt);

    // Seed some data first
    rt.block_on(async {
        for i in 0..100 {
            let key = format!("bench_read:{i}");
            let range = format!("range:{i}");
            let _ = client
                .put(ItemCreateUpdate {
                    partition_key: key,
                    range_key: Some(range),
                    message: base64_encode(b"benchmark_value"),
                })
                .await;
        }
    });

    c.bench_function("get_item", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("bench_read:{}", i % 100);
            let range = format!("range:{}", i % 100);
            rt.block_on(async {
                let _ = client.get(&key, &range).await;
            });
            i += 1;
        });
    });

    rt.block_on(async { client.shutdown().await });
}

fn bench_put_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let client = build_client(&rt);

    let mut group = c.benchmark_group("throughput");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("put_single", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("bench_tp:{i}");
            let range = format!("range:{i}");
            rt.block_on(async {
                let _ = client
                    .put(ItemCreateUpdate {
                        partition_key: key,
                        range_key: Some(range),
                        message: base64_encode(b"throughput_test"),
                    })
                    .await;
            });
            i += 1;
        });
    });
    group.finish();

    rt.block_on(async { client.shutdown().await });
}

criterion_group!(benches, bench_put, bench_get, bench_put_throughput);
criterion_main!(benches);
