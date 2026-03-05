use gossipgrid_client::{GossipGridClient, ItemCreateUpdate, base64_encode};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
mod common;
use common::print_report;

const SEED_NODE: &str = "127.0.0.1:3001";
const TOTAL_ITEMS: usize = 100_000;
const BATCH_SIZE: usize = 25;

fn build_client(rt: &Runtime) -> GossipGridClient {
    rt.block_on(async {
        GossipGridClient::builder()
            .seed_nodes(vec![SEED_NODE.to_string()])
            .heartbeat_interval(Duration::from_secs(30))
            .build()
            .await
            .expect("Failed to connect to cluster. Is a node running on localhost:3001?")
    })
}

fn main() {
    let rt = Runtime::new().unwrap();
    let client = Arc::new(build_client(&rt));

    let concurrency: usize = std::env::var("CONCURRENCY")
        .unwrap_or_else(|_| "1".to_string())
        .parse()
        .expect("CONCURRENCY must be a positive integer");

    let batch_size: usize = std::env::var("BATCH_SIZE")
        .unwrap_or_else(|_| BATCH_SIZE.to_string())
        .parse()
        .expect("BATCH_SIZE must be a positive integer");

    let total_items: usize = std::env::var("TOTAL_ITEMS")
        .unwrap_or_else(|_| TOTAL_ITEMS.to_string())
        .parse()
        .expect("TOTAL_ITEMS must be a positive integer");

    println!("Batch Write Benchmark");
    println!("  CONCURRENCY = {concurrency}");
    println!("  BATCH_SIZE  = {batch_size}");
    println!("  TOTAL_ITEMS = {total_items}");

    // ── Single-item PUT baseline ──
    let num_single = total_items;
    let client_single = client.clone();
    let (mut single_latencies, single_total, single_errors) = rt.block_on(async {
        common::run_concurrent_bench(num_single, concurrency, move |i| {
            let client = client_single.clone();
            async move {
                let key = format!("batch_bench_single:{i}");
                let range = format!("range:{i}");
                let value = format!("v_{i}_{:0>1000}", "");
                client
                    .put(ItemCreateUpdate {
                        partition_key: key,
                        range_key: Some(range),
                        message: base64_encode(value.as_bytes()),
                    })
                    .await
            }
        })
        .await
    });

    print_report(
        &format!("SINGLE PUT ({num_single} items)"),
        &mut single_latencies,
        single_total,
    );
    if single_errors > 0 {
        println!("  errors {single_errors}");
    }

    // ── Batch PUT ──
    let num_batches = total_items / batch_size;
    let client_batch = client.clone();
    let (mut batch_latencies, batch_total, batch_errors) = rt.block_on(async {
        common::run_concurrent_bench(num_batches, concurrency, move |batch_idx| {
            let client = client_batch.clone();
            let bs = batch_size;
            async move {
                let start = batch_idx * bs;
                let items: Vec<ItemCreateUpdate> = (start..start + bs)
                    .map(|i| ItemCreateUpdate {
                        partition_key: format!("batch_bench_batch:{i}"),
                        range_key: Some(format!("range:{i}")),
                        message: base64_encode(format!("v_{i}_{:0>1000}", "").as_bytes()),
                    })
                    .collect();
                client.put_batch(items).await
            }
        })
        .await
    });

    print_report(
        &format!(
            "BATCH PUT ({num_batches} batches × {batch_size} items = {} items)",
            num_batches * batch_size
        ),
        &mut batch_latencies,
        batch_total,
    );
    if batch_errors > 0 {
        println!("  errors {batch_errors}");
    }

    // Summary comparison
    let single_throughput = if single_total.as_secs_f64() > 0.0 {
        num_single as f64 / single_total.as_secs_f64()
    } else {
        0.0
    };
    let batch_throughput = if batch_total.as_secs_f64() > 0.0 {
        (num_batches * batch_size) as f64 / batch_total.as_secs_f64()
    } else {
        0.0
    };
    println!("\n── Throughput Comparison ──");
    println!("  Single PUT:  {single_throughput:>10.1} items/sec");
    println!("  Batch PUT:   {batch_throughput:>10.1} items/sec");
    if single_throughput > 0.0 {
        println!(
            "  Speedup:     {:.2}x",
            batch_throughput / single_throughput
        );
    }

    if let Ok(c) = Arc::try_unwrap(client) {
        rt.block_on(async move { c.shutdown().await });
    }
}
