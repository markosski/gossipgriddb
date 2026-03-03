use gossipgrid_client::GossipGridClient;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
mod common;
use common::print_report;

const SEED_NODE: &str = "127.0.0.1:3001";
const NUM_REQUESTS: usize = 1_000_000;

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

    // Read concurrency from environment or default to 1 (sequential)
    let concurrency: usize = std::env::var("CONCURRENCY")
        .unwrap_or_else(|_| "1".to_string())
        .parse()
        .expect("CONCURRENCY must be a positive integer");

    println!("Starting benchmarks with CONCURRENCY = {}", concurrency);

    // ── PUT latency ──
    let client_put = client.clone();
    let (mut put_latencies, put_total, put_errors) = rt.block_on(async {
        common::run_concurrent_bench(NUM_REQUESTS, concurrency, move |i| {
            let client = client_put.clone();
            async move {
                let key = format!("latency_put:{i}");
                let range = format!("range:{i}");
                let value = format!("value_{i}_{:0>1000}", "");
                client.put(&key, &range, value.as_bytes()).await
            }
        })
        .await
    });

    print_report("PUT", &mut put_latencies, put_total);
    if put_errors > 0 {
        println!("  errors {put_errors}");
    }

    // ── GET latency (reads back the same keys) ──
    let client_get = client.clone();
    let (mut get_latencies, get_total, get_errors) = rt.block_on(async {
        common::run_concurrent_bench(NUM_REQUESTS, concurrency, move |i| {
            let client = client_get.clone();
            async move {
                let key = format!("latency_put:{i}");
                let range = format!("range:{i}");
                client.get(&key, &range).await
            }
        })
        .await
    });

    print_report("GET", &mut get_latencies, get_total);
    if get_errors > 0 {
        println!("  errors {get_errors}");
    }

    // We only have Arc references left, so we can't cleanly call shutdown on the client itself if it expects `&self` or `&mut self` without Arc.
    // Assuming `shutdown` isn't strictly necessary for a benchmark exiting anyway, we can just drop it,
    // or if `shutdown()` is available on `Arc<GossipGridClient>` or `&GossipGridClient`.
    // Let's assume `client.shutdown().await` works on the reference.
    if let Ok(c) = Arc::try_unwrap(client) {
        let c: gossipgrid_client::GossipGridClient = c;
        rt.block_on(async move { c.shutdown().await });
    }
}
