use gossipgrid_client::GossipGridClient;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

const SEED_NODE: &str = "127.0.0.1:3001";
const NUM_REQUESTS: usize = 1000;

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

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx]
}

fn print_report(label: &str, latencies: &mut Vec<Duration>) {
    latencies.sort();
    let total: Duration = latencies.iter().sum();
    let mean = total / latencies.len() as u32;

    println!("\n── {label} ({} requests) ──", latencies.len());
    println!("  min    {:>10.3?}", latencies.first().unwrap());
    println!("  p50    {:>10.3?}", percentile(latencies, 50.0));
    println!("  p90    {:>10.3?}", percentile(latencies, 90.0));
    println!("  p95    {:>10.3?}", percentile(latencies, 95.0));
    println!("  p99    {:>10.3?}", percentile(latencies, 99.0));
    println!("  max    {:>10.3?}", latencies.last().unwrap());
    println!("  mean   {:>10.3?}", mean);
    println!("  total  {:>10.3?}", total);
}

fn main() {
    let rt = Runtime::new().unwrap();
    let client = build_client(&rt);

    // ── PUT latency ──
    let mut put_latencies = Vec::with_capacity(NUM_REQUESTS);
    let mut put_errors = 0u64;

    for i in 0..NUM_REQUESTS {
        let key = format!("latency_put:{i}");
        let range = format!("range:{i}");
        let value = format!("value_{i}");

        let start = Instant::now();
        let result = rt.block_on(async { client.put(&key, &range, value.as_bytes()).await });
        let elapsed = start.elapsed();

        match result {
            Ok(_) => put_latencies.push(elapsed),
            Err(e) => {
                put_errors += 1;
                if put_errors <= 3 {
                    eprintln!("  PUT error #{put_errors}: {e}");
                }
            }
        }
    }

    print_report("PUT", &mut put_latencies);
    if put_errors > 0 {
        println!("  errors {put_errors}");
    }

    // ── GET latency (reads back the same keys) ──
    let mut get_latencies = Vec::with_capacity(NUM_REQUESTS);
    let mut get_errors = 0u64;

    for i in 0..NUM_REQUESTS {
        let key = format!("latency_put:{i}");
        let range = format!("range:{i}");

        let start = Instant::now();
        let result = rt.block_on(async { client.get(&key, &range).await });
        let elapsed = start.elapsed();

        match result {
            Ok(_) => get_latencies.push(elapsed),
            Err(e) => {
                get_errors += 1;
                if get_errors <= 3 {
                    eprintln!("  GET error #{get_errors}: {e}");
                }
            }
        }
    }

    print_report("GET", &mut get_latencies);
    if get_errors > 0 {
        println!("  errors {get_errors}");
    }

    rt.block_on(async { client.shutdown().await });
}
