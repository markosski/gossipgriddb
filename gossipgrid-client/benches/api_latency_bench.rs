use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use reqwest::Client;
use serde_json::json;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

const API_NODE: &str = "http://127.0.0.1:3001";
const NUM_REQUESTS: usize = 1000;

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx]
}

fn print_report(label: &str, latencies: &mut [Duration]) {
    latencies.sort();
    if latencies.is_empty() {
        println!("\n── {label} (0 requests) ──");
        return;
    }
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
    let client = Client::new();

    // ── PUT latency ──
    let mut put_latencies = Vec::with_capacity(NUM_REQUESTS);
    let mut put_errors = 0u64;

    for i in 0..NUM_REQUESTS {
        let key = format!("api_latency_put:{i}");
        let range = format!("range:{i}");
        let value = format!("value_{i}");

        let body = json!({
            "partition_key": key,
            "range_key": range,
            "message": BASE64_STANDARD.encode(value.as_bytes()),
        });

        let url = format!("{API_NODE}/items");

        let start = Instant::now();
        let result = rt.block_on(async {
            client
                .post(&url)
                .json(&body)
                .send()
                .await?
                .error_for_status()
        });
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

    print_report("PUT (API)", &mut put_latencies);
    if put_errors > 0 {
        println!("  errors {put_errors}");
    }

    // ── GET latency (reads back the same keys) ──
    let mut get_latencies = Vec::with_capacity(NUM_REQUESTS);
    let mut get_errors = 0u64;

    for i in 0..NUM_REQUESTS {
        let key = format!("api_latency_put:{i}");
        let range = format!("range:{i}");
        let url = format!("{API_NODE}/items/{key}/{range}");

        let start = Instant::now();
        let result = rt.block_on(async { client.get(&url).send().await?.error_for_status() });
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

    print_report("GET (API)", &mut get_latencies);
    if get_errors > 0 {
        println!("  errors {get_errors}");
    }
}
