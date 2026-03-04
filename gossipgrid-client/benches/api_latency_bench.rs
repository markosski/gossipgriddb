use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use reqwest::Client;
use serde_json::json;
use tokio::runtime::Runtime;
mod common;
use common::print_report;

const API_NODE: &str = "http://127.0.0.1:3001";
const NUM_REQUESTS: usize = 1_000_000;

fn main() {
    let rt = Runtime::new().unwrap();
    let client = Client::new();

    let concurrency: usize = std::env::var("CONCURRENCY")
        .unwrap_or_else(|_| "1".to_string())
        .parse()
        .expect("CONCURRENCY must be a positive integer");

    println!("Starting API benchmarks with CONCURRENCY = {}", concurrency);

    // ── PUT latency ──
    let client_put = client.clone();
    let (mut put_latencies, put_total, put_errors) = rt.block_on(async {
        common::run_concurrent_bench(NUM_REQUESTS, concurrency, move |i| {
            let client = client_put.clone();
            async move {
                let key = format!("api_latency_put:{i}");
                let range = format!("range:{i}");
                let value = format!("value_{i}_{:0>1000}", "");

                let body = json!({
                    "partition_key": key,
                    "range_key": range,
                    "message": BASE64_STANDARD.encode(value.as_bytes()),
                });

                let url = format!("{API_NODE}/items");

                client
                    .post(&url)
                    .json(&body)
                    .send()
                    .await?
                    .error_for_status()
            }
        })
        .await
    });

    print_report("PUT (API)", &mut put_latencies, put_total);
    if put_errors > 0 {
        println!("  errors {put_errors}");
    }

    // ── GET latency (reads back the same keys) ──
    let client_get = client.clone();
    let (mut get_latencies, get_total, get_errors) = rt.block_on(async {
        common::run_concurrent_bench(NUM_REQUESTS, concurrency, move |i| {
            let client = client_get.clone();
            async move {
                let key = format!("api_latency_put:{i}");
                let range = format!("range:{i}");
                let url = format!("{API_NODE}/items/{key}/{range}");

                client.get(&url).send().await?.error_for_status()
            }
        })
        .await
    });

    print_report("GET (API)", &mut get_latencies, get_total);
    if get_errors > 0 {
        println!("  errors {get_errors}");
    }
}
