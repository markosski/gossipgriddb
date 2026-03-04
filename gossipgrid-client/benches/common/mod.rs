use std::time::Duration;

#[allow(dead_code)]
pub fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx]
}

pub fn print_report(label: &str, latencies: &mut [Duration], total_wall_clock: Duration) {
    latencies.sort();
    if latencies.is_empty() {
        println!("\n── {label} (0 requests) ──");
        return;
    }
    let total_sum: Duration = latencies.iter().sum();
    let mean = total_sum / latencies.len() as u32;

    println!("\n── {label} ({} requests) ──", latencies.len());
    println!("  min    {:>10.3?}", latencies.first().unwrap());
    println!("  p50    {:>10.3?}", percentile(latencies, 50.0));
    println!("  p90    {:>10.3?}", percentile(latencies, 90.0));
    println!("  p95    {:>10.3?}", percentile(latencies, 95.0));
    println!("  p99    {:>10.3?}", percentile(latencies, 99.0));
    println!("  max    {:>10.3?}", latencies.last().unwrap());
    println!("  mean   {:>10.3?}", mean);
    println!(
        "  total  {:>10.3?} (sum: {:>10.3?})",
        total_wall_clock, total_sum
    );
}

use std::future::Future;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// Runs a benchmark function with a controlled level of concurrency.
///
/// `num_requests`: Total number of requests to execute.
/// `concurrency`: Maximum number of concurrent tasks.
/// `task_fn`: The async function to execute for each request. It takes the request index.
#[allow(dead_code)]
pub async fn run_concurrent_bench<F, Fut, T, E>(
    num_requests: usize,
    concurrency: usize,
    task_fn: F,
) -> (Vec<Duration>, Duration, u64)
where
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    let sem = Arc::new(Semaphore::new(concurrency));
    let mut join_set = JoinSet::new();
    let task_fn = Arc::new(task_fn);

    let mut latencies = Vec::with_capacity(num_requests);
    let mut errors = 0u64;
    let start_all = std::time::Instant::now();

    for i in 0..num_requests {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let task_fn = task_fn.clone();

        join_set.spawn(async move {
            let start = std::time::Instant::now();
            let res = task_fn(i).await;
            let elapsed = start.elapsed();
            drop(permit);
            (res, elapsed)
        });
    }

    while let Some(res) = join_set.join_next().await {
        if let Ok((result, elapsed)) = res {
            match result {
                Ok(_) => latencies.push(elapsed),
                Err(e) => {
                    errors += 1;
                    if errors <= 3 {
                        eprintln!("  Error #{errors}: {e}");
                    }
                }
            }
        }
    }

    let total_elapsed = start_all.elapsed();
    (latencies, total_elapsed, errors)
}
