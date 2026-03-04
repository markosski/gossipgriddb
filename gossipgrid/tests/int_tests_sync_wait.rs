use gossipgrid::web::ItemOpsResponseEnvelope;
use log::info;
use std::time::{Duration, Instant};

mod helpers;

#[tokio::test]
async fn test_sync_wait_success() {
    let _ = env_logger::try_init();

    let nodes = helpers::start_test_cluster(1, 3).await; // 1 leader, 3 replicas

    let port1 = nodes[0].1.read().await.get_simple_node().unwrap().web_port;
    let leader_url = format!("http://localhost:{port1}/items");

    let client = reqwest::Client::new();
    let partition_key = "sync_success_test";

    // Perform a POST to the leader.
    // By default, POST /items triggers sync wait semantics (waiting for replica ACKs before responding).
    let payload = format!(r#"{{"partition_key": "{partition_key}", "message": "success"}}"#);

    let start_time = Instant::now();
    info!("Sending POST to leader, expecting successful sync...");
    let resp = client
        .post(leader_url)
        .body(payload)
        .send()
        .await
        .expect("Failed to send POST");

    let status = resp.status();
    let duration = start_time.elapsed();
    let text = resp.text().await.unwrap();

    info!("POST took: {duration:?} with status: {status}");

    assert!(status.is_success(), "POST failed: {text}");
    assert!(
        duration < Duration::from_millis(500),
        "Duration was too long {duration:?}, meaning it probably hit a timeout instead of actual successful sync completion"
    );

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}

#[tokio::test]
async fn test_sync_timeout_on_replica_failure() {
    let _ = env_logger::try_init();

    let mut nodes = helpers::start_test_cluster(1, 3).await;

    // Abort the replica
    let (replica_runtime, _) = nodes.remove(1);
    replica_runtime.unwrap().abort();
    let (replica_runtime, _) = nodes.remove(1);
    replica_runtime.unwrap().abort();

    // wait for nodes to fully terminate
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Immediately perform a POST to the leader.
    let port1 = nodes[0].1.read().await.get_simple_node().unwrap().web_port;
    let leader_url = format!("http://localhost:{port1}/items");
    let client = reqwest::Client::new();

    let start_time = Instant::now();
    let payload = r#"{"partition_key": "timeout_test", "message": "fail"}"#;

    info!("Sending POST to leader, expecting timeout...");
    let resp = client
        .post(leader_url)
        .body(payload)
        .send()
        .await
        .expect("Failed to send POST");

    let duration = start_time.elapsed();
    info!("POST took: {duration:?}");

    // It should fail with 500
    assert_eq!(resp.status(), 500);
    let body = resp.text().await.unwrap();
    let response: ItemOpsResponseEnvelope =
        serde_json::from_str(&body).unwrap_or_else(|_| panic!("Failed to parse JSON: {body}"));
    assert!(
        response
            .error
            .unwrap()
            .contains("Request failed, not all items synced")
    );

    // And it should take at least 500ms
    assert!(
        duration >= Duration::from_millis(500),
        "Duration was too short: {duration:?}. Leader didn't wait long enough."
    );

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}
