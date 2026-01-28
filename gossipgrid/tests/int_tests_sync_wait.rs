use gossipgrid::web::ItemOpsResponseEnvelope;
use log::info;
use std::time::{Duration, Instant};

mod helpers;

#[tokio::test]
async fn test_sync_ordering_proof() {
    let _ = env_logger::try_init();

    let nodes = helpers::start_test_cluster(1, 3).await;

    let port1 = nodes[0].1.read().await.get_simple_node().unwrap().web_port;
    let port2 = nodes[1].1.read().await.get_simple_node().unwrap().web_port;
    let leader_url = format!("http://localhost:{port1}/items");
    let replica_url = format!("http://localhost:{port2}/items/ordering_test");

    let client = reqwest::Client::new();
    let partition_key = "ordering_test";

    let start_time = Instant::now();

    // 1. Spawn background POST request
    let post_handle = tokio::spawn(async move {
        let client = reqwest::Client::new();
        let payload = format!(r#"{{"partition_key": "{partition_key}", "message": "proof"}}"#);
        let resp = client
            .post(leader_url)
            .body(payload)
            .send()
            .await
            .expect("Failed to send POST");

        let status = resp.status();
        let text = resp.text().await.unwrap();
        (status, text, Instant::now())
    });

    // 2. Poll replica store for the item
    let mut item_found_time = None;
    for _ in 0..100 {
        // 100 * 10ms = 1s timeout
        let replica_res = client.get(&replica_url).send().await;

        if let Ok(res) = replica_res
            && res.status().is_success()
        {
            item_found_time = Some(Instant::now());
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let t1 = item_found_time.expect("Item never appeared on replica");

    // 3. Wait for POST to complete
    let (status, body, t2) = post_handle.await.unwrap();

    assert!(status.is_success(), "POST failed: {body}");

    info!(
        "Replica had item at: {:?}, Leader responded at: {:?}",
        t1 - start_time,
        t2 - start_time
    );

    // 4. Assert T1 <= T2
    assert!(
        t1 <= t2,
        "Replica found item AFTER leader responded! (T1: {:?}, T2: {:?})",
        t1 - start_time,
        t2 - start_time
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
