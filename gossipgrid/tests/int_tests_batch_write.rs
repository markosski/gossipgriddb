use gossipgrid::web::ItemBatchResponseEnvelope;
use log::info;

mod helpers;

/// Task 3.1 – Verify batch single-node writes via the HTTP API.
/// Sends a batch of items to a single leader's `POST /items/batch` endpoint
/// and asserts that all items are reported as inserted with no errors.
#[tokio::test]
async fn test_batch_write_single_node() {
    let _ = env_logger::try_init();

    // 1 partition, 3 replicas → every item goes to the same leader
    let nodes = helpers::start_test_cluster(1, 3).await;

    let port = nodes[0].1.read().await.get_simple_node().unwrap().web_port;
    let batch_url = format!("http://localhost:{port}/items/batch");
    let client = reqwest::Client::new();

    // Build a batch of 10 items, all with the same partition key
    let items: Vec<serde_json::Value> = (0..10)
        .map(|i| {
            serde_json::json!({
                "partition_key": "batch_test",
                "range_key": format!("range:{i}"),
                "message": base64_encode(format!("value_{i}").as_bytes()),
            })
        })
        .collect();

    info!("Sending batch of {} items to {batch_url}", items.len());
    let resp = client
        .post(&batch_url)
        .json(&items)
        .send()
        .await
        .expect("Failed to send batch POST");

    let status = resp.status();
    let body = resp.text().await.unwrap();
    info!("Batch POST status={status}, body={body}");
    assert!(status.is_success(), "Batch POST failed: {body}");

    let envelope: ItemBatchResponseEnvelope =
        serde_json::from_str(&body).expect("Failed to parse batch response");

    assert_eq!(
        envelope.inserted, 10,
        "Expected 10 inserted items, got {}",
        envelope.inserted
    );
    assert!(
        envelope.errors.is_none() || envelope.errors.as_ref().unwrap().is_empty(),
        "Expected no errors, got {:?}",
        envelope.errors
    );

    // Verify items are readable
    for i in 0..10 {
        let get_url = format!("http://localhost:{port}/items/batch_test/range:{i}");
        let get_resp = client
            .get(&get_url)
            .send()
            .await
            .expect("Failed to GET item");
        assert!(
            get_resp.status().is_success(),
            "GET for item {i} failed: {}",
            get_resp.status()
        );
    }

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}

/// Task 3.2 – Verify `put_batch` routes correctly in a multi-node cluster
/// using the gossipgrid-client smart client.
///
/// Uses multiple partition keys that are likely to hash to different leaders
/// and confirms all items succeed.
#[tokio::test]
async fn test_batch_write_client_multi_node() {
    let _ = env_logger::try_init();

    // 3 partitions, 3 replicas → items should spread across leaders
    let nodes = helpers::start_test_cluster(3, 3).await;

    // Give leaders time to finish balancing
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Gather seed addresses from the started cluster (client expects ip:web_port)
    let mut seed_nodes = Vec::new();
    for (_rt, mem) in &nodes {
        let state = mem.read().await;
        let simple = state.get_simple_node().unwrap();
        seed_nodes.push(format!("{}:{}", simple.address.ip, simple.web_port));
    }

    let client = gossipgrid_client::GossipGridClient::builder()
        .seed_nodes(seed_nodes)
        .heartbeat_interval(std::time::Duration::from_secs(30))
        .build()
        .await
        .expect("Failed to connect smart client to test cluster");

    // Create items with diverse partition keys to spread across leaders
    let items: Vec<gossipgrid_client::ItemCreateUpdate> = (0..30)
        .map(|i| gossipgrid_client::ItemCreateUpdate {
            partition_key: format!("multi_batch_pk_{i}"),
            range_key: Some(format!("rk_{i}")),
            message: gossipgrid_client::base64_encode(format!("payload_{i}").as_bytes()),
        })
        .collect();

    info!("Sending batch of {} items via smart client", items.len());
    let result = client.put_batch(items).await;

    match &result {
        Ok(envelope) => {
            info!(
                "Batch result: inserted={}, errors={:?}",
                envelope.inserted, envelope.errors
            );
        }
        Err(e) => {
            panic!("put_batch failed: {e}");
        }
    }

    let envelope = result.unwrap();
    assert_eq!(
        envelope.inserted, 30,
        "Expected 30 inserted items, got {}",
        envelope.inserted
    );
    assert!(
        envelope.errors.is_none() || envelope.errors.as_ref().unwrap().is_empty(),
        "Expected no errors, got {:?}",
        envelope.errors
    );

    // Verify a sample of items are readable via the client
    for i in [0, 10, 20, 29] {
        let key = format!("multi_batch_pk_{i}");
        let range = format!("rk_{i}");
        let get_result = client.get(&key, &range).await;
        assert!(
            get_result.is_ok(),
            "GET for item {i} failed: {:?}",
            get_result.err()
        );
    }

    client.shutdown().await;
    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}

fn base64_encode(data: &[u8]) -> String {
    use base64::prelude::*;
    BASE64_STANDARD.encode(data)
}
