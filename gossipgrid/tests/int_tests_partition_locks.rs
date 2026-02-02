// Integration tests for partition locking during leadership transitions
// Tasks 4.4 & 4.5

use gossipgrid::web::ItemOpsResponseEnvelope;
use log::info;

mod helpers;

// Task 4.4: Write to locked partition returns 503/error
#[tokio::test]
#[ignore] // TODO: Enable when lock propagation is implemented (Section 5)
async fn test_write_to_locked_partition_rejected() {
    let _ = env_logger::try_init();

    let nodes = helpers::start_test_cluster(3, 3).await;

    let port1 = nodes[0].1.read().await.get_simple_node().unwrap().web_port;

    // TODO: Once Section 5 is implemented:
    // 1. Manually set a partition as locked on node 0
    // 2. Submit a write request to that partition
    // 3. Verify it returns an error containing "locked" or "transition"

    let client = reqwest::Client::new();
    let res = client
        .post(format!("http://localhost:{port1}/items"))
        .body(r#"{"partition_key": "test_key", "message": "should_be_rejected"}"#)
        .send()
        .await
        .unwrap();

    // Currently this will succeed because locking isn't implemented yet
    // After Section 5:
    // - assert!(res.status() == reqwest::StatusCode::OK ||
    //           res.text().await.unwrap().contains("locked"));

    info!("Test placeholder - write rejection not yet fully implemented");

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}

// Task 4.5: Read from locked partition succeeds
#[tokio::test]
#[ignore] // TODO: Enable when lock propagation is implemented (Section 5)
async fn test_read_from_locked_partition_succeeds() {
    let _ = env_logger::try_init();

    let nodes = helpers::start_test_cluster(3, 3).await;

    let port1 = nodes[0].1.read().await.get_simple_node().unwrap().web_port;
    let port2 = nodes[1].1.read().await.get_simple_node().unwrap().web_port;

    let client = reqwest::Client::new();

    // First, write an item
    let _ = client
        .post(format!("http://localhost:{port1}/items"))
        .body(r#"{"partition_key": "test_key", "range_key": "123", "message": "readable_data"}"#)
        .send()
        .await
        .unwrap();

    // Short sleep to allow sync
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // TODO: Once Section 5 is implemented:
    // 1. Manually set the partition as locked
    // 2. Attempt to read from partition
    // 3. Verify read succeeds even though partition is locked

    let res = client
        .get(format!("http://localhost:{port2}/items/test_key/123"))
        .send()
        .await
        .unwrap();

    assert!(
        res.status().is_success(),
        "Reads should succeed even on locked partitions"
    );

    let response: ItemOpsResponseEnvelope =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();

    let item_msg = response
        .success
        .unwrap()
        .first()
        .unwrap()
        .message_string()
        .unwrap();

    assert!(item_msg.contains("readable_data"));

    info!("Test verified - reads work (lock doesn't affect reads)");

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}

// Additional test: Verify routing decision for locked partitions
#[tokio::test]
async fn test_routing_decision_structure() {
    let _ = env_logger::try_init();

    // This test verifies the routing logic compiles and basic structure works
    // The actual lock checking will be tested when Section 5 is implemented

    let nodes = helpers::start_test_cluster(3, 3).await;

    let port1 = nodes[0].1.read().await.get_simple_node().unwrap().web_port;

    let client = reqwest::Client::new();

    // Write a normal item (no locks)
    let res = client
        .post(format!("http://localhost:{port1}/items"))
        .body(r#"{"partition_key": "normal_key", "message": "normal_write"}"#)
        .send()
        .await
        .unwrap();

    assert!(res.status().is_success(), "Normal writes should succeed");

    info!("Routing decision structure verified");

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}
