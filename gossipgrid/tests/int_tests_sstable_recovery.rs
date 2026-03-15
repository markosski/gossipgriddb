use gossipgrid::node::NodeBuilder;
use gossipgrid::web::ItemOpsResponseEnvelope;

mod helpers;

#[tokio::test]
async fn test_sstable_wal_recovery_after_crash() {
    let _ = env_logger::try_init();

    let test_uuid = uuid::Uuid::new_v4().to_string();
    unsafe {
        std::env::set_var(
            "GOSSIPGRID_BASE_DIR",
            format!("/tmp/gossipgrid_test_{}", test_uuid),
        );
    }

    let web_port = helpers::get_free_port();
    let local_addr = format!("127.0.0.1:{}", helpers::get_free_port());

    let cluster_config =
        gossipgrid::cluster::Cluster::new(format!("test_cluster_{}", test_uuid), 1, 0, 1, 1, false);

    let node = NodeBuilder::new()
        .address(&local_addr)
        .unwrap()
        .web_port(web_port)
        .cluster(cluster_config.clone())
        .build()
        .await
        .unwrap();

    // Wait for the node to start and become ready
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    let client = reqwest::Client::new();

    // Insert an item
    let res = client
        .post(format!("http://localhost:{}/items", web_port))
        .body(r#"{"partition_key": "123", "message": "hello crash recovery"}"#)
        .send()
        .await
        .unwrap();
    assert!(res.status().is_success());

    // Verify it was inserted
    let res = client
        .get(format!("http://localhost:{}/items/123", web_port))
        .send()
        .await
        .unwrap();
    assert!(res.status().is_success());
    let response: ItemOpsResponseEnvelope =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();
    assert_eq!(response.success.unwrap().len(), 1);

    // Abort node strictly without calling shutdown to simulate a crash.
    // Memtable will not be flushed to SSTable files, data only exists in WAL.
    node.abort();

    // Give it a moment to fully shut down and release TCP ports
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // Restart the node (using same config, same web_port, same data directory)
    let node_restarted = NodeBuilder::new()
        .address(&local_addr)
        .unwrap()
        .web_port(web_port)
        .cluster(cluster_config)
        .build()
        .await
        .unwrap();

    // Wait for node to initialize, join, and hydrate from WAL
    tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;

    // Verify the item was recovered from the WAL
    let res = client
        .get(format!("http://localhost:{}/items/123", web_port))
        .send()
        .await
        .unwrap();
    assert!(res.status().is_success());
    let response: ItemOpsResponseEnvelope =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();

    let items = response.success.unwrap();
    assert_eq!(items.len(), 1);
    assert!(
        items[0]
            .message_string()
            .unwrap()
            .contains("hello crash recovery")
    );

    node_restarted.abort();

    // Clean up
    let _ = std::fs::remove_dir_all(format!("/tmp/gossipgrid_test_{}", test_uuid));
}
