use gossipgrid::node::NodeState;
use gossipgrid::web::ItemOpsResponseEnvelope;

use log::info;

mod helpers;

#[tokio::test]
async fn test_publish_and_retrieve_items() {
    let _ = env_logger::try_init();

    let nodes = helpers::start_test_cluster(3, 3).await;

    let port1 = nodes[0].1.read().await.get_simple_node().unwrap().web_port;
    let port2 = nodes[1].1.read().await.get_simple_node().unwrap().web_port;

    let client = reqwest::Client::new();
    let _ = client
        .post(format!("http://localhost:{port1}/items"))
        .body(r#"{"partition_key": "123", "range_key": "456", "message": "foo1"}"#)
        .send()
        .await
        .unwrap();

    // Submitting another item to ensure we are fetching the correct one
    let _ = client
        .post(format!("http://localhost:{port1}/items"))
        .body(r#"{"partition_key": "123", "range_key": "457", "message": "foo1"}"#)
        .send()
        .await
        .unwrap();

    // Short sleep to allow sync to propagate
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let res = client
        .get(format!("http://localhost:{port2}/items/123/456"))
        .send()
        .await
        .unwrap();

    assert!(res.status().is_success());

    let response: ItemOpsResponseEnvelope =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();

    let item_count = response.success.as_ref().unwrap().len();
    let item_msg = response
        .success
        .unwrap()
        .first()
        .unwrap()
        .message_string()
        .unwrap();

    assert!(item_count == 1);
    assert!(item_msg.contains("foo1"));

    info!("success");

    tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await;

    {
        let node_guard = nodes[2].1.read().await;
        let node = match &*node_guard {
            NodeState::Joined(state) => state,
            _ => panic!("Node is not in Joined state"),
        };

        let count = node.get_all_items_count();
        assert_eq!(count, 2);
    }

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}

#[tokio::test]
async fn test_publish_and_retrieve_many_item() {
    let _ = env_logger::try_init();

    let nodes = helpers::start_test_cluster(3, 3).await;

    let port1 = nodes[0].1.read().await.get_simple_node().unwrap().web_port;
    let port2 = nodes[1].1.read().await.get_simple_node().unwrap().web_port;

    let client = reqwest::Client::new();
    let _ = client
        .post(format!("http://localhost:{port1}/items"))
        .body(r#"{"partition_key": "123", "range_key": "456", "message": "foo1"}"#)
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("http://localhost:{port2}/items"))
        .body(r#"{"partition_key": "123", "range_key": "457", "message": "foo2"}"#)
        .send()
        .await
        .unwrap();

    // Short sleep to allow sync to propagate
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Get several itmes
    let res = client
        .get(format!("http://localhost:{port1}/items/123"))
        .send()
        .await
        .unwrap();
    assert!(res.status().is_success());

    let response: ItemOpsResponseEnvelope =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();
    let items = response.success.unwrap();

    assert!(items.len() == 2);

    // Get items limited to 1
    let res = client
        .get(format!("http://localhost:{port2}/items/123?limit=1"))
        .send()
        .await
        .unwrap();
    assert!(res.status().is_success());

    let response: ItemOpsResponseEnvelope =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();
    let items = response.success.unwrap();

    assert!(items.len() == 1);

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}

#[tokio::test]
async fn test_publish_and_delete_item() {
    let _ = env_logger::try_init();

    let nodes = helpers::start_test_cluster(3, 3).await;

    let port1 = nodes[0].1.read().await.get_simple_node().unwrap().web_port;
    let port2 = nodes[1].1.read().await.get_simple_node().unwrap().web_port;

    let client = reqwest::Client::new();
    let _ = client
        .post(format!("http://localhost:{port1}/items"))
        .body(r#"{"partition_key": "123", "message": "foo1"}"#)
        .send()
        .await
        .unwrap();

    // Short sleep to allow sync to propagate
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let res = client
        .get(format!("http://localhost:{port2}/items/123"))
        .send()
        .await
        .unwrap();

    let response: ItemOpsResponseEnvelope =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();

    let item_msg = response
        .success
        .unwrap()
        .first()
        .unwrap()
        .message_string()
        .unwrap();

    assert!(item_msg.contains("foo1"));

    let _ = client
        .delete(format!("http://localhost:{port2}/items/123"))
        .send()
        .await
        .unwrap();

    let res = client
        .get(format!("http://localhost:{port1}/items/123"))
        .send()
        .await
        .unwrap();

    let response: ItemOpsResponseEnvelope =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();

    assert!(response.error.unwrap().contains("No items found"));

    // verify cluster item count
    let node_guard = nodes[2].1.read().await;
    let node = match &*node_guard {
        NodeState::Joined(state) => state,
        _ => panic!("Node is not in Joined state"),
    };

    let count = node.get_all_items_count();
    assert_eq!(count, 0);
    drop(node_guard);

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}

#[tokio::test]
async fn test_publish_and_upsert_item() {
    let _ = env_logger::try_init();

    let nodes = helpers::start_test_cluster(3, 3).await;

    let port1 = nodes[0].1.read().await.get_simple_node().unwrap().web_port;
    let port2 = nodes[1].1.read().await.get_simple_node().unwrap().web_port;
    let port3 = nodes[2].1.read().await.get_simple_node().unwrap().web_port;

    let client = reqwest::Client::new();
    let _ = client
        .post(format!("http://localhost:{port1}/items"))
        .body(r#"{"partition_key": "123", "message": "foo1"}"#)
        .send()
        .await
        .unwrap();

    // Short sleep to allow sync to propagate
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let res = client
        .get(format!("http://localhost:{port2}/items/123"))
        .send()
        .await
        .unwrap();

    let response: ItemOpsResponseEnvelope =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();
    let item_msg = response
        .success
        .unwrap()
        .first()
        .unwrap()
        .message_string()
        .unwrap();

    assert!(item_msg.contains("foo1"));

    let _ = client
        .post(format!("http://localhost:{port2}/items"))
        .body(r#"{"partition_key": "123", "message": "foo2"}"#)
        .send()
        .await
        .unwrap();

    // Short sleep to allow sync to propagate due to no read-after-write consistency
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let res = client
        .get(format!("http://localhost:{port3}/items/123"))
        .send()
        .await
        .unwrap();

    let response: ItemOpsResponseEnvelope =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();
    let item_msg = response
        .success
        .unwrap()
        .first()
        .unwrap()
        .message_string()
        .unwrap();

    assert!(item_msg.contains("foo2"));

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}
