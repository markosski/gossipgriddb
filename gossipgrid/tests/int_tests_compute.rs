use gossipgrid::web::{FunctionListResponse, ItemGenericResponseEnvelope};
mod helpers;

#[tokio::test]
async fn test_compute_sum_with_registered_function() {
    let _ = env_logger::try_init();

    let nodes = helpers::start_test_cluster_with_env(3, 3).await;

    let port1 = nodes[0].1.read().await.get_simple_node().unwrap().web_port;

    let client = reqwest::Client::new();

    // Insert some items with JSON data
    let _ = client
        .post(format!("http://localhost:{port1}/items"))
        .body(r#"{"partition_key": "calc", "range_key": "1", "message": "{\"v\": 10}"}"#)
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("http://localhost:{port1}/items"))
        .body(r#"{"partition_key": "calc", "range_key": "2", "message": "{\"v\": 20}"}"#)
        .send()
        .await
        .unwrap();

    // Short sleep to allow sync to propagate
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Register a function manually (simulating config load)
    let env1 = nodes[0].2.clone();
    env1.get_function_registry()
        .register(
            "sum_values".to_string(),
            "local sum = 0; local item = next_item(); while item ~= nil do sum = sum + item.data.v; item = next_item(); end; return sum".to_string(),
        )
        .unwrap();

    // List functions to verify registration
    let res = client
        .get(format!("http://localhost:{port1}/functions"))
        .send()
        .await
        .unwrap();

    let list_response: FunctionListResponse =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();
    assert!(list_response.functions.contains(&"sum_values".to_string()));

    // Execute function via GET /items with fn param
    let res = client
        .get(format!("http://localhost:{port1}/items/calc?fn=sum_values"))
        .send()
        .await
        .unwrap();

    assert!(res.status().is_success());

    let response: ItemGenericResponseEnvelope =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();

    let result = response
        .success
        .unwrap()
        .get("result")
        .unwrap()
        .as_f64()
        .unwrap();
    assert_eq!(result, 30.0);

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}
