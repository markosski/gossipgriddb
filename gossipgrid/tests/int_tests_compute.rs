use gossipgrid::web::ItemGenericResponseEnvelope;
mod helpers;

#[tokio::test]
async fn test_compute_sum() {
    let _ = env_logger::try_init();

    let nodes = helpers::start_test_cluster(3, 3).await;

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

    // Short sleep to allow sync to propagate if needed (though we mostly test local compute)
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Execute compute with lazy iteration pattern
    let compute_req = r#"{
        "script": "local sum = 0; local item = next_item(); while item ~= nil do sum = sum + item.data.v; item = next_item(); end; return sum"
    }"#;

    let res = client
        .post(format!("http://localhost:{port1}/compute/calc"))
        .header("Content-Type", "application/json")
        .body(compute_req)
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
