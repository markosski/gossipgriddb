use gossipgrid::WalLocalFile;
use gossipgrid::event_bus::EventBus;
use gossipgrid::node::{NodeAddress, NodeError, NodeRuntime};
use gossipgrid::store::memory_store::InMemoryStore;
use gossipgrid::{
    ClusterHealth,
    env::{self, Env},
    node::{self, NodeState},
};
use log::info;
use std::env::set_var;
use std::net::TcpListener;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

pub fn get_free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

pub async fn stop_nodes(runtimes: Vec<Result<NodeRuntime, NodeError>>) {
    for runtime in runtimes {
        runtime.unwrap().abort();
    }
}

pub async fn start_test_cluster(
    partition_count: u16,
    replication_factor: u8,
) -> Vec<(Result<NodeRuntime, NodeError>, Arc<RwLock<NodeState>>)> {
    let test_uuid = Uuid::new_v4().to_string();

    let local_addr: NodeAddress = format!("127.0.0.1:{}", get_free_port())
        .as_str()
        .try_into()
        .unwrap();
    let local_addr_2: NodeAddress = format!("127.0.0.1:{}", get_free_port())
        .as_str()
        .try_into()
        .unwrap();
    let local_addr_3: NodeAddress = format!("127.0.0.1:{}", get_free_port())
        .as_str()
        .try_into()
        .unwrap();
    let web_port = get_free_port();
    let web_port_2 = get_free_port();
    let web_port_3 = get_free_port();
    let bus1 = EventBus::new();
    let bus2 = EventBus::new();
    let bus3 = EventBus::new();

    unsafe {
        set_var("GOSSIPGRID_BASE_DIR", "/tmp/gossipgrid_test");
    }

    let env1: Arc<Env> = Arc::new(env::Env::new(
        Box::new(InMemoryStore::default()),
        Box::new(
            WalLocalFile::new(&format!("{}/wal_1", test_uuid), true)
                .await
                .unwrap(),
        ),
        bus1,
    ));

    let env2: Arc<Env> = Arc::new(env::Env::new(
        Box::new(InMemoryStore::default()),
        Box::new(
            WalLocalFile::new(&format!("{}/wal_2", test_uuid), true)
                .await
                .unwrap(),
        ),
        bus2,
    ));

    let env3: Arc<Env> = Arc::new(env::Env::new(
        Box::new(InMemoryStore::default()),
        Box::new(
            WalLocalFile::new(&format!("{}/wal_3", test_uuid), true)
                .await
                .unwrap(),
        ),
        bus3,
    ));

    let cluster_config = gossipgrid::cluster::Cluster::new(
        "test_cluser".to_string(),
        3,
        0,
        partition_count,
        replication_factor,
        true,
    );

    let node_memory_1 = Arc::new(RwLock::new(NodeState::init(
        local_addr.clone(),
        web_port,
        None,
        Some(cluster_config),
    )));

    let node_memory_2 = Arc::new(RwLock::new(NodeState::init(
        local_addr_2.clone(),
        web_port_2,
        Some(local_addr.clone()),
        None,
    )));

    let node_memory_3 = Arc::new(RwLock::new(NodeState::init(
        local_addr_3.clone(),
        web_port_3,
        Some(local_addr.clone()),
        None,
    )));

    let node_1 = node::start_node(local_addr, web_port, node_memory_1.clone(), env1.clone()).await;

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let node_2 = node::start_node(
        local_addr_2,
        web_port_2,
        node_memory_2.clone(),
        env2.clone(),
    )
    .await;

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let node_3 = node::start_node(
        local_addr_3,
        web_port_3,
        node_memory_3.clone(),
        env3.clone(),
    )
    .await;

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut counter = 0;
    loop {
        info!("start cluster iteration: {counter}");
        if counter == 20 {
            panic!("Nodes did not join the cluster in time");
        }

        let node_mem_1 = node_memory_1.read().await;
        let node_mem_2 = node_memory_2.read().await;
        let node_mem_3 = node_memory_3.read().await;

        let joined_1 = if let NodeState::Joined(this_node) = &*node_mem_1 {
            let health = this_node.cluster.get_cluster_health();
            info!("Node 1 cluster health: {:?}", health);
            health == ClusterHealth::Healthy
        } else {
            false
        };

        let joined_2 = if let NodeState::Joined(this_node) = &*node_mem_2 {
            let health = this_node.cluster.get_cluster_health();
            info!("Node 2 cluster health: {:?}", health);
            health == ClusterHealth::Healthy
        } else {
            false
        };

        let joined_3 = if let NodeState::Joined(this_node) = &*node_mem_3 {
            let health = this_node.cluster.get_cluster_health();
            info!("Node 3 cluster health: {:?}", health);
            health == ClusterHealth::Healthy
        } else {
            false
        };

        if joined_1 && joined_2 && joined_3 {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        counter += 1;
    }
    // extra time for leaders to balance out
    vec![
        (node_1, node_memory_1),
        (node_2, node_memory_2),
        (node_3, node_memory_3),
    ]
}
