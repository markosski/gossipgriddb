use crate::node::NodeState;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize)]
pub struct TopologyNodeInfo {
    pub address: String,
    pub web_port: u16,
    pub state: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopologyResponse {
    pub cluster_name: String,
    pub cluster_size: u8,
    pub partition_count: u16,
    pub replication_factor: u8,
    pub nodes: HashMap<u8, TopologyNodeInfo>,
    pub partition_assignments: HashMap<u8, Vec<u16>>,
    pub partition_leaders: HashMap<u16, u8>,
}

pub async fn handle_get_topology(
    memory: Arc<RwLock<NodeState>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let memory_read = memory.read().await;

    if let Some(this_node) = memory_read.as_joined_node() {
        let cluster = &this_node.cluster;

        let mut nodes = HashMap::new();
        let all_peers = cluster.get_all_peers_cloned();
        for (node_index, simple_node) in &all_peers {
            nodes.insert(
                *node_index,
                TopologyNodeInfo {
                    address: simple_node.address.ip_and_port.clone(),
                    web_port: simple_node.web_port,
                    state: format!("{:?}", simple_node.state),
                },
            );
        }

        let mut partition_assignments = HashMap::new();
        for (node_index, (partitions, _)) in &cluster.partition_assignments {
            partition_assignments.insert(*node_index, partitions.iter().map(|p| p.0).collect());
        }

        let partition_leaders: HashMap<u16, u8> = cluster
            .partition_leaders
            .iter()
            .map(|(pid, nid)| (pid.0, *nid))
            .collect();

        let response = TopologyResponse {
            cluster_name: cluster.cluster_name.clone(),
            cluster_size: cluster.cluster_size,
            partition_count: cluster.partition_count,
            replication_factor: cluster.replication_factor,
            nodes,
            partition_assignments,
            partition_leaders,
        };

        Ok(warp::reply::json(&response))
    } else {
        Err(warp::reject::not_found())
    }
}
