use crate::cluster::{AssignmentState, PartitionId};
use crate::node::NodeState;
use crate::web::items::WebError;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterNodeInfo {
    pub node_index: u8,
    pub assignment_state: AssignmentState,
    pub assigned_partitions: Vec<PartitionId>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterTopologyResponse {
    pub cluster_name: String,
    pub cluster_size: u8,
    pub nodes: Vec<ClusterNodeInfo>,
}

pub async fn handle_get_cluster(
    memory: Arc<RwLock<NodeState>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let memory = memory.read().await;

    if let Some(node) = memory.as_joined_node() {
        let cluster = &node.cluster;
        let mut nodes = Vec::new();

        for (index, (partitions, state)) in &cluster.partition_assignments {
            nodes.push(ClusterNodeInfo {
                node_index: *index,
                assignment_state: state.clone(),
                assigned_partitions: partitions.clone(),
            });
        }

        nodes.sort_by_key(|n| n.node_index);

        let response = ClusterTopologyResponse {
            cluster_name: cluster.cluster_name.clone(),
            cluster_size: cluster.cluster_size,
            nodes,
        };

        Ok(warp::reply::json(&response))
    } else {
        Err(warp::reject::custom(WebError::NodeNotReady(
            "Node is not in a joined state, cluster topology unavailable".to_string(),
        )))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResizeRequest {
    pub new_size: u8,
}

pub async fn handle_resize_cluster(
    req: ResizeRequest,
    memory: Arc<RwLock<NodeState>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let mut memory = memory.write().await;

    if let Some(node) = memory.as_joined_node_mut() {
        match node.cluster.resize(req.new_size) {
            Ok(_) => {
                node.tick_hlc();
                Ok(warp::reply::with_status(
                    warp::reply::json(
                        &serde_json::json!({"status": "success", "new_size": req.new_size}),
                    ),
                    warp::http::StatusCode::OK,
                ))
            }
            Err(e) => Ok(warp::reply::with_status(
                warp::reply::json(
                    &serde_json::json!({"status": "error", "message": e.to_string()}),
                ),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            )),
        }
    } else {
        Err(warp::reject::custom(WebError::NodeNotReady(
            "Node is not in a joined state, resize unavailable".to_string(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::HLC;
    use crate::cluster::Cluster;
    use crate::gossip::Gossip;
    use crate::node::JoinedNode;
    use crate::node::NodeAddress;
    use crate::sync::SyncState;
    use dashmap::DashMap;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU8;
    use warp::Filter;

    #[tokio::test]
    async fn test_handle_get_cluster() {
        let cluster = Cluster::new("test_cluster".to_string(), 3, 0, 9, 2, true);

        let node = JoinedNode {
            address: NodeAddress::parse_unchecked("127.0.0.1:4001"),
            web_port: 3001,
            cluster,
            partition_counts: DashMap::new(),
            node_hlc: Mutex::new(HLC::default()),
            gossip: Gossip {
                next_node_index: AtomicU8::new(0),
            },
            sync_state: SyncState {
                replica_partition_lsn: DashMap::new(),
                lsn_waiters: DashMap::new(),
                leader_confirmed_lsn: DashMap::new(),
                leader_tip_lsns: DashMap::new(),
            },
            is_syncing: false,
            is_hydrating: false,
        };

        let memory = Arc::new(RwLock::new(NodeState::Joined(node)));

        let response = warp::test::request()
            .method("GET")
            .path("/cluster")
            .reply(
                &warp::any()
                    .and(warp::any().map(move || memory.clone()))
                    .and_then(handle_get_cluster),
            )
            .await;

        assert_eq!(response.status(), 200);

        let topology: ClusterTopologyResponse = serde_json::from_slice(response.body()).unwrap();

        assert_eq!(topology.cluster_name, "test_cluster");
        assert_eq!(topology.cluster_size, 3);
        assert_eq!(topology.nodes.len(), 3);
        assert_eq!(topology.nodes[0].node_index, 0);
        assert_eq!(
            topology.nodes[0].assignment_state,
            AssignmentState::Unassigned
        );
    }

    #[tokio::test]
    async fn test_handle_resize_cluster() {
        let cluster = Cluster::new("test_cluster".to_string(), 3, 0, 9, 2, true);

        let node = JoinedNode {
            address: NodeAddress::parse_unchecked("127.0.0.1:4001"),
            web_port: 3001,
            cluster,
            partition_counts: DashMap::new(),
            node_hlc: Mutex::new(HLC::default()),
            gossip: Gossip {
                next_node_index: AtomicU8::new(0),
            },
            sync_state: SyncState {
                replica_partition_lsn: DashMap::new(),
                lsn_waiters: DashMap::new(),
                leader_confirmed_lsn: DashMap::new(),
                leader_tip_lsns: DashMap::new(),
            },
            is_syncing: false,
            is_hydrating: false,
        };

        let memory = Arc::new(RwLock::new(NodeState::Joined(node)));

        let response = warp::test::request()
            .method("POST")
            .path("/cluster/resize")
            .json(&ResizeRequest { new_size: 5 })
            .reply(
                &warp::path!("cluster" / "resize")
                    .and(warp::post())
                    .and(warp::body::json())
                    .and(warp::any().map(move || memory.clone()))
                    .and_then(handle_resize_cluster),
            )
            .await;

        assert_eq!(response.status(), 200);

        let body: serde_json::Value = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(body["status"], "success");
        assert_eq!(body["new_size"], 5);
    }
}
