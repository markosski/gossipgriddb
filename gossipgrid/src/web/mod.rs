mod cluster;
mod functions;
mod items;
mod server;

use std::{collections::HashMap, sync::Arc, time::Duration};

pub use functions::{ComputeRequest, ComputeResponse, FunctionListResponse};
use futures::{future::join_all, lock::Mutex};
pub use items::{
    ItemCreateUpdate, ItemGenericResponseEnvelope, ItemOpsResponseEnvelope, ItemResponse, WebError,
};
use log::{debug, error, info, warn};
use reqwest::Method;
use serde::{Deserialize, Serialize};
pub use server::web_server_task;
use tokio::sync::RwLock;
use warp::filters::path::FullPath;

use crate::{
    NodeAddress, PartitionKey, RangeKey, StorageKey, cluster::PartitionId, env::Env,
    node::NodeState,
};

const CANNOT_PERFORM_ACTION_IN_CURRENT_STATE: &str =
    "Cannot perform action in current state, is cluster ready?";

#[derive(Debug, Clone)]
struct RouteTarget {
    remote_addr: NodeAddress,
    web_port: u16,
}

#[derive(Debug, Clone)]
enum RouteDecision {
    Local,
    Remote { target: RouteTarget },
}

async fn route_request<T: Serialize, P: for<'de> Deserialize<'de>>(
    client: &reqwest::Client,
    target: &RouteTarget,
    url: &str,
    method: &Method,
    body: Option<&T>,
) -> Result<P, String> {
    let endpoint = format!(
        "http://{}:{}{}",
        target.remote_addr.ip, target.web_port, url
    );
    info!("Proxying request to {endpoint}");

    let resp_builder = match *method {
        Method::GET => client.get(endpoint),
        Method::POST => client
            .post(endpoint)
            .timeout(Duration::from_secs(3))
            .json(&body.expect("Body is required for POST")),
        Method::DELETE => client.delete(endpoint),
        _ => Err("Unexpected request method")?,
    };

    let resp = resp_builder
        .send()
        .await
        .map_err(|e| format!("Failed to send request: {e}"))?;

    let bytes = resp
        .bytes()
        .await
        .map_err(|e| format!("Failed to read response bytes: {e}"))?;

    serde_json::from_slice::<P>(&bytes).map_err(|e| e.to_string())
}

async fn decide_routing(
    partition_key: &PartitionKey,
    method: &Method,
    memory: Arc<RwLock<NodeState>>,
) -> Result<RouteDecision, ItemOpsResponseEnvelope> {
    let memory_read = memory.read().await;
    if let Some(this_node) = memory_read.as_joined_node() {
        let this_node_addr = this_node.get_address().clone();

        let leader_candidate = this_node
            .cluster
            .find_leader_for_partition(partition_key.value());

        let replica_candidate = this_node
            .cluster
            .find_replica_for_partition(partition_key.value());

        let route_target = if matches!(method, &Method::POST | &Method::DELETE) {
            // All write operations should go to leader
            leader_candidate
        } else if matches!(method, &Method::GET) {
            // All read operations should go to leader first, unless there is no leader, go to replica
            // This configuration ensures read-after-write consistency
            leader_candidate.or(replica_candidate)
        } else {
            None
        };

        match route_target {
            None => Err(ItemOpsResponseEnvelope {
                success: None,
                error: Some("Could not route to node".to_string()),
            }),
            Some(destination) if destination.address == this_node_addr => Ok(RouteDecision::Local),
            Some(destination) => Ok(RouteDecision::Remote {
                target: RouteTarget {
                    remote_addr: destination.address.clone(),
                    web_port: destination.web_port,
                },
            }),
        }
    } else {
        Err(ItemOpsResponseEnvelope {
            success: None,
            error: Some(CANNOT_PERFORM_ACTION_IN_CURRENT_STATE.to_string()),
        })
    }
}

fn partition_key_from_path(req_path: &FullPath) -> Option<PartitionKey> {
    let path = req_path.as_str();
    let path_only = path.split('?').next().unwrap_or("");
    let mut segments = path_only
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty());
    match (segments.next(), segments.next()) {
        (Some("items"), Some(partition_id)) if !partition_id.is_empty() => {
            Some(PartitionKey(partition_id.to_string()))
        }
        (Some("compute"), Some(partition_id)) if !partition_id.is_empty() => {
            Some(PartitionKey(partition_id.to_string()))
        }
        _ => None,
    }
}

async fn try_route_request(
    req_path: &FullPath,
    params: Option<&HashMap<String, String>>,
    method: Method,
    item_submit: Option<ItemCreateUpdate>,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<Option<serde_json::Value>, warp::Rejection> {
    let partition_key = match (&method, &item_submit) {
        (&Method::POST, Some(item)) => {
            StorageKey::new(
                PartitionKey(item.partition_key.clone()),
                item.range_key.clone().map(RangeKey),
            )
            .partition_key
        }
        (&Method::GET, None) | (&Method::DELETE, None) | (&Method::POST, None) => {
            match partition_key_from_path(req_path) {
                Some(partition_key) => partition_key,
                None => return Err(warp::reject::custom(WebError::RequestNotRoutable)),
            }
        }
        _ => return Err(warp::reject::custom(WebError::RequestNotRoutable)),
    };

    let node_address = {
        let memory = memory.read().await;
        memory.get_address().clone()
    };

    info!(
        "node={}; try route req_path: {:?}, method: {}, item_submit: {:?}",
        node_address, req_path, &method, &item_submit
    );

    let routing_result: Result<RouteDecision, ItemOpsResponseEnvelope> =
        decide_routing(&partition_key, &method, memory.clone()).await;

    let routing = match routing_result {
        Ok(decision) => decision,
        Err(response) => return Ok(Some(serde_json::to_value(response).unwrap())),
    };

    info!(
        "node={}; routing result: {:?} for item {:?}",
        node_address, &routing, &item_submit
    );

    match routing {
        RouteDecision::Remote { target } => {
            let mut url = req_path.as_str().to_string();
            if let Some(params) = params
                && !params.is_empty()
            {
                let query_string = serde_urlencoded::to_string(params).unwrap_or_default();
                if !query_string.is_empty() {
                    url.push('?');
                    url.push_str(&query_string);
                }
            }

            match route_request::<ItemCreateUpdate, serde_json::Value>(
                env.get_http_client(),
                &target,
                &url,
                &method,
                item_submit.as_ref(),
            )
            .await
            {
                Ok(remote_response) => Ok(Some(remote_response)),
                Err(err) => {
                    let err_resp = ItemGenericResponseEnvelope {
                        success: None,
                        error: Some(format!("Routing error: {err}")),
                    };
                    Ok(Some(serde_json::to_value(err_resp).unwrap()))
                }
            }
        }
        RouteDecision::Local => Ok(None),
    }
}

async fn wait_for_sync(
    memory: Arc<RwLock<NodeState>>,
    requirements: Vec<(PartitionId, u64)>,
) -> bool {
    // check
    let no_wait_for_sync = std::env::var("GOSSIPGRID_NO_WAIT_FOR_SYNC_ACK");
    if let Ok(val) = no_wait_for_sync
        && val == "1"
    {
        return true;
    }

    let mut futures = Vec::new();
    let confirmed_lsn = Arc::new(Mutex::new(HashMap::new()));
    {
        let state = memory.read().await;

        if let Some(node) = state.as_joined_node() {
            for (partition, target_lsn) in requirements {
                let replicas = node.cluster.get_all_healthy_replicas(&partition);

                let node_address = state.get_address().clone();
                if replicas.is_empty() {
                    warn!("node={node_address}; No replicas available to wait for sync");
                    confirmed_lsn.lock().await.insert(partition, target_lsn);
                    continue;
                }

                // Fast path: already synced
                if node.get_confirmed_lsn(partition) >= target_lsn {
                    continue;
                }

                let mut watcher = node.get_lsn_watcher(partition);
                let confirmed_lsn_move = confirmed_lsn.clone();
                futures.push(async move {
                    let success = match tokio::time::timeout(
                        Duration::from_millis(500),
                        watcher.wait_for(|lsn| *lsn >= target_lsn),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {
                            debug!("sync confirmed for partition={partition}, lsn={target_lsn}");
                            true
                        }
                        Ok(Err(e)) => {
                            error!("node={node_address}; Watcher error waiting for sync: {e}");
                            false
                        }
                        Err(e) => {
                            error!("node={node_address}; Timeout waiting for sync: {e}");
                            false
                        }
                    };

                    if success {
                        confirmed_lsn_move
                            .lock()
                            .await
                            .insert(partition, target_lsn);
                    }
                    success
                });
            }
        }
    }

    {
        let memory = memory.write().await;
        let confirmed_lsn = confirmed_lsn.lock().await;
        if let Some(node) = memory.as_joined_node() {
            for (partition, lsn) in confirmed_lsn.iter() {
                node.sync_state
                    .leader_confirmed_lsn
                    .insert(*partition, *lsn);
            }
        }
    }

    if !futures.is_empty() {
        let results = join_all(futures).await;
        results.into_iter().all(|success| success)
    } else {
        true
    }
}
