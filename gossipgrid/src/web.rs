use crate::clock::HLC;
use crate::clock::now_millis;
use crate::cluster::PartitionId;
use crate::env::Env;
use crate::item::{Item, ItemStatus};
use crate::node::{self, JoinedNode, NodeAddress, NodeState};
use crate::store::{PartitionKey, RangeKey, StorageKey};
use base64::engine::general_purpose;
use bincode::{Decode, Encode};
use futures::future::join_all;
use log::debug;
use log::{error, info, warn};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use warp::filters::path::FullPath;

use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use warp::Filter;

const CANNOT_PERFORM_ACTION_IN_CURRENT_STATE: &str =
    "Cannot perform action in current state, is cluster ready?";

#[derive(Debug)]
pub enum WebError {
    NodeNotReady(String),
    RequestNotRoutable,
}
impl warp::reject::Reject for WebError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemResponse {
    pub message: String,
    pub status: ItemStatus,
    pub hlc: HLC,
}

impl ItemResponse {
    pub fn message_string(&self) -> Result<String, String> {
        use base64::prelude::*;

        let decoded_bytes = general_purpose::STANDARD
            .decode(&self.message)
            .map_err(|e| e.to_string())?;
        match String::from_utf8(decoded_bytes) {
            Ok(s) => Ok(s),
            Err(e) => Err(e.to_string()),
        }
    }
}

impl From<Item> for ItemResponse {
    fn from(item: Item) -> Self {
        use base64::prelude::*;

        ItemResponse {
            message: general_purpose::STANDARD.encode(&item.message),
            status: item.status,
            hlc: item.hlc,
        }
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct ItemCreateUpdate {
    pub partition_key: String,
    pub range_key: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeRequest {
    pub script: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeResponse {
    pub result: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ItemOpsResponseEnvelope {
    pub success: Option<Vec<ItemResponse>>,
    pub error: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ItemGenericResponseEnvelope {
    pub success: Option<HashMap<String, serde_json::Value>>,
    pub error: Option<String>,
}

// HTTP server implementation
fn with_memory(
    memory: Arc<RwLock<NodeState>>,
) -> impl Filter<Extract = (Arc<RwLock<NodeState>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || memory.clone())
}

fn with_env(
    env: Arc<Env>,
) -> impl Filter<Extract = (Arc<Env>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || env.clone())
}

#[derive(Debug, Clone, Copy)]
pub enum ProxyMethod {
    Get,
    Post,
    Delete,
}

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
        } else {
            // All read operations should go to replica, unless there is no replica available
            // TODO: make this configurable, if we want read after write consistency we need to still direct to leader
            replica_candidate.or(leader_candidate)
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
    method: Method,
    item_submit: Option<ItemCreateUpdate>,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<Option<ItemOpsResponseEnvelope>, warp::Rejection> {
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
        Err(response) => return Ok(Some(response)),
    };

    info!(
        "node={}; routing result: {:?} for item {:?}",
        node_address, &routing, &item_submit
    );

    match routing {
        RouteDecision::Remote { target } => {
            match route_request::<ItemCreateUpdate, ItemOpsResponseEnvelope>(
                env.get_http_client(),
                &target,
                req_path.as_str(),
                &method,
                item_submit.as_ref(),
            )
            .await
            {
                Ok(remote_response) => Ok(Some(remote_response)),
                Err(err) => Ok(Some(ItemOpsResponseEnvelope {
                    success: None,
                    error: Some(format!("Routing error: {err}")),
                })),
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

async fn handle_post_item(
    req_path: FullPath,
    item_submit: ItemCreateUpdate,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let partition_count = {
        let memory = memory.read().await;
        if let Some(node) = memory.as_joined_node() {
            node.cluster.partition_count
        } else {
            return Err(warp::reject::custom(WebError::NodeNotReady(
                "Node is not in active state".to_string(),
            )));
        }
    };

    match try_route_request(
        &req_path,
        Method::POST,
        Some(item_submit.clone()),
        memory.clone(),
        env.clone(),
    )
    .await
    {
        Ok(Some(response_envelope)) => {
            return Ok(warp::reply::with_status(
                warp::reply::json(&response_envelope),
                warp::http::StatusCode::OK,
            ));
        }
        Ok(None) => {
            // handle locally below
        }
        Err(rejection) => return Err(rejection),
    }

    let storage_key = StorageKey::new(
        PartitionKey(item_submit.partition_key.clone()),
        item_submit.range_key.clone().map(RangeKey),
    );
    let message_bytes = item_submit.message.as_bytes().to_vec();

    // Phase 1: Prepare (Read Lock is enough now)
    let item_entry = {
        let memory = memory.read().await;
        if let Some(node) = memory.as_joined_node() {
            if !node.is_ready() {
                return Err(warp::reject::custom(WebError::NodeNotReady(
                    CANNOT_PERFORM_ACTION_IN_CURRENT_STATE.to_string(),
                )));
            }
            Ok(node.prepare_local_item(&storage_key, message_bytes, ItemStatus::Active))
        } else {
            Err(warp::reject::custom(WebError::NodeNotReady(
                CANNOT_PERFORM_ACTION_IN_CURRENT_STATE.to_string(),
            )))
        }
    }?;

    let io_result = JoinedNode::insert_items_io_only(
        partition_count,
        vec![item_entry.clone()],
        env.get_store(),
        env.get_wal(),
    )
    .await;

    // Phase 2: Commit
    match io_result {
        Ok((_added_keys, reqs, max_hlc)) => {
            match wait_for_sync(memory.clone(), reqs).await {
                false => {
                    let response = ItemOpsResponseEnvelope {
                        success: None,
                        error: Some("Request failed, not all items synced".to_string()),
                    };
                    return Ok(warp::reply::with_status(
                        warp::reply::json(&response),
                        warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                    ));
                }
                true => {
                    // keep going
                }
            }

            {
                let memory = memory.read().await;
                if let Some(node) = memory.as_joined_node() {
                    node.node_hlc_item_merge(max_hlc);
                }
            }

            let response = ItemOpsResponseEnvelope {
                success: Some(vec![ItemResponse::from(item_entry.item)]),
                error: None,
            };
            Ok(warp::reply::with_status(
                warp::reply::json(&response),
                warp::http::StatusCode::OK,
            ))
        }
        Err(e) => {
            let response = ItemOpsResponseEnvelope {
                success: None,
                error: Some(format!("Item submission error: {e}")),
            };
            Ok(warp::reply::with_status(
                warp::reply::json(&response),
                warp::http::StatusCode::OK,
            ))
        }
    }
}

async fn handle_get_items_without_range(
    store_key: String,
    req_path: FullPath,
    params: HashMap<String, String>,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<impl warp::Reply, warp::Rejection> {
    handle_get_items(store_key, "".to_string(), req_path, params, memory, env).await
}

async fn handle_get_items(
    store_key: String,
    range_key: String,
    req_path: FullPath,
    params: HashMap<String, String>,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<impl warp::Reply, warp::Rejection> {
    match try_route_request(&req_path, Method::GET, None, memory.clone(), env.clone()).await {
        Ok(Some(response_envelope)) => {
            return Ok(warp::reply::json(&response_envelope));
        }
        Ok(None) => {
            // handle locally below
        }
        Err(rejection) => return Err(rejection),
    }

    let storage_key = StorageKey::new(
        PartitionKey(store_key),
        if range_key.is_empty() {
            None
        } else {
            Some(RangeKey(range_key))
        },
    );
    // let storage_key: StorageKey = store_key.parse().unwrap();
    let storage_key_string = storage_key.to_string();
    let memory = memory.read().await;

    match &*memory {
        node::NodeState::Joined(node) => {
            let limit = params
                .get("limit")
                .map(|v| v.parse::<usize>().unwrap_or(10))
                .unwrap_or(10);

            let store_ref = env.get_store();

            let item_entries = match node.get_items(limit, &storage_key, store_ref).await {
                Ok(item_entry) => item_entry,
                Err(e) => {
                    let response = ItemOpsResponseEnvelope {
                        success: None,
                        error: Some(format!("Item retrieval error: {e}")),
                    };
                    return Ok(warp::reply::json(&response));
                }
            };

            if !item_entries.is_empty() {
                let response = ItemOpsResponseEnvelope {
                    success: Some(
                        item_entries
                            .iter()
                            .cloned()
                            .map(|ie| ie.item.into())
                            .collect(),
                    ),
                    error: None,
                };
                Ok(warp::reply::json(&response))
            } else {
                let response = ItemOpsResponseEnvelope {
                    success: None,
                    error: Some(format!("No items found: {storage_key_string}")),
                };
                Ok(warp::reply::json(&response))
            }
        }
        _ => {
            let response = ItemOpsResponseEnvelope {
                success: None,
                error: Some(CANNOT_PERFORM_ACTION_IN_CURRENT_STATE.to_string()),
            };
            Ok(warp::reply::json(&response))
        }
    }
}

async fn handle_get_item_count(
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let memory = memory.read().await;

    match &*memory {
        node::NodeState::Joined(node) => {
            let tasks_count = node.get_all_items_count();
            let local_partition_counts = node.get_local_partition_item_count(env.get_store()).await;

            let response = ItemGenericResponseEnvelope {
                success: Some(
                    vec![
                        (
                            "all_item_count".to_string(),
                            serde_json::Value::String(tasks_count.to_string()),
                        ),
                        (
                            "local_partition_counts".to_string(),
                            serde_json::to_value(&local_partition_counts)
                                .unwrap_or(serde_json::Value::Null),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                ),
                error: None,
            };
            Ok(warp::reply::json(&response))
        }
        _ => {
            let response = ItemGenericResponseEnvelope {
                success: None,
                error: Some(CANNOT_PERFORM_ACTION_IN_CURRENT_STATE.to_string()),
            };
            Ok(warp::reply::json(&response))
        }
    }
}

async fn handle_remove_item(
    store_key: String,
    range_key: String,
    req_path: FullPath,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let partition_count = {
        let memory = memory.read().await;
        if let Some(node) = memory.as_joined_node() {
            node.cluster.partition_count
        } else {
            return Err(warp::reject::custom(WebError::NodeNotReady(
                "Node is not in Joined state".to_string(),
            )));
        }
    };

    match try_route_request(&req_path, Method::DELETE, None, memory.clone(), env.clone()).await {
        Ok(Some(response_envelope)) => {
            return Ok(warp::reply::with_status(
                warp::reply::json(&response_envelope),
                warp::http::StatusCode::OK,
            ));
        }
        Ok(None) => {
            // handle locally below
        }
        Err(rejection) => return Err(rejection),
    }

    let storage_key = StorageKey::new(
        PartitionKey(store_key),
        if range_key.is_empty() {
            None
        } else {
            Some(RangeKey(range_key))
        },
    );

    // Phase 1: Prepare (Read Lock is enough now)
    let item_entry = {
        let memory = memory.read().await;
        if let Some(node) = memory.as_joined_node() {
            if !node.is_ready() {
                return Err(warp::reject::custom(WebError::NodeNotReady(
                    CANNOT_PERFORM_ACTION_IN_CURRENT_STATE.to_string(),
                )));
            }
            Ok(node.prepare_local_item(&storage_key, vec![], ItemStatus::Tombstone(now_millis())))
        } else {
            Err(warp::reject::custom(WebError::NodeNotReady(
                CANNOT_PERFORM_ACTION_IN_CURRENT_STATE.to_string(),
            )))
        }
    }?;

    let io_result = JoinedNode::insert_items_io_only(
        partition_count,
        vec![item_entry.clone()],
        env.get_store(),
        env.get_wal(),
    )
    .await;

    // Phase 2: Commit (Read Lock is enough now)
    match io_result {
        Ok((_added_keys, reqs, max_hlc)) => {
            match wait_for_sync(memory.clone(), reqs).await {
                false => {
                    let response = ItemOpsResponseEnvelope {
                        success: None,
                        error: Some("Request failed, not all items synced".to_string()),
                    };
                    return Ok(warp::reply::with_status(
                        warp::reply::json(&response),
                        warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                    ));
                }
                true => {
                    // keep going
                }
            }

            {
                let memory = memory.write().await;
                if let Some(node) = memory.as_joined_node() {
                    node.node_hlc_item_merge(max_hlc);
                }
            }

            let response = ItemOpsResponseEnvelope {
                success: Some(vec![ItemResponse::from(item_entry.item)]),
                error: None,
            };
            Ok(warp::reply::with_status(
                warp::reply::json(&response),
                warp::http::StatusCode::OK,
            ))
        }
        Err(e) => {
            let response = ItemOpsResponseEnvelope {
                success: None,
                error: Some(format!("Item deletion error: {e}")),
            };
            Ok(warp::reply::with_status(
                warp::reply::json(&response),
                warp::http::StatusCode::OK,
            ))
        }
    }
}

async fn handle_remove_item_without_range(
    store_key: String,
    req_path: FullPath,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<impl warp::Reply, warp::Rejection> {
    handle_remove_item(store_key, "".to_string(), req_path, memory, env).await
}

async fn handle_compute_items(
    partition_id: String,
    req_path: FullPath,
    compute_req: ComputeRequest,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let partition_key = PartitionKey(partition_id);

    let routing_result = decide_routing(&partition_key, &Method::POST, memory.clone()).await;
    let routing = match routing_result {
        Ok(decision) => decision,
        Err(envelope) => return Ok(warp::reply::json(&envelope)),
    };

    match routing {
        RouteDecision::Remote { target } => {
            match route_request::<ComputeRequest, ItemGenericResponseEnvelope>(
                env.get_http_client(),
                &target,
                req_path.as_str(),
                &Method::POST,
                Some(&compute_req),
            )
            .await
            {
                Ok(remote_response) => Ok(warp::reply::json(&remote_response)),
                Err(err) => Ok(warp::reply::json(&ItemGenericResponseEnvelope {
                    success: None,
                    error: Some(format!("Routing error: {err}")),
                })),
            }
        }
        RouteDecision::Local => {
            let memory = memory.read().await;

            match &*memory {
                node::NodeState::Joined(node) => {
                    let limit = 1000; // Default limit for compute
                    let storage_key = StorageKey::new(partition_key, None);
                    let store_ref = env.get_store();

                    let item_entries = match node.get_items(limit, &storage_key, store_ref).await {
                        Ok(item_entry) => item_entry,
                        Err(e) => {
                            let response = ItemGenericResponseEnvelope {
                                success: None,
                                error: Some(format!("Item retrieval error for compute: {e}")),
                            };
                            return Ok(warp::reply::json(&response));
                        }
                    };

                    match crate::compute::execute_lua(item_entries.into_iter(), &compute_req.script)
                    {
                        Ok(result) => {
                            let response = ItemGenericResponseEnvelope {
                                success: Some(
                                    vec![("result".to_string(), result)].into_iter().collect(),
                                ),
                                error: None,
                            };
                            Ok(warp::reply::json(&response))
                        }
                        Err(e) => {
                            let response = ItemGenericResponseEnvelope {
                                success: None,
                                error: Some(format!("Lua execution error: {e}")),
                            };
                            Ok(warp::reply::json(&response))
                        }
                    }
                }
                _ => {
                    let response = ItemGenericResponseEnvelope {
                        success: None,
                        error: Some(CANNOT_PERFORM_ACTION_IN_CURRENT_STATE.to_string()),
                    };
                    Ok(warp::reply::json(&response))
                }
            }
        }
    }
}

pub async fn web_server_task(
    listen_on_address: NodeAddress,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    let post_item = warp::path!("items")
        .and(warp::post())
        .and(warp::path::full())
        .and(warp::body::json())
        .and(with_memory(memory.clone()))
        .and(with_env(env.clone()))
        .and_then(handle_post_item);

    let get_items_count = warp::path!("items")
        .and(warp::get())
        .and(with_memory(memory.clone()))
        .and(with_env(env.clone()))
        .and_then(handle_get_item_count);

    let get_item = warp::path!("items" / String / String)
        .and(warp::get())
        .and(warp::path::full())
        .and(warp::query::<HashMap<String, String>>())
        .and(with_memory(memory.clone()))
        .and(with_env(env.clone()))
        .and_then(handle_get_items);

    let get_items = warp::path!("items" / String)
        .and(warp::get())
        .and(warp::path::full())
        .and(warp::query::<HashMap<String, String>>())
        .and(with_memory(memory.clone()))
        .and(with_env(env.clone()))
        .and_then(handle_get_items_without_range);

    let remove_item_with_range = warp::path!("items" / String / String)
        .and(warp::delete())
        .and(warp::path::full())
        .and(with_memory(memory.clone()))
        .and(with_env(env.clone()))
        .and_then(handle_remove_item);

    let remove_item = warp::path!("items" / String)
        .and(warp::delete())
        .and(warp::path::full())
        .and(with_memory(memory.clone()))
        .and(with_env(env.clone()))
        .and_then(handle_remove_item_without_range);

    let compute_items = warp::path!("compute" / String)
        .and(warp::post())
        .and(warp::path::full())
        .and(warp::body::json())
        .and(with_memory(memory.clone()))
        .and(with_env(env.clone()))
        .and_then(handle_compute_items);

    let address = listen_on_address
        .as_str()
        .to_string()
        .parse::<SocketAddr>()
        .expect("Failed to parse address for web server");

    let server = warp::serve(
        post_item
            .or(get_item)
            .or(get_items)
            .or(get_items_count)
            .or(remove_item_with_range)
            .or(remove_item)
            .or(compute_items),
    )
    .run(address);

    tokio::select! {
        _ = server => {},
        _ = shutdown_rx.recv() => {
            info!("Shutting down web server");
        }
    }
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn simple_test() {}
}
