use crate::clock::HLC;
use crate::clock::now_millis;
use crate::env::Env;
use crate::item::{Item, ItemStatus};
use crate::node::{self, JoinedNode, NodeState};
use crate::store::{PartitionKey, RangeKey, StorageKey};
use crate::web::CANNOT_PERFORM_ACTION_IN_CURRENT_STATE;
use crate::web::try_route_request;
use crate::web::wait_for_sync;
use base64::engine::general_purpose;
use bincode::{Decode, Encode};
use log::debug;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::filters::path::FullPath;

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

pub async fn handle_post_item(
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
        None,
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

pub async fn handle_get_items_without_range(
    store_key: String,
    req_path: FullPath,
    params: HashMap<String, String>,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<impl warp::Reply, warp::Rejection> {
    handle_get_items(store_key, "".to_string(), req_path, params, memory, env).await
}

pub async fn handle_get_items(
    store_key: String,
    range_key: String,
    req_path: FullPath,
    params: HashMap<String, String>,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<impl warp::Reply, warp::Rejection> {
    match try_route_request(
        &req_path,
        Some(&params),
        Method::GET,
        None,
        memory.clone(),
        env.clone(),
    )
    .await
    {
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
    let storage_key_string = storage_key.to_string();
    let memory = memory.read().await;

    match &*memory {
        node::NodeState::Joined(node) => {
            // Check for function execution via 'fn' query param
            let function_name = params.get("fn");

            let limit = params
                .get("limit")
                .map(|v| v.parse::<usize>().unwrap_or(1000))
                .unwrap_or(if function_name.is_some() { 1000 } else { 10 });

            let skip_null_rk = params
                .get("skip_null_rk")
                .map(|v| v.parse::<bool>().unwrap_or(false))
                .unwrap_or(false);

            let store_ref = env.get_store();

            let item_entries = match node
                .get_items(limit, skip_null_rk, &storage_key, store_ref)
                .await
            {
                Ok(item_entry) => item_entry,
                Err(e) => {
                    let response = ItemOpsResponseEnvelope {
                        success: None,
                        error: Some(format!("Item retrieval error: {e}")),
                    };
                    return Ok(warp::reply::json(&response));
                }
            };

            // If 'fn' param is present, execute the registered function
            if let Some(fn_name) = function_name {
                let registry = env.get_function_registry();
                match registry.get(fn_name) {
                    Some(func) => {
                        let start = std::time::Instant::now();
                        let item_count = item_entries.len();
                        let compute_result =
                            crate::compute::execute_lua_async(item_entries, &func.script).await;
                        debug!(
                            "Lua execution: {:?} for {} items",
                            start.elapsed(),
                            item_count
                        );

                        match compute_result {
                            Ok(result) => {
                                let response = ItemGenericResponseEnvelope {
                                    success: Some(
                                        vec![("result".to_string(), result)].into_iter().collect(),
                                    ),
                                    error: None,
                                };
                                return Ok(warp::reply::json(&response));
                            }
                            Err(e) => {
                                let response = ItemGenericResponseEnvelope {
                                    success: None,
                                    error: Some(format!("Function execution error: {e}")),
                                };
                                return Ok(warp::reply::json(&response));
                            }
                        }
                    }
                    None => {
                        let response = ItemGenericResponseEnvelope {
                            success: None,
                            error: Some(format!("Function not found: {fn_name}")),
                        };
                        return Ok(warp::reply::json(&response));
                    }
                }
            }

            // Normal item retrieval
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

pub async fn handle_get_item_count(
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

pub async fn handle_remove_item(
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

    match try_route_request(
        &req_path,
        None,
        Method::DELETE,
        None,
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

pub async fn handle_remove_item_without_range(
    store_key: String,
    req_path: FullPath,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<impl warp::Reply, warp::Rejection> {
    handle_remove_item(store_key, "".to_string(), req_path, memory, env).await
}
