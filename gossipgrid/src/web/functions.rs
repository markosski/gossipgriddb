use crate::env::Env;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeRequest {
    pub script: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeResponse {
    pub result: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionListResponse {
    pub functions: Vec<String>,
}

pub async fn handle_list_functions(env: Arc<Env>) -> Result<impl warp::Reply, warp::Rejection> {
    let registry = env.get_function_registry();
    let functions = registry.list();

    let response = FunctionListResponse { functions };
    Ok(warp::reply::json(&response))
}
