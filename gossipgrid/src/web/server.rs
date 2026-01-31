use crate::env::Env;
use crate::node::{NodeAddress, NodeState};
use crate::web::cluster::{handle_get_cluster, handle_resize_cluster};
use crate::web::functions::handle_list_functions;
use crate::web::items::{
    handle_get_item_count, handle_get_items, handle_get_items_without_range, handle_post_item,
    handle_remove_item, handle_remove_item_without_range,
};
use log::info;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::Filter;

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

    // Function registry routes

    let list_functions = warp::path!("functions")
        .and(warp::get())
        .and(with_env(env.clone()))
        .and_then(handle_list_functions);

    let get_cluster = warp::path!("cluster")
        .and(warp::get())
        .and(with_memory(memory.clone()))
        .and_then(handle_get_cluster);

    let resize_cluster = warp::path!("cluster" / "resize")
        .and(warp::post())
        .and(warp::body::json())
        .and(with_memory(memory.clone()))
        .and_then(handle_resize_cluster);

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
            .or(list_functions)
            .or(get_cluster)
            .or(resize_cluster),
    )
    .run(address);

    tokio::select! {
        _ = server => {},
        _ = shutdown_rx.recv() => {
            info!("Shutting down web server");
        }
    }
}
