//! # gossipgrid-client
//!
//! A topology-aware Rust client for GossipGridDB that routes requests
//! directly to the correct partition leader or replica, eliminating
//! unnecessary proxy hops.
//!
//! ## Quick Start
//!
//! ```no_run
//! use gossipgrid_client::GossipGridClient;
//! use std::time::Duration;
//!
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = GossipGridClient::builder()
//!         .seed_nodes(vec!["127.0.0.1:3001".to_string()])
//!         .heartbeat_interval(Duration::from_secs(3))
//!         .build()
//!         .await?;
//!
//!     // Put an item
//!     client.put("my_store", "my_range", b"hello world").await?;
//!
//!     // Get an item
//!     let item = client.get("my_store", "my_range").await?;
//!
//!     // Delete an item
//!     client.delete("my_store", "my_range").await?;
//!
//!     // Shutdown the client (stops heartbeat)
//!     client.shutdown().await;
//!     Ok(())
//! }
//! ```

pub mod error;
pub mod routing;
pub mod topology;
pub mod types;

pub use error::ClientError;
pub use topology::{NodeInfo, TopologySnapshot};
pub use types::{NodeAddress, PartitionId};

use log::{error, info};
use routing::Operation;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(3);

/// Builder for constructing a [`GossipGridClient`].
pub struct ClientBuilder {
    seed_nodes: Vec<String>,
    heartbeat_interval: Duration,
}

impl ClientBuilder {
    fn new() -> Self {
        ClientBuilder {
            seed_nodes: Vec::new(),
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
        }
    }

    /// Set the seed node addresses (host:port of the HTTP API).
    /// At least one seed node is required.
    pub fn seed_nodes(mut self, seeds: Vec<String>) -> Self {
        self.seed_nodes = seeds;
        self
    }

    /// Set the heartbeat interval for topology refresh.
    /// Default: 3 seconds.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Build the client: fetches initial topology and spawns the heartbeat task.
    pub async fn build(self) -> Result<GossipGridClient, ClientError> {
        if self.seed_nodes.is_empty() {
            return Err(ClientError::NoHealthyNodes);
        }

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("Failed to create HTTP client");

        let initial_topology =
            TopologySnapshot::fetch_from_seeds(&http_client, &self.seed_nodes).await?;

        info!(
            "Connected to cluster '{}' ({} nodes, {} partitions)",
            initial_topology.cluster_name,
            initial_topology.nodes.len(),
            initial_topology.partition_count
        );

        let topology = Arc::new(RwLock::new(initial_topology));
        let seeds = Arc::new(self.seed_nodes);

        // Spawn heartbeat task
        let shutdown_tx = tokio::sync::watch::channel(false).0;
        let shutdown_rx = shutdown_tx.subscribe();

        let heartbeat_handle = {
            let topology = topology.clone();
            let seeds = seeds.clone();
            let http_client = http_client.clone();
            let interval = self.heartbeat_interval;
            tokio::spawn(heartbeat_task(
                topology,
                seeds,
                http_client,
                interval,
                shutdown_rx,
            ))
        };

        Ok(GossipGridClient {
            http_client,
            topology,
            seeds,
            shutdown_tx,
            _heartbeat_handle: heartbeat_handle,
        })
    }
}

/// A topology-aware client for GossipGridDB.
///
/// This client maintains a local copy of the cluster topology and routes
/// requests directly to the correct partition leader (for writes) or
/// replica (for reads), eliminating unnecessary proxy hops.
///
/// Created via [`GossipGridClient::builder()`].
pub struct GossipGridClient {
    http_client: reqwest::Client,
    topology: Arc<RwLock<TopologySnapshot>>,
    seeds: Arc<Vec<String>>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    _heartbeat_handle: tokio::task::JoinHandle<()>,
}

impl GossipGridClient {
    /// Create a new builder for the client.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Put (create/update) an item.
    ///
    /// Routes to the partition leader for the given store key.
    pub async fn put(
        &self,
        store_key: &str,
        range_key: &str,
        value: &[u8],
    ) -> Result<serde_json::Value, ClientError> {
        let snapshot = self.topology.read().await;
        let (node, _partition_id) =
            routing::resolve_target(&snapshot, store_key, Operation::Write)?;
        let url = format!("{}/items", node.address.http_base_url(node.web_port));
        drop(snapshot);

        let body = serde_json::json!({
            "partition_key": store_key,
            "range_key": range_key,
            "message": base64_encode(value),
        });

        let response = self.http_client.post(&url).json(&body).send().await;

        match response {
            Ok(resp) => self.handle_response(resp).await,
            Err(e) => {
                let addr = url.clone();
                self.trigger_topology_refresh().await;
                Err(ClientError::ConnectionFailed {
                    address: addr,
                    source: e,
                })
            }
        }
    }

    /// Get an item by store key and range key.
    ///
    /// Routes to a healthy replica for the partition, falling back to the leader.
    pub async fn get(
        &self,
        store_key: &str,
        range_key: &str,
    ) -> Result<serde_json::Value, ClientError> {
        let snapshot = self.topology.read().await;
        let (node, _partition_id) = routing::resolve_target(&snapshot, store_key, Operation::Read)?;
        let url = format!(
            "{}/items/{}/{}",
            node.address.http_base_url(node.web_port),
            store_key,
            range_key
        );
        drop(snapshot);

        let response = self.http_client.get(&url).send().await;

        match response {
            Ok(resp) => self.handle_response(resp).await,
            Err(e) => {
                let addr = url.clone();
                self.trigger_topology_refresh().await;
                Err(ClientError::ConnectionFailed {
                    address: addr,
                    source: e,
                })
            }
        }
    }

    /// Get items by store key only (without range key).
    ///
    /// Routes to a healthy replica for the partition, falling back to the leader.
    pub async fn get_by_partition(
        &self,
        store_key: &str,
    ) -> Result<serde_json::Value, ClientError> {
        let snapshot = self.topology.read().await;
        let (node, _partition_id) = routing::resolve_target(&snapshot, store_key, Operation::Read)?;
        let url = format!(
            "{}/items/{}",
            node.address.http_base_url(node.web_port),
            store_key
        );
        drop(snapshot);

        let response = self.http_client.get(&url).send().await;

        match response {
            Ok(resp) => self.handle_response(resp).await,
            Err(e) => {
                let addr = url.clone();
                self.trigger_topology_refresh().await;
                Err(ClientError::ConnectionFailed {
                    address: addr,
                    source: e,
                })
            }
        }
    }

    /// Delete an item by store key and range key.
    ///
    /// Routes to the partition leader.
    pub async fn delete(
        &self,
        store_key: &str,
        range_key: &str,
    ) -> Result<serde_json::Value, ClientError> {
        let snapshot = self.topology.read().await;
        let (node, _partition_id) =
            routing::resolve_target(&snapshot, store_key, Operation::Delete)?;
        let url = format!(
            "{}/items/{}/{}",
            node.address.http_base_url(node.web_port),
            store_key,
            range_key
        );
        drop(snapshot);

        let response = self.http_client.delete(&url).send().await;

        match response {
            Ok(resp) => self.handle_response(resp).await,
            Err(e) => {
                let addr = url.clone();
                self.trigger_topology_refresh().await;
                Err(ClientError::ConnectionFailed {
                    address: addr,
                    source: e,
                })
            }
        }
    }

    /// Gracefully shut down the client, stopping the heartbeat task.
    pub async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Trigger an immediate out-of-band topology refresh.
    async fn trigger_topology_refresh(&self) {
        let current = self.topology.read().await.clone();
        match current.refresh(&self.http_client, &self.seeds).await {
            Ok(new_snapshot) => {
                *self.topology.write().await = new_snapshot;
                info!("Topology refreshed after connection error");
            }
            Err(e) => {
                error!("Failed to refresh topology: {e}");
            }
        }
    }

    /// Handle an HTTP response, converting non-success status codes to errors.
    async fn handle_response(
        &self,
        response: reqwest::Response,
    ) -> Result<serde_json::Value, ClientError> {
        let status = response.status();
        if status.is_success() {
            let body: serde_json::Value =
                response
                    .json()
                    .await
                    .map_err(|e| ClientError::ServerError {
                        status: status.as_u16(),
                        message: format!("Failed to parse response: {e}"),
                    })?;
            Ok(body)
        } else {
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            Err(ClientError::ServerError {
                status: status.as_u16(),
                message,
            })
        }
    }
}

/// Background heartbeat task that periodically refreshes the topology.
async fn heartbeat_task(
    topology: Arc<RwLock<TopologySnapshot>>,
    seeds: Arc<Vec<String>>,
    http_client: reqwest::Client,
    interval: Duration,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                let current = topology.read().await.clone();
                match current.refresh(&http_client, &seeds).await {
                    Ok(new_snapshot) => {
                        *topology.write().await = new_snapshot;
                    }
                    Err(e) => {
                        error!("Heartbeat topology refresh failed: {e}");
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                info!("Heartbeat task shutting down");
                return;
            }
        }
    }
}

/// Simple base64 encoding for binary values.
fn base64_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(data.len() * 4 / 3 + 4);
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        let _ = write!(s, "{}", CHARS[((n >> 18) & 0x3F) as usize] as char);
        let _ = write!(s, "{}", CHARS[((n >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            let _ = write!(s, "{}", CHARS[((n >> 6) & 0x3F) as usize] as char);
        } else {
            s.push('=');
        }
        if chunk.len() > 2 {
            let _ = write!(s, "{}", CHARS[(n & 0x3F) as usize] as char);
        } else {
            s.push('=');
        }
    }
    s
}
