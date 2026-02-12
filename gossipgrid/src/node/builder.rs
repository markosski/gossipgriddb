//! Builder pattern for creating and starting GossipGrid nodes.
//!
//! # Example
//!
//! ```no_run
//! use gossipgrid::node::NodeBuilder;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Start a seed node with ephemeral cluster
//!     let node = NodeBuilder::new()
//!         .address("127.0.0.1:4009")?
//!         .web_port(3001)
//!         .ephemeral(3, 9, 2)  // cluster_size=3, partitions=9, replication=2
//!         .build()
//!         .await?;
//!
//!     // Start a joining node
//!     let joining_node = NodeBuilder::new()
//!         .address("127.0.0.1:4010")?
//!         .web_port(3002)
//!         .join_peer("127.0.0.1:4009")?
//!         .build()
//!         .await?;
//!
//!     // Graceful shutdown
//!     node.run_until_shutdown().await?;
//!     joining_node.run_until_shutdown().await?;
//!     Ok(())
//! }
//! ```

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::cluster::Cluster;
use crate::env::Env;
use crate::event_bus::EventBus;
use crate::store::Store;
use crate::store::memory_store::InMemoryStore;
use gossipgrid_wal::WalLocalFile;

use super::address::NodeAddress;
use super::state::NodeState;
use super::types::NodeError;
use super::{NodeRuntime, start_node};

static EPHEMERAL: &str = "ephemeral";

/// Builder for creating and starting a GossipGrid node.
///
/// Use this builder to configure a node before starting it. At minimum,
/// you must specify an address and web port. For a seed node, also specify
/// a cluster configuration (via `cluster()` or `ephemeral()`). For a joining
/// node, specify a peer to connect to via `join_peer()`.
pub struct NodeBuilder {
    address: Option<NodeAddress>,
    web_port: Option<u16>,
    join_peer: Option<NodeAddress>,
    cluster_config: Option<Cluster>,
    store: Option<Box<dyn Store + Send + Sync>>,
    is_ephemeral: bool,
    functions_path: Option<std::path::PathBuf>,
}

#[derive(serde::Deserialize)]
struct FunctionConfig {
    name: String,
    script: String,
}

impl Default for NodeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeBuilder {
    /// Create a new NodeBuilder with default settings.
    pub fn new() -> Self {
        Self {
            address: None,
            web_port: None,
            join_peer: None,
            cluster_config: None,
            store: None,
            is_ephemeral: false,
            functions_path: None,
        }
    }

    /// Set a path to a JSON file containing functions to register at startup.
    ///
    /// The file should contain a JSON array of objects with "name" and "script" fields.
    pub fn with_functions(mut self, path: std::path::PathBuf) -> Self {
        self.functions_path = Some(path);
        self
    }

    /// Set the address for gossip and sync communication.
    ///
    /// This is the address other nodes will use to connect to this node.
    /// Format: "host:port" (e.g., "127.0.0.1:4009")
    pub fn address(mut self, addr: &str) -> Result<Self, NodeError> {
        self.address = Some(
            addr.try_into()
                .map_err(|e| NodeError::ConfigurationError(format!("Invalid address: {e}")))?,
        );
        Ok(self)
    }

    /// Set the HTTP API port for this node.
    ///
    /// This port is used for the REST API (items endpoint, status, etc.).
    pub fn web_port(mut self, port: u16) -> Self {
        self.web_port = Some(port);
        self
    }

    /// Set a peer address to join an existing cluster.
    ///
    /// When set, this node will attempt to join the cluster by contacting
    /// the specified peer during startup.
    pub fn join_peer(mut self, addr: &str) -> Result<Self, NodeError> {
        self.join_peer =
            Some(addr.try_into().map_err(|e| {
                NodeError::ConfigurationError(format!("Invalid peer address: {e}"))
            })?);
        Ok(self)
    }

    /// Set a peer address to join an ephemeral cluster.
    ///
    /// Use this when joining a cluster that was started with `.ephemeral()`.
    /// This ensures WAL truncation and proper ephemeral mode handling.
    pub fn join_ephemeral(mut self, addr: &str) -> Result<Self, NodeError> {
        self.join_peer =
            Some(addr.try_into().map_err(|e| {
                NodeError::ConfigurationError(format!("Invalid peer address: {e}"))
            })?);
        self.is_ephemeral = true;
        Ok(self)
    }

    /// Set a custom cluster configuration.
    ///
    /// Use this for persistent clusters where the configuration should
    /// be loaded from disk or specified explicitly.
    pub fn cluster(mut self, config: Cluster) -> Self {
        self.cluster_config = Some(config);
        self
    }

    /// Create an ephemeral in-memory cluster.
    ///
    /// This is useful for testing or development. The cluster state
    /// will be lost when all nodes are stopped.
    ///
    /// # Arguments
    /// * `cluster_size` - Expected number of nodes in the cluster
    /// * `partition_count` - Number of data partitions
    /// * `replication_factor` - Number of replicas for each partition
    pub fn ephemeral(
        mut self,
        cluster_size: u8,
        partition_count: u16,
        replication_factor: u8,
    ) -> Self {
        self.cluster_config = Some(Cluster::new(
            EPHEMERAL.to_string(),
            cluster_size,
            0, // this_node_index will be assigned
            partition_count,
            replication_factor,
            true, // is_ephemeral
        ));
        self.is_ephemeral = true;
        self
    }

    /// Set a custom store implementation.
    ///
    /// By default, an `InMemoryStore` is used. Use this to provide
    /// a custom persistent store implementation.
    pub fn store(mut self, store: Box<dyn Store + Send + Sync>) -> Self {
        self.store = Some(store);
        self
    }

    /// Build and start the node.
    ///
    /// Returns a `NodeRuntime` handle that can be used to control
    /// the node's lifecycle (abort, wait for shutdown).
    ///
    /// # Errors
    /// Returns an error if:
    /// - Required configuration (address, web_port) is missing
    /// - Neither cluster config nor join_peer is specified
    /// - The node fails to start (port binding, etc.)
    pub async fn build(self) -> Result<NodeRuntime, NodeError> {
        let address = self
            .address
            .ok_or_else(|| NodeError::ConfigurationError("Address is required".to_string()))?;

        let web_port = self
            .web_port
            .ok_or_else(|| NodeError::ConfigurationError("Web port is required".to_string()))?;

        // Validate: need either cluster config or join_peer
        if self.cluster_config.is_none() && self.join_peer.is_none() {
            return Err(NodeError::ConfigurationError(
                "Either cluster configuration or join_peer must be specified".to_string(),
            ));
        }

        // Create store (default to InMemoryStore)
        let store: Box<dyn Store + Send + Sync> = self
            .store
            .unwrap_or_else(|| Box::new(InMemoryStore::default()));

        // Determine WAL namespace
        let wal_namespace = if self.is_ephemeral {
            EPHEMERAL.to_string()
        } else if let Some(ref config) = self.cluster_config {
            config.cluster_name.clone()
        } else {
            EPHEMERAL.to_string()
        };

        // Create WAL
        let wal = WalLocalFile::new(crate::fs::wal_dir(&wal_namespace), self.is_ephemeral)
            .await
            .map_err(|e| NodeError::ConfigurationError(format!("Failed to create WAL: {e}")))?;

        // Create EventBus and Env
        let bus = EventBus::new();
        let env: Arc<Env> = Arc::new(Env::new(store, Box::new(wal), bus));

        // Create NodeState
        let node_state = Arc::new(RwLock::new(NodeState::init(
            address.clone(),
            web_port,
            self.join_peer,
            self.cluster_config,
        )));

        // Load static functions if provided
        if let Some(path) = self.functions_path {
            match std::fs::read_to_string(&path) {
                Ok(content) => match serde_json::from_str::<Vec<FunctionConfig>>(&content) {
                    Ok(functions) => {
                        for func in functions {
                            match env
                                .get_function_registry()
                                .register(func.name.clone(), func.script)
                            {
                                Ok(()) => log::info!("Registered function '{}'", func.name),
                                Err(e) => log::error!(
                                    "Failed to register function '{}': {}",
                                    func.name,
                                    e
                                ),
                            }
                        }
                    }
                    Err(e) => log::error!("Failed to parse functions file {path:?}: {e}"),
                },
                Err(e) => log::error!("Failed to read functions file {path:?}: {e}"),
            }
        }

        // Start the node
        start_node(address, web_port, node_state, env).await
    }
}
