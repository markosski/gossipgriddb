mod address;
mod builder;
mod state;
mod types;

pub use address::{NodeAddress, NodeAddressParseError};
pub use builder::NodeBuilder;
pub use types::NodeError;

#[doc(hidden)]
pub use state::{
    DisconnectedNode, JoinedNode, NodeState, PreJoinNode, SimpleNode, SimpleNodeState,
};
#[doc(hidden)]
pub use types::{NodeConfig, NodeId, NodeIdPartitionId, PartitionCount};

use serde::{Deserialize, Serialize};

use crate::clock::now_millis;
use crate::cluster::PartitionId;
use crate::env::Env;
use crate::event_bus::{Event, start_event_loop};
use crate::fs as fs_paths;
use crate::gossip::{handle_messages_task, send_membership_gossip_task};
use crate::sync::LsnOffset;
use crate::{sync, wal, web};
use laminar::{Config, Socket, SocketEvent};
use log::info;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::try_join;

/// Handle that can be used to abort a running node.
/// This handle is cloneable and can be passed to other tasks.
#[derive(Clone)]
pub struct AbortHandle {
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
}

impl AbortHandle {
    /// Signals the node to shutdown.
    pub fn abort(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// Handle to a running node runtime.
/// Allows for programmatic shutdown and waiting for the node to stop.
pub struct NodeRuntime {
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    handle: tokio::task::JoinHandle<Result<(), NodeError>>,
}

impl NodeRuntime {
    /// Returns an AbortHandle that can be used to signal shutdown.
    pub fn abort_handle(&self) -> AbortHandle {
        AbortHandle {
            shutdown_tx: self.shutdown_tx.clone(),
        }
    }

    /// Signals the node to shutdown.
    pub fn abort(&self) {
        let _ = self.shutdown_tx.send(());
    }

    /// Waits for the node to shutdown and returns any errors encountered.
    pub async fn wait(self) -> Result<(), NodeError> {
        self.handle
            .await
            .map_err(|e| NodeError::ErrorStartingNode(format!("Node runtime task panicked: {e}")))?
    }

    /// Runs the node until Ctrl+C is received, then performs graceful shutdown.
    ///
    /// This is a convenience method that sets up a Ctrl+C handler and waits
    /// for the node to shutdown. For custom shutdown logic, use `abort_handle()`
    /// and `wait()` directly.
    pub async fn run_until_shutdown(self) -> Result<(), NodeError> {
        let abort_handle = self.abort_handle();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            abort_handle.abort();
        });
        self.wait().await
    }
}

/// Main entry point for starting node instance.
/// This is were all the main background processes are spun up.
/// See examples/demo for CLI implementation to properly start nodes.
///
/// # Example
/// ```
/// let node_state = Arc::new(RwLock::new(NodeState::init(
///     local_addr.clone(),
///     web_port,
///     peer_addr, // Expected for seed node but not for joining node
///     cluster_config, // Expected for seed node but not for joining node
/// )));
///
/// let bus = EventBus::new();
/// let store = Box::new(gossipgrid::store::memory_store::InMemoryStore::new());
///
/// let env: Arc<Env> = Arc::new(env::Env::new(
///     store,
///     Box::new(
///         gossipgrid::WalLocalFile::new("<typically_cluster_name>", true)
///             .await
///             .unwrap(),
///     ),
///     bus,
/// ));
///
/// let node = node::start_node(local_addr, web_port, node_state, env).await?;
///
/// let abort_handle = node.abort_handle();
/// tokio::spawn(async move {
///     if let Ok(()) = tokio::signal::ctrl_c().await {
///         println!("Shutdown signal received");
///         abort_handle.abort();
///     }
/// });
///
/// node.wait().await?;
/// ```
pub async fn start_node(
    local_addr: NodeAddress,
    web_port: u16,
    node_state: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<NodeRuntime, NodeError> {
    let (shutdown_sender, shutdown_receiver) = tokio::sync::broadcast::channel(1);

    let node_address = {
        let node_state = node_state.read().await;
        node_state.get_address().clone()
    };

    info!(
        "node={}; Starting application: {}",
        &node_address.as_str(),
        &local_addr
    );

    // Print cluster configuration if available
    {
        let state = node_state.read().await;
        if let Some(cluster) = (*state).get_cluster_config() {
            info!("Cluster configuration:");
            info!("  Name: {}", cluster.cluster_name);
            info!("  Size: {}", cluster.cluster_size);
            info!("  Partitions: {}", cluster.partition_count);
            info!("  Replication factor: {}", cluster.replication_factor);
            info!("Partition assignments:");
            for (node_idx, (partitions, assignment)) in &cluster.partition_assignments {
                info!(
                    "  Node {}: {} partitions -> {:?}",
                    node_idx,
                    partitions.len(),
                    assignment
                );
            }
        }
    }

    let socket_config = Config {
        heartbeat_interval: Some(Duration::from_secs(3)),
        max_packet_size: 400 * 1024,
        ..Default::default()
    };

    let mut local_open = local_addr.clone();
    local_open.ip = "0.0.0.0".to_string();

    // Start UDP connection and send/receive channels
    let mut socket = Socket::bind_with_config(local_open.as_str(), socket_config).map_err(|e| {
        NodeError::ErrorStartingNode(format!("Failed to bind UDP socket on {local_open}: {e}"))
    })?;
    let sender = Arc::new(socket.get_packet_sender());
    let receiver = Arc::new(socket.get_event_receiver());
    let (udp_send, udp_receive) = tokio::sync::mpsc::channel::<SocketEvent>(1_000_000);

    struct BlockingShutdownGuard {
        tx: std::sync::mpsc::Sender<()>,
    }

    impl Drop for BlockingShutdownGuard {
        fn drop(&mut self) {
            let _ = self.tx.send(());
        }
    }

    // Create a channel to signal the blocking thread to stop
    let (blocking_shutdown_tx, blocking_shutdown_rx) = std::sync::mpsc::channel();
    let shutdown_guard = BlockingShutdownGuard {
        tx: blocking_shutdown_tx,
    };

    tokio::task::spawn_blocking(move || {
        // This call blocks and internally uses manual_poll in a loop, otherwise we cannot cleanly interrupts this process
        loop {
            socket.manual_poll(std::time::Instant::now());
            if blocking_shutdown_rx.try_recv().is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    tokio::task::spawn_blocking(move || {
        // After start_polling, we can drain events and forward them
        while let Ok(evt) = receiver.recv() {
            if udp_send.blocking_send(evt).is_err() {
                break;
            }
        }
    });

    // Start Gossip send message process
    let gossip_sending = tokio::spawn(send_membership_gossip_task(
        node_address.clone(),
        sender.clone(),
        node_state.clone(),
        env.clone(),
        shutdown_receiver.resubscribe(),
    ));

    // Start Gossip receive message process
    let gossip_receiving = tokio::spawn(handle_messages_task(
        node_address.clone(),
        udp_receive,
        sender.clone(),
        node_state.clone(),
        env.clone(),
        shutdown_receiver.resubscribe(),
    ));

    // Start Web server process
    let web_server = tokio::spawn(web::web_server_task(
        NodeAddress::new("0.0.0.0".to_string(), web_port),
        node_state.clone(),
        env.clone(),
        shutdown_receiver.resubscribe(),
    ));

    // Start Sync server process - bind listener first so errors propagate
    let sync_listener = tokio::net::TcpListener::bind(node_address.as_str())
        .await
        .map_err(|e| {
            NodeError::ErrorStartingNode(format!(
                "Failed to bind TCP sync server on {node_address}: {e}"
            ))
        })?;
    let sync_server = tokio::spawn(sync::server_sync_handler_task(
        node_address.clone(),
        sync_listener,
        node_state.clone(),
        env.clone(),
        shutdown_receiver.resubscribe(),
    ));

    // Start Sync client process
    let sync_client = tokio::spawn(sync::client_send_sync_request_task(
        node_address.clone(),
        node_state.clone(),
        env.clone(),
        shutdown_receiver.resubscribe(),
    ));

    // Start WAL flusher
    let flush_wal = tokio::spawn(wal::wal_flush(
        node_address.clone(),
        env.clone(),
        shutdown_receiver.resubscribe(),
    ));

    // Start event loop and hydration
    let event_bus_rx = env
        .get_event_bus()
        .take_receiver()
        .expect("Event channel receiver already taken");

    let event_loop = start_event_loop(
        event_bus_rx,
        node_state.clone(),
        env.clone(),
        shutdown_sender.clone(),
    )
    .await;
    let bus = env.get_event_bus();
    // If this is a new node, we need to start the in memory hydration process if applicable
    bus.emit(Event::StartInMemoryHydration);

    let runtime_handle = tokio::spawn(async move {
        // Ensure the blocking shutdown guard lives as long as this task
        let _guard = shutdown_guard;

        // Panic if one of the tasks fails
        try_join!(
            gossip_receiving,
            gossip_sending,
            web_server,
            sync_server,
            sync_client,
            flush_wal,
            event_loop
        )
        .map_err(|e| NodeError::ErrorStartingNode(format!("Node  failed: {e}")))?;

        Ok(())
    });

    Ok(NodeRuntime {
        shutdown_tx: shutdown_sender,
        handle: runtime_handle,
    })
}

/// Metadata about a node's state, persisted to disk.
///
/// This is used to recover the node's state after a restart, including its
/// position in the replication log (LSN) for each partition it replicates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetadata {
    /// The name of the cluster this node belongs to.
    pub cluster_name: String,
    /// Last seen Hybrid Logical Clock timestamp.
    pub node_hlc_timestamp: u64,
    /// Latest LSN processed for each replicated partition.
    pub replica_partition_lsn: Option<HashMap<PartitionId, LsnOffset>>,
    /// Timestamp when this metadata was persisted.
    pub persisted_at: u64,
}

impl NodeMetadata {
    /// Creates a new `NodeMetadata` instance from a `JoinedNode`.
    pub fn from_joined_node(node: &JoinedNode) -> Self {
        Self {
            cluster_name: node.cluster.cluster_name.clone(),
            node_hlc_timestamp: node.node_hlc.lock().unwrap().timestamp,
            replica_partition_lsn: Some(
                node.sync_state
                    .replica_partition_lsn
                    .iter()
                    .map(|r| (*r.key(), r.value().clone()))
                    .collect(),
            ),
            persisted_at: now_millis(),
        }
    }

    /// Loads `NodeMetadata` from the filesystem for a given cluster and node index.
    pub fn load(cluster_name: &str, node_index: u8) -> Result<Option<Self>, NodeError> {
        let path = fs_paths::node_md_state_path(cluster_name, node_index);
        if !path.exists() {
            return Ok(None);
        }

        let json =
            fs::read_to_string(path).map_err(|e| NodeError::NodePersistenceError(e.to_string()))?;
        let state: NodeMetadata = serde_json::from_str(&json)
            .map_err(|e| NodeError::NodePersistenceError(e.to_string()))?;
        Ok(Some(state))
    }
}
