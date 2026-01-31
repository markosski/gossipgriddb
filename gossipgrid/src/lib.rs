//! GossipGrid - A distributed Key/Value database with partition-aware compute.
//!
//! # Quick Start
//!
//! ```no_run
//! use gossipgrid::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Start a seed node with ephemeral cluster
//!     let node = NodeBuilder::new()
//!         .address("127.0.0.1:4009")?
//!         .web_port(3001)
//!         .ephemeral(3, 9, 2)
//!         .build()
//!         .await?;
//!
//!     // Graceful shutdown on Ctrl+C
//!     let abort_handle = node.abort_handle();
//!     tokio::spawn(async move {
//!         let _ = tokio::signal::ctrl_c().await;
//!         abort_handle.abort();
//!     });
//!
//!     node.wait().await?;
//!     Ok(())
//! }
//! ```

#[doc(hidden)]
pub mod clock;
#[doc(hidden)]
pub mod compute;
#[doc(hidden)]
pub mod env;
#[doc(hidden)]
pub mod event_bus;
#[doc(hidden)]
pub mod function_registry;
#[doc(hidden)]
pub mod util;

pub(crate) mod fs;
pub(crate) mod gossip;
#[doc(hidden)]
pub mod item;
pub(crate) mod sync;
#[doc(hidden)]
pub mod wal;
#[doc(hidden)]
pub use wal::WalLocalFile;
#[doc(hidden)]
pub mod web;

pub mod cluster;
pub mod node;
pub mod prelude;
pub mod store;

pub use cluster::{Cluster, ClusterHealth, ClusterOperationError};
pub use node::{AbortHandle, NodeAddress, NodeBuilder, NodeError, NodeRuntime};
#[cfg(feature = "memory-store")]
pub use store::memory_store::InMemoryStore;
pub use store::{DataStoreError, PartitionKey, RangeKey, StorageKey, Store, StoreEngine};

/// Unified error type for GossipGrid operations.
///
/// This enum wraps all public error types for easier error handling with the `?` operator.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Node(#[from] NodeError),
    #[error(transparent)]
    Cluster(#[from] ClusterOperationError),
    #[error(transparent)]
    Store(#[from] DataStoreError),
}

/// Convenient Result type alias using the unified [`Error`].
pub type Result<T> = std::result::Result<T, Error>;
