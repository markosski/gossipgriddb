//! Convenient re-exports for common GossipGrid types.
//!
//! # Example
//!
//! ```
//! use gossipgrid::prelude::*;
//! ```

pub use crate::{
    AbortHandle, Cluster, NodeAddress, NodeBuilder, NodeError, NodeRuntime, PartitionKey, RangeKey,
    StorageKey, Store, StoreEngine,
};

#[cfg(feature = "memory-store")]
pub use crate::InMemoryStore;
