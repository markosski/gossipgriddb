//! Storage abstraction layer for GossipGrid.
//!
//! This module provides the [`Store`] and [`StoreEngine`] traits for implementing
//! custom storage backends.
//!
//! When the `memory-store` feature is enabled (default), the built-in
//! [`InMemoryStore`](memory_store::InMemoryStore) is available.
//!
//! # Key Types
//!
//! - [`PartitionKey`] - The primary key used for partitioning data
//! - [`RangeKey`] - Optional secondary key for ordering within a partition  
//! - [`StorageKey`] - Composite key combining partition and range keys

#[cfg(feature = "memory-store")]
pub mod memory_store;

use std::{collections::HashMap, str::FromStr};

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    cluster::PartitionId,
    item::{Item, ItemEntry},
};

/// Primary key used for data partitioning.
///
/// Items with the same partition key are stored on the same set of nodes.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode, Serialize, Deserialize,
)]
pub struct PartitionKey(pub String);
impl PartitionKey {
    pub fn value(&self) -> &str {
        &self.0
    }
}

/// Secondary key for ordering items within a partition.
///
/// When combined with a [`PartitionKey`], allows storing multiple related items.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode, Serialize, Deserialize,
)]
pub struct RangeKey(pub String);
impl RangeKey {
    pub fn value(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode, Serialize, Deserialize)]
pub struct StorageKey {
    pub partition_key: PartitionKey,
    pub range_key: Option<RangeKey>,
}

impl StorageKey {
    pub fn new(partition_key: PartitionKey, range_key: Option<RangeKey>) -> Self {
        StorageKey {
            partition_key,
            range_key,
        }
    }
}

impl std::fmt::Display for StorageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.range_key {
            Some(rk) => write!(f, "{}_{}", self.partition_key.value(), rk.value()),
            None => write!(f, "{}", self.partition_key.value()),
        }
    }
}

impl FromStr for StorageKey {
    type Err = DataStoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.splitn(2, '_').collect();
        let partition_key = PartitionKey(parts[0].to_string());
        let range_key = if parts.len() > 1 {
            Some(RangeKey(parts[1].to_string()))
        } else {
            None
        };
        Ok(StorageKey::new(partition_key, range_key))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct GetManyOptions {
    pub limit: usize,
    pub skip_null_rk: bool,
}

impl Default for GetManyOptions {
    fn default() -> Self {
        Self {
            limit: 100,
            skip_null_rk: false,
        }
    }
}

#[async_trait::async_trait]
pub trait StoreEngine: Send + Sync {
    /// Is this purely in-memory implementation?
    fn is_in_memory_store(&self) -> bool;

    /// Get item by key and range_key
    async fn get(
        &self,
        partition: &PartitionId,
        key: &StorageKey,
    ) -> Result<Option<ItemEntry>, DataStoreError>;

    /// Options for get_many operation.
    /// Returns items sorted in descending order by range key within a partition.
    async fn get_many(
        &self,
        partition: &PartitionId,
        key: &StorageKey,
        options: GetManyOptions,
    ) -> Result<Vec<ItemEntry>, DataStoreError>;

    /// Insert item by key and range_key
    /// If an item with the same key and range_key exists, it should be updated
    async fn insert(
        &self,
        partition: &PartitionId,
        key: &StorageKey,
        value: Item,
    ) -> Result<(), DataStoreError>;

    /// Remove item by key and optional range_key
    /// If range_key is None, remove all items with the given key
    async fn remove(&self, partition: &PartitionId, key: &StorageKey)
    -> Result<(), DataStoreError>;

    /// Get counts of items per partition
    /// Counts should exclude deleted items
    async fn partition_counts(&self) -> Result<HashMap<PartitionId, usize>, DataStoreError>;
}

#[async_trait::async_trait]
pub trait Store: StoreEngine {}
impl<T: StoreEngine> Store for T {}

#[derive(Error, Debug)]
pub enum DataStoreError {
    #[error("Store `Get Item` operation error: {0}")]
    StoreGetOperationError(String),
    #[error("Store `Insert Item` operation error: {0}")]
    StoreInsertOperationError(String),
    #[error("Store `Remove Item` operation error: {0}")]
    StoreRemoveOperationError(String),
    #[error("Store general operation error: {0}")]
    StoreOperationError(String),
    #[error("Storage key parsing error: {0}")]
    StorageKeyParsingError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_storage_key_with_pk_and_rk() {
        let parsed_storage_key = "partition1_range1".parse::<StorageKey>().unwrap();
        let expected_storage_key = StorageKey::new(
            PartitionKey("partition1".to_string()),
            Some(RangeKey("range1".to_string())),
        );

        assert_eq!(parsed_storage_key, expected_storage_key);
    }

    #[test]
    fn test_parse_storage_key_with_pk_and_no_rk() {
        let parsed_storage_key = "partition1".parse::<StorageKey>().unwrap();
        let expected_storage_key = StorageKey::new(PartitionKey("partition1".to_string()), None);

        assert_eq!(parsed_storage_key, expected_storage_key);
    }
}
