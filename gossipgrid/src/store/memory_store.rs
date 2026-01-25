use dashmap::DashMap;
use log::{debug, info};
use std::collections::{BTreeMap, HashMap};

use crate::cluster::PartitionId;
use crate::item::{Item, ItemEntry, ItemStatus};
use crate::store::{DataStoreError, PartitionKey, RangeKey, StorageKey, StoreEngine};

pub struct InMemoryStore {
    item_partitions: DashMap<PartitionId, DashMap<PartitionKey, BTreeMap<Option<RangeKey>, Item>>>,
    partition_counts: DashMap<PartitionId, usize>,
}

impl Default for InMemoryStore {
    fn default() -> Self {
        InMemoryStore {
            item_partitions: DashMap::new(),
            partition_counts: DashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl StoreEngine for InMemoryStore {
    fn is_in_memory_store(&self) -> bool {
        true
    }

    async fn get(
        &self,
        partition_id: &PartitionId,
        key: &StorageKey,
    ) -> Result<Option<ItemEntry>, DataStoreError> {
        let maybe_partition = self.item_partitions.get(partition_id);
        if let Some(pk_map) = maybe_partition
            && let Some(rk_map_ref) = pk_map.get(&key.partition_key)
        {
            let rk_map = rk_map_ref.value();
            if let Some(item) = rk_map.get(&key.range_key)
                && item.status == ItemStatus::Active
            {
                return Ok(Some(ItemEntry::new(key.clone(), item.clone())));
            }
        }
        Ok(None)
    }

    async fn get_many(
        &self,
        partition_id: &PartitionId,
        key: &StorageKey,
        limit: usize,
    ) -> Result<Vec<ItemEntry>, DataStoreError> {
        let maybe_partition = self.item_partitions.get(partition_id);
        let mut data: Vec<ItemEntry> = vec![];
        let mut counter = 0;

        if let Some(pk_map) = maybe_partition
            && let Some(rk_map_ref) = pk_map.get(&key.partition_key)
        {
            let rk_map = rk_map_ref.value();
            for (rk, item) in rk_map.iter() {
                if counter == limit {
                    break;
                }

                if item.status != ItemStatus::Active {
                    continue;
                }

                let match_rk = match (&key.range_key, rk) {
                    (Some(filter_rk), Some(entry_rk)) => {
                        entry_rk.value().contains(filter_rk.value())
                    }
                    (None, _) => true,
                    (Some(_), None) => false,
                };

                if match_rk {
                    data.push(ItemEntry {
                        storage_key: StorageKey::new(key.partition_key.clone(), rk.clone()),
                        item: item.clone(),
                    });
                    counter += 1;
                }
            }
        }
        Ok(data)
    }

    async fn insert(
        &self,
        partition: &PartitionId,
        key: &StorageKey,
        value: Item,
    ) -> Result<(), DataStoreError> {
        let pk_map = self.item_partitions.entry(*partition).or_default();

        let mut rk_map_ref = pk_map.entry(key.partition_key.clone()).or_default();
        let rk_map = rk_map_ref.value_mut();

        debug!(
            "Adding item to partition: {:?}, key: {}, value: {:?}",
            partition,
            key.to_string(),
            value
        );

        let mut is_status_change_active = false;
        let mut is_status_change_tombstone = false;

        let existing = rk_map.insert(key.range_key.clone(), value.clone());
        if let Some(old) = existing {
            // Check if status changed
            if old.status != value.status {
                match value.status {
                    ItemStatus::Active => is_status_change_active = true,
                    ItemStatus::Tombstone(_) => is_status_change_tombstone = true,
                }
            }
        } else if matches!(value.status, ItemStatus::Active) {
            is_status_change_active = true;
        }

        if is_status_change_active {
            let mut count = self.partition_counts.entry(*partition).or_insert(0);
            *count += 1;
        } else if is_status_change_tombstone {
            let mut count = self.partition_counts.entry(*partition).or_insert(0);
            if *count > 0 {
                *count -= 1;
            }
        }

        Ok(())
    }

    async fn remove(
        &self,
        partition: &PartitionId,
        key: &StorageKey,
    ) -> Result<(), DataStoreError> {
        info!(
            "Removing item from partition: {:?}, key: {}",
            partition,
            key.to_string()
        );

        if let Some(pk_map) = self.item_partitions.get(partition)
            && let Some(mut rk_map_ref) = pk_map.get_mut(&key.partition_key)
        {
            let rk_map = rk_map_ref.value_mut();
            if let Some(removed_item) = rk_map.remove(&key.range_key)
                && removed_item.status == ItemStatus::Active
            {
                let mut count = self.partition_counts.entry(*partition).or_insert(0);
                if *count > 0 {
                    *count -= 1;
                }
            }
            // TODO: Cleanup empty rk_map?
            // Probably better to leave it to avoid frequent allocations if keys are reused,
            // but if we have millions of partition keys it might consume memory.
            // For now, let's keep it simple.
        }
        Ok(())
    }

    async fn partition_counts(&self) -> Result<HashMap<PartitionId, usize>, DataStoreError> {
        let mut result = HashMap::new();
        for entry in self.partition_counts.iter() {
            result.insert(*entry.key(), *entry.value());
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use crate::{clock::HLC, cluster::Cluster};

    #[tokio::test]
    async fn test_insert_item_pk_only() {
        use super::*;
        let store = InMemoryStore::default();
        let cluster_config = Cluster::new("test_cluster".to_string(), 3, 0, 6, 3, true);

        let storage_key = StorageKey {
            partition_key: PartitionKey("item1".to_string()),
            range_key: None,
        };
        let partition = cluster_config.hash_key(storage_key.partition_key.value());

        let _ = store
            .insert(
                &partition,
                &storage_key,
                Item {
                    message: "test".as_bytes().to_vec(),
                    status: ItemStatus::Active,
                    hlc: HLC::new(),
                },
            )
            .await;

        let retrieved_item = store.get(&partition, &storage_key).await.unwrap();
        assert!(retrieved_item.is_some());
        let item_entry = retrieved_item.unwrap();
        assert_eq!(item_entry.storage_key, storage_key);
        assert_eq!(item_entry.item.message, "test".as_bytes().to_vec());
    }

    #[tokio::test]
    async fn test_insert_item_pk_and_rk() {
        use super::*;

        let cluster_config = Cluster::new("test_cluster".to_string(), 3, 0, 6, 3, true);
        let store = InMemoryStore::default();

        let storage_key = StorageKey {
            partition_key: PartitionKey("123".to_string()),
            range_key: Some(RangeKey("456".to_string())),
        };
        let partition = cluster_config.hash_key(storage_key.partition_key.value());

        let _ = store
            .insert(
                &partition,
                &storage_key,
                Item {
                    message: "test".as_bytes().to_vec(),
                    status: ItemStatus::Active,
                    hlc: HLC::new(),
                },
            )
            .await;

        let retrieved_item = store.get(&partition, &storage_key).await.unwrap();
        assert!(retrieved_item.is_some());
        let item_entry = retrieved_item.unwrap();
        assert_eq!(item_entry.storage_key, storage_key);
        assert_eq!(item_entry.item.message, "test".as_bytes().to_vec());
    }

    #[tokio::test]
    async fn test_partition_counts() {
        use super::*;
        let store = InMemoryStore::default();

        let partition1 = PartitionId(1);
        let partition2 = PartitionId(2);

        let storage_key1 = StorageKey {
            partition_key: PartitionKey("item1".to_string()),
            range_key: None,
        };
        let storage_key2 = StorageKey {
            partition_key: PartitionKey("item2".to_string()),
            range_key: None,
        };
        let storage_key3 = StorageKey {
            partition_key: PartitionKey("item3".to_string()),
            range_key: None,
        };

        let _ = store
            .insert(
                &partition1,
                &storage_key1,
                Item {
                    message: "test1".as_bytes().to_vec(),
                    status: ItemStatus::Active,
                    hlc: HLC::new(),
                },
            )
            .await;

        let _ = store
            .insert(
                &partition1,
                &storage_key2,
                Item {
                    message: "test2".as_bytes().to_vec(),
                    status: ItemStatus::Active,
                    hlc: HLC::new(),
                },
            )
            .await;

        let _ = store
            .insert(
                &partition2,
                &storage_key3,
                Item {
                    message: "test3".as_bytes().to_vec(),
                    status: ItemStatus::Active,
                    hlc: HLC::new(),
                },
            )
            .await;

        let counts = store.partition_counts().await.unwrap();
        assert_eq!(counts.get(&partition1), Some(&2));
        assert_eq!(counts.get(&partition2), Some(&1));
    }

    #[tokio::test]
    async fn test_get_many_filtering() {
        use super::*;
        let store = InMemoryStore::default();
        let partition = PartitionId(1);
        let pk = PartitionKey("user123".to_string());

        // Insert multiple range keys
        for rk_val in &["profile", "settings", "posts", "session"] {
            let sk = StorageKey::new(pk.clone(), Some(RangeKey(rk_val.to_string())));
            store
                .insert(
                    &partition,
                    &sk,
                    Item {
                        message: rk_val.as_bytes().to_vec(),
                        status: ItemStatus::Active,
                        hlc: HLC::new(),
                    },
                )
                .await
                .unwrap();
        }

        // 1. Get all for pk
        let sk_query = StorageKey::new(pk.clone(), None);
        let items = store.get_many(&partition, &sk_query, 10).await.unwrap();
        assert_eq!(items.len(), 4);

        // 2. Get with range filter (e.g., contains "se")
        let sk_filter = StorageKey::new(pk.clone(), Some(RangeKey("se".to_string())));
        let filtered_items = store.get_many(&partition, &sk_filter, 10).await.unwrap();
        // matches "settings" and "session"
        assert_eq!(filtered_items.len(), 2);
        let returned_rk: Vec<String> = filtered_items
            .iter()
            .map(|i| i.storage_key.range_key.as_ref().unwrap().0.clone())
            .collect();
        assert!(returned_rk.contains(&"settings".to_string()));
        assert!(returned_rk.contains(&"session".to_string()));

        // 3. Limit
        let limited_items = store.get_many(&partition, &sk_query, 2).await.unwrap();
        assert_eq!(limited_items.len(), 2);
    }
}
