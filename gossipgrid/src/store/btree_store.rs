use bf_tree::{BfTree, LeafReadResult, ScanReturnField};
use dashmap::DashMap;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use crate::cluster::PartitionId;
use crate::item::{Item, ItemEntry};
use crate::store::{DataStoreError, GetManyOptions, StorageKey, StoreEngine};

// Increased buffer size to handle larger items
const BUFFER_SIZE: usize = 65536; // 64KB
// Increased circular buffer size to a more reasonable 32MB (default)
const CB_SIZE: usize = 1024 * 1024 * 32;

pub struct BTreeStore {
    inner: Arc<BTreeStoreInner>,
}

struct BTreeStoreInner {
    file_path: String,
    item_partitions: DashMap<PartitionId, BfTree>,
    partition_counts: DashMap<PartitionId, usize>,
}

impl BTreeStore {
    pub fn new(file_path: &str, _truncate: bool) -> Self {
        let store = BTreeStore {
            inner: Arc::new(BTreeStoreInner {
                file_path: file_path.to_owned(),
                item_partitions: DashMap::new(),
                partition_counts: DashMap::new(),
            }),
        };
        store.load_partition();
        store
    }

    fn load_partition(&self) {
        let inner = self.inner.clone();
        if let Ok(entries) = std::fs::read_dir(&inner.file_path) {
            for entry in entries.flatten() {
                if let Ok(file_name) = entry.file_name().into_string() {
                    if let Ok(partition_val) = file_name.parse::<u16>() {
                        let partition_id = PartitionId::from(partition_val);
                        let tree = Self::new_partition(&inner.file_path, &partition_id);

                        inner.item_partitions.insert(partition_id, tree);
                        inner.partition_counts.insert(partition_id, 0);
                    }
                }
            }
        }
    }

    fn new_partition(file_path: &str, partition_id: &PartitionId) -> BfTree {
        let path = format!("{}/{}", file_path, partition_id.value());

        if let Some(parent) = std::path::Path::new(&path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        let mut config = bf_tree::Config::new(path.clone(), CB_SIZE);
        config.leaf_page_size(32768);
        config.cb_max_record_size(8192);
        config.cb_min_record_size(8);

        // Use 10ms batching for the BTree WAL. This prevents every single
        // write from paying the 150-200ms fdatasync penalty individually.
        // let mut wal_config = bf_tree::WalConfig::new(wal_path);
        // wal_config.flush_interval(std::time::Duration::from_millis(10));
        // let wal_config = Arc::new(wal_config);
        // config.enable_write_ahead_log(wal_config);

        if std::fs::metadata(&path).is_ok() {
            BfTree::new_from_snapshot(config, None)
                .map_err(|e| format!("Failed to load partition {}: {:?}", partition_id, e))
                .unwrap()
        } else {
            let tree = BfTree::with_config(config, None)
                .map_err(|e| format!("Failed to create partition {}: {:?}", partition_id, e))
                .unwrap();
            tree.snapshot();
            tree
        }
    }
}

impl BTreeStoreInner {
    fn get_tree(
        &self,
        partition_id: &PartitionId,
    ) -> dashmap::mapref::one::Ref<'_, PartitionId, BfTree> {
        if let Some(tree) = self.item_partitions.get(partition_id) {
            tree
        } else {
            self.item_partitions
                .entry(partition_id.clone())
                .or_insert_with(|| BTreeStore::new_partition(&self.file_path, partition_id))
                .downgrade()
        }
    }
}

#[async_trait::async_trait]
impl StoreEngine for BTreeStore {
    fn is_in_memory_store(&self) -> bool {
        false
    }

    async fn get(
        &self,
        partition_id: &PartitionId,
        key: &StorageKey,
    ) -> Result<Option<ItemEntry>, DataStoreError> {
        let inner = self.inner.clone();
        let partition_id = *partition_id;
        let key = key.clone();

        tokio::task::spawn_blocking(move || {
            let tree = inner.get_tree(&partition_id);
            let mut out_buffer = vec![0u8; BUFFER_SIZE];
            let results = tree.read(key.to_string().as_bytes(), &mut out_buffer);

            match results {
                LeafReadResult::Found(size) => {
                    let (item, _) = bincode::decode_from_slice::<Item, _>(
                        &out_buffer[..size as usize],
                        bincode::config::standard(),
                    )
                    .map_err(|e| {
                        DataStoreError::StoreGetOperationError(format!("Decode error: {}", e))
                    })?;
                    Ok(Some(ItemEntry::new(key, item)))
                }
                _ => Ok(None),
            }
        })
        .await
        .map_err(|e| {
            DataStoreError::StoreOperationError(format!("Blocking task panicked: {}", e))
        })?
    }

    async fn get_many(
        &self,
        partition_id: &PartitionId,
        key: &StorageKey,
        options: GetManyOptions,
    ) -> Result<Vec<ItemEntry>, DataStoreError> {
        let inner = self.inner.clone();
        let partition_id = *partition_id;
        let key = key.clone();

        tokio::task::spawn_blocking(move || {
            let tree = inner.get_tree(&partition_id);
            let mut data: Vec<ItemEntry> = vec![];
            let prefix = key.to_string();
            let mut iter = tree
                .scan_with_count(
                    prefix.as_bytes(),
                    options.limit,
                    ScanReturnField::KeyAndValue,
                )
                .map_err(|e| DataStoreError::StoreOperationError(format!("Scan error: {:?}", e)))?;

            let mut buffer = vec![0u8; BUFFER_SIZE];
            while let Some((k_len, v_len)) = iter.next(&mut buffer) {
                let value_bytes = &buffer[k_len..k_len + v_len];
                let (item, _) =
                    bincode::decode_from_slice::<Item, _>(value_bytes, bincode::config::standard())
                        .map_err(|e| {
                            DataStoreError::StoreOperationError(format!("Decode error: {}", e))
                        })?;

                let key_str = std::str::from_utf8(&buffer[..k_len]).map_err(|e| {
                    DataStoreError::StoreOperationError(format!("Key encoding error: {}", e))
                })?;
                let storage_key = StorageKey::from_str(key_str)
                    .map_err(|e| DataStoreError::StorageKeyParsingError(e.to_string()))?;

                data.push(ItemEntry::new(storage_key, item));
            }
            Ok(data)
        })
        .await
        .map_err(|e| {
            DataStoreError::StoreOperationError(format!("Blocking task panicked: {}", e))
        })?
    }

    async fn insert(
        &self,
        partition_id: &PartitionId,
        key: &StorageKey,
        value: Item,
    ) -> Result<(), DataStoreError> {
        let inner = self.inner.clone();
        let partition_id = *partition_id;
        let key = key.clone();

        tokio::task::spawn_blocking(move || {
            let data = bincode::encode_to_vec(value, bincode::config::standard()).map_err(|e| {
                DataStoreError::StoreInsertOperationError(format!("Encode error: {}", e))
            })?;

            let tree = inner.get_tree(&partition_id);
            // Optimization: We don't do a 'read' here because the caller (NodeState)
            // has already performed the necessary state checks.
            tree.insert(key.to_string().as_bytes(), &data);

            // Simple count tracking for local partition counts
            inner.partition_counts.entry(partition_id).or_insert(0);

            Ok(())
        })
        .await
        .map_err(|e| {
            DataStoreError::StoreOperationError(format!("Blocking task panicked: {}", e))
        })?
    }

    async fn remove(
        &self,
        partition_id: &PartitionId,
        key: &StorageKey,
    ) -> Result<(), DataStoreError> {
        let inner = self.inner.clone();
        let partition_id = *partition_id;
        let key = key.clone();

        tokio::task::spawn_blocking(move || {
            let tree = inner.get_tree(&partition_id);
            tree.delete(key.to_string().as_bytes());

            if let Some(mut count) = inner.partition_counts.get_mut(&partition_id) {
                if *count > 0 {
                    *count -= 1;
                }
            }
            Ok(())
        })
        .await
        .map_err(|e| {
            DataStoreError::StoreOperationError(format!("Blocking task panicked: {}", e))
        })?
    }

    async fn partition_counts(&self) -> Result<HashMap<PartitionId, usize>, DataStoreError> {
        let mut result = HashMap::new();
        for entry in self.inner.partition_counts.iter() {
            result.insert(*entry.key(), *entry.value());
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{HLC, now_millis};
    use crate::item::ItemStatus;
    use crate::store::PartitionKey;
    use std::fs;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_insert_loop() {
        let uuid = Uuid::new_v4();
        let path = format!("./test_data_loop_{}", uuid);
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();

        let store = BTreeStore::new(&path, false);
        let partition = PartitionId(1);

        let start_time = now_millis();
        let mut last_log = start_time;

        for i in 0..100 {
            let key = StorageKey::new(PartitionKey(format!("pk{}", i)), None);
            let item = Item {
                message: format!("test_value_{}", i).into_bytes(),
                status: ItemStatus::Active,
                hlc: HLC::new(),
            };
            store.insert(&partition, &key, item).await.unwrap();

            let now = now_millis();
            println!("wrote pk{} in {} ms", i, now - last_log);
            last_log = now;
        }

        println!(
            "Total time for 100 inserts: {} ms",
            now_millis() - start_time
        );
        let _ = fs::remove_dir_all(&path);
    }
}
