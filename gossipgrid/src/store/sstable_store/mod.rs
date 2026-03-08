//! SSTable-backed storage engine implementation.
//!
//! This module is feature-gated behind `sstable-store`.

mod string_utils;

use std::{
    collections::{BTreeMap, HashMap},
    fs,
    ops::Bound,
    path::PathBuf,
    sync::Arc,
};

use bincode::config;
use dashmap::DashMap;
use sstable::{Options, SSIterator, Table};
use string_utils::{escape_key_component, split_encoded_key, unescape_key_component};
use tokio::sync::RwLock;

use crate::{
    StoreEngine,
    clock::{self, HLC},
    cluster::PartitionId,
    item::{Item, ItemEntry, ItemStatus},
    store::{DataStoreError, GetManyOptions, PartitionKey, RangeKey, StorageKey},
};

#[cfg(test)]
mod tests;

#[allow(dead_code)]
pub(crate) fn encode_storage_key(key: &StorageKey) -> Vec<u8> {
    let mut encoded = escape_key_component(key.partition_key.value());
    if let Some(range_key) = &key.range_key {
        encoded.push('/');
        encoded.push_str(&escape_key_component(range_key.value()));
    }
    encoded.into_bytes()
}

#[allow(dead_code)]
pub(crate) fn decode_storage_key(encoded: &[u8]) -> Result<StorageKey, DataStoreError> {
    let encoded = std::str::from_utf8(encoded).map_err(|err| {
        DataStoreError::StorageKeyParsingError(format!("invalid UTF-8 key bytes: {err}"))
    })?;

    let (raw_pk, raw_rk) = split_encoded_key(encoded).ok_or_else(|| {
        DataStoreError::StorageKeyParsingError("invalid escaped key encoding".to_string())
    })?;

    let partition_key = PartitionKey(unescape_key_component(raw_pk).ok_or_else(|| {
        DataStoreError::StorageKeyParsingError("invalid partition key escape sequence".to_string())
    })?);

    let range_key = match raw_rk {
        Some(raw_rk) => Some(RangeKey(unescape_key_component(raw_rk).ok_or_else(
            || {
                DataStoreError::StorageKeyParsingError(
                    "invalid range key escape sequence".to_string(),
                )
            },
        )?)),
        None => None,
    };

    Ok(StorageKey::new(partition_key, range_key))
}

#[allow(dead_code)]
fn encoded_partition_prefix(partition_key: &PartitionKey) -> Vec<u8> {
    escape_key_component(partition_key.value()).into_bytes()
}

#[allow(dead_code)]
fn matches_partition_key(encoded_key: &[u8], encoded_partition_prefix: &[u8]) -> bool {
    encoded_key == encoded_partition_prefix
        || encoded_key
            .strip_prefix(encoded_partition_prefix)
            .is_some_and(|suffix| suffix.first() == Some(&b'/'))
}

#[allow(dead_code)]
fn encode_item(item: &Item) -> Result<Vec<u8>, DataStoreError> {
    bincode::encode_to_vec(item, config::standard()).map_err(|err| {
        DataStoreError::StoreOperationError(format!("failed to encode item for SSTable: {err}"))
    })
}

#[allow(dead_code)]
fn decode_item(bytes: &[u8]) -> Result<Item, DataStoreError> {
    bincode::decode_from_slice(bytes, config::standard())
        .map(|(item, _)| item)
        .map_err(|err| {
            DataStoreError::StoreOperationError(format!(
                "failed to decode item from SSTable: {err}"
            ))
        })
}

#[allow(dead_code)]
fn open_table(path: &PathBuf) -> Result<Table, DataStoreError> {
    Table::new_from_file(Options::default(), path).map_err(|err| {
        DataStoreError::StoreOperationError(format!(
            "failed to open SSTable `{}`: {err}",
            path.display()
        ))
    })
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct PartitionStore {
    pub(crate) memtable: BTreeMap<Vec<u8>, Item>,
    pub(crate) sstable_files: Vec<PathBuf>,
    pub(crate) partition_dir: PathBuf,
    pub(crate) active_count: usize,
}

#[allow(dead_code)]
impl PartitionStore {
    pub(crate) fn new(partition_dir: PathBuf) -> Result<Self, DataStoreError> {
        fs::create_dir_all(&partition_dir).map_err(|err| {
            DataStoreError::StoreOperationError(format!(
                "failed to create partition directory `{}`: {err}",
                partition_dir.display()
            ))
        })?;

        Ok(Self {
            memtable: BTreeMap::new(),
            sstable_files: Vec::new(),
            partition_dir,
            active_count: 0,
        })
    }

    pub(crate) fn get(&self, key: &StorageKey) -> Result<Option<ItemEntry>, DataStoreError> {
        let encoded_key = encode_storage_key(key);

        if let Some(item) = self.memtable.get(&encoded_key) {
            return Ok((item.status == ItemStatus::Active)
                .then(|| ItemEntry::new(key.clone(), item.clone())));
        }

        for path in self.sstable_files.iter().rev() {
            let table = open_table(path)?;
            if let Some(value) = table.get(&encoded_key).map_err(|err| {
                DataStoreError::StoreGetOperationError(format!(
                    "failed reading key `{}` from `{}`: {err}",
                    key,
                    path.display()
                ))
            })? {
                let item = decode_item(&value)?;
                return Ok(
                    (item.status == ItemStatus::Active).then(|| ItemEntry::new(key.clone(), item))
                );
            }
        }

        Ok(None)
    }

    pub(crate) fn get_many(
        &self,
        key: &StorageKey,
        options: GetManyOptions,
    ) -> Result<Vec<ItemEntry>, DataStoreError> {
        let merged = self.collect_partition_items(&key.partition_key)?;
        let mut items = Vec::new();

        for (encoded_key, item) in merged {
            if item.status != ItemStatus::Active {
                continue;
            }

            let storage_key = decode_storage_key(&encoded_key)?;
            if options.skip_null_rk && storage_key.range_key.is_none() {
                continue;
            }

            let matches_range = match (&key.range_key, &storage_key.range_key) {
                (Some(filter), Some(entry)) => entry.value().contains(filter.value()),
                (Some(_), None) => false,
                (None, _) => true,
            };

            if matches_range {
                items.push(ItemEntry::new(storage_key, item));
            }
        }

        items.reverse();
        items.truncate(options.limit);
        Ok(items)
    }

    pub(crate) fn insert(&mut self, key: &StorageKey, item: Item) -> Result<(), DataStoreError> {
        let is_newly_active = item.status == ItemStatus::Active;
        let was_active = self.get(key)?.is_some();

        match (was_active, is_newly_active) {
            (false, true) => self.active_count += 1,
            (true, false) => {
                if self.active_count > 0 {
                    self.active_count -= 1;
                }
            }
            _ => {}
        }

        self.memtable.insert(encode_storage_key(key), item);
        Ok(())
    }

    pub(crate) fn remove(&mut self, key: &StorageKey, hlc: HLC) -> Result<(), DataStoreError> {
        let was_active = self.get(key)?.is_some();
        if was_active && self.active_count > 0 {
            self.active_count -= 1;
        }

        let tombstone = Item {
            message: vec![],
            status: ItemStatus::Tombstone(hlc.timestamp),
            hlc,
        };
        self.memtable.insert(encode_storage_key(key), tombstone);
        Ok(())
    }

    pub(crate) fn count(&self) -> Result<usize, DataStoreError> {
        Ok(self.active_count)
    }

    #[allow(dead_code)]
    pub(crate) fn recompute_count(&mut self) -> Result<(), DataStoreError> {
        let mut merged = HashMap::new();

        for path in &self.sstable_files {
            for (encoded_key, item) in read_all_sstable_entries(path)? {
                merged.insert(encoded_key, item);
            }
        }

        for (encoded_key, item) in &self.memtable {
            merged.insert(encoded_key.clone(), item.clone());
        }

        self.active_count = merged
            .values()
            .filter(|item| item.status == ItemStatus::Active)
            .count();
        Ok(())
    }

    fn collect_partition_items(
        &self,
        partition_key: &PartitionKey,
    ) -> Result<BTreeMap<Vec<u8>, Item>, DataStoreError> {
        let encoded_partition = encoded_partition_prefix(partition_key);
        let mut merged = BTreeMap::new();

        for path in &self.sstable_files {
            for (encoded_key, item) in read_partition_entries(path, &encoded_partition)? {
                merged.insert(encoded_key, item);
            }
        }

        let start = Bound::Included(encoded_partition.clone());
        for (encoded_key, item) in self.memtable.range((start, Bound::Unbounded)) {
            if !matches_partition_key(encoded_key, &encoded_partition) {
                break;
            }
            merged.insert(encoded_key.clone(), item.clone());
        }

        Ok(merged)
    }
}

#[allow(dead_code)]
fn read_partition_entries(
    path: &PathBuf,
    encoded_partition: &[u8],
) -> Result<Vec<(Vec<u8>, Item)>, DataStoreError> {
    let table = open_table(path)?;
    let mut iter = table.iter();
    let mut entries = Vec::new();

    iter.seek(encoded_partition);
    while iter.valid() {
        let mut encoded_key = Vec::new();
        let mut value = Vec::new();
        if !iter.current(&mut encoded_key, &mut value) {
            break;
        }

        if !matches_partition_key(&encoded_key, encoded_partition) {
            break;
        }

        entries.push((encoded_key, decode_item(&value)?));
        if !iter.advance() {
            break;
        }
    }

    Ok(entries)
}

#[allow(dead_code)]
fn read_all_sstable_entries(path: &PathBuf) -> Result<Vec<(Vec<u8>, Item)>, DataStoreError> {
    let table = open_table(path)?;
    let mut iter = table.iter();
    let mut entries = Vec::new();

    while let Some((encoded_key, value)) = iter.next() {
        entries.push((encoded_key, decode_item(&value)?));
    }

    Ok(entries)
}

#[allow(dead_code)]
pub struct SstableStore {
    partitions: DashMap<PartitionId, Arc<RwLock<PartitionStore>>>,
    data_dir: PathBuf,
    flush_threshold_bytes: usize,
}

#[async_trait::async_trait]
impl StoreEngine for SstableStore {
    async fn get(
        &self,
        partition: &PartitionId,
        key: &StorageKey,
    ) -> Result<Option<ItemEntry>, DataStoreError> {
        let partition_store = match self.partitions.get(partition) {
            Some(p) => p.value().clone(),
            None => return Ok(None),
        };

        let key = key.clone();

        tokio::task::spawn_blocking(move || {
            let part = partition_store.blocking_read();
            part.get(&key)
        })
        .await
        .unwrap_or_else(|err| {
            Err(DataStoreError::StoreGetOperationError(format!(
                "spawn_blocking task failed: {err}"
            )))
        })
    }

    async fn get_many(
        &self,
        partition: &PartitionId,
        key: &StorageKey,
        options: GetManyOptions,
    ) -> Result<Vec<ItemEntry>, DataStoreError> {
        let partition_store = match self.partitions.get(partition) {
            Some(p) => p.value().clone(),
            None => return Ok(vec![]),
        };

        let key = key.clone();
        let options = options.clone();

        tokio::task::spawn_blocking(move || {
            let part = partition_store.blocking_read();
            part.get_many(&key, options)
        })
        .await
        .unwrap_or_else(|err| {
            Err(DataStoreError::StoreGetOperationError(format!(
                "spawn_blocking task failed: {err}"
            )))
        })
    }

    async fn insert(
        &self,
        partition: &PartitionId,
        key: &StorageKey,
        item: Item,
    ) -> Result<(), DataStoreError> {
        let partition_store = match self.partitions.get_mut(partition) {
            Some(p) => p.value().clone(),
            None => return Ok(()),
        };

        let key = key.clone();

        tokio::task::spawn_blocking(move || {
            let mut part = partition_store.blocking_write();
            part.insert(&key, item)
        })
        .await
        .unwrap_or_else(|err| {
            Err(DataStoreError::StoreGetOperationError(format!(
                "spawn_blocking task failed: {err}"
            )))
        })
    }

    async fn remove(
        &self,
        partition: &PartitionId,
        key: &StorageKey,
    ) -> Result<(), DataStoreError> {
        let partition_store = match self.partitions.get_mut(partition) {
            Some(p) => p.value().clone(),
            None => return Ok(()),
        };

        let key = key.clone();

        tokio::task::spawn_blocking(move || {
            let mut part = partition_store.blocking_write();
            part.remove(&key, HLC::new().tick_hlc(clock::now_millis()))
        })
        .await
        .unwrap_or_else(|err| {
            Err(DataStoreError::StoreGetOperationError(format!(
                "spawn_blocking task failed: {err}"
            )))
        })
    }

    async fn partition_counts(&self) -> Result<HashMap<PartitionId, usize>, DataStoreError> {
        let mut counts = HashMap::new();
        for entry in self.partitions.iter() {
            let partition_id = *entry.key();
            let store = entry.value().read().await;
            counts.insert(partition_id, store.count()?);
        }
        Ok(counts)
    }

    fn is_in_memory_store(&self) -> bool {
        false
    }
}
