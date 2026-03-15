//! SSTable-backed storage engine implementation.
//!
//! This module is feature-gated behind `sstable-store`.

mod compactor;
mod key_codecs;
mod merge_iterator;
mod partition_store;

#[cfg(test)]
mod tests;

use crate::store::sstable_store::partition_store::dump_to_sstable;
use std::{collections::HashMap, fs, path::PathBuf, sync::Arc};

use dashmap::DashMap;
use log::info;
use pwal::Wal;
use tokio::sync::RwLock;

use crate::{
    StoreEngine,
    clock::{self, HLC},
    cluster::PartitionId,
    item::{Item, ItemEntry},
    store::{
        DataStoreError, GetManyOptions, StorageKey, sstable_store::partition_store::PartitionStore,
    },
    wal::WalRecord,
};

pub(crate) const NUM_WAL_SEGMENTS_TO_RETAIN: usize = 2;
pub(crate) const SST_EXTENSION: &str = "sst";

pub struct SstableStore {
    partitions: DashMap<PartitionId, Arc<RwLock<PartitionStore>>>,
    data_dir: PathBuf,
    flush_threshold_bytes: usize,
    wal: Arc<dyn Wal<WalRecord>>,
}

impl SstableStore {
    pub fn new(
        data_dir: PathBuf,
        wal: Arc<dyn Wal<WalRecord>>,
        flush_threshold_bytes: usize,
        truncate: bool,
    ) -> Result<Self, DataStoreError> {
        let partitions = DashMap::new();

        if truncate && data_dir.exists() {
            if let Err(err) = fs::remove_dir_all(&data_dir) {
                log::warn!(
                    "Failed to delete existing data directory {:?}: {}",
                    data_dir,
                    err
                );
            }
        }

        if data_dir.exists() {
            for entry in fs::read_dir(&data_dir).map_err(|err| {
                DataStoreError::StoreOperationError(format!(
                    "failed to read data directory `{}`: {err}",
                    data_dir.display()
                ))
            })? {
                let entry = entry.map_err(|err| {
                    DataStoreError::StoreOperationError(format!(
                        "failed to read directory entry: {err}"
                    ))
                })?;
                let path = entry.path();

                if path.is_dir() {
                    if let Some(partition_id_str) = path.file_name().and_then(|s| s.to_str()) {
                        if let Ok(pid_val) = partition_id_str.parse::<u16>() {
                            let partition_id = PartitionId(pid_val);
                            let partition_store = PartitionStore::load_from_dir(path.clone())?;

                            partitions.insert(partition_id, Arc::new(RwLock::new(partition_store)));
                        }
                    }
                }
            }
        } else {
            fs::create_dir_all(&data_dir).map_err(|err| {
                DataStoreError::StoreOperationError(format!(
                    "failed to create data directory `{}`: {err}",
                    data_dir.display()
                ))
            })?;
        }

        Ok(Self {
            partitions,
            data_dir,
            flush_threshold_bytes,
            wal,
        })
    }

    async fn apply_item_mutation(
        &self,
        partition: &PartitionId,
        key: &StorageKey,
        item: Option<Item>,
    ) -> Result<(), DataStoreError> {
        let partition_store = {
            if let Some(p) = self.partitions.get(partition) {
                p.value().clone()
            } else {
                let path = self.data_dir.join(partition.0.to_string());
                let partition_store = PartitionStore::new(path)?;
                let arc = Arc::new(RwLock::new(partition_store));
                self.partitions.insert(*partition, arc.clone());
                arc
            }
        };

        let key = key.clone();
        let threshold = self.flush_threshold_bytes;

        let data_to_flush = tokio::task::spawn_blocking(move || {
            let mut part = partition_store.blocking_write();

            // Handle the different in-memory mutations based on the optional item
            if let Some(item) = item {
                part.insert(&key, item)?;
            } else {
                part.remove(&key, HLC::new().tick_hlc(clock::now_millis()))?;
            }

            if part.memtable_size_bytes >= threshold {
                let dir = part.partition_dir.clone();
                Ok(part.trigger_flush().map(|arc| (arc, dir)))
            } else {
                Ok(None)
            }
        })
        .await
        .unwrap_or_else(|err| {
            Err(DataStoreError::StoreGetOperationError(format!(
                "spawn_blocking task failed: {err}"
            )))
        })?;

        if let Some((memtable_arc, partition_dir)) = data_to_flush {
            let partition_store = self.partitions.get(partition).unwrap().value().clone();
            let partition = partition.clone();
            let wal = self.wal.clone();

            tokio::spawn(async move {
                let timestamp = crate::clock::now_millis();
                let file_name = format!("{}.{}", timestamp, SST_EXTENSION);
                let sst_path = partition_dir.join(file_name);

                let write_res = tokio::task::spawn_blocking({
                    let memtable_arc = memtable_arc.clone();
                    let sst_path = sst_path.clone();
                    move || dump_to_sstable(&memtable_arc, &sst_path)
                })
                .await
                .unwrap_or_else(|err| {
                    Err(DataStoreError::StoreOperationError(format!(
                        "background flush spawn_blocking failed: {err}"
                    )))
                });

                if write_res.is_ok() {
                    {
                        let mut part = partition_store.write().await;
                        part.complete_flush(sst_path);
                    }

                    let partition_store_clone = partition_store.clone();

                    let handle = tokio::runtime::Handle::current();
                    let _ = tokio::task::spawn_blocking(move || {
                        handle.block_on(async move {
                            if let Err(e) = wal
                                .purge_segments(
                                    partition.into(),
                                    pwal::WalPurgeTarget::RetainLatestSegments(
                                        NUM_WAL_SEGMENTS_TO_RETAIN,
                                    ),
                                )
                                .await
                            {
                                log::error!(
                                    "Failed to purge WAL (remove) for partition {}: {:?}",
                                    partition,
                                    e
                                );
                            }

                            let mut part = partition_store_clone.write().await;
                            if let Err(e) = part.trigger_partition_compaction().await {
                                log::error!(
                                    "Failed to trigger compaction for partition {}: {:?}",
                                    partition,
                                    e
                                );
                            }
                        });
                    });
                } else {
                    let mut part = partition_store.write().await;
                    part.flushing_memtable = None;
                    log::error!(
                        "Failed to flush SSTable for partition {}: {:?}",
                        partition,
                        write_res.err()
                    );
                }
            });
        }
        Ok(())
    }
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
        self.apply_item_mutation(partition, key, Some(item)).await
    }

    async fn remove(
        &self,
        partition: &PartitionId,
        key: &StorageKey,
    ) -> Result<(), DataStoreError> {
        self.apply_item_mutation(partition, key, None).await
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

    async fn shutdown(&self) -> Result<(), DataStoreError> {
        info!("Writing down sstable memtable state");

        for entry in self.partitions.iter() {
            let partition_store = entry.value().clone();

            let data_to_flush = {
                let mut part = partition_store.write().await;
                let dir = part.partition_dir.clone();
                part.trigger_flush().map(|arc| (arc, dir))
            };

            if let Some((memtable_arc, partition_dir)) = data_to_flush {
                let timestamp = crate::clock::now_millis();
                let file_name = format!("{}.{}", timestamp, SST_EXTENSION);
                let sst_path = partition_dir.join(file_name);

                dump_to_sstable(&memtable_arc, &sst_path)?;

                let mut part = partition_store.write().await;
                part.complete_flush(sst_path);

                // Shutdown flush is synchronous — WAL purge is handled during normal
                // insert/remove flush cycles, not here. We just ensure memtables are persisted.
            }
        }
        Ok(())
    }
}
