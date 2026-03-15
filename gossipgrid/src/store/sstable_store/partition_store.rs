use std::{collections::BTreeMap, fs, ops::Bound, path::PathBuf, sync::Arc};

use sstable::SSIterator;

use bincode::config;
use sstable::{Options, Table, TableBuilder};

use crate::{
    clock::HLC,
    item::{Item, ItemEntry, ItemStatus},
    store::{
        DataStoreError, GetManyOptions, PartitionKey, StorageKey,
        sstable_store::{
            SST_EXTENSION,
            compactor::Compactor,
            key_codecs::{
                decode_storage_key, encode_storage_key, encoded_partition_prefix,
                matches_partition_key,
            },
            merge_iterator::{MergeIterator, OwningSSIterator},
        },
    },
};

pub(crate) const MAX_SSTABLE_SEGMENTS: usize = 8;
pub(crate) const ITEM_OVERHEAD_BYTES: usize = 24; // Approximation for Item struct overhead (status enum + HLC + vec pointer)

pub(crate) fn encode_item(item: &Item) -> Result<Vec<u8>, DataStoreError> {
    bincode::encode_to_vec(item, config::standard()).map_err(|err| {
        DataStoreError::StoreOperationError(format!("failed to encode item for SSTable: {err}"))
    })
}

pub(crate) fn decode_item(bytes: &[u8]) -> Result<Item, DataStoreError> {
    bincode::decode_from_slice(bytes, config::standard())
        .map(|(item, _)| item)
        .map_err(|err| {
            DataStoreError::StoreOperationError(format!(
                "failed to decode item from SSTable: {err}"
            ))
        })
}

pub(crate) fn open_table(path: &PathBuf) -> Result<Table, DataStoreError> {
    Table::new_from_file(Options::default(), path).map_err(|err| {
        DataStoreError::StoreOperationError(format!(
            "failed to open SSTable `{}`: {err}",
            path.display()
        ))
    })
}

#[derive(Debug, Clone)]
pub(crate) struct PartitionStore {
    pub(crate) memtable: BTreeMap<Vec<u8>, Item>,
    pub(crate) flushing_memtable: Option<Arc<BTreeMap<Vec<u8>, Item>>>,
    pub(crate) memtable_size_bytes: usize,
    pub(crate) sstable_files: Vec<PathBuf>,
    pub(crate) partition_dir: PathBuf,
    pub(crate) active_count: usize,
}

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
            flushing_memtable: None,
            memtable_size_bytes: 0,
            sstable_files: Vec::new(),
            partition_dir,
            active_count: 0,
        })
    }

    pub(crate) fn load_from_dir(partition_dir: PathBuf) -> Result<Self, DataStoreError> {
        let mut store = Self::new(partition_dir.clone())?;

        let mut sst_files = Vec::new();
        for sst_entry in fs::read_dir(&partition_dir).map_err(|err| {
            DataStoreError::StoreOperationError(format!(
                "failed to read partition directory `{}`: {err}",
                partition_dir.display()
            ))
        })? {
            let sst_entry = sst_entry.map_err(|err| {
                DataStoreError::StoreOperationError(format!("failed to read file entry: {err}"))
            })?;
            let sst_path = sst_entry.path();
            if sst_path.is_file() && sst_path.extension().is_some_and(|ext| ext == SST_EXTENSION) {
                sst_files.push(sst_path);
            }
        }

        // Sort SSTable files by filename (timestamp) oldest to newest
        sst_files.sort();
        store.sstable_files = sst_files;

        // Recompute active count from SSTables
        store.recompute_count()?;

        Ok(store)
    }

    pub(crate) fn get(&self, key: &StorageKey) -> Result<Option<ItemEntry>, DataStoreError> {
        let encoded_key = encode_storage_key(key);

        if let Some(item) = self.memtable.get(&encoded_key) {
            return Ok((item.status == ItemStatus::Active)
                .then(|| ItemEntry::new(key.clone(), item.clone())));
        }

        if let Some(flushing) = &self.flushing_memtable {
            if let Some(item) = flushing.get(&encoded_key) {
                return Ok((item.status == ItemStatus::Active)
                    .then(|| ItemEntry::new(key.clone(), item.clone())));
            }
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

        let encoded_key = encode_storage_key(key);
        // Approximation of size added: key length + message length + constant overhead for Item struct
        self.memtable_size_bytes += Self::calculate_entry_size(key, &item);
        self.memtable.insert(encoded_key, item);
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
        let encoded_key = encode_storage_key(key);
        self.memtable_size_bytes += Self::calculate_entry_size(key, &tombstone);
        self.memtable.insert(encoded_key, tombstone);
        Ok(())
    }

    pub(crate) fn count(&self) -> Result<usize, DataStoreError> {
        Ok(self.active_count)
    }

    // TODO: Right now we are re-calculating sst files on each start.
    //  in the future we may consider persisting counts per sstable when materializing memtable.
    pub(crate) fn recompute_count(&mut self) -> Result<(), DataStoreError> {
        let mut iters: Vec<Box<dyn Iterator<Item = (Vec<u8>, Item)>>> = Vec::new();

        // 1. Add SSTable iterators (oldest tracking first)
        for path in &self.sstable_files {
            let table = open_table(path)?;
            let owning_iter = OwningSSIterator::new(table);
            iters.push(Box::new(owning_iter));
        }

        // 2. Add flushing memtable iterator
        if let Some(flushing) = &self.flushing_memtable {
            let entries: Vec<_> = flushing
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            iters.push(Box::new(entries.into_iter()));
        }

        // 3. Add active memtable iterator
        let entries: Vec<_> = self
            .memtable
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        iters.push(Box::new(entries.into_iter()));

        let mut active_count = 0;
        let merge_iter = MergeIterator::new(iters);

        for (_, item) in merge_iter {
            if item.status == ItemStatus::Active {
                active_count += 1;
            }
        }

        self.active_count = active_count;
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

        if let Some(flushing) = &self.flushing_memtable {
            for (encoded_key, item) in flushing.range((start.clone(), Bound::Unbounded)) {
                if !matches_partition_key(encoded_key, &encoded_partition) {
                    break;
                }
                merged.insert(encoded_key.clone(), item.clone());
            }
        }

        for (encoded_key, item) in self.memtable.range((start, Bound::Unbounded)) {
            if !matches_partition_key(encoded_key, &encoded_partition) {
                break;
            }
            merged.insert(encoded_key.clone(), item.clone());
        }

        Ok(merged)
    }

    fn calculate_entry_size(key: &StorageKey, item: &Item) -> usize {
        let encoded_key = encode_storage_key(key);
        encoded_key.len() + item.message.len() + ITEM_OVERHEAD_BYTES
    }

    pub(crate) fn trigger_flush(&mut self) -> Option<Arc<BTreeMap<Vec<u8>, Item>>> {
        if self.memtable.is_empty() || self.flushing_memtable.is_some() {
            return None;
        }
        let memtable = std::mem::take(&mut self.memtable);
        self.memtable_size_bytes = 0;
        let arc = Arc::new(memtable);
        self.flushing_memtable = Some(arc.clone());
        Some(arc)
    }

    #[allow(dead_code)]
    pub(crate) fn complete_flush(&mut self, sstable_path: PathBuf) {
        self.flushing_memtable = None;
        self.sstable_files.push(sstable_path);
    }

    pub(crate) fn compact(
        &self,
    ) -> Result<Option<(PathBuf, Vec<PathBuf>, BTreeMap<Vec<u8>, Item>)>, DataStoreError> {
        Compactor::compact(&self.partition_dir, &self.sstable_files)
    }

    pub(crate) async fn complete_compaction(&mut self, new_path: PathBuf, old_paths: Vec<PathBuf>) {
        let old_paths_set: std::collections::HashSet<_> = old_paths.into_iter().collect();
        let mut new_files = Vec::new();

        for path in &self.sstable_files {
            if !old_paths_set.contains(path) {
                new_files.push(path.clone());
            }
        }
        new_files.push(new_path);
        new_files.sort();
        self.sstable_files = new_files;

        // Delete old files in a blocking task
        let _ = tokio::task::spawn_blocking(move || {
            for path in old_paths_set {
                if let Err(e) = fs::remove_file(&path) {
                    log::error!(
                        "Failed to remove compacted SSTable file {}: {:?}",
                        path.display(),
                        e
                    );
                }
            }
        })
        .await;
    }

    pub(crate) async fn trigger_partition_compaction(&mut self) -> Result<(), DataStoreError> {
        let compaction_data = self.compact()?;

        if let Some((new_path, old_paths, merged)) = compaction_data {
            let write_res = tokio::task::spawn_blocking({
                let new_path = new_path.clone();
                move || write_memtable_to_sstable(&merged, &new_path)
            })
            .await
            .unwrap_or_else(|err| {
                Err(DataStoreError::StoreOperationError(format!(
                    "compaction spawn_blocking failed: {err}"
                )))
            });

            if write_res.is_ok() {
                self.complete_compaction(new_path, old_paths).await;
            } else {
                log::error!("Failed to write compacted SSTable to {:?}", new_path);
            }
        }

        Ok(())
    }
}

pub(crate) fn write_memtable_to_sstable(
    memtable: &BTreeMap<Vec<u8>, Item>,
    path: &PathBuf,
) -> Result<(), DataStoreError> {
    let file = fs::File::create(path).map_err(|err| {
        DataStoreError::StoreOperationError(format!("failed to create SSTable file: {err}"))
    })?;

    let mut builder = TableBuilder::new(Options::default(), file);

    for (encoded_key, item) in memtable {
        let encoded_value = encode_item(item)?;
        builder.add(encoded_key, &encoded_value).map_err(|err| {
            DataStoreError::StoreOperationError(format!("failed to write to SSTable: {err}"))
        })?;
    }

    builder.finish().map_err(|err| {
        DataStoreError::StoreOperationError(format!("failed to finish SSTable: {err}"))
    })?;

    Ok(())
}

pub(crate) fn read_partition_entries(
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

pub(crate) fn read_all_sstable_entries(
    path: &PathBuf,
) -> Result<Vec<(Vec<u8>, Item)>, DataStoreError> {
    let table = open_table(path)?;
    let mut iter = table.iter();
    let mut entries = Vec::new();

    while let Some((encoded_key, value)) = iter.next() {
        entries.push((encoded_key, decode_item(&value)?));
    }

    Ok(entries)
}
