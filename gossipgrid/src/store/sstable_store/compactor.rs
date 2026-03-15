use std::{collections::BTreeMap, path::PathBuf};

use crate::{
    item::{Item, ItemStatus},
    store::{
        DataStoreError,
        sstable_store::{
            SST_EXTENSION,
            partition_store::{read_all_sstable_entries, MAX_SSTABLE_SEGMENTS},
        },
    },
};

pub(crate) struct Compactor;

impl Compactor {
    pub(crate) fn compact(
        partition_dir: &PathBuf,
        sstable_files: &Vec<PathBuf>,
    ) -> Result<Option<(PathBuf, Vec<PathBuf>, BTreeMap<Vec<u8>, Item>)>, DataStoreError> {
        if sstable_files.len() < MAX_SSTABLE_SEGMENTS {
            return Ok(None);
        }

        let files_to_compact = sstable_files.clone();
        let mut merged = BTreeMap::new();

        for path in &files_to_compact {
            for (encoded_key, item) in read_all_sstable_entries(path)? {
                merged.insert(encoded_key, item);
            }
        }

        // Drop tombstones
        merged.retain(|_, item| item.status == ItemStatus::Active);

        let timestamp = crate::clock::now_millis();
        let new_sst_path = partition_dir.join(format!("{}.compact.{}", timestamp, SST_EXTENSION));

        Ok(Some((new_sst_path, files_to_compact, merged)))
    }
}
