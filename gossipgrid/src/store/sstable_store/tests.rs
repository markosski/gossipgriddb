use std::{
    fs::File,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{clock::HLC, item::ItemStatus};

use super::*;
use sstable::TableBuilder;

fn temp_partition_dir(test_name: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("gossipgrid-{test_name}-{nanos}-{unique}"))
}

fn test_item(message: &str, status: ItemStatus) -> Item {
    Item {
        message: message.as_bytes().to_vec(),
        status,
        hlc: HLC::new(),
    }
}

fn write_sstable(
    partition_dir: &std::path::Path,
    file_name: &str,
    entries: Vec<(StorageKey, Item)>,
) -> PathBuf {
    let path = partition_dir.join(file_name);
    let file = File::create(&path).unwrap();
    let mut builder = TableBuilder::new(Options::default(), file);

    let mut encoded_entries = entries
        .into_iter()
        .map(|(key, item)| (encode_storage_key(&key), encode_item(&item).unwrap()))
        .collect::<Vec<_>>();
    encoded_entries.sort_by(|left, right| left.0.cmp(&right.0));

    for (key, value) in encoded_entries {
        builder.add(&key, &value).unwrap();
    }
    builder.finish().unwrap();
    path
}

#[test]
fn test_storage_key_roundtrip_with_special_characters() {
    let key = StorageKey::new(
        PartitionKey("user/with\\slashes".to_string()),
        Some(RangeKey("profile/settings\\".to_string())),
    );

    let encoded = encode_storage_key(&key);
    let decoded = decode_storage_key(&encoded).unwrap();

    assert_eq!(decoded, key);
}

#[test]
fn test_storage_key_roundtrip_with_empty_range_key() {
    let key = StorageKey::new(
        PartitionKey("user-123".to_string()),
        Some(RangeKey(String::new())),
    );

    let encoded = encode_storage_key(&key);
    assert_eq!(std::str::from_utf8(&encoded).unwrap(), "user-123/");
    let decoded = decode_storage_key(&encoded).unwrap();

    assert_eq!(decoded, key);
}

#[test]
fn test_storage_key_roundtrip_without_range_key() {
    let key = StorageKey::new(PartitionKey("user-123".to_string()), None);

    let encoded = encode_storage_key(&key);
    assert_eq!(std::str::from_utf8(&encoded).unwrap(), "user-123");
    let decoded = decode_storage_key(&encoded).unwrap();

    assert_eq!(decoded, key);
}

#[test]
fn test_partition_store_crud_and_count_across_memtable_and_sstables() {
    let partition_dir = temp_partition_dir("crud");
    let mut store = PartitionStore::new(partition_dir.clone()).unwrap();
    let pk = PartitionKey("acct-1".to_string());

    let existing = StorageKey::new(pk.clone(), Some(RangeKey("001".to_string())));
    let tombstoned = StorageKey::new(pk.clone(), Some(RangeKey("002".to_string())));
    let new_item = StorageKey::new(pk.clone(), Some(RangeKey("003".to_string())));

    let sstable_path = write_sstable(
        &partition_dir,
        "000001.sst",
        vec![
            (existing.clone(), test_item("old-001", ItemStatus::Active)),
            (tombstoned.clone(), test_item("old-002", ItemStatus::Active)),
        ],
    );
    store.sstable_files.push(sstable_path);
    store.recompute_count().unwrap();

    store
        .insert(&new_item, test_item("new-003", ItemStatus::Active))
        .unwrap();
    store
        .remove(
            &tombstoned,
            HLC {
                timestamp: 1,
                counter: 0,
            },
        )
        .unwrap();

    let existing_entry = store.get(&existing).unwrap().unwrap();
    assert_eq!(existing_entry.item.message, b"old-001".to_vec());

    let new_entry = store.get(&new_item).unwrap().unwrap();
    assert_eq!(new_entry.item.message, b"new-003".to_vec());

    assert!(store.get(&tombstoned).unwrap().is_none());
    assert_eq!(store.count().unwrap(), 2);

    fs::remove_dir_all(partition_dir).unwrap();
}

#[test]
fn test_partition_store_get_many_filtering_and_order() {
    let partition_dir = temp_partition_dir("get-many");
    let mut store = PartitionStore::new(partition_dir.clone()).unwrap();
    let pk = PartitionKey("user123".to_string());

    let base = vec![
        (
            StorageKey::new(pk.clone(), Some(RangeKey("profile".to_string()))),
            test_item("profile", ItemStatus::Active),
        ),
        (
            StorageKey::new(pk.clone(), Some(RangeKey("settings".to_string()))),
            test_item("settings", ItemStatus::Active),
        ),
        (
            StorageKey::new(pk.clone(), Some(RangeKey("session".to_string()))),
            test_item("session", ItemStatus::Active),
        ),
    ];

    let sstable_path = write_sstable(&partition_dir, "000001.sst", base);
    store.sstable_files.push(sstable_path);
    store.recompute_count().unwrap();
    store
        .insert(
            &StorageKey::new(pk.clone(), Some(RangeKey("timeline".to_string()))),
            test_item("timeline", ItemStatus::Active),
        )
        .unwrap();
    store
        .insert(
            &StorageKey::new(pk.clone(), None),
            test_item("no-range", ItemStatus::Active),
        )
        .unwrap();

    let filtered = store
        .get_many(
            &StorageKey::new(pk.clone(), Some(RangeKey("se".to_string()))),
            GetManyOptions {
                limit: 10,
                skip_null_rk: true,
            },
        )
        .unwrap();

    let filtered_rks = filtered
        .iter()
        .map(|entry| {
            entry
                .storage_key
                .range_key
                .as_ref()
                .unwrap()
                .value()
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        filtered_rks,
        vec!["settings".to_string(), "session".to_string()]
    );

    let ordered = store
        .get_many(
            &StorageKey::new(pk.clone(), None),
            GetManyOptions {
                limit: 3,
                skip_null_rk: true,
            },
        )
        .unwrap();
    let ordered_rks = ordered
        .iter()
        .map(|entry| {
            entry
                .storage_key
                .range_key
                .as_ref()
                .unwrap()
                .value()
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        ordered_rks,
        vec![
            "timeline".to_string(),
            "settings".to_string(),
            "session".to_string()
        ]
    );

    let with_null = store
        .get_many(
            &StorageKey::new(pk, None),
            GetManyOptions {
                limit: 10,
                skip_null_rk: false,
            },
        )
        .unwrap();
    assert_eq!(with_null.len(), 5);
    assert!(
        with_null
            .iter()
            .any(|entry| entry.storage_key.range_key.is_none())
    );

    fs::remove_dir_all(partition_dir).unwrap();
}

#[test]
fn test_partition_store_flush_persists_data_and_clears_memtable() {
    let partition_dir = temp_partition_dir("flush");
    let mut store = PartitionStore::new(partition_dir.clone()).unwrap();
    let pk = PartitionKey("user-1".to_string());

    let key1 = StorageKey::new(pk.clone(), Some(RangeKey("001".to_string())));
    let key2 = StorageKey::new(pk.clone(), Some(RangeKey("002".to_string())));

    store.insert(&key1, test_item("val1", ItemStatus::Active)).unwrap();
    store.insert(&key2, test_item("val2", ItemStatus::Active)).unwrap();

    assert_eq!(store.memtable.len(), 2);
    assert!(store.sstable_files.is_empty());

    let arc_flushing = store.trigger_flush().unwrap();
    assert!(store.memtable.is_empty());
    assert!(store.flushing_memtable.is_some());
    
    let timestamp = crate::clock::now_millis();
    let sst_path = partition_dir.join(format!("{}.sst", timestamp));
    
    write_memtable_to_sstable(&arc_flushing, &sst_path).unwrap();
    store.complete_flush(sst_path);

    assert!(store.flushing_memtable.is_none());
    assert_eq!(store.sstable_files.len(), 1);

    // Verify data can still be read
    let entry1 = store.get(&key1).unwrap().unwrap();
    assert_eq!(entry1.item.message, b"val1".to_vec());
    let entry2 = store.get(&key2).unwrap().unwrap();
    assert_eq!(entry2.item.message, b"val2".to_vec());
    
    fs::remove_dir_all(partition_dir).unwrap();
}

#[test]
fn test_partition_store_reads_during_active_flush() {
    let partition_dir = temp_partition_dir("flush-reads");
    let mut store = PartitionStore::new(partition_dir.clone()).unwrap();
    let pk = PartitionKey("user-reads".to_string());

    let key1 = StorageKey::new(pk.clone(), Some(RangeKey("001".to_string())));
    
    store.insert(&key1, test_item("val1", ItemStatus::Active)).unwrap();
    
    // trigger flush but don't complete it yet
    let _arc = store.trigger_flush().unwrap();
    
    // memtable is empty, but we can still read from flushing_memtable
    assert!(store.memtable.is_empty());
    
    let entry = store.get(&key1).unwrap().unwrap();
    assert_eq!(entry.item.message, b"val1".to_vec());
    
    // write a new item during flush
    let key2 = StorageKey::new(pk.clone(), Some(RangeKey("002".to_string())));
    store.insert(&key2, test_item("val2", ItemStatus::Active)).unwrap();
    
    assert_eq!(store.memtable.len(), 1);
    
    // both should be readable
    let entry2 = store.get(&key2).unwrap().unwrap();
    assert_eq!(entry2.item.message, b"val2".to_vec());
    
    fs::remove_dir_all(partition_dir).unwrap();
}
