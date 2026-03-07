## 1. Project Setup

- [x] 1.1 Add `sstable` crate dependency to `gossipgrid/Cargo.toml`
- [x] 1.2 Add `sstable-store` feature flag to `gossipgrid/Cargo.toml` (not in default features)
- [x] 1.3 Create module `gossipgrid/src/store/sstable_store/` with `mod.rs`

## 2. Key Encoding

- [x] 2.1 Implement key encoding function: `StorageKey` → `{partition_key}/{range_key}` as `Vec<u8>`
- [x] 2.2 Implement key decoding function: `&[u8]` → `StorageKey`
- [x] 2.3 Add unit tests for key encoding/decoding roundtrip, edge cases (empty range key, special characters)

## 3. Per-Partition Store

- [x] 3.1 Define `PartitionStore` struct holding: memtable (`BTreeMap<Vec<u8>, Item>`), list of SSTable file paths, partition directory path
- [x] 3.2 Implement `PartitionStore::get()` — check memtable first, then SSTables newest-to-oldest using `Table::get()`
- [x] 3.3 Implement `PartitionStore::get_many()` — merge memtable BTreeMap prefix iteration with SSTable `TableIterator` prefix scans, deduplicate by key (newest wins), reverse for descending order
- [x] 3.4 Implement `PartitionStore::insert()` — write to memtable BTreeMap
- [x] 3.5 Implement `PartitionStore::remove()` — write tombstone entry to memtable
- [x] 3.6 Implement `PartitionStore::count()` — count active (non-tombstoned) items across memtable and SSTables

## 4. SstableStore (StoreEngine Implementation)

- [ ] 4.1 Define `SstableStore` struct holding: `DashMap<PartitionId, PartitionStore>`, data directory path, flush threshold config
- [ ] 4.2 Implement `StoreEngine::is_in_memory_store()` — return `false`
- [ ] 4.3 Implement `StoreEngine::get()` — route to partition's `PartitionStore::get()` via `spawn_blocking`
- [ ] 4.4 Implement `StoreEngine::get_many()` — route to partition's `PartitionStore::get_many()` via `spawn_blocking`
- [ ] 4.5 Implement `StoreEngine::insert()` — route to partition's `PartitionStore::insert()`, check flush threshold
- [ ] 4.6 Implement `StoreEngine::remove()` — route to partition's `PartitionStore::remove()`
- [ ] 4.7 Implement `StoreEngine::partition_counts()` — aggregate counts from all `PartitionStore` instances

## 5. Memtable Flush

- [ ] 5.1 Implement `PartitionStore::flush()` — sort memtable entries, write to new SSTable file via `TableBuilder`, clear memtable
- [ ] 5.2 Implement double-buffer swap: on flush trigger, swap active memtable with fresh empty one so writes continue while flushing
- [ ] 5.3 Ensure reads check both active memtable and flushing memtable during flush
- [ ] 5.4 Record flushed LSN for WAL truncation
- [ ] 5.5 Trigger flush on `spawn_blocking` when memtable exceeds size threshold

## 6. SSTable Compaction

- [ ] 6.1 Implement `PartitionStore::compact()` — open all SSTable files, merge-iterate, drop tombstones, write single new file via `TableBuilder`
- [ ] 6.2 Trigger compaction check after each flush (when file count exceeds threshold, e.g., 8)
- [ ] 6.3 Ensure old SSTable files are removed only after new compacted file is fully written
- [ ] 6.4 Ensure reads are not blocked during compaction (swap file list atomically after compaction completes)

## 7. WAL Integration & Crash Recovery

- [ ] 7.1 Implement `SstableStore::new()` — scan partition directories, load existing SSTable file lists, initialize empty memtables
- [ ] 7.2 Implement WAL replay on startup — read WAL from last known LSN, replay into memtables
- [ ] 7.3 Persist last flushed LSN per partition (e.g., to a metadata file in partition directory)
- [ ] 7.4 Integrate WAL truncation after successful flush

## 8. NodeBuilder Integration

- [ ] 8.1 Wire `SstableStore` into `NodeBuilder` so it can be used via `.store(Box::new(SstableStore::new(config)))`
- [ ] 8.2 Add data directory path configuration

## 9. Testing

- [x] 9.1 Unit tests for `PartitionStore` CRUD operations (mirror existing `InMemoryStore` tests)
- [x] 9.2 Unit tests for `get_many` ordering, filtering, skip_null_rk, and limit
- [ ] 9.3 Unit tests for flush: verify data persists after flush, memtable is cleared
- [ ] 9.4 Unit tests for compaction: verify tombstones are removed, data integrity preserved
- [ ] 9.5 Integration test: insert items, restart `SstableStore` (simulated), verify items are recovered from SSTable files
- [ ] 9.6 Integration test: WAL replay — insert items, simulate crash (don't flush), restart, verify recovery from WAL
- [ ] 9.7 Integration test: reads during flush — concurrent read/write during active flush
