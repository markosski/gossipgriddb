## ADDED Requirements

### Requirement: Persistent storage via SSTable engine
The system SHALL provide a persistent disk-backed storage engine (`SstableStore`) that implements the `StoreEngine` trait, using the `sstable` crate with an LSM-tree architecture (memtable + SSTable files).

#### Scenario: Store survives node restart
- **WHEN** a node with `SstableStore` is stopped and restarted
- **THEN** all previously inserted items SHALL be available without full WAL replay

#### Scenario: Feature flag isolation
- **WHEN** the `sstable-store` feature is enabled in `Cargo.toml`
- **THEN** `SstableStore` SHALL be available as a storage backend
- **AND** the existing `InMemoryStore` (under `memory-store` feature) SHALL remain unaffected

### Requirement: Get item by key
The system SHALL retrieve a single item by exact `StorageKey` (partition_key + range_key) lookup. The store SHALL check the memtable first, then SSTable files from newest to oldest, returning the first match.

#### Scenario: Item exists in memtable
- **WHEN** `get` is called with a `StorageKey` that exists in the active memtable
- **THEN** the corresponding `ItemEntry` SHALL be returned

#### Scenario: Item exists in SSTable
- **WHEN** `get` is called with a `StorageKey` not in memtable but present in an SSTable file
- **THEN** the corresponding `ItemEntry` SHALL be returned

#### Scenario: Item is tombstoned
- **WHEN** `get` is called for a key whose most recent entry has `ItemStatus::Tombstone`
- **THEN** `None` SHALL be returned

#### Scenario: Item does not exist
- **WHEN** `get` is called for a key that does not exist in memtable or any SSTable
- **THEN** `None` SHALL be returned

### Requirement: Get many items by partition key
The system SHALL retrieve multiple items for a given partition key, returning results in descending order by range key, consistent with `InMemoryStore` behavior.

#### Scenario: Retrieve all items for a partition key
- **WHEN** `get_many` is called with a `StorageKey` containing only a partition_key (no range_key filter)
- **THEN** all active items with that partition_key SHALL be returned in descending range key order, up to the specified `limit`

#### Scenario: Filter by range key substring
- **WHEN** `get_many` is called with a `StorageKey` containing a range_key value
- **THEN** only items whose range_key contains the filter value SHALL be returned

#### Scenario: Skip null range keys
- **WHEN** `get_many` is called with `GetManyOptions.skip_null_rk = true`
- **THEN** items with `None` range key SHALL be excluded from results

#### Scenario: Merge across memtable and SSTables
- **WHEN** items for the same partition key exist in both the memtable and SSTable files
- **THEN** results SHALL be merged, with the newest entry winning for duplicate keys

### Requirement: Insert item
The system SHALL insert items into the memtable, writing to the WAL first for durability. If an item with the same key exists, it SHALL be updated.

#### Scenario: Insert new item
- **WHEN** `insert` is called with a new `StorageKey`
- **THEN** the item SHALL be written to the WAL and stored in the memtable
- **AND** the partition item count SHALL be incremented

#### Scenario: Update existing item
- **WHEN** `insert` is called with an existing `StorageKey`
- **THEN** the item in the memtable SHALL be replaced with the new value

### Requirement: Remove item
The system SHALL remove items by writing a tombstone entry to the memtable (and WAL), consistent with SSTable immutability.

#### Scenario: Remove existing item
- **WHEN** `remove` is called for an existing key
- **THEN** a tombstone entry SHALL be written to the memtable
- **AND** the partition item count SHALL be decremented

#### Scenario: Remove non-existent item
- **WHEN** `remove` is called for a key that does not exist
- **THEN** a tombstone SHALL still be written (to mask older SSTable entries)

### Requirement: Partition counts
The system SHALL maintain accurate per-partition item counts, excluding tombstoned items.

#### Scenario: Counts reflect active items only
- **WHEN** `partition_counts` is called
- **THEN** the returned counts SHALL reflect only items with `ItemStatus::Active`, across both memtable and SSTable files

### Requirement: Per-partition directory layout
The system SHALL organize storage with a separate directory per partition, each containing its own memtable and SSTable files.

#### Scenario: Partition isolation
- **WHEN** items are inserted into different partitions
- **THEN** each partition's data SHALL be stored in a separate directory (e.g., `data/{partition_id}/`)
- **AND** each partition SHALL have independent flush and compaction schedules

### Requirement: Memtable flush to SSTable
The system SHALL flush the in-memory write buffer (memtable) to an immutable SSTable file on disk when a configurable size threshold is reached.

#### Scenario: Size-based flush trigger
- **WHEN** the memtable for a partition exceeds the configured size threshold (default: 4 MB)
- **THEN** the memtable SHALL be flushed to a new SSTable file in that partition's directory
- **AND** a fresh empty memtable SHALL replace the flushed one
- **AND** the WAL SHALL be purged, retaining only the latest 2 segments for the partition

#### Scenario: Reads during flush
- **WHEN** a flush is in progress for a partition
- **THEN** read operations SHALL check both the active memtable and the flushing memtable to ensure no data is missed

### Requirement: SSTable compaction
The system SHALL compact SSTable files to reclaim space and maintain read performance.

#### Scenario: Compaction triggered by file count
- **WHEN** the number of SSTable files for a partition exceeds a threshold (e.g., 8 files)
- **THEN** the system SHALL merge all files into one, dropping tombstoned entries

#### Scenario: Compaction does not block reads
- **WHEN** compaction is running for a partition
- **THEN** read operations SHALL continue to work against the existing SSTable files until the new compacted file is ready

### Requirement: WAL integration for crash recovery
The system SHALL reuse the existing `pwal`-based WAL infrastructure for crash recovery of unflushed memtable data.

#### Scenario: Crash recovery on restart
- **WHEN** a node crashes with unflushed memtable data
- **THEN** on restart, the `SstableStore` SHALL replay the WAL from known LSN to reconstruct the memtable state
- **AND** SSTable files on disk SHALL remain intact and usable

