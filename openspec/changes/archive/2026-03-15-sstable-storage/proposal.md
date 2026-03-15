## Why

GossipGrid currently only has an in-memory storage backend (`InMemoryStore`). All data is lost on node restart, limiting the system to ephemeral workloads. Adding a persistent, disk-backed storage engine is essential for production use. The `sstable` crate (v0.11.1) provides a solid foundation — it implements sorted string tables with bloom filters, checksums, compression, and efficient range iteration. Building an LSM-tree-style engine on top of it enables durable, high-performance storage that aligns well with GossipGrid's partition-key/range-key data model.

## What Changes

- Add a new `SstableStore` implementation of the `StoreEngine` trait using an LSM-tree architecture:
  - In-memory write buffer (memtable) for fast writes
  - Background flush of memtable to immutable SSTable files on disk
  - Reads merge results from memtable and SSTable files
  - Compaction of SSTable files to reclaim space and maintain read performance
- Reuse the existing `pwal`-based WAL infrastructure (`WalRecord`, `FramedWalRecordItem` in `wal.rs`) for crash recovery of unflushed memtable data
- Introduce a new `sstable-store` feature flag (similar to existing `memory-store`)
- Encode `StorageKey` + `PartitionId` as SSTable keys to maintain sorted ordering for efficient range queries via `get_many`
- Handle tombstone-based deletes compatible with SSTable immutability
- Serialize `Item`/`ItemEntry` values using existing bincode serialization

## Capabilities

### New Capabilities
- `sstable-storage`: Persistent disk-backed storage engine implementing `StoreEngine` using the `sstable` crate with LSM-tree architecture (memtable, WAL, flush, compaction)

### Modified Capabilities
_None — this is a new storage backend behind a feature flag; no existing specs change._

## Impact

- **Dependencies**: New dependency on `sstable` crate (v0.11.1); no transitive dependency conflicts expected
- **Code**: New module `gossipgrid/src/store/sstable_store.rs` (or submodule) alongside existing `memory_store.rs`
- **Feature flags**: New `sstable-store` feature in `gossipgrid/Cargo.toml`; default remains `memory-store`
- **Configuration**: Nodes will need a configurable data directory path for SSTable files and WAL
- **APIs**: No changes to public APIs — `SstableStore` implements the same `StoreEngine` trait
- **Existing tests**: Unaffected; new tests needed for the SSTable backend
- **Performance considerations**: Writes go to memtable (fast), reads may need to check multiple SSTables (mitigated by bloom filters), compaction runs in background
