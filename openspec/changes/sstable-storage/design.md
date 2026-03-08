## Context

GossipGrid currently only supports `InMemoryStore` — a `DashMap`-based implementation of the `StoreEngine` trait. Data survives restarts only because the WAL (`pwal`) replays into the in-memory store on startup (`hydrate_store_from_wal_task`). This works but means every restart replays the entire WAL, and memory is the scaling limit.

The storage architecture is pluggable: `NodeBuilder::store()` accepts `Box<dyn Store + Send + Sync>`, and `Env` holds the store alongside a separate WAL instance. The `StoreEngine` trait defines 5 operations: `get`, `get_many`, `insert`, `remove`, and `partition_counts`.

The `sstable` crate provides immutable sorted string tables with `TableBuilder` (write, keys must be added in sorted order), `Table` (read via `get` or `iter`/`SSIterator`), bloom filters, and checksums. Keys and values are `&[u8]`.

## Goals / Non-Goals

**Goals:**
- Persistent storage that survives restarts without full WAL replay
- Implement all `StoreEngine` operations with correct semantics (tombstone handling, descending range key order in `get_many`, partition counts)
- Reuse existing `pwal` infrastructure for crash recovery of unflushed writes
- Additive change behind `sstable-store` feature flag — no disruption to `InMemoryStore`

**Non-Goals:**
- Multi-version concurrency control (MVCC) — not needed for current use
- Cross-partition transactions
- Custom compaction scheduling/tuning (use simple size-tiered strategy for v1)
- Replacing the WAL — we reuse `pwal` as-is

## Decisions

### 1. LSM-tree architecture with memtable + SSTable levels

**Decision:** Use a classic two-level LSM approach: mutable memtable (in-memory BTreeMap) → immutable SSTable files on disk.

**Why:** SSTables are immutable by design — the `sstable` crate's `TableBuilder` requires keys in sorted order and produces read-only files. To support mutable `insert`/`remove` operations required by `StoreEngine`, we need a write buffer (memtable) that flushes to SSTables periodically.

**Alternatives considered:**
- Direct file rewrite per mutation: Too slow, defeats SSTable benefits
- Using a different crate with built-in mutability (e.g., sled, rocksdb): Adds heavier dependencies; the `sstable` crate is lightweight and aligns with the project's approach of building components from primitives

### 2. Key encoding: `{partition_key}/{range_key}`

**Decision:** Encode SSTable keys as `{partition_key}/{range_key}` (UTF-8 bytes). No partition ID in the key — partition isolation is handled at the directory level (see Decision #3).

**Why:** The `StoreEngine` trait already receives `&PartitionId` on every call, so embedding it in keys would be redundant. Without it:
- Keys are shorter and simpler
- Items for the same partition key are contiguous → `get_many` uses prefix scan
- Range keys sort lexicographically within their partition key → natural SSTable ordering
- `get` is a direct key lookup via `Table::get()`

**Alternatives considered:**
- Encoding `partition_id` in key prefix: Redundant since caller provides it; wastes bytes and forces prefix scanning past other partitions
- Binary key encoding: More compact but harder to debug; string encoding is sufficient for v1

### 3. Per-partition directory layout

**Decision:** Each partition gets its own directory (e.g., `data/{partition_id}/`) containing its own memtable and SSTable files. Keys within each partition's files are just `{partition_key}/{range_key}`.

**Why:**
- No wasted prefix scanning — queries don't skip over other partitions' data
- Natural partition isolation — partition reassignment means ignoring/deleting a directory
- Smaller per-partition SSTable files → better bloom filter hit rates
- Independent flush/compaction — hot partitions can flush without affecting cold ones
- `partition_counts` reduces to counting per partition directory

**Alternatives considered:**
- Single SSTable namespace for all partitions with partition_id in key prefix: Simpler compaction but forces prefix scanning and couples storage keys to cluster topology

### 4. Reuse existing `pwal` WAL for memtable durability

**Decision:** Write mutations to the existing WAL (via `WalRecord::Put`/`WalRecord::Delete`) before applying to memtable. On restart, replay remaining WAL segments into the store instead of executing a full store hydration strategy. Utilize an async event-driven architecture and a graceful shutdown flush to manage WAL file rotation safely.

**Why:** The `pwal` crate supports per-partition log streams. Because `SstableStore` insertions into memtables are idempotent based on HLC timestamps, we completely decouple the WAL truncation logic from the storage engine:
- Upon reaching a size threshold and successfully flushing an SSTable, `SstableStore` simply emits an `SstableFlushed(PartitionId)` event via `EventBus`.
- An event listener instructs `pwal` to truncate its log, keeping only the most recent segment files for that partition (guaranteeing we keep unflushed items that straddled a segment boundary).
- On a crash, the node restarts and `pwal` blindly replays its remaining segment records back into the memtable. Duplicate records that were already flushed to SSTables prior to the crash are idempotently merged or overwritten safely.
- During a graceful shutdown, a new `StoreEngine::flush_all()` method forces all memtables to flush and emit events, resulting in near-zero WAL segments remaining, allowing instant/zero-replay boot times.

### 5. Memtable flush trigger: size-based threshold

**Decision:** Flush memtable to a new SSTable file when it reaches a configurable size threshold (default: 4 MB estimated serialized size).

**Why:** Simple, predictable. Time-based flushing could leave large memtables during write bursts. The blocking `TableBuilder` disk write is offloaded to a detached background task (`tokio::spawn`), allowing the actual `insert` method to return `Ok(())` instantaneously when the size threshold is crossed.

### 6. Simple size-tiered compaction for v1

**Decision:** When the number of SSTable files exceeds a threshold (e.g., 8 files), merge all files into one, dropping tombstoned entries. This evaluation is triggered by the `EventBus` listener reacting to an `SstableFlushed` event.

**Why:** Simplest compaction strategy. Prevents unbounded file growth while keeping implementation minimal. Tying the trigger to the `SstableFlushed` event ensures we evaluate size bounds only when they are effectively increased, preventing tight-loop checks. Can be upgraded to leveled compaction later if needed.

### 7. Tombstone-based deletes

**Decision:** `remove()` inserts a tombstone marker in the memtable (reusing `ItemStatus::Tombstone`). Tombstones are written to SSTables and removed during compaction.

**Why:** SSTables are immutable — can't delete in place. Tombstones are already part of the `Item` model (`ItemStatus::Tombstone(u64)`), so the data model needs no changes.

### 8. Read path: merge memtable + SSTable results

**Decision:** For `get`: check memtable first, then search SSTables (newest to oldest). For `get_many`: merge results from memtable BTreeMap iteration and SSTable `TableIterator` prefix scans, deduplicating by key (newest wins).

**Why:** This is the standard LSM read path. Memtable check first gives O(log n) for recent writes. Bloom filters on SSTables eliminate unnecessary disk reads for `get`.

### 9. `get_many` descending order via reverse iteration

**Decision:** `get_many` collects items in ascending order from the BTreeMap/SSTable iterators, then reverses the result to return descending order by range key (matching `InMemoryStore` behavior).

**Why:** Both BTreeMap and SSTable iterate in ascending key order naturally. The existing test suite (`test_get_many_order`) expects descending order. Reversing a limited result set is efficient.

## Risks / Trade-offs

**[Write amplification]** → LSM writes data multiple times (memtable → SSTable → compaction). Acceptable for this use case where reads are more frequent than writes.

**[Read amplification on `get_many`]** → May need to scan across multiple SSTables per partition before compaction merges them. Mitigated by limiting SSTable file count via compaction and bloom filter short-circuiting on `get`.

**[Many small partitions → many directories]** → If partition count is high, each gets its own directory and file set. Acceptable since partition count is fixed at cluster creation and typically modest (6-64).

**[Synchronous `sstable` crate]** → `TableBuilder` and `Table` use synchronous I/O. Mitigated by running flush/compaction on `spawn_blocking` tasks. Read operations use `spawn_blocking` for SSTable access.

**[Memtable memory pressure]** → Large write bursts could grow memtable significantly before flush. Mitigated by the size-based flush threshold and the option to trigger emergency flush.

**[Concurrent access during flush]** → While flushing, new writes go to a fresh memtable while the old one is being written to disk. Reads must check both the active memtable and the flushing memtable. Requires careful coordination with `RwLock` or swap-based double buffering.
