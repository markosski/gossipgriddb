# Proposal: Segmented WAL and Log Rotation

## Summary
Implement a segmented Write-Ahead Log (WAL) system with automatic log rotation and cleanup capabilities for the `gossipgrid-wal` crate. This transition moves from a single large file per partition to multiple fixed-size segments, enabling efficient cleanup and better disk management.

## Problem Statement
The current implementation of `gossipgrid-wal` uses a single file per partition (`part_{id}.wal`). This approach has several limitations:
- **Unbounded Growth**: Files grow indefinitely, eventually consuming all available disk space.
- **Cleanup Difficulty**: Deleting data from the middle of a file is complex and inefficient.
- **Recovery Overhead**: Scanning a single massive file on startup increases recovery time.
- **Risk of Corruption**: A single corrupt file impacts the entire history of a partition.

## What Changes
1.  **Segmented Storage**: WAL files will follow the pattern `part_{partition}_{segment_index}.wal` (e.g., `part_1_0000000001.wal`).
2.  **Automatic Rotation**: When the active segment exceeds `SEGMENT_SIZE_KB` (default 64MB), it is closed and a new segment is created.
3.  **Discovery Logic**: On startup, the system will scan the base directory to identify all segments for each partition and resume writing to the highest-indexed segment.
4.  **Chained Reading**: `WalReader` will be updated to seamlessly stream records across segment boundaries.
5.  **Cleanup API**: A new method `purge_before(partition_id, min_lsn)` will be introduced to safely delete segments that only contain records with LSNs lower than the specified threshold.

## Capabilities

### New Capabilities
- `segmented-storage`: Manages multiple file segments per partition, including naming conventions and directory scanning.
- `automatic-rotation`: Monitors segment size and triggers rotation to a new file when thresholds are met.
- `chained-streaming`: Transparently handles transitions between segments during read/scan operations.
- `lsn-based-cleanup`: Safely deletes non-active segments based on LSN checkpoints provided by the application.

### Modified Capabilities
- `wal-persistence`: (Internal) The core append logic will be modified to handle segment transitions.

## Impact
- **Crate**: `gossipgrid-wal`
- **Structs**: `WalLocalFile`, `WalFile` (internal structure updated).
- **Traits**: No breaking changes to `WalWriter` or `WalReader` traits, but implementations will be significantly refactored.
- **Disk Layout**: File naming changes in the `base_dir`.
