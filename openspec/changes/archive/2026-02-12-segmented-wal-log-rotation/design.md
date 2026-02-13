# Design: Segmented WAL and Log Rotation

## Context
The `gossipgrid-wal` crate provides a Write-Ahead Log for partition-based data. Currently, it maintains one file per partition. As the database grows, these files become unmanageable. We need to introduce a segmented approach where data is split into multiple files (segments), allowing for easier cleanup and rotation.

## Goals / Non-Goals

**Goals:**
- **Segmentation**: Split partition data across multiple files of a fixed maximum size.
- **Auto-Rotation**: Automatically switch to a new segment when the current one is full.
- **Efficient Discovery**: Quickly find the active segment on restart.
- **Cleanup**: Provide a mechanism to delete old segments that are no longer needed for recovery.
- **Backward Compatibility**: Ensure existing `WalReader` and `WalWriter` traits continue to work without changes to their API.

**Non-Goals:**
- **Compression**: While useful, compression of old segments is deferred to a future iteration.
- **Checksumming**: Integrity checks are out of scope for this specific rotation logic change.
- **Index Files**: We will rely on filesystem scanning initially rather than maintaining a separate metadata index.

## Decisions

### 1. File Naming Convention
Segments will be named using the following pattern:
`part_{partition_id}_{segment_index}.wal`
The `segment_index` will be a 10-digit, zero-padded integer (e.g., `part_1_0000000001.wal`). This ensures that standard lexical sorting matches chronological order.

### 2. Segment Rotation Logic
The `WalWriter` implementation (`WalLocalFile`) will track the `segment_offset` (bytes written to the current file). When `segment_offset + record_size > SEGMENT_SIZE_MAX`, the following occurs:
1.  Flush the current `BufWriter`.
2.  Perform a `sync_data()` syscall on the underlying file handle.
3.  Close the current file.
4.  Increment the `segment_index`.
5.  Open a new file for writing.

### 3. "Discovery" on Startup
When `WalLocalFile` is initialized, it will perform a `std::fs::read_dir` on the `base_dir`. It will filter for files matching the partition pattern, extract the `segment_index`, and identify the highest index. 
- If no files exist, it starts at index `0000000001`.
- If files exist, it opens the highest index in `append` mode.

### 4. Chained Reader Implementation
The `WalReader`'s `stream_from` method will be refactored to use a "Segmented Iterator". 
- When the iterator reaches the end of the current segment, it will check for the existence of the next segment (`index + 1`).
- If it exists, it opens the next segment and continues streaming transparently to the caller.
- This maintains the abstraction of a single continuous stream of LSNs.

### 5. Cleanup (Purging)
A `purge_before(partition_id, min_lsn)` method will be added. 
- It lists all segments for the partition except the active one.
- For each segment, it reads the last 8 bytes (the LSN of the last record).
- If `last_lsn < min_lsn`, the segment file is deleted.

### 6. Segment Pre-allocation
New segment files are pre-allocated to `segment_size_max` bytes using the Linux `fallocate` syscall with the `FALLOC_FL_KEEP_SIZE` flag. This reserves contiguous disk blocks without changing the file's logical size, so existing `seek(End(-8))` patterns in `read_lsn` and purge logic continue to work correctly. On non-Linux platforms the pre-allocation step is a no-op — correctness is unaffected, only the fragmentation-reduction benefit is lost.

Pre-allocation happens in two places:
1.  When a new segment is opened for the first time (initial append for a partition).
2.  When `rotate_segment` creates the next segment file.

## Risks / Trade-offs

- **File Handle Management**: Active writers hold open handles. Readers only hold handles while actively iterating. We must ensure `WalFile` is dropped correctly on rotation to avoid leaking handles.
- **Performance**: Directory scanning on startup is $O(N)$ relative to the number of segments. With a 64MB segment size, $10,000$ segments would be $640GB$, so $N$ is unlikely to be large enough to cause significant latency in the near term.
- **Atomic Rotation**: There is a brief window where a crash could occur after closing one segment but before opening the next. However, since the LSN is incremented *after* the write succeeds, the system remains consistent on recovery.
