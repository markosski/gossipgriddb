# Tasks: Segmented WAL and Log Rotation

## 1. Foundation & Structures

- [x] 1.1 Update `WalFile` struct to include `segment_index` and `segment_offset`.
- [x] 1.2 Implement a helper function to generate segment filenames (e.g., `part_1_0000000001.wal`).
- [x] 1.3 Update `WalLocalFile` to use `SEGMENT_SIZE_MAX` constant for rotation threshold.
- [x] 1.4 Pre-allocate new segment files to `segment_size_max` on creation using `fallocate(FALLOC_FL_KEEP_SIZE)` on Linux (no-op on other platforms).

## 2. Segment Discovery & Startup

- [x] 2.1 Implement `find_latest_segment(partition_id)` to scan the base directory and return the highest existing segment index and its size.
- [x] 2.2 Refactor `WalLocalFile::append` internal file opening to use discovery logic instead of assuming a single file name.
- [x] 2.3 Ensure `read_lsn` correctly handles reading the last record from the latest segment during initialization.

## 3. Rotation Implementation

- [x] 3.1 In `WalLocalFile::append`, add a check if the current record will exceed the segment size.
- [x] 3.2 Implement the `rotate_segment(partition_id)` method: flush, sync, close current, and open new indexed file.
- [x] 3.3 Update the `write_handles` map with the new file handle post-rotation.

## 4. Chained Reader Implementation

- [x] 4.1 Create a `SegmentedIterator` wrapper that can handle switching between multiple `BufReader`s.
- [x] 4.2 Update `stream_from` to find the correct starting segment based on the requested `from_lsn` or `from_offset`.
- [x] 4.3 Implement "next segment" logic in the iterator: when EOF is reached, attempt to open `index + 1`.

## 5. Cleanup Logic

- [x] 5.1 Implement `purge_before(partition_id, min_lsn)` in `WalLocalFile`.
- [x] 5.2 Add logic to check the tail of each segment to determine if its max LSN is eligible for deletion. Mark all eligible segments as deletable and perform deletion.
- [x] 5.3 Ensure the active segment is never deleted by the purge logic.

## 6. Testing & Validation

- [x] 6.1 Add a test case for basic rotation: write enough data to trigger a second segment and verify file existence.
- [x] 6.2 Add a test case for chained reading: read records that span across two segments.
- [x] 6.3 Add a test case for recovery: restart the WAL and ensure it continues from the latest segment and LSN.
- [x] 6.4 Add a test case for purging: verify segments are deleted only when they and all prior segments are below the `min_lsn`.
