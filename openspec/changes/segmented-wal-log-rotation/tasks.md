# Tasks: Segmented WAL and Log Rotation

## 1. Foundation & Structures

- [ ] 1.1 Update `WalFile` struct to include `segment_index` and `segment_offset`.
- [ ] 1.2 Implement a helper function to generate segment filenames (e.g., `part_1_0000000001.wal`).
- [ ] 1.3 Update `WalLocalFile` to use `SEGMENT_SIZE_KB` constant for rotation threshold.

## 2. Segment Discovery & Startup

- [ ] 2.1 Implement `find_latest_segment(partition_id)` to scan the base directory and return the highest existing segment index and its size.
- [ ] 2.2 Refactor `WalLocalFile::append` internal file opening to use discovery logic instead of assuming a single file name.
- [ ] 2.3 Ensure `read_lsn` correctly handles reading the last record from the latest segment during initialization.

## 3. Rotation Implementation

- [ ] 3.1 In `WalLocalFile::append`, add a check if the current record will exceed the segment size.
- [ ] 3.2 Implement the `rotate_segment(partition_id)` method: flush, sync, close current, and open new indexed file.
- [ ] 3.3 Update the `write_handles` map with the new file handle post-rotation.

## 4. Chained Reader Implementation

- [ ] 4.1 Create a `SegmentedIterator` wrapper that can handle switching between multiple `BufReader`s.
- [ ] 4.2 Update `stream_from` to find the correct starting segment based on the requested `from_lsn` or `from_offset`.
- [ ] 4.3 Implement "next segment" logic in the iterator: when EOF is reached, attempt to open `index + 1`.

## 5. Cleanup Logic

- [ ] 5.1 Implement `purge_before(partition_id, min_lsn)` in `WalLocalFile`.
- [ ] 5.2 Add logic to check the tail of each segment to determine if its max LSN is eligible for deletion.
- [ ] 5.3 Ensure the active segment is never deleted by the purge logic.

## 6. Testing & Validation

- [ ] 6.1 Add a test case for basic rotation: write enough data to trigger a second segment and verify file existence.
- [ ] 6.2 Add a test case for chained reading: read records that span across two segments.
- [ ] 6.3 Add a test case for recovery: restart the WAL and ensure it continues from the latest segment and LSN.
- [ ] 6.4 Add a test case for purging: verify segments are deleted only when they and all prior segments are below the `min_lsn`.
