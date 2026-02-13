# Specifications: Segmented WAL and Log Rotation

## ADDED Requirements

### Requirement: Segment-based File Naming
The system SHALL store partition data in multiple files using a segment-based naming convention.

#### Scenario: First segment creation
- **WHEN** a new partition is initialized
- **THEN** a file named `part_{partition}_0000000001.wal` is created.

### Requirement: Automatic Segment Rotation
The system SHALL rotate to a new segment when the current segment exceeds the size threshold.

#### Scenario: Writing beyond threshold
- **WHEN** a record is appended that would cause the current segment to exceed `SEGMENT_SIZE_MAX`
- **THEN** the system closes the current segment and creates `part_{partition}_{index+1}.wal`.

### Requirement: Startup Discovery
The system SHALL identify and resume from the latest available segment on startup.

#### Scenario: Resuming after restart
- **WHEN** the WAL is initialized and segments `0000000001` through `0000000005` exist
- **THEN** writing resumes at the end of segment `0000000005`.

### Requirement: Chained Segment Streaming
The `WalReader` SHALL transparently transition between segment files during iteration.

#### Scenario: Reading across boundary
- **WHEN** the reader reaches the end of segment `N` and segment `N+1` exists
- **THEN** the reader opens segment `N+1` and continues streaming without returning EOF.

### Requirement: LSN-based Purging
The system SHALL allow deleting old segments that are no longer required for data recovery.

#### Scenario: Purging old segments
- **WHEN** `purge_before(partition_id, min_lsn)` is called
- **THEN** all segments whose highest LSN is less than `min_lsn` are deleted from disk, provided they are not the active segment.

### Requirement: Segment Pre-allocation
The system SHALL pre-allocate disk space for new segment files on Linux to reduce fragmentation and avoid mid-write space exhaustion.

#### Scenario: New segment creation on Linux
- **WHEN** a new segment file is created (either initial or via rotation) on a Linux host
- **THEN** the system calls `fallocate` with `FALLOC_FL_KEEP_SIZE` to reserve `segment_size_max` bytes of contiguous disk space without changing the file's logical size.

#### Scenario: Non-Linux platform
- **WHEN** a new segment file is created on a non-Linux platform
- **THEN** no pre-allocation is performed and the system operates normally without contiguous space reservation.

## MODIFIED Requirements

### Requirement: WAL Isolation
Modified to ensure that operations (rotation, cleanup) on one partition do not affect the concurrent operation of other partitions.
