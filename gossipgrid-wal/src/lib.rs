use bincode::{Decode, Encode};
use log::{error, info, warn};
use std::{
    collections::HashMap,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
};
use thiserror::Error;
use tokio::io::BufWriter;
use tokio::sync::Mutex;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::RwLock,
};

use serde::{Deserialize, Serialize};

pub type Lsn = u64;

/// Maximum segment size in bytes (64 MB).
const SEGMENT_SIZE_MAX: u64 = 64 * 1024 * 1024;

/// Generate a segment filename, e.g. `part_1_0000000001.wal`
fn segment_filename(partition: WalPartitionId, segment_index: u64) -> String {
    format!("part_{partition}_{segment_index:010}.wal")
}

/// Scan `base_dir` for segment files belonging to `partition` and return
/// `(highest_segment_index, file_size_in_bytes)`.
///
/// Segment files follow the pattern `part_{partition}_{index:010}.wal`.
/// If no matching files are found the function returns `(1, 0)` so the
/// first write creates segment index 1.
fn find_latest_segment(base_dir: &std::path::Path, partition: WalPartitionId) -> (u64, u64) {
    let prefix = format!("part_{}_", partition.0);

    let best = std::fs::read_dir(base_dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name().into_string().ok()?;
            if !name.starts_with(&prefix) || !name.ends_with(".wal") {
                return None;
            }
            // Extract the segment index between the second '_' and '.wal'
            let without_ext = name.strip_suffix(".wal")?;
            let idx_str = without_ext.strip_prefix(&prefix)?;
            let idx: u64 = idx_str.parse().ok()?;
            let size = entry.metadata().ok()?.len();
            Some((idx, size))
        })
        .max_by_key(|(idx, _)| *idx);

    best.unwrap_or((1, 0))
}

/// Return a sorted list of `(segment_index, file_size)` for every segment
/// file belonging to `partition` inside `base_dir`.
///
/// The returned vec is ordered by `segment_index` ascending.
fn find_all_segments(base_dir: &std::path::Path, partition: WalPartitionId) -> Vec<(u64, u64)> {
    let prefix = format!("part_{}_", partition.0);

    let mut segments: Vec<(u64, u64)> = std::fs::read_dir(base_dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name().into_string().ok()?;
            if !name.starts_with(&prefix) || !name.ends_with(".wal") {
                return None;
            }
            let without_ext = name.strip_suffix(".wal")?;
            let idx_str = without_ext.strip_prefix(&prefix)?;
            let idx: u64 = idx_str.parse().ok()?;
            let size = entry.metadata().ok()?.len();
            Some((idx, size))
        })
        .collect();

    segments.sort_by_key(|(idx, _)| *idx);
    segments
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode, Serialize, Deserialize)]
pub struct WalPartitionId(pub u16);

impl WalPartitionId {
    pub fn value(self) -> u16 {
        self.0
    }
}

impl std::fmt::Display for WalPartitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u16> for WalPartitionId {
    fn from(value: u16) -> Self {
        WalPartitionId(value)
    }
}

#[derive(Serialize, Deserialize, Encode, Decode, Clone, Debug, PartialEq)]
pub enum WalRecord {
    Put {
        partition: WalPartitionId,
        key: Vec<u8>,
        value: Vec<u8>,
        hlc: u64,
    },
    Delete {
        partition: WalPartitionId,
        key: Vec<u8>,
        hlc: u64,
    },
}

#[derive(Serialize, Deserialize, Encode, Decode, Debug)]
pub struct FramedWalRecord {
    pub lsn: Lsn,
    pub record: WalRecord,
}

#[async_trait::async_trait]
pub trait WalReader: Send + Sync {
    async fn stream_from(
        &self,
        from_lsn: Lsn,
        from_offset: u64,
        partition: WalPartitionId,
        is_exclusive: bool,
    ) -> Box<dyn Iterator<Item = Result<(FramedWalRecord, u64), WalError>> + '_>;

    async fn get_lsn_watcher(
        &self,
        partition_id: WalPartitionId,
    ) -> tokio::sync::watch::Receiver<u64>;
}

#[async_trait::async_trait]
pub trait WalWriter: Send + Sync {
    async fn append(&self, rec: WalRecord) -> Result<(u64, u64), WalError>;
    async fn io_sync(&self) -> Result<(), WalError>;
}

#[async_trait::async_trait]
pub trait Wal: WalReader + WalWriter {}
impl<T: WalReader + WalWriter> Wal for T {}

pub struct WalLocalFile {
    base_dir: PathBuf,
    truncate_at_start: bool,
    pub write_handles: RwLock<HashMap<WalPartitionId, Arc<Mutex<WalFile>>>>,
    pub lsn_watchers: RwLock<HashMap<WalPartitionId, tokio::sync::watch::Sender<u64>>>,
}

#[derive(Debug)]
pub struct WalFile {
    buffer: tokio::io::BufWriter<tokio::fs::File>,
    current_lsn: u64,
    current_offset: u64,
    partition: WalPartitionId,
    segment_index: u64,
    segment_offset: u64,
}

impl WalFile {
    async fn open_write(path: &String, truncate_at_start: bool) -> Result<File, WalError> {
        let mut write_file = tokio::fs::OpenOptions::new();
        write_file.read(true).create(true);

        if truncate_at_start {
            write_file.write(true);
            write_file.truncate(true);
        } else {
            write_file.append(true);
        }
        write_file
            .open(path)
            .await
            .map_err(|e| WalError::GeneralError(format!("Failed to open WAL file '{path}': {e}")))
    }
}

impl WalLocalFile {
    pub async fn new(base_dir: PathBuf, truncate: bool) -> Result<Self, WalError> {
        if truncate {
            match tokio::fs::remove_dir_all(&base_dir).await {
                Ok(()) => info!("deleted WAL dir '{base_dir:?}'"),
                Err(e) => warn!(
                    "Failed to delete WAL dir '{base_dir:?}', this may be ok if no previous cluster existed: {e}"
                ),
            }
        }

        match tokio::fs::create_dir_all(&base_dir).await {
            Ok(()) => info!("created WAL dir '{base_dir:?}'"),
            Err(e) => {
                error!("Failed to create WAL dir '{base_dir:?}': {e}");
                Err(WalError::GeneralError(format!(
                    "Failed to create WAL dir '{base_dir:?}': {e}"
                )))?
            }
        }

        Ok(Self {
            base_dir,
            truncate_at_start: truncate,
            write_handles: RwLock::new(HashMap::new()),
            lsn_watchers: RwLock::new(HashMap::new()),
        })
    }
}

#[async_trait::async_trait]
impl WalWriter for WalLocalFile {
    async fn io_sync(&self) -> Result<(), WalError> {
        let handles = self.write_handles.read().await;
        for (_, fd) in handles.iter() {
            let fd = fd.lock().await;
            let _ = fd.buffer.get_ref().sync_data().await;
        }
        Ok(())
    }

    async fn append(&self, rec: WalRecord) -> Result<(u64, u64), WalError> {
        let partition = match rec {
            WalRecord::Put { partition, .. } => partition,
            WalRecord::Delete { partition, .. } => partition,
        };

        // Try to get the file lock with a read lock on the map
        let maybe_file = {
            let map = self.write_handles.read().await;
            map.get(&partition).cloned()
        };

        let wal_file_lock = if let Some(lock) = maybe_file {
            lock
        } else {
            // Need to insert
            let mut map = self.write_handles.write().await;
            // Check again in case someone else inserted
            if let Some(lock) = map.get(&partition).cloned() {
                lock
            } else {
                let base_str = self.base_dir.to_str().ok_or_else(|| {
                    WalError::GeneralError("WAL base directory path is not valid UTF-8".to_string())
                })?;

                // Discover the latest segment for this partition
                let (seg_index, seg_size) = find_latest_segment(&self.base_dir, partition);
                let seg_name = segment_filename(partition, seg_index);
                let writer_path = format!("{base_str}/{seg_name}");

                // Open the file first to get latest LSN and offset
                let (lsn, offset) = if self.truncate_at_start {
                    (0, 0)
                } else {
                    match tokio::fs::File::open(&writer_path).await {
                        Ok(mut file) => WalLocalFile::read_lsn(&mut file).await.unwrap_or((0, 0)),
                        Err(e) => {
                            warn!(
                                "Failed to open WAL file, assuming fresh cluster, using (0, 0) : {e}"
                            );
                            (0, 0)
                        }
                    }
                };

                let file_write = WalFile::open_write(&writer_path, self.truncate_at_start).await?;

                let wal_file = WalFile {
                    buffer: tokio::io::BufWriter::new(file_write),
                    current_lsn: lsn,
                    current_offset: offset,
                    partition,
                    segment_index: seg_index,
                    segment_offset: seg_size,
                };

                let lock = Arc::new(Mutex::new(wal_file));
                map.insert(partition, lock.clone());
                lock
            }
        };

        let mut wal_file_write = wal_file_lock.lock().await;
        let current_lsn = wal_file_write.current_lsn;
        let current_offset = wal_file_write.current_offset;

        let new_lsn = current_lsn
            .checked_add(1)
            .ok_or_else(|| WalError::GeneralError("LSN overflow".to_string()))?;
        let framed = FramedWalRecord {
            lsn: new_lsn,
            record: rec.clone(),
        };
        let payload = bincode::encode_to_vec(&framed, bincode::config::standard())
            .map_err(|e| WalError::GeneralError(format!("Failed to encode WAL record: {e}")))?;

        // Check if this record will exceed the segment size
        // Rotate to a new segment if needed
        let record_size = 4 + payload.len() as u64 + 8; // len header + payload + lsn footer

        // segment_offset > 0 prevents infinite rotation loop if single record is greater than max segment size
        if wal_file_write.segment_offset + record_size > SEGMENT_SIZE_MAX
            && wal_file_write.segment_offset > 0
        {
            WalLocalFile::rotate_segment(&self.base_dir, &mut wal_file_write).await?;
        }

        let (lsn, offset) = WalLocalFile::write_data(
            &mut wal_file_write.buffer,
            &payload,
            new_lsn,
            current_offset,
        )
        .await?;

        wal_file_write.current_offset = offset;
        wal_file_write.current_lsn = lsn;
        wal_file_write.segment_offset += record_size;

        // Also update watchers
        {
            let map = self.lsn_watchers.read().await;
            if let Some(sender) = map.get(&partition) {
                let _ = sender.send(lsn);
            }
        }

        Ok((lsn, offset))
    }
}

impl WalLocalFile {
    async fn read_lsn(file: &mut File) -> Result<(u64, u64), std::io::Error> {
        // Get the file size first
        let file_size = file.seek(SeekFrom::End(0)).await?;

        // If file is empty or too small to contain an LSN, return 0
        if file_size < 8 {
            return Ok((0, 0));
        }

        // Seek to 8 bytes before the end
        file.seek(SeekFrom::End(-8)).await?;

        let mut buf = [0u8; 8];
        file.read_exact(&mut buf).await?;

        let lsn = u64::from_le_bytes(buf);
        Ok((lsn, file_size))
    }

    /// Write full record data to WAL file
    ///
    /// # Arguments
    /// * `buffer` - mutable reference to buffer writer
    /// * `payload` - record data to write
    /// * `lsn` - LSN of the record
    /// * `offset` - offset of the record
    ///
    /// # Returns
    /// Tuple of LSN and offset
    async fn write_data(
        buffer: &mut BufWriter<File>,
        payload: &[u8],
        lsn: u64,
        offset: u64,
    ) -> Result<(u64, u64), WalError> {
        let len: u32 = payload.len() as u32;

        // Combine length, payload, and LSN into a single buffer for a more atomic write
        let mut full_payload = Vec::with_capacity(4 + payload.len() + 8);
        full_payload.extend_from_slice(&len.to_le_bytes());
        full_payload.extend_from_slice(payload);
        full_payload.extend_from_slice(&lsn.to_le_bytes());

        buffer
            .write_all(&full_payload)
            .await
            .map_err(|e| WalError::GeneralError(e.to_string()))?;

        buffer
            .flush()
            .await
            .map_err(|e| WalError::GeneralError(e.to_string()))?;

        Ok((lsn, offset + 8 + 4 + len as u64))
    }

    /// Rotate the current WAL segment: flush, sync, close, and open a new segment file.
    ///
    /// This mutates the `WalFile` in-place, replacing the buffer with a new file handle
    /// pointing at the next segment index. The `segment_offset` is reset to 0.
    async fn rotate_segment(
        base_dir: &std::path::Path,
        wal_file: &mut WalFile,
    ) -> Result<(), WalError> {
        // 1. Flush the BufWriter
        wal_file
            .buffer
            .flush()
            .await
            .map_err(|e| WalError::GeneralError(format!("Failed to flush before rotation: {e}")))?;

        // 2. sync_data on the underlying file
        wal_file
            .buffer
            .get_ref()
            .sync_data()
            .await
            .map_err(|e| WalError::GeneralError(format!("Failed to sync before rotation: {e}")))?;

        // 3. Increment segment index and open new file
        let new_index = wal_file.segment_index + 1;

        let base_str = base_dir.to_str().ok_or_else(|| {
            WalError::GeneralError("WAL base directory path is not valid UTF-8".to_string())
        })?;

        let seg_name = segment_filename(wal_file.partition, new_index);
        let new_path = format!("{base_str}/{seg_name}");

        let new_file = WalFile::open_write(&new_path, false).await?;

        info!(
            "WAL rotation: partition {} segment {} -> {}",
            wal_file.partition, wal_file.segment_index, new_index
        );

        // 4. Replace internal state (old file is dropped / closed)
        wal_file.buffer = tokio::io::BufWriter::new(new_file);
        wal_file.segment_index = new_index;
        wal_file.segment_offset = 0;

        Ok(())
    }
}

#[async_trait::async_trait]
impl WalReader for WalLocalFile {
    /// Scan WAL file from a specific LSN and offset
    ///
    /// # Arguments
    /// * `from_lsn` - LSN to start reading from
    /// * `from_offset` - offset to start reading from
    /// * `partition` - partition to read from
    /// * `is_exclusive` - whether to read exclusive of the LSN
    ///
    /// # Returns
    /// Tuple of FramedWalRecord and end offset (used to read next record)
    async fn stream_from(
        &self,
        from_lsn: u64,
        from_offset: u64,
        partition: WalPartitionId,
        is_exclusive: bool,
    ) -> Box<dyn Iterator<Item = Result<(FramedWalRecord, u64), WalError>> + '_> {
        let base_dir_str = match self.base_dir.to_str() {
            Some(s) => s,
            None => {
                return Box::new(std::iter::once(Err(WalError::GeneralError(
                    "WAL base directory path is not valid UTF-8".to_string(),
                ))));
            }
        };

        // Discover all segments for this partition, sorted by index.
        let segments = find_all_segments(&self.base_dir, partition);

        // Determine the starting segment index and local file offset.
        // `from_offset` is a global offset across all segments.  We walk
        // the ordered segment list, accumulating sizes, to find which
        // segment contains `from_offset`.
        let (start_seg_pos, local_offset) = if from_offset == 0 || segments.is_empty() {
            // Start from the very first segment at file position 0.
            (0usize, 0u64)
        } else {
            let mut cumulative: u64 = 0;
            let mut found = None;
            for (i, &(_idx, size)) in segments.iter().enumerate() {
                if cumulative + size > from_offset {
                    found = Some((i, from_offset - cumulative));
                    break;
                }
                cumulative += size;
            }
            // If the offset is exactly at the end of the last segment,
            // position to the next (possibly non-existent yet) segment at 0.
            found.unwrap_or((segments.len(), 0))
        };

        // Build the initial list of segment indices we will iterate through.
        let mut seg_indices: Vec<u64> = segments
            .iter()
            .skip(start_seg_pos)
            .map(|&(idx, _)| idx)
            .collect();

        // If there are no segments at all, create the first segment file so
        // the empty-iterator path still works correctly.
        if seg_indices.is_empty() {
            if let Err(e) = std::fs::create_dir_all(&self.base_dir) {
                return Box::new(std::iter::once(Err(WalError::GeneralError(format!(
                    "Failed to create WAL directory: {e}"
                )))));
            }
            let path = format!("{base_dir_str}/{}", segment_filename(partition, 1));
            if let Err(e) = std::fs::File::create(&path) {
                return Box::new(std::iter::once(Err(WalError::GeneralError(format!(
                    "Failed to create WAL file: {e}"
                )))));
            }
            seg_indices.push(1);
        }

        // Open the first segment file and seek to local_offset.
        let first_idx = seg_indices[0];
        let first_path = format!("{base_dir_str}/{}", segment_filename(partition, first_idx));
        let file = match std::fs::File::open(&first_path) {
            Ok(f) => f,
            Err(e) => {
                return Box::new(std::iter::once(Err(WalError::GeneralError(format!(
                    "Failed to open WAL segment file '{first_path}': {e}"
                )))));
            }
        };

        let mut reader = std::io::BufReader::new(file);
        if let Err(e) = reader.seek(std::io::SeekFrom::Start(local_offset)) {
            error!("Failed to seek WAL file: {e}");
            return Box::new(std::iter::once(Err(WalError::GeneralError(format!(
                "Failed to seek WAL file: {e}"
            )))));
        }

        // State for the chained iterator.
        let mut current_offset = from_offset; // global offset
        let mut seg_cursor = 0usize; // position within seg_indices
        let mut current_seg_index = first_idx;
        let base_dir_owned = self.base_dir.clone();

        let iter = std::iter::from_fn(move || {
            loop {
                // --- Try to read a record from the current reader ---

                // Read the 4-byte payload length header.
                let len = {
                    let mut len_buf = [0u8; 4];
                    let mut bytes_read = 0;
                    while bytes_read < 4 {
                        match reader.read(&mut len_buf[bytes_read..]) {
                            Ok(0) => {
                                if bytes_read == 0 {
                                    // TODO: skip to proper segment based on LSN/offset
                                    // Clean EOF on this segment – try the next one.
                                    seg_cursor += 1;

                                    // Check if the next segment index is known.
                                    let next_index = if seg_cursor < seg_indices.len() {
                                        seg_indices[seg_cursor]
                                    } else {
                                        // Speculatively try index + 1 (the writer
                                        // may have rotated since we scanned).
                                        current_seg_index + 1
                                    };

                                    let next_name = segment_filename(partition, next_index);
                                    let next_path = format!(
                                        "{}/{}",
                                        base_dir_owned.to_str().unwrap_or(""),
                                        next_name,
                                    );

                                    match std::fs::File::open(&next_path) {
                                        Ok(f) => {
                                            reader = std::io::BufReader::new(f);
                                            current_seg_index = next_index;
                                            // Update seg_indices if we discovered a new segment.
                                            if seg_cursor >= seg_indices.len() {
                                                seg_indices.push(next_index);
                                            }
                                            continue; // retry read from new reader
                                        }
                                        Err(_) => return None, // no more segments
                                    }
                                } else {
                                    return Some(Err(WalError::GeneralError(
                                        "Unexpected EOF reading length".to_string(),
                                    )));
                                }
                            }
                            Ok(n) => bytes_read += n,
                            Err(e) => {
                                return Some(Err(WalError::GeneralError(format!(
                                    "error: {e}, for partition: {partition}, offset: {current_offset}, lsn: {from_lsn}",
                                ))));
                            }
                        }
                    }
                    u32::from_le_bytes(len_buf) as usize
                };

                // Read the payload.
                let payload = {
                    let mut payload_buf = vec![0u8; len];
                    if let Err(e) = reader.read_exact(&mut payload_buf) {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            return None; // Partial record at EOF
                        }
                        return Some(Err(WalError::GeneralError(format!(
                            "failed to read entire payload content into buffer, {e}, for partition: {partition}, offset: {current_offset}, current lsn: {from_lsn}, len: {len}",
                        ))));
                    }
                    payload_buf
                };

                // Read the 8-byte LSN footer.
                let lsn = {
                    let mut lsn_buf = [0u8; 8];
                    if let Err(e) = reader.read_exact(&mut lsn_buf) {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            return None; // Partial record at EOF
                        }
                        return Some(Err(WalError::GeneralError(format!(
                            "failed to read entire lsn into buffer, {e}, for partition: {partition}, offset: {current_offset}, current lsn: {from_lsn}",
                        ))));
                    }
                    u64::from_le_bytes(lsn_buf)
                };

                // Verify LSN and offset match (same as before).
                if from_offset > 0 && from_offset == current_offset && (lsn - from_lsn) != 1 {
                    return Some(Err(WalError::LsnOffsetMismatch(format!(
                        "LSN mismatch in WAL file partition: {partition}, from lsn: {from_lsn}, from offset: {from_offset}, current lsn: {lsn}, current offset: {current_offset}",
                    ))));
                }

                let (framed, _) = match bincode::decode_from_slice::<FramedWalRecord, _>(
                    &payload,
                    bincode::config::standard(),
                ) {
                    Ok(result) => result,
                    Err(e) => {
                        return Some(Err(WalError::GeneralError(format!(
                            "Failed to decode WAL record: {e}"
                        ))));
                    }
                };

                let record_len = (8 + 4 + len) as u64;
                current_offset += record_len;

                if (is_exclusive && lsn > from_lsn) || (!is_exclusive && lsn >= from_lsn) {
                    return Some(Ok((framed, current_offset)));
                }
                // else: skip and keep looping
            }
        });
        Box::new(iter)
    }

    async fn get_lsn_watcher(
        &self,
        partition_id: WalPartitionId,
    ) -> tokio::sync::watch::Receiver<u64> {
        // Try getting a read lock first
        {
            let map = self.lsn_watchers.read().await;
            if let Some(sender) = map.get(&partition_id) {
                return sender.subscribe();
            }
        }

        // Only take write lock if we need to insert
        let mut map = self.lsn_watchers.write().await;
        if let Some(sender) = map.get(&partition_id) {
            sender.subscribe()
        } else {
            let (tx, rx) = tokio::sync::watch::channel(0);
            map.insert(partition_id, tx);
            rx
        }
    }
}

#[derive(Error, Debug)]
pub enum WalError {
    #[error("General WAL error: {0}")]
    GeneralError(String),
    #[error("LSN and Offset mismatch: {0}")]
    LsnOffsetMismatch(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_wal() {
        let wal = WalLocalFile::new(PathBuf::from("/tmp/test"), true)
            .await
            .unwrap();

        let record1 = WalRecord::Put {
            partition: WalPartitionId(0),
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            hlc: 0,
        };

        let record2 = WalRecord::Put {
            partition: WalPartitionId(0),
            key: b"key2".to_vec(),
            value: b"value2".to_vec(),
            hlc: 0,
        };

        let (lsn, offset) = wal.append(record1.clone()).await.unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(offset, 26);

        let (lsn, offset) = wal.append(record2.clone()).await.unwrap();
        assert_eq!(lsn, 2);
        assert_eq!(offset, 54);

        let (record, offset) = wal
            .stream_from(1, 0, WalPartitionId(0), false)
            .await
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(record.lsn, 1);
        assert_eq!(record.record, record1);
        assert_eq!(offset, 26);

        let (record, offset) = wal
            .stream_from(1, 0, WalPartitionId(0), true)
            .await
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(record.lsn, 2);
        assert_eq!(record.record, record2);
        assert_eq!(offset, 54);

        // Uses offset instead of fast forward to LSN
        let (record, offset) = wal
            .stream_from(1, 26, WalPartitionId(0), true)
            .await
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(record.lsn, 2);
        assert_eq!(record.record, record2);
        assert_eq!(offset, 54);

        // Unhappy Path: Uses offset instead of fast forward to LSN
        let record = wal.stream_from(1, 25, WalPartitionId(0), true).await.next();

        assert!(record.is_none());

        let record = wal.stream_from(2, 0, WalPartitionId(0), true).await.next();
        assert!(record.is_none());
    }

    #[tokio::test]
    async fn test_read_lsn_empty_file() {
        // Create a temporary empty file
        let temp_path = "/tmp/test_empty_wal.bin";
        let file = tokio::fs::File::create(temp_path).await.unwrap();
        drop(file); // Close the file

        // Open the file for reading
        let mut file = tokio::fs::File::open(temp_path).await.unwrap();

        // Test read_lsn on empty file - should return 0
        let result = WalLocalFile::read_lsn(&mut file).await;

        assert!(result.is_ok(), "read_lsn should succeed on empty file");
        assert_eq!(
            result.unwrap(),
            (0, 0),
            "read_lsn should return 0 on empty file"
        );

        // Clean up
        tokio::fs::remove_file(temp_path).await.ok();
    }
}
