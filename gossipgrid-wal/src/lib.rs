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

// TODO: not a big fan of this, maybe we can parameterize at call site
#[cfg(not(test))]
/// Maximum segment size in bytes (64 MB).
const SEGMENT_SIZE_MAX: u64 = 64 * 1024 * 1024;
#[cfg(test)]
/// Maximum segment size in bytes (1 KB) for testing.
const SEGMENT_SIZE_MAX: u64 = 1024;

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

/// Find the segment that contains `target_lsn` by reading the last-LSN footer
/// of each segment file. Returns the segment index whose last LSN is >= target_lsn.
/// Used by the LSN-only convenience reader.
fn find_segment_for_lsn(
    base_dir: &std::path::Path,
    partition: WalPartitionId,
    target_lsn: Lsn,
) -> Option<u64> {
    let segments = find_all_segments(base_dir, partition);
    if segments.is_empty() {
        return None;
    }

    for &(idx, size) in &segments {
        if let Ok(last_lsn) = read_segment_last_lsn(base_dir, partition, idx, size) {
            if last_lsn >= target_lsn {
                return Some(idx);
            }
        }
    }

    // target_lsn is beyond all segments — return the last one so
    // the reader can attempt to read from it (will get EOF).
    segments.last().map(|(idx, _)| *idx)
}

/// Helper to read the last LSN from a segment file footer.
fn read_segment_last_lsn(
    base_dir: &std::path::Path,
    partition: WalPartitionId,
    idx: u64,
    size: u64,
) -> Result<u64, std::io::Error> {
    if size < 8 {
        return Ok(0); // Empty or too-small segment
    }
    let path = base_dir.join(segment_filename(partition, idx));
    let mut file = std::fs::File::open(&path)?;
    file.seek(SeekFrom::End(-8))?;
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
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

/// A position within the segmented WAL, combining segment identity with
/// a byte offset local to that segment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub struct WalPosition {
    pub segment_index: u64,
    pub local_offset: u64,
}

impl WalPosition {
    pub fn new(segment_index: u64, local_offset: u64) -> Self {
        Self {
            segment_index,
            local_offset,
        }
    }

    pub fn zero() -> Self {
        Self {
            segment_index: 0,
            local_offset: 0,
        }
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
    /// Read records starting from a specific segment position.
    /// This is the precise, segment-aware reader used by the sync protocol.
    async fn stream_from(
        &self,
        from_lsn: Lsn,
        from_position: WalPosition,
        partition: WalPartitionId,
        is_exclusive: bool,
    ) -> Box<dyn Iterator<Item = Result<(FramedWalRecord, WalPosition), WalError>> + '_>;

    /// Read records starting from a given LSN.
    /// Convenience method that locates the correct segment automatically
    /// by scanning segment LSN footers. Slightly slower on first call
    /// but does not require the caller to track segment positions.
    async fn stream_from_lsn(
        &self,
        from_lsn: Lsn,
        partition: WalPartitionId,
        is_exclusive: bool,
    ) -> Box<dyn Iterator<Item = Result<(FramedWalRecord, WalPosition), WalError>> + '_>;

    async fn get_lsn_watcher(
        &self,
        partition_id: WalPartitionId,
    ) -> tokio::sync::watch::Receiver<u64>;
}

#[async_trait::async_trait]
pub trait WalWriter: Send + Sync {
    async fn append(&self, rec: WalRecord) -> Result<(Lsn, WalPosition), WalError>;
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
    partition: WalPartitionId,
    segment_index: u64,
    /// Bytes written to the current segment file. Also serves as the
    /// local file offset for the next write.
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

    async fn append(&self, rec: WalRecord) -> Result<(Lsn, WalPosition), WalError> {
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

                // Open the file first to get latest LSN
                let lsn = if self.truncate_at_start {
                    0
                } else {
                    match tokio::fs::File::open(&writer_path).await {
                        Ok(mut file) => WalLocalFile::read_lsn(&mut file)
                            .await
                            .map(|(lsn, _)| lsn)
                            .unwrap_or(0),
                        Err(e) => {
                            warn!(
                                "Failed to open WAL file, assuming fresh cluster, using lsn 0 : {e}"
                            );
                            0
                        }
                    }
                };

                let file_write = WalFile::open_write(&writer_path, self.truncate_at_start).await?;

                let wal_file = WalFile {
                    buffer: tokio::io::BufWriter::new(file_write),
                    current_lsn: lsn,
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

        let segment_offset_before = wal_file_write.segment_offset;
        WalLocalFile::write_data(&mut wal_file_write.buffer, &payload, new_lsn).await?;

        wal_file_write.segment_offset += record_size;
        wal_file_write.current_lsn = new_lsn;

        let position = WalPosition {
            segment_index: wal_file_write.segment_index,
            local_offset: segment_offset_before + record_size,
        };

        // Also update watchers
        {
            let map = self.lsn_watchers.read().await;
            if let Some(sender) = map.get(&partition) {
                let _ = sender.send(new_lsn);
            }
        }

        Ok((new_lsn, position))
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
    async fn write_data(
        buffer: &mut BufWriter<File>,
        payload: &[u8],
        lsn: u64,
    ) -> Result<(), WalError> {
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

        Ok(())
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

    /// Delete old WAL segments for a partition whose max LSN is less than `min_lsn`.
    /// The currently active segment is NEVER deleted.
    pub async fn purge_before(
        &self,
        partition: WalPartitionId,
        min_lsn: Lsn,
    ) -> Result<u32, WalError> {
        let active_idx = if let Some(lock) = self.write_handles.read().await.get(&partition) {
            lock.lock().await.segment_index
        } else {
            0
        };
        let segments = find_all_segments(&self.base_dir, partition);
        let mut deleted_count = 0;

        for (idx, size) in segments {
            // Never delete the active segment
            if idx == active_idx {
                continue;
            }

            // Check if this segment's max LSN is below min_lsn
            match read_segment_last_lsn(&self.base_dir, partition, idx, size) {
                Ok(last_lsn) => {
                    if last_lsn < min_lsn {
                        let path = self.base_dir.join(segment_filename(partition, idx));
                        if let Err(e) = std::fs::remove_file(&path) {
                            warn!("Failed to delete WAL segment {path:?}: {e}");
                        } else {
                            info!("Purged WAL segment: {path:?}");
                            deleted_count += 1;
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to read LSN from segment {idx} during purge: {e}");
                }
            }
        }

        Ok(deleted_count)
    }
}

#[async_trait::async_trait]
impl WalReader for WalLocalFile {
    /// Scan WAL records starting from a specific segment position.
    ///
    /// # Arguments
    /// * `from_lsn` - LSN used for validation and inclusive/exclusive filtering
    /// * `from_position` - segment index and local byte offset to seek to
    /// * `partition` - partition to read from
    /// * `is_exclusive` - whether to read exclusive of the LSN
    ///
    /// # Returns
    /// Iterator yielding `(FramedWalRecord, WalPosition)` where `WalPosition`
    /// is the position *after* this record (can be passed to stream_from for
    /// the next read).
    async fn stream_from(
        &self,
        from_lsn: u64,
        from_position: WalPosition,
        partition: WalPartitionId,
        is_exclusive: bool,
    ) -> Box<dyn Iterator<Item = Result<(FramedWalRecord, WalPosition), WalError>> + '_> {
        let base_dir_str = match self.base_dir.to_str() {
            Some(s) => s,
            None => {
                return Box::new(std::iter::once(Err(WalError::GeneralError(
                    "WAL base directory path is not valid UTF-8".to_string(),
                ))));
            }
        };

        // Determine the starting segment index and local offset.
        let (start_seg_index, local_offset) = if from_position.segment_index == 0 {
            // segment_index 0 means "start from the first available segment"
            let segments = find_all_segments(&self.base_dir, partition);
            if segments.is_empty() {
                // No segments exist yet — ensure the first segment file is created.
                if let Err(e) = std::fs::create_dir_all(&self.base_dir) {
                    return Box::new(std::iter::once(Err(WalError::GeneralError(format!(
                        "Failed to create WAL directory: {e}"
                    )))));
                }
                let path = format!("{base_dir_str}/{}", segment_filename(partition, 1));
                if let Err(e) = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .open(&path)
                {
                    return Box::new(std::iter::once(Err(WalError::GeneralError(format!(
                        "Failed to create WAL file: {e}"
                    )))));
                }
                (1u64, 0u64)
            } else {
                (segments[0].0, 0u64)
            }
        } else {
            (from_position.segment_index, from_position.local_offset)
        };

        // Open the starting segment file and seek to local_offset.
        let first_path = format!(
            "{base_dir_str}/{}",
            segment_filename(partition, start_seg_index)
        );
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
        let mut current_local_offset = local_offset;
        let mut current_seg_index = start_seg_index;
        let is_first_record = from_position.local_offset > 0;
        let mut first_record_checked = false;
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
                                    // Clean EOF on this segment – try the next one.
                                    let next_index = current_seg_index + 1;

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
                                            current_local_offset = 0;
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
                                    "error: {e}, for partition: {partition}, segment: {current_seg_index}, offset: {current_local_offset}, lsn: {from_lsn}",
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
                            "failed to read entire payload content into buffer, {e}, for partition: {partition}, segment: {current_seg_index}, offset: {current_local_offset}, current lsn: {from_lsn}, len: {len}",
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
                            "failed to read entire lsn into buffer, {e}, for partition: {partition}, segment: {current_seg_index}, offset: {current_local_offset}, current lsn: {from_lsn}",
                        ))));
                    }
                    u64::from_le_bytes(lsn_buf)
                };

                // Verify LSN continuity on the first record when resuming from a position.
                if is_first_record && !first_record_checked {
                    first_record_checked = true;
                    if lsn != from_lsn + 1 {
                        return Some(Err(WalError::LsnOffsetMismatch(format!(
                            "LSN mismatch in WAL file partition: {partition}, from lsn: {from_lsn}, segment: {}, offset: {}, current lsn: {lsn}",
                            from_position.segment_index, from_position.local_offset,
                        ))));
                    }
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
                current_local_offset += record_len;

                let position = WalPosition {
                    segment_index: current_seg_index,
                    local_offset: current_local_offset,
                };

                if (is_exclusive && lsn > from_lsn) || (!is_exclusive && lsn >= from_lsn) {
                    return Some(Ok((framed, position)));
                }
                // else: skip and keep looping
            }
        });
        Box::new(iter)
    }

    /// Convenience method: read records starting from a given LSN.
    /// Scans segment LSN footers to find the correct starting segment.
    async fn stream_from_lsn(
        &self,
        from_lsn: u64,
        partition: WalPartitionId,
        is_exclusive: bool,
    ) -> Box<dyn Iterator<Item = Result<(FramedWalRecord, WalPosition), WalError>> + '_> {
        let position = if from_lsn == 0 {
            WalPosition::zero()
        } else {
            match find_segment_for_lsn(&self.base_dir, partition, from_lsn) {
                Some(seg_idx) => WalPosition::new(seg_idx, 0),
                None => WalPosition::zero(),
            }
        };

        self.stream_from(from_lsn, position, partition, is_exclusive)
            .await
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

        let (lsn, pos) = wal.append(record1.clone()).await.unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(pos.local_offset, 26);
        assert_eq!(pos.segment_index, 1);

        let (lsn, pos) = wal.append(record2.clone()).await.unwrap();
        assert_eq!(lsn, 2);
        assert_eq!(pos.local_offset, 54);
        assert_eq!(pos.segment_index, 1);

        // Read from the beginning (non-exclusive)
        let (record, pos) = wal
            .stream_from(1, WalPosition::zero(), WalPartitionId(0), false)
            .await
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(record.lsn, 1);
        assert_eq!(record.record, record1);
        assert_eq!(pos.local_offset, 26);

        // Read from the beginning (exclusive — skip LSN 1)
        let (record, pos) = wal
            .stream_from(1, WalPosition::zero(), WalPartitionId(0), true)
            .await
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(record.lsn, 2);
        assert_eq!(record.record, record2);
        assert_eq!(pos.local_offset, 54);

        // Resume from a specific position (segment 1, offset 26)
        let (record, pos) = wal
            .stream_from(1, WalPosition::new(1, 26), WalPartitionId(0), true)
            .await
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(record.lsn, 2);
        assert_eq!(record.record, record2);
        assert_eq!(pos.local_offset, 54);

        // Unhappy Path: wrong offset — read lands in garbage, should return None
        let record = wal
            .stream_from(1, WalPosition::new(1, 25), WalPartitionId(0), true)
            .await
            .next();
        assert!(record.is_none());

        // Exclusive past last record — nothing to read
        let record = wal
            .stream_from(2, WalPosition::zero(), WalPartitionId(0), true)
            .await
            .next();
        assert!(record.is_none());
    }

    #[tokio::test]
    async fn test_stream_from_lsn() {
        let wal = WalLocalFile::new(PathBuf::from("/tmp/test_stream_from_lsn"), true)
            .await
            .unwrap();

        let record1 = WalRecord::Put {
            partition: WalPartitionId(0),
            key: b"key1".to_vec(),
            value: b"val1".to_vec(),
            hlc: 0,
        };
        let record2 = WalRecord::Put {
            partition: WalPartitionId(0),
            key: b"key2".to_vec(),
            value: b"val2".to_vec(),
            hlc: 0,
        };
        let record3 = WalRecord::Put {
            partition: WalPartitionId(0),
            key: b"key3".to_vec(),
            value: b"val3".to_vec(),
            hlc: 0,
        };

        wal.append(record1.clone()).await.unwrap();
        wal.append(record2.clone()).await.unwrap();
        wal.append(record3.clone()).await.unwrap();

        // stream_from_lsn with lsn=0 should read all
        let all: Vec<_> = wal
            .stream_from_lsn(0, WalPartitionId(0), false)
            .await
            .collect::<Vec<_>>();
        assert_eq!(all.len(), 3);

        // stream_from_lsn with lsn=2, exclusive should give only record 3
        let from2: Vec<_> = wal
            .stream_from_lsn(2, WalPartitionId(0), true)
            .await
            .collect::<Vec<_>>();
        assert_eq!(from2.len(), 1);
        let (rec, _) = from2[0].as_ref().unwrap();
        assert_eq!(rec.lsn, 3);
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

    #[tokio::test]
    async fn test_wal_purge() {
        let test_dir = PathBuf::from("/tmp/test_wal_purge");
        let wal = WalLocalFile::new(test_dir.clone(), true).await.unwrap();
        let partition = WalPartitionId(0);

        let record = WalRecord::Put {
            partition,
            key: vec![0; 10],
            value: vec![0; 10],
            hlc: 0,
        };

        wal.append(record.clone()).await.unwrap(); // Segment 1
        assert!(test_dir.join("part_0_0000000001.wal").exists());

        // Since we only have one segment, and it's active, it should NOT be deleted.
        let deleted = wal.purge_before(partition, 10).await.unwrap();
        assert_eq!(deleted, 0);
        assert!(test_dir.join("part_0_0000000001.wal").exists());

        // Manually trigger a rotation to ensure Segment 1 is closed and Segment 2 is active.
        {
            let mut handles = wal.write_handles.write().await;
            let lock = handles.get_mut(&partition).unwrap();
            let mut wal_file = lock.lock().await;
            WalLocalFile::rotate_segment(&test_dir, &mut wal_file)
                .await
                .unwrap();
        }
        // Now Segment 2 is active. Append a record to it.
        wal.append(record.clone()).await.unwrap(); // LSN 2

        assert!(test_dir.join("part_0_0000000001.wal").exists());
        assert!(test_dir.join("part_0_0000000002.wal").exists());

        // Purge. Segment 1 has max LSN 1. 1 < 10, so it should be gone.
        let deleted = wal.purge_before(partition, 10).await.unwrap();
        assert_eq!(deleted, 1);
        assert!(!test_dir.join("part_0_0000000001.wal").exists());
    }

    #[tokio::test]
    async fn test_wal_rotation() {
        let test_dir = PathBuf::from("/tmp/test_wal_rotation");
        let wal = WalLocalFile::new(test_dir.clone(), true).await.unwrap();
        let partition = WalPartitionId(0);

        let record = WalRecord::Put {
            partition,
            key: vec![0; 100],
            value: vec![0; 100],
            hlc: 0,
        };

        // Write ~6 records to exceed 1KB (each record is > 200 bytes)
        for _ in 0..6 {
            wal.append(record.clone()).await.unwrap();
        }

        assert!(test_dir.join("part_0_0000000001.wal").exists());
        assert!(test_dir.join("part_0_0000000002.wal").exists());
    }

    #[tokio::test]
    async fn test_wal_chained_reading() {
        let test_dir = PathBuf::from("/tmp/test_wal_chained_reading");
        let wal = WalLocalFile::new(test_dir.clone(), true).await.unwrap();
        let partition = WalPartitionId(0);

        let record = WalRecord::Put {
            partition,
            key: vec![0; 100],
            value: vec![0; 100],
            hlc: 0,
        };

        // Write 10 records to span multiple segments
        for i in 0..10 {
            let mut rec = record.clone();
            if let WalRecord::Put { hlc, .. } = &mut rec {
                *hlc = i as u64;
            }
            wal.append(rec).await.unwrap();
        }

        // Stream all records from LSN 0
        let all: Vec<_> = wal.stream_from_lsn(0, partition, false).await.collect();
        assert_eq!(all.len(), 10);

        // Verify sequence
        for (i, res) in all.into_iter().enumerate() {
            let (framed, _) = res.unwrap();
            assert_eq!(framed.lsn, (i + 1) as u64);
            if let WalRecord::Put { hlc, .. } = framed.record {
                assert_eq!(hlc, i as u64);
            }
        }
    }

    #[tokio::test]
    async fn test_wal_recovery() {
        let test_dir = PathBuf::from("/tmp/test_wal_recovery");
        let partition = WalPartitionId(0);
        let record = WalRecord::Put {
            partition,
            key: b"data".to_vec(),
            value: b"value".to_vec(),
            hlc: 123,
        };

        {
            let wal = WalLocalFile::new(test_dir.clone(), true).await.unwrap();
            wal.append(record.clone()).await.unwrap();
            wal.append(record.clone()).await.unwrap();
            // Ensure data is synced
            wal.io_sync().await.unwrap();
        }

        // Simulate restart
        {
            let wal = WalLocalFile::new(test_dir.clone(), false).await.unwrap();
            let (lsn, pos) = wal.append(record.clone()).await.unwrap();

            // Should have resumed after LSN 2
            assert_eq!(lsn, 3);
            assert_eq!(pos.segment_index, 1);

            let all: Vec<_> = wal.stream_from_lsn(0, partition, false).await.collect();
            assert_eq!(all.len(), 3);
        }
    }
}
