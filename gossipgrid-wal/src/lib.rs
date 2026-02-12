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
                let writer_path = format!(
                    "{}/part_{}.wal",
                    self.base_dir
                        .to_str()
                        .ok_or_else(|| WalError::GeneralError(
                            "WAL base directory path is not valid UTF-8".to_string()
                        ))?,
                    partition
                );
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

        let (lsn, offset) = WalLocalFile::write_data(
            &mut wal_file_write.buffer,
            &payload,
            new_lsn,
            current_offset,
        )
        .await?;

        wal_file_write.current_offset = offset;
        wal_file_write.current_lsn = lsn;

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
        let path = format!("{base_dir_str}/part_{partition}.wal");

        let file = if let Ok(file) = std::fs::File::open(&path) {
            file
        } else {
            // make sure directory is there
            if let Err(e) = std::fs::create_dir_all(&self.base_dir) {
                return Box::new(std::iter::once(Err(WalError::GeneralError(format!(
                    "Failed to create WAL directory: {e}"
                )))));
            }

            match std::fs::File::create(&path) {
                Ok(_) => match std::fs::File::open(&path) {
                    Ok(f) => f,
                    Err(e) => {
                        return Box::new(std::iter::once(Err(WalError::GeneralError(format!(
                            "Failed to reopen WAL file after creation: {e}"
                        )))));
                    }
                },
                Err(e) => {
                    return Box::new(std::iter::once(Err(WalError::GeneralError(format!(
                        "Failed to create WAL file: {e}"
                    )))));
                }
            }
        };

        let mut reader = std::io::BufReader::new(file);

        if let Err(e) = reader.seek(std::io::SeekFrom::Start(from_offset)) {
            error!("Failed to seek WAL file: {e}");
            return Box::new(std::iter::once(Err(WalError::GeneralError(format!(
                "Failed to seek WAL file: {e}"
            )))));
        }

        let mut current_offset = from_offset;
        let iter = std::iter::from_fn(move || {
            loop {
                // Read payload lengths
                let len = {
                    let mut len_buf = [0u8; 4];
                    let mut bytes_read = 0;
                    while bytes_read < 4 {
                        match reader.read(&mut len_buf[bytes_read..]) {
                            Ok(0) => {
                                if bytes_read == 0 {
                                    return None; // Clean EOF
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

                let payload = {
                    let mut payload_buf = vec![0u8; len];
                    if let Err(e) = reader.read_exact(&mut payload_buf) {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            return None; // Partial record at EOF, treat as "not yet ready"
                        }
                        return Some(Err(WalError::GeneralError(format!(
                            "failed to read entire payload content into buffer, {e}, for partition: {partition}, offset: {current_offset}, current lsn: {from_lsn}, len: {len}",
                        ))));
                    }
                    payload_buf
                };

                let lsn = {
                    let mut lsn_buf = [0u8; 8];
                    if let Err(e) = reader.read_exact(&mut lsn_buf) {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            return None; // Partial record at EOF, treat as "not yet ready"
                        }
                        return Some(Err(WalError::GeneralError(format!(
                            "failed to read entire lsn into buffer, {e}, for partition: {partition}, offset: {current_offset}, current lsn: {from_lsn}",
                        ))));
                    }

                    u64::from_le_bytes(lsn_buf)
                };

                // verify LSN and offset match
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
