use crate::{
    segment::{find_latest_segment, segment_filename},
    types::{FramedWalRecord, Lsn, WalError, WalPartitionId, WalPosition, WalWriter},
    wal::WalLocalFile,
};
use bincode::{Decode, Encode};
use log::{info, warn};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

#[derive(Debug)]
pub(crate) struct WalFile {
    pub(crate) buffer: BufWriter<File>,
    pub(crate) current_lsn: u64,
    pub(crate) partition: WalPartitionId,
    pub(crate) segment_index: u64,
    /// Bytes written to the current segment file. Also serves as the
    /// local file offset for the next write.
    pub(crate) segment_offset: u64,
}

impl WalFile {
    pub(crate) async fn open_write(
        path: &String,
        truncate_at_start: bool,
    ) -> Result<File, WalError> {
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

#[async_trait::async_trait]
impl<T> WalWriter<T> for WalLocalFile
where
    T: Encode + Decode<()> + Send + Sync + 'static,
{
    async fn append(
        &self,
        partition: WalPartitionId,
        rec: T,
    ) -> Result<(Lsn, WalPosition), WalError> {
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
                    buffer: BufWriter::new(file_write),
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
            record: rec,
        };
        let payload = bincode::encode_to_vec(&framed, bincode::config::standard())
            .map_err(|e| WalError::GeneralError(format!("Failed to encode WAL record: {e}")))?;

        // Check if this record will exceed the segment size
        // Rotate to a new segment if needed
        let record_size = 4 + payload.len() as u64 + 8; // len header + payload + lsn footer

        // segment_offset > 0 prevents infinite rotation loop if single record is greater than max segment size
        if wal_file_write.segment_offset + record_size > self.max_segment_size
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
    pub(crate) async fn read_lsn(file: &mut File) -> Result<(u64, u64), std::io::Error> {
        // Get the file size first
        let file_size = file.seek(std::io::SeekFrom::End(0)).await?;

        // If file is empty or too small to contain an LSN, return 0
        if file_size < 8 {
            return Ok((0, 0));
        }

        // Seek to 8 bytes before the end
        file.seek(std::io::SeekFrom::End(-8)).await?;

        let mut buf = [0u8; 8];
        file.read_exact(&mut buf).await?;

        let lsn = u64::from_le_bytes(buf);
        Ok((lsn, file_size))
    }

    /// Write full record data to WAL file
    pub(crate) async fn write_data(
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
    pub(crate) async fn rotate_segment(
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
        wal_file.buffer = BufWriter::new(new_file);
        wal_file.segment_index = new_index;
        wal_file.segment_offset = 0;

        Ok(())
    }
}
