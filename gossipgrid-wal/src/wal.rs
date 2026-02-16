use crate::{
    segment::{find_all_segments, read_segment_last_lsn, segment_filename},
    types::{Lsn, WalCommon, WalError, WalPartitionId},
    writer::*,
};
use log::{error, info, warn};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::Mutex;
use tokio::{sync::RwLock, sync::watch};

pub struct WalLocalFile {
    pub(crate) base_dir: PathBuf,
    pub(crate) truncate_at_start: bool,
    pub(crate) max_segment_size: u64,
    pub(crate) write_handles: RwLock<HashMap<WalPartitionId, Arc<Mutex<WalFile>>>>,
    pub(crate) lsn_watchers: RwLock<HashMap<WalPartitionId, watch::Sender<u64>>>,
}

impl WalLocalFile {
    pub async fn new(
        base_dir: PathBuf,
        truncate: bool,
        max_segment_size: u64,
    ) -> Result<Self, WalError> {
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
            max_segment_size,
            write_handles: RwLock::new(HashMap::new()),
            lsn_watchers: RwLock::new(HashMap::new()),
        })
    }
}

#[async_trait::async_trait]
impl WalCommon for WalLocalFile {
    async fn io_sync(&self) -> Result<(), WalError> {
        let handles = self.write_handles.read().await;
        for (_, fd) in handles.iter() {
            let fd = fd.lock().await;
            let _ = fd.buffer.get_ref().sync_data().await;
        }
        Ok(())
    }

    async fn get_lsn_watcher(&self, partition_id: WalPartitionId) -> watch::Receiver<u64> {
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
            let (tx, rx) = watch::channel(0);
            map.insert(partition_id, tx);
            rx
        }
    }
}

impl WalLocalFile {
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
#[cfg(test)]
mod tests {
    use crate::tests::TestRecord;
    use crate::types::{FramedWalRecord, WalPosition};
    use crate::{WalReader, WalWriter};

    use super::*;

    const TEST_SEGMENT_SIZE: u64 = 1024;

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
        let wal = WalLocalFile::new(test_dir.clone(), true, TEST_SEGMENT_SIZE)
            .await
            .unwrap();
        let partition = WalPartitionId(0);

        let record = TestRecord::Put {
            key: vec![0; 10],
            value: vec![0; 10],
            hlc: 0,
        };

        wal.append(partition, record.clone()).await.unwrap(); // Segment 1
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
        wal.append(partition, record.clone()).await.unwrap(); // LSN 2

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
        let wal = WalLocalFile::new(test_dir.clone(), true, TEST_SEGMENT_SIZE)
            .await
            .unwrap();
        let partition = WalPartitionId(0);

        let record = TestRecord::Put {
            key: vec![0; 100],
            value: vec![0; 100],
            hlc: 0,
        };

        // Write ~6 records to exceed 1KB (each record is > 200 bytes)
        for _ in 0..6 {
            wal.append(partition, record.clone()).await.unwrap();
        }

        assert!(test_dir.join("part_0_0000000001.wal").exists());
        assert!(test_dir.join("part_0_0000000002.wal").exists());
    }

    #[tokio::test]
    async fn test_wal_chained_reading() {
        let test_dir = PathBuf::from("/tmp/test_wal_chained_reading");
        let wal = WalLocalFile::new(test_dir.clone(), true, TEST_SEGMENT_SIZE)
            .await
            .unwrap();
        let partition = WalPartitionId(0);

        let record = TestRecord::Put {
            key: vec![0; 100],
            value: vec![0; 100],
            hlc: 0,
        };

        // Write 10 records to span multiple segments
        for i in 0..10 {
            let mut rec = record.clone();
            let TestRecord::Put { hlc, .. } = &mut rec;
            *hlc = i as u64;
            wal.append(partition, rec).await.unwrap();
        }

        // Stream all records from LSN 0
        let all: Vec<Result<(FramedWalRecord<TestRecord>, WalPosition), WalError>> =
            wal.stream_from_lsn(0, partition, false).await.collect();
        assert_eq!(all.len(), 10);

        // Verify sequence
        for (i, res) in all.into_iter().enumerate() {
            let (framed, _) = res.unwrap();
            assert_eq!(framed.lsn, (i + 1) as u64);
            let TestRecord::Put { hlc, .. } = framed.record;
            assert_eq!(hlc, i as u64);
        }
    }

    #[tokio::test]
    async fn test_wal_recovery() {
        let test_dir = PathBuf::from("/tmp/test_wal_recovery");
        let partition = WalPartitionId(0);
        let record = TestRecord::Put {
            key: b"data".to_vec(),
            value: b"value".to_vec(),
            hlc: 123,
        };

        {
            let wal = WalLocalFile::new(test_dir.clone(), true, TEST_SEGMENT_SIZE)
                .await
                .unwrap();
            wal.append(partition, record.clone()).await.unwrap();
            wal.append(partition, record.clone()).await.unwrap();
            // Ensure data is synced
            wal.io_sync().await.unwrap();
        }

        // Simulate restart
        {
            let wal = WalLocalFile::new(test_dir.clone(), false, TEST_SEGMENT_SIZE)
                .await
                .unwrap();
            let (lsn, pos) = wal.append(partition, record.clone()).await.unwrap();

            // Should have resumed after LSN 2
            assert_eq!(lsn, 3);
            assert_eq!(pos.segment_index, 1);

            let all: Vec<Result<(FramedWalRecord<TestRecord>, WalPosition), WalError>> =
                wal.stream_from_lsn(0, partition, false).await.collect();
            assert_eq!(all.len(), 3);
        }
    }
}
