use crate::{
    segment::{find_all_segments, find_segment_for_lsn, segment_filename},
    types::{FramedWalRecord, WalError, WalPartitionId, WalPosition, WalReader},
    wal::WalLocalFile,
};
use bincode::{Decode, Encode};
use log::{debug, error};
use std::io::{Read, Seek};

#[async_trait::async_trait]
impl<T> WalReader<T> for WalLocalFile
where
    T: Encode + Decode<()> + Send + Sync + 'static,
{
    async fn stream_from(
        &self,
        from_lsn: u64,
        from_position: WalPosition,
        partition: WalPartitionId,
        is_exclusive: bool,
    ) -> Box<dyn Iterator<Item = Result<(FramedWalRecord<T>, WalPosition), WalError>> + '_> {
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
                    // If we are exclusive and we read the record we thought we finished, just skip it.
                    // This can happen if offsets were reported slightly differently between nodes.
                    if is_exclusive && lsn == from_lsn {
                        debug!(
                            "Redundant LSN {lsn} found at offset {current_local_offset} during exclusive stream for partition {partition}"
                        );
                        // Continue loop to read next record
                    } else if lsn != from_lsn + 1 {
                        return Some(Err(WalError::LsnOffsetMismatch(format!(
                            "LSN mismatch in WAL file partition: {partition}, from lsn: {from_lsn}, segment: {}, offset: {}, current lsn: {lsn}",
                            from_position.segment_index, from_position.local_offset,
                        ))));
                    }
                }

                let (framed, _) = match bincode::decode_from_slice::<FramedWalRecord<T>, _>(
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

    async fn stream_from_lsn(
        &self,
        from_lsn: u64,
        partition: WalPartitionId,
        is_exclusive: bool,
    ) -> Box<dyn Iterator<Item = Result<(FramedWalRecord<T>, WalPosition), WalError>> + '_> {
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
}
