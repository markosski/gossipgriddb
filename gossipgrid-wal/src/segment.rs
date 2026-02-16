use std::io::SeekFrom;
use std::io::{Read, Seek};

use crate::types::{Lsn, WalPartitionId};

/// Generate a segment filename, e.g. `part_1_0000000001.wal`
pub fn segment_filename(partition: WalPartitionId, segment_index: u64) -> String {
    format!("part_{partition}_{segment_index:010}.wal")
}

/// Scan `base_dir` for segment files belonging to `partition` and return
/// `(highest_segment_index, file_size_in_bytes)`.
///
/// Segment files follow the pattern `part_{partition}_{index:010}.wal`.
/// If no matching files are found the function returns `(1, 0)` so the
/// first write creates segment index 1.
pub fn find_latest_segment(base_dir: &std::path::Path, partition: WalPartitionId) -> (u64, u64) {
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
pub fn find_all_segments(base_dir: &std::path::Path, partition: WalPartitionId) -> Vec<(u64, u64)> {
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
pub fn find_segment_for_lsn(
    base_dir: &std::path::Path,
    partition: WalPartitionId,
    target_lsn: Lsn,
) -> Option<u64> {
    let segments = find_all_segments(base_dir, partition);
    if segments.is_empty() {
        return None;
    }

    for &(idx, size) in &segments {
        if let Ok(last_lsn) = read_segment_last_lsn(base_dir, partition, idx, size)
            && last_lsn >= target_lsn
        {
            return Some(idx);
        }
    }

    // target_lsn is beyond all segments — return the last one so
    // the reader can attempt to read from it (will get EOF).
    segments.last().map(|(idx, _)| *idx)
}

/// Helper to read the last LSN from a segment file footer.
pub fn read_segment_last_lsn(
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
