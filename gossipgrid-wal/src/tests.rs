use std::path::PathBuf;

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::{
    types::{FramedWalRecord, WalError, WalPartitionId, WalPosition, WalReader, WalWriter},
    wal::WalLocalFile,
};

#[derive(Serialize, Deserialize, Encode, Decode, Clone, Debug, PartialEq)]
pub enum TestRecord {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        hlc: u64,
    },
}

#[tokio::test]
async fn test_wal() {
    let wal = WalLocalFile::new(PathBuf::from("/tmp/test"), true, 1024)
        .await
        .unwrap();

    let record1 = TestRecord::Put {
        key: b"key".to_vec(),
        value: b"value".to_vec(),
        hlc: 0,
    };

    let record2 = TestRecord::Put {
        key: b"key2".to_vec(),
        value: b"value2".to_vec(),
        hlc: 0,
    };

    let (lsn, pos) = wal
        .append(WalPartitionId(0), record1.clone())
        .await
        .unwrap();
    assert_eq!(lsn, 1);
    // Size changed because partition id is no longer in the record
    assert_eq!(pos.local_offset, 25);
    assert_eq!(pos.segment_index, 1);

    let (lsn, pos) = wal
        .append(WalPartitionId(0), record2.clone())
        .await
        .unwrap();
    assert_eq!(lsn, 2);
    assert_eq!(pos.local_offset, 52);
    assert_eq!(pos.segment_index, 1);

    // Read from the beginning (non-exclusive)
    let (record, pos): (FramedWalRecord<TestRecord>, WalPosition) = wal
        .stream_from(1, WalPosition::zero(), WalPartitionId(0), false)
        .await
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(record.lsn, 1);
    assert_eq!(record.record, record1);
    assert_eq!(pos.local_offset, 25);

    // Read from the beginning (exclusive — skip LSN 1)
    let (record, pos): (FramedWalRecord<TestRecord>, WalPosition) = wal
        .stream_from(1, WalPosition::zero(), WalPartitionId(0), true)
        .await
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(record.lsn, 2);
    assert_eq!(record.record, record2);
    assert_eq!(pos.local_offset, 52);

    // Resume from a specific position (segment 1, offset 25)
    let (record, pos): (FramedWalRecord<TestRecord>, WalPosition) = wal
        .stream_from(1, WalPosition::new(1, 25), WalPartitionId(0), true)
        .await
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(record.lsn, 2);
    assert_eq!(record.record, record2);
    assert_eq!(pos.local_offset, 52);

    // Unhappy Path: wrong offset — read lands in garbage, should return None
    let record: Option<Result<(FramedWalRecord<TestRecord>, WalPosition), WalError>> = wal
        .stream_from(1, WalPosition::new(1, 24), WalPartitionId(0), true)
        .await
        .next();
    assert!(record.is_none());

    // Exclusive past last record — nothing to read
    let record: Option<Result<(FramedWalRecord<TestRecord>, WalPosition), WalError>> = wal
        .stream_from(2, WalPosition::zero(), WalPartitionId(0), true)
        .await
        .next();
    assert!(record.is_none());
}

#[tokio::test]
async fn test_stream_from_lsn() {
    let wal = WalLocalFile::new(PathBuf::from("/tmp/test_stream_from_lsn"), true, 1024)
        .await
        .unwrap();

    let record1 = TestRecord::Put {
        key: b"key1".to_vec(),
        value: b"val1".to_vec(),
        hlc: 0,
    };
    let record2 = TestRecord::Put {
        key: b"key2".to_vec(),
        value: b"val2".to_vec(),
        hlc: 0,
    };
    let record3 = TestRecord::Put {
        key: b"key3".to_vec(),
        value: b"val3".to_vec(),
        hlc: 0,
    };

    wal.append(WalPartitionId(0), record1.clone())
        .await
        .unwrap();
    wal.append(WalPartitionId(0), record2.clone())
        .await
        .unwrap();
    wal.append(WalPartitionId(0), record3.clone())
        .await
        .unwrap();

    // stream_from_lsn with lsn=0 should read all
    let all: Vec<Result<(FramedWalRecord<TestRecord>, WalPosition), WalError>> = wal
        .stream_from_lsn(0, WalPartitionId(0), false)
        .await
        .collect();
    assert_eq!(all.len(), 3);

    // stream_from_lsn with lsn=2, exclusive should give only record 3
    let from2: Vec<Result<(FramedWalRecord<TestRecord>, WalPosition), WalError>> = wal
        .stream_from_lsn(2, WalPartitionId(0), true)
        .await
        .collect();
    assert_eq!(from2.len(), 1);
    let (rec, _) = from2[0].as_ref().unwrap();
    assert_eq!(rec.lsn, 3);
}
