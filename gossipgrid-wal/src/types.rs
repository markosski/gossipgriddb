use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type Lsn = u64;

#[derive(Error, Debug)]
pub enum WalError {
    #[error("General WAL error: {0}")]
    GeneralError(String),
    #[error("LSN and Offset mismatch: {0}")]
    LsnOffsetMismatch(String),
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

#[derive(Serialize, Deserialize, Encode, Decode, Debug)]
pub struct FramedWalRecord<T> {
    pub lsn: Lsn,
    pub record: T,
}

#[async_trait::async_trait]
pub trait Wal<T>: WalReader<T> + WalWriter<T> + Send + Sync {}
impl<T, W> Wal<T> for W where W: WalReader<T> + WalWriter<T> + Send + Sync {}

#[async_trait::async_trait]
pub trait WalCommon: Send + Sync {
    async fn io_sync(&self) -> Result<(), WalError>;
    async fn get_lsn_watcher(
        &self,
        partition_id: WalPartitionId,
    ) -> tokio::sync::watch::Receiver<u64>;
}

#[async_trait::async_trait]
pub trait WalReader<T>: WalCommon + Send + Sync {
    /// Read records starting from a specific segment position.
    /// This is the precise, segment-aware reader used by the sync protocol.
    async fn stream_from(
        &self,
        from_lsn: Lsn,
        from_position: WalPosition,
        partition: WalPartitionId,
        is_exclusive: bool,
    ) -> Box<dyn Iterator<Item = Result<(FramedWalRecord<T>, WalPosition), WalError>> + '_>;

    /// Read records starting from a given LSN.
    /// Convenience method that locates the correct segment automatically
    /// by scanning segment LSN footers. Slightly slower on first call
    /// but does not require the caller to track segment positions.
    async fn stream_from_lsn(
        &self,
        from_lsn: Lsn,
        partition: WalPartitionId,
        is_exclusive: bool,
    ) -> Box<dyn Iterator<Item = Result<(FramedWalRecord<T>, WalPosition), WalError>> + '_>;
}

#[async_trait::async_trait]
pub trait WalWriter<T>: WalCommon + Send + Sync {
    async fn append(
        &self,
        partition: WalPartitionId,
        data: T,
    ) -> Result<(Lsn, WalPosition), WalError>;
}
