use crate::types::PartitionId;
use thiserror::Error;

/// Errors that can occur during client operations.
#[derive(Error, Debug)]
pub enum ClientError {
    /// The partition leader is unavailable (down or not yet elected).
    /// Writes and deletes cannot proceed until a new leader is elected.
    #[error("Leader unavailable for partition {partition_id}")]
    LeaderUnavailable { partition_id: PartitionId },

    /// All seed/known nodes are unreachable.
    #[error("No healthy nodes available")]
    NoHealthyNodes,

    /// A specific node connection failed.
    #[error("Connection failed to {address}: {source}")]
    ConnectionFailed {
        address: String,
        #[source]
        source: reqwest::Error,
    },

    /// The server returned an HTTP error.
    #[error("Server error (status {status}): {message}")]
    ServerError { status: u16, message: String },
}
