use serde::{Deserialize, Serialize};
use std::fmt;

/// A partition identifier, wrapping a `u16`.
///
/// Must match the server's `gossipgrid::cluster::PartitionId` semantically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartitionId(pub u16);

impl PartitionId {
    pub fn value(self) -> u16 {
        self.0
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u16> for PartitionId {
    fn from(value: u16) -> Self {
        PartitionId(value)
    }
}

/// A parsed node address consisting of an IP and port.
///
/// Mirrors the server's `gossipgrid::node::NodeAddress` without the `bincode` dependency.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeAddress {
    pub ip: String,
    pub port: u16,
}

impl NodeAddress {
    pub fn new(ip: String, port: u16) -> Self {
        NodeAddress { ip, port }
    }

    /// Parse from a "ip:port" string. Returns None if the format is invalid.
    pub fn parse(addr: &str) -> Option<Self> {
        let parts: Vec<&str> = addr.split(':').collect();
        if parts.len() == 2 {
            let port = parts[1].parse::<u16>().ok()?;
            Some(NodeAddress::new(parts[0].to_string(), port))
        } else {
            None
        }
    }

    /// Format as "ip:port".
    pub fn as_str(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }

    /// Build an HTTP base URL using this address's IP with a different port (the web port).
    pub fn http_base_url(&self, web_port: u16) -> String {
        format!("http://{}:{}", self.ip, web_port)
    }
}

impl fmt::Display for NodeAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.ip, self.port)
    }
}

/// A request to create or update an item.
///
/// Mirrors the server's `ItemCreateUpdate` struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemCreateUpdate {
    pub partition_key: String,
    pub range_key: Option<String>,
    pub message: String,
}

/// Error details for a single item in a batch operation.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BatchItemError {
    pub partition_key: String,
    pub range_key: Option<String>,
    pub reason: String,
}

/// Response envelope for batch write operations.
///
/// Contains the count of successfully inserted items and any per-item errors.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ItemBatchResponseEnvelope {
    pub inserted: usize,
    pub errors: Option<Vec<BatchItemError>>,
}
