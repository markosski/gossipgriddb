use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};
use thiserror::Error;

/// Error type for parsing a `NodeAddress` from a string.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum NodeAddressParseError {
    #[error("Invalid node address format (expected 'ip:port'): {0}")]
    InvalidFormat(String),
    #[error("Invalid port number in address '{0}': {1}")]
    InvalidPort(String, String),
}

#[derive(Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NodeAddress {
    pub ip: String,
    pub port: u16,
    pub ip_and_port: String,
}

impl NodeAddress {
    pub fn new(ip: String, port: u16) -> Self {
        NodeAddress {
            ip: ip.clone(),
            port,
            ip_and_port: format!("{ip}:{port}"),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.ip_and_port
    }

    /// Parse a node address from a string, panicking on invalid input.
    ///
    /// This is a convenience method primarily for tests and internal use.
    /// For fallible parsing, use `.parse::<NodeAddress>()` or `NodeAddress::try_from()`.
    pub fn parse_unchecked(addr: &str) -> Self {
        addr.parse()
            .unwrap_or_else(|e| panic!("Invalid node address '{addr}': {e}"))
    }
}

// Implement from trait for NodeAddress to convert from SocketAddr
impl From<&std::net::SocketAddr> for NodeAddress {
    fn from(addr: &std::net::SocketAddr) -> Self {
        NodeAddress::new(addr.ip().to_string(), addr.port())
    }
}

impl fmt::Display for NodeAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.ip_and_port)
    }
}

impl Debug for NodeAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.ip_and_port)
    }
}

impl TryFrom<&str> for NodeAddress {
    type Error = NodeAddressParseError;

    fn try_from(addr: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = addr.split(':').collect();
        if parts.len() != 2 {
            return Err(NodeAddressParseError::InvalidFormat(addr.to_string()));
        }
        let ip = parts[0].to_string();
        let port = parts[1]
            .parse::<u16>()
            .map_err(|e| NodeAddressParseError::InvalidPort(addr.to_string(), e.to_string()))?;
        Ok(NodeAddress::new(ip, port))
    }
}

impl std::str::FromStr for NodeAddress {
    type Err = NodeAddressParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}
