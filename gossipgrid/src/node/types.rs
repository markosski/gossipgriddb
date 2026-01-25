use crate::cluster::PartitionId;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};
use thiserror::Error;

pub type NodeId = u8;

#[derive(Error, Debug)]
pub enum NodeError {
    #[error("Error starting node: {0}")]
    ErrorStartingNode(String),
    #[error("Error joining node: {0}")]
    ErrorJoiningNode(String),
    #[error("Error performing item operation: {0}")]
    ItemOperationError(String),
    #[error("Error persisting node state: {0}")]
    NodePersistenceError(String),
    #[error("Configuration error: {0}")]
    ConfigurationError(String),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Encode, Decode, Serialize, Deserialize)]
pub struct NodeIdPartitionId(pub NodeId, pub PartitionId);

impl NodeIdPartitionId {
    pub fn node_id(&self) -> NodeId {
        self.0
    }
    pub fn partition_id(&self) -> PartitionId {
        self.1
    }
}

impl Display for NodeIdPartitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.0, self.1)
    }
}

pub type PartitionCount = HashMap<NodeId, HashMap<PartitionId, usize>>;

#[derive(Debug, Clone, Default)]
pub struct NodeConfig {
    pub base_dir: Option<String>,
}
