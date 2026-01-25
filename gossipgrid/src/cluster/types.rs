use crate::clock::HLC;
use crate::node::{NodeAddress, SimpleNode, SimpleNodeState};
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClusterOperationError {
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("Cluster not found: {0}")]
    ClusterNotFound(String),
    #[error("Error updating cluster state: {0}")]
    ClusterStateChangeError(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub enum ClusterHealth {
    Healthy,
    PartiallyHealthy,
    Unhealthy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, PartialEq)]
pub struct AssignmentNode {
    pub address: NodeAddress,
    pub web_port: u16,
    pub last_seen: u64,
    pub hlc: HLC,
    pub partition_item_counts: HashMap<PartitionId, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, PartialEq)]
pub enum AssignmentState {
    Unassigned,
    JoinedHydrating { node: AssignmentNode },
    JoinedSyncing { node: AssignmentNode },
    Joined { node: AssignmentNode },
    Disconnected { node: AssignmentNode },
}

impl AssignmentState {
    pub fn get_node(&self) -> Option<&AssignmentNode> {
        match self {
            AssignmentState::Unassigned => None,
            AssignmentState::JoinedHydrating { node } => Some(node),
            AssignmentState::JoinedSyncing { node } => Some(node),
            AssignmentState::Joined { node } => Some(node),
            AssignmentState::Disconnected { node } => Some(node),
        }
    }
}

impl From<&AssignmentState> for Option<SimpleNode> {
    fn from(state: &AssignmentState) -> Option<SimpleNode> {
        match state {
            AssignmentState::Unassigned => None,
            AssignmentState::JoinedHydrating { node } => Some(SimpleNode {
                address: node.address.clone(),
                web_port: node.web_port,
                last_seen: node.last_seen,
                state: SimpleNodeState::JoinedHydrating,
                hlc: node.hlc.clone(),
                partition_item_counts: node.partition_item_counts.clone(),
                leading_partitions: Default::default(),
            }),
            AssignmentState::JoinedSyncing { node } => Some(SimpleNode {
                address: node.address.clone(),
                web_port: node.web_port,
                last_seen: node.last_seen,
                state: SimpleNodeState::JoinedSyncing,
                hlc: node.hlc.clone(),
                partition_item_counts: node.partition_item_counts.clone(),
                leading_partitions: Default::default(),
            }),
            AssignmentState::Joined { node } => Some(SimpleNode {
                address: node.address.clone(),
                web_port: node.web_port,
                last_seen: node.last_seen,
                state: SimpleNodeState::Joined,
                hlc: node.hlc.clone(),
                partition_item_counts: node.partition_item_counts.clone(),
                leading_partitions: Default::default(),
            }),
            AssignmentState::Disconnected { node } => Some(SimpleNode {
                address: node.address.clone(),
                web_port: node.web_port,
                last_seen: node.last_seen,
                state: SimpleNodeState::Disconnected,
                hlc: node.hlc.clone(),
                partition_item_counts: node.partition_item_counts.clone(),
                leading_partitions: Default::default(),
            }),
        }
    }
}

impl From<&SimpleNode> for AssignmentState {
    fn from(snode: &SimpleNode) -> AssignmentState {
        let node = AssignmentNode {
            address: snode.address.clone(),
            web_port: snode.web_port,
            last_seen: snode.last_seen,
            hlc: snode.hlc.clone(),
            partition_item_counts: snode.partition_item_counts.clone(),
        };
        match snode.state {
            SimpleNodeState::JoinedHydrating => AssignmentState::JoinedHydrating { node },
            SimpleNodeState::JoinedSyncing => AssignmentState::JoinedSyncing { node },
            SimpleNodeState::Joined => AssignmentState::Joined { node },
            SimpleNodeState::Disconnected => AssignmentState::Disconnected { node },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMetadata {
    pub cluster_name: String,
    pub cluster_size: u8,
    pub this_node_index: u8,
    pub partition_count: u16,
    pub replication_factor: u8,
    pub partition_assignments: HashMap<u8, Vec<PartitionId>>,
    pub created_at: u64,
}

pub type PartitionAssignments = HashMap<u8, (Vec<PartitionId>, AssignmentState)>;
