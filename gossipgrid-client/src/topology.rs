use crate::error::ClientError;
use crate::types::{NodeAddress, PartitionId};
use log::{error, warn};
use serde::Deserialize;
use std::collections::HashMap;

/// Information about a single node in the cluster.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeInfo {
    pub address: NodeAddress,
    pub web_port: u16,
    pub state: String,
}

/// Raw topology response from the server (matches the JSON shape).
/// Partition IDs come as u16 keys in JSON, which we convert to PartitionId.
#[derive(Debug, Clone, Deserialize)]
struct RawTopologyResponse {
    pub cluster_name: String,
    pub partition_count: u16,
    pub replication_factor: u8,
    pub nodes: HashMap<u8, RawNodeInfo>,
    pub partition_assignments: HashMap<u8, Vec<u16>>,
    pub partition_leaders: HashMap<u16, u8>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawNodeInfo {
    pub address: String,
    pub web_port: u16,
    pub state: String,
}

/// A snapshot of the cluster topology, used for client-side routing decisions.
#[derive(Debug, Clone)]
pub struct TopologySnapshot {
    pub cluster_name: String,
    pub partition_count: u16,
    pub replication_factor: u8,
    pub nodes: HashMap<u8, NodeInfo>,
    pub partition_assignments: HashMap<u8, Vec<PartitionId>>,
    pub partition_leaders: HashMap<PartitionId, u8>,
}

impl TopologySnapshot {
    /// Fetch the topology from a single node's `/cluster/topology` endpoint.
    async fn fetch(client: &reqwest::Client, base_url: &str) -> Result<Self, ClientError> {
        let url = format!("http://{base_url}/cluster/topology");
        let response =
            client
                .get(&url)
                .send()
                .await
                .map_err(|e| ClientError::ConnectionFailed {
                    address: base_url.to_string(),
                    source: e,
                })?;

        if !response.status().is_success() {
            return Err(ClientError::ServerError {
                status: response.status().as_u16(),
                message: response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Failed to read response body".to_string()),
            });
        }

        let raw: RawTopologyResponse =
            response
                .json()
                .await
                .map_err(|e| ClientError::ConnectionFailed {
                    address: base_url.to_string(),
                    source: e,
                })?;

        Ok(Self::from_raw(raw))
    }

    /// Convert the raw JSON response into typed topology.
    fn from_raw(raw: RawTopologyResponse) -> Self {
        let nodes = raw
            .nodes
            .into_iter()
            .map(|(idx, raw_node)| {
                let address = NodeAddress::parse(&raw_node.address)
                    .unwrap_or_else(|| NodeAddress::new(raw_node.address.clone(), 0));
                (
                    idx,
                    NodeInfo {
                        address,
                        web_port: raw_node.web_port,
                        state: raw_node.state,
                    },
                )
            })
            .collect();

        let partition_assignments = raw
            .partition_assignments
            .into_iter()
            .map(|(idx, pids)| (idx, pids.into_iter().map(PartitionId).collect()))
            .collect();

        let partition_leaders = raw
            .partition_leaders
            .into_iter()
            .map(|(pid, nid)| (PartitionId(pid), nid))
            .collect();

        TopologySnapshot {
            cluster_name: raw.cluster_name,
            partition_count: raw.partition_count,
            replication_factor: raw.replication_factor,
            nodes,
            partition_assignments,
            partition_leaders,
        }
    }

    /// Fetch topology with seed node fallback.
    /// Tries each seed in order until one succeeds.
    pub async fn fetch_from_seeds(
        client: &reqwest::Client,
        seeds: &[String],
    ) -> Result<Self, ClientError> {
        for seed in seeds {
            match Self::fetch(client, seed).await {
                Ok(snapshot) => return Ok(snapshot),
                Err(e) => {
                    warn!("Failed to fetch topology from seed {seed}: {e}");
                    continue;
                }
            }
        }
        Err(ClientError::NoHealthyNodes)
    }

    /// Try to fetch topology from any known node in the current snapshot.
    /// Falls back to seed nodes if all known nodes fail.
    pub async fn refresh(
        &self,
        client: &reqwest::Client,
        seeds: &[String],
    ) -> Result<Self, ClientError> {
        // Try known nodes first (by web port)
        for node in self.nodes.values() {
            if node.state == "Joined" {
                let base_url = format!("{}:{}", node.address.ip, node.web_port);
                match Self::fetch(client, &base_url).await {
                    Ok(snapshot) => return Ok(snapshot),
                    Err(e) => {
                        error!("Failed to fetch topology from known node {base_url}: {e}");
                        continue;
                    }
                }
            }
        }

        // Fall back to seeds
        Self::fetch_from_seeds(client, seeds).await
    }
}
