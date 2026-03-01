use crate::error::ClientError;
use crate::topology::{NodeInfo, TopologySnapshot};
use crate::types::PartitionId;

/// The type of operation being performed, used to determine routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Read,
    Write,
    Delete,
}

/// Hash a partition key to a partition ID, identical to the server's `Cluster::hash_key_inner`.
///
/// Uses xxHash64 with seed 0 and `Hash` trait for `&str` to ensure byte-identical results.
pub fn hash_key(partition_count: u16, key: &str) -> PartitionId {
    use std::hash::{Hash, Hasher};
    use twox_hash::XxHash64;

    let mut h = XxHash64::with_seed(0);
    key.hash(&mut h);
    PartitionId((h.finish() % partition_count as u64) as u16)
}

/// Resolve the target node for a given key and operation.
///
/// Returns the target `NodeInfo` and the resolved `PartitionId`.
pub fn resolve_target<'a>(
    snapshot: &'a TopologySnapshot,
    key: &str,
    operation: Operation,
) -> Result<(&'a NodeInfo, PartitionId), ClientError> {
    let partition_id = hash_key(snapshot.partition_count, key);

    match operation {
        Operation::Read => resolve_read_target(snapshot, partition_id),
        Operation::Write | Operation::Delete => resolve_write_target(snapshot, partition_id),
    }
}

/// For reads: find a healthy replica for the partition, falling back to leader.
fn resolve_read_target<'a>(
    snapshot: &'a TopologySnapshot,
    partition_id: PartitionId,
) -> Result<(&'a NodeInfo, PartitionId), ClientError> {
    let leader_node_index = snapshot.partition_leaders.get(&partition_id);

    // Try to find a non-leader Joined replica that holds this partition
    for (node_index, partitions) in &snapshot.partition_assignments {
        if partitions.contains(&partition_id) {
            // Prefer non-leader replicas
            if leader_node_index.is_some_and(|&leader| leader == *node_index) {
                continue;
            }
            if let Some(node) = snapshot.nodes.get(node_index) {
                if node.state == "Joined" {
                    return Ok((node, partition_id));
                }
            }
        }
    }

    // Fall back to leader
    if let Some(&leader_idx) = leader_node_index {
        if let Some(node) = snapshot.nodes.get(&leader_idx) {
            if node.state == "Joined" {
                return Ok((node, partition_id));
            }
        }
    }

    Err(ClientError::LeaderUnavailable { partition_id })
}

/// For writes/deletes: route to leader only.
fn resolve_write_target<'a>(
    snapshot: &'a TopologySnapshot,
    partition_id: PartitionId,
) -> Result<(&'a NodeInfo, PartitionId), ClientError> {
    if let Some(&leader_idx) = snapshot.partition_leaders.get(&partition_id) {
        if let Some(node) = snapshot.nodes.get(&leader_idx) {
            if node.state == "Joined" {
                return Ok((node, partition_id));
            }
        }
    }

    Err(ClientError::LeaderUnavailable { partition_id })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeAddress;

    #[test]
    fn test_hash_key_deterministic() {
        let result1 = hash_key(9, "user:123");
        let result2 = hash_key(9, "user:123");
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_hash_key_within_range() {
        for i in 0..100 {
            let key = format!("key:{i}");
            let partition = hash_key(9, &key);
            assert!(
                partition.0 < 9,
                "partition {} out of range for key {key}",
                partition.0
            );
        }
    }

    #[test]
    fn test_resolve_write_to_leader() {
        let snapshot = make_test_snapshot();
        let (node, _pid) = resolve_target(&snapshot, "test_key", Operation::Write).unwrap();
        assert_eq!(node.state, "Joined");
    }

    #[test]
    fn test_resolve_read_prefers_replica() {
        let snapshot = make_test_snapshot();
        let partition_id = hash_key(snapshot.partition_count, "test_key");
        let leader_idx = snapshot.partition_leaders.get(&partition_id);
        let (node, _pid) = resolve_target(&snapshot, "test_key", Operation::Read).unwrap();

        let has_non_leader_replica = snapshot.partition_assignments.iter().any(|(idx, parts)| {
            parts.contains(&partition_id)
                && leader_idx.is_some_and(|&l| l != *idx)
                && snapshot.nodes.get(idx).is_some_and(|n| n.state == "Joined")
        });

        if has_non_leader_replica {
            if let Some(&leader) = leader_idx {
                let leader_node = snapshot.nodes.get(&leader).unwrap();
                assert_ne!(
                    node.address, leader_node.address,
                    "Read should prefer replica over leader"
                );
            }
        }
    }

    #[test]
    fn test_leader_unavailable_on_write() {
        let mut snapshot = make_test_snapshot();
        snapshot.partition_leaders.clear();
        let result = resolve_target(&snapshot, "test_key", Operation::Write);
        assert!(matches!(result, Err(ClientError::LeaderUnavailable { .. })));
    }

    fn make_test_snapshot() -> TopologySnapshot {
        use std::collections::HashMap;

        let mut nodes = HashMap::new();
        nodes.insert(
            0,
            NodeInfo {
                address: NodeAddress::new("127.0.0.1".to_string(), 4009),
                web_port: 3001,
                state: "Joined".to_string(),
            },
        );
        nodes.insert(
            1,
            NodeInfo {
                address: NodeAddress::new("127.0.0.1".to_string(), 4010),
                web_port: 3002,
                state: "Joined".to_string(),
            },
        );
        nodes.insert(
            2,
            NodeInfo {
                address: NodeAddress::new("127.0.0.1".to_string(), 4011),
                web_port: 3003,
                state: "Joined".to_string(),
            },
        );

        let mut partition_assignments = HashMap::new();
        partition_assignments.insert(0, vec![PartitionId(0), PartitionId(3), PartitionId(6)]);
        partition_assignments.insert(1, vec![PartitionId(1), PartitionId(4), PartitionId(7)]);
        partition_assignments.insert(2, vec![PartitionId(2), PartitionId(5), PartitionId(8)]);

        let mut partition_leaders = HashMap::new();
        for p in 0..9u16 {
            partition_leaders.insert(PartitionId(p), (p % 3) as u8);
        }

        TopologySnapshot {
            cluster_name: "test_cluster".to_string(),
            partition_count: 9,
            replication_factor: 2,
            nodes,
            partition_assignments,
            partition_leaders,
        }
    }
}
