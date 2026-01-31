use crate::cluster::types::{AssignmentState, PartitionAssignments, PartitionId};
use std::collections::HashMap;

pub fn hash_combined(partition: PartitionId, node: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    use twox_hash::XxHash64;

    let mut h = XxHash64::with_seed(0);
    (partition, node).hash(&mut h);
    h.finish()
}

#[allow(dead_code)]
pub fn generate_partition_assignments_rendezvous(
    cluster_size: u8,
    partition_count: u16,
    replication_factor: u8,
) -> PartitionAssignments {
    use std::collections::BTreeMap;

    let mut assignments: HashMap<u8, Vec<PartitionId>> = HashMap::new();

    for partition_id in 0..partition_count {
        let mut hashed: BTreeMap<u64, u8> = BTreeMap::new();

        for node_idx in 0..cluster_size {
            let node_placeholder = format!("node_{node_idx}");
            let h = hash_combined(partition_id.into(), &node_placeholder);
            hashed.insert(h, node_idx);
        }

        let owners = hashed
            .values()
            .take(replication_factor as usize)
            .cloned()
            .collect::<Vec<_>>();

        for owner in owners {
            assignments
                .entry(owner)
                .or_default()
                .push(partition_id.into());
        }
    }

    assignments
        .into_iter()
        .map(|(idx, partitions)| (idx, (partitions, AssignmentState::Unassigned)))
        .collect()
}
