use crate::clock::{HLC, now_millis};
use crate::cluster::strategy;
use crate::cluster::types::{
    AssignmentState, ClusterHealth, ClusterMetadata, ClusterOperationError, PartitionAssignments,
    PartitionId,
};
use crate::fs as fs_paths;
use crate::node::{NodeAddress, NodeId, SimpleNode, SimpleNodeState};
use bincode::{Decode, Encode};
use log::{error, info};
use std::collections::HashMap;
use std::fs;
use std::io;

/// Cluster configuration and state management.
///
/// `Cluster` holds the cluster topology including partition assignments,
/// node membership, and leader election state. For ephemeral clusters, use
/// [`NodeBuilder::ephemeral()`](crate::NodeBuilder::ephemeral). For persistent
/// clusters, create with [`Cluster::new()`] and save with [`Cluster::save()`].
///
/// # Example
///
/// ```no_run
/// use gossipgrid::Cluster;
///
/// // Create a persistent cluster configuration
/// let cluster = Cluster::new(
///     "my_cluster".to_string(),
///     3,   // cluster_size
///     0,   // this_node_index
///     9,   // partition_count
///     2,   // replication_factor
///     false, // is_ephemeral
/// );
/// cluster.save().expect("Failed to save cluster");
/// ```
#[derive(Debug, Clone, Encode, Decode)]
pub struct Cluster {
    pub cluster_name: String,
    pub cluster_size: u8,
    pub this_node_index: u8,
    pub partition_count: u16,
    pub replication_factor: u8,
    pub partition_assignments: PartitionAssignments,
    pub partition_leaders: HashMap<PartitionId, NodeId>,
    pub created_at: u64,
    pub updated_at: u64,
    pub is_ephemeral: bool,
    pub config_hlc: HLC,
}

impl From<Cluster> for ClusterMetadata {
    fn from(val: Cluster) -> Self {
        ClusterMetadata {
            cluster_name: val.cluster_name,
            cluster_size: val.cluster_size,
            this_node_index: val.this_node_index,
            partition_count: val.partition_count,
            replication_factor: val.replication_factor,
            partition_assignments: val
                .partition_assignments
                .iter()
                .map(|(&k, (v, _))| (k, v.clone()))
                .collect(),
            created_at: val.created_at,
            config_hlc: val.config_hlc,
        }
    }
}

impl Cluster {
    pub fn new(
        cluster_name: String,
        cluster_size: u8,
        this_node_index: u8,
        partition_count: u16,
        replication_factor: u8,
        is_ephemeral: bool,
    ) -> Self {
        let now = now_millis();
        let partition_assignments = strategy::generate_partition_assignments_rendezvous(
            cluster_size,
            partition_count,
            replication_factor,
        );

        Self {
            cluster_name,
            cluster_size,
            this_node_index,
            partition_count,
            replication_factor,
            partition_assignments,
            partition_leaders: HashMap::new(),
            created_at: now,
            updated_at: now,
            is_ephemeral,
            config_hlc: HLC {
                timestamp: now,
                counter: 0,
            },
        }
    }

    pub fn exists(cluster_name: &str) -> bool {
        fs_paths::cluster_metadata_path(cluster_name).exists()
    }

    pub fn load(cluster_name: &str) -> Result<Self, ClusterOperationError> {
        let path = fs_paths::cluster_metadata_path(cluster_name);
        if !path.exists() {
            return Err(ClusterOperationError::ClusterNotFound(
                cluster_name.to_string(),
            ));
        }
        let json = fs::read_to_string(path)?;
        let metadata: ClusterMetadata = serde_json::from_str(&json)?;

        let cconfig = Cluster {
            cluster_name: metadata.cluster_name,
            cluster_size: metadata.cluster_size,
            this_node_index: metadata.this_node_index,
            partition_count: metadata.partition_count,
            replication_factor: metadata.replication_factor,
            partition_assignments: metadata
                .partition_assignments
                .into_iter()
                .map(|(k, v)| (k, (v, AssignmentState::Unassigned)))
                .collect(),
            partition_leaders: std::collections::HashMap::new(),
            created_at: metadata.created_at,
            updated_at: now_millis(),
            is_ephemeral: false,
            config_hlc: metadata.config_hlc,
        };
        Ok(cconfig)
    }

    pub fn list_clusters() -> Result<Vec<String>, ClusterOperationError> {
        let dir = fs_paths::clusters_dir();
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut clusters = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;

            let cluster_dir = match entry.file_name().into_string() {
                Ok(file_name) => file_name,
                Err(_) => continue,
            };
            clusters.push(cluster_dir);
        }
        Ok(clusters)
    }

    pub fn save(&self) -> Result<(), ClusterOperationError> {
        if self.is_ephemeral {
            info!("Ephemeral cluster, not saving metadata to disk");
            return Ok(());
        }

        fs_paths::ensure_clusters_dir(&self.cluster_name)?;
        let path = fs_paths::cluster_metadata_path(&self.cluster_name);
        let metadata: ClusterMetadata = self.clone().into();
        let json = serde_json::to_string_pretty(&metadata)?;
        fs::write(path, json)?;
        Ok(())
    }

    pub fn assign_partition_leaders(&mut self) -> bool {
        let mut new_elections = HashMap::<PartitionId, NodeId>::new();
        let cluster_size = self.cluster_size;

        // Stage 1: Seed the elections with the current leaders if they are still healthy.
        // This provides stability if no better candidate (i.e., a Joined node with higher priority) is found.
        // NOTE: Hydrating nodes are excluded as they are not ready for leadership.
        for (partition_id, leader_node_id) in &self.partition_leaders {
            if let Some((partitions, assignment)) = self.partition_assignments.get(leader_node_id)
                && partitions.contains(partition_id)
                && matches!(
                    assignment,
                    AssignmentState::Joined { .. } | AssignmentState::JoinedSyncing { .. }
                )
            {
                new_elections.insert(*partition_id, *leader_node_id);
            }
        }

        // Stage 2: Evaluate all assigned nodes to find the optimal leader for each partition.
        // We use a state-aware priority system where 'Joined' nodes always outrank 'JoinedSyncing' nodes.
        // NOTE: We allow 'JoinedSyncing' nodes to hold "paper leadership" to prevent bootstrap deadlocks
        // during fresh cluster starts (where everyone is syncing). Actual data safety is enforced
        // at the API level (web.rs), which rejects writes if the leader is not yet fully 'Joined'.
        // Nodes in 'JoinedHydrating' are COMPLETELY ignored as they are still replaying local WAL.
        for (node_id, (partitions, assignment)) in &self.partition_assignments {
            let state_rank = match assignment {
                AssignmentState::Joined { .. } => 0, // Priority Rank 0: Fully joined
                AssignmentState::JoinedSyncing { .. } => 1, // Priority Rank 1: Still syncing
                AssignmentState::JoinedHydrating { .. } => continue, // Hydrating nodes cannot be leaders
                _ => continue, // Disconnected or Unassigned nodes cannot be leaders
            };

            Self::elect_from_node(
                &mut new_elections,
                &self.partition_assignments,
                node_id,
                partitions,
                cluster_size,
                state_rank,
            );
        }

        let changed = self.partition_leaders != new_elections;
        self.partition_leaders = new_elections;
        changed
    }

    /// Evaluates a node's suitability as a leader for its assigned partitions.
    /// Winner is determined by a composite score: (state_rank << 16) | circular_distance_from_ideal_node.
    fn elect_from_node(
        new_elections: &mut HashMap<PartitionId, NodeId>,
        assignments: &PartitionAssignments,
        node_id: &NodeId,
        partitions: &Vec<PartitionId>,
        cluster_size: u8,
        state_rank: u8,
    ) {
        for partition in partitions {
            // Calculate circular distance from the 'ideal' node index (partition_id % cluster_size).
            let ideal_node = (partition.0 % cluster_size as u16) as u8;
            let new_priority =
                (*node_id as i16 - ideal_node as i16).rem_euclid(cluster_size as i16) as u16;

            // Build a composite score where state is the most significant factor.
            let new_score = ((state_rank as u32) << 16) | (new_priority as u32);

            if let Some(current_best_id) = new_elections.get(partition) {
                let current_state_rank = match assignments.get(current_best_id) {
                    Some((_, AssignmentState::Joined { .. })) => 0,
                    _ => 1,
                };

                let current_priority = (*current_best_id as i16 - ideal_node as i16)
                    .rem_euclid(cluster_size as i16) as u16;
                let current_score = ((current_state_rank as u32) << 16) | (current_priority as u32);

                // If the new candidate has a better (lower) score, they take over leadership.
                if new_score < current_score {
                    new_elections.insert(*partition, *node_id);
                }
            } else {
                // If partition has no leader yet, this node takes it.
                new_elections.insert(*partition, *node_id);
            }
        }
    }

    pub fn assign_node(
        &mut self,
        node_index: &u8,
        node: &SimpleNode,
    ) -> Result<bool, ClusterOperationError> {
        if let Some((_, assignment)) = self.partition_assignments.get_mut(node_index) {
            let changed = *assignment != node.into();
            *assignment = node.into();
            self.updated_at = now_millis();
            let leader_changed = self.assign_partition_leaders();
            if changed || leader_changed {
                self.config_hlc = self.config_hlc.tick_hlc(self.updated_at);
            }
            Ok(changed || leader_changed)
        } else {
            Err(ClusterOperationError::IoError(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Invalid node index: {node_index}"),
            )))
        }
    }

    pub fn unassign_node(
        &mut self,
        node_index: &u8,
        current_hlc: &HLC,
    ) -> Result<bool, ClusterOperationError> {
        let mut changed = false;
        let assignment_data = self.partition_assignments.get_mut(node_index);
        if let Some((_, assignment)) = assignment_data {
            match assignment {
                AssignmentState::Joined { node } | AssignmentState::JoinedSyncing { node } => {
                    let mut updated_node = node.clone();
                    updated_node.hlc = current_hlc.tick_hlc(now_millis());
                    *assignment = AssignmentState::Disconnected { node: updated_node };
                    changed = true;
                }
                _ => {}
            }
        }
        let leader_changed = self.assign_partition_leaders();
        if changed || leader_changed {
            self.updated_at = now_millis();
            self.config_hlc = self.config_hlc.tick_hlc(self.updated_at);
        }
        Ok(changed || leader_changed)
    }

    pub fn assign_next(&mut self, node: &SimpleNode) -> Result<(u8, bool), ClusterOperationError> {
        for node_index in 0..self.cluster_size {
            if let Some((_, assignment)) = self.partition_assignments.get_mut(&node_index)
                && *assignment == AssignmentState::Unassigned
            {
                *assignment = node.into();
                self.updated_at = now_millis();
                let leader_changed = self.assign_partition_leaders();
                return Ok((node_index, leader_changed));
            }
        }
        Err(ClusterOperationError::IoError(io::Error::other(
            "No unassigned nodes available",
        )))
    }

    pub fn resize(&mut self, new_size: u8) -> Result<(), ClusterOperationError> {
        if new_size == self.cluster_size {
            return Ok(());
        }

        // Prevent resize if any partition handshake is in progress
        if self.has_locked_partitions() {
            return Err(ClusterOperationError::ResizeInProgress(
                "Cannot resize: partition leadership handshake in progress".to_string(),
            ));
        }

        if new_size < self.cluster_size {
            let disconnected_nodes = self
                .partition_assignments
                .values()
                .filter(|(_, assignment)| {
                    matches!(assignment, AssignmentState::Disconnected { .. })
                })
                .count();

            if disconnected_nodes == 0 {
                return Err(ClusterOperationError::DownsizeError(
                    "Cannot downsize cluster: no nodes are in Disconnected state".to_string(),
                ));
            }
        }

        let old_assignments = self.partition_assignments.clone();

        // Regenerate assignments with new size using Rendezvous hashing for stability
        self.partition_assignments = strategy::generate_partition_assignments_rendezvous(
            new_size,
            self.partition_count,
            self.replication_factor,
        );

        // Preserve states of existing nodes that are still within new size
        for node_index in 0..new_size {
            if let Some((_, old_state)) = old_assignments.get(&node_index) {
                if let Some((_, new_state)) = self.partition_assignments.get_mut(&node_index) {
                    *new_state = old_state.clone();
                }
            }
        }

        self.cluster_size = new_size;
        self.updated_at = now_millis();
        self.config_hlc = self.config_hlc.tick_hlc(self.updated_at);

        // Update leadership
        self.assign_partition_leaders();

        // Persist
        self.save()?;

        Ok(())
    }

    pub fn is_fully_assigned(&self) -> bool {
        !self
            .partition_assignments
            .values()
            .any(|(_, assignment)| matches!(assignment, AssignmentState::Unassigned))
    }

    pub fn get_cluster_health(&self) -> ClusterHealth {
        let all_peers = self.get_all_peers_cloned();

        // 1. Check if all partitions have a leader
        if self.partition_leaders.len() != self.partition_count as usize {
            return ClusterHealth::Unhealthy;
        }

        // 2. Check if all leaders are in 'Joined' state
        let all_leaders_joined = self.partition_leaders.values().all(|&leader_node_id| {
            all_peers
                .get(&leader_node_id)
                .map(|node| node.state == SimpleNodeState::Joined)
                .unwrap_or(false)
        });

        if !all_leaders_joined {
            return ClusterHealth::Unhealthy;
        }

        // 3. Check if all assigned nodes (including replicas) are in 'Joined' state
        let all_joined = self
            .partition_assignments
            .values()
            .all(|(_, assignment)| matches!(assignment, AssignmentState::Joined { .. }));

        if all_joined {
            ClusterHealth::Healthy
        } else {
            ClusterHealth::PartiallyHealthy
        }
    }

    pub fn get_assigned_partitions(&self, node_index: u8) -> Option<Vec<PartitionId>> {
        self.partition_assignments
            .get(&node_index)
            .map(|(partitions, _)| partitions.clone())
    }

    pub fn hash_key(&self, key: &str) -> PartitionId {
        Self::hash_key_inner(self.partition_count, key)
    }

    pub fn hash_key_inner(partition_count: u16, key: &str) -> PartitionId {
        use std::hash::{Hash, Hasher};
        use twox_hash::XxHash64;

        let mut h = XxHash64::with_seed(0);
        key.hash(&mut h);
        let id = h.finish() % partition_count as u64;
        PartitionId(id as u16)
    }

    pub fn find_leader_for_partition(&self, key: &str) -> Option<SimpleNode> {
        let partition_id = self.hash_key(key);
        match self.partition_leaders.get(&partition_id) {
            Some(leader_node_id) => self
                .partition_assignments
                .get(leader_node_id)
                .filter(|(_, assignment)| matches!(assignment, AssignmentState::Joined { .. }))
                .and_then(|(_, assignment)| assignment.into()),
            None => None,
        }
    }

    pub fn find_replica_for_partition(&self, key: &str) -> Option<SimpleNode> {
        let partition = self.hash_key(key);

        if let Some((partitions, assignment)) =
            self.partition_assignments.get(&self.this_node_index)
            && partitions.contains(&partition)
        {
            return assignment.into();
        }

        for (_node_index, (partitions, assignment)) in self.partition_assignments.iter() {
            if matches!(assignment, AssignmentState::Joined { .. })
                && !self.partition_leaders.contains_key(&partition)
                && partitions.contains(&partition)
            {
                return assignment.into();
            }
        }

        None
    }

    pub fn get_all_peers_cloned(&self) -> HashMap<u8, SimpleNode> {
        let mut node_leadership: HashMap<u8, Vec<PartitionId>> = HashMap::new();
        for (pid, nid) in &self.partition_leaders {
            node_leadership.entry(*nid).or_default().push(*pid);
        }

        let mut peers = HashMap::new();
        for (node_index, (_partitions, assignment)) in self.partition_assignments.iter() {
            let simple_node: Option<SimpleNode> = assignment.into();

            if let Some(mut snode) = simple_node {
                if let Some(lp) = node_leadership.get(node_index) {
                    snode.leading_partitions = lp.clone();
                }
                peers.insert(*node_index, snode);
            }
        }
        peers
    }

    pub fn other_peers(&self) -> HashMap<u8, SimpleNode> {
        let Some(node_assignment) = self.partition_assignments.get(&self.this_node_index) else {
            error!("Current node index is not within partition assignments");
            return HashMap::new();
        };

        let assigned_node = match &node_assignment.1 {
            AssignmentState::JoinedHydrating { node } => node,
            AssignmentState::JoinedSyncing { node } => node,
            AssignmentState::Joined { node } => node,
            AssignmentState::Disconnected { node } => node,
            AssignmentState::Unassigned => {
                return HashMap::new();
            }
        };

        self.get_all_peers_cloned()
            .iter()
            .filter(|(_k, v)| v.address != assigned_node.address)
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    pub fn get_all_healthy_replicas(&self, partition: &PartitionId) -> Vec<SimpleNode> {
        let mut replicas = Vec::new();
        for (node_index, (partitions, assignment)) in self.partition_assignments.iter() {
            if matches!(assignment, AssignmentState::Joined { .. })
                && partitions.contains(partition)
            {
                let astate: Option<SimpleNode> = assignment.into();

                if let Some(nid) = self.partition_leaders.get(partition)
                    && nid == node_index
                {
                    continue;
                }

                if let Some(snode) = astate {
                    replicas.push(snode.clone())
                }
            }
        }
        replicas
    }

    pub fn get_this_leaders_partitions(&self) -> Vec<PartitionId> {
        let mut partitions = vec![];
        for (pid, nid) in self.partition_leaders.iter() {
            if nid == &self.this_node_index {
                partitions.push(*pid);
            }
        }
        partitions
    }

    pub fn contains_partition(&self, partition: &PartitionId) -> bool {
        match self.partition_assignments.get(&self.this_node_index) {
            Some(replicas) => replicas.0.contains(partition),
            None => false,
        }
    }

    pub fn update_assignment_state_for_this_node(&mut self, simple_node: &SimpleNode) -> bool {
        let (_, assignment_state) = self
            .partition_assignments
            .get_mut(&self.this_node_index)
            .unwrap();

        let new_assignment_state = AssignmentState::from(simple_node);

        if *assignment_state != new_assignment_state {
            *assignment_state = new_assignment_state;
            let _leader_changed = self.assign_partition_leaders();
            return true;
        }
        false
    }

    pub fn update_cluster_membership(
        &mut self,
        sender_node: &SimpleNode,
        other_peers: &HashMap<NodeId, SimpleNode>,
        new_cluster_size: u8,
        incoming_config_hlc: &HLC,
    ) -> Result<bool, ClusterOperationError> {
        let mut changed = false;

        if incoming_config_hlc > &self.config_hlc {
            // Merge incoming config HLC to keep track of the latest topology version
            self.config_hlc = HLC::merge(&self.config_hlc, incoming_config_hlc, now_millis());

            if new_cluster_size != self.cluster_size {
                info!(
                    "node={}; Cluster size change detected: {} -> {}",
                    self.this_node_index, self.cluster_size, new_cluster_size
                );
                self.resize(new_cluster_size)?;
                changed = true;
            }
        }

        let sender_idx = self.get_node_index(&sender_node.address);

        for (idx, (_, assignment)) in &mut self.partition_assignments {
            let remote_node = if Some(*idx) == sender_idx {
                Some(sender_node)
            } else {
                other_peers.get(idx)
            };

            if let Some(remote_node) = remote_node {
                let mut next_assignment = None;

                match &mut *assignment {
                    AssignmentState::Joined { node, .. }
                    | AssignmentState::JoinedHydrating { node, .. }
                    | AssignmentState::Disconnected { node, .. } => {
                        if node.address == sender_node.address {
                            if remote_node.hlc >= node.hlc {
                                next_assignment = Some(remote_node.into());
                            }
                        } else if remote_node.hlc > node.hlc {
                            next_assignment = Some(remote_node.into());
                        }
                    }
                    AssignmentState::JoinedSyncing { .. } => {
                        next_assignment = Some(remote_node.into());
                    }
                    AssignmentState::Unassigned => {
                        next_assignment = Some(remote_node.into());
                    }
                }

                if let Some(state) = next_assignment
                    && *assignment != state
                {
                    *assignment = state;
                    changed = true;
                }
            }
        }

        let mut claims = Vec::new();
        if let Some(idx) = sender_idx {
            for pid in &sender_node.leading_partitions {
                claims.push((*pid, idx, sender_node.hlc.clone()));
            }
        }
        for (idx, node) in other_peers {
            for pid in &node.leading_partitions {
                claims.push((*pid, *idx, node.hlc.clone()));
            }
        }

        for (pid, claimer_idx, claimer_hlc) in claims {
            let update_needed = if let Some(current_leader_idx) = self.partition_leaders.get(&pid) {
                if current_leader_idx == &claimer_idx {
                    false
                } else if let Some((_, current_assignment)) =
                    self.partition_assignments.get(current_leader_idx)
                {
                    match current_assignment {
                        AssignmentState::Joined { node, .. }
                        | AssignmentState::JoinedHydrating { node, .. }
                        | AssignmentState::Disconnected { node, .. } => claimer_hlc > node.hlc,
                        AssignmentState::JoinedSyncing { .. } => true,
                        AssignmentState::Unassigned => true,
                    }
                } else {
                    true
                }
            } else {
                true
            };

            if update_needed {
                self.partition_leaders.insert(pid, claimer_idx);
                changed = true;
            }
        }

        let leader_changed = self.assign_partition_leaders();
        self.updated_at = now_millis();

        Ok(changed || leader_changed)
    }

    pub fn get_node_index(&self, address: &NodeAddress) -> Option<u8> {
        for (idx, (_, assignment)) in &self.partition_assignments {
            match assignment {
                AssignmentState::Joined { node }
                | AssignmentState::JoinedHydrating { node }
                | AssignmentState::JoinedSyncing { node } => {
                    if node.address == *address {
                        return Some(*idx);
                    }
                }
                AssignmentState::Disconnected { node } => {
                    if node.address == *address {
                        return Some(*idx);
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// Check if any node in the cluster has non-empty locked_partitions.
    /// Used to prevent concurrent resize operations during leadership transitions.
    pub fn has_locked_partitions(&self) -> bool {
        for (_node_index, (_, assignment)) in &self.partition_assignments {
            match assignment {
                AssignmentState::Joined { node }
                | AssignmentState::JoinedSyncing { node }
                | AssignmentState::JoinedHydrating { node }
                | AssignmentState::Disconnected { node } => {
                    // Note: locked_partitions is not directly on AssignmentNode,
                    // it's gossiped via SimpleNode. For now, return false as this
                    // will be properly implemented when we add lock initiation logic.
                    // This is a placeholder for task 3.2.
                    let _ = node; // Suppress unused warning
                }
                AssignmentState::Unassigned => {}
            }
        }
        false // TODO: Implement once locked_partitions is propagated through gossip
    }

    /// Check if a specific partition is locked on any node in the cluster.
    pub fn is_partition_locked(&self, _partition: PartitionId) -> bool {
        // TODO: Implement once locked_partitions is properly propagated
        false
    }

    /// Get list of nodes that have a specific partition locked.
    pub fn get_nodes_with_locked_partition(&self, _partition: PartitionId) -> Vec<NodeId> {
        // TODO: Implement once locked_partitions is properly propagated
        Vec::new()
    }
}
