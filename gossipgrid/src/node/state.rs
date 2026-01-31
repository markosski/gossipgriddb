use crate::clock::{HLC, now_millis};
use crate::cluster::PartitionId;
use crate::cluster::{self, Cluster};
use crate::env::Env;
use crate::event_bus::{Event, EventBus};
use crate::fs as fs_paths;
use crate::gossip::Gossip;
use crate::item::{Item, ItemEntry, ItemStatus};
use crate::node::NodeMetadata;
use crate::store::{DataStoreError, StorageKey, Store};
use crate::sync::{FramedWalRecordItem, SyncState};
use crate::wal::{Wal, WalRecord};
use bincode::{Decode, Encode};
use dashmap::DashMap;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU8;
use tokio::sync::RwLock;

use super::address::NodeAddress;
use super::types::{NodeError, NodeId};

pub enum NodeState {
    PreJoin(PreJoinNode),
    Joined(JoinedNode),
    Disconnected(DisconnectedNode),
}

#[derive(Debug)]
pub struct DisconnectedNode {
    pub address: NodeAddress,
    pub web_port: u16,
    pub node_hlc: HLC,
}

#[derive(Debug)]
pub struct PreJoinNode {
    pub address: NodeAddress,
    pub web_port: u16,
    pub peer_node: NodeAddress,
    pub cluster: Option<Cluster>,
    pub node_hlc: HLC,
}

pub struct JoinedNode {
    pub address: NodeAddress,
    pub web_port: u16,
    pub cluster: Cluster,
    pub(crate) partition_counts: DashMap<NodeId, HashMap<PartitionId, usize>>,
    pub node_hlc: Mutex<HLC>,
    // Used for gossip operations
    pub(crate) gossip: Gossip,
    // Used for active sync operations
    pub(crate) sync_state: SyncState,
    // True when node is catching up, always after node restart
    pub is_syncing: bool,
    // True when node is hydrating, always after node restart but only for in-memory stores
    pub is_hydrating: bool,
}

impl std::fmt::Debug for JoinedNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinedNode")
            .field("all_peers", &self.cluster.get_all_peers_cloned())
            .field("cluster_config", &self.cluster)
            .field("this_node", &self.address)
            .field("this_node_web_port", &self.web_port)
            .field("node_hlc", &self.node_hlc)
            .field("next_node_index", &self.gossip.next_node_index)
            .finish()
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, PartialEq)]
pub enum SimpleNodeState {
    JoinedHydrating,
    JoinedSyncing,
    Joined,
    Disconnected,
}

/// Represents a node in the cluster with important information to share with other nodes during gossip
#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, PartialEq)]
pub struct SimpleNode {
    pub address: NodeAddress,
    pub state: SimpleNodeState,
    pub web_port: u16,
    pub last_seen: u64,
    pub hlc: HLC,
    pub partition_item_counts: HashMap<PartitionId, usize>,
    pub leading_partitions: Vec<PartitionId>,
}

impl fmt::Display for SimpleNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SimpleNode {{ address: {}, state: {:?}, web_port: {}, last_seen: {}, hlc: {:?}, partition_item_counts sum: {}, leading_partitions count: {} }}",
            self.address,
            self.state,
            self.web_port,
            self.last_seen,
            self.hlc,
            self.partition_item_counts.values().sum::<usize>(),
            self.leading_partitions.len()
        )
    }
}

impl NodeState {
    /// Initiate node in either Joined or PreJoin state
    ///
    /// If cluster_config is provided, node is initialized as Joined
    /// If seed_peer is provided, node is initialized as PreJoin
    /// If both are provided, node is initialized as PreJoined
    /// If neither are provided, invalid state - panic
    pub fn init(
        local_addr: NodeAddress,
        local_web_port: u16,
        seed_peer: Option<NodeAddress>,
        cluster_config: Option<cluster::Cluster>,
    ) -> NodeState {
        match (cluster_config, seed_peer) {
            (Some(cluster_config), None) => {
                let mut joined_node = JoinedNode {
                    cluster: cluster_config.clone(),
                    partition_counts: DashMap::new(),
                    address: local_addr,
                    web_port: local_web_port,
                    node_hlc: Mutex::new(HLC {
                        timestamp: now_millis(),
                        counter: 0,
                    }),
                    gossip: Gossip {
                        next_node_index: AtomicU8::new(0),
                    },
                    sync_state: SyncState {
                        replica_partition_lsn: DashMap::new(),
                        lsn_waiters: DashMap::new(),
                        leader_confirmed_lsn: DashMap::new(),
                        leader_tip_lsns: DashMap::new(),
                    },
                    is_syncing: false,
                    is_hydrating: true,
                };

                // Load persisted metadata if not ephemeral
                if let Err(e) = joined_node.load_node_metadata() {
                    error!("Failed to load node metadata: {e}");
                }

                let this_node = joined_node.get_simple_node();
                if let Ok((_, changed)) = joined_node.cluster.assign_next(&this_node)
                    && changed
                {
                    joined_node.tick_hlc();
                }

                // Persist initial node metadata
                joined_node.save_node_metadata(false).unwrap();

                NodeState::Joined(joined_node)
            }
            (cluster_config, Some(seed_peer)) => {
                NodeState::PreJoin(PreJoinNode {
                    address: local_addr,
                    web_port: local_web_port,
                    peer_node: seed_peer,
                    cluster: cluster_config,
                    node_hlc: HLC {
                        timestamp: 0, // Initialize HLC with zero timestamp otherwise new node will be seen as source of truth for cluster state
                        counter: 0,
                    },
                })
            }
            (None, None) => {
                panic!(
                    "NodeState must be initialized with either a cluster config or a seed peer address"
                );
            }
        }
    }

    pub fn get_address(&self) -> &NodeAddress {
        match self {
            NodeState::PreJoin(pre_join_node) => &pre_join_node.address,
            NodeState::Joined(joined_node) => &joined_node.address,
            NodeState::Disconnected(disconnected_node) => &disconnected_node.address,
        }
    }

    pub fn get_cluster_config(&self) -> Option<&Cluster> {
        match self {
            NodeState::PreJoin(node) => node.cluster.as_ref(),
            NodeState::Joined(node) => Some(&node.cluster),
            NodeState::Disconnected(_) => None,
        }
    }

    pub fn get_state_name(&self) -> &str {
        match self {
            NodeState::PreJoin(_) => "PreJoin",
            NodeState::Joined(node) => {
                if node.is_syncing {
                    "Syncing"
                } else {
                    "Joined"
                }
            }
            NodeState::Disconnected(_) => "Disconnected",
        }
    }

    pub fn as_joined_node(&self) -> Option<&JoinedNode> {
        match self {
            NodeState::Joined(node) => Some(node),
            _ => None,
        }
    }

    pub fn as_joined_node_mut(&mut self) -> Option<&mut JoinedNode> {
        match self {
            NodeState::Joined(node) => Some(node),
            _ => None,
        }
    }

    pub fn is_joined_ready(&self) -> bool {
        match self {
            NodeState::Joined(node) => !(node.is_syncing || node.is_hydrating),
            _ => false,
        }
    }

    pub fn is_joined(&self) -> bool {
        matches!(self, NodeState::Joined(_))
    }

    pub fn get_simple_node_state(&self) -> Option<SimpleNodeState> {
        match self {
            NodeState::Joined(node) if node.is_hydrating => Some(SimpleNodeState::JoinedHydrating),
            NodeState::Joined(node) if node.is_syncing => Some(SimpleNodeState::JoinedSyncing),
            NodeState::Joined { .. } => Some(SimpleNodeState::Joined),
            NodeState::Disconnected { .. } => Some(SimpleNodeState::Disconnected),
            _ => None,
        }
    }

    pub fn get_simple_node(&self) -> Option<SimpleNode> {
        match self {
            NodeState::Joined(node) => Some(node.get_simple_node()),
            NodeState::PreJoin(node) => {
                Some(node.get_simple_node_as(SimpleNodeState::JoinedSyncing))
            }
            NodeState::Disconnected(node) => Some(SimpleNode {
                address: node.address.clone(),
                state: SimpleNodeState::Disconnected,
                web_port: node.web_port,
                last_seen: now_millis(),
                hlc: node.node_hlc.clone(),
                partition_item_counts: HashMap::new(),
                leading_partitions: Vec::new(),
            }),
        }
    }
}

impl DisconnectedNode {
    pub fn get_address(&self) -> &NodeAddress {
        &self.address
    }
}

impl PreJoinNode {
    pub fn get_simple_node_as(&self, snode_state: SimpleNodeState) -> SimpleNode {
        SimpleNode {
            address: self.address.clone(),
            state: snode_state,
            web_port: self.web_port,
            last_seen: now_millis(),
            hlc: self.node_hlc.clone(),
            partition_item_counts: HashMap::new(), // PreJoin node has no item counts yet
            leading_partitions: Vec::new(),
        }
    }

    pub fn get_address(&self) -> &NodeAddress {
        &self.address
    }

    pub fn get_peer_node(&self) -> &NodeAddress {
        &self.peer_node
    }

    pub async fn to_joined_state(
        &mut self,
        cluster_config: Cluster,
        store: &dyn Store,
        bus: &EventBus,
    ) -> Result<JoinedNode, NodeError> {
        let counts = store
            .partition_counts()
            .await
            .map_err(|e| NodeError::ErrorJoiningNode(e.to_string()))?;

        let partition_counts_map = DashMap::new();
        partition_counts_map.insert(cluster_config.this_node_index, counts);

        let mut joined_node = JoinedNode {
            address: self.address.clone(),
            web_port: self.web_port,
            cluster: cluster_config,
            partition_counts: partition_counts_map,
            node_hlc: Mutex::new(HLC {
                timestamp: now_millis(),
                counter: 0,
            }),
            gossip: Gossip {
                next_node_index: AtomicU8::new(0),
            },
            sync_state: SyncState {
                replica_partition_lsn: DashMap::new(),
                lsn_waiters: DashMap::new(),
                leader_confirmed_lsn: DashMap::new(),
                leader_tip_lsns: DashMap::new(),
            },
            is_syncing: true,
            is_hydrating: true,
        };

        // Load persisted metadata if not ephemeral
        joined_node.load_node_metadata()?;

        // hydrate store if needed
        bus.emit(Event::StartInMemoryHydration);

        Ok(joined_node)
    }
}

impl JoinedNode {
    pub fn get_simple_node(&self) -> SimpleNode {
        let leading_partitions = self
            .cluster
            .partition_leaders
            .iter()
            .filter(|&(_pid, &nid)| nid == self.cluster.this_node_index)
            .map(|(pid, _nid)| *pid)
            .collect();

        let state = if self.is_hydrating {
            SimpleNodeState::JoinedHydrating
        } else if self.is_syncing {
            SimpleNodeState::JoinedSyncing
        } else {
            SimpleNodeState::Joined
        };

        SimpleNode {
            address: self.address.clone(),
            state,
            web_port: self.web_port,
            last_seen: now_millis(),
            hlc: self.node_hlc.lock().unwrap().clone(),
            partition_item_counts: self
                .partition_counts
                .get(&self.cluster.this_node_index)
                .map(|c| c.clone())
                .unwrap_or_default(),
            leading_partitions,
        }
    }

    pub fn is_ready(&self) -> bool {
        !(self.is_syncing || self.is_hydrating)
    }

    pub fn get_hlc(&self) -> HLC {
        self.node_hlc.lock().unwrap().clone()
    }

    pub fn tick_hlc(&self) -> HLC {
        let mut hlc = self.node_hlc.lock().unwrap();
        *hlc = hlc.tick_hlc(now_millis());
        hlc.clone()
    }

    pub fn merge_hlc(&self, other: &HLC) {
        let mut hlc = self.node_hlc.lock().unwrap();
        *hlc = HLC::merge(&hlc, other, now_millis());
    }

    pub fn node_hlc_item_merge(&self, max_seen_hlc: HLC) {
        let mut hlc = self.node_hlc.lock().unwrap();
        *hlc = HLC::merge(&hlc, &max_seen_hlc, now_millis());
    }

    pub fn get_address(&self) -> &NodeAddress {
        &self.address
    }

    fn node_metadata_file_path(&self) -> Result<PathBuf, NodeError> {
        fs_paths::ensure_clusters_dir(self.cluster.cluster_name.as_str())
            .map_err(|e| NodeError::NodePersistenceError(e.to_string()))?;
        Ok(fs_paths::node_md_state_path(
            &self.cluster.cluster_name,
            self.cluster.this_node_index,
        ))
    }

    /// Persist information about node HLC state to disk.
    /// Purpose of this information is to allow node to resume from last known HLC state after restart.
    /// We only want to persist new version of the state when node processed new items from peers.
    pub fn save_node_metadata(&self, overwrite: bool) -> Result<(), NodeError> {
        if self.cluster.is_ephemeral {
            return Ok(()); // Do not persist state for ephemeral nodes
        }

        let path =
            fs_paths::node_md_state_path(&self.cluster.cluster_name, self.cluster.this_node_index);
        if !path.exists() || (path.exists() && overwrite) {
            let path = self.node_metadata_file_path()?;
            let state = NodeMetadata::from_joined_node(self);
            let json = serde_json::to_string_pretty(&state)
                .map_err(|e| NodeError::NodePersistenceError(e.to_string()))?;
            fs::write(path, json).map_err(|e| NodeError::NodePersistenceError(e.to_string()))
        } else {
            Ok(())
        }
    }

    pub fn load_node_metadata(&mut self) -> Result<(), NodeError> {
        if self.cluster.is_ephemeral {
            return Ok(());
        }

        if let Some(metadata) =
            NodeMetadata::load(&self.cluster.cluster_name, self.cluster.this_node_index)?
        {
            let mut hlc = self.node_hlc.lock().unwrap();
            *hlc = HLC {
                timestamp: metadata.node_hlc_timestamp,
                counter: 0,
            };

            self.sync_state.replica_partition_lsn.clear();
            if let Some(lsns) = metadata.replica_partition_lsn {
                for (k, v) in lsns {
                    self.sync_state.replica_partition_lsn.insert(k, v);
                }
            }
        }
        Ok(())
    }

    pub fn prepare_local_item(
        &self,
        storage_key: &StorageKey,
        message: Vec<u8>,
        item_state: ItemStatus,
    ) -> ItemEntry {
        let mut hlc = self.node_hlc.lock().unwrap();
        let advanced_item_hlc = HLC::merge_and_tick(&hlc, &HLC::new(), now_millis());
        *hlc = advanced_item_hlc.clone();

        let item = Item {
            message,
            status: item_state,
            hlc: advanced_item_hlc.clone(),
        };

        ItemEntry {
            storage_key: storage_key.clone(),
            item,
        }
    }

    /// Add single item, updating if it exists or is older
    async fn insert_item(
        partition: &PartitionId,
        entry: ItemEntry,
        store: &dyn Store,
        wal: &dyn Wal,
    ) -> Result<Option<(StorageKey, Vec<(PartitionId, u64)>)>, NodeError> {
        let wal_record = match entry.item.status {
            ItemStatus::Active => WalRecord::Put {
                partition: *partition,
                key: entry.storage_key.to_string().into_bytes(),
                value: entry.item.message.clone(),
                hlc: entry.item.hlc.clone().timestamp,
            },
            ItemStatus::Tombstone(_) => WalRecord::Delete {
                partition: *partition,
                key: entry.storage_key.to_string().into_bytes(),
                hlc: entry.item.hlc.clone().timestamp,
            },
        };

        let wait_requirements = match wal.append(wal_record).await {
            Ok((lsn, _lsn_offset)) => {
                vec![(*partition, lsn)]
            }
            Err(e) => {
                return Err(NodeError::ItemOperationError(e.to_string()));
            }
        };

        let maybe_existing_item = store
            .get(partition, &entry.storage_key)
            .await
            .map_err(|e| NodeError::ItemOperationError(e.to_string()))?;

        if let Some(existing_entry) = maybe_existing_item {
            if entry.item.hlc > existing_entry.item.hlc {
                let new_item = Item {
                    message: entry.item.message.clone(),
                    status: entry.item.status.clone(),
                    hlc: HLC::merge(&existing_entry.item.hlc, &entry.item.hlc, now_millis()),
                };

                store
                    .insert(partition, &entry.storage_key, new_item.clone())
                    .await
                    .map_err(|e| NodeError::ItemOperationError(e.to_string()))?;

                Ok(Some((entry.storage_key.clone(), wait_requirements)))
            } else {
                // If the new entry is older we do nothing
                Ok(None)
            }
        } else {
            // Insert new item, note it may also be a tombstone item
            store
                .insert(partition, &entry.storage_key, entry.item.clone())
                .await
                .map_err(|e| NodeError::ItemOperationError(e.to_string()))?;

            Ok(Some((entry.storage_key.clone(), wait_requirements)))
        }
    }

    /// IO-Only version of insert_items. Does NOT mutate Node state.
    /// This is used in the first phase of a two-phase update where we perform IO,
    /// then wait for replication sync, and finally update the node's HLC.
    /// Returns (AddedItems, WaitRequirements, MaxSeenHLC).
    pub async fn insert_items_io_only<I>(
        partition_count: u16,
        items: I,
        store: &dyn Store,
        wal: &dyn Wal,
    ) -> Result<(Vec<StorageKey>, Vec<(PartitionId, u64)>, HLC), NodeError>
    where
        I: IntoIterator<Item = ItemEntry>,
    {
        let mut added_items = Vec::new();
        let mut wait_requirements = Vec::new();
        let mut max_seen_hlc = HLC::new();

        for entry in items {
            // Track max HLC seen in incoming items
            max_seen_hlc = HLC::merge(&max_seen_hlc, &entry.item.hlc, 0);
            let partition =
                Cluster::hash_key_inner(partition_count, entry.storage_key.partition_key.value());

            if let Some((storage_key, reqs)) =
                JoinedNode::insert_item(&partition, entry, store, wal).await?
            {
                added_items.push(storage_key);
                wait_requirements.extend(reqs);
            }
        }

        Ok((added_items, wait_requirements, max_seen_hlc))
    }

    /// Add new items to the node. This will process the items and add them to the store and WAL.
    ///
    /// This is the preferred method for background operations like Sync where internal
    /// node state (HLC) should be updated immediately after successful insertion.
    /// This method also filters incoming items by their partition to ensure only items
    /// assigned to this node are processed.
    pub async fn insert_items<I>(
        &self,
        items: I,
        store: &dyn Store,
        wal: &dyn Wal,
    ) -> Result<(Vec<StorageKey>, Vec<(PartitionId, u64)>), NodeError>
    where
        I: IntoIterator<Item = ItemEntry>,
    {
        let partition_count = self.cluster.partition_count;

        // Efficiently filter and process items in one pass via insert_items_io_only.
        // We only pass items that are assigned to this node.
        let filtered_items = items.into_iter().filter(|entry| {
            let partition =
                Cluster::hash_key_inner(partition_count, entry.storage_key.partition_key.value());
            self.cluster.contains_partition(&partition)
        });

        let (added, reqs, max_hlc) =
            JoinedNode::insert_items_io_only(partition_count, filtered_items, store, wal).await?;

        // Update node HLC after successful insertion
        self.node_hlc_item_merge(max_hlc);
        Ok((added, reqs))
    }

    pub async fn get_item(
        &self,
        store_key: &StorageKey,
        store: &dyn Store,
    ) -> Result<Option<ItemEntry>, NodeError> {
        let partition = self.cluster.hash_key(store_key.partition_key.value());
        let maybe_item = store
            .get(&partition, store_key)
            .await
            .map_err(|e| NodeError::ItemOperationError(e.to_string()))?;

        Ok(maybe_item)
    }

    pub async fn get_items(
        &self,
        limit: usize,
        skip_null_rk: bool,
        store_key: &StorageKey,
        store: &dyn Store,
    ) -> Result<Vec<ItemEntry>, NodeError> {
        let partition = self.cluster.hash_key(store_key.partition_key.value());
        let options = crate::store::GetManyOptions {
            limit,
            skip_null_rk,
        };
        let maybe_item = store
            .get_many(&partition, store_key, options)
            .await
            .map_err(|e| NodeError::ItemOperationError(e.to_string()))?;
        Ok(maybe_item)
    }

    pub fn get_all_items_count(&self) -> usize {
        let mut unique_partitions = HashMap::<PartitionId, usize>::new();

        // Collect counts from all peers (including self if in assignments)
        for entry in self.cluster.partition_assignments.iter() {
            let assignment = &entry.1.1;
            if let cluster::AssignmentState::Joined { node, .. } = assignment {
                for (pid, count) in &node.partition_item_counts {
                    let current = unique_partitions.entry(*pid).or_insert(0);
                    if *count > *current {
                        *current = *count;
                    }
                }
            }
        }

        // Ensure our latest local counts take precedence (might be newer than last gossip)
        if let Some(local_entry) = self.partition_counts.get(&self.cluster.this_node_index) {
            for (pid, count) in local_entry.iter() {
                let current = unique_partitions.entry(*pid).or_insert(0);
                if *count > *current {
                    *current = *count;
                }
            }
        }

        unique_partitions.values().sum()
    }

    pub async fn get_local_partition_item_count(
        &self,
        store: &dyn Store,
    ) -> HashMap<PartitionId, usize> {
        store.partition_counts().await.unwrap()
    }

    pub fn update_partition_item_counts(
        &self,
        node_index: u8,
        partition_counts: HashMap<PartitionId, usize>,
    ) {
        self.partition_counts.insert(node_index, partition_counts);
    }

    pub fn push_lsn_sync(&self, partition: PartitionId, lsn: u64) {
        // Update confirmed LSN first
        self.sync_state
            .leader_confirmed_lsn
            .entry(partition)
            .and_modify(|v| {
                if *v < lsn {
                    *v = lsn
                }
            })
            .or_insert(lsn);

        // Notify watchers
        if let Some(sender) = self.sync_state.lsn_waiters.get(&partition) {
            let _ = sender.send(lsn);
        }
    }

    pub fn update_confirmed_lsn(&self, partition: PartitionId, lsn: u64) {
        self.push_lsn_sync(partition, lsn);
    }

    pub fn get_lsn_watcher(&self, partition: PartitionId) -> tokio::sync::watch::Receiver<u64> {
        let current_lsn = self
            .sync_state
            .leader_confirmed_lsn
            .get(&partition)
            .map(|v| *v.value())
            .unwrap_or(0);
        if let Some(sender) = self.sync_state.lsn_waiters.get(&partition) {
            sender.subscribe()
        } else {
            let (tx, rx) = tokio::sync::watch::channel(current_lsn);
            self.sync_state.lsn_waiters.insert(partition, tx);
            rx
        }
    }

    pub fn get_confirmed_lsn(&self, partition: PartitionId) -> u64 {
        self.sync_state
            .leader_confirmed_lsn
            .get(&partition)
            .map(|v| *v.value())
            .unwrap_or(0)
    }

    // TODO: ensure proper error
    /// Upon node restart, hydrate in-memory store by replaying WAL.
    /// Note this is only used for in-memory implementations of Store which need a re-load of data on every start
    pub async fn hydrate_store_from_wal_task(
        node_state: Arc<RwLock<NodeState>>,
        env: Arc<Env>,
    ) -> Result<(), NodeError> {
        let now = now_millis();
        let (node_address, is_cluster_ephemeral) = {
            let node_state = node_state.read().await;
            let node_address = node_state.get_address().clone();
            let is_cluster_ephemeral = if let Some(node) = node_state.as_joined_node() {
                node.cluster.is_ephemeral
            } else {
                false
            };
            (node_address, is_cluster_ephemeral)
        };

        if is_cluster_ephemeral {
            info!("node={node_address}; Skipping WAL hydration for ephemeral cluster");
            let mut node_state = node_state.write().await;
            if let Some(this_node) = node_state.as_joined_node_mut() {
                this_node.is_hydrating = false;
                let simple_node = this_node.get_simple_node();
                if this_node
                    .cluster
                    .update_assignment_state_for_this_node(&simple_node)
                {
                    this_node.tick_hlc();
                }
            }
            return Ok(());
        }

        info!("node={node_address}; Started hydrating store from WAL");
        let store = env.get_store();

        if !store.is_in_memory_store() {
            let mut node_state = node_state.write().await;
            if let Some(this_node) = node_state.as_joined_node_mut() {
                this_node.is_hydrating = false;
                let simple_node = this_node.get_simple_node();
                if this_node
                    .cluster
                    .update_assignment_state_for_this_node(&simple_node)
                {
                    this_node.tick_hlc();
                }
            }
            return Ok(());
        }

        let partitions = {
            let node_state = node_state.read().await;
            if let Some(this_node) = node_state.as_joined_node() {
                this_node
                    .cluster
                    .get_assigned_partitions(this_node.cluster.this_node_index)
                    .unwrap_or_default()
            } else {
                vec![]
            }
        };

        let mut hydration_tasks = Vec::new();

        for partition in partitions {
            let env = env.clone();
            let node_address = node_address.clone();
            let handle = tokio::task::spawn_blocking(move || {
                let rt = tokio::runtime::Handle::current();
                let wal = env.get_wal();
                let store = env.get_store();
                let lsn_accum = 0;
                let offset_accum = 0;

                // stream_from is async but returns a sync iterator
                let stream =
                    rt.block_on(wal.stream_from(lsn_accum, offset_accum, partition, false));

                for record_result in stream {
                    match record_result {
                        Ok((record, _offset)) => {
                            let framed_item: FramedWalRecordItem = record.into();
                            let storage_key: StorageKey =
                                match String::from_utf8(framed_item.key_bytes)
                                    .map_err(|e| e.to_string())
                                    .and_then(|s| {
                                        s.parse().map_err(|e: DataStoreError| e.to_string())
                                    }) {
                                    Ok(s) => s,
                                    Err(e) => {
                                        error!(
                                            "node={node_address}; Error parsing storage key: {e}"
                                        );
                                        continue;
                                    }
                                };

                            let result = rt.block_on(store.insert(
                                &partition,
                                &storage_key,
                                framed_item.item,
                            ));

                            if let Err(e) = result {
                                error!("node={node_address}; Error inserting item: {e}");
                                continue;
                            }
                        }
                        Err(e) => {
                            error!("node={node_address}; Error reading from WAL: {e}");
                            break;
                        }
                    }
                }
                Ok::<(), NodeError>(())
            });
            hydration_tasks.push(handle);
        }

        let results = futures::future::join_all(hydration_tasks).await;
        for result in results {
            match result {
                Ok(inner_result) => inner_result?,
                Err(e) => {
                    return Err(NodeError::ItemOperationError(format!(
                        "Hydration task panicked: {e}"
                    )));
                }
            }
        }

        let mut node_state = node_state.write().await;
        if let Some(this_node) = node_state.as_joined_node_mut() {
            this_node.is_hydrating = false;
            // TODO: this pattern is several places, consider simplifying it
            let simple_node = this_node.get_simple_node();
            if this_node
                .cluster
                .update_assignment_state_for_this_node(&simple_node)
            {
                this_node.tick_hlc();
            }
        }

        let elapsed = (now_millis() - now) / 1000;
        info!("node={node_address}; Finished hydrating store from WAL in {elapsed} seconds");
        Ok(())
    }
}
