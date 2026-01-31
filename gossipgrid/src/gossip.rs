use crate::clock::HLC;
use crate::cluster::{self, Cluster, ClusterMetadata};
use crate::env::Env;

use crate::clock::now_millis;
use crate::node::{self, NodeAddress, NodeState, PreJoinNode, SimpleNode};
use crate::util;
use bincode::{Decode, Encode};
use crossbeam_channel::Sender;
use laminar::{Packet, SocketEvent};
use log::{debug, error, info, warn};

use std::cmp::min;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;
use stdext::function_name;
use tokio::sync::mpsc::Receiver;

use tokio::sync::RwLock;

use tokio::time::sleep;

pub struct Gossip {
    pub next_node_index: AtomicU8,
}

impl Gossip {
    fn next_node(&self, other_peers: &HashMap<u8, SimpleNode>) -> Option<u8> {
        let mut nodes: Vec<u8> = other_peers.keys().cloned().collect();
        nodes.sort();

        let selected_peer = nodes
            .get(self.next_node_index.load(Ordering::Relaxed) as usize % other_peers.len())
            .cloned()?;

        self.next_node_index.fetch_add(1, Ordering::Relaxed);
        Some(selected_peer)
    }

    pub fn next_nodes(
        &mut self,
        count: u8,
        this_node_idx: u8,
        other_peers: HashMap<u8, SimpleNode>,
    ) -> Vec<u8> {
        let peer_size = other_peers.len();
        if peer_size == 0 {
            return vec![];
        }
        let mut selected_next = vec![];

        for _ in 0..count {
            selected_next.push(self.next_node(&other_peers).unwrap_or(this_node_idx));
        }

        selected_next
    }
}

const GOSSIP_INTERVAL: Duration = Duration::from_secs(3);
const FAILURE_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Encode, Decode)]
pub struct NodeJoinAck {
    pub cluster_config: Cluster,
    pub node: SimpleNode,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct NodeJoinRequest {
    pub node: SimpleNode,
    pub peer_address: String,
    pub node_cluster_metadata: Option<NodeClusterMetadata>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct NodeClusterMetadata {
    pub node_index: u8,
    pub owned_partitions: Vec<u16>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct GossipMembershipMessage {
    pub node: SimpleNode,
    pub other_peers: HashMap<u8, SimpleNode>,
    pub cluster_size: u8,
    pub config_hlc: HLC,
}

impl fmt::Display for GossipMembershipMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GossipMembershipMessage {{ node: {}, other_peers: {}, cluster_size: {}, config_hlc: {:?} }}",
            self.node,
            util::DisplayVec(
                self.other_peers
                    .iter()
                    .map(|(k, v)| format!("{}:{}", k, v))
                    .collect()
            ),
            self.cluster_size,
            self.config_hlc,
        )
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum UdpWireMessage {
    Gossip(GossipMembershipMessage),
    GossipJoinRequest(NodeJoinRequest),
    GossipJoinRequestAck(NodeJoinAck),
}

impl fmt::Display for UdpWireMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UdpWireMessage::Gossip(msg) => write!(f, "Gossip({})", msg),
            UdpWireMessage::GossipJoinRequest(msg) => write!(f, "GossipJoinRequest({:?})", msg),
            UdpWireMessage::GossipJoinRequestAck(msg) => {
                write!(f, "GossipJoinRequestAck({:?})", msg)
            }
        }
    }
}

pub async fn handle_messages_task(
    local_addr: NodeAddress,
    mut receiver: Receiver<SocketEvent>,
    sender: Arc<Sender<Packet>>,
    node_state: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("node={local_addr}; Shutting down message handler");
                break;
            }
            event = receiver.recv() => {
                match event {
                    Some(event) => {
                        match event {
            SocketEvent::Packet(packet) => {
                let src = packet.addr();
                let msg = packet.payload();

                if let Ok((wire, _)) = bincode::decode_from_slice::<UdpWireMessage, _>(
                    msg,
                    bincode::config::standard(),
                ) {
                    match wire {
                        UdpWireMessage::Gossip(message) => {
                            handle_membership_message(
                                &src,
                                message,
                                node_state.clone(),
                            )
                            .await;
                        }
                        UdpWireMessage::GossipJoinRequest(message) => {
                            handle_join_request(
                                &src,
                                sender.clone(),
                                message,
                                node_state.clone(),
                            )
                            .await;
                        }
                        UdpWireMessage::GossipJoinRequestAck(message) => {
                            handle_join_ack(&src, message, node_state.clone(), env.clone()).await;
                        }
                    }
                } else {
                    error!("Failed to decode wire message from {src}");
                }
            }
            SocketEvent::Timeout(address) => {
                warn!("node={}; Client timed out: {}", &local_addr, address);
            }
            SocketEvent::Disconnect(address) => {
                error!("node={}; Client disconnected: {}", &local_addr, address);
            }
            SocketEvent::Connect(address) => {
                info!("node={}; Client connected: {}", &local_addr, address);
            }
        }
                    }
                    None => {
                        break;
                    }
                }
            }
        }
    }
}

async fn send_join_request(
    join_node: &NodeAddress,
    node_state: &PreJoinNode,
    sender: &Sender<Packet>,
) {
    let cluster_metadata = if let Some(cluster) = &node_state.cluster {
        let cluster_metadata: ClusterMetadata = cluster.clone().into();
        Some(NodeClusterMetadata {
            node_index: cluster.this_node_index,
            owned_partitions: cluster_metadata
                .partition_assignments
                .get(&cluster.this_node_index)
                .unwrap_or(&vec![])
                .iter()
                .map(|x| x.value())
                .collect(),
        })
    } else {
        None
    };

    let msg = UdpWireMessage::GossipJoinRequest(NodeJoinRequest {
        node: node_state.get_simple_node_as(node::SimpleNodeState::JoinedSyncing),
        peer_address: join_node.as_str().to_string(),
        node_cluster_metadata: cluster_metadata,
    });

    if let Ok(encoded) = bincode::encode_to_vec(&msg, bincode::config::standard()) {
        let packet =
            laminar::Packet::reliable_unordered(join_node.as_str().parse().unwrap(), encoded);
        match sender.send(packet) {
            Ok(_) => {}
            Err(e) => {
                error!(
                    "node={}; Failed to send JoinMessage to {}: {}",
                    node_state.get_address(),
                    join_node,
                    e
                );
            }
        }
    } else {
        error!(
            "node={}; Failed to encode JoinMessage; message not sent",
            node_state.get_address().as_str()
        );
    }
}

pub async fn send_join_request_ack(
    peer_node: &SimpleNode,
    local_addr: &NodeAddress,
    sender: Arc<Sender<Packet>>,
    state: Arc<RwLock<NodeState>>,
) {
    info!("node={local_addr}; Sending Join Request ACK");

    let node_state = state.read().await;
    match &*node_state {
        node::NodeState::Joined(this_node) => {
            // Create a copy of cluster config and assign new node index
            let mut new_cluster_config = this_node.cluster.clone();
            let node_index = this_node.cluster.get_node_index(&peer_node.address);

            match node_index {
                Some(n) => {
                    new_cluster_config.this_node_index = n;
                }
                None => {
                    error!(
                        "node={}; Failed to assign new node in cluster config",
                        this_node.get_address(),
                    );
                    return;
                }
            }

            let message = UdpWireMessage::GossipJoinRequestAck(NodeJoinAck {
                cluster_config: new_cluster_config,
                node: this_node.get_simple_node(),
            });

            if let Ok(encoded_ack) = bincode::encode_to_vec(&message, bincode::config::standard()) {
                let packet = laminar::Packet::reliable_unordered(
                    peer_node.address.as_str().parse().unwrap(),
                    encoded_ack,
                );
                match sender.send(packet) {
                    Ok(_) => {}
                    Err(e) => {
                        error!(
                            "node={}; Failed to send GossipJoinAck to {}: {}",
                            local_addr,
                            peer_node.address.as_str(),
                            e
                        );
                        return;
                    }
                }
            } else {
                error!("node={local_addr}; Failed to encode GossipJoinAck; message not sent");
                return;
            }

            info!(
                "node_addr={}; sent {:?} to {:?}",
                &local_addr, &message, &peer_node.address
            );
        }
        state => {
            error!(
                "node={}; {}; Cannot handle operation in current state: {}",
                state.get_address(),
                function_name!(),
                state.get_state_name()
            );
        }
    }
}

async fn handle_join_request(
    src: &SocketAddr,
    sender: Arc<Sender<Packet>>,
    message: NodeJoinRequest,
    state: Arc<RwLock<NodeState>>,
) {
    let node_address = {
        let node_state = state.read().await;
        node_state.get_address().clone()
    };

    info!(
        "node={}; Join Message Received {:?} from {:?}:{:?}",
        node_address.as_str(),
        message,
        src.ip(),
        src.port()
    );

    let cluster_updated = {
        let mut node_state = state.write().await;
        if let node::NodeState::Joined(this_node) = &mut *node_state {
            let is_member = this_node.cluster.get_node_index(&message.node.address);

            if let Some(node_id) = is_member {
                warn!(
                    "node={}; Remote node {} is already a known member, but may be re-joining",
                    &this_node.address, &message.node.address,
                );

                let mut updated_node = message.node.clone();
                updated_node.last_seen = now_millis();

                let changed = this_node
                    .cluster
                    .assign_node(&node_id, &updated_node)
                    .unwrap_or_default();
                if changed {
                    this_node.tick_hlc();
                }
                true
            } else if this_node.cluster.is_fully_assigned() {
                error!("node={}; Cluster is full, cannot join", &this_node.address);
                false
            } else {
                info!(
                    "node={}; New remote node {:?} joining cluster",
                    &this_node.address, &message.node
                );
                let mut updated_node = message.node.clone();
                updated_node.last_seen = now_millis();

                if let Some(join_metadata) = message.node_cluster_metadata {
                    if let Some(partitions) = this_node
                        .cluster
                        .get_assigned_partitions(join_metadata.node_index)
                        && partitions.iter().map(|x| x.0).collect::<Vec<_>>()
                            == join_metadata.owned_partitions
                    {
                        let changed = this_node
                            .cluster
                            .assign_node(&join_metadata.node_index, &updated_node)
                            .unwrap_or_default();
                        if changed {
                            this_node.tick_hlc();
                        }
                        true
                    } else {
                        error!(
                            "node={}; Cannot join, metadata provided but not matching node index",
                            &this_node.address
                        );
                        false
                    }
                } else {
                    // no cluster metadata provided
                    if let Ok((_idx, changed)) = this_node.cluster.assign_next(&updated_node) {
                        if changed {
                            this_node.tick_hlc();
                        }
                        true
                    } else {
                        false
                    }
                }
            }
        } else {
            error!(
                "node={}; {}; Cannot handle operation in current state: {}",
                node_state.get_address(),
                function_name!(),
                node_state.get_state_name()
            );
            return;
        }
    };

    // Respond with ack message
    if cluster_updated {
        send_join_request_ack(&message.node, &node_address, sender.clone(), state.clone()).await;
    }
}

async fn handle_join_ack(
    src: &SocketAddr,
    message: NodeJoinAck,
    state: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) {
    let mut node_state = state.write().await;
    info!(
        "node={}; Message Received {:?} from {:?}:{:?}",
        node_state.get_address(),
        &message,
        src.ip(),
        src.port()
    );

    match &mut *node_state {
        // If we are PreJoin and received a message, this means we got acknowledgement
        // At this stage we transition node to Joined state as well as inherit all cluster state information
        node::NodeState::PreJoin(this_node) => {
            this_node.node_hlc = HLC::merge(&this_node.node_hlc, &message.node.hlc, now_millis());

            let joined_node = match this_node
                .to_joined_state(
                    message.cluster_config.clone(),
                    env.get_store(),
                    env.get_event_bus(),
                )
                .await
            {
                Ok(n) => {
                    info!(
                        "node={}; Successfully transitioned to Joined state",
                        this_node.get_address()
                    );
                    n
                }
                Err(e) => {
                    error!(
                        "node={}; Failed to transition to Joined state due to error: {e}",
                        this_node.get_address(),
                    );
                    return;
                }
            };

            // Ensure cluster config and node metadata is persisted on the node local file system (if not ehphemeral)
            joined_node.cluster.save().unwrap();
            joined_node.save_node_metadata(false).unwrap();

            // update node_state to joined state
            *node_state = NodeState::Joined(joined_node);

            // Ensure our own state is correctly reflected as Syncing in our owned partition assignments
            if let Some(joined_node) = node_state.as_joined_node_mut() {
                let simple_node = joined_node.get_simple_node();
                let state_changed = joined_node
                    .cluster
                    .update_assignment_state_for_this_node(&simple_node);

                let membership_changed = joined_node
                    .cluster
                    .update_cluster_membership(
                        &message.node,
                        &message.cluster_config.other_peers(),
                        message.cluster_config.cluster_size,
                        &message.cluster_config.config_hlc,
                    )
                    .unwrap_or_default();

                if state_changed || membership_changed {
                    joined_node.tick_hlc();
                }
            }
        }
        state => {
            error!(
                "node={}; fn={}; Cannot handle operation in current state: {}",
                src,
                function_name!(),
                state.get_state_name()
            );
        }
    }
}

async fn handle_membership_message(
    src: &SocketAddr,
    message: GossipMembershipMessage,
    state: Arc<RwLock<NodeState>>,
) {
    let mut node_state = state.write().await;
    info!(
        "node={}; Message Received {} from {}:{}",
        node_state.get_address(),
        message,
        src.ip(),
        src.port()
    );

    match &mut *node_state {
        node::NodeState::Joined(this_node) => {
            // 1. Update cluster membership
            // This is called for every gossip message. Inside update_cluster_membership,
            // we check the sender's HLC against our local knowledge for that specific sender
            // to decide whether to update its state.
            let changed = this_node
                .cluster
                .update_cluster_membership(
                    &message.node,
                    &message.other_peers,
                    message.cluster_size,
                    &message.config_hlc,
                )
                .unwrap_or_default();

            // 2. Merge local HLC with the incoming message's HLC to keep clocks synchronized.
            this_node.merge_hlc(&message.node.hlc);

            // 3. If membership or leadership changed, tick HLC to propagate the new state.
            if changed {
                this_node.tick_hlc();
            }
        }
        state => {
            error!(
                "node={}; fn={}; Cannot handle operation in current state: {}",
                src,
                function_name!(),
                state.get_state_name()
            );
        }
    }
}

pub async fn send_membership_gossip_task(
    node_address: NodeAddress,
    sender: Arc<Sender<Packet>>,
    node_state: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    loop {
        {
            let mut node_state = node_state.write().await;
            match &mut *node_state {
                node::NodeState::Joined(this_node) => {
                    let now_millis = now_millis();

                    // Remove peers that haven't responded within FAILURE_TIMEOUT
                    let peers = this_node.cluster.get_all_peers_cloned();
                    let local_addr = this_node.get_address();
                    let mut nodes_to_unassign: Vec<u8> = vec![];
                    for peer in peers.iter() {
                        if &peer.1.address != local_addr
                            && now_millis - peer.1.last_seen > FAILURE_TIMEOUT.as_millis() as u64
                            && !this_node.cluster.other_peers().is_empty()
                        {
                            nodes_to_unassign.push(*peer.0);
                            info!(
                                "node={}; Removed peer: {} from known peers",
                                &this_node.get_address(),
                                &peer.0
                            );
                        }
                    }

                    // Mark unreachable nodes
                    let mut state_changed = false;
                    for idx in nodes_to_unassign {
                        let hlc = this_node.get_hlc();
                        if this_node
                            .cluster
                            .unassign_node(&idx, &hlc)
                            .unwrap_or_default()
                        {
                            state_changed = true;
                        }
                    }

                    if state_changed {
                        this_node.tick_hlc();
                    }
                }
                node::NodeState::PreJoin(this_node) => {
                    // Send join message to the peer node
                    send_join_request(&this_node.peer_node, this_node, &sender).await;

                    info!("node={}; Node PreJoin", this_node.get_address());
                }
                _ => {
                    error!(
                        "node={}; fn={}; Cannot send gossip in current state:",
                        node_state.get_address(),
                        function_name!(),
                    );
                    return;
                }
            }
        };

        let next_nodes = {
            let mut node_state = node_state.write().await;
            if let node::NodeState::Joined(this_node) = &mut *node_state {
                let gossip_count = min(2, this_node.cluster.replication_factor);
                // TODO: not just next nodes, but nodes that share partitions with this one
                this_node.gossip.next_nodes(
                    gossip_count,
                    this_node.cluster.this_node_index,
                    this_node.cluster.other_peers(),
                )
            } else {
                vec![]
            }
        };

        let partition_counts = env
            .get_store()
            .partition_counts()
            .await
            .unwrap_or(HashMap::new());

        {
            let mut node_state = node_state.write().await;
            if let node::NodeState::Joined(this_node) = &mut *node_state {
                this_node
                    .partition_counts
                    .insert(this_node.cluster.this_node_index, partition_counts);

                // Ensure local cluster view has the updated counts for this node
                let simple_node = this_node.get_simple_node();
                if this_node
                    .cluster
                    .update_assignment_state_for_this_node(&simple_node)
                {
                    this_node.tick_hlc();
                }
            }
        }

        {
            let node_state = node_state.read().await;
            if let node::NodeState::Joined(this_node) = &*node_state {
                for peer_idx in next_nodes.into_iter() {
                    // Resolve address for peer_idx
                    let peer_address = this_node
                        .cluster
                        .partition_assignments
                        .get(&peer_idx)
                        .and_then(|(_, assignment)| match assignment {
                            cluster::AssignmentState::Joined { node, .. } => {
                                Some(node.address.as_str().to_string())
                            }
                            cluster::AssignmentState::JoinedSyncing { node, .. } => {
                                Some(node.address.as_str().to_string())
                            }
                            cluster::AssignmentState::Disconnected { node, .. } => {
                                Some(node.address.as_str().to_string())
                            }
                            _ => None,
                        });

                    let peer_dest_addr = match peer_address {
                        Some(addr) => addr,
                        None => {
                            warn!(
                                "node={}; Could not resolve address for node index {}",
                                this_node.get_address(),
                                peer_idx
                            );
                            continue;
                        }
                    };

                    info!(
                        "node={}; selected node: {} ({})",
                        this_node.get_address(),
                        peer_idx,
                        &peer_dest_addr
                    );

                    let msg = UdpWireMessage::Gossip(GossipMembershipMessage {
                        node: node_state.get_simple_node().unwrap(),
                        other_peers: this_node.cluster.other_peers(),
                        cluster_size: this_node.cluster.cluster_size,
                        config_hlc: this_node.cluster.config_hlc.clone(),
                    });

                    if let Ok(encoded_gossip) =
                        bincode::encode_to_vec(&msg, bincode::config::standard())
                    {
                        let packet = laminar::Packet::reliable_ordered(
                            peer_dest_addr.parse().unwrap(),
                            encoded_gossip,
                            None,
                        );
                        match sender.send(packet) {
                            Ok(_) => {}
                            Err(e) => {
                                error!(
                                    "node={}; Failed to send GossipMessage to {}: {}",
                                    node_state.get_address(),
                                    peer_dest_addr,
                                    e
                                );
                                return;
                            }
                        }
                    } else {
                        error!(
                            "node={}; Failed to encode GossipMessage; message not sent",
                            node_state.get_address()
                        );
                        return;
                    }

                    info!(
                        "node={:?}; sent {} to {:?}",
                        node_state.get_address(),
                        msg,
                        &peer_dest_addr,
                    );
                }

                let node_address = this_node.get_address();

                info!(
                    "node={}; Cluster health: {:?}",
                    node_address,
                    &this_node.cluster.get_cluster_health()
                );
                info!(
                    "node={}; IsSyncing: {}, IsHydrating: {}",
                    node_address, &this_node.is_syncing, &this_node.is_hydrating
                );
                info!(
                    "node={}; Node Index: {}, State: {}",
                    node_address,
                    &this_node.cluster.this_node_index,
                    &node_state.get_state_name()
                );
                debug!(
                    "node={}; Leader partition LSNs: {:?}",
                    node_address,
                    &this_node
                        .sync_state
                        .leader_confirmed_lsn
                        .iter()
                        .map(|entry| format!("{}:{:?}", entry.key(), entry.value()))
                        .collect::<Vec<_>>(),
                );
                debug!(
                    "node={}; Replica partition LSNs: {:?}",
                    node_address,
                    &this_node
                        .sync_state
                        .replica_partition_lsn
                        .iter()
                        .map(|entry| format!("{}:{:?}", entry.key(), entry.value()))
                        .collect::<Vec<_>>(),
                );
                info!(
                    "node={}; Known peers: {:?}",
                    node_address,
                    this_node
                        .cluster
                        .other_peers()
                        .iter()
                        .map(|(k, v)| format!("{}:{}", k, v))
                        .collect::<Vec<_>>(),
                );
                debug!(
                    "node={}; Total item count: {}",
                    node_address,
                    &this_node.get_all_items_count()
                );
                info!(
                    "node={}; Number of leader partitions: {}",
                    node_address,
                    &this_node.cluster.get_this_leaders_partitions().len(),
                );
            }
        };

        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("node={}; Shutting down gossip sender", &node_address);
                break;
            }
            _ = sleep(GOSSIP_INTERVAL) => {}
        }
    }
}
