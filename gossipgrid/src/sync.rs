use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::clock::now_millis;
use crate::cluster::PartitionId;
use crate::env::Env;
use crate::item::ItemEntry;
use crate::node::{self, NodeAddress, NodeId, NodeState};
use crate::store::{DataStoreError, StorageKey};
use crate::wal::{FramedWalRecordItem, WalRecord};
use bincode::{Decode, Encode};
use dashmap::DashMap;
use gossipgrid_wal::{FramedWalRecord, WalPosition};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::time::timeout;

const SERVER_NO_RECORDS_FOUND_TIMEOUT_MILLIS: u64 = 500;
// delay before trying another scan for more items
const SERVER_RECORD_FOUND_DELAY_MILLIS: u64 = 10;
const SERVER_SCAN_TIMEOUT_MILLIS: u16 = 500;
const SERVER_PAYLOAD_SIZE_MAX_BYTES: u64 = 1024 * 1024;
const SERVER_SCAN_TIMEOUT_SYNC_MILLIS: u16 = 1000;
const SERVER_PAYLOAD_SIZE_MAX_SYNC_BYTES: u64 = 1024 * 1024 * 10;
const CLIENT_NOTHING_TO_REQUEST_DELAY_MILLIS: u64 = 50;
// will make for more responsive replication, do not reduce to 0 it will degrade performance
const CLIENT_MAIN_LOOP_DELAY_MILLIS: u64 = 3;
const CLIENT_REQUEST_TIMEOUT_MILLIS: u64 = 1000;
const SYNC_FLAG_LSN_DELTA: u64 = 100;
const CLIENT_WAITING_FOR_HYDRATION_COMPLETED_MILLIS: u64 = 1000;

// TODO: rename these fields to have better meaning
pub struct SyncState {
    // replicas knowledge on Leader's LSN and the WAL offset
    pub replica_partition_lsn: DashMap<PartitionId, LsnOffset>,
    // sync channels to communicate sync completion between leader and replicas
    pub lsn_waiters: DashMap<PartitionId, tokio::sync::watch::Sender<u64>>,
    // Max seen LSNs for each node/partition combination this leader is aware of
    pub leader_confirmed_lsn: DashMap<PartitionId, u64>,
    // Repicas knowledge on Leader's tip LSN
    pub leader_tip_lsns: DashMap<PartitionId, u64>,
}

impl SyncState {
    fn leader_confirmed_lsn_for_partitions(
        &self,
        partition_ids: &[PartitionId],
    ) -> HashMap<PartitionId, u64> {
        let mut lsns = HashMap::new();
        for partition_id in partition_ids {
            if let Some(lsn) = self.leader_confirmed_lsn.get(partition_id) {
                lsns.insert(*partition_id, *lsn);
            }
        }
        lsns
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LsnOffset {
    pub lsn: u64,
    pub position: WalPosition,
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct SyncRequest {
    pub partitions: Vec<SyncRequestPartition>,
    pub scan_timeout_millis: u16,
    pub max_size_bytes: u64,
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct SyncRequestPartition {
    pub partition: PartitionId,
    pub lsn: u64,
    pub position: WalPosition,
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct SyncResponseWalRecord {
    pub partition: PartitionId,
    pub item: ItemEntry,
    pub lsn: u64,
    pub position: WalPosition,
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct SyncResponse {
    pub current_lsns: HashMap<PartitionId, u64>,
    pub records: Vec<SyncResponseWalRecord>,
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct SyncResponseAck {
    pub partition_lsns: HashMap<PartitionId, u64>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum TcpWireMessage {
    Request(SyncRequest),
    Response(SyncResponse),
    ResponseAck(SyncResponseAck),
}

pub async fn read_frame(stream: &mut TcpStream) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];

    // read_exact returns Err on EOF; treat that as "no frame"
    if let Err(e) = stream.read_exact(&mut len_buf).await {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            return Ok(None);
        } else {
            return Err(e);
        }
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(Some(buf))
}

pub async fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> io::Result<()> {
    let len = payload.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(payload).await?;
    stream.flush().await?;
    Ok(())
}

pub async fn client_send_sync_request_task(
    node_address: NodeAddress,
    node_state: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    let connection_cache = Arc::new(Mutex::new(HashMap::<NodeAddress, TcpStream>::new()));
    let active_sync_nodes = Arc::new(Mutex::new(HashSet::<NodeAddress>::new()));
    let last_failure_log_time = Arc::new(DashMap::<NodeAddress, u64>::new());

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("node={}; shutting down sync poller", &node_address);
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(CLIENT_MAIN_LOOP_DELAY_MILLIS)) => {}
        }

        let requests = {
            let mut requests: HashMap<NodeAddress, SyncRequest> = HashMap::new();
            let mut leader_partitions: HashMap<NodeId, Vec<PartitionId>> = HashMap::new();

            let state = node_state.read().await;
            if let NodeState::Joined(this_node) = &*state {
                // await hydration to complete first
                if this_node.is_hydrating {
                    info!(
                        "node={}; is_hydrating={}; waiting for hydration to complete",
                        node_address, this_node.is_hydrating
                    );
                    tokio::time::sleep(Duration::from_millis(
                        CLIENT_WAITING_FOR_HYDRATION_COMPLETED_MILLIS,
                    ))
                    .await;
                    continue;
                }

                if let Some(assignments) = this_node
                    .cluster
                    .partition_assignments
                    .get(&this_node.cluster.this_node_index)
                {
                    for partition_id in &assignments.0 {
                        if let Some(leader_id) =
                            this_node.cluster.partition_leaders.get(partition_id)
                            && *leader_id != this_node.cluster.this_node_index
                        {
                            leader_partitions
                                .entry(*leader_id)
                                .or_default()
                                .push(*partition_id);
                        }
                    }
                }

                let mut scan_timeout_millis = SERVER_SCAN_TIMEOUT_MILLIS;
                let mut max_size_bytes = SERVER_PAYLOAD_SIZE_MAX_BYTES;
                if this_node.is_syncing {
                    scan_timeout_millis = SERVER_SCAN_TIMEOUT_SYNC_MILLIS;
                    max_size_bytes = SERVER_PAYLOAD_SIZE_MAX_SYNC_BYTES;
                }

                for (leader_id, partitions) in leader_partitions {
                    if let Some(leader_node) = this_node.cluster.other_peers().get(&leader_id) {
                        for partition in partitions {
                            let lsn_offset = this_node
                                .sync_state
                                .replica_partition_lsn
                                .get(&partition)
                                .map(|v| v.value().clone())
                                .unwrap_or_default();

                            if requests.contains_key(&leader_node.address) {
                                let request = requests.get_mut(&leader_node.address).unwrap();
                                let request_partition = SyncRequestPartition {
                                    partition,
                                    lsn: lsn_offset.lsn,
                                    position: lsn_offset.position,
                                };
                                request.partitions.push(request_partition);
                            } else {
                                requests.insert(
                                    leader_node.address.clone(),
                                    SyncRequest {
                                        partitions: vec![SyncRequestPartition {
                                            partition,
                                            lsn: lsn_offset.lsn,
                                            position: lsn_offset.position,
                                        }],
                                        scan_timeout_millis,
                                        max_size_bytes,
                                    },
                                );
                            }
                        }
                    }
                }
            }
            requests
        };

        if requests.is_empty() {
            tokio::time::sleep(Duration::from_millis(
                CLIENT_NOTHING_TO_REQUEST_DELAY_MILLIS,
            ))
            .await;
            continue;
        }

        for (target_addr, req) in requests {
            let mut active = active_sync_nodes.lock().await;
            if active.contains(&target_addr) {
                continue;
            }
            active.insert(target_addr.clone());
            drop(active);

            let state = node_state.clone();
            let env = env.clone();
            let active_sync_nodes = active_sync_nodes.clone();
            let target_addr_clone = target_addr.clone();
            let req = req.clone();
            let cache = connection_cache.clone();
            let local_node_address = node_address.clone();
            let last_failure_log_time = last_failure_log_time.clone();

            // We handle the connection cache within the task now to avoid lock contention
            tokio::spawn(async move {
                let stream = {
                    let mut c = cache.lock().await;
                    c.remove(&target_addr_clone)
                };

                let mut stream = if let Some(s) = stream {
                    s
                } else {
                    let addr = SocketAddr::new(
                        target_addr_clone.ip.as_str().parse().unwrap(),
                        target_addr_clone.port,
                    );
                    match TcpStream::connect(&addr).await {
                        Ok(s) => {
                            let _ = s.set_nodelay(true);
                            last_failure_log_time.remove(&target_addr_clone);
                            s
                        }
                        Err(e) => {
                            let now = now_millis();
                            let should_log = match last_failure_log_time.get(&target_addr_clone) {
                                Some(last_log) => now - *last_log > 5000,
                                None => true,
                            };

                            if should_log {
                                warn!(
                                    "node={}; failed to connect to {}, error: {}",
                                    local_node_address, &target_addr_clone, &e
                                );
                                last_failure_log_time.insert(target_addr_clone.clone(), now);
                            }

                            active_sync_nodes.lock().await.remove(&target_addr_clone);
                            return;
                        }
                    }
                };

                let mut success = false;
                let request = TcpWireMessage::Request(req);
                if let Ok(encoded_req) =
                    bincode::encode_to_vec(&request, bincode::config::standard())
                    && write_frame(&mut stream, &encoded_req).await.is_ok()
                    && let Ok(Ok(Some(buf))) = timeout(
                        Duration::from_millis(CLIENT_REQUEST_TIMEOUT_MILLIS),
                        read_frame(&mut stream),
                    )
                    .await
                    && let Ok((TcpWireMessage::Response(response), _)) =
                        bincode::decode_from_slice::<TcpWireMessage, _>(
                            &buf,
                            bincode::config::standard(),
                        )
                {
                    success = true;
                    let server_addr = stream.peer_addr().unwrap();
                    let response_ack_map = client_handle_sync_response(
                        &server_addr,
                        response.clone(),
                        state.clone(),
                        env.clone(),
                    )
                    .await;

                    if !response_ack_map.is_empty() {
                        let response_ack = TcpWireMessage::ResponseAck(SyncResponseAck {
                            partition_lsns: response_ack_map,
                        });
                        if let Ok(encoded_ack) =
                            bincode::encode_to_vec(&response_ack, bincode::config::standard())
                        {
                            if write_frame(&mut stream, &encoded_ack).await.is_err() {
                                success = false;
                            }
                        } else {
                            success = false;
                        }
                    }

                    evaluate_sync_toggle(state).await;
                }

                // Return the stream to the cache only if the operation was successful
                if success {
                    cache.lock().await.insert(target_addr_clone.clone(), stream);
                }
                active_sync_nodes.lock().await.remove(&target_addr_clone);
            });
        }
    }
}

async fn evaluate_sync_toggle(state: Arc<RwLock<NodeState>>) {
    // 1. Initial check with read lock to see if we even need to bother
    {
        let node_state = state.read().await;
        let this_node = if let node::NodeState::Joined(this_node) = &*node_state {
            if !this_node.is_syncing {
                return;
            }
            this_node
        } else {
            return;
        };

        if !is_syncing_complete(this_node) {
            return;
        }
    }

    // 2. Only if we think sync is complete, take the write lock to toggle it off
    let mut node_state = state.write().await;
    let this_node = if let node::NodeState::Joined(this_node) = &mut *node_state {
        if !this_node.is_syncing {
            return;
        }
        this_node
    } else {
        return;
    };

    // Re-check under write lock to be safe
    if !is_syncing_complete(this_node) {
        return;
    }

    info!(
        "node={}; Syncing complete, toggling off syncing mode",
        this_node.get_address()
    );
    this_node.is_syncing = false;
    if let node::NodeState::Joined(this_node) = &mut *node_state {
        // Ensure our own state is correctly reflected as syncing in our owned partition assignments
        let simple_node = this_node.get_simple_node();
        if this_node
            .cluster
            .update_assignment_state_for_this_node(&simple_node)
        {
            this_node.tick_hlc();
        }
    }
}

fn is_syncing_complete(this_node: &node::JoinedNode) -> bool {
    let Some(assignments) = this_node
        .cluster
        .partition_assignments
        .get(&this_node.cluster.this_node_index)
    else {
        return true;
    };

    for partition_id in &assignments.0 {
        if let Some(leader_id) = this_node.cluster.partition_leaders.get(partition_id) {
            // Check if we are the leader, if so we don't need to sync
            if *leader_id == this_node.cluster.this_node_index {
                continue;
            }

            let replica_lsn = this_node
                .sync_state
                .replica_partition_lsn
                .get(partition_id)
                .map(|v| v.value().lsn)
                .unwrap_or(0);

            let leader_tip_lsn = this_node
                .sync_state
                .leader_tip_lsns
                .get(partition_id)
                .map(|v| *v.value())
                .unwrap_or(u64::MAX); // If we don't know the leader LSN, assume we are behind

            if leader_tip_lsn > replica_lsn && leader_tip_lsn - replica_lsn > SYNC_FLAG_LSN_DELTA {
                return false;
            }
        } else {
            // If we don't know the leader, we can't be sure we are caught up
            return false;
        }
    }
    true
}

async fn client_handle_sync_response(
    src: &SocketAddr,
    response: SyncResponse,
    state: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> HashMap<PartitionId, u64> {
    let node_address = {
        let node_state = state.read().await;
        node_state.get_address().clone()
    };

    let node_state = state.read().await;
    if let node::NodeState::Joined(this_node) = &*node_state {
        let added_items = this_node
            .insert_items(
                response
                    .records
                    .iter()
                    .map(|r| r.item.clone())
                    .collect::<Vec<_>>(),
                env.get_store(),
                env.get_wal(),
            )
            .await;

        if !response.records.is_empty() {
            debug!(
                "node={}; Received SyncResponse from {}:{} with {} items",
                node_address,
                src.ip(),
                src.port(),
                response.records.len()
            );
        }

        match added_items {
            Ok((added, _)) => {
                let mut involved_partitions = HashSet::new();

                for record in &response.records {
                    involved_partitions.insert(record.partition);

                    this_node
                        .sync_state
                        .replica_partition_lsn
                        .entry(record.partition)
                        .and_modify(|x| {
                            if record.lsn > x.lsn {
                                *x = LsnOffset {
                                    lsn: record.lsn,
                                    position: record.position,
                                };
                            }
                        })
                        .or_insert(LsnOffset {
                            lsn: record.lsn,
                            position: record.position,
                        });
                }

                for (partition, leader_lsn) in response.current_lsns {
                    this_node
                        .sync_state
                        .leader_tip_lsns
                        .insert(partition, leader_lsn);
                }

                if !response.records.is_empty() {
                    let _ = this_node.save_node_metadata(true);
                    if !added.is_empty() {
                        info!(
                            "node={}; Synced {} items from {}",
                            this_node.get_address(),
                            added.len(),
                            src
                        );
                    }

                    let mut response_ack = HashMap::new();
                    for partition in involved_partitions {
                        if let Some(lsn_offset) =
                            this_node.sync_state.replica_partition_lsn.get(&partition)
                        {
                            response_ack.insert(partition, lsn_offset.lsn);
                        }
                    }
                    response_ack
                } else {
                    HashMap::new()
                }
            }
            Err(e) => {
                error!(
                    "node={}; Error inserting synced items: {}",
                    this_node.get_address(),
                    e
                );
                HashMap::new()
            }
        }
    } else {
        HashMap::new()
    }
}

pub async fn server_sync_handler_task(
    node_address: NodeAddress,
    listener: TcpListener,
    state: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    loop {
        let (stream, addr) = tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("node={}; Shutting down sync server", &node_address);
                break;
            }
            Ok((stream, addr)) = listener.accept() => (stream, addr),
            else => break,
        };

        let async_node_address = node_address.clone();
        let async_state = state.clone();
        let async_env = env.clone();

        tokio::spawn(async move {
            handle_connection(stream, addr, async_node_address, async_state, async_env).await;
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    addr: SocketAddr,
    node_address: NodeAddress,
    state: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) {
    let _ = stream.set_nodelay(true);
    loop {
        let buf = match read_frame(&mut stream).await {
            Ok(Some(buf)) => buf,
            Ok(None) => break,
            Err(e) => {
                error!("node={node_address}; Error reading frame from {addr}: {e}");
                break;
            }
        };

        if let Ok((wire, _)) =
            bincode::decode_from_slice::<TcpWireMessage, _>(&buf, bincode::config::standard())
        {
            match wire {
                TcpWireMessage::Request(request) => {
                    server_handle_request(
                        request,
                        addr,
                        &node_address,
                        &mut stream,
                        state.clone(),
                        env.clone(),
                    )
                    .await;
                }
                TcpWireMessage::ResponseAck(ack) => {
                    info!(
                        "node={}; replica {} responded with ack",
                        node_address, &addr
                    );
                    let state_read = state.read().await;
                    if let node::NodeState::Joined(this_node) = &*state_read {
                        for (partition, lsn) in ack.partition_lsns {
                            this_node.update_confirmed_lsn(partition, lsn);
                        }
                    }
                }
                _ => {
                    error!(
                        "node={}; Received unknown message from {}:{}",
                        &node_address,
                        addr.ip(),
                        addr.port()
                    );
                    break;
                }
            }
        } else {
            error!(
                "node={}; Error decoding message from {}",
                &node_address, addr
            );
            break;
        }
    }
}

pub async fn server_handle_request(
    request: SyncRequest,
    addr: SocketAddr,
    node_address: &NodeAddress,
    stream: &mut TcpStream,
    state: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) {
    // Check LSN in the request and update Leader's LSN if needed
    // This is mainly useful in cases where Replica is caught up but Leader never received Ack message
    {
        let node_state = state.read().await;
        if let node::NodeState::Joined(this_node) = &*node_state {
            for partition in &request.partitions {
                this_node.update_confirmed_lsn(partition.partition, partition.lsn);
            }
        }
    }

    let mut wal_scan_lsn_tracker: HashMap<PartitionId, (u64, WalPosition)> = HashMap::new();
    let mut items: Vec<SyncResponseWalRecord> = Vec::new();
    let mut current_payload_size = 0;
    let wal_lock = env.get_wal();
    let time_now = now_millis();

    let mut watchers = Vec::new();
    for partition in &request.partitions {
        watchers.push(wal_lock.get_lsn_watcher(partition.partition.into()).await);
    }

    let mut current_iteration_partitions: HashSet<PartitionId> =
        request.partitions.iter().map(|p| p.partition).collect();

    loop {
        let mut data_found_in_this_iteration = false;

        for req_partition in &request.partitions {
            if !current_iteration_partitions.contains(&req_partition.partition) {
                continue;
            }

            let (last_lsn, last_position) = {
                if let Some((lsn, pos)) = wal_scan_lsn_tracker.get_mut(&req_partition.partition) {
                    (*lsn, *pos)
                } else {
                    let result = (req_partition.lsn, req_partition.position);
                    wal_scan_lsn_tracker.insert(req_partition.partition, result);
                    result
                }
            };

            let is_exclusive = last_lsn > 0;
            let iter: Box<
                dyn Iterator<
                    Item = Result<
                        (FramedWalRecord<WalRecord>, WalPosition),
                        gossipgrid_wal::WalError,
                    >,
                >,
            > = wal_lock
                .stream_from(
                    last_lsn,
                    last_position,
                    req_partition.partition.into(),
                    is_exclusive,
                )
                .await;

            for record in iter {
                if current_payload_size >= request.max_size_bytes {
                    break;
                }

                match record {
                    Ok((framed_record, position)) => {
                        data_found_in_this_iteration = true;
                        let framed_item: FramedWalRecordItem = framed_record.into();

                        wal_scan_lsn_tracker
                            .entry(req_partition.partition)
                            .and_modify(|pair| {
                                if framed_item.lsn > pair.0 {
                                    *pair = (framed_item.lsn, position);
                                }
                            });

                        let storage_key: StorageKey =
                            match String::from_utf8(framed_item.key_bytes.clone())
                                .map_err(|e| e.to_string())
                                .and_then(|s| s.parse().map_err(|e: DataStoreError| e.to_string()))
                            {
                                Ok(s) => s,
                                Err(e) => {
                                    error!("Error parsing storage key: {e}");
                                    continue;
                                }
                            };

                        current_payload_size += framed_item.item.message.len() as u64;
                        items.push(SyncResponseWalRecord {
                            partition: req_partition.partition,
                            item: ItemEntry {
                                storage_key,
                                item: framed_item.item,
                            },
                            lsn: framed_item.lsn,
                            position,
                        });
                    }
                    Err(e) => {
                        error!("Error reading from WAL: {e}");
                        break;
                    }
                }
            }
        }

        let elapsed = now_millis() - time_now;
        if current_payload_size > 0
            || elapsed >= SERVER_NO_RECORDS_FOUND_TIMEOUT_MILLIS
            || elapsed >= request.scan_timeout_millis as u64
        {
            break;
        }

        if !data_found_in_this_iteration {
            let mut wait_futures = Vec::new();
            for (i, watcher) in watchers.iter_mut().enumerate() {
                let partition_id = request.partitions[i].partition;
                let current_lsn = wal_scan_lsn_tracker
                    .get(&partition_id)
                    .map(|(lsn, _)| *lsn)
                    .unwrap_or(request.partitions[i].lsn);
                let target_lsn = current_lsn + 1;
                wait_futures.push(Box::pin(async move {
                    let _ = watcher.wait_for(move |lsn| *lsn >= target_lsn).await;
                    partition_id
                }));
            }

            let wait_timeout = SERVER_NO_RECORDS_FOUND_TIMEOUT_MILLIS
                .saturating_sub(elapsed)
                .max(SERVER_RECORD_FOUND_DELAY_MILLIS);

            match timeout(
                Duration::from_millis(wait_timeout),
                futures::future::select_all(wait_futures),
            )
            .await
            {
                Ok(_) => {
                    // Reset to all partitions to catch any other simultaneous updates
                    current_iteration_partitions =
                        request.partitions.iter().map(|p| p.partition).collect();
                }
                Err(_) => break,
            }
        } else {
            // Data found, reset to all partitions for next pass if payload not full
            current_iteration_partitions = request.partitions.iter().map(|p| p.partition).collect();
        }
    }

    // get latests LSNs for partitions requested by client
    let requested_partitions: Vec<PartitionId> =
        request.partitions.iter().map(|p| p.partition).collect();
    let current_lsn = {
        let node_state = state.read().await;
        if let node::NodeState::Joined(this_node) = &*node_state {
            this_node
                .sync_state
                .leader_confirmed_lsn_for_partitions(&requested_partitions)
        } else {
            HashMap::new()
        }
    };

    let items_length = items.len();
    let response = TcpWireMessage::Response(SyncResponse {
        current_lsns: current_lsn,
        records: items,
    });

    if let Ok(encoded_resp) = bincode::encode_to_vec(&response, bincode::config::standard()) {
        match write_frame(stream, &encoded_resp).await {
            Ok(_) => {
                debug!(
                    "node={node_address}; wrote SyncReponse with {items_length} items to {addr}",
                );
            }
            Err(e) => {
                error!("node={node_address}; Failed to send SyncResponse to {addr}: {e}");
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::HLC;
    use crate::cluster::Cluster;
    use crate::gossip::Gossip;
    use crate::node::JoinedNode;
    use crate::node::{SimpleNode, SimpleNodeState};
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU8;

    #[tokio::test]
    async fn test_evaluate_sync_toggle_caught_up() {
        let node_index = 0;
        let p1 = PartitionId(0);
        // Create cluster with 1 partition, so node 0 only has p1
        let mut cluster = Cluster::new("test".to_string(), 3, node_index, 1, 2, true);

        // Node 0 owns p1, node 1 is leader
        // We must ensure node 1 is in a state that allows it to be a leader
        let node1 = SimpleNode {
            address: NodeAddress::new("127.0.0.1".to_string(), 8001),
            state: SimpleNodeState::Joined,
            web_port: 8081,
            last_seen: now_millis(),
            hlc: HLC::default(),
            partition_item_counts: HashMap::new(),
            leading_partitions: vec![],
        };
        cluster.assign_node(&1, &node1).unwrap();
        cluster.partition_leaders.insert(p1, 1);

        let mut joined_node = JoinedNode {
            address: NodeAddress::new("127.0.0.1".to_string(), 8000),
            web_port: 8080,
            cluster,
            partition_counts: DashMap::new(),
            node_hlc: Mutex::new(HLC::default()),
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

        // Assign node 0 to the cluster
        let simple_this_node = joined_node.get_simple_node();
        joined_node
            .cluster
            .assign_node(&node_index, &simple_this_node)
            .unwrap();

        // Set state: p1 is caught up
        joined_node.sync_state.replica_partition_lsn.insert(
            p1,
            LsnOffset {
                lsn: 1000,
                position: WalPosition::default(),
            },
        );
        joined_node.sync_state.leader_tip_lsns.insert(p1, 1050); // Delta 50 < 100

        let state = Arc::new(RwLock::new(NodeState::Joined(joined_node)));

        evaluate_sync_toggle(state.clone()).await;

        let node_state = state.read().await;
        if let NodeState::Joined(n) = &*node_state {
            assert!(!n.is_syncing);
        } else {
            panic!("Expected Joined state");
        }
    }

    #[tokio::test]
    async fn test_evaluate_sync_toggle_multi_leader_one_behind() {
        let node_index = 0;
        let p1 = PartitionId(0);
        let p2 = PartitionId(2);
        // 3 partitions, repl factor 2. Node 0 owns p0, p2.
        let mut cluster = Cluster::new("test".to_string(), 3, node_index, 3, 2, true);

        // p1 from node 1, p2 from node 2
        let node1 = SimpleNode {
            address: NodeAddress::new("127.0.0.1".to_string(), 8001),
            state: SimpleNodeState::Joined,
            web_port: 8081,
            last_seen: now_millis(),
            hlc: HLC::default(),
            partition_item_counts: HashMap::new(),
            leading_partitions: vec![],
        };
        let node2 = SimpleNode {
            address: NodeAddress::new("127.0.0.1".to_string(), 8002),
            state: SimpleNodeState::Joined,
            web_port: 8082,
            last_seen: now_millis(),
            hlc: HLC::default(),
            partition_item_counts: HashMap::new(),
            leading_partitions: vec![],
        };
        cluster.assign_node(&1, &node1).unwrap();
        cluster.assign_node(&2, &node2).unwrap();

        cluster.partition_leaders.insert(p1, 1);
        cluster.partition_leaders.insert(p2, 2);

        let mut joined_node = JoinedNode {
            address: NodeAddress::new("127.0.0.1".to_string(), 8000),
            web_port: 8080,
            cluster,
            partition_counts: DashMap::new(),
            node_hlc: Mutex::new(HLC::default()),
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

        // Assign node 0 to the cluster
        let simple_this_node = joined_node.get_simple_node();
        joined_node
            .cluster
            .assign_node(&node_index, &simple_this_node)
            .unwrap();

        // p1 is caught up, p2 is behind
        joined_node.sync_state.replica_partition_lsn.insert(
            p1,
            LsnOffset {
                lsn: 1000,
                position: WalPosition::default(),
            },
        );
        joined_node.sync_state.leader_tip_lsns.insert(p1, 1050);

        joined_node.sync_state.replica_partition_lsn.insert(
            p2,
            LsnOffset {
                lsn: 500,
                position: WalPosition::default(),
            },
        );
        joined_node.sync_state.leader_tip_lsns.insert(p2, 1000); // Delta 500 > 100

        let state = Arc::new(RwLock::new(NodeState::Joined(joined_node)));

        evaluate_sync_toggle(state.clone()).await;

        let node_state = state.read().await;
        if let NodeState::Joined(n) = &*node_state {
            assert!(n.is_syncing, "Should still be syncing because p2 is behind");
        } else {
            panic!("Expected Joined state");
        }
    }

    #[tokio::test]
    async fn test_evaluate_sync_toggle_unknown_leader_tip() {
        let node_index = 0;
        let p1 = PartitionId(0);
        let mut cluster = Cluster::new("test".to_string(), 3, node_index, 1, 2, true);

        cluster.partition_leaders.insert(p1, 1);

        let mut joined_node = JoinedNode {
            address: NodeAddress::new("127.0.0.1".to_string(), 8000),
            web_port: 8080,
            cluster,
            partition_counts: DashMap::new(),
            node_hlc: Mutex::new(HLC::default()),
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

        // Assign node 0 to the cluster
        let simple_this_node = joined_node.get_simple_node();
        joined_node
            .cluster
            .assign_node(&node_index, &simple_this_node)
            .unwrap();

        // p1 replica LSN is 1000, but we don't know leader tip yet
        joined_node.sync_state.replica_partition_lsn.insert(
            p1,
            LsnOffset {
                lsn: 1000,
                position: WalPosition::default(),
            },
        );

        let state = Arc::new(RwLock::new(NodeState::Joined(joined_node)));

        evaluate_sync_toggle(state.clone()).await;

        let node_state = state.read().await;
        if let NodeState::Joined(n) = &*node_state {
            assert!(
                n.is_syncing,
                "Should still be syncing if leader tip is unknown"
            );
        } else {
            panic!("Expected Joined state");
        }
    }
}
