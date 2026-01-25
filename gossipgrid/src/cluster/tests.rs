use crate::clock::HLC;
use crate::cluster::state::Cluster;
use crate::cluster::types::{AssignmentNode, AssignmentState, PartitionId};
use crate::node::{self, NodeAddress, NodeId, NodeState, SimpleNode, SimpleNodeState};
use std::collections::HashMap;

fn build_config(assignments: Vec<(NodeId, Vec<u16>)>) -> Cluster {
    let partition_assignments: HashMap<u8, (Vec<PartitionId>, AssignmentState)> = assignments
        .into_iter()
        .map(|(idx, partitions)| {
            let partitions = partitions
                .into_iter()
                .map(PartitionId)
                .collect::<Vec<PartitionId>>();
            (idx, (partitions, AssignmentState::Unassigned))
        })
        .collect();

    Cluster {
        cluster_name: "test-cluster".into(),
        cluster_size: partition_assignments.len() as u8,
        this_node_index: 0,
        partition_count: 0,
        replication_factor: 0,
        partition_assignments,
        partition_leaders: HashMap::new(),
        created_at: 0,
        updated_at: 0,
        is_ephemeral: true,
    }
}

#[test]
fn test_cluster_metadata_creation() {
    let metadata = Cluster::new("test-cluster".to_string(), 3, 0, 9, 2, true);
    assert_eq!(metadata.cluster_name, "test-cluster");
    assert_eq!(metadata.cluster_size, 3);
    assert_eq!(metadata.partition_count, 9);
    assert_eq!(metadata.replication_factor, 2);
    assert_eq!(metadata.partition_assignments.len(), 3);
}

#[test]
fn test_partition_assignments_unassigned() {
    let cluster_config = Cluster::new("test-cluster".to_string(), 3, 0, 9, 2, true);
    for (_, assignment) in cluster_config.partition_assignments.values() {
        assert_eq!(*assignment, AssignmentState::Unassigned);
    }
}

#[test]
fn test_assign_node() {
    let mut cluster_config = Cluster::new("test-cluster".to_string(), 3, 0, 9, 2, true);
    let node: SimpleNode = SimpleNode {
        address: NodeAddress::parse_unchecked("127.0.0.1:4109"),
        state: node::SimpleNodeState::Joined,
        web_port: 3001,
        last_seen: 0,
        hlc: HLC::default(),
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![],
    };
    let result = cluster_config.assign_node(&0, &node);
    assert!(result.is_ok());

    if let Some((_, assignment)) = cluster_config.partition_assignments.get(&0) {
        match assignment {
            AssignmentState::Joined { node, .. } => {
                assert_eq!(node.address.ip, "127.0.0.1");
                assert_eq!(node.address.port, 4109);
            }
            _ => panic!("Expected Assigned variant"),
        }
    }
}

#[test]
fn test_is_fully_assigned() {
    let mut cluster_config = Cluster::new("test-cluster".to_string(), 3, 0, 9, 2, true);
    assert!(!cluster_config.is_fully_assigned());

    let node_1: SimpleNode = SimpleNode {
        address: NodeAddress::parse_unchecked("127.0.0.1:4109"),
        state: node::SimpleNodeState::Joined,
        web_port: 3001,
        last_seen: 0,
        hlc: HLC::default(),
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![],
    };
    let node_2: SimpleNode = SimpleNode {
        address: NodeAddress::parse_unchecked("127.0.0.1:4110"),
        state: node::SimpleNodeState::Joined,
        web_port: 3002,
        last_seen: 0,
        hlc: HLC::default(),
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![],
    };
    let node_3: SimpleNode = SimpleNode {
        address: NodeAddress::parse_unchecked("127.0.0.1:4111"),
        state: node::SimpleNodeState::Joined,
        web_port: 3003,
        last_seen: 0,
        hlc: HLC::default(),
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![],
    };
    cluster_config.assign_node(&0, &node_1).unwrap();
    cluster_config.assign_node(&1, &node_2).unwrap();
    assert!(!cluster_config.is_fully_assigned());

    cluster_config.assign_node(&2, &node_3).unwrap();
    assert!(cluster_config.is_fully_assigned());
}

#[test]
fn test_leadership_all_assigned_lowest_index() {
    let mut config = build_config(vec![(0, vec![0, 1]), (1, vec![0, 1]), (2, vec![0, 1])]);

    config
        .assign_node(
            &0,
            &SimpleNode {
                address: NodeAddress::parse_unchecked("127.0.0.1:4109"),
                state: node::SimpleNodeState::Joined,
                web_port: 3001,
                last_seen: 0,
                hlc: HLC::default(),
                partition_item_counts: HashMap::new(),
                leading_partitions: vec![],
            },
        )
        .unwrap();
    config
        .assign_node(
            &1,
            &SimpleNode {
                address: NodeAddress::parse_unchecked("127.0.0.1:4110"),
                state: node::SimpleNodeState::Joined,
                web_port: 3002,
                last_seen: 0,
                hlc: HLC::default(),
                partition_item_counts: HashMap::new(),
                leading_partitions: vec![],
            },
        )
        .unwrap();
    config
        .assign_node(
            &2,
            &SimpleNode {
                address: NodeAddress::parse_unchecked("127.0.0.1:4111"),
                state: node::SimpleNodeState::Joined,
                web_port: 3003,
                last_seen: 0,
                hlc: HLC::default(),
                partition_item_counts: HashMap::new(),
                leading_partitions: vec![],
            },
        )
        .unwrap();

    // config.partition_leaders is no longer empty because assign_node now calls assign_partition_leaders()
    assert!(!config.partition_leaders.is_empty());

    config.assign_partition_leaders();

    assert!(config.partition_leaders.len() == 2);

    assert_eq!(config.partition_leaders.get(&PartitionId(0)), Some(&0));
    assert_eq!(config.partition_leaders.get(&PartitionId(1)), Some(&1));
}

#[test]
fn test_leadership_node_0_disconnected() {
    let mut config = build_config(vec![(0, vec![0, 1]), (1, vec![0, 1]), (2, vec![0, 1])]);

    config
        .assign_node(
            &1,
            &SimpleNode {
                address: NodeAddress::parse_unchecked("127.0.0.1:4110"),
                state: node::SimpleNodeState::Joined,
                web_port: 3002,
                last_seen: 0,
                hlc: HLC::default(),
                partition_item_counts: HashMap::new(),
                leading_partitions: vec![],
            },
        )
        .unwrap();
    config
        .assign_node(
            &2,
            &SimpleNode {
                address: NodeAddress::parse_unchecked("127.0.0.1:4111"),
                state: node::SimpleNodeState::Joined,
                web_port: 3003,
                last_seen: 0,
                hlc: HLC::default(),
                partition_item_counts: HashMap::new(),
                leading_partitions: vec![],
            },
        )
        .unwrap();
    config
        .assign_node(
            &0,
            &SimpleNode {
                address: NodeAddress::parse_unchecked("127.0.0.1:4109"),
                state: node::SimpleNodeState::Disconnected,
                web_port: 3001,
                last_seen: 0,
                hlc: HLC::default(),
                partition_item_counts: HashMap::new(),
                leading_partitions: vec![],
            },
        )
        .unwrap();

    config.assign_partition_leaders();

    assert!(config.partition_leaders.len() == 2);

    assert_eq!(config.partition_leaders.get(&PartitionId(0)), Some(&1));
}

#[test]
fn test_leadership_node_0_unassigned() {
    let mut config = build_config(vec![(0, vec![0, 1]), (1, vec![0, 1]), (2, vec![0, 1])]);

    config
        .assign_node(
            &1,
            &SimpleNode {
                address: NodeAddress::parse_unchecked("127.0.0.1:4110"),
                state: node::SimpleNodeState::Joined,
                web_port: 3002,
                last_seen: 0,
                hlc: HLC::default(),
                partition_item_counts: HashMap::new(),
                leading_partitions: vec![],
            },
        )
        .unwrap();
    config
        .assign_node(
            &2,
            &SimpleNode {
                address: NodeAddress::parse_unchecked("127.0.0.1:4111"),
                state: node::SimpleNodeState::Joined,
                web_port: 3003,
                last_seen: 0,
                hlc: HLC::default(),
                partition_item_counts: HashMap::new(),
                leading_partitions: vec![],
            },
        )
        .unwrap();

    config.assign_partition_leaders();

    assert!(config.partition_leaders.len() == 2);

    assert_eq!(config.partition_leaders.get(&PartitionId(0)), Some(&1));
}

#[test]
fn test_leadership_sticky() {
    let mut config = build_config(vec![(0, vec![0, 1]), (1, vec![0, 1])]);

    config
        .assign_node(
            &0,
            &SimpleNode {
                address: NodeAddress::parse_unchecked("127.0.0.1:4109"),
                state: node::SimpleNodeState::Joined,
                web_port: 3001,
                last_seen: 0,
                hlc: HLC::default(),
                partition_item_counts: HashMap::new(),
                leading_partitions: vec![],
            },
        )
        .unwrap();

    config
        .assign_node(
            &1,
            &SimpleNode {
                address: NodeAddress::parse_unchecked("127.0.0.1:4110"),
                state: node::SimpleNodeState::Joined,
                web_port: 3002,
                last_seen: 0,
                hlc: HLC::default(),
                partition_item_counts: HashMap::new(),
                leading_partitions: vec![],
            },
        )
        .unwrap();

    config.assign_partition_leaders();

    assert_eq!(config.partition_leaders.get(&PartitionId(0)), Some(&0));

    config.unassign_node(&0, &HLC::default()).unwrap();

    config.assign_partition_leaders();

    assert_eq!(config.partition_leaders.get(&PartitionId(0)), Some(&1));

    config
        .assign_node(
            &0,
            &SimpleNode {
                address: NodeAddress::parse_unchecked("127.0.0.1:4109"),
                state: node::SimpleNodeState::Joined,
                web_port: 3001,
                last_seen: 0,
                hlc: HLC::default(),
                partition_item_counts: HashMap::new(),
                leading_partitions: vec![],
            },
        )
        .unwrap();

    config.assign_partition_leaders();

    assert_eq!(config.partition_leaders.get(&PartitionId(0)), Some(&0));
}

#[test]
fn test_next_node() {
    let cluster_config = Cluster::new("EPHEMERAL".to_string(), 4, 0, 8, 2, true);

    let mut memory = NodeState::init(
        NodeAddress::new("127.0.0.1".to_string(), 1000),
        3001,
        None,
        Some(cluster_config),
    );
    match memory {
        NodeState::Joined(ref mut joined) => {
            joined
                .cluster
                .assign_next(&SimpleNode {
                    address: NodeAddress::new("127.0.0.1".to_string(), 1200),
                    state: node::SimpleNodeState::Joined,
                    web_port: 3001,
                    last_seen: 100,
                    hlc: HLC::default(),
                    partition_item_counts: HashMap::new(),
                    leading_partitions: vec![],
                })
                .unwrap();
            joined
                .cluster
                .assign_next(&SimpleNode {
                    address: NodeAddress::new("127.0.0.1".to_string(), 1300),
                    state: node::SimpleNodeState::Joined,
                    web_port: 3002,
                    last_seen: 100,
                    hlc: HLC::default(),
                    partition_item_counts: HashMap::new(),
                    leading_partitions: vec![],
                })
                .unwrap();
            joined
                .cluster
                .assign_next(&SimpleNode {
                    address: NodeAddress::new("127.0.0.1".to_string(), 1400),
                    state: node::SimpleNodeState::Joined,
                    web_port: 3003,
                    last_seen: 100,
                    hlc: HLC::default(),
                    partition_item_counts: HashMap::new(),
                    leading_partitions: vec![],
                })
                .unwrap();

            let other_peers = joined.cluster.other_peers();
            let next_nodes =
                joined
                    .gossip
                    .next_nodes(4, joined.cluster.this_node_index, other_peers);
            assert_eq!(next_nodes, vec!(1, 2, 3, 1));
        }
        _ => {
            panic!("NodeState should be Joined");
        }
    }
}

#[test]
fn test_next_nodes() {
    let cluster_config = Cluster::new("EPHEMERAL".to_string(), 3, 0, 8, 2, true);

    let mut memory = NodeState::init(
        NodeAddress::new("127.0.0.1".to_string(), 1000),
        3001,
        None,
        Some(cluster_config),
    );
    match memory {
        NodeState::Joined(ref mut joined) => {
            joined
                .cluster
                .assign_next(&SimpleNode {
                    address: NodeAddress::new("127.0.0.1".to_string(), 1200),
                    state: node::SimpleNodeState::Joined,
                    web_port: 3001,
                    last_seen: 100,
                    hlc: HLC::default(),
                    partition_item_counts: HashMap::new(),
                    leading_partitions: vec![],
                })
                .unwrap();
            joined
                .cluster
                .assign_next(&SimpleNode {
                    address: NodeAddress::new("127.0.0.1".to_string(), 1300),
                    state: node::SimpleNodeState::Joined,
                    web_port: 3002,
                    last_seen: 100,
                    hlc: HLC::default(),
                    partition_item_counts: HashMap::new(),
                    leading_partitions: vec![],
                })
                .unwrap();

            let next_nodes_1 = joined.gossip.next_nodes(
                2,
                joined.cluster.this_node_index,
                joined.cluster.other_peers(),
            );
            let next_nodes_2 = joined.gossip.next_nodes(
                2,
                joined.cluster.this_node_index,
                joined.cluster.other_peers(),
            );

            assert_eq!(next_nodes_1, vec!(1, 2));
            assert_eq!(next_nodes_2, vec!(1, 2));
        }
        _ => {
            panic!("NodeState should be Joined");
        }
    }
}

#[test]
fn test_partition_balancing() {
    let config = Cluster::new("test".into(), 3, 0, 6, 2, true);
    for node_idx in 0..3 {
        let partitions = config.get_assigned_partitions(node_idx).unwrap();
        assert_eq!(partitions.len(), 4);
    }
}

#[test]
fn test_hlc_ordering_with_clock_drift() {
    let mut config = Cluster::new("test".into(), 3, 0, 6, 2, true);

    let older_hlc = HLC {
        timestamp: 1000,
        counter: 0,
    };
    let newer_hlc = HLC {
        timestamp: 1100,
        counter: 0,
    };

    let node_1_joined = SimpleNode {
        address: NodeAddress::parse_unchecked("127.0.0.1:4001"),
        state: node::SimpleNodeState::Joined,
        web_port: 3001,
        last_seen: 2000,
        hlc: newer_hlc.clone(),
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![],
    };

    let node_1_disconnected_stale = SimpleNode {
        address: NodeAddress::parse_unchecked("127.0.0.1:4001"),
        state: node::SimpleNodeState::Disconnected,
        web_port: 3001,
        last_seen: 3000,
        hlc: older_hlc.clone(),
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![],
    };

    let from_2: NodeAddress = NodeAddress::parse_unchecked("127.0.0.1:4002");
    let dummy_sender = SimpleNode {
        address: from_2.clone(),
        state: node::SimpleNodeState::Joined,
        web_port: 3002,
        last_seen: 2000,
        hlc: HLC {
            timestamp: 1000,
            counter: 0,
        },
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![],
    };

    config.assign_node(&1, &node_1_joined).unwrap();
    assert!(matches!(
        config.partition_assignments.get(&1).unwrap().1,
        AssignmentState::Joined { .. }
    ));

    let mut peers = std::collections::HashMap::new();
    peers.insert(1, node_1_disconnected_stale);
    config
        .update_cluster_membership(&dummy_sender, &peers)
        .unwrap();

    assert!(matches!(
        config.partition_assignments.get(&1).unwrap().1,
        AssignmentState::Joined { .. }
    ));

    let newer_hlc_2 = HLC {
        timestamp: 1200,
        counter: 0,
    };
    let node_1_joined_newer = SimpleNode {
        address: NodeAddress::parse_unchecked("127.0.0.1:4001"),
        state: node::SimpleNodeState::Joined,
        web_port: 3001,
        last_seen: 2500,
        hlc: newer_hlc_2.clone(),
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![],
    };
    peers.insert(1, node_1_joined_newer.clone());
    config
        .update_cluster_membership(&node_1_joined_newer, &peers)
        .unwrap();

    assert!(matches!(
        config.partition_assignments.get(&1).unwrap().1,
        AssignmentState::Joined { .. }
    ));

    let authoritative_hlc = HLC {
        timestamp: 1300,
        counter: 0,
    };
    let node_1_disconnected_authoritative = SimpleNode {
        address: NodeAddress::parse_unchecked("127.0.0.1:4001"),
        state: node::SimpleNodeState::Disconnected,
        web_port: 3001,
        last_seen: 3500,
        hlc: authoritative_hlc.clone(),
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![],
    };
    peers.insert(1, node_1_disconnected_authoritative);
    config
        .update_cluster_membership(&dummy_sender, &peers)
        .unwrap();

    assert!(matches!(
        config.partition_assignments.get(&1).unwrap().1,
        AssignmentState::Disconnected { .. }
    ));
}

#[test]
fn test_leadership_merge_gossip() {
    let mut config = Cluster::new("test".into(), 3, 0, 1, 3, true);
    let this_addr: NodeAddress = NodeAddress::parse_unchecked("127.0.0.1:4000");
    let from_1: NodeAddress = NodeAddress::parse_unchecked("127.0.0.1:4001");
    let from_2: NodeAddress = NodeAddress::parse_unchecked("127.0.0.1:4002");

    let hlc_0 = HLC {
        timestamp: 1000,
        counter: 0,
    };
    let hlc_1 = HLC {
        timestamp: 1100,
        counter: 0,
    };
    let hlc_2 = HLC {
        timestamp: 1200,
        counter: 0,
    };

    let node_0 = SimpleNode {
        address: this_addr.clone(),
        state: SimpleNodeState::Joined,
        web_port: 3000,
        last_seen: 1000,
        hlc: hlc_0.clone(),
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![],
    };
    let node_1 = SimpleNode {
        address: from_1.clone(),
        state: SimpleNodeState::Joined,
        web_port: 3001,
        last_seen: 1000,
        hlc: hlc_1.clone(),
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![PartitionId(0)],
    };
    let node_2 = SimpleNode {
        address: from_2.clone(),
        state: SimpleNodeState::Joined,
        web_port: 3002,
        last_seen: 1000,
        hlc: hlc_2.clone(),
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![PartitionId(0)],
    };

    config.assign_node(&0, &node_0).unwrap();
    config.assign_node(&1, &node_1).unwrap();
    config.assign_node(&2, &node_2).unwrap();
    config.assign_partition_leaders();

    // ideal_node for P0 in 3n cluster is 0. Node 0 is Joined and owns P0.
    // Even if Node 1 or Node 2 claim leadership with higher HLC,
    // assign_partition_leaders will re-elect Node 0 because it has higher priority (0).
    assert_eq!(config.partition_leaders.get(&PartitionId(0)), Some(&0));

    let mut peers = HashMap::new();
    peers.insert(1, node_1.clone());
    config.update_cluster_membership(&node_1, &peers).unwrap();

    assert_eq!(config.partition_leaders.get(&PartitionId(0)), Some(&0));

    let mut peers = HashMap::new();
    peers.insert(2, node_2.clone());
    config.update_cluster_membership(&node_2, &peers).unwrap();

    assert_eq!(config.partition_leaders.get(&PartitionId(0)), Some(&0));

    let mut peers = HashMap::new();
    peers.insert(1, node_1.clone());
    config.update_cluster_membership(&node_1, &peers).unwrap();

    assert_eq!(config.partition_leaders.get(&PartitionId(0)), Some(&0));
}

#[test]
fn test_duplicate_leading_partitions() {
    let mut config = Cluster::new("test".into(), 1, 0, 2, 1, true);
    let node = SimpleNode {
        address: NodeAddress::parse_unchecked("127.0.0.1:4000"),
        state: SimpleNodeState::Joined,
        web_port: 3000,
        last_seen: 1000,
        hlc: HLC::new(),
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![],
    };
    config.assign_node(&0, &node).unwrap();
    config.partition_leaders.clear();

    config.partition_leaders.insert(PartitionId(1), 0);
    config.partition_leaders.insert(PartitionId(4), 0);
    config.partition_leaders.insert(PartitionId(7), 0);

    let peers = config.get_all_peers_cloned();
    let this_node = peers.get(&0).unwrap();

    use std::collections::HashSet;
    let mut seen = HashSet::new();
    for pid in &this_node.leading_partitions {
        assert!(seen.insert(pid.0), "Duplicate partition ID found: {pid:?}");
    }

    assert_eq!(this_node.leading_partitions.len(), 3);
}

#[test]
fn test_node_state_unification() {
    let addr: NodeAddress = NodeAddress::parse_unchecked("127.0.0.1:4001");
    let hlc = HLC::new();

    let snode_syncing = SimpleNode {
        address: addr.clone(),
        state: SimpleNodeState::JoinedSyncing,
        web_port: 3001,
        last_seen: 1000,
        hlc: hlc.clone(),
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![],
    };

    let assignment: AssignmentState = (&snode_syncing).into();
    assert!(matches!(assignment, AssignmentState::JoinedSyncing { .. }));

    let snode_joined = SimpleNode {
        address: addr.clone(),
        state: SimpleNodeState::Joined,
        web_port: 3001,
        last_seen: 1000,
        hlc: hlc.clone(),
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![],
    };

    let assignment: AssignmentState = (&snode_joined).into();
    assert!(matches!(assignment, AssignmentState::Joined { .. }));
}

#[test]
fn test_syncing_node_leadership() {
    let mut config = Cluster::new("test_cluster".to_string(), 3, 0u8, 16, 1, false);

    let node1 = SimpleNode {
        address: NodeAddress::parse_unchecked("127.0.0.1:4001"),
        state: SimpleNodeState::JoinedSyncing,
        web_port: 3001,
        last_seen: 1000,
        hlc: HLC {
            timestamp: 2000,
            counter: 0,
        },
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![],
    };
    config.assign_node(&1, &node1).unwrap();

    config.assign_partition_leaders();

    let mut elected_for_some = false;
    for i in 0..16 {
        if config.partition_leaders.get(&PartitionId(i)) == Some(&1) {
            elected_for_some = true;
            break;
        }
    }
    assert!(
        elected_for_some,
        "Node 1 (Syncing) should be elected as leader for its partitions if no Joined nodes are available"
    );

    let node0 = SimpleNode {
        address: NodeAddress::parse_unchecked("127.0.0.1:4000"),
        state: SimpleNodeState::Joined,
        web_port: 3000,
        last_seen: 1000,
        hlc: HLC {
            timestamp: 1000,
            counter: 0,
        },
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![],
    };
    config.assign_node(&0, &node0).unwrap();

    config.assign_partition_leaders();

    for i in 0..16 {
        if let Some(partitions) = config.get_assigned_partitions(0)
            && partitions.contains(&PartitionId(i))
        {
            assert_eq!(
                config.partition_leaders.get(&PartitionId(i)),
                Some(&0),
                "Node 0 (Joined) should take over leader for partition {i}"
            );
        }
    }
}

#[test]
fn test_leader_election_distribution_balance() {
    let mut config = Cluster::new("test-cluster".to_string(), 3, 0, 9, 2, true);

    for i in 0..3 {
        let node = SimpleNode {
            address: NodeAddress::new("127.0.0.1".to_string(), 4110 + i as u16),
            state: node::SimpleNodeState::Joined,
            web_port: 3000 + i as u16,
            last_seen: 0,
            hlc: HLC::default(),
            partition_item_counts: HashMap::new(),
            leading_partitions: vec![],
        };
        config.assign_node(&i, &node).unwrap();
    }

    config.assign_partition_leaders();

    let mut leader_counts = HashMap::new();
    for i in 0..9 {
        let leader = config.partition_leaders.get(&PartitionId(i)).unwrap();
        *leader_counts.entry(*leader).or_insert(0) += 1;
    }

    assert_eq!(leader_counts.get(&0), Some(&3));
    assert_eq!(leader_counts.get(&1), Some(&3));
    assert_eq!(leader_counts.get(&2), Some(&3));
}

#[test]
fn test_leader_election_distribution_on_node_failure() {
    let mut config = Cluster::new("test-cluster".to_string(), 3, 0, 9, 2, true);

    for i in 0..3 {
        let node = SimpleNode {
            address: NodeAddress::new("127.0.0.1".to_string(), 4110 + i as u16),
            state: node::SimpleNodeState::Joined,
            web_port: 3000 + i as u16,
            last_seen: 0,
            hlc: HLC::default(),
            partition_item_counts: HashMap::new(),
            leading_partitions: vec![],
        };
        config.assign_node(&i, &node).unwrap();
    }

    config.assign_partition_leaders();

    // Total balance 3-3-3
    let mut leader_counts = HashMap::new();
    for i in 0..9 {
        let leader = config.partition_leaders.get(&PartitionId(i)).unwrap();
        *leader_counts.entry(*leader).or_insert(0) += 1;
    }
    assert_eq!(leader_counts.get(&0), Some(&3));
    assert_eq!(leader_counts.get(&1), Some(&3));
    assert_eq!(leader_counts.get(&2), Some(&3));

    // Simulate Node 0 dropping off
    config.unassign_node(&0, &HLC::default()).unwrap();
    config.assign_partition_leaders();

    let mut leader_counts = HashMap::new();
    for i in 0..9 {
        if let Some(leader) = config.partition_leaders.get(&PartitionId(i)) {
            *leader_counts.entry(*leader).or_insert(0) += 1;
        }
    }

    // With static priority (partition_id % 3) and RF=2:
    // P0, P3, P6 were led by N0. Replicas are {0, 1}. Only 1 available -> N1 takes 3.
    // P1, P4, P7 were led by N1. Replicas are {1, 2}. N1 is higher priority -> N1 keeps 3.
    // P2, P5, P8 were led by N2. Replicas are {2, 0}. N0 is down -> N2 keeps 3.
    // Total: N1=6, N2=3
    assert_eq!(leader_counts.get(&1), Some(&6));
    assert_eq!(leader_counts.get(&2), Some(&3));
}

#[test]
fn test_leader_election_distribution_on_rejoin() {
    let mut config = Cluster::new("test-cluster".to_string(), 3, 0, 9, 2, true);

    for i in 0..3 {
        let node = SimpleNode {
            address: NodeAddress::new("127.0.0.1".to_string(), 4110 + i as u16),
            state: node::SimpleNodeState::Joined,
            web_port: 3000 + i as u16,
            last_seen: 0,
            hlc: HLC::default(),
            partition_item_counts: HashMap::new(),
            leading_partitions: vec![],
        };
        config.assign_node(&i, &node).unwrap();
    }

    // 1. Initial balance 3-3-3
    config.assign_partition_leaders();

    // 2. Node 0 fails -> Balance shifts to N1=6, N2=3
    config.unassign_node(&0, &HLC::default()).unwrap();
    config.assign_partition_leaders();
    assert_eq!(
        config
            .partition_leaders
            .values()
            .filter(|&&n| n == 1)
            .count(),
        6
    );
    assert_eq!(
        config
            .partition_leaders
            .values()
            .filter(|&&n| n == 2)
            .count(),
        3
    );

    // 3. Node 0 rejoins -> Should balance back to 3-3-3
    let node0 = SimpleNode {
        address: NodeAddress::new("127.0.0.1".to_string(), 4110),
        state: node::SimpleNodeState::Joined,
        web_port: 3000,
        last_seen: 100,
        hlc: HLC::default(),
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![],
    };
    config.assign_node(&0, &node0).unwrap();
    config.assign_partition_leaders();

    let mut leader_counts = HashMap::new();
    for i in 0..9 {
        let leader = config.partition_leaders.get(&PartitionId(i)).unwrap();
        *leader_counts.entry(*leader).or_insert(0) += 1;
    }

    assert_eq!(leader_counts.get(&0), Some(&3));
    assert_eq!(leader_counts.get(&1), Some(&3));
    assert_eq!(leader_counts.get(&2), Some(&3));
}

#[tokio::test]
async fn test_leadership_hydrating_exclusion() {
    let mut cluster = Cluster::new("test-cluster".to_string(), 3, 0, 9, 2, true);
    let p0 = PartitionId(0);
    let p1 = PartitionId(1);
    let p2 = PartitionId(2);

    // Node 0 is ideal for P0, Node 1 for P1, Node 2 for P2
    // Set Node 0 to Hydrating, Node 1 to Syncing, Node 2 to Joined
    cluster.partition_assignments.insert(
        0,
        (
            vec![p0, p1, p2],
            AssignmentState::JoinedHydrating {
                node: AssignmentNode {
                    address: NodeAddress::new("127.0.0.1".to_string(), 8080),
                    web_port: 8080,
                    last_seen: 0,
                    hlc: HLC::default(),
                    partition_item_counts: Default::default(),
                },
            },
        ),
    );
    cluster.partition_assignments.insert(
        1,
        (
            vec![p0, p1, p2],
            AssignmentState::JoinedSyncing {
                node: AssignmentNode {
                    address: NodeAddress::new("127.0.0.1".to_string(), 8081),
                    web_port: 8081,
                    last_seen: 0,
                    hlc: HLC::default(),
                    partition_item_counts: Default::default(),
                },
            },
        ),
    );
    cluster.partition_assignments.insert(
        2,
        (
            vec![p0, p1, p2],
            AssignmentState::Joined {
                node: AssignmentNode {
                    address: NodeAddress::new("127.0.0.1".to_string(), 8082),
                    web_port: 8082,
                    last_seen: 0,
                    hlc: HLC::default(),
                    partition_item_counts: Default::default(),
                },
            },
        ),
    );

    cluster.assign_partition_leaders();

    // Node 0 is Hydrating, so it should NEVER be leader.
    // For P0 (Ideal=0):
    // Node 0: Hydrating (Excluded)
    // Node 1: JoinedSyncing (Rank 1)
    // Node 2: Joined (Rank 0) -> Winner
    assert_eq!(cluster.partition_leaders.get(&p0), Some(&2));

    // For P1 (Ideal=1):
    // Node 1: JoinedSyncing (Rank 1)
    // Node 2: Joined (Rank 0) -> Winner
    assert_eq!(cluster.partition_leaders.get(&p1), Some(&2));

    // For P2 (Ideal=2):
    // Node 2: Joined (Rank 0) -> Winner
    assert_eq!(cluster.partition_leaders.get(&p2), Some(&2));
}

#[test]
fn test_leadership_change_detection() {
    let mut config = Cluster::new("test".into(), 3, 0, 1, 3, true);

    let node0 = SimpleNode {
        address: NodeAddress::parse_unchecked("127.0.0.1:4000"),
        state: SimpleNodeState::Joined,
        web_port: 3000,
        last_seen: 1000,
        hlc: HLC::new(),
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![],
    };
    let node1 = SimpleNode {
        address: NodeAddress::parse_unchecked("127.0.0.1:4001"),
        state: SimpleNodeState::Joined,
        web_port: 3001,
        last_seen: 1000,
        hlc: HLC::new(),
        partition_item_counts: HashMap::new(),
        leading_partitions: vec![],
    };

    // First assignment - will elect node 0 as leader for P0
    let changed = config.assign_node(&0, &node0).unwrap();
    assert!(changed);
    assert_eq!(config.partition_leaders.get(&PartitionId(0)), Some(&0));

    // Assigning node 1 - node 0 is still better leader for P0 (lower index 0 vs 1)
    let changed = config.assign_node(&1, &node1).unwrap();
    // state changed (node 1 assigned) but leadership didn't change for P0
    assert!(changed);
    assert_eq!(config.partition_leaders.get(&PartitionId(0)), Some(&0));

    // Disconnecting node 0 - node 1 should take over
    let changed = config.unassign_node(&0, &HLC::new()).unwrap();
    assert!(changed);
    assert_eq!(config.partition_leaders.get(&PartitionId(0)), Some(&1));

    // Calling assign_partition_leaders manually when no change occurred
    let changed = config.assign_partition_leaders();
    assert!(!changed);
}
