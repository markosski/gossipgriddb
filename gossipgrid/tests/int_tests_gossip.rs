use gossipgrid::{
    cluster::{AssignmentState, PartitionId},
    node::NodeState,
};
use log::info;

use crate::helpers::start_test_cluster;

mod helpers;

#[tokio::test]
async fn test_node1_drops() {
    let _ = env_logger::try_init();

    let nodes = start_test_cluster(6, 2).await;
    // wait for cluster to stabilize
    tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;

    {
        let node0 = nodes[0].1.read().await;
        if let NodeState::Joined(node) = &*node0 {
            let mut node0_partitions = node.cluster.get_assigned_partitions(0).unwrap();
            let mut node1_partitions = node.cluster.get_assigned_partitions(1).unwrap();
            let mut node2_partitions = node.cluster.get_assigned_partitions(2).unwrap();

            node0_partitions.sort();
            assert_eq!(
                node0_partitions,
                vec!(
                    PartitionId(0),
                    PartitionId(1),
                    PartitionId(3),
                    PartitionId(4),
                    PartitionId(5)
                )
            );
            assert_eq!(node.cluster.partition_leaders[&PartitionId(0)], 0);
            assert_eq!(node.cluster.partition_leaders[&PartitionId(3)], 0);
            assert_eq!(node.cluster.partition_leaders[&PartitionId(5)], 0);

            node1_partitions.sort();
            assert_eq!(node1_partitions, vec!(PartitionId(2), PartitionId(5)));
            // Node 1 (index 1) doesn't lead anything in the initial state as Node 0 and Node 2 are preferred

            node2_partitions.sort();
            assert_eq!(
                node2_partitions,
                vec!(
                    PartitionId(0),
                    PartitionId(1),
                    PartitionId(2),
                    PartitionId(3),
                    PartitionId(4)
                )
            );
            assert_eq!(node.cluster.partition_leaders[&PartitionId(1)], 2);
            assert_eq!(node.cluster.partition_leaders[&PartitionId(2)], 2);
            assert_eq!(node.cluster.partition_leaders[&PartitionId(4)], 2);
        }
    }

    // kill first node
    let first_node = &nodes[0];
    first_node.0.as_ref().unwrap().abort();

    // keep checking nodes till nodes converge to acknowleding node0 is down
    loop {
        let node1 = nodes[1].1.read().await;
        if let NodeState::Joined(node) = &*node1 {
            let node0_assignments = &node.cluster.partition_assignments[&0];
            if matches!(node0_assignments.1, AssignmentState::Disconnected { .. }) {
                // Now node 1 is the leader
                info!("node1 known leader {:?}", &node.cluster.partition_leaders);
                assert_eq!(node.cluster.partition_leaders[&PartitionId(0)], 2);
                assert_eq!(node.cluster.partition_leaders[&PartitionId(1)], 2);
                assert_eq!(node.cluster.partition_leaders[&PartitionId(2)], 2);
                assert_eq!(node.cluster.partition_leaders[&PartitionId(3)], 2);
                assert_eq!(node.cluster.partition_leaders[&PartitionId(4)], 2);
                assert_eq!(node.cluster.partition_leaders[&PartitionId(5)], 1);
                break;
            } else {
                info!("node0 not disconnected yet will wait...");
                tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
            }
        }
    }

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}
