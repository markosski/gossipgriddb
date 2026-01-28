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
            let node0_partitions = node.cluster.get_assigned_partitions(0).unwrap();
            let node1_partitions = node.cluster.get_assigned_partitions(1).unwrap();
            let node2_partitions = node.cluster.get_assigned_partitions(2).unwrap();

            assert!(
                node0_partitions
                    == vec!(
                        PartitionId(0), // L
                        PartitionId(2), // L
                        PartitionId(3), // L
                        PartitionId(5)  // L
                    )
            );
            info!("partition leaders: {:?}", node.cluster.partition_leaders);
            assert!(node.cluster.partition_leaders[&PartitionId(0)] == 0);
            assert!(node.cluster.partition_leaders[&PartitionId(3)] == 0);

            assert!(
                node1_partitions
                    == vec!(
                        PartitionId(0), // R
                        PartitionId(1), // L
                        PartitionId(3), // R
                        PartitionId(4)  // L
                    )
            );
            assert!(node.cluster.partition_leaders[&PartitionId(1)] == 1);
            assert!(node.cluster.partition_leaders[&PartitionId(4)] == 1);

            assert!(
                node2_partitions
                    == vec!(
                        PartitionId(1), // R
                        PartitionId(2), // R
                        PartitionId(4), // R
                        PartitionId(5)  // R
                    )
            );
            assert!(node.cluster.partition_leaders[&PartitionId(2)] == 2);
            assert!(node.cluster.partition_leaders[&PartitionId(5)] == 2);
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
                assert!(node.cluster.partition_leaders[&PartitionId(0)] == 1);
                assert!(node.cluster.partition_leaders[&PartitionId(1)] == 1);
                assert!(node.cluster.partition_leaders[&PartitionId(2)] == 2);
                assert!(node.cluster.partition_leaders[&PartitionId(3)] == 1);
                assert!(node.cluster.partition_leaders[&PartitionId(4)] == 1);
                assert!(node.cluster.partition_leaders[&PartitionId(5)] == 2);
                break;
            } else {
                info!("node0 not disconnected yet will wait...");
                tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
            }
        }
    }

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}
