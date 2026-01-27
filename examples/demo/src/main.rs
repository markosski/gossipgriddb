use demo::cli;
use gossipgrid::prelude::*;

/// Demo CLI for GossipGrid
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), NodeError> {
    env_logger::init();

    let cli_matched = cli::node_cli().get_matches();
    match cli_matched.subcommand() {
        Some(("create", create_matches)) => {
            let name = create_matches
                .get_one::<String>("name")
                .expect("Cluster name is required");
            let size: u8 = create_matches
                .get_one::<String>("size")
                .unwrap()
                .parse()
                .expect("Invalid cluster size");
            let partitions: u16 = create_matches
                .get_one::<String>("partitions")
                .unwrap()
                .parse()
                .expect("Invalid partition count");
            let replication: u8 = create_matches
                .get_one::<String>("replication")
                .unwrap()
                .parse()
                .expect("Invalid replication factor");

            if replication > size {
                eprintln!("Error: replication factor must be less than or equal to cluster size.");
                std::process::exit(1);
            }

            if gossipgrid::cluster::Cluster::exists(name) {
                eprintln!("Error: Cluster '{name}' already exists.");
                std::process::exit(0);
            }

            let cluster = gossipgrid::cluster::Cluster::new(
                name.clone(),
                size,
                0,
                partitions,
                replication,
                false,
            );

            cluster.save().expect("Failed to save cluster metadata");
            Ok(())
        }
        Some(("list", _)) => {
            let clusters =
                gossipgrid::cluster::Cluster::list_clusters().expect("Failed to list clusters");
            if clusters.is_empty() {
                println!("No clusters found.");
            } else {
                println!("Available clusters:");
                for cluster in clusters {
                    println!("  - {cluster}");
                }
            }
            Ok(())
        }
        Some(("start", start_matches)) => {
            let name = start_matches.get_one::<String>("name");
            let ephemeral = start_matches.get_flag("ephemeral");

            let host: String = start_matches
                .get_one::<String>("host")
                .unwrap()
                .parse()
                .expect("Invalid host IP address");
            let web_port: u16 = start_matches
                .get_one::<String>("web-port")
                .unwrap()
                .parse()
                .expect("Invalid web port");

            // Build node using NodeBuilder
            let mut builder = NodeBuilder::new().address(&host)?.web_port(web_port);

            if ephemeral {
                let size: u8 = start_matches
                    .get_one::<String>("size")
                    .unwrap()
                    .parse()
                    .expect("Invalid size");
                let partition_size: u16 = start_matches
                    .get_one::<String>("partition-size")
                    .unwrap()
                    .parse()
                    .expect("Invalid partition size");
                let replication_factor: u8 = start_matches
                    .get_one::<String>("replication-factor")
                    .unwrap()
                    .parse()
                    .expect("Invalid replication factor");

                builder = builder.ephemeral(size, partition_size, replication_factor);
            } else if let Some(name) = name {
                let cluster_config = gossipgrid::cluster::Cluster::load(name)
                    .expect("Failed to load cluster metadata, did you create cluster first?");
                builder = builder.cluster(cluster_config);
            }

            let node = builder.build().await?;
            node.run_until_shutdown().await
        }
        Some(("join", sub_matches)) => {
            let name = sub_matches.get_one::<String>("name");
            let ephemeral = sub_matches.get_flag("ephemeral");
            let peer_address = sub_matches
                .get_one::<String>("peer-address")
                .expect("Address is required");
            let host: String = sub_matches
                .get_one::<String>("host")
                .unwrap()
                .parse()
                .expect("Invalid host IP address");
            let web_port: u16 = sub_matches
                .get_one::<String>("web-port")
                .unwrap()
                .parse()
                .expect("Invalid web port");

            // Build node using NodeBuilder
            let mut builder = NodeBuilder::new().address(&host)?.web_port(web_port);

            // Use join_ephemeral for ephemeral clusters, join_peer for persistent
            if ephemeral {
                builder = builder.join_ephemeral(peer_address)?;
            } else {
                builder = builder.join_peer(peer_address)?;
                if let Some(name) = name
                    && let Ok(cluster_config) = gossipgrid::cluster::Cluster::load(name)
                {
                    builder = builder.cluster(cluster_config);
                }
            }

            let node = builder.build().await?;
            node.run_until_shutdown().await
        }
        _ => Err(NodeError::ConfigurationError("Invalid command".to_string())),
    }
}
