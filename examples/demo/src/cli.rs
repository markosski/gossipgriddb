use clap::{Command, Parser};

const WEB_PORT_DEFAULT: &str = "3001";

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct NodeCliArgs {
    #[arg(short, long)]
    name: String,

    #[arg(short, long, default_value_t = 1)]
    count: u8,
}

pub fn node_cli() -> Command {
    Command::new("gossipgrid")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .disable_help_flag(true)
        .subcommand(
            Command::new("create")
                .about("Create a new cluster")
                .arg(
                    clap::Arg::new("name")
                        .long("name")
                        .value_name("NAME")
                        .help("Name of the cluster")
                        .required(true),
                )
                .arg(
                    clap::Arg::new("size")
                        .long("size")
                        .value_name("SIZE")
                        .help("Number of nodes in the cluster")
                        .required(true),
                )
                .arg(
                    clap::Arg::new("partitions")
                        .long("partitions")
                        .value_name("PARTITIONS")
                        .help("Number of partitions in the cluster")
                        .required(true),
                )
                .arg(
                    clap::Arg::new("replication")
                        .long("replication")
                        .value_name("REPLICATION")
                        .help("Replication factor")
                        .required(true),
                ),
        )
        .subcommand(
            Command::new("start")
                .about("Start existing or ephemeral cluster node")
                .arg(
                    clap::Arg::new("name")
                        .long("name")
                        .value_name("NAME")
                        .help("Name of the cluster to start")
                        .required(false),
                )
                .arg(
                    clap::Arg::new("ephemeral")
                        .long("ephemeral")
                        .required(false)
                        .num_args(0)
                        .help("Start an ephemeral cluster")
                        .requires_all(["size", "partition-size", "replication-factor"]),
                )
                .group(
                    clap::ArgGroup::new("identity")
                        .args(["name", "ephemeral"])
                        .required(true)
                        .multiple(false),
                )
                .arg(
                    clap::Arg::new("size")
                        .short('s')
                        .long("size")
                        .value_name("SIZE")
                        .help("Size of the cluster")
                        .conflicts_with("name")
                        .required(false),
                )
                .arg(
                    clap::Arg::new("partition-size")
                        .short('p')
                        .long("partition-size")
                        .value_name("PARTITION_SIZE")
                        .help("Number of partitions")
                        .conflicts_with("name")
                        .required(false),
                )
                .arg(
                    clap::Arg::new("replication-factor")
                        .short('r')
                        .long("replication-factor")
                        .value_name("REPLICATION_FACTOR")
                        .help("Replication factor")
                        .conflicts_with("name")
                        .required(false),
                )
                .arg(
                    clap::Arg::new("host")
                        .short('h')
                        .long("host")
                        .value_name("HOST")
                        .required(true),
                )
                .arg(
                    clap::Arg::new("web-port")
                        .short('w')
                        .long("web-port")
                        .value_name("WEB_PORT")
                        .default_value(WEB_PORT_DEFAULT),
                ),
        )
        .subcommand(Command::new("list").about("List all clusters"))
        .subcommand(
            Command::new("join")
                .about("Join an existing cluster")
                .arg(
                    clap::Arg::new("name")
                        .long("name")
                        .value_name("NAME")
                        .help("Name of the cluster to join")
                        .required(false),
                )
                .arg(
                    clap::Arg::new("ephemeral")
                        .long("ephemeral")
                        .required(false)
                        .num_args(0)
                        .help("Join an ephemeral cluster"),
                )
                .group(
                    clap::ArgGroup::new("identity")
                        .args(["name", "ephemeral"])
                        .required(true)
                        .multiple(false),
                )
                .arg(
                    clap::Arg::new("host")
                        .short('h')
                        .long("host")
                        .value_name("HOST")
                        .required(true),
                )
                .arg(
                    clap::Arg::new("peer-address")
                        .short('a')
                        .long("peer-address")
                        .value_name("PEER_ADDRESS")
                        .help("Network address of the peer node to join")
                        .required(true),
                )
                .arg(
                    clap::Arg::new("web-port")
                        .short('w')
                        .long("web-port")
                        .value_name("WEB_PORT")
                        .default_value(WEB_PORT_DEFAULT),
                ),
        )
}
