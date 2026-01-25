use std::fs;
use std::io;
use std::path::PathBuf;

const DEFAULT_DIR: &str = ".gossipgrid";
const CLUSTERS_DIR: &str = "clusters";
const WAL_DIR: &str = "wal";

fn app_root() -> PathBuf {
    let base_dir_override = std::env::var("GOSSIPGRID_BASE_DIR");

    if let Ok(base_dir_override) = base_dir_override {
        PathBuf::from(base_dir_override)
    } else {
        std::env::var("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(DEFAULT_DIR)
    }
}

pub fn clusters_dir() -> PathBuf {
    app_root().join(CLUSTERS_DIR)
}

pub fn ensure_clusters_dir(name: &str) -> Result<PathBuf, io::Error> {
    let dir = clusters_dir().join(name);
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

pub fn wal_dir(cluster_name: &str) -> PathBuf {
    clusters_dir().join(cluster_name).join(WAL_DIR)
}

pub fn cluster_metadata_path(cluster_name: &str) -> PathBuf {
    clusters_dir()
        .join(cluster_name)
        .join(format!("{}.json", &cluster_name))
}

pub fn node_md_state_path(cluster_name: &str, node_index: u8) -> PathBuf {
    clusters_dir()
        .join(cluster_name)
        .join(format!("node_{node_index}.json"))
}
