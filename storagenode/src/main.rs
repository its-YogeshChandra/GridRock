mod server;
mod db;
mod client;
pub mod storage_proto {
    tonic::include_proto!("storage_system");
}

pub mod node_comm{
    tonic::include_proto!("node_comm");
}
use std::sync::{Arc, RwLock};

use tonic::transport::Server;
use tokio;
mod node;
mod gprc_server;
use tokio::sync::mpsc::channel;
use node::node_utils::{create_raft_node, Msg};
mod grpc_client;
mod errors;
use std::collections::HashMap;

pub struct ClusterState {
    pub leader_id: u64,
    pub peers: HashMap<u64, String>,
}

struct NodeConfig {
    node_id: u64,
    port: u16,
    peer_ids: Vec<u64>,
    peer_registry: HashMap<u64, String>,
}

/// Reads node configuration from environment variables.
///
/// Required env vars:
///   - NODE_ID   : u64 — this node's raft ID (e.g. "1")
///   - NODE_PORT : u16 — gRPC port to listen on (e.g. "50051")
///   - NODE_PEERS: comma-separated list of "id:address" for all nodes including self
///                 (e.g. "1:[::1]:50051,2:[::1]:50052,3:[::1]:50053")
fn read_node_config() -> NodeConfig {
    let node_id: u64 = std::env::var("NODE_ID")
        .expect("NODE_ID env var is required")
        .parse()
        .expect("NODE_ID must be a valid u64");

    let port: u16 = std::env::var("NODE_PORT")
        .expect("NODE_PORT env var is required")
        .parse()
        .expect("NODE_PORT must be a valid u16");

    let peers_raw = std::env::var("NODE_PEERS")
        .expect("NODE_PEERS env var is required (e.g. '1:[::1]:50051,2:[::1]:50052,3:[::1]:50053')");

    let mut peer_ids: Vec<u64> = Vec::new();
    let mut peer_registry: HashMap<u64, String> = HashMap::new();

    for peer_entry in peers_raw.split(',') {
        let trimmed = peer_entry.trim();
        if trimmed.is_empty() {
            continue;
        }
        // Split on first ':' only — address itself may contain ':'
        let colon_pos = trimmed.find(':').expect("Invalid NODE_PEERS format, expected id:address");
        let id_str = &trimmed[..colon_pos];
        let addr_str = &trimmed[colon_pos + 1..];

        let peer_id: u64 = id_str.parse().expect("Invalid peer ID in NODE_PEERS");
        peer_ids.push(peer_id);
        peer_registry.insert(peer_id, addr_str.to_string());
    }

    if peer_ids.is_empty() {
        panic!("NODE_PEERS must contain at least one peer entry");
    }

    NodeConfig {
        node_id,
        port,
        peer_ids,
        peer_registry,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // Read all config from stdin
    let config = read_node_config();

    // Build cluster state (leader_id starts at 0 — updated by processor_node after election)
    let cluster_state = Arc::new(RwLock::new(ClusterState {
        leader_id: 0,
        peers: config.peer_registry,
    }));

    let cluster_for_node_comm_service = cluster_state.clone();
    let cluster_for_processor_node = cluster_state.clone();
  
    let addr = format!("0.0.0.0:{}", config.port).parse()?;
    println!("Server is listening on port {}", config.port);

    // Create the tokio mpsc channel for sending messages to the raft processor
    let (tx, rx) = channel::<Msg>(100);

    // Create the raft node with the configured ID and peer list
    let mut node = create_raft_node(config.node_id, config.peer_ids);

    //tx for the processor node 
    let tx_processor_node = tx.clone();
    
    // Spawn the raft processor loop
    tokio::spawn(async move {
        node::node_utils::processor_node(&mut node, rx, cluster_for_processor_node, tx_processor_node).await;
    });

    // Create the grpc server with both client-facing and node-to-node services
    Server::builder()
        .add_service(storage_proto::grid_rock_server::GridRockServer::new(
            server::StorageServer { tx: tx.clone() },
        ))
        .add_service(node_comm::node_comm_server::NodeCommServer::new(
            gprc_server::node_comm_server::NodeCommServer { tx: tx.clone(), cluster_state: cluster_for_node_comm_service },
        ))
        .serve(addr)
        .await?;

    Ok(())
}