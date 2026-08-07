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
use std::io::Write;

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

/// Reads node configuration from stdin at startup.
///
/// Prompts for:
///   - Node ID (u64)
///   - Port (u16)  
///   - Number of peers in the cluster (including self)
///   - For each peer: "id:address" (e.g. "2:[::1]:50052")
///
/// Returns a NodeConfig with all parsed values.
fn read_node_config() -> NodeConfig {
    let mut input = String::new();

    // Read node ID
    print!("Enter node ID: ");
    std::io::stdout().flush().unwrap();
    std::io::stdin().read_line(&mut input).expect("Failed to read node ID");
    let node_id: u64 = input.trim().parse().expect("Invalid node ID");
    input.clear();

    // Read port
    print!("Enter port: ");
    std::io::stdout().flush().unwrap();
    std::io::stdin().read_line(&mut input).expect("Failed to read port");
    let port: u16 = input.trim().parse().expect("Invalid port");
    input.clear();

    // Read number of peers
    print!("Enter number of peers (including self): ");
    std::io::stdout().flush().unwrap();
    std::io::stdin().read_line(&mut input).expect("Failed to read peer count");
    let peer_count: usize = input.trim().parse().expect("Invalid peer count");
    input.clear();

    // Read each peer as "id:address"
    let mut peer_ids: Vec<u64> = Vec::new();
    let mut peer_registry: HashMap<u64, String> = HashMap::new();

    for i in 0..peer_count {
        print!("Enter peer {} (id:address, e.g. 2:[::1]:50052): ", i + 1);
        std::io::stdout().flush().unwrap();
        std::io::stdin().read_line(&mut input).expect("Failed to read peer");

        let trimmed = input.trim();
        // Split on first ':' only — address itself may contain ':'
        let colon_pos = trimmed.find(':').expect("Invalid format, expected id:address");
        let id_str = &trimmed[..colon_pos];
        let addr_str = &trimmed[colon_pos + 1..];

        let peer_id: u64 = id_str.parse().expect("Invalid peer ID");
        peer_ids.push(peer_id);
        peer_registry.insert(peer_id, addr_str.to_string());

        input.clear();
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
  
    let addr = format!("[::1]:{}", config.port).parse()?;
    println!("Server is listening on port {}", config.port);

    // Create the mpsc channel for sending messages to the raft processor
    let (tx, rx) = channel::<Msg>(100);

    // Create the raft node with the configured ID and peer list
    let mut node = create_raft_node(config.node_id, config.peer_ids);

    // Spawn the raft processor loop
    tokio::spawn(async move {
        node::node_utils::processor_node(&mut node, rx, cluster_for_processor_node).await;
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