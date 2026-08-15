//functions for the client side for node to node communication
use crate::node_comm::node_comm_client::NodeCommClient;
use crate::node_comm::{
    RaftMessageRequest, RaftMessageResponse,
    ForwardProposalRequest, ForwardProposalResponse,
    GetClusterInfoRequest, GetClusterInfoResponse,
    JoinClusterRequest, JoinClusterResponse,
};

/// Helper to build a gRPC endpoint URL from a host string and port.
/// Accepts hostnames, IPv4, or IPv6 addresses.
fn build_endpoint(host: &str, port: u16) -> String {
    // If the host looks like an IPv6 address (contains ':'), wrap in brackets
    if host.contains(':') {
        format!("http://[{}]:{}", host, port)
    } else {
        format!("http://{}:{}", host, port)
    }
}

/// Sends a serialized raft message (AppendEntries, Vote, Heartbeat, etc.) to a peer node.
/// The `message` field in RaftMessageRequest should contain the protobuf-serialized eraftpb::Message bytes.
pub async fn send_raft_message(
    peer_host: &str,
    port: u16,
    message: RaftMessageRequest,
) -> Result<RaftMessageResponse, Box<dyn std::error::Error>> {
    let endpoint = build_endpoint(peer_host, port);
    let mut client = NodeCommClient::connect(endpoint).await?;

    let response = client.send_raft_message(message).await?;
    Ok(response.into_inner())
}

/// Forwards a client write proposal to the leader node.
/// Called when a follower receives a client write request and needs the leader to propose it.
pub async fn forward_proposal(
    peer_host: &str,
    port: u16,
    proposal_data: Vec<u8>,
    sender_node_id: u64,
) -> Result<ForwardProposalResponse, Box<dyn std::error::Error>> {
    let endpoint = build_endpoint(peer_host, port);
    let mut client = NodeCommClient::connect(endpoint).await?;

    let request = ForwardProposalRequest {
        proposal_data,
        sender_node_id,
    };

    let response = client.forward_proposal(request).await?;
    Ok(response.into_inner())
}

/// Queries the current cluster membership from a peer node.
pub async fn get_cluster_info(
    peer_host: &str,
    port: u16,
) -> Result<GetClusterInfoResponse, Box<dyn std::error::Error>> {
    let endpoint = build_endpoint(peer_host, port);
    let mut client = NodeCommClient::connect(endpoint).await?;

    let request = GetClusterInfoRequest {};

    let response = client.get_cluster_info(request).await?;
    Ok(response.into_inner())
}

/// Sends a join cluster request to an existing node in the cluster.
/// Called by a new node that wants to join the raft cluster.
pub async fn join_cluster(
    peer_host: &str,
    port: u16,
    node_id: u64,
    self_address: String,
) -> Result<JoinClusterResponse, Box<dyn std::error::Error>> {
    let endpoint = build_endpoint(peer_host, port);
    let mut client = NodeCommClient::connect(endpoint).await?;

    let request = JoinClusterRequest {
        node_id,
        address: self_address,
    };

    let response = client.join_cluster(request).await?;
    Ok(response.into_inner())
}
