//functions for the client side for node to node commuincation 
use tonic;
use crate::node_comm::node_comm_client::NodeCommClient;
use crate::node_comm::{RaftMessageRequest, RaftMessageResponse, ForwardProposalRequest, ForwardProposalResponse, GetClusterInfoRequest, GetClusterInfoResponse, JoinClusterRequest, JoinClusterResponse};
use tokio;



pub async fn send_raft_message() -> Result<(), Box<dyn std::error::Error>>{

    Ok(())
}

pub async fn forward_proposal() -> Result<(), Box<dyn std::error::Error>>{
    Ok(())
}


pub async fn get_cluster_info() -> Result<(), Box<dyn std::error::Error>>{
    Ok(())
}


pub async fn join_cluster() -> Result<(), Box<dyn std::error::Error>>{
    Ok(())
}