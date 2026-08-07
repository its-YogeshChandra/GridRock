//gprc server side functions for node to node communication
use tonic::{Request, Response, Status};
use crate::node_comm::{ForwardProposalRequest, ForwardProposalResponse, GetClusterInfoRequest, GetClusterInfoResponse, JoinClusterRequest, JoinClusterResponse, RaftMessageRequest, RaftMessageResponse, node_comm_server::NodeComm, PeerInfo};  
use tokio::sync::{mpsc::{Sender}, oneshot};
use tokio;
use protobuf::Message as ProtobufMessage;
use crate::node::node_utils::{Msg, ProposeMessage};
use std::sync::{Arc, RwLock};
use crate::ClusterState;

pub struct NodeCommServer{
    pub tx: Sender<crate::node::node_utils::Msg>,
    pub cluster_state: Arc<RwLock<ClusterState>>,
}

#[tonic::async_trait]
impl NodeComm for NodeCommServer {

  //sending message to raft 
  async fn send_raft_message(&self, request: Request<RaftMessageRequest>) -> Result<Response<RaftMessageResponse>, Status> {
  let request = request.into_inner();

  //deserealize the bytes into raftmsg 
  let msg = raft::eraftpb::Message::parse_from_bytes(&request.message)
    .map_err(|e| Status::internal(format!("decode failed: {}", e)))?;

   //create the raft message 
   let raft_msg = Msg::Raft(msg);
   self.tx.send(raft_msg).await.map_err(|e| Status::internal(format!("send failed: {}", e)))?;

    //create the response
    let response = RaftMessageResponse {
        success : true
    };

    Ok(Response::new(response))
 }


 //send the proosal 
  async fn forward_proposal(&self , request: Request<ForwardProposalRequest> ) -> Result<Response<ForwardProposalResponse>, Status> {
  
  let request = request.into_inner();
 
   //create the proposal 
   let proposal_data = raft::eraftpb::Message::parse_from_bytes(&request.proposal_data)
    .map_err(|e| Status::internal(format!("decode failed: {}", e)))?;

    let raft_msg = Msg::Raft(proposal_data);
    self.tx.send(raft_msg).await.map_err(|e| Status::internal(format!("send failed: {}", e)))?;

    //create the response 
    let response = ForwardProposalResponse {
        success : true,
        message : "Proposal forwarded successfully".to_string()
    };

    Ok(Response::new(response))
  }

  //get the cluster info 
 async  fn get_cluster_info (&self , _request: Request<GetClusterInfoRequest> ) -> Result<Response<GetClusterInfoResponse>, Status> {

let state = self.cluster_state.read().unwrap();
//create vector of peer info by iterating through hashmap from state cluster 
let mut peers = Vec::new();

for values in self.cluster_state.read().unwrap().peers.iter().enumerate(){
   let peer_info = PeerInfo {
    node_id : values.0 as u64,
    address : values.1.1.to_string(),
   };
   peers.push(peer_info);
}

 let response = GetClusterInfoResponse {
    leader_id : state.leader_id,
    peers : peers
 };

 

 Ok(Response::new(response))   
 
 }

 async fn join_cluster(&self, request: Request<JoinClusterRequest>)-> Result<Response<JoinClusterResponse>, Status> {
 let request = request.into_inner();
 
 
 
  let response = JoinClusterResponse {
    success : true,
    message : "Node joined successfully".to_string(),
    peers : vec![]
  };

  Ok(Response::new(response))
 }  


}