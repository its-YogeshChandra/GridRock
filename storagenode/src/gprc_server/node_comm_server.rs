use prost::{Message, bytes::Bytes};
//gprc server side functions for node to node communication
use tonic::{Request, Response, Status};
use crate::node_comm::{ForwardProposalRequest, ForwardProposalResponse, GetClusterInfoRequest, GetClusterInfoResponse, JoinClusterRequest, JoinClusterResponse, RaftMessageRequest, RaftMessageResponse, node_comm_server::NodeComm, PeerInfo};  
use tokio::sync::{mpsc::{Sender}, oneshot};
use tokio;
use protobuf::Message as ProtobufMessage;
use crate::node::node_utils::{Msg, ProposeMessage, OperationType, ConfChangeMessage};
use std::sync::{Arc, RwLock};
use crate::ClusterState;
use crate::storage_proto::CreateRequest;
use crate::server::{RaftProcessedResponse, COUNTER};
use crate::errors::request_errors::ClientGrpcRequestProcessingError;
use crate::storage_proto::raft_proposal::Operation;
use crate::storage_proto::{RaftProposal};
use std::sync::atomic::{ Ordering};

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


  //send the proposal, 
  //function will be called when a follower forwards the message to the leader
  //the follower sends the already-serialized RaftProposal bytes — just pass them through
  async fn forward_proposal(&self , request: Request<ForwardProposalRequest> ) -> Result<Response<ForwardProposalResponse>, Status> {
  
        let request_val = request.into_inner();
        let proposal_data = request_val.proposal_data;

        // Decode the RaftProposal to determine operation type (the bytes are already a valid RaftProposal)
        let raft_proposal = RaftProposal::decode(proposal_data.as_slice())
            .map_err(|e| Status::internal(format!("failed to decode forwarded proposal: {}", e)))?;

        let operation_type = match &raft_proposal.operation {
            Some(Operation::Create(_)) => OperationType::Create,
            Some(Operation::Update(_)) => OperationType::Update,
            Some(Operation::Delete(_)) => OperationType::Delete,
            Some(Operation::Get(_)) => OperationType::Get,
            None => return Err(Status::internal("forwarded proposal has no operation")),
        };

        //use the tokio oneshot to create 
        let (tx, rx) = oneshot::channel::<Result<RaftProcessedResponse, ClientGrpcRequestProcessingError>>();
        
        // Use the original proposal_id from the forwarded proposal
        let id = raft_proposal.proposal_id;

        let propose_msg_data: ProposeMessage = ProposeMessage{
            id,
            data: proposal_data, // pass the raw bytes through — already a valid RaftProposal
            operation_type,
            response_tx: tx 
        };

        self.tx.send(Msg::Propose { proposemsg: propose_msg_data }).await.map_err(|e| Status::internal(e.to_string()))?;

        let result = rx.await.map_err(|e| Status::internal(e.to_string()))?;
        match result {
            Ok(_) => {
              let response_val = ForwardProposalResponse{
               success: true, 
               message: "proposal forwarded and committed successfully".to_string()
              };
                                
                Ok(Response::new(response_val))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
  }

  //get the cluster info 
 async  fn get_cluster_info (&self , _request: Request<GetClusterInfoRequest> ) -> Result<Response<GetClusterInfoResponse>, Status> {

let state = self.cluster_state.read().unwrap();
//create vector of peer info by iterating through hashmap from state cluster 
let mut peers = Vec::new();

for (node_id, addr) in state.peers.iter() {
    peers.push(PeerInfo { node_id: *node_id, address: addr.clone() });
}

let response = GetClusterInfoResponse {
    leader_id : state.leader_id,
    peers : peers
 };

 Ok(Response::new(response))   
 
 }

 //function to handle join cluster request 
 async fn join_cluster(&self, request: Request<JoinClusterRequest>)-> Result<Response<JoinClusterResponse>, Status> {
 
 let request_val = request.into_inner();

 //check if node id from request is already exists in the cluster 
 //send the error back to the client 
 let peers ={
  let state = self.cluster_state.read().unwrap();
  state.peers.clone()
 };
  
 if peers.contains_key(&request_val.node_id) {
  return Err(Status::internal("Node already exists in the cluster")); 
 }

 // create oneshot channel to wait for raft commit result
 let (tx, rx) = oneshot::channel::<Result<RaftProcessedResponse, ClientGrpcRequestProcessingError>>();

 let id = COUNTER.fetch_add(1, Ordering::SeqCst); 

 // forge the conf change message 
 let cc = raft::eraftpb::ConfChange{
    node_id: request_val.node_id,
    change_type: raft::eraftpb::ConfChangeType::AddNode,
    context: Bytes::from(request_val.address),
    id: id,
    unknown_fields: protobuf::UnknownFields::default(),
    cached_size: protobuf::CachedSize::default(), 
 };

 // create raft message used in msg enum confchange field   
 let cc_msg = ConfChangeMessage{
    id: id,
    cc: cc,
    response_tx: tx,
 };

 self.tx.send(Msg::ConfChange { confchange_msg: cc_msg }).await.map_err(|e| Status::internal(e.to_string()))?;

 // wait for the conf change to be committed by raft
 let result = rx.await.map_err(|e| Status::internal(e.to_string()))?;
 match result {
    Ok(_) => {
        // Build peer list from the current cluster state after commit
        let peer_list = {
            let state = self.cluster_state.read().unwrap();
            state.peers.iter().map(|(id, addr)| PeerInfo {
                node_id: *id,
                address: addr.clone(),
            }).collect()
        };

        let response = JoinClusterResponse {
            success: true,
            message: "Node joined successfully".to_string(),
            peers: peer_list,
        };
        Ok(Response::new(response))
    }
    Err(e) => Err(Status::internal(e.to_string())),
 }
 }  


}