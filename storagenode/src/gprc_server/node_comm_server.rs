//gprc server side functions for node to node communication
use prost::Message;
use tonic::{Request, Response, Status};
use crate::node_comm::{ForwardProposalRequest, ForwardProposalResponse, GetClusterInfoRequest, GetClusterInfoResponse, JoinClusterRequest, JoinClusterResponse, RaftMessageRequest, RaftMessageResponse, node_comm_server::NodeComm};  
use tokio::sync::{mpsc::{Sender}, oneshot};
use tokio;

pub struct NodeCoommServer{
    pub tx: Sender<crate::node::node_utils::Msg>
}

#[tonic::async_trait]
impl NodeComm for NodeCoommServer {
 
  async fn send_raft_message(&self, request: Request<RaftMessageRequest>) -> Result<Response<RaftMessageResponse>, Status> {



    //create the response 
    let response = RaftMessageResponse {
        success : true
    };

    Ok(Response::new(response))
 }

  async fn forward_proposal(&self , request: Request<ForwardProposalRequest> ) -> Result<Response<ForwardProposalResponse>, Status> {

    let response = ForwardProposalResponse {
        success : true,
        message : "Proposal forwarded successfully".to_string()
    };

    Ok(Response::new(response))
  }

 async  fn get_cluster_info (&self , request: Request<GetClusterInfoRequest> ) -> Result<Response<GetClusterInfoResponse>, Status> {
  let request = request.into_inner();
 
 let response = GetClusterInfoResponse {
    leader_id : 0,
    peers : vec![]
 };
 
 Ok(Response::new(response))   
 
 }

 async fn join_cluster(&self, request: Request<JoinClusterRequest>)-> Result<Response<JoinClusterResponse>, Status> {
  let response = JoinClusterResponse {
    success : true,
    message : "Node joined successfully".to_string(),
    peers : vec![]
  };

  Ok(Response::new(response))
 }  


}