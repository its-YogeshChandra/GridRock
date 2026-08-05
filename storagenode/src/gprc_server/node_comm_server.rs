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
 
  async fn send_raft_message(&self, request: Request<RaftMessageRequest>) -> Result<Response<ForwardProposalRequest>, Status> {

 } 


  async fn forward_proposal(&self , request: Request<ForwardProposalRequest> ) -> Result<Response<ForwardProposalResponse>, Status> {

 }

 async  fn get_cluster_info (&self , request: Request<GetClusterInfoRequest> ) -> Result<Response<GetClusterInfoResponse>, Status> {

 }

 async fn join_cluster(&self, request: Request<JoinClusterRequest>)-> Result<Response<JoinClusterResponse>, Status> {

 }  


}