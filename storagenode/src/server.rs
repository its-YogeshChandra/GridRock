use prost::Message;
use tonic::{Request, Response, Status};
use crate::storage_proto::raft_proposal::Operation;
use crate::storage_proto::{
    CreateRequest, DelValRequest, PutRequest, GetValRequest, StorageResponse, RaftProposal,
    grid_rock_server::GridRock,
};
use tokio::sync::{mpsc::{Sender}, oneshot};
use crate::node::node_utils::{Msg, OperationType, ProposeMessage};
use crate::errors::request_errors::ClientGrpcRequestProcessingError;

use std::sync::atomic::{AtomicU64, Ordering};
pub struct StorageServer{
    pub tx: Sender<crate::node::node_utils::Msg>
}

#[derive(Debug)]
pub struct RaftProcessedResponse{
    pub id : Option<String>,
    pub success : bool,
    pub data : Option<CreateRequest>
}


//counter for the id of the request send by grpc handler to the processing node loop 
static COUNTER: AtomicU64 = AtomicU64::new(1);
static NODE_ID: AtomicU64 = AtomicU64::new(0);

/// Call once at startup to set this node's ID for proposal ID generation.
pub fn init_node_id(node_id: u64) {
    NODE_ID.store(node_id, Ordering::SeqCst);
}

/// Generates a globally unique proposal ID: high 16 bits = node_id, low 48 bits = counter.
/// This ensures IDs from different nodes never clash.
pub fn next_proposal_id() -> u64 {
    let nid = NODE_ID.load(Ordering::SeqCst);
    let seq = COUNTER.fetch_add(1, Ordering::SeqCst);
    (nid << 48) | (seq & 0x0000_FFFF_FFFF_FFFF)
}

#[tonic::async_trait]
impl GridRock for StorageServer {
    /// Creates a new entry in storage. Fails if the unique_id already exists.
    async fn create_valin_storage(
        &self,
        request: Request<PutRequest>,
    ) -> Result<Response<StorageResponse>, Status> //need to update the response type
    {
        //check for node responsible for key range (testing against config from shard controller)
        let request_val = request.into_inner();
        let unique_id = request_val.unique_id.clone();
        eprintln!("[gRPC] CREATE received | id={}", unique_id);

        //use the tokio oneshot to create 
        let (tx, rx) = oneshot::channel::<Result<RaftProcessedResponse, ClientGrpcRequestProcessingError>>();
        
        //forge the propose msg for raft 
        let id = next_proposal_id();

        let raft_proposal = RaftProposal{
           proposal_id : id, 
           operation:Some(Operation::Create(request_val))
        };

        let mut data_buffer = Vec::new();
        raft_proposal.encode(&mut data_buffer).map_err(|e| Status::internal(format!("server error: {}", e)))?;
        
        let propose_msg_data: ProposeMessage = ProposeMessage{
            id : id,
             data : data_buffer,
             operation_type : OperationType::Create,
             response_tx: tx 
        };

        eprintln!("[gRPC] CREATE sending to raft | proposal_id={:#018x}", id);
        self.tx.send(Msg::Propose { proposemsg: propose_msg_data }).await.map_err(|e| Status::internal(e.to_string()))?;

        let result = rx.await.map_err(|e| Status::internal(e.to_string()))?;
        match result {
            Ok(_) => {
                eprintln!("[gRPC] CREATE success | id={}", unique_id);
                let response_val = StorageResponse {
                    message: format!("Value with key '{}' successfully created", unique_id),
                    success: true,
                    ..Default::default()
                };
                Ok(Response::new(response_val))
            }
            Err(e) => {
                eprintln!("[gRPC] CREATE error | id={} err={}", unique_id, e);
                Err(Status::internal(e.to_string()))
            }
        }
           }

    /// Updates an existing entry's balance. Fails if the unique_id does not exist.
    async fn update_valin_storage(
        &self,
        request: Request<PutRequest>,
    ) -> Result<Response<StorageResponse>, Status> {
        
        let request_val = request.into_inner();
        let unique_id = request_val.unique_id.clone();
        eprintln!("[gRPC] UPDATE received | id={}", unique_id);

        //use the tokio oneshot to create 
        let (tx, rx) = oneshot::channel::<Result<RaftProcessedResponse, ClientGrpcRequestProcessingError>>();
        
        //forge the propose msg for raft 
        let id = next_proposal_id();

        let raft_proposal = RaftProposal{
           proposal_id : id, 
           operation:Some(Operation::Update(request_val))
        };
        let mut data_buffer = Vec::new();
        raft_proposal.encode(&mut data_buffer).map_err(|e| Status::internal(format!("server error: {}", e)))?;
        
        let propose_msg_data: ProposeMessage = ProposeMessage{
            id : id,
             data : data_buffer,
             operation_type : OperationType::Update,
             response_tx: tx 
        };

        eprintln!("[gRPC] UPDATE sending to raft | proposal_id={:#018x}", id);
        self.tx.send(Msg::Propose { proposemsg: propose_msg_data }).await.map_err(|e| Status::internal(e.to_string()))?;

        let result = rx.await.map_err(|e| Status::internal(e.to_string()))?;
        match result {
            Ok(_) => {
                eprintln!("[gRPC] UPDATE success | id={}", unique_id);
                let response_val = StorageResponse {
                    message: format!("Value with key '{}' successfully updated", unique_id),
                    success: true,
                    ..Default::default()
                };
                Ok(Response::new(response_val))
            }
            Err(e) => {
                eprintln!("[gRPC] UPDATE error | id={} err={}", unique_id, e);
                Err(Status::internal(e.to_string()))
            }
        }
    }

    /// Retrieves an entry from storage by unique_id. Returns its fields in the response message.
    async fn get_valfrom_storage(
        &self,
        request: Request<GetValRequest>,
    ) -> Result<Response<StorageResponse>, Status> {

        let request_val = request.into_inner();
        let unique_id = request_val.unique_id.clone();
        eprintln!("[gRPC] GET received | id={}", unique_id);

        //use the tokio oneshot to create 
        let (tx, rx) = oneshot::channel::<Result<RaftProcessedResponse, ClientGrpcRequestProcessingError>>();
        
        //forge the propose msg for raft 
        let id = next_proposal_id();

        let raft_proposal = RaftProposal{
           proposal_id : id, 
           operation:Some(Operation::Get(request_val))
        };
        let mut data_buffer = Vec::new();
        raft_proposal.encode(&mut data_buffer).map_err(|e| Status::internal(format!("server error: {}", e)))?;
        
        let propose_msg_data: ProposeMessage = ProposeMessage{
            id : id,
             data : data_buffer,
             operation_type : OperationType::Get,
             response_tx: tx 
        };

        eprintln!("[gRPC] GET sending to raft | proposal_id={:#018x}", id);
        self.tx.send(Msg::Propose { proposemsg: propose_msg_data }).await.map_err(|e| Status::internal(e.to_string()))?;

        let result = rx.await.map_err(|e| Status::internal(e.to_string()))?;
        match result {
            Ok(response) => {
                eprintln!("[gRPC] GET success | id={}", unique_id);
                let mut response_val = StorageResponse {
                    message: format!("Value with key '{}' retrieved successfully", unique_id),
                    success: true,
                    ..Default::default()
                };


                // Populate the data fields from the record if present
                if let Some(record) = response.data {
                    response_val.data = Some(record.encode_to_vec());
                }

                Ok(Response::new(response_val))
            }
            Err(e) => {
                eprintln!("[gRPC] GET error | id={} err={}", unique_id, e);
                Err(Status::internal(e.to_string()))
            }
        } 
    }

    /// Deletes an entry from storage by unique_id. Fails if the key does not exist.
    async fn del_valfrom_storage(
        &self,
        request: Request<DelValRequest>,
    ) -> Result<Response<StorageResponse>, Status> {
        let request_val = request.into_inner();
        let unique_id = request_val.unique_id.clone();
        eprintln!("[gRPC] DELETE received | id={}", unique_id);

        //use the tokio oneshot to create 
        let (tx, rx) = oneshot::channel::<Result<RaftProcessedResponse, ClientGrpcRequestProcessingError>>();
        
        //forge the propose msg for raft 
        let id = next_proposal_id();

        let raft_proposal = RaftProposal{
           proposal_id : id, 
           operation:Some(Operation::Delete(request_val))
        };
        let mut data_buffer = Vec::new();
        raft_proposal.encode(&mut data_buffer).map_err(|e| Status::internal(format!("server error: {}", e)))?;
        
        let propose_msg_data: ProposeMessage = ProposeMessage{
            id : id,
             data : data_buffer,
             operation_type : OperationType::Delete,
             response_tx: tx 
        };

        eprintln!("[gRPC] DELETE sending to raft | proposal_id={:#018x}", id);
        self.tx.send(Msg::Propose { proposemsg: propose_msg_data }).await.map_err(|e| Status::internal(e.to_string()))?;

        let result = rx.await.map_err(|e| Status::internal(e.to_string()))?;
        match result {
            Ok(_) => {
                eprintln!("[gRPC] DELETE success | id={}", unique_id);
                let response_val = StorageResponse {
                    message: format!("Value with key '{}' successfully deleted", unique_id),
                    success: true,
                    ..Default::default()
                };
                Ok(Response::new(response_val))
            }
            Err(e) => {
                eprintln!("[gRPC] DELETE error | id={} err={}", unique_id, e);
                Err(Status::internal(e.to_string()))
            }
        } 
    }
}
