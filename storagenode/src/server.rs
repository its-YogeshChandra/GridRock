use prost::Message;
use tonic::{Request, Response, Status};
use crate::storage_proto::raft_proposal::Operation;
use crate::storage_proto::{
    CreateRequest, DelValRequest, UpdateRequest, GetValRequest, StorageResponse, RaftProposal,
    grid_rock_server::GridRock,
};
use tokio::sync::{mpsc::{Sender}, oneshot};
use crate::node::node_utils::{Msg, OperationType, ProposeMessage};
use crate::errors::request_errors::ClientGrpcRequestProcessingError;

pub struct StorageServer{
    pub tx: Sender<crate::node::node_utils::Msg>
}

pub struct RafProcessedResponse{
    pub id : u64,
    pub success : bool,
    pub data : Option<CreateRequest>
}

static mut COUNTER: u64 = 0;

#[tonic::async_trait]
impl GridRock for StorageServer {
    /// Creates a new entry in storage. Fails if the unique_id already exists.
    async fn create_valin_storage(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<StorageResponse>, Status> //need to update the response type
    {

        //check for node responsible for key range (testing against config from shard controller )
        
        let request_val = request.into_inner();
        let unique_id = request_val.unique_id.clone();

        //use the tokio oneshot to create 
        let (tx, rx) = oneshot::channel::<Result<RafProcessedResponse, ClientGrpcRequestProcessingError>>();
        
        //forge the propose msg for raft 
        let id = unsafe{COUNTER + 1};

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

        self.tx.send(Msg::Propose { proposemsg: propose_msg_data }).await.map_err(|e| Status::internal(e.to_string()))?;

        let result = rx.await.map_err(|e| Status::internal(e.to_string()))?;
        match result {
            Ok(_) => {
                let response_val = StorageResponse {
                    message: format!("Value with key '{}' successfully created", unique_id),
                    success: true,
                };
                Ok(Response::new(response_val))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
           }

    /// Updates an existing entry's balance. Fails if the unique_id does not exist.
    async fn update_valin_storage(
        &self,
        request: Request<UpdateRequest>,
    ) -> Result<Response<StorageResponse>, Status> {
        
        let request_val = request.into_inner();
        let unique_id = request_val.unique_id.clone();

        //use the tokio oneshot to create 
        let (tx, rx) = oneshot::channel::<Result<RafProcessedResponse, ClientGrpcRequestProcessingError>>();
        
        //forge the propose msg for raft 
        let id = unsafe{COUNTER + 1};

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

        self.tx.send(Msg::Propose { proposemsg: propose_msg_data }).await.map_err(|e| Status::internal(e.to_string()))?;

        let result = rx.await.map_err(|e| Status::internal(e.to_string()))?;
        match result {
            Ok(_) => {
                let response_val = StorageResponse {
                    message: format!("Value with key '{}' successfully updated", unique_id),
                    success: true,
                };
                Ok(Response::new(response_val))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    /// Retrieves an entry from storage by unique_id. Returns its fields in the response message.
    async fn get_valfrom_storage(
        &self,
        request: Request<GetValRequest>,
    ) -> Result<Response<StorageResponse>, Status> {

        let request_val = request.into_inner();
        let unique_id = request_val.unique_id.clone();

        //use the tokio oneshot to create 
        let (tx, rx) = oneshot::channel::<Result<RafProcessedResponse, ClientGrpcRequestProcessingError>>();
        
        //forge the propose msg for raft 
        let id = unsafe{COUNTER + 1};

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

        self.tx.send(Msg::Propose { proposemsg: propose_msg_data }).await.map_err(|e| Status::internal(e.to_string()))?;

        let result = rx.await.map_err(|e| Status::internal(e.to_string()))?;
        match result {
            Ok(_) => {
                let response_val = StorageResponse {
                    message: format!("Value with key '{}' successfully updated", unique_id),
                    success: true,
                };
                Ok(Response::new(response_val))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        } 
    }

    /// Deletes an entry from storage by unique_id. Fails if the key does not exist.
    async fn del_valfrom_storage(
        &self,
        request: Request<DelValRequest>,
    ) -> Result<Response<StorageResponse>, Status> {
        let request_val = request.into_inner();
        let unique_id = request_val.unique_id.clone();

        //use the tokio oneshot to create 
        let (tx, rx) = oneshot::channel::<Result<RafProcessedResponse, ClientGrpcRequestProcessingError>>();
        
        //forge the propose msg for raft 
        let id = unsafe{COUNTER + 1};

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

        self.tx.send(Msg::Propose { proposemsg: propose_msg_data }).await.map_err(|e| Status::internal(e.to_string()))?;

        let result = rx.await.map_err(|e| Status::internal(e.to_string()))?;
        match result {
            Ok(_) => {
                let response_val = StorageResponse {
                    message: format!("Value with key '{}' successfully deleted", unique_id),
                    success: true,
                };
                Ok(Response::new(response_val))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        } 
    }
}
