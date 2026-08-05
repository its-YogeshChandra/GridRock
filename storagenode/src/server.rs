use prost::Message;
use tonic::{Request, Response, Status};
use crate::storage_proto::{
    CreateRequest, DelValRequest, UpdateRequest, GetValRequest, StorageResponse,
    grid_rock_server::GridRock,
};
use crate::db::db_utils::get_db_connection;
use std::sync::mpsc::{Sender};

pub struct StorageServer{
    pub tx: Sender<crate::node::node_utils::Msg>
}

#[tonic::async_trait]
impl GridRock for StorageServer {
    /// Creates a new entry in storage. Fails if the unique_id already exists.
    async fn create_valin_storage(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<StorageResponse>, Status> {

        //check for node responsible for key range (testing against config from shard controller )





        let request_val = request.into_inner();
        let unique_id = request_val.unique_id.clone();

        let db = get_db_connection().map_err(|e| Status::internal(e.to_string()))?;

        // Check if the key already exists
        match db.get(unique_id.as_bytes()) {
            Ok(Some(_)) => {
                return Err(Status::already_exists(format!(
                    "Key '{}' already exists in storage",
                    unique_id
                )));
            }
            Ok(None) => {
                // Serialize the CreateRequest into protobuf bytes and store it
                let mut buffer = Vec::new();
                request_val
                    .encode(&mut buffer)
                    .map_err(|e| Status::internal(format!("Failed to encode value: {}", e)))?;

                db.put(unique_id.as_bytes(), buffer)
                    .map_err(|e| Status::internal(format!("Failed to write to DB: {}", e)))?;

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
        let unique_id = &request_val.unique_id;

        let db = get_db_connection().map_err(|e| Status::internal(e.to_string()))?;

        match db.get(unique_id.as_bytes()) {
            Ok(Some(existing_bytes)) => {
                // Decode the existing CreateRequest record
                let mut existing_record = CreateRequest::decode(existing_bytes.as_slice())
                    .map_err(|e| {
                        Status::internal(format!("Failed to decode existing record: {}", e))
                    })?;

                // Apply the update — UpdateRequest only carries unique_id + balance
                existing_record.balance = request_val.balance;

                // Re-encode and write back
                let mut buffer = Vec::new();
                existing_record
                    .encode(&mut buffer)
                    .map_err(|e| Status::internal(format!("Failed to encode value: {}", e)))?;

                db.put(unique_id.as_bytes(), buffer)
                    .map_err(|e| Status::internal(format!("Failed to write to DB: {}", e)))?;

                let response_val = StorageResponse {
                    message: format!(
                        "Value with key '{}' successfully updated (balance -> {})",
                        unique_id, request_val.balance
                    ),
                    success: true,
                };
                Ok(Response::new(response_val))
            }
            Ok(None) => Err(Status::not_found(format!(
                "Key '{}' not found in storage",
                unique_id
            ))),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    /// Retrieves an entry from storage by unique_id. Returns its fields in the response message.
    async fn get_valfrom_storage(
        &self,
        request: Request<GetValRequest>,
    ) -> Result<Response<StorageResponse>, Status> {
        let request_val = request.into_inner();
        let unique_id = &request_val.unique_id;

        let db = get_db_connection().map_err(|e| Status::internal(e.to_string()))?;

        match db.get(unique_id.as_bytes()) {
            Ok(Some(stored_bytes)) => {
                // Decode the stored CreateRequest record
                let record = CreateRequest::decode(stored_bytes.as_slice()).map_err(|e| {
                    Status::internal(format!("Failed to decode stored record: {}", e))
                })?;

                let response_val = StorageResponse {
                    message: format!(
                        "unique_id: {}, balance: {}, executable: {}, rent_epoch: {}, data_hash: {}, last_updated_slot: {}",
                        record.unique_id,
                        record.balance,
                        record.executable,
                        record.rent_epoch,
                        record.data_hash,
                        record.last_updated_slot
                    ),
                    success: true,
                };
                Ok(Response::new(response_val))
            }
            Ok(None) => Err(Status::not_found(format!(
                "Key '{}' not found in storage",
                unique_id
            ))),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    /// Deletes an entry from storage by unique_id. Fails if the key does not exist.
    async fn del_valfrom_storage(
        &self,
        request: Request<DelValRequest>,
    ) -> Result<Response<StorageResponse>, Status> {
        let request_val = request.into_inner();
        let unique_id = &request_val.unique_id;

        let db = get_db_connection().map_err(|e| Status::internal(e.to_string()))?;

        // Verify the key exists before deleting
        match db.get(unique_id.as_bytes()) {
            Ok(Some(_)) => {
                db.delete(unique_id.as_bytes())
                    .map_err(|e| Status::internal(format!("Failed to delete from DB: {}", e)))?;

                let response_val = StorageResponse {
                    message: format!("Value with key '{}' successfully deleted", unique_id),
                    success: true,
                };
                Ok(Response::new(response_val))
            }
            Ok(None) => Err(Status::not_found(format!(
                "Key '{}' not found in storage",
                unique_id
            ))),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}
