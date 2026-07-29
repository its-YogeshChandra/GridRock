use tonic::{transport::Server, Request, Response, Status};
use crate::storage_proto::{CreateRequest, DelValRequest, UpdateRequest, GetValRequest, StorageResponse, grid_rock_server::{GridRock, GridRockServer}};

#[derive(Debug, Default)]
pub struct StorageServer;

#[tonic::async_trait]
impl GridRock for StorageServer { 
   async fn create_valin_storage(&self, request:Request<CreateRequest>) -> Result<Response<StorageResponse>,Status>{ 
    let request_val = request.into_inner();

    //write the function to store the value in storage using grpc 
    let response_val = StorageResponse{
        message: "value successfully set".to_string(),
        success: true
    };
    Ok(Response::new(response_val)) 
   } 
   
   async fn update_valin_storage(&self, request:Request<UpdateRequest>) -> Result<Response<StorageResponse>,Status>{ 
   let request_val = request.into_inner();
   let response_val = StorageResponse{
        message: "value successfully set".to_string(),
        success: true
    };
    Ok(Response::new(response_val)) 
   } 
    
   async fn get_valfrom_storage(&self, request:Request<GetValRequest>) -> Result<Response<StorageResponse>,Status>{ 
    let request_val = request.into_inner();   
    let response_val = StorageResponse{
        message: "value successfully set".to_string(),
        success: true
    };
    Ok(Response::new(response_val)) 
   } 
   async fn del_valfrom_storage(&self, request:Request<DelValRequest>) -> Result<Response<StorageResponse>,Status>{ 
     let request_val = request.into_inner();   
    let response_val = StorageResponse{
        message: "value successfully set".to_string(),
        success: true
    };
    Ok(Response::new(response_val)) 

   } 
}
