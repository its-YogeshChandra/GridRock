use tonic::{ Request, Response, Status};
use crate::storage_proto::{CreateRequest, DelValRequest, UpdateRequest, GetValRequest, StorageResponse, grid_rock_server::{GridRock, GridRockServer}};
use rocksdb::{DB, Options};
use crate::db::db_utils::getDBconnection;

pub struct RocksdbRequest <T>{ 
    key: String,
    value : T 
}


#[derive(Debug, Default)]
pub struct StorageServer;

#[tonic::async_trait]
impl GridRock for StorageServer { 
   async fn create_valin_storage(&self, request:Request<CreateRequest>) -> Result<Response<StorageResponse>,Status>{ 
    let request_val = request.into_inner();

    //write the function to store the value in storage using grpc
    let db = getDBconnection(); 
    match db {
        Ok(db) =>{
            //check for the value if already present 
            let unique_id = &request_val.unique_id;
            match db.get(unique_id){
                Ok(Some(_)) =>{ 
                    let response = StorageResponse{
                        message: "value already present".to_string(),
                        success: false
                    }; 
                    return Err(Status::internal(response.message)); 
                } 
                Ok(None) =>{
                    //update the value in the database 
                    db.put(request_val.unique_id.as_bytes(), request_val.balance.to_string().as_bytes()).unwrap();
                    let response_val = StorageResponse{
                         message: "value successfully set".to_string(),
                         success: true
                    };
                    return Ok(Response::new(response_val)) 
                } 
                Err(e) =>{
                    return Err(Status::internal(e.to_string()));
                } 
            }; 
        } 
        Err(e) =>{
            return Err(Status::internal(e.to_string()));
        }
    }; 
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
