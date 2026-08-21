#[path = "utils.rs"]
pub mod utils;
pub use utils::get_shard_node_address;
use serde::de::DeserializeOwned;
use crate::storage_services::{DelValRequest, GetValRequest, PutRequest, StorageResponse, grid_rock_client::GridRockClient};
use tonic::transport::Channel;
use rand::RngExt;

// -----------------------------------------
// CRUD functions — take an existing client + entity,
// so the same unique_id flows through Create -> Update -> Get -> Delete
//take the request from the client 

pub fn get_random_value<T: Copy>(values: &[T]) -> T {
    let mut rng = rand::rng();
    let index = rng.random_range(0..values.len());
    values[index]
}


pub async fn create_val<T: DeserializeOwned>(
    client: &mut GridRockClient<Channel>,
    request:PutRequest 
) -> Result<StorageResponse, Box<dyn std::error::Error>> {
    let response = client
        .create_valin_storage(request)
        .await;

    match response {
        Ok(resp) => {
           Ok(resp.into_inner()) 
        }
        Err(e) => {
            eprintln!("[CREATE] failed for  {:?}", e);
            return Err(Box::new(e));
        }
    }
}

//update the value 
pub async fn update_val(
    client: &mut GridRockClient<Channel>,
    request:PutRequest
) -> Result<StorageResponse, Box<dyn std::error::Error>> {
    let response = client
        .update_valin_storage(request)
        .await;
    
    match response {
        Ok(resp) => {
           Ok(resp.into_inner()) 
        }
        Err(e) => {
            eprintln!("[UPDATE] failed for  {:?}", e);
            return Err(Box::new(e));
        }
    }
}

//get val for the things 
pub async fn get_val(
    client: &mut GridRockClient<Channel>,
    request: GetValRequest,
) -> Result<StorageResponse, Box<dyn std::error::Error>> {
    let response = client.get_valfrom_storage(request).await;

    match response {
        Ok(resp) => {
           Ok(resp.into_inner()) 
        }
        Err(e) => {
            eprintln!("[GET] failed for  {:?}", e);
            return Err(Box::new(e));
        }
    }
}

pub async fn delete_val(
    client: &mut GridRockClient<Channel>,
    request: DelValRequest,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client
        .del_valfrom_storage(request)
        .await?;
    println!("[DELETE] {:#?}", response.into_inner());
    Ok(())
}


