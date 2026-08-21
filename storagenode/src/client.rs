use crate::storage_proto::grid_rock_client::GridRockClient;
use crate::storage_proto::{PutRequest, DelValRequest, GetValRequest};
use tokio;

//create function 
pub async fn create_val()-> Result<(), Box<dyn std::error::Error>>{
    let mut client = GridRockClient::connect("http://[::1]:50051").await?;
    let create_request = PutRequest {
        unique_id: "test".to_string(),
        value: b"hello gridrock".to_vec(),
    };
    let response = client.create_valin_storage(create_request).await?;
    println!("{:#?}", response);
    Ok(())
}

//update function 
pub async fn update_val()-> Result<(), Box<dyn std::error::Error>>{
    let mut client = GridRockClient::connect("http://[::1]:50051").await?;
    let update_request = PutRequest {
        unique_id: "test".to_string(),
        value: b"updated value".to_vec(),
    };
    let response = client.update_valin_storage(update_request).await?;
    println!("{:#?}", response);
    Ok(())
}

//get function 
pub async fn get_val()-> Result<(), Box<dyn std::error::Error>>{
    let mut client = GridRockClient::connect("http://[::1]:50051").await?;
    let get_request = GetValRequest {
        unique_id: "test".to_string(),
        };
    let response = client.get_valfrom_storage(get_request).await?;
    println!("{:#?}", response);
    Ok(())
}

//delete function 
pub async fn delete_val()-> Result<(), Box<dyn std::error::Error>>{
    let mut client = GridRockClient::connect("http://[::1]:50051").await?;
    let delete_request = DelValRequest {
        unique_id: "test".to_string(),
       };
    let response = client.del_valfrom_storage(delete_request).await?;
    println!("{:#?}", response);
    Ok(())
}


#[tokio::main]
async fn main()-> Result<(), Box<dyn std::error::Error>>{
create_val().await?;
update_val().await?;
get_val().await?;
delete_val().await?;

Ok(())
}