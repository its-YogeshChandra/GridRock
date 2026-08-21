//extends functions using utils 
//these functions are the base functions that are extended by the client sdk for calling the key value store 
//these are the crud functions 
// ponytail: signatures will match PutRequest/GetValRequest/DelValRequest when implemented
use crate::shard_config_services::{GetFullConfigRequest, GetFullConfigResponse, shard_controller_client::ShardControllerClient};
use tonic::transport::Channel; 


//get shacontroller config 
//fetch the request from the shard controller 
///helper function 
async fn get_shard_config(client: &mut ShardControllerClient<Channel>, request: GetFullConfigRequest ) -> Result<GetFullConfigResponse, Box<dyn std::error::Error>>{
    //call the function to get full config 
    let response = client.get_full_config(request).await?;
    //return the config     
    Ok(response.into_inner())
}

//function to hash the key 
async fn get_key_hash(){
    //take the key 
    //hash the key 
    //return the hash 
}

async fn get_shard_node_address(){
    //take the shard config 
    //hash the keys provided 
    //check the shard config for the particular key  
    //get shard from the kye from the shard config     
}


