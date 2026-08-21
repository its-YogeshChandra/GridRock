//extends functions using utils 
//these functions are the base functions that are extended by the client sdk for calling the key value store 
//these are the crud functions 
// ponytail: signatures will match PutRequest/GetValRequest/DelValRequest when implemented
use crate::shard_config_services::{GetFullConfigRequest, GetFullConfigResponse, shard_controller_client::ShardControllerClient};
use tonic::transport::Channel; 
use xxhash_rust::xxh3;

//get shacontroller config 
//fetch the request from the shard controller 
///helper function
pub async fn get_shard_config(client: &mut ShardControllerClient<Channel>, request: GetFullConfigRequest ) -> Result<GetFullConfigResponse, Box<dyn std::error::Error>>{
    //call the function to get full config 
    let response = client.get_full_config(request).await?;
    //return the config     
    Ok(response.into_inner())
}

//function to hash the key
pub async fn get_key_hash<T : AsRef<[u8]>>(key: T ) -> u64{
    //take the key  ||  //hash the key
    let key_hash = xxh3::xxh3_64(key.as_ref());
     
    //return the hash 
    key_hash 
}

//take the shard controller address + the key,
//return the address of the storage node responsible for that key
pub async fn get_shard_node_address(address : String, key: &str) -> Result<String,Box<dyn std::error::Error>>{
    //take the shard config
    let request = GetFullConfigRequest::default();
    let mut client = ShardControllerClient::connect(address).await?;
    let config_response = get_shard_config(&mut client, request).await?;

    if !config_response.success {
        return Err(format!("shard controller error: {}", config_response.message).into());
    }

    //hash the keys provided
    let hashed_key = get_key_hash(key).await;

    //check the shard config for the particular key
    let shard_config = config_response.config;
    if shard_config.is_empty() {
        return Err("shard config is empty : no storage nodes registered".into());
    }

    //do binary search on the config : fyi : config is sorted by tick_value
    //partition_point finds the FIRST shard with tick_value >= hashed_key :
    //that shard owns the ring segment the key falls in
    let idx = shard_config
        .partition_point(|shard_val| shard_val.tick_value < hashed_key);

    //wrap around the ring if the key hash is larger than every tick value
    let shard_val = if idx == shard_config.len() {
        &shard_config[0]
    } else {
        &shard_config[idx]
    };

    //get shard from the kye from the shard config
    Ok(shard_val.address.clone())
}

