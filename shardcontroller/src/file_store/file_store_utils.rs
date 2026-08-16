//create the static file storage
// extends functions to talk to interact with the file storage
use core::panic;
use protobuf::json;
use serde;
use serde_json;
use std::fs::{self, File, OpenOptions, create_dir_all};
use std::path::Path;

sharding:
  version: 1
  shard_key: user_id

  strategy:
    type: consistent_hash
    algorithm: xxhash64
    virtual_nodes: 150

  nodes:
    - id: shard-0
      address: "db-shard-0.internal:5432"
      weight: 1
      replicas:
        - "db-shard-0-replica-1.internal:5432"
        - "db-shard-0-replica-2.internal:5432"

    - id: shard-1
      address: "db-shard-1.internal:5432"
      weight: 1
      replicas:
        - "db-shard-1-replica-1.internal:5432"

    - id: shard-2
      address: "db-shard-2.internal:5432"
      weight: 1
      replicas: []

    - id: shard-3
      address: "db-shard-3.internal:5432"
      weight: 2
      replicas:
        - "db-shard-3-replica-1.internal:5432"





struct Strategy {
        strat_type : String,
        algorithm : String 
    }

pub struct ShardConfig {
    version = u64,
    shard_key : user_id,
    virtual_nodes : 
    
}

//helper function take the dir and create file at that location
fn write_config_file() {}

//create
pub fn create_shard_config<T>(
    dir_path: impl AsRef<Path>,
    shard_config: Option<ShardConfig>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut dir_path = dir_path.as_ref().to_path_buf();
    if dir_path.exists() {
        let mut main_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir_path)?;
    } else {
        let mut dir_path = create_dir_all(dir_path)?;
    }
    Ok(())
}

//update
pub fn update_shard_configuration() {}

//get
pub fn get_shard_configuration() {}

//delete
pub fn delete_shard_configuration() {}
