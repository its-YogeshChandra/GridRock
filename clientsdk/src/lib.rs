//need to create lib.rs
pub mod storage_services {
    tonic::include_proto!("storage_system");
}

pub mod shard_config_services {
    tonic::include_proto!("shard_config");
}
pub mod states;
pub mod errors;
pub mod client;


//extend the functions from this lib
