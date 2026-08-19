//need to create lib.rs
pub mod storage_services {
    tonic::include_proto!("storage_system");
}

pub mod shard_config_services {
    tonic::include_proto!("shard_config");
}




//extend the functions from this lib
