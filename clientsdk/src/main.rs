use std::env;
use tokio;
use tonic::server;
mod client;
mod shard_controller;

pub mod utils;
pub mod storage_proto {
    tonic::include_proto!("storage_system");
}

//helper function
fn parse_env_arguments() {
    let server_address_vec: Vec<String> = vec![];
    for i in 0..5 {
        let env_var = format!("Server{}address", i);
        println!("env var is : {}", env_var)
    }
}

#[tokio::main]
async fn main() {
    parse_env_arguments();
}
