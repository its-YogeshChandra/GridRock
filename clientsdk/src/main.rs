use std::env;
use tokio;
mod client;
mod shard_controller;

pub mod utils;
pub mod storage_proto {
    tonic::include_proto!("storage_system");
}

use shard_controller::config::{get_config_store, hashing_function, init_config_store};

//helper function
fn parse_env_arguments() -> Vec<String> {
    let mut server_address_vec: Vec<String> = vec![];
    for i in 0..5 {
        let env_var = format!("Server{}Address", i);
        let msg = format!("failed to read : {}", env_var);
        let server_address_val = env::var(env_var).expect(&msg);

        //push the value into the server address vec
        server_address_vec.push(server_address_val);
    }

    server_address_vec
}

#[tokio::main]
async fn main() {
    //read server addresses from environment
    let server_addresses = parse_env_arguments();

    //build the config (hashes each address onto the ring) and store it globally.
    //this lives for the entire lifetime of the program.
    init_config_store(server_addresses);
}
