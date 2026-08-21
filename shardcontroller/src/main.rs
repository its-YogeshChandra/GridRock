//crate grpc server
mod controllers;
mod configstore;
use tokio;
use tonic::transport::Server;

//extends shard config service from the proto buf build 
pub mod shard_config_service {
    tonic::include_proto!("shard_config");
}

use std::env;
use configstore::config_store::init_config_store;
use controllers::config_controller::ConfigController;
use shard_config_service::shard_controller_server::ShardControllerServer;

//helper function
//reads Server0Address, Server1Address, ... until a variable is missing,
//so the ring size is set by the environment (3 nodes in docker compose)
fn parse_env_arguments() -> Vec<String> {
    let mut server_address_vec: Vec<String> = vec![];
    let mut i = 0;

    loop {
        let env_var = format!("Server{}Address", i);
        match env::var(&env_var) {
            Ok(server_address_val) => server_address_vec.push(server_address_val),
            Err(_) => break,
        }
        i += 1;
    }

    if server_address_vec.is_empty() {
        panic!("no Server0Address env var found : at least one storage node address is required");
    }

    server_address_vec
}

#[tokio::main]
async fn main()  {
    //read server addresses from environment
    let server_addresses = parse_env_arguments();

    //build the config (hashes each address onto the ring) and store it globally.
    //this lives for the entire lifetime of the program.
    init_config_store(server_addresses);

    //bind the grpc server on port 50060
    let addr = "0.0.0.0:50060".parse().expect("invalid server address");

    println!("ShardController gRPC server listening on {}", addr);

    //create the grpc service
    Server::builder()
        .add_service(ShardControllerServer::new(ConfigController))
        .serve(addr)
        .await
        .expect("failed to start shard controller server");
}