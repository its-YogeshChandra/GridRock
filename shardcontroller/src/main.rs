//crate grpc server
mod controllers;
mod configstore;
use tokio;
pub mod shard_config_service {
    tonic::include_proto!("shard_config");
}



#[tokio::main]
async fn main() {
    
}
