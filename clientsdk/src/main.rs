use tokio;
mod client;
pub mod utils;

pub mod storage_proto {
    tonic::include_proto!("storage_system");
}

#[tokio::main]
async fn main() {}
