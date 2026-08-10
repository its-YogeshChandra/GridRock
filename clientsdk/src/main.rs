use tokio;
use tonic;

pub mod storage_proto {
    tonic::include_proto!("storage_system");
}

#[tokio::main]
async fn main() {}
