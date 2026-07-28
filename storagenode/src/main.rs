mod server;
mod client;
use tokio;
pub mod greeter {
    tonic::include_proto!("greeter");
}
use greeter::greeter_server::GreeterServer;
use tonic::{transport::Server};
use server::MineGreeterServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn::std::error::Error>> {
    let address = "[::1]:50051".parse()?;
    Server::builder()
        .add_service(GreeterServer::new(MineGreeterServer::default()))
        .serve(address)
        .await?;

    Ok(())
}
