mod server;
use tokio;
pub mod greeter {
    tonic::include_proto!("greeter");
}
use greeter::greeter_server::GreeterServer;
use tonic::{transport::Server};

#[tokio::main]
async fn main() {
    let address = "[IP_ADDRESS]".parse().unwrap();
    let greeter = server::GreeterServer;
    Server::builder()
        .add_service(GreeterServer::new(greeter))
        .serve(address)
        .await;
}
