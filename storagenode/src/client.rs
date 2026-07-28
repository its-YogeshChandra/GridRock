pub mod greeter{
    tonic::include_proto!("greeter");
}

use crate::greeter::greeter_client::GreeterClient;
use crate::greeter::HelloRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = GreeterClient::connect("http://[::1]:50051").await?;
    let request = tonic::Request::new(HelloRequest {
        name: "Alice".into(),
    });
    let response = client.say_hello(request).await?;
    println!("Response: {}", response.into_inner().message);
    Ok(())
}