use tonic::{Request, Response, Status};
use crate::greeter::greeter_server;
use crate::greeter::{HelloRequest, HelloResponse};

#[derive(Debug, Default)]
pub struct MineGreeterServer;

#[tonic::async_trait]
impl greeter_server::Greeter for MineGreeterServer {
    async fn say_hello(&self, request: Request<HelloRequest>) -> Result<Response<HelloResponse>, Status> {
        let req = request.into_inner().name;
        Ok(Response::new(HelloResponse {
            message: format!("Hello {}", req),
        }))
    }

}
