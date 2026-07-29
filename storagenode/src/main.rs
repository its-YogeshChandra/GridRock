mod server;
pub mod storage_proto {
    tonic::include_proto!("storage_system");
}
use tonic::transport::Server;
use tokio;

#[tokio::main]
async fn main( 
    
) -> Result<(), Box<dyn std::error::Error>> {
     let addr = "[::1]:50051".parse()?;
    Server::builder()
    .add_service(storage_proto::grid_rock_server::GridRockServer::new(server::StorageServer))
    .serve(addr).await?;
    
    Ok(())
}