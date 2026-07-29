mod server;
mod db;
mod client;
pub mod storage_proto {
    tonic::include_proto!("storage_system");
}
use tonic::transport::Server;
use tokio;
use raft::{
    Config, 
    storage::MemStorage,
    raw_node::RawNode,
};
use slog::{Drain, o};

#[tokio::main]
async fn main( 
    
) -> Result<(), Box<dyn std::error::Error>> {
     let addr = "[::1]:50051".parse()?;
     println!("server is listening on the port 50051");

     //create the config for creating raft node
     //question : what config do in the first place  
     let mut config = Config{
        id: 1,
        ..Default::default()
     };

     config.id = 3;

   //initialize logger 
   let logger = slog::Logger::root(slog);


   //question : what this .validate is validating 
   //and again what this is validating the config ? 
   config.validate().unwrap();

   let storage = MemStorage::new_with_conf_state((vec![1], vec![]));
   let mut node = RawNode::new(&config, node storage).unwrap();

    Server::builder()
    .add_service(storage_proto::grid_rock_server::GridRockServer::new(server::StorageServer))
    .serve(addr).await?;
    
    Ok(())
}