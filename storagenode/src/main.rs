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
mod node;
use std::sync::mpsc::{channel};

enum Msg {
    Propose{id: u8, callback: Box<dyn FnOnce(Result<(), raft::Error>) + Send>},
    Raft(raft::eraftpb::Message),
    // You can add more message types here if needed
}

pub struct process_node_tx{
 tx : std::sync::mpsc::Sender<Msg>
}

#[tokio::main]
async fn main( 
    
) -> Result<(), Box<dyn std::error::Error>> {
     let addr = "[::1]:50051".parse()?;
     println!("server is listening on the port 50051");

     //create the tx and rx for the channel 
     let (tx, rx) = channel::<Msg>();
     
     //create the process node tx 
     let process_node_tx = process_node_tx{
         tx: tx.clone()
     };

    Server::builder()
    .add_service(storage_proto::grid_rock_server::GridRockServer::new(server::StorageServer))
    //function to share this tx with every grpc function 
    .serve(addr).await?;
    
    
    Ok(())
}