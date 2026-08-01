mod server;
mod db;
mod client;
pub mod storage_proto {
    tonic::include_proto!("storage_system");
}
use tonic::transport::Server;
use tokio;
mod node;
use std::sync::mpsc::{channel};
use node::node_utils::{create_raft_node, processor_node, Msg};


pub struct process_node_tx{
 tx : std::sync::mpsc::Sender<Msg>
}

#[tokio::main]
async fn main( 
    
) -> Result<(), Box<dyn std::error::Error>> {

    //accept the ports , id and peers from the command line
    let mut port = String::new();
    let mut id = String::new();

    //create three peers 
    let mut peer1 = String::new();
    let mut peer2 = String::new();
    let mut peer3 = String::new();

    std::io::stdin().read_line(&mut port).expect("Failed to read port");
    std::io::stdin().read_line(&mut id).expect("Failed to read id");
   std::io::stdin().read_line(&mut peer1).expect("Failed to read peer1");
   std::io::stdin().read_line(&mut peer2).expect("Failed to read peer2");
   std::io::stdin().read_line(&mut peer3).expect("Failed to read peer3"); 

   

    let addr = "[::1]:50051".parse()?;
    println!("server is listening on the port 50051");

     //create the tx and rx for the channel 
     let (tx, rx) = channel::<Msg>();
     
     //create the process node tx 
     let process_node_tx = process_node_tx{
         tx: tx.clone()
     };

     let id = 1;
    let peers = vec![1, 2, 3];
     let node = create_raft_node(id, peers);

     //spawn a new thread to run the processor_node function
     std::thread::spawn(move || {
        node::node_utils::processor_node(node, rx);
     });


    //create the grpc server 
     Server::builder()
    .add_service(storage_proto::grid_rock_server::GridRockServer::new(server::StorageServer))
    //function to share this tx with every grpc function 
    .serve(addr).await?;
    
    
    Ok(())
}