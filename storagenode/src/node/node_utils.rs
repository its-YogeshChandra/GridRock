use raft::{
    Config, StateRole, raw_node::RawNode, storage::MemStorage,
};
use slog::{Discard, o};
use crate::storage_proto::{
    GetValRequest,
    UpdateRequest,
    CreateRequest,
    DelValRequest    
};
use std::sync::mpsc::{channel, RecvTimeoutError};
use std::time::{Instant, Duration};

//Create a private module to "seal" the trait
mod private {
    pub trait Sealed {}
    
    // Implement the private trait ONLY for your specific structs
    impl Sealed for super::GetValRequest {}
    impl Sealed for super::UpdateRequest {}
    impl Sealed for super::CreateRequest {}
    impl Sealed for super::DelValRequest {}
}

//Define the public trait and require the private `Sealed` bound
pub trait AllowedTypes: private::Sealed {
    // You can also add common methods here if needed
}

//Implement the public trait for your structs
impl AllowedTypes for GetValRequest {}
impl AllowedTypes for UpdateRequest {}
impl AllowedTypes for CreateRequest {}
impl AllowedTypes for DelValRequest {}



//function to create the raft node
pub fn create_raft_node(id: u64, peers: Vec<u64>) -> RawNode<MemStorage> {
//question : do we need to create raft node every time ? or have to create it once and check if already present 
//question : 

    //check if raw node is already present 
    let mut config = Config{
        id,
        ..Default::default()
    };
    config.validate().unwrap();
    let node_storage = MemStorage::new_with_conf_state((peers, vec![]));
    let logger = slog::Logger::root(slog::Discard, o!());
    RawNode::new(&config, node_storage, &logger).unwrap()
}


//node processor is the main function for the whole raft system 
 //receiving the request 
 //check weather the node is leader or not
 //if not either send error back wrong node || either pass the request to other node
  //checking the request against the config
 //check the correct config from shard controller if incorrect config present  
 //if current node is leader , then update the logs and replicates log entry to followers 
 // if followers acknowledge 
 // node marks the added to 
 //then it gets added to the queue  
 //and then it get executed  
pub fn processor_node_fn<T>(request: T, mut node: RawNode<MemStorage>) where T: AllowedTypes { 

    let (tx, rx) = channel::<T>();

    let timeout = Duration::from_millis(100);    
    let mut remaining_timeout = timeout;

    loop {
        let now = Instant::now()
        //loop will constantly tick the node
        node.tick(); 

        //node hectic nature to fix the thing 
    }
    
    //check if the node is the leader 
    let node_state = node.raft.state; 

    match node_state {
       StateRole::Follower => {
       //send back the request that wrong leader 
       //question : but followers too had to add the data when they need to replicate the logs  
       }
       StateRole::Leader => {
       //this one take the request and process accordingly 

       }
       StateRole::Candidate => {
       //vote for itself in this case 
       
       }
       _ => {}
    } 

}