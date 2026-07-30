use raft::{
    Config, 
    storage::MemStorage,
    raw_node::RawNode,
};
use slog::{Discard, o};
use crate::storage_proto::{
    GetValRequest,
    UpdateRequest,
    CreateRequest,
    DelValRequest    
};

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
pub fn processor_node_fn<T>(request: T) {
      
    
}