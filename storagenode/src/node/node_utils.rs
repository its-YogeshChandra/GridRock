use raft::eraftpb::Message;
use raft::{
    Config, StateRole, raw_node::RawNode, storage::MemStorage,
    Error
};
use slog::{Discard, o};
use tokio::fs::read;
use tonic::transport;
use crate::storage_proto::{
    GetValRequest,
    UpdateRequest,
    CreateRequest,
    DelValRequest    
};
use std::collections::HashMap;
use std::sync::mpsc::{channel, RecvTimeoutError, Receiver};
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


pub enum Msg {
    Propose{id: u8, callback: Box<dyn FnOnce(Result<(), Error>) + Send>},
    Raft(Message),
    // You can add more message types here if needed
}


//function to create the raft node
pub fn create_raft_node(id: u64, peers: Vec<u64>) -> RawNode<MemStorage> {
//question : do we need to create raft node every time ? or have to create it once and check if already present 
    
    let mut config = Config{
        id,
        election_tick: 10,
        heartbeat_tick: 1,
        ..Default::default()
    };
    config.validate().unwrap();
    let node_storage = MemStorage::new_with_conf_state((peers, vec![]));
    let logger = slog::Logger::root(slog::Discard, o!());
    RawNode::new(&config, node_storage, &logger).unwrap()
}

//helper function to process the ready state of the raft node 
fn process_ready_state(node: &mut RawNode<MemStorage>) {
    if !node.has_ready() {
        return;
    }

    let mut ready = node.ready();

    // Process messages
    for msg in ready.take_messages() {
        // Handle messages (e.g., send to other nodes)
    }

    // Apply snapshot if present
    if !ready.snapshot().is_empty() {
        node.mut_store().wl().apply_snapshot(ready.snapshot().clone()).unwrap();
    }

    // Append entries to storage
    if !ready.entries().is_empty() {
        node.mut_store().wl().append(ready.entries()).unwrap();
    }

    // Update hard state if present
    if let Some(hs) = ready.hs() {
        node.mut_store().wl().set_hardstate(hs.clone());
    }

    // Process committed entries
    if !ready.committed_entries().is_empty() {
        for entry in ready.take_committed_entries() {
            if entry.data.is_empty() {
                continue;
            }
            match entry.get_entry_type() {
                raft::eraftpb::EntryType::EntryNormal => {
                    // Handle normal entry (apply to state machine)
                }
                raft::eraftpb::EntryType::EntryConfChange => {
                    // Handle configuration change entry
                }
                raft::eraftpb::EntryType::EntryConfChangeV2 => {
                    // Handle configuration change v2 entry
                }
                _ => {
                    eprintln!("Unhandled entry type");
                }
            }
        }
    }

   for msg in ready.take_persisted_messages() {
        // Handle persisted messages (e.g., send to other nodes)
    }

    let mut light_rd = node.advance(ready);
    
    for msg in light_rd.take_messages() {
        // Handle messages after advancing the node
    
    }

    for entry in light_rd.take_committed_entries(){

    }

    node.advance_apply();


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
pub fn processor_node(mut node: RawNode<MemStorage>, rx: Receiver<Msg>) { 
    
    let timeout = Duration::from_millis(100);    
    let mut remaining_timeout = timeout;

    //the transaction will be handled here 
    let mut cbs = HashMap::new();

    loop {
        let now = Instant::now();
        
        match rx.recv_timeout(remaining_timeout) {
        //direct on what to perform on different conditions 
            Ok(Msg::Propose{id, callback})  => {
                //tools to fix the thin
                cbs.insert(id, callback);
                node.propose(vec![], vec![id]).map_err(|e| println!("Error proposing: {}", e)).unwrap();

                //check if node has something to process
                if !node.has_ready() {
                   continue; 
                };

               let is_ready_processed = process_ready_state(&mut node); 


         }

            Ok(Msg::Raft(m)) => {
                node.step(m).unwrap();

                //check if node has something to process 
                if node.has_ready() {
                   let is_ready_processed = process_ready_state(&mut node);  
                };

            
            }


            Err(RecvTimeoutError::Timeout) => {
               //ready to do something  
            }

            Err(RecvTimeoutError::Disconnected) => {
                unimplemented!("Channel disconnected"); 
            }
        }

        let elapsed = now.elapsed();
        if elapsed >= remaining_timeout {
            remaining_timeout = timeout;

            //drive raft event after timeout 
            node.tick();        
        }
        else {
            remaining_timeout -= elapsed;
            
        }

    }

}