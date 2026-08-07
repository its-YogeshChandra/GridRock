use crate::storage_proto::{CreateRequest, DelValRequest, GetValRequest, UpdateRequest};
use raft::eraftpb;
use prost::Message;
use raft::{Config, Error, StateRole, raw_node::RawNode, storage::MemStorage};
use slog::{Discard, o};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc::Receiver, oneshot};
use tokio::time::timeout;
use crate::errors::request_errors::ClientGrpcRequestProcessingError;
use crate::server::RaftProcessedResponse;
use crate::db::{db_create, db_read, db_update, db_delete, get_db_connection};
use crate::storage_proto::RaftProposal;
use crate::storage_proto::raft_proposal::Operation;

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

//the correct messaage type is needed

#[derive(Debug, PartialEq)]
pub enum OperationType {
    Create,
    Update,
    Delete,
    Get,
}

pub struct ProposeMessage {
    pub id: u64,
    //the data is deserailized using the protobuf meessage type
    pub data: Vec<u8>,
    pub operation_type: OperationType,
    pub response_tx: oneshot::Sender<Result<RaftProcessedResponse, ClientGrpcRequestProcessingError>>,
}

pub enum Msg {
    Propose { proposemsg: ProposeMessage },
    Raft(eraftpb::Message),
    // You can add more message types here if needed
}

pub fn append_committed_entry(entry: eraftpb::Entry, cbs: &mut HashMap<u64, oneshot::Sender<Result<RaftProcessedResponse, ClientGrpcRequestProcessingError>>>, db: &rocksdb::DB) {
    //check if the entry is data entry
    let proposal = match RaftProposal::decode(entry.data.as_ref()) {
                        Ok(p) => p,
                        Err(e) => {
                            eprintln!("Failed to decode RaftProposal: {:?}", e);
                            return;
                        }
                    };

                    let proposal_id = proposal.proposal_id;

                    
                    // Apply the operation to the state machine (RocksDB)
                    // Build the response or error based on which operation it is
                    let result: Result<RaftProcessedResponse, ClientGrpcRequestProcessingError> = match proposal.operation {
                        Some(Operation::Create(ref create_req)) => {
                            match db_create(&db, create_req) {
                                Ok(created_id) => Ok(RaftProcessedResponse {
                                    id: Some(created_id.to_string()),
                                    success: true,
                                    data: Some(create_req.clone()),
                                }),
                                Err(e) => {
                                    eprintln!("DB create failed: {}", e);
                                    Err(ClientGrpcRequestProcessingError::DbResponseFailed)
                                }
                            }
                        }

                        Some(Operation::Update(ref update_req)) => {
                            match db_update(&db, &update_req.unique_id, update_req.balance) {
                                Ok(updated_id) => Ok(RaftProcessedResponse {
                                    id: Some(updated_id.to_string()),
                                    success: true,
                                    data: None,
                                }),
                                Err(e) => {
                                    eprintln!("DB update failed: {}", e);
                                    Err(ClientGrpcRequestProcessingError::DbResponseFailed)
                                }
                            }
                        }

                        Some(Operation::Delete(ref del_req)) => {
                            match db_delete(&db, &del_req.unique_id) {
                                Ok(deleted_id) => Ok(RaftProcessedResponse {
                                    id: Some(deleted_id.to_string()),
                                    success: true,
                                    data: None,
                                }),
                                Err(e) => {
                                    eprintln!("DB delete failed: {}", e);
                                    Err(ClientGrpcRequestProcessingError::DbResponseFailed)
                                }
                            }
                        }

                        Some(Operation::Get(ref get_req)) => {
                            match db_read(&db, &get_req.unique_id) {
                                Ok(record) => Ok(RaftProcessedResponse {
                                    id: Some(get_req.unique_id.clone()),
                                    success: true,
                                    data: Some(record),
                                }),
                                Err(e) => {
                                    eprintln!("DB read failed: {}", e);
                                    Err(ClientGrpcRequestProcessingError::DbResponseFailed)
                                }
                            }
                        }

                        None => {
                            eprintln!("No operation found in RaftProposal (id: {})", proposal_id);
                            Err(ClientGrpcRequestProcessingError::NodeUnaware)
                        }
                    };

                    // Send the result back to the gRPC handler if this node is the leader
                    // (followers won't have an entry in cbs — they just applied to DB silently)
                    if let Some(sender) = cbs.remove(&proposal_id) {
                        let _ = sender.send(result);
                    }
    
}



//function to create the raft node
pub fn create_raft_node(id: u64, peers: Vec<u64>) -> RawNode<MemStorage> {
    //question : do we need to create raft node every time ? or have to create it once and check if already present

    let mut config = Config {
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
fn process_ready_state(
    node: &mut RawNode<MemStorage>,
    cbs: &mut HashMap<u64, oneshot::Sender<Result<RaftProcessedResponse, ClientGrpcRequestProcessingError>>>,
) {
    if !node.has_ready() {
        return;
    }

    let mut ready = node.ready();

    // Process messages
    if !ready.messages().is_empty() {
        for msg in ready.take_messages() {
            // Handle messages (e.g., send to other nodes)
            
        }
    }

    // Apply snapshot if present
    if !ready.snapshot().is_empty() {
        node.mut_store()
            .wl()
            .apply_snapshot(ready.snapshot().clone())
            .unwrap();
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
                    // Decode the RaftProposal from the committed entry data
                    let proposal = match RaftProposal::decode(entry.data.as_ref()) {
                        Ok(p) => p,
                        Err(e) => {
                            eprintln!("Failed to decode RaftProposal: {:?}", e);
                            continue;
                        }
                    };

                    let proposal_id = proposal.proposal_id;

                    // Open DB connection — if this fails, notify the waiting client (if any) and move on
                    let db = match get_db_connection() {
                        Ok(db) => db,
                        Err(e) => {
                            eprintln!("Failed to open DB connection: {:?}", e);
                            if let Some(sender) = cbs.remove(&proposal_id) {
                                let _ = sender.send(Err(ClientGrpcRequestProcessingError::DbResponseFailed));
                            }
                            continue;
                        }
                    };

                    // Apply the operation to the state machine (RocksDB)
                    // Build the response or error based on which operation it is
                    let result: Result<RaftProcessedResponse, ClientGrpcRequestProcessingError> = match proposal.operation {
                        Some(Operation::Create(ref create_req)) => {
                            match db_create(&db, create_req) {
                                Ok(created_id) => Ok(RaftProcessedResponse {
                                    id: Some(created_id.to_string()),
                                    success: true,
                                    data: Some(create_req.clone()),
                                }),
                                Err(e) => {
                                    eprintln!("DB create failed: {}", e);
                                    Err(ClientGrpcRequestProcessingError::DbResponseFailed)
                                }
                            }
                        }

                        Some(Operation::Update(ref update_req)) => {
                            match db_update(&db, &update_req.unique_id, update_req.balance) {
                                Ok(updated_id) => Ok(RaftProcessedResponse {
                                    id: Some(updated_id.to_string()),
                                    success: true,
                                    data: None,
                                }),
                                Err(e) => {
                                    eprintln!("DB update failed: {}", e);
                                    Err(ClientGrpcRequestProcessingError::DbResponseFailed)
                                }
                            }
                        }

                        Some(Operation::Delete(ref del_req)) => {
                            match db_delete(&db, &del_req.unique_id) {
                                Ok(deleted_id) => Ok(RaftProcessedResponse {
                                    id: Some(deleted_id.to_string()),
                                    success: true,
                                    data: None,
                                }),
                                Err(e) => {
                                    eprintln!("DB delete failed: {}", e);
                                    Err(ClientGrpcRequestProcessingError::DbResponseFailed)
                                }
                            }
                        }

                        Some(Operation::Get(ref get_req)) => {
                            match db_read(&db, &get_req.unique_id) {
                                Ok(record) => Ok(RaftProcessedResponse {
                                    id: Some(get_req.unique_id.clone()),
                                    success: true,
                                    data: Some(record),
                                }),
                                Err(e) => {
                                    eprintln!("DB read failed: {}", e);
                                    Err(ClientGrpcRequestProcessingError::DbResponseFailed)
                                }
                            }
                        }

                        None => {
                            eprintln!("No operation found in RaftProposal (id: {})", proposal_id);
                            Err(ClientGrpcRequestProcessingError::NodeUnaware)
                        }
                    };

                    // Send the result back to the gRPC handler if this node is the leader
                    // (followers won't have an entry in cbs — they just applied to DB silently)
                    if let Some(sender) = cbs.remove(&proposal_id) {
                        let _ = sender.send(result);
                    }
                }
                raft::eraftpb::EntryType::EntryConfChange => {
                    // Handle configuration change entry

                   //there is no decode function  
                    let config_change_data = eraftpb::ConfChange::new();
                   //apply the data to the raft node 
                   let conf_state = node.apply_conf_change(&config_change_data).unwrap();    
                   node.mut_store().wl().set_conf_state(conf_state); 

                }
                raft::eraftpb::EntryType::EntryConfChangeV2 => {
                    // Handle configuration change v2 entry
                    eprintln!(
                        "confchange received but not implemented yet"
                    )
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

    for entry in light_rd.take_committed_entries() {}

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
pub async fn processor_node(mut node: &mut RawNode<MemStorage>, mut rx: Receiver<Msg>) {
    let timeout_dur = Duration::from_millis(100);
    let mut remaining_timeout = timeout_dur;

    //cbs storage for the response of the client
    //contains key as the proposal id
    //contains value as the response tx 
    let mut cbs: HashMap<u64, oneshot::Sender<Result<RaftProcessedResponse, ClientGrpcRequestProcessingError>>> = HashMap::new();

    loop {
        let now = Instant::now();

        match timeout(remaining_timeout, rx.recv()).await {
            Ok(Some(Msg::Propose { proposemsg })) => {
                //check if the node is leader or not
                let is_leader = node.raft.state == StateRole::Leader;
                if !is_leader {
                    // If not leader, send an error back to the client
                    let _ = proposemsg.response_tx.send(Err(ClientGrpcRequestProcessingError::NotLeader));
                    continue;
                }

                //check for the operation if the operation is get request
                if proposemsg.operation_type == OperationType::Get {
                    // If it's a Get operation, send an error back to the client for now
                    //later we can implement seperate function to handle get request
                    let _ = proposemsg.response_tx.send(Err(ClientGrpcRequestProcessingError::GetRequestNotSupported));
                    continue;
                }

                //store the callback in the cbs hashmap
                cbs.insert(
                    proposemsg.id,
                     proposemsg.response_tx,
                );

                //propse the message to the raft node
                node.propose(proposemsg.id.to_be_bytes().to_vec(), proposemsg.data)
                    .unwrap();

                //check if raft node has data to be processed
                if node.has_ready() {
                    process_ready_state(&mut node, &mut cbs);
                }
            }

            Ok(Some(Msg::Raft(msg))) => match node.step(msg) {
                Ok(_) => {
                    if node.has_ready() {
                        process_ready_state(&mut node, &mut cbs);
                    }
                }
                Err(e) => {
                    eprintln!("Error stepping raft node: {:?}", e);
                }
            },

            Ok(None) => {
                unimplemented!("Channel disconnected");
            }

            Err(_) => {
                // Timeout occurred, drive the Raft node
                node.tick();
            }
        }

        let elapsed = now.elapsed();
        if elapsed >= remaining_timeout {
            remaining_timeout = timeout_dur;

            //drive raft event after timeout
            node.tick();

            //check if raft node has data to be processed
            if node.has_ready() {
                process_ready_state(&mut node, &mut cbs);
            }
        } else {
            remaining_timeout -= elapsed;
        }
    }

}

