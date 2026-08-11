use crate::node_comm::RaftMessageRequest;
use protobuf::Message as protobufMessage;
use raft::eraftpb;
use prost::Message;
use raft::{Config, StateRole, raw_node::RawNode, storage::MemStorage};
use slog::{Drain, o};
use std::collections::HashMap;

use std::ops::ControlFlow::Continue;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc::Receiver, oneshot};
use tokio::time::timeout;
use crate::errors::request_errors::ClientGrpcRequestProcessingError;
use crate::server::RaftProcessedResponse;
use crate::db::{db_create, db_read, db_update, db_delete, get_db_connection};
use crate::storage_proto::RaftProposal;
use crate::storage_proto::raft_proposal::Operation;
use std::sync::{Arc, RwLock};
use crate::ClusterState;
use crate::grpc_client::node_comm_client::{forward_proposal, send_raft_message};
use slog_stdlog;


//Create a private module to "seal" the trait
// mod private {
//     pub trait Sealed {}

//     // Implement the private trait ONLY for your specific structs
//     impl Sealed for super::GetValRequest {}
//     impl Sealed for super::UpdateRequest {}
//     impl Sealed for super::CreateRequest {}
//     impl Sealed for super::DelValRequest {}
// }

// //Define the public trait and require the private `Sealed` bound
// pub trait AllowedTypes: private::Sealed {
//     // You can also add common methods here if needed
// }

// //Implement the public trait for your structs
// impl AllowedTypes for GetValRequest {}
// impl AllowedTypes for UpdateRequest {}
// impl AllowedTypes for CreateRequest {}
// impl AllowedTypes for DelValRequest {}

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
pub struct ConfChangeMessage{
    pub id: u64,
    pub cc: eraftpb::ConfChange,
    pub response_tx: oneshot::Sender<Result<RaftProcessedResponse, ClientGrpcRequestProcessingError>>,
}

/// Tracks a pending linearizable read waiting for ReadState confirmation from raft.
pub struct PendingRead {
    /// The serialized RaftProposal bytes (contains the Get operation + key)
    pub data: Vec<u8>,
    /// Channel to send the read result back to the gRPC handler
    pub response_tx: oneshot::Sender<Result<RaftProcessedResponse, ClientGrpcRequestProcessingError>>,
}



pub enum Msg {
    Propose { proposemsg: ProposeMessage },
    Raft(eraftpb::Message),
    ConfChange{confchange_msg: ConfChangeMessage},
    // You can add more message types here if needed
}

/// Parses an address string into (host, port).
/// Supports:
///   - "[::1]:50051"         -> ("::1", 50051)
///   - "storagenode1:50051"  -> ("storagenode1", 50051)
///   - "172.17.0.2:50051"    -> ("172.17.0.2", 50051)
fn parse_address(address: &str) -> (String, u16) {
    if let Some(bracket_end) = address.rfind(']') {
        // IPv6 bracket format: [::1]:50051
        let host = address[1..bracket_end].to_string();
        let port_str = &address[bracket_end + 2..]; // skip ]:
        (host, port_str.parse().expect("invalid port in address"))
    } else {
        // hostname:port or ipv4:port — split on the last ':'
        let colon_pos = address.rfind(':').expect("expected host:port format");
        let host = address[..colon_pos].to_string();
        let port_str = &address[colon_pos + 1..];
        (host, port_str.parse().expect("invalid port in address"))
    }
}

//helper function to send message 
pub async fn send_messages(msg: eraftpb::Message, cluster_state: Option<Arc<RwLock<ClusterState>>>) -> bool {
 // Handle persisted messages (e.g., send to other nodes)  
    let receiver_id = msg.to;
    let receiver_address_string: String = match cluster_state {
        Some(ref value) => {
            let cluster_state_lock = value.read().unwrap(); //returns hashmap
            match cluster_state_lock.peers.get(&receiver_id) {
                Some(address) => address.clone(),
                None => {
                    eprintln!("Failed to get address for receiver id {}", receiver_id);
                    return false;
                }
            }
        }
        None => {
            eprintln!("No cluster state available; cannot send message to peer {}", receiver_id);
            return false;
        }
    };
        
        
    //extract address and port from peer string
    let (receiver_address, receiver_port) = parse_address(&receiver_address_string);
    
    //used unwrap ( future me problem )
    let msg_bytes = msg.write_to_bytes().unwrap(); 
   
    //contruct the message request 
    let message= RaftMessageRequest {
        message: msg_bytes,
    };
        
    let client_response = send_raft_message(&receiver_address, receiver_port, message).await;
    match client_response {
        Ok(_) => {
            eprintln!("Message sent to peer {}", receiver_address);  
        }
        Err(e) => {
            eprintln!("Failed to send raft message: {}", e);
            return false
        }
    }  
    true  
}


//helper function to commit the entry in the key value store
pub fn append_committed_entry(entry: eraftpb::Entry, cbs: &mut HashMap<u64, oneshot::Sender<Result<RaftProcessedResponse, ClientGrpcRequestProcessingError>>>, db: &rocksdb::DB) {
    //check if the entry is data entry
    let proposal = match RaftProposal::decode(entry.data.as_ref()) {
                        Ok(p) => p,
                        Err(e) => {
                            eprintln!("[Raft] Failed to decode RaftProposal: {:?}", e);
                            return;
                        }
                    };

                    let proposal_id = proposal.proposal_id;
                    eprintln!("[Raft] Committing entry | proposal_id={:#018x}", proposal_id);

                    
                    // Apply the operation to the state machine (RocksDB)
                    // Build the response or error based on which operation it is
                    let result: Result<RaftProcessedResponse, ClientGrpcRequestProcessingError> = match proposal.operation {
                        Some(Operation::Create(ref create_req)) => {
                            eprintln!("[Raft] DB CREATE | key={}", create_req.unique_id);
                            match db_create(&db, create_req) {
                                Ok(created_id) => Ok(RaftProcessedResponse {
                                    id: Some(created_id.to_string()),
                                    success: true,
                                    data: Some(create_req.clone()),
                                }),
                                Err(e) => {
                                    eprintln!("[Raft] DB create failed: {}", e);
                                    Err(ClientGrpcRequestProcessingError::DbResponseFailed)
                                }
                            }
                        }

                        Some(Operation::Update(ref update_req)) => {
                            eprintln!("[Raft] DB UPDATE | key={}", update_req.unique_id);
                            match db_update(&db, &update_req.unique_id, update_req.balance) {
                                Ok(updated_id) => Ok(RaftProcessedResponse {
                                    id: Some(updated_id.to_string()),
                                    success: true,
                                    data: None,
                                }),
                                Err(e) => {
                                    eprintln!("[Raft] DB update failed: {}", e);
                                    Err(ClientGrpcRequestProcessingError::DbResponseFailed)
                                }
                            }
                        }

                        Some(Operation::Delete(ref del_req)) => {
                            eprintln!("[Raft] DB DELETE | key={}", del_req.unique_id);
                            match db_delete(&db, &del_req.unique_id) {
                                Ok(deleted_id) => Ok(RaftProcessedResponse {
                                    id: Some(deleted_id.to_string()),
                                    success: true,
                                    data: None,
                                }),
                                Err(e) => {
                                    eprintln!("[Raft] DB delete failed: {}", e);
                                    Err(ClientGrpcRequestProcessingError::DbResponseFailed)
                                }
                            }
                        }

                        Some(Operation::Get(ref get_req)) => {
                            eprintln!("[Raft] DB GET (via commit) | key={}", get_req.unique_id);
                            match db_read(&db, &get_req.unique_id) {
                                Ok(record) => Ok(RaftProcessedResponse {
                                    id: Some(get_req.unique_id.clone()),
                                    success: true,
                                    data: Some(record),
                                }),
                                Err(e) => {
                                    eprintln!("[Raft] DB read failed: {}", e);
                                    Err(ClientGrpcRequestProcessingError::DbResponseFailed)
                                }
                            }
                        }

                        None => {
                            eprintln!("[Raft] No operation found in RaftProposal (id: {:#018x})", proposal_id);
                            Err(ClientGrpcRequestProcessingError::NodeUnaware)
                        }
                    };

                    // Send the result back to the gRPC handler if this node is the leader
                    // (followers won't have an entry in cbs — they just applied to DB silently)
                    if let Some(sender) = cbs.remove(&proposal_id) {
                        eprintln!("[Raft] Firing callback for proposal_id={:#018x}", proposal_id);
                        let _ = sender.send(result);
                    } else {
                        eprintln!("[Raft] No callback for proposal_id={:#018x} (follower apply)", proposal_id);
                    }
    
}


//function to create the raft node
pub fn create_raft_node(id: u64, peers: Vec<u64>) -> RawNode<MemStorage> {
    //question : do we need to create raft node every time ? or have to create it once and check if already present

    let config = Config {
        id,
        election_tick: 10,
        heartbeat_tick: 1,
        ..Default::default()
    };
    config.validate().unwrap();
    let node_storage = MemStorage::new_with_conf_state((peers, vec![]));
    let logger = slog::Logger::root(slog_stdlog::StdLog.fuse(), o!());
    RawNode::new(&config, node_storage, &logger).unwrap()
}

//helper function to process the ready state of the raft node
async fn process_ready_state(
    node: &mut RawNode<MemStorage>,
    cbs: &mut HashMap<u64, oneshot::Sender<Result<RaftProcessedResponse, ClientGrpcRequestProcessingError>>>,
    cluster_state: Option<Arc<RwLock<ClusterState>>>,
    pending_reads: &mut HashMap<Vec<u8>, PendingRead>,
) {
    if !node.has_ready() {
        return;
    }

    let mut ready = node.ready();

    // Process messages
    if !ready.messages().is_empty() {
        for msg in ready.take_messages() {
            let send_response = send_messages(msg, cluster_state.clone()).await;
            if !send_response {
                panic!("Failed to send raft message");
            }
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
        //update the leader id in the cluster state 
        if let Some(value) = cluster_state.clone(){
            let mut state = value.write().unwrap();
            state.leader_id = node.raft.leader_id;
        }
    }

    // Process read states — these are linearizable read confirmations from raft
    // Each ReadState means raft has confirmed the leader still holds leadership,
    // so it's safe to read from the local DB at this point
    for rs in ready.read_states() {
        if let Some(pending) = pending_reads.remove(&rs.request_ctx) {
            // Decode the RaftProposal to extract the Get key
            let result = match RaftProposal::decode(pending.data.as_ref()) {
                Ok(proposal) => match proposal.operation {
                    Some(Operation::Get(ref get_req)) => {
                        let db = match get_db_connection() {
                            Ok(db) => db,
                            Err(e) => {
                                eprintln!("Failed to open DB for read: {:?}", e);
                                let _ = pending.response_tx.send(Err(
                                    ClientGrpcRequestProcessingError::DbResponseFailed,
                                ));
                                continue;
                            }
                        };
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
                    _ => {
                        eprintln!("ReadState contained non-Get operation");
                        Err(ClientGrpcRequestProcessingError::NodeUnaware)
                    }
                },
                Err(e) => {
                    eprintln!("Failed to decode RaftProposal for read: {:?}", e);
                    Err(ClientGrpcRequestProcessingError::NodeUnaware)
                }
            };
            let _ = pending.response_tx.send(result);
        }
    }

    // Process committed entries
    if !ready.committed_entries().is_empty() {
        eprintln!("[Raft] process_ready_state: {} committed entries", ready.committed_entries().len());
        for entry in ready.take_committed_entries() {
            if entry.data.is_empty() {
                continue;
            }
            match entry.get_entry_type() {
                raft::eraftpb::EntryType::EntryNormal => {
                    // Open DB connection for this entry
                    let db = match get_db_connection() {
                        Ok(db) => db,
                        Err(e) => {
                            eprintln!("Failed to open DB connection: {:?}", e);
                            continue;
                        }
                    };

                    // Decode, apply to RocksDB, and send response via the helper
                    append_committed_entry(entry, cbs, &db);
                }

                raft::eraftpb::EntryType::EntryConfChange => {
                    // Deserialize the ConfChange from committed entry data
                    let mut conf_change_data = eraftpb::ConfChange::default();
                    conf_change_data.merge_from_bytes(&entry.data).ok().unwrap();

                    // Apply the conf change to the raft node
                    let conf_state = node.apply_conf_change(&conf_change_data).unwrap();
                    node.mut_store().wl().set_conf_state(conf_state);

                    // Update the cluster state with new peer
                    let peer_address = String::from_utf8(conf_change_data.context.to_vec())
                        .unwrap_or_default();

                    if let Some(ref cluster_state_ref) = cluster_state {
                        let mut state = cluster_state_ref.write().unwrap();

                       //check for the new node change type (addition or removal) 
                        match conf_change_data.get_change_type() {
                            eraftpb::ConfChangeType::AddNode => {
                                state.peers.insert(conf_change_data.node_id, peer_address);
                            }
                            eraftpb::ConfChangeType::RemoveNode => {
                                state.peers.remove(&conf_change_data.node_id);
                            }
                            _ => {}
                        }
                    }

                    // Send success response back via the callback channel
                    // The conf change id was encoded as 8-byte big-endian in the entry context
                    let conf_change_id = conf_change_data.id;
                    if let Some(sender) = cbs.remove(&conf_change_id) {
                        let _ = sender.send(Ok(RaftProcessedResponse {
                            id: Some(conf_change_data.node_id.to_string()),
                            success: true,
                            data: None,
                        }));
                    }
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

    //for sending persistent messages 
    for msg in ready.take_persisted_messages() {
        let send_msg_response = send_messages(msg, cluster_state.clone()).await; 
        if !send_msg_response{
            panic!("failed to send raft message")
        }   
    }

    let mut light_rd = node.advance(ready);

    // Send any additional messages to peers (transport layer — not yet implemented)
    for msg in light_rd.take_messages() {
        // TODO: send to peer nodes via transport
        let send_msg_response = send_messages(msg, cluster_state.clone()).await;
    
        if !send_msg_response{
            //send the response back to the grpc handler
            panic!("failed to send raft message")
        }
    }

    // Apply any additional committed entries from the light ready
    for entry in light_rd.take_committed_entries() {
        if entry.data.is_empty() {
            continue;
        }
        let db = match get_db_connection() {
            Ok(db) => db,
            Err(e) => {
                eprintln!("Failed to open DB connection: {:?}", e);
                continue;
            }
        };
        append_committed_entry(entry, cbs, &db);
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
pub async fn processor_node(mut node: &mut RawNode<MemStorage>, mut rx: Receiver<Msg>, cluster_state: Arc<RwLock<ClusterState>>, sender: tokio::sync::mpsc::Sender<Msg>) {
    let timeout_dur = Duration::from_millis(100);
    let mut remaining_timeout = timeout_dur;

    //cbs storage for the response of the client
    //contains key as the proposal id
    //contains value as the response tx 
    let mut cbs: HashMap<u64, oneshot::Sender<Result<RaftProcessedResponse, ClientGrpcRequestProcessingError>>> = HashMap::new();

    //pending reads storage for linearizable read requests
    //keyed by the read context (proposal id as bytes), value is the PendingRead struct
    let mut pending_reads: HashMap<Vec<u8>, PendingRead> = HashMap::new();

    //kick off the the leader campaign 
    let election_campaign = node.campaign().map_err(|_| "Failed to start leader election");
    if election_campaign.is_err(){
        panic!("election campaign failed")
    }

    loop {
        let now = Instant::now();

        match timeout(remaining_timeout, rx.recv()).await {
            Ok(Some(Msg::Propose { proposemsg })) => {
                //check if the node is leader or not
                let is_leader = node.raft.state == StateRole::Leader;
                let role = if is_leader { "LEADER" } else { "FOLLOWER" };
                eprintln!("[Raft] Propose received | role={} leader_id={} proposal_id={:#018x}",
                    role, node.raft.leader_id, proposemsg.id);

                if !is_leader && node.raft.leader_id == 0 {
                    eprintln!("[Raft] No leader yet — re-queuing proposal");
                    let reque_response  = sender.send(Msg::Propose { proposemsg }).await;
                    if reque_response.is_err() {
                        panic!("failed to reque the request from grpc handler");
                    }
                    continue;
                }



                if !is_leader {

                    //call the forward proposal function to send request to client
                    let leader_id = node.raft.leader_id; 


                    // Extract leader address under a short-lived lock, then drop it before .await
                    let leader_data = {
                        let cluster_state = cluster_state.read().unwrap();
                        cluster_state.peers.get(&leader_id).cloned()
                    };

                    let Some(leader_data) = leader_data else {
                        let send_error = proposemsg.response_tx.send(Err(ClientGrpcRequestProcessingError::LeaderNotFound));
                        if send_error.is_err() {
                            panic!("error sending error back to grpc handler");
                        }
                        continue;
                    };

                    //get address and port from the string
                    let (leader_address, leader_port) = parse_address(&leader_data);
                    eprintln!("[Raft] Forwarding proposal to leader at {}:{}", leader_address, leader_port);

                    let client_response = forward_proposal(&leader_address, leader_port, proposemsg.data, node.raft.id).await;
                    match client_response {
                        Ok(response) => {
                            let grpc_response = RaftProcessedResponse { id: None, success: response.success, data: None };
                            let _ = proposemsg.response_tx.send(Ok(grpc_response));
                        }
                        Err(_error) => {
                            let _ = proposemsg.response_tx.send(Err(ClientGrpcRequestProcessingError::RequestForwardingFailed));
                        }
                    }

                    continue;
                }

                //check for the operation if the operation is get request
                if proposemsg.operation_type == OperationType::Get {
                    // Linearizable read: use read_index to confirm leader lease,
                    // then read from local DB once raft confirms via ReadState
                    let rctx = proposemsg.id.to_be_bytes().to_vec();
                    pending_reads.insert(
                        rctx.clone(),
                        PendingRead {
                            data: proposemsg.data,
                            response_tx: proposemsg.response_tx,
                        },
                    );

                    node.read_index(rctx);

                    if node.has_ready() {
                        process_ready_state(&mut node, &mut cbs, Some(cluster_state.clone()), &mut pending_reads).await;
                    }
                    continue;
                }

                //store the callback in the cbs hashmap
                cbs.insert(
                    proposemsg.id,
                     proposemsg.response_tx,
                );

                eprintln!("[Raft] Proposing locally | proposal_id={:#018x}", proposemsg.id);
                //propse the message to the raft node
                node.propose(proposemsg.id.to_be_bytes().to_vec(), proposemsg.data)
                    .unwrap();

                //check if raft node has data to be processed
                if node.has_ready() {
                    process_ready_state(&mut node, &mut cbs, Some(cluster_state.clone()), &mut pending_reads).await;
                }
            }

            Ok(Some(Msg::Raft(msg))) => match node.step(msg) {
                Ok(_) => {
                    if node.has_ready() {
                        process_ready_state(&mut node, &mut cbs, Some(cluster_state.clone()), &mut pending_reads).await;
                    }
                }
                Err(e) => {
                    eprintln!("Error stepping raft node: {:?}", e);
                }
            },

            //function for conf change request 
            Ok(Some(Msg::ConfChange{confchange_msg})) => {
                //handle the conf change request
                let cc_id = confchange_msg.id;

                // Store the callback so we can respond when the conf change is committed
                cbs.insert(cc_id, confchange_msg.response_tx);

                match node.propose_conf_change(cc_id.to_be_bytes().to_vec(), confchange_msg.cc) {
                    Ok(_) => {
                        if node.has_ready() {
                            process_ready_state(&mut node, &mut cbs, Some(cluster_state.clone()), &mut pending_reads).await;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error proposing conf change: {:?}", e);
                        // Remove and notify the caller about the failure
                        if let Some(sender) = cbs.remove(&cc_id) {
                            let _ = sender.send(Err(ClientGrpcRequestProcessingError::NodeUnaware));
                        }
                    }
                }
            }

            Ok(None) => {
                unimplemented!("Channel disconnected");
            }

            Err(_) => {
             node.tick();
             eprintln!("[Raft] tick | role={:?} leader_id={} term={}",
                node.raft.state, node.raft.leader_id, node.raft.term);
            }
        }

        let elapsed = now.elapsed();
        if elapsed >= remaining_timeout {
            remaining_timeout = timeout_dur;

            //drive raft event after timeout
            node.tick();

            //check if raft node has data to be processed
            if node.has_ready() {
                process_ready_state(&mut node, &mut cbs, Some(cluster_state.clone()), &mut pending_reads).await;
            }
        } else {
            remaining_timeout -= elapsed;
        }
    }

}
