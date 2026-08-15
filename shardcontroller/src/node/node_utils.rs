use std::io::empty;

use protobuf::json;
use raft::{
    Config,
    raw_node::{self, RawNode},
    storage::MemStorage,
};
use slog::{Drain, o};
use slog_stdlog;
use tokio::{
    sync::{
        mpsc::{self, channel},
        oneshot,
    },
    time::{Duration, Instant, timeout},
};

//has to change t with the struct definition from grpc client handler
pub struct ProposeMsgRequest<T> {
    id: u64,
    data: Vec<u64>,
    rx_sender: oneshot::Sender<T>,
}

pub enum ProposeMsg<T> {
    ProposeMsg { propose_msg: ProposeMsgRequest<T> },
    Raft(raft::eraftpb::Message),
    ConfigChange,
}

//create node for the thing
pub fn create_node(config: Config) -> RawNode<MemStorage> {
    //create logger
    let logger = slog::Logger::root(slog_stdlog::StdLog.fuse(), o!());

    //create storage
    let store = MemStorage::new_with_conf_state((vec![1], vec![]));

    match RawNode::new(&config, store, &logger) {
        Ok(value) => value,
        Err(_) => {
            panic!("failed to create raft node")
        }
    }
}

//create process node
pub async fn process_node<T>(
    proposemsg: ProposeMsg<T>,
    mut recv: mpsc::Receiver<ProposeMsg<T>>,
    node: &mut RawNode<MemStorage>,
    sender: mpsc::Sender<ProposeMsg<T>>,
) {
    let timeout_dur = Duration::from_millis(100);
    let mut remaining_timeout = timeout_dur;

    //loop creation never ending loop
    //there shouldn't be a break condition in the loop
    loop {
        let now = Instant::now();

        match timeout(remaining_timeout, recv.recv()).await {
            Ok(_) => {}
            Err(_) => {}
        }
        let elaspsed_time = now.elapsed();

        if elaspsed_time >= remaining_timeout {
            remaining_timeout = timeout_dur;
            node.tick();

            //check for the process ready state at this point
        } else {
            remaining_timeout -= elaspsed_time
        }
    }
}
