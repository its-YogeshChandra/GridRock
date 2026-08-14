use raft::{
    Config,
    raw_node::{self, RawNode},
    storage::MemStorage,
};
use slog::{Drain, o};
use slog_stdlog;

pub enum ProposeMsg {}

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
pub fn process_node(proposemsg: ProposeMsg) {}
