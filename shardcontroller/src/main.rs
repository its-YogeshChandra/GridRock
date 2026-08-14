use raft::Config;
use std::env;
use tokio::{self, main};
mod node;

pub struct Arguments {
    node_id: String,
    node_port: String,
}

//helper function
fn parse_env_arguments() -> Result<(u64, String), Box<dyn std::error::Error>> {
    //take the arguments from the env file and parse them
    let address1 = env::var("address1")?;
    let address2 = env::var("address1")?;
    let address3 = env::var("address1")?;

    let node_id_arg = env::var(" node_id")?;
    let node_id: u64 = node_id_arg.parse().expect("not a number");
    let node_port = env::var("node_port")?;

    let address_vector = [address1, address2, address3];

    Ok((node_id, node_port))
}

#[main]
async fn main() {
    let (node_id, node_port) = match parse_env_arguments() {
        Ok(value) => value,
        Err(_) => panic!("failed to get argument from parse env arguments"),
    };

    let config = Config {
        id: node_id,
        election_tick: 10,
        heartbeat_tick: 1,
        ..Default::default()
    };
}
