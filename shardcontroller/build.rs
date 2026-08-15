use std::error::Error;

fn node_to_node_comm() -> Result<(), Box<dyn Error>> {
    tonic_prost_build::compile_protos("src/node_comm.proto")?;
    Ok(())
}

fn main() {
    //call the node to node communication proto
    let node_proto_response = node_to_node_comm();
    match node_proto_response {
        Ok(_) => {
            println!("node comm proto build successfull")
        }
        Err(_) => {
            panic!("failed to create node comm proto build ")
        }
    }
}
