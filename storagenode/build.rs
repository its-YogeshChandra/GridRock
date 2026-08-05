
pub fn build_storage_proto() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("src/proto/storage.proto")?;
    Ok(())
}

pub fn build_node_comm_proto() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("src/proto/node_communication.proto")?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    build_storage_proto()?;
    build_node_comm_proto()?;
    Ok(())
}