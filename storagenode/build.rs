
pub fn build_storage_proto() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("src/proto/storage.proto")?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    build_storage_proto()?;
    Ok(())
}