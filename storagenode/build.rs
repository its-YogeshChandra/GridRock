
pub fn build_storage_proto() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("src/proto/storage.proto")?;
    Ok(())
}

pub fn build_node_comm_proto() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("src/proto/node_communication.proto")?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set PROTOC to the Homebrew-installed binary so prost-build can find it.
    // If protoc is already on PATH via PROTOC env var, respect that; otherwise
    // fall back to the Homebrew default location.
    if std::env::var("PROTOC").is_err() {
        let protoc_path = "/opt/homebrew/bin/protoc";
        if std::path::Path::new(protoc_path).exists() {
            // SAFETY: build.rs is single-threaded, so set_var is safe here.
            unsafe { std::env::set_var("PROTOC", protoc_path) };
        }
    }
    
    build_storage_proto()?;
    build_node_comm_proto()?;
    Ok(())
}