pub fn build_storage_proto() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("src/proto/storage.proto")?;
    Ok(())
}

pub fn build_shard_config_proto() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("src/proto/shardconfig.proto")?;
    Ok(())
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Point to the wrapper script so protoc version 35.x reports as 3.21.0
    // (tonic_prost_build expects major version 3)
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let wrapper = format!("{}/protoc-wrapper.sh", manifest_dir);
    // SAFETY: build.rs is single-threaded, so set_var is safe here.
    unsafe { std::env::set_var("PROTOC", &wrapper) };

    build_storage_proto()?;
    build_shard_config_proto()?;
    Ok(())
}

