fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("src/greeter.proto")?;
    tonic_prost_build::compile_protos(proto)
    Ok(())
}
