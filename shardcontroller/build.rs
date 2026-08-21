pub fn main() {
    // Point to the wrapper script so protoc version 35.x reports as 3.21.0
    // (tonic_prost_build expects major version 3).
    // The wrapper only exists on the host — in Docker it is absent and apt's
    // protobuf-compiler (3.21) is picked up from PATH instead.
    if std::env::var("PROTOC").is_err() {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let wrapper = format!("{}/protoc-wrapper.sh", manifest_dir);
        if std::path::Path::new(&wrapper).exists() {
            // SAFETY: build.rs is single-threaded, so set_var is safe here.
            unsafe { std::env::set_var("PROTOC", &wrapper) };
        }
    }

    tonic_prost_build::compile_protos("src/proto/config.proto").expect("Failed to compile proto");
}
