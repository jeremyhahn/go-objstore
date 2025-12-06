use std::path::Path;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Support both Docker and local builds
    let (proto_file, proto_dir) = if Path::new("/proto/objstore.proto").exists() {
        ("/proto/objstore.proto", "/proto")
    } else {
        ("../../proto/objstore.proto", "../../proto")
    };

    println!("cargo:rerun-if-changed={}", proto_file);

    // Create src/proto directory if it doesn't exist
    let proto_out_dir = Path::new("src/proto");
    if !proto_out_dir.exists() {
        fs::create_dir_all(proto_out_dir)?;
    }

    // Configure tonic to generate client code
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir("src/proto")
        .compile(&[proto_file], &[proto_dir])?;

    Ok(())
}
