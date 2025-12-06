use bytes::Bytes;
use go_objstore::{ObjectStore, ObjectStoreClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("==> QUIC/HTTP3 Client Example");
    println!("Note: This requires the go-objstore server to be running with HTTP3 enabled");

    // Create a QUIC client
    let addr = "127.0.0.1:4433".parse()?;
    let mut client = ObjectStoreClient::quic(addr, "localhost").await?;
    println!("✓ Created QUIC client");

    // Health check
    match client.health().await {
        Ok(health) => println!("✓ Health check: {:?}", health.status),
        Err(e) => println!("! Health check failed: {} (server may not have HTTP3 enabled)", e),
    }

    // Put an object
    let key = "examples/quic-test.txt";
    let data = Bytes::from("Hello from QUIC client!");

    match client.put(key, data.clone(), None).await {
        Ok(put_response) => {
            println!("✓ Put object: {}", key);
            println!("  Success: {}", put_response.success);

            // Check if exists
            let exists = client.exists(key).await?;
            println!("✓ Object exists: {}", exists);

            // Get the object
            let (retrieved_data, metadata) = client.get(key).await?;
            println!("✓ Got object: {} bytes", metadata.size);
            println!("  Data: {:?}", String::from_utf8_lossy(&retrieved_data));

            // Delete the object
            client.delete(key).await?;
            println!("✓ Deleted object: {}", key);

            println!("\n==> QUIC client example completed successfully!");
        }
        Err(e) => {
            println!("! Failed to put object: {}", e);
            println!("\nNote: QUIC/HTTP3 requires special server configuration.");
            println!("Ensure the go-objstore server is running with HTTP3 enabled.");
        }
    }

    Ok(())
}
