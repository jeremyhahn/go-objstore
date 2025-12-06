use bytes::Bytes;
use go_objstore::{Metadata, ObjectStore, ObjectStoreClient};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("==> REST Client Example");

    // Create a REST client
    let mut client = ObjectStoreClient::rest("http://localhost:8080")?;
    println!("✓ Created REST client");

    // Health check
    let health = client.health().await?;
    println!("✓ Health check: {:?}", health.status);

    // Put an object
    let key = "examples/rest-test.txt";
    let data = Bytes::from("Hello from REST client!");

    let mut custom = HashMap::new();
    custom.insert("example".to_string(), "rest".to_string());

    let metadata = Metadata {
        content_type: Some("text/plain".to_string()),
        custom,
        ..Default::default()
    };

    let put_response = client.put(key, data.clone(), Some(metadata)).await?;
    println!("✓ Put object: {}", key);
    println!("  ETag: {:?}", put_response.etag);

    // Check if exists
    let exists = client.exists(key).await?;
    println!("✓ Object exists: {}", exists);

    // Get metadata
    let metadata = client.get_metadata(key).await?;
    println!("✓ Got metadata:");
    println!("  Size: {} bytes", metadata.size);
    println!("  Content-Type: {:?}", metadata.content_type);
    println!("  Custom: {:?}", metadata.custom);

    // Get the object
    let (retrieved_data, metadata) = client.get(key).await?;
    println!("✓ Got object: {} bytes", metadata.size);
    println!("  Data: {:?}", String::from_utf8_lossy(&retrieved_data));

    // List objects
    let list_request = go_objstore::ListRequest {
        prefix: Some("examples/".to_string()),
        ..Default::default()
    };

    let list_response = client.list(list_request).await?;
    println!("✓ Listed {} objects with prefix 'examples/'", list_response.objects.len());
    for obj in &list_response.objects {
        println!("  - {} ({} bytes)", obj.key, obj.metadata.size);
    }

    // Delete the object
    client.delete(key).await?;
    println!("✓ Deleted object: {}", key);

    // Verify deletion
    let exists = client.exists(key).await?;
    println!("✓ Object exists after deletion: {}", exists);

    println!("\n==> REST client example completed successfully!");

    Ok(())
}
