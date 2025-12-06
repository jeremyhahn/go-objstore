use bytes::Bytes;
use go_objstore::{ObjectStore, ObjectStoreClient};

/// Demonstrates using the unified ObjectStore trait
async fn test_client(mut client: impl ObjectStore, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n==> Testing {} client", name);

    // Health check
    let health = client.health().await?;
    println!("  ✓ Health: {:?}", health.status);

    // Put object
    let key = &format!("examples/unified-{}.txt", name.to_lowercase());
    let data = Bytes::from(format!("Hello from {}!", name));

    client.put(key, data.clone(), None).await?;
    println!("  ✓ Put object");

    // Check exists
    let exists = client.exists(key).await?;
    println!("  ✓ Exists: {}", exists);

    // Get object
    let (retrieved_data, metadata) = client.get(key).await?;
    println!("  ✓ Got object: {} bytes", metadata.size);
    assert_eq!(retrieved_data, data);

    // Delete object
    client.delete(key).await?;
    println!("  ✓ Deleted object");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("==> Unified Client Example");
    println!("Demonstrating polymorphism with ObjectStore trait");

    // Test REST client
    let rest_client = ObjectStoreClient::rest("http://localhost:8080")?;
    test_client(rest_client, "REST").await?;

    // Test gRPC client
    let grpc_client = ObjectStoreClient::grpc("http://localhost:50051").await?;
    test_client(grpc_client, "gRPC").await?;

    // Demonstrate using trait objects
    println!("\n==> Using trait objects");

    let clients: Vec<Box<dyn ObjectStore>> = vec![
        Box::new(ObjectStoreClient::rest("http://localhost:8080")?),
        Box::new(ObjectStoreClient::grpc("http://localhost:50051").await?),
    ];

    for (i, mut client) in clients.into_iter().enumerate() {
        let key = &format!("examples/trait-object-{}.txt", i);
        let data = Bytes::from(format!("Trait object {}", i));

        client.put(key, data.clone(), None).await?;
        println!("  ✓ Put via trait object {}", i);

        client.delete(key).await?;
        println!("  ✓ Deleted via trait object {}", i);
    }

    println!("\n==> Unified client example completed successfully!");
    println!("This demonstrates that all clients implement the same ObjectStore trait,");
    println!("allowing for flexible, protocol-agnostic code.");

    Ok(())
}
