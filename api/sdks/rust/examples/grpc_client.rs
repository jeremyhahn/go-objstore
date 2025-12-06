use bytes::Bytes;
use go_objstore::{LifecyclePolicy, Metadata, ObjectStore, ObjectStoreClient, ReplicationMode, ReplicationPolicy};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("==> gRPC Client Example");

    // Create a gRPC client
    let mut client = ObjectStoreClient::grpc("http://localhost:50051").await?;
    println!("✓ Created gRPC client");

    // Health check
    let health = client.health().await?;
    println!("✓ Health check: {:?}", health.status);

    // Put an object
    let key = "examples/grpc-test.txt";
    let data = Bytes::from("Hello from gRPC client!");

    let mut custom = HashMap::new();
    custom.insert("example".to_string(), "grpc".to_string());
    custom.insert("protocol".to_string(), "protobuf".to_string());

    let metadata = Metadata {
        content_type: Some("text/plain".to_string()),
        custom,
        ..Default::default()
    };

    let put_response = client.put(key, data.clone(), Some(metadata)).await?;
    println!("✓ Put object: {}", key);
    println!("  Success: {}", put_response.success);

    // Check if exists
    let exists = client.exists(key).await?;
    println!("✓ Object exists: {}", exists);

    // Get metadata
    let metadata = client.get_metadata(key).await?;
    println!("✓ Got metadata:");
    println!("  Size: {} bytes", metadata.size);
    println!("  Custom: {:?}", metadata.custom);

    // Get the object
    let (retrieved_data, metadata) = client.get(key).await?;
    println!("✓ Got object: {} bytes", metadata.size);
    println!("  Data: {:?}", String::from_utf8_lossy(&retrieved_data));

    // List objects
    let list_request = go_objstore::ListRequest {
        prefix: Some("examples/".to_string()),
        max_results: Some(10),
        ..Default::default()
    };

    let list_response = client.list(list_request).await?;
    println!("✓ Listed {} objects", list_response.objects.len());

    // Lifecycle Policies
    println!("\n==> Testing Lifecycle Policies");

    let policy = LifecyclePolicy {
        id: "example-policy".to_string(),
        prefix: "examples/".to_string(),
        retention_seconds: 86400, // 1 day
        action: "delete".to_string(),
        destination_type: None,
        destination_settings: HashMap::new(),
    };

    client.add_policy(policy).await?;
    println!("✓ Added lifecycle policy");

    let policies = client.get_policies(None).await?;
    println!("✓ Retrieved {} lifecycle policies", policies.len());
    for policy in &policies {
        println!("  - {} (prefix: {}, action: {})", policy.id, policy.prefix, policy.action);
    }

    client.remove_policy("example-policy").await?;
    println!("✓ Removed lifecycle policy");

    // Replication Policies
    println!("\n==> Testing Replication Policies");

    let mut source_settings = HashMap::new();
    source_settings.insert("path".to_string(), "/tmp/source".to_string());

    let mut dest_settings = HashMap::new();
    dest_settings.insert("path".to_string(), "/tmp/dest".to_string());

    let repl_policy = ReplicationPolicy {
        id: "example-replication".to_string(),
        source_backend: "local".to_string(),
        source_settings,
        source_prefix: "examples/".to_string(),
        destination_backend: "local".to_string(),
        destination_settings: dest_settings,
        check_interval_seconds: 300,
        last_sync_time: None,
        enabled: true,
        encryption: None,
        replication_mode: ReplicationMode::Transparent,
    };

    client.add_replication_policy(repl_policy).await?;
    println!("✓ Added replication policy");

    let repl_policies = client.get_replication_policies().await?;
    println!("✓ Retrieved {} replication policies", repl_policies.len());

    // Clean up replication policy
    client.remove_replication_policy("example-replication").await?;
    println!("✓ Removed replication policy");

    // Delete the object
    client.delete(key).await?;
    println!("\n✓ Deleted object: {}", key);

    println!("\n==> gRPC client example completed successfully!");

    Ok(())
}
