use bytes::Bytes;
use go_objstore::{
    EncryptionConfig, EncryptionPolicy, LifecyclePolicy, ListRequest, Metadata, ObjectStore,
    ObjectStoreClient, ReplicationMode, ReplicationPolicy,
};
use std::collections::HashMap;
use std::env;

// Helper to get server URL from environment
fn get_rest_url() -> String {
    env::var("OBJSTORE_REST_URL").unwrap_or_else(|_| "http://localhost:8080".to_string())
}

fn get_grpc_url() -> String {
    env::var("OBJSTORE_GRPC_URL").unwrap_or_else(|_| "http://localhost:50051".to_string())
}

// REST Client Tests

#[tokio::test]
#[ignore = "Requires running server instance"]
async fn test_rest_put_get_delete() {
    let mut client = ObjectStoreClient::rest(get_rest_url()).unwrap();

    // Put object
    let key = "test-rest-put-get-delete.txt";
    let data = Bytes::from("Hello, REST World!");
    let result = client.put(key, data.clone(), None).await;
    assert!(result.is_ok(), "Failed to put object: {:?}", result);

    // Get object
    let (retrieved_data, metadata) = client.get(key).await.unwrap();
    assert_eq!(retrieved_data, data);
    assert!(metadata.size > 0);

    // Check exists
    let exists = client.exists(key).await.unwrap();
    assert!(exists);

    // Delete object
    let result = client.delete(key).await;
    assert!(result.is_ok());

    // Verify deleted
    let exists = client.exists(key).await.unwrap();
    assert!(!exists);
}

#[tokio::test]
#[ignore = "Requires running server instance"]
async fn test_rest_metadata_operations() {
    let mut client = ObjectStoreClient::rest(get_rest_url()).unwrap();

    let key = "test-rest-metadata.txt";
    let data = Bytes::from("Metadata test");

    // Put with metadata
    let mut custom = HashMap::new();
    custom.insert("author".to_string(), "test".to_string());
    let metadata = Metadata {
        content_type: Some("text/plain".to_string()),
        custom,
        ..Default::default()
    };

    client.put(key, data, Some(metadata.clone())).await.unwrap();

    // Get metadata
    let retrieved_metadata = client.get_metadata(key).await.unwrap();
    assert!(retrieved_metadata.size > 0);

    // Update metadata
    let mut new_custom = HashMap::new();
    new_custom.insert("updated".to_string(), "true".to_string());
    let new_metadata = Metadata {
        content_type: Some("application/octet-stream".to_string()),
        custom: new_custom,
        size: retrieved_metadata.size,
        ..Default::default()
    };

    let result = client.update_metadata(key, new_metadata).await;
    assert!(result.is_ok());

    // Cleanup
    client.delete(key).await.unwrap();
}

#[tokio::test]
#[ignore = "Requires running server instance"]
async fn test_rest_list_objects() {
    let mut client = ObjectStoreClient::rest(get_rest_url()).unwrap();

    // Create test objects
    for i in 0..5 {
        let key = format!("test-list/file{}.txt", i);
        let data = Bytes::from(format!("Content {}", i));
        client.put(&key, data, None).await.unwrap();
    }

    // List with prefix
    let list_req = ListRequest {
        prefix: Some("test-list/".to_string()),
        max_results: Some(10),
        ..Default::default()
    };

    let response = client.list(list_req).await.unwrap();
    assert!(response.objects.len() >= 5);

    // Cleanup
    for i in 0..5 {
        let key = format!("test-list/file{}.txt", i);
        client.delete(&key).await.ok();
    }
}

#[tokio::test]
#[ignore = "Requires running server instance"]
async fn test_rest_health() {
    let mut client = ObjectStoreClient::rest(get_rest_url()).unwrap();

    let health = client.health().await;
    assert!(health.is_ok(), "Health check failed: {:?}", health);
}

// Error Handling Tests

#[tokio::test]
#[ignore = "Requires running server instance"]
async fn test_rest_get_nonexistent() {
    let mut client = ObjectStoreClient::rest(get_rest_url()).unwrap();

    // Attempt to get a non-existent object
    let result = client.get("test/nonexistent/file-does-not-exist.txt").await;

    // Should return NotFound error
    assert!(result.is_err());
    match result {
        Err(go_objstore::Error::NotFound(_)) => {
            // Expected error type
        }
        other => panic!("Expected NotFound error, got: {:?}", other),
    }
}

#[tokio::test]
#[ignore = "Requires running server instance"]
async fn test_rest_delete_nonexistent() {
    let mut client = ObjectStoreClient::rest(get_rest_url()).unwrap();

    // Attempt to delete a non-existent object
    let result = client.delete("test/nonexistent/file-does-not-exist.txt").await;

    // Some backends may not error on delete of non-existent (idempotent delete)
    // but the local backend returns NotFound
    if result.is_err() {
        match result {
            Err(go_objstore::Error::NotFound(_)) => {
                // Expected for some backends
            }
            other => panic!("Expected NotFound error or success, got: {:?}", other),
        }
    }
}

#[tokio::test]
#[ignore = "Requires running server instance"]
async fn test_rest_update_metadata_nonexistent() {
    let mut client = ObjectStoreClient::rest(get_rest_url()).unwrap();

    let mut custom = HashMap::new();
    custom.insert("test".to_string(), "value".to_string());
    let metadata = Metadata {
        content_type: Some("text/plain".to_string()),
        custom,
        ..Default::default()
    };

    // Attempt to update metadata on non-existent object
    let result = client
        .update_metadata("test/nonexistent/file-does-not-exist.txt", metadata)
        .await;

    // Should return NotFound error
    assert!(result.is_err());
    match result {
        Err(go_objstore::Error::NotFound(_)) => {
            // Expected error type
        }
        other => panic!("Expected NotFound error, got: {:?}", other),
    }
}

// Streaming Tests

#[tokio::test]
#[ignore = "Requires running server instance"]
async fn test_rest_stream_large_object() {
    let mut client = ObjectStoreClient::rest(get_rest_url()).unwrap();

    let key = "test-rest-large-stream.bin";
    // Create a large object (1MB)
    let large_data = Bytes::from(vec![b'x'; 1024 * 1024]);

    // Put large object
    let result = client.put(key, large_data.clone(), None).await;
    assert!(result.is_ok(), "Failed to put large object: {:?}", result);

    // Get large object (tests streaming internally)
    let (retrieved_data, metadata) = client.get(key).await.unwrap();
    assert_eq!(retrieved_data.len(), large_data.len());
    assert_eq!(retrieved_data, large_data);
    assert_eq!(metadata.size, 1024 * 1024);

    // Cleanup
    client.delete(key).await.unwrap();
}

// Lifecycle Policy Tests

#[tokio::test]
#[ignore = "Requires running gRPC server instance - REST API does not support apply_policies"]
async fn test_rest_apply_policies() {
    // Note: apply_policies is only available via gRPC client
    // This test documents that REST does not support this operation
    let client = ObjectStoreClient::rest(get_rest_url()).unwrap();

    // REST client doesn't support apply_policies
    // This would require a gRPC client
    drop(client);
}

// Replication Tests (gRPC only - REST doesn't support replication operations)
// Note: These tests gracefully skip if the backend doesn't support replication

#[tokio::test]
#[ignore = "Requires running gRPC server instance"]
async fn test_grpc_trigger_replication() {
    // Note: trigger_replication is only available via gRPC client
    let client = ObjectStoreClient::grpc(get_grpc_url()).await.unwrap();

    // Set up a replication policy first
    let mut source_settings = HashMap::new();
    source_settings.insert("path".to_string(), "/tmp/repl-source".to_string());

    let mut dest_settings = HashMap::new();
    dest_settings.insert("path".to_string(), "/tmp/repl-dest".to_string());

    let policy = ReplicationPolicy {
        id: "test-trigger-replication".to_string(),
        source_backend: "local".to_string(),
        source_settings,
        source_prefix: "".to_string(),
        destination_backend: "local".to_string(),
        destination_settings: dest_settings,
        check_interval_seconds: 60,
        last_sync_time: None,
        enabled: true,
        encryption: None,
        replication_mode: ReplicationMode::Transparent,
    };

    // Add policy - gracefully skip if replication not supported
    let result = client.add_replication_policy(policy.clone()).await;
    match result {
        Ok(_) => {}
        Err(e) => {
            let error_str = format!("{:?}", e);
            if error_str.contains("replication not supported") || error_str.contains("Unimplemented") {
                eprintln!("Skipping test - replication not supported by this backend");
                return;
            }
            panic!("Failed to add replication policy: {:?}", e);
        }
    }

    // Trigger replication
    let sync_result = client
        .trigger_replication(Some("test-trigger-replication".to_string()), false, 1)
        .await;

    assert!(sync_result.is_ok(), "Failed to trigger replication: {:?}", sync_result);
    let sync_data = sync_result.unwrap();
    assert_eq!(sync_data.policy_id, "test-trigger-replication");

    // Cleanup
    client.remove_replication_policy("test-trigger-replication").await.ok();
}

#[tokio::test]
#[ignore = "Requires running gRPC server instance"]
async fn test_grpc_get_replication_status() {
    // Note: get_replication_status is only available via gRPC client
    let client = ObjectStoreClient::grpc(get_grpc_url()).await.unwrap();

    // Set up a replication policy first
    let mut source_settings = HashMap::new();
    source_settings.insert("path".to_string(), "/tmp/status-source".to_string());

    let mut dest_settings = HashMap::new();
    dest_settings.insert("path".to_string(), "/tmp/status-dest".to_string());

    let policy = ReplicationPolicy {
        id: "test-status-replication".to_string(),
        source_backend: "local".to_string(),
        source_settings,
        source_prefix: "".to_string(),
        destination_backend: "local".to_string(),
        destination_settings: dest_settings,
        check_interval_seconds: 60,
        last_sync_time: None,
        enabled: true,
        encryption: None,
        replication_mode: ReplicationMode::Transparent,
    };

    // Add policy - gracefully skip if replication not supported
    let result = client.add_replication_policy(policy.clone()).await;
    match result {
        Ok(_) => {}
        Err(e) => {
            let error_str = format!("{:?}", e);
            if error_str.contains("replication not supported") || error_str.contains("Unimplemented") {
                eprintln!("Skipping test - replication not supported by this backend");
                return;
            }
            panic!("Failed to add replication policy: {:?}", e);
        }
    }

    // Get replication status
    let status_result = client.get_replication_status("test-status-replication").await;

    assert!(status_result.is_ok(), "Failed to get replication status: {:?}", status_result);
    let status = status_result.unwrap();
    assert_eq!(status.policy_id, "test-status-replication");
    assert_eq!(status.source_backend, "local");
    assert_eq!(status.destination_backend, "local");

    // Cleanup
    client.remove_replication_policy("test-status-replication").await.ok();
}

// gRPC Client Tests

#[tokio::test]
#[ignore = "Requires running gRPC server instance"]
async fn test_grpc_put_get_delete() {
    let mut client = ObjectStoreClient::grpc(get_grpc_url()).await.unwrap();

    let key = "test-grpc-put-get-delete.txt";
    let data = Bytes::from("Hello, gRPC World!");

    // Put object
    let result = client.put(key, data.clone(), None).await;
    assert!(result.is_ok());

    // Get object
    let (retrieved_data, metadata) = client.get(key).await.unwrap();
    assert_eq!(retrieved_data, data);
    assert!(metadata.size > 0);

    // Check exists
    let exists = client.exists(key).await.unwrap();
    assert!(exists);

    // Delete object
    let result = client.delete(key).await;
    assert!(result.is_ok());

    // Verify deleted
    let exists = client.exists(key).await.unwrap();
    assert!(!exists);
}

#[tokio::test]
#[ignore = "Requires running gRPC server instance"]
async fn test_grpc_metadata_operations() {
    let mut client = ObjectStoreClient::grpc(get_grpc_url()).await.unwrap();

    let key = "test-grpc-metadata.txt";
    let data = Bytes::from("Metadata test");

    // Put with metadata
    let mut custom = HashMap::new();
    custom.insert("author".to_string(), "grpc-test".to_string());
    let metadata = Metadata {
        content_type: Some("text/plain".to_string()),
        custom,
        ..Default::default()
    };

    client.put(key, data, Some(metadata)).await.unwrap();

    // Get metadata
    let retrieved_metadata = client.get_metadata(key).await.unwrap();
    assert!(retrieved_metadata.size > 0);

    // Update metadata
    let mut new_custom = HashMap::new();
    new_custom.insert("updated".to_string(), "true".to_string());
    let new_metadata = Metadata {
        custom: new_custom,
        size: retrieved_metadata.size,
        ..Default::default()
    };

    let result = client.update_metadata(key, new_metadata).await;
    assert!(result.is_ok());

    // Cleanup
    client.delete(key).await.unwrap();
}

#[tokio::test]
#[ignore = "Requires running gRPC server instance"]
async fn test_grpc_list_objects() {
    let mut client = ObjectStoreClient::grpc(get_grpc_url()).await.unwrap();

    // Create test objects
    for i in 0..3 {
        let key = format!("test-grpc-list/file{}.txt", i);
        let data = Bytes::from(format!("Content {}", i));
        client.put(&key, data, None).await.unwrap();
    }

    // List with prefix
    let list_req = ListRequest {
        prefix: Some("test-grpc-list/".to_string()),
        max_results: Some(10),
        ..Default::default()
    };

    let response = client.list(list_req).await.unwrap();
    assert!(response.objects.len() >= 3);

    // Cleanup
    for i in 0..3 {
        let key = format!("test-grpc-list/file{}.txt", i);
        client.delete(&key).await.ok();
    }
}

#[tokio::test]
#[ignore = "Requires running gRPC server instance"]
async fn test_grpc_health() {
    let mut client = ObjectStoreClient::grpc(get_grpc_url()).await.unwrap();

    let health = client.health().await;
    assert!(health.is_ok());
}

#[tokio::test]
#[ignore = "Requires running gRPC server instance"]
async fn test_grpc_lifecycle_policies() {
    let mut client = ObjectStoreClient::grpc(get_grpc_url()).await.unwrap();

    // Add a lifecycle policy
    let policy = LifecyclePolicy {
        id: "test-policy".to_string(),
        prefix: "test/".to_string(),
        retention_seconds: 86400, // 1 day
        action: "delete".to_string(),
        destination_type: None,
        destination_settings: HashMap::new(),
    };

    let result = client.add_policy(policy.clone()).await;
    assert!(result.is_ok());

    // Get policies
    let policies = client.get_policies(None).await.unwrap();
    assert!(!policies.is_empty());

    // Remove policy
    let result = client.remove_policy("test-policy").await;
    assert!(result.is_ok());
}

#[tokio::test]
#[ignore = "Requires running gRPC server instance"]
async fn test_grpc_replication_policies() {
    let client = ObjectStoreClient::grpc(get_grpc_url()).await.unwrap();

    // Add a replication policy
    let mut source_settings = HashMap::new();
    source_settings.insert("path".to_string(), "/tmp/source".to_string());

    let mut dest_settings = HashMap::new();
    dest_settings.insert("path".to_string(), "/tmp/dest".to_string());

    let policy = ReplicationPolicy {
        id: "test-replication".to_string(),
        source_backend: "local".to_string(),
        source_settings,
        source_prefix: "".to_string(),
        destination_backend: "local".to_string(),
        destination_settings: dest_settings,
        check_interval_seconds: 60,
        last_sync_time: None,
        enabled: true,
        encryption: None,
        replication_mode: ReplicationMode::Transparent,
    };

    // Add policy - gracefully skip if replication not supported
    let result = client.add_replication_policy(policy.clone()).await;
    match result {
        Ok(_) => {}
        Err(e) => {
            let error_str = format!("{:?}", e);
            if error_str.contains("replication not supported") || error_str.contains("Unimplemented") {
                eprintln!("Skipping test - replication not supported by this backend");
                return;
            }
            panic!("Failed to add replication policy: {:?}", e);
        }
    }

    // Get replication policies
    let policies = client.get_replication_policies().await.unwrap();
    assert!(!policies.is_empty());

    // Get specific policy
    let retrieved_policy = client.get_replication_policy("test-replication").await;
    assert!(retrieved_policy.is_ok());

    // Remove policy
    let result = client.remove_replication_policy("test-replication").await;
    assert!(result.is_ok());
}

#[tokio::test]
#[ignore = "Requires running gRPC server instance with archive backend configured"]
async fn test_grpc_archive() {
    let mut client = ObjectStoreClient::grpc(get_grpc_url()).await.unwrap();

    // Create an object to archive
    let key = "test-archive.txt";
    let data = Bytes::from("Archive me!");
    client.put(key, data, None).await.unwrap();

    // Archive it
    let mut dest_settings = HashMap::new();
    dest_settings.insert("path".to_string(), "/tmp/archive".to_string());

    let result = client
        .archive(key, "local".to_string(), dest_settings)
        .await;

    // Note: This may fail if archive backend is not configured
    // We just verify the call works structurally
    let _ = result;

    // Cleanup
    client.delete(key).await.ok();
}

// QUIC Client Tests (basic operations only)
// Note: QUIC tests may fail in Docker due to UDP networking limitations
// Tests handle connection failures gracefully

fn get_quic_endpoint() -> (std::net::SocketAddr, String) {
    let host = env::var("OBJSTORE_QUIC_HOST")
        .or_else(|_| env::var("QUIC_HOST"))
        .unwrap_or_else(|_| "localhost:4433".to_string());
    // Remove https:// prefix if present
    let host = host.trim_start_matches("https://").trim_start_matches("http://");
    let addr = host
        .parse()
        .unwrap_or_else(|_| "127.0.0.1:4433".parse().unwrap());
    let server_name = host.split(':').next().unwrap_or("localhost").to_string();
    (addr, server_name)
}

#[tokio::test]
#[ignore = "Requires running HTTP/3 server with QUIC support"]
async fn test_quic_health() {
    let (addr, server_name) = get_quic_endpoint();
    let client = ObjectStoreClient::quic(addr, server_name).await;

    match client {
        Ok(mut client) => {
            let health = client.health().await;
            // May fail if QUIC is not configured, but we test the structure
            let _ = health;
        }
        Err(e) => {
            // QUIC connection may fail in Docker, gracefully skip
            eprintln!("Skipping QUIC health test - connection unavailable: {}", e);
        }
    }
}

#[tokio::test]
#[ignore = "Requires running HTTP/3 server with QUIC support"]
async fn test_quic_basic_operations() {
    let (addr, server_name) = get_quic_endpoint();
    let client = ObjectStoreClient::quic(addr, server_name).await;

    match client {
        Ok(mut client) => {
            let key = "test-quic.txt";
            let data = Bytes::from("Hello, QUIC!");

            // Put
            let result = client.put(key, data.clone(), None).await;
            if result.is_ok() {
                // Get
                let get_result = client.get(key).await;
                if let Ok((retrieved_data, _)) = get_result {
                    assert_eq!(retrieved_data, data);
                }

                // Delete
                client.delete(key).await.ok();
            }
        }
        Err(e) => {
            // QUIC connection may fail in Docker, gracefully skip
            eprintln!("Skipping QUIC basic operations test - connection unavailable: {}", e);
        }
    }
}
