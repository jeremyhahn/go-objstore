//! Unit tests for the MCP and Unix clients, and unified-client constructor
//! coverage for the new Mcp and Unix variants.
//!
//! These tests run without a live server: MCP uses a mockito HTTP server,
//! Unix uses an in-process listener thread.

use base64::Engine as _;
use bytes::Bytes;
use go_objstore::{
    AuthConfig, Error, HealthStatus, LifecyclePolicy, ListRequest, McpClient, Metadata,
    ObjectStore, ObjectStoreClient, ReplicationMode, ReplicationPolicy, UnixClient,
};
use std::collections::HashMap;

// ── helpers ───────────────────────────────────────────────────────────────────

fn mcp_ok(inner: serde_json::Value) -> String {
    let text = serde_json::to_string(&inner).unwrap();
    serde_json::json!({
        "jsonrpc": "2.0",
        "result": {
            "content": [{ "type": "text", "text": text }]
        },
        "id": 1
    })
    .to_string()
}

fn sample_lifecycle_policy() -> LifecyclePolicy {
    LifecyclePolicy {
        id: "lc1".to_string(),
        prefix: "logs/".to_string(),
        retention_seconds: 86400,
        action: "delete".to_string(),
        destination_type: None,
        destination_settings: HashMap::new(),
    }
}

fn sample_replication_policy() -> ReplicationPolicy {
    ReplicationPolicy {
        id: "r1".to_string(),
        source_backend: "s3".to_string(),
        source_settings: HashMap::new(),
        source_prefix: String::new(),
        destination_backend: "gcs".to_string(),
        destination_settings: HashMap::new(),
        check_interval_seconds: 300,
        last_sync_time: None,
        enabled: true,
        encryption: None,
        replication_mode: ReplicationMode::Opaque,
    }
}

// ── ObjectStoreClient constructor tests ───────────────────────────────────────

#[test]
fn unified_mcp_constructor_ok() {
    let client = ObjectStoreClient::mcp("http://localhost:8080");
    assert!(client.is_ok());
    assert!(matches!(client.unwrap(), ObjectStoreClient::Mcp(_)));
}

#[test]
fn unified_mcp_with_auth_constructor_ok() {
    let auth = AuthConfig {
        token: Some("tok".to_string()),
        tenant_id: Some("t1".to_string()),
        ..Default::default()
    };
    let client = ObjectStoreClient::mcp_with_auth("http://localhost:8080", auth);
    assert!(client.is_ok());
    assert!(matches!(client.unwrap(), ObjectStoreClient::Mcp(_)));
}

#[test]
fn unified_unix_constructor_ok() {
    let client = ObjectStoreClient::unix("/tmp/test.sock");
    assert!(client.is_ok());
    assert!(matches!(client.unwrap(), ObjectStoreClient::Unix(_)));
}

// ── MCP delegation through ObjectStoreClient ─────────────────────────────────

#[tokio::test]
async fn unified_mcp_health_delegation() {
    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("POST", "/")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(mcp_ok(
            serde_json::json!({ "status": "healthy", "version": "2.0" }),
        ))
        .create_async()
        .await;

    let client = ObjectStoreClient::mcp(server.url()).unwrap();
    let h = client.health().await.unwrap();
    assert_eq!(h.status, HealthStatus::Serving);
}

#[tokio::test]
async fn unified_mcp_put_delegation() {
    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("POST", "/")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(mcp_ok(serde_json::json!({ "success": true })))
        .create_async()
        .await;

    let client = ObjectStoreClient::mcp(server.url()).unwrap();
    let r = client
        .put("k", Bytes::from_static(b"v"), None)
        .await
        .unwrap();
    assert!(r.success);
}

#[tokio::test]
async fn unified_mcp_get_delegation() {
    let encoded = base64::engine::general_purpose::STANDARD.encode(b"payload");
    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("POST", "/")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(mcp_ok(
            serde_json::json!({ "success": true, "data": encoded }),
        ))
        .create_async()
        .await;

    let client = ObjectStoreClient::mcp(server.url()).unwrap();
    let (data, _) = client.get("k").await.unwrap();
    assert_eq!(&data[..], b"payload");
}

#[tokio::test]
async fn unified_mcp_delete_delegation() {
    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("POST", "/")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(mcp_ok(serde_json::json!({ "success": true })))
        .create_async()
        .await;

    let client = ObjectStoreClient::mcp(server.url()).unwrap();
    let r = client.delete("k").await.unwrap();
    assert!(r.success);
}

#[tokio::test]
async fn unified_mcp_list_delegation() {
    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("POST", "/")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(mcp_ok(serde_json::json!({
            "success": true, "keys": ["a", "b"], "truncated": false, "next_token": ""
        })))
        .create_async()
        .await;

    let client = ObjectStoreClient::mcp(server.url()).unwrap();
    let r = client.list(ListRequest::default()).await.unwrap();
    assert_eq!(r.objects.len(), 2);
}

#[tokio::test]
async fn unified_mcp_exists_delegation() {
    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("POST", "/")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(mcp_ok(
            serde_json::json!({ "success": true, "exists": true }),
        ))
        .create_async()
        .await;

    let client = ObjectStoreClient::mcp(server.url()).unwrap();
    assert!(client.exists("k").await.unwrap());
}

#[tokio::test]
async fn unified_mcp_get_metadata_delegation() {
    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("POST", "/")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(mcp_ok(serde_json::json!({
            "success": true, "size": 10, "content_type": "text/plain", "custom": {}
        })))
        .create_async()
        .await;

    let client = ObjectStoreClient::mcp(server.url()).unwrap();
    let meta = client.get_metadata("k").await.unwrap();
    assert_eq!(meta.size, 10);
}

#[tokio::test]
async fn unified_mcp_update_metadata_delegation() {
    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("POST", "/")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(mcp_ok(serde_json::json!({ "success": true })))
        .create_async()
        .await;

    let client = ObjectStoreClient::mcp(server.url()).unwrap();
    client
        .update_metadata("k", Metadata::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn unified_mcp_archive_delegation() {
    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("POST", "/")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(mcp_ok(serde_json::json!({ "success": true })))
        .create_async()
        .await;

    let client = ObjectStoreClient::mcp(server.url()).unwrap();
    client
        .archive("k", "glacier".to_string(), HashMap::new())
        .await
        .unwrap();
}

#[tokio::test]
async fn unified_mcp_policy_ops_delegation() {
    let mut server = mockito::Server::new_async().await;
    // Respond to all POST / calls with success.
    let _m = server
        .mock("POST", "/")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(mcp_ok(serde_json::json!({
            "success": true, "policies": [], "count": 0,
            "policies_count": 0, "objects_processed": 0
        })))
        .expect_at_least(1)
        .create_async()
        .await;

    let client = ObjectStoreClient::mcp(server.url()).unwrap();
    client.add_policy(sample_lifecycle_policy()).await.unwrap();
    client.remove_policy("lc1").await.unwrap();
    assert!(client.get_policies(None).await.unwrap().is_empty());
    assert_eq!(client.apply_policies().await.unwrap(), (0, 0));
}

#[tokio::test]
async fn unified_mcp_replication_ops_delegation() {
    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("POST", "/")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(mcp_ok(serde_json::json!({
            "success": true,
            "policies": [],
            "id": "r1",
            "source_backend": "s3",
            "destination_backend": "gcs",
            "check_interval": 300,
            "enabled": true,
            "replication_mode": "opaque",
            "result": {
                "policy_id": "r1", "synced": 1, "deleted": 0,
                "failed": 0, "bytes_total": 0, "duration": "0s", "errors": []
            },
            "policy_id": "r1",
            "total_objects_synced": 0, "total_objects_deleted": 0,
            "total_bytes_synced": 0, "total_errors": 0, "sync_count": 0,
            "source_backend": "s3", "destination_backend": "gcs", "enabled": true
        })))
        .expect_at_least(1)
        .create_async()
        .await;

    let client = ObjectStoreClient::mcp(server.url()).unwrap();
    client
        .add_replication_policy(sample_replication_policy())
        .await
        .unwrap();
    client.remove_replication_policy("r1").await.unwrap();
    client.get_replication_policies().await.unwrap();
    client.get_replication_policy("r1").await.unwrap();
    client.trigger_replication(None, false, 1).await.unwrap();
    client.get_replication_status("r1").await.unwrap();
    client.close().await.unwrap();
}

// ── Unix delegation through ObjectStoreClient ─────────────────────────────────

/// Spawn a minimal Unix-socket server that always returns the given response.
fn spawn_unix_server(response: &'static str) -> std::path::PathBuf {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("srv.sock");
    let listener = std::os::unix::net::UnixListener::bind(&path).expect("bind");
    let p = path.clone();
    std::thread::spawn(move || {
        let _dir = dir;
        loop {
            use std::io::{Read, Write};
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut buf = [0u8; 4096];
                    loop {
                        let n = stream.read(&mut buf).unwrap_or(0);
                        if n == 0 {
                            break;
                        }
                        let s = String::from_utf8_lossy(&buf[..n]);
                        if s.contains('\n') {
                            break;
                        }
                    }
                    let mut resp = response.to_string();
                    resp.push('\n');
                    stream.write_all(resp.as_bytes()).ok();
                }
                Err(_) => break,
            }
        }
    });
    p
}

fn unix_ok(v: serde_json::Value) -> &'static str {
    let s = serde_json::json!({ "jsonrpc":"2.0","result":v,"id":1 }).to_string();
    Box::leak(s.into_boxed_str())
}

#[tokio::test]
async fn unified_unix_health_delegation() {
    let path = spawn_unix_server(unix_ok(serde_json::json!({
        "status": "healthy", "version": "1.0"
    })));
    let client = ObjectStoreClient::unix(&path).unwrap();
    let h = client.health().await.unwrap();
    assert_eq!(h.status, HealthStatus::Serving);
}

#[tokio::test]
async fn unified_unix_put_delegation() {
    let path = spawn_unix_server(unix_ok(serde_json::json!({ "success": true })));
    let client = ObjectStoreClient::unix(&path).unwrap();
    let r = client
        .put("k", Bytes::from_static(b"v"), None)
        .await
        .unwrap();
    assert!(r.success);
}

#[tokio::test]
async fn unified_unix_delete_delegation() {
    let path = spawn_unix_server(unix_ok(serde_json::json!({ "success": true })));
    let client = ObjectStoreClient::unix(&path).unwrap();
    let r = client.delete("k").await.unwrap();
    assert!(r.success);
}

#[tokio::test]
async fn unified_unix_exists_delegation() {
    let path = spawn_unix_server(unix_ok(serde_json::json!({ "exists": true })));
    let client = ObjectStoreClient::unix(&path).unwrap();
    assert!(client.exists("k").await.unwrap());
}

#[tokio::test]
async fn unified_unix_list_delegation() {
    let path = spawn_unix_server(unix_ok(serde_json::json!({
        "objects": [{ "key": "a", "size": 1, "last_modified": null }],
        "next_cursor": "",
        "is_truncated": false
    })));
    let client = ObjectStoreClient::unix(&path).unwrap();
    let r = client.list(ListRequest::default()).await.unwrap();
    assert_eq!(r.objects.len(), 1);
}

#[tokio::test]
async fn unified_unix_close() {
    let client = ObjectStoreClient::unix("/tmp/no-such.sock").unwrap();
    client.close().await.unwrap();
}

// ── McpClient: usable as trait object ────────────────────────────────────────

#[test]
fn mcp_client_as_trait_object() {
    let _mcp_direct = McpClient::new("http://localhost:8080").unwrap();
    // Wrap in ObjectStoreClient so it goes through the trait impl.
    let unified = ObjectStoreClient::mcp("http://localhost:8080").unwrap();
    let _boxed: Box<dyn ObjectStore> = Box::new(unified);
}

// ── UnixClient: connect fails gracefully on missing socket ───────────────────

#[tokio::test]
async fn unix_connect_error_is_io() {
    let client = UnixClient::new("/tmp/definitely-does-not-exist-xyz123.sock").unwrap();
    let err = client.health().await.unwrap_err();
    assert!(matches!(err, Error::Io(_)));
}
