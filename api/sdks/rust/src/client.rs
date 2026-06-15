use crate::auth::AuthConfig;
use crate::error::Result;
use crate::grpc_client::GrpcClient;
use crate::mcp_client::McpClient;
use crate::quic_client::QuicClient;
use crate::rest_client::RestClient;
use crate::types::*;
use crate::unix_client::UnixClient;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;

/// Trait for object store operations
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Put an object into storage
    async fn put(&self, key: &str, data: Bytes, metadata: Option<Metadata>) -> Result<PutResponse>;

    /// Get an object from storage
    async fn get(&self, key: &str) -> Result<(Bytes, Metadata)>;

    /// Delete an object from storage
    async fn delete(&self, key: &str) -> Result<DeleteResponse>;

    /// List objects with optional prefix filtering
    async fn list(&self, list_req: ListRequest) -> Result<ListResponse>;

    /// Check if an object exists
    async fn exists(&self, key: &str) -> Result<bool>;

    /// Get metadata for an object
    async fn get_metadata(&self, key: &str) -> Result<Metadata>;

    /// Update metadata for an object
    async fn update_metadata(&self, key: &str, metadata: Metadata) -> Result<()>;

    /// Health check
    async fn health(&self) -> Result<HealthResponse>;
}

/// Unified client that supports multiple protocols
pub enum ObjectStoreClient {
    Rest(RestClient),
    Grpc(GrpcClient),
    Quic(QuicClient),
    Mcp(McpClient),
    Unix(UnixClient),
}

impl ObjectStoreClient {
    /// Create a new REST client
    pub fn rest(base_url: impl Into<String>) -> Result<Self> {
        Ok(ObjectStoreClient::Rest(RestClient::new(base_url)?))
    }

    /// Create a new gRPC client
    pub async fn grpc(endpoint: impl Into<String>) -> Result<Self> {
        Ok(ObjectStoreClient::Grpc(GrpcClient::new(endpoint).await?))
    }

    /// Create a new QUIC/HTTP3 client
    pub async fn quic(
        server_addr: std::net::SocketAddr,
        server_name: impl Into<String>,
    ) -> Result<Self> {
        Ok(ObjectStoreClient::Quic(
            QuicClient::new(server_addr, server_name).await?,
        ))
    }

    /// Create a new MCP (HTTP JSON-RPC 2.0) client
    pub fn mcp(base_url: impl Into<String>) -> Result<Self> {
        Ok(ObjectStoreClient::Mcp(McpClient::new(base_url)?))
    }

    /// Create a new MCP client with authentication configuration
    pub fn mcp_with_auth(base_url: impl Into<String>, auth: AuthConfig) -> Result<Self> {
        Ok(ObjectStoreClient::Mcp(McpClient::new_with_auth(
            base_url, auth,
        )?))
    }

    /// Create a new Unix-socket (JSON-RPC 2.0) client
    pub fn unix(socket_path: impl AsRef<std::path::Path>) -> Result<Self> {
        Ok(ObjectStoreClient::Unix(UnixClient::new(socket_path)?))
    }
}

#[async_trait]
impl ObjectStore for ObjectStoreClient {
    async fn put(&self, key: &str, data: Bytes, metadata: Option<Metadata>) -> Result<PutResponse> {
        match self {
            ObjectStoreClient::Rest(client) => client.put(key, data, metadata).await,
            ObjectStoreClient::Grpc(client) => client.put(key.to_string(), data, metadata).await,
            ObjectStoreClient::Quic(client) => client.put(key, data, metadata).await,
            ObjectStoreClient::Mcp(client) => client.put(key, data, metadata).await,
            ObjectStoreClient::Unix(client) => client.put(key, data, metadata).await,
        }
    }

    async fn get(&self, key: &str) -> Result<(Bytes, Metadata)> {
        match self {
            ObjectStoreClient::Rest(client) => client.get(key).await,
            ObjectStoreClient::Grpc(client) => client.get(key.to_string()).await,
            ObjectStoreClient::Quic(client) => client.get(key).await,
            ObjectStoreClient::Mcp(client) => client.get(key).await,
            ObjectStoreClient::Unix(client) => client.get(key).await,
        }
    }

    async fn delete(&self, key: &str) -> Result<DeleteResponse> {
        match self {
            ObjectStoreClient::Rest(client) => client.delete(key).await,
            ObjectStoreClient::Grpc(client) => client.delete(key.to_string()).await,
            ObjectStoreClient::Quic(client) => client.delete(key).await,
            ObjectStoreClient::Mcp(client) => client.delete(key).await,
            ObjectStoreClient::Unix(client) => client.delete(key).await,
        }
    }

    async fn list(&self, list_req: ListRequest) -> Result<ListResponse> {
        match self {
            ObjectStoreClient::Rest(client) => client.list(list_req).await,
            ObjectStoreClient::Grpc(client) => client.list(list_req).await,
            ObjectStoreClient::Quic(client) => client.list(list_req).await,
            ObjectStoreClient::Mcp(client) => client.list(list_req).await,
            ObjectStoreClient::Unix(client) => client.list(list_req).await,
        }
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        match self {
            ObjectStoreClient::Rest(client) => client.exists(key).await,
            ObjectStoreClient::Grpc(client) => client.exists(key.to_string()).await,
            ObjectStoreClient::Quic(client) => client.exists(key).await,
            ObjectStoreClient::Mcp(client) => client.exists(key).await,
            ObjectStoreClient::Unix(client) => client.exists(key).await,
        }
    }

    async fn get_metadata(&self, key: &str) -> Result<Metadata> {
        match self {
            ObjectStoreClient::Rest(client) => client.get_metadata(key).await,
            ObjectStoreClient::Grpc(client) => client.get_metadata(key.to_string()).await,
            ObjectStoreClient::Quic(client) => client.get_metadata(key).await,
            ObjectStoreClient::Mcp(client) => client.get_metadata(key).await,
            ObjectStoreClient::Unix(client) => client.get_metadata(key).await,
        }
    }

    async fn update_metadata(&self, key: &str, metadata: Metadata) -> Result<()> {
        match self {
            ObjectStoreClient::Rest(client) => client.update_metadata(key, metadata).await,
            ObjectStoreClient::Grpc(client) => {
                client.update_metadata(key.to_string(), metadata).await
            }
            ObjectStoreClient::Quic(client) => client.update_metadata(key, metadata).await,
            ObjectStoreClient::Mcp(client) => client.update_metadata(key, metadata).await,
            ObjectStoreClient::Unix(client) => client.update_metadata(key, metadata).await,
        }
    }

    async fn health(&self) -> Result<HealthResponse> {
        match self {
            ObjectStoreClient::Rest(client) => client.health().await,
            ObjectStoreClient::Grpc(client) => client.health(None).await,
            ObjectStoreClient::Quic(client) => client.health().await,
            ObjectStoreClient::Mcp(client) => client.health().await,
            ObjectStoreClient::Unix(client) => client.health().await,
        }
    }
}

/// Extended operations available on all transports (REST, gRPC, QUIC).
impl ObjectStoreClient {
    /// Archive an object to a different storage backend
    pub async fn archive(
        &self,
        key: &str,
        destination_type: String,
        destination_settings: HashMap<String, String>,
    ) -> Result<()> {
        match self {
            ObjectStoreClient::Rest(client) => {
                client
                    .archive(key, destination_type, destination_settings)
                    .await
            }
            ObjectStoreClient::Grpc(client) => {
                client
                    .archive(key.to_string(), destination_type, destination_settings)
                    .await
            }
            ObjectStoreClient::Quic(client) => {
                client
                    .archive(key, destination_type, destination_settings)
                    .await
            }
            ObjectStoreClient::Mcp(client) => {
                client
                    .archive(key, destination_type, destination_settings)
                    .await
            }
            ObjectStoreClient::Unix(client) => {
                client
                    .archive(key, destination_type, destination_settings)
                    .await
            }
        }
    }

    /// Add a lifecycle policy
    pub async fn add_policy(&self, policy: LifecyclePolicy) -> Result<()> {
        match self {
            ObjectStoreClient::Rest(client) => client.add_policy(policy).await,
            ObjectStoreClient::Grpc(client) => client.add_policy(policy).await,
            ObjectStoreClient::Quic(client) => client.add_policy(policy).await,
            ObjectStoreClient::Mcp(client) => client.add_policy(policy).await,
            ObjectStoreClient::Unix(client) => client.add_policy(policy).await,
        }
    }

    /// Remove a lifecycle policy
    pub async fn remove_policy(&self, id: &str) -> Result<()> {
        match self {
            ObjectStoreClient::Rest(client) => client.remove_policy(id).await,
            ObjectStoreClient::Grpc(client) => client.remove_policy(id.to_string()).await,
            ObjectStoreClient::Quic(client) => client.remove_policy(id).await,
            ObjectStoreClient::Mcp(client) => client.remove_policy(id).await,
            ObjectStoreClient::Unix(client) => client.remove_policy(id).await,
        }
    }

    /// Get all lifecycle policies, optionally filtered by prefix
    pub async fn get_policies(&self, prefix: Option<String>) -> Result<Vec<LifecyclePolicy>> {
        match self {
            ObjectStoreClient::Rest(client) => client.get_policies(prefix).await,
            ObjectStoreClient::Grpc(client) => client.get_policies(prefix).await,
            ObjectStoreClient::Quic(client) => client.get_policies(prefix).await,
            ObjectStoreClient::Mcp(client) => client.get_policies(prefix).await,
            ObjectStoreClient::Unix(client) => client.get_policies(prefix).await,
        }
    }

    /// Apply all lifecycle policies, returning (policies_count, objects_processed)
    pub async fn apply_policies(&self) -> Result<(i32, i32)> {
        match self {
            ObjectStoreClient::Rest(client) => client.apply_policies().await,
            ObjectStoreClient::Grpc(client) => client.apply_policies().await,
            ObjectStoreClient::Quic(client) => client.apply_policies().await,
            ObjectStoreClient::Mcp(client) => client.apply_policies().await,
            ObjectStoreClient::Unix(client) => client.apply_policies().await,
        }
    }

    /// Add a replication policy
    pub async fn add_replication_policy(&self, policy: ReplicationPolicy) -> Result<()> {
        match self {
            ObjectStoreClient::Rest(client) => client.add_replication_policy(policy).await,
            ObjectStoreClient::Grpc(client) => client.add_replication_policy(policy).await,
            ObjectStoreClient::Quic(client) => client.add_replication_policy(policy).await,
            ObjectStoreClient::Mcp(client) => client.add_replication_policy(policy).await,
            ObjectStoreClient::Unix(client) => client.add_replication_policy(policy).await,
        }
    }

    /// Remove a replication policy
    pub async fn remove_replication_policy(&self, id: &str) -> Result<()> {
        match self {
            ObjectStoreClient::Rest(client) => client.remove_replication_policy(id).await,
            ObjectStoreClient::Grpc(client) => {
                client.remove_replication_policy(id.to_string()).await
            }
            ObjectStoreClient::Quic(client) => client.remove_replication_policy(id).await,
            ObjectStoreClient::Mcp(client) => client.remove_replication_policy(id).await,
            ObjectStoreClient::Unix(client) => client.remove_replication_policy(id).await,
        }
    }

    /// Get all replication policies
    pub async fn get_replication_policies(&self) -> Result<Vec<ReplicationPolicy>> {
        match self {
            ObjectStoreClient::Rest(client) => client.get_replication_policies().await,
            ObjectStoreClient::Grpc(client) => client.get_replication_policies().await,
            ObjectStoreClient::Quic(client) => client.get_replication_policies().await,
            ObjectStoreClient::Mcp(client) => client.get_replication_policies().await,
            ObjectStoreClient::Unix(client) => client.get_replication_policies().await,
        }
    }

    /// Get a specific replication policy
    pub async fn get_replication_policy(&self, id: &str) -> Result<ReplicationPolicy> {
        match self {
            ObjectStoreClient::Rest(client) => client.get_replication_policy(id).await,
            ObjectStoreClient::Grpc(client) => client.get_replication_policy(id.to_string()).await,
            ObjectStoreClient::Quic(client) => client.get_replication_policy(id).await,
            ObjectStoreClient::Mcp(client) => client.get_replication_policy(id).await,
            ObjectStoreClient::Unix(client) => client.get_replication_policy(id).await,
        }
    }

    /// Trigger replication synchronization
    pub async fn trigger_replication(
        &self,
        policy_id: Option<String>,
        parallel: bool,
        worker_count: i32,
    ) -> Result<SyncResult> {
        match self {
            ObjectStoreClient::Rest(client) => {
                client
                    .trigger_replication(policy_id, parallel, worker_count)
                    .await
            }
            ObjectStoreClient::Grpc(client) => {
                client
                    .trigger_replication(policy_id, parallel, worker_count)
                    .await
            }
            ObjectStoreClient::Quic(client) => {
                client
                    .trigger_replication(policy_id, parallel, worker_count)
                    .await
            }
            ObjectStoreClient::Mcp(client) => {
                client
                    .trigger_replication(policy_id, parallel, worker_count)
                    .await
            }
            ObjectStoreClient::Unix(client) => {
                client
                    .trigger_replication(policy_id, parallel, worker_count)
                    .await
            }
        }
    }

    /// Get replication status for a policy
    pub async fn get_replication_status(&self, id: &str) -> Result<ReplicationStatus> {
        match self {
            ObjectStoreClient::Rest(client) => client.get_replication_status(id).await,
            ObjectStoreClient::Grpc(client) => client.get_replication_status(id.to_string()).await,
            ObjectStoreClient::Quic(client) => client.get_replication_status(id).await,
            ObjectStoreClient::Mcp(client) => client.get_replication_status(id).await,
            ObjectStoreClient::Unix(client) => client.get_replication_status(id).await,
        }
    }

    /// Close the client, releasing any underlying resources.
    pub async fn close(&self) -> Result<()> {
        match self {
            ObjectStoreClient::Rest(client) => client.close().await,
            ObjectStoreClient::Grpc(client) => client.close().await,
            ObjectStoreClient::Quic(client) => client.close().await,
            ObjectStoreClient::Mcp(client) => client.close().await,
            ObjectStoreClient::Unix(client) => client.close().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use mockito::Server;

    // =========================================================================
    // Unified client (ObjectStoreClient) tests.
    //
    // The unified client is a thin enum that delegates each call to the
    // selected protocol's client. The REST arm is driven end-to-end against a
    // `mockito` HTTP server, which covers the bulk of the delegating match arms
    // in this file. The gRPC and QUIC arms are proven structurally (variant
    // selection + that a call routes into that protocol's client), since
    // end-to-end exercise of those arms needs a live gRPC/QUIC server.
    // =========================================================================

    fn rest(url: String) -> ObjectStoreClient {
        ObjectStoreClient::rest(url).unwrap()
    }

    fn sample_replication_policy() -> ReplicationPolicy {
        ReplicationPolicy {
            id: "repl-1".to_string(),
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

    // ---- delegation: REST (end-to-end via mockito) ----

    #[tokio::test]
    async fn unified_delegates_rest() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/health")
            .with_status(200)
            .with_body(r#"{"status":"healthy","version":"1.0.0"}"#)
            .create_async()
            .await;

        let client = rest(server.url());
        assert!(matches!(client, ObjectStoreClient::Rest(_)));
        let health = client.health().await.unwrap();
        mock.assert_async().await;
        assert_eq!(health.status, HealthStatus::Serving);
    }

    // ---- delegation: gRPC (structural) ----

    #[tokio::test]
    async fn unified_delegates_grpc() {
        // A live gRPC server is unavailable in unit tests, so delegation is
        // proven structurally: a connection-refused endpoint fails fast and the
        // error originates from the gRPC arm, demonstrating the call routed into
        // the Grpc client rather than REST.
        let result = ObjectStoreClient::grpc("http://127.0.0.1:1").await;
        match result {
            Err(Error::GrpcTransport(_)) | Err(Error::Configuration(_)) => {}
            Err(other) => panic!("unexpected gRPC error variant: {other:?}"),
            Ok(_) => panic!("expected connection to 127.0.0.1:1 to fail"),
        }
    }

    /// Stand up a bare TCP listener that accepts connections but never speaks
    /// gRPC. `GrpcClient::new` performs only a TCP-level connect, so this lets
    /// us construct a real `ObjectStoreClient::Grpc(_)` (covering the `grpc`
    /// constructor) and route calls through every gRPC delegation match arm
    /// (each RPC then fails, which is expected -- we only assert it routed).
    async fn grpc_over_dead_tcp() -> Option<ObjectStoreClient> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.ok()?;
        let addr = listener.local_addr().ok()?;
        // Accept-and-drop loop so connects succeed at the TCP layer.
        tokio::spawn(async move {
            loop {
                if listener.accept().await.is_err() {
                    break;
                }
            }
        });
        ObjectStoreClient::grpc(format!("http://{addr}")).await.ok()
    }

    // Ignored in the unit suite: spawns a background accept-loop and routes
    // gRPC calls that leak a non-terminating task, which hangs process exit.
    // Delegation is covered by `unified_delegates_grpc` (fast-fail routing) and
    // the `grpc_client` unit suite; this runs under the integration suite.
    #[ignore = "leaks background task; covered by integration tests"]
    #[tokio::test]
    async fn unified_grpc_constructor_and_arms() {
        // Cover the `grpc` constructor success path and every gRPC delegation
        // arm. If construction does not succeed in this environment we skip
        // rather than fail (the structural unified_delegates_grpc still asserts
        // routing).
        let client = match grpc_over_dead_tcp().await {
            Some(c) => c,
            None => return,
        };
        assert!(matches!(client, ObjectStoreClient::Grpc(_)));

        let _ = client.put("k", Bytes::from_static(b"d"), None).await;
        let _ = client.get("k").await;
        let _ = client.delete("k").await;
        let _ = client.list(ListRequest::default()).await;
        let _ = client.exists("k").await;
        let _ = client.get_metadata("k").await;
        let _ = client.update_metadata("k", Metadata::default()).await;
        let _ = client.health().await;
        let _ = client
            .archive("k", "glacier".to_string(), HashMap::new())
            .await;
        let _ = client
            .add_policy(LifecyclePolicy {
                id: "p1".to_string(),
                prefix: String::new(),
                retention_seconds: 1,
                action: "delete".to_string(),
                destination_type: None,
                destination_settings: HashMap::new(),
            })
            .await;
        let _ = client.remove_policy("p1").await;
        let _ = client.get_policies(None).await;
        let _ = client.apply_policies().await;
        let _ = client
            .add_replication_policy(sample_replication_policy())
            .await;
        let _ = client.remove_replication_policy("repl-1").await;
        let _ = client.get_replication_policies().await;
        let _ = client.get_replication_policy("repl-1").await;
        let _ = client.trigger_replication(None, false, 1).await;
        let _ = client.get_replication_status("repl-1").await;
        let _ = client.close().await;
    }

    // ---- delegation: QUIC (structural) ----

    // Ignored in the unit suite: constructs a real QUIC (quinn) endpoint whose
    // background driver does not terminate, hanging process exit. QUIC delegation
    // is covered by the `quic_client` unit suite; this runs under integration.
    #[ignore = "leaks QUIC endpoint driver; covered by integration tests"]
    #[tokio::test]
    async fn unified_delegates_quic() {
        // Build a unified client over the QUIC arm pointed at an unused local
        // address. Construction succeeds (the endpoint binds locally); a call
        // then routes into the Quic client and fails at connect time, proving
        // the unified client delegates to the QUIC arm.
        let addr: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
        let client = ObjectStoreClient::quic(addr, "localhost").await.unwrap();
        assert!(matches!(client, ObjectStoreClient::Quic(_)));
        let err = client.health().await.unwrap_err();
        assert!(matches!(
            err,
            Error::QuicConnection(_)
                | Error::H3(_)
                | Error::Configuration(_)
                | Error::OperationFailed(_)
        ));
    }

    // ---- close ----

    #[tokio::test]
    async fn unified_close() {
        // REST close is a safe no-op and is safe to call more than once.
        let client = rest("http://localhost:9".to_string());
        client.close().await.unwrap();
        client.close().await.unwrap();
    }

    // Ignored in the unit suite: constructs a real QUIC (quinn) endpoint. Even
    // after close() the background driver can keep the process from exiting,
    // hanging the test runner. QUIC close is exercised by the `quic_client` unit
    // suite (quic_close) and the integration suite.
    #[ignore = "constructs QUIC endpoint; covered by integration tests"]
    #[tokio::test]
    async fn unified_close_quic() {
        // QUIC close releases the endpoint without error.
        let addr: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
        let quic = ObjectStoreClient::quic(addr, "localhost").await.unwrap();
        quic.close().await.unwrap();
    }

    // ---- per-op REST-arm delegation coverage ----

    #[tokio::test]
    async fn unified_put_and_get_delegate_rest() {
        let mut server = Server::new_async().await;
        let put_mock = server
            .mock("PUT", "/objects/k")
            .with_status(201)
            .with_header("etag", "\"e\"")
            .create_async()
            .await;
        let get_mock = server
            .mock("GET", "/objects/k")
            .with_status(200)
            .with_header("content-length", "5")
            .with_body("hello")
            .create_async()
            .await;

        let client = rest(server.url());
        assert!(
            client
                .put("k", Bytes::from_static(b"hello"), None)
                .await
                .unwrap()
                .success
        );
        let (data, _meta) = client.get("k").await.unwrap();
        assert_eq!(&data[..], b"hello");

        put_mock.assert_async().await;
        get_mock.assert_async().await;
    }

    #[tokio::test]
    async fn unified_delete_exists_list_delegate_rest() {
        let mut server = Server::new_async().await;
        let del = server
            .mock("DELETE", "/objects/k")
            .with_status(204)
            .create_async()
            .await;
        let head = server
            .mock("HEAD", "/objects/k")
            .with_status(200)
            .create_async()
            .await;
        let list = server
            .mock("GET", "/objects")
            .with_status(200)
            .with_body(r#"{"objects":[],"common_prefixes":[],"truncated":false}"#)
            .create_async()
            .await;

        let client = rest(server.url());
        assert!(client.delete("k").await.unwrap().success);
        assert!(client.exists("k").await.unwrap());
        assert!(client
            .list(ListRequest::default())
            .await
            .unwrap()
            .objects
            .is_empty());

        del.assert_async().await;
        head.assert_async().await;
        list.assert_async().await;
    }

    #[tokio::test]
    async fn unified_metadata_ops_delegate_rest() {
        let mut server = Server::new_async().await;
        // The metadata endpoint returns a JSON body (ObjectResponse); mock it
        // accordingly so the client can parse size and content_type correctly.
        let get_meta = server
            .mock("GET", "/metadata/k")
            .with_status(200)
            .with_body(r#"{"key":"k","size":5,"content_type":"text/plain"}"#)
            .create_async()
            .await;
        let put_meta = server
            .mock("PUT", "/metadata/k")
            .with_status(200)
            .create_async()
            .await;

        let client = rest(server.url());
        let meta = client.get_metadata("k").await.unwrap();
        assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
        assert_eq!(meta.size, 5);
        client
            .update_metadata("k", Metadata::default())
            .await
            .unwrap();

        get_meta.assert_async().await;
        put_meta.assert_async().await;
    }

    #[tokio::test]
    async fn unified_archive_and_policy_ops_delegate_rest() {
        let mut server = Server::new_async().await;
        let archive = server
            .mock("POST", "/archive")
            .with_status(200)
            .create_async()
            .await;
        let add = server
            .mock("POST", "/policies")
            .with_status(201)
            .create_async()
            .await;
        let remove = server
            .mock("DELETE", "/policies/p1")
            .with_status(200)
            .create_async()
            .await;
        let get = server
            .mock("GET", "/policies")
            .with_status(200)
            .with_body(r#"{"policies":[]}"#)
            .create_async()
            .await;
        let apply = server
            .mock("POST", "/policies/apply")
            .with_status(200)
            .with_body(r#"{"policies_count":1,"objects_processed":2}"#)
            .create_async()
            .await;

        let client = rest(server.url());
        client
            .archive("k", "glacier".to_string(), HashMap::new())
            .await
            .unwrap();
        client
            .add_policy(LifecyclePolicy {
                id: "p1".to_string(),
                prefix: String::new(),
                retention_seconds: 1,
                action: "delete".to_string(),
                destination_type: None,
                destination_settings: HashMap::new(),
            })
            .await
            .unwrap();
        client.remove_policy("p1").await.unwrap();
        assert!(client.get_policies(None).await.unwrap().is_empty());
        assert_eq!(client.apply_policies().await.unwrap(), (1, 2));

        archive.assert_async().await;
        add.assert_async().await;
        remove.assert_async().await;
        get.assert_async().await;
        apply.assert_async().await;
    }

    #[tokio::test]
    async fn unified_replication_ops_delegate_rest() {
        let mut server = Server::new_async().await;
        let add = server
            .mock("POST", "/replication/policies")
            .with_status(201)
            .create_async()
            .await;
        let remove = server
            .mock("DELETE", "/replication/policies/repl-1")
            .with_status(200)
            .create_async()
            .await;
        let get_all = server
            .mock("GET", "/replication/policies")
            .with_status(200)
            .with_body(r#"{"policies":[]}"#)
            .create_async()
            .await;
        let get_one = server
            .mock("GET", "/replication/policies/repl-1")
            .with_status(200)
            .with_body(
                r#"{"id":"repl-1","source_backend":"s3","destination_backend":"gcs","check_interval_seconds":300,"replication_mode":"opaque"}"#,
            )
            .create_async()
            .await;
        let trigger = server
            .mock("POST", "/replication/trigger")
            .with_status(200)
            .with_body(
                r#"{"result":{"policy_id":"repl-1","synced":1,"deleted":0,"failed":0,"bytes_total":10,"duration":"1s","errors":[]}}"#,
            )
            .create_async()
            .await;
        let status = server
            .mock("GET", "/replication/status/repl-1")
            .with_status(200)
            .with_body(
                r#"{"policy_id":"repl-1","source_backend":"s3","destination_backend":"gcs","enabled":true,"sync_count":1}"#,
            )
            .create_async()
            .await;

        let client = rest(server.url());
        client
            .add_replication_policy(sample_replication_policy())
            .await
            .unwrap();
        client.remove_replication_policy("repl-1").await.unwrap();
        assert!(client.get_replication_policies().await.unwrap().is_empty());
        assert_eq!(
            client.get_replication_policy("repl-1").await.unwrap().id,
            "repl-1"
        );
        assert_eq!(
            client
                .trigger_replication(None, false, 1)
                .await
                .unwrap()
                .policy_id,
            "repl-1"
        );
        assert_eq!(
            client
                .get_replication_status("repl-1")
                .await
                .unwrap()
                .policy_id,
            "repl-1"
        );

        add.assert_async().await;
        remove.assert_async().await;
        get_all.assert_async().await;
        get_one.assert_async().await;
        trigger.assert_async().await;
        status.assert_async().await;
    }

    // Ignored in the unit suite: constructs a real QUIC (quinn) endpoint whose
    // background driver does not terminate, hanging process exit. The QUIC
    // delegation arms are covered by the `quic_client` unit suite; this runs
    // under the integration suite.
    #[ignore = "leaks QUIC endpoint driver; covered by integration tests"]
    #[tokio::test]
    async fn unified_all_ops_route_through_quic_arm() {
        // Drive every delegating method through the QUIC arm (pointed at a dead
        // address). Each call executes its `ObjectStoreClient::Quic(_) =>` match
        // arm in this file before failing at connect time, covering the QUIC
        // delegation arms. We only assert the calls return (Err is expected).
        let addr: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
        let client = ObjectStoreClient::quic(addr, "localhost").await.unwrap();

        let _ = client.put("k", Bytes::from_static(b"d"), None).await;
        let _ = client.get("k").await;
        let _ = client.delete("k").await;
        let _ = client.list(ListRequest::default()).await;
        let _ = client.exists("k").await;
        let _ = client.get_metadata("k").await;
        let _ = client.update_metadata("k", Metadata::default()).await;
        let _ = client.health().await;
        let _ = client
            .archive("k", "glacier".to_string(), HashMap::new())
            .await;
        let _ = client
            .add_policy(LifecyclePolicy {
                id: "p1".to_string(),
                prefix: String::new(),
                retention_seconds: 1,
                action: "delete".to_string(),
                destination_type: None,
                destination_settings: HashMap::new(),
            })
            .await;
        let _ = client.remove_policy("p1").await;
        let _ = client.get_policies(None).await;
        let _ = client.apply_policies().await;
        let _ = client
            .add_replication_policy(sample_replication_policy())
            .await;
        let _ = client.remove_replication_policy("repl-1").await;
        let _ = client.get_replication_policies().await;
        let _ = client.get_replication_policy("repl-1").await;
        let _ = client.trigger_replication(None, false, 1).await;
        let _ = client.get_replication_status("repl-1").await;
    }

    // ---- retained: constructor + trait-object + concurrency ----

    #[test]
    fn unified_rest_constructor_ok() {
        assert!(ObjectStoreClient::rest("http://localhost:8080").is_ok());
    }

    #[tokio::test]
    async fn unified_usable_as_trait_object() {
        let client = ObjectStoreClient::rest("http://localhost:8080").unwrap();
        let boxed: Box<dyn ObjectStore> = Box::new(client);
        let _ = boxed.health().await;
    }

    #[tokio::test]
    async fn unified_concurrent_usage() {
        use std::sync::Arc;

        let client = Arc::new(ObjectStoreClient::rest("http://localhost:8080").unwrap());
        let handles: Vec<_> = (0..3)
            .map(|i| {
                let client_clone = Arc::clone(&client);
                tokio::spawn(async move {
                    let _ = client_clone.exists(&format!("test-{}.txt", i)).await;
                })
            })
            .collect();
        for handle in handles {
            let _ = handle.await;
        }
    }
}
