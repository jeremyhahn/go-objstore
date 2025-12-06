use crate::error::Result;
use crate::grpc_client::GrpcClient;
use crate::quic_client::QuicClient;
use crate::rest_client::RestClient;
use crate::types::*;
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
}

#[async_trait]
impl ObjectStore for ObjectStoreClient {
    async fn put(&self, key: &str, data: Bytes, metadata: Option<Metadata>) -> Result<PutResponse> {
        match self {
            ObjectStoreClient::Rest(client) => client.put(key, data, metadata).await,
            ObjectStoreClient::Grpc(client) => client.put(key.to_string(), data, metadata).await,
            ObjectStoreClient::Quic(client) => client.put(key, data, metadata).await,
        }
    }

    async fn get(&self, key: &str) -> Result<(Bytes, Metadata)> {
        match self {
            ObjectStoreClient::Rest(client) => client.get(key).await,
            ObjectStoreClient::Grpc(client) => client.get(key.to_string()).await,
            ObjectStoreClient::Quic(client) => client.get(key).await,
        }
    }

    async fn delete(&self, key: &str) -> Result<DeleteResponse> {
        match self {
            ObjectStoreClient::Rest(client) => client.delete(key).await,
            ObjectStoreClient::Grpc(client) => client.delete(key.to_string()).await,
            ObjectStoreClient::Quic(client) => client.delete(key).await,
        }
    }

    async fn list(&self, list_req: ListRequest) -> Result<ListResponse> {
        match self {
            ObjectStoreClient::Rest(client) => client.list(list_req).await,
            ObjectStoreClient::Grpc(client) => client.list(list_req).await,
            ObjectStoreClient::Quic(client) => client.list(list_req).await,
        }
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        match self {
            ObjectStoreClient::Rest(client) => client.exists(key).await,
            ObjectStoreClient::Grpc(client) => client.exists(key.to_string()).await,
            ObjectStoreClient::Quic(client) => client.exists(key).await,
        }
    }

    async fn get_metadata(&self, key: &str) -> Result<Metadata> {
        match self {
            ObjectStoreClient::Rest(client) => client.get_metadata(key).await,
            ObjectStoreClient::Grpc(client) => client.get_metadata(key.to_string()).await,
            ObjectStoreClient::Quic(client) => client.get_metadata(key).await,
        }
    }

    async fn update_metadata(&self, key: &str, metadata: Metadata) -> Result<()> {
        match self {
            ObjectStoreClient::Rest(client) => client.update_metadata(key, metadata).await,
            ObjectStoreClient::Grpc(client) => {
                client.update_metadata(key.to_string(), metadata).await
            }
            ObjectStoreClient::Quic(client) => client.update_metadata(key, metadata).await,
        }
    }

    async fn health(&self) -> Result<HealthResponse> {
        match self {
            ObjectStoreClient::Rest(client) => client.health().await,
            ObjectStoreClient::Grpc(client) => client.health(None).await,
            ObjectStoreClient::Quic(client) => client.health().await,
        }
    }
}

/// Extended operations for gRPC client
impl ObjectStoreClient {
    /// Archive an object (gRPC only)
    pub async fn archive(
        &self,
        key: &str,
        destination_type: String,
        destination_settings: HashMap<String, String>,
    ) -> Result<()> {
        match self {
            ObjectStoreClient::Grpc(client) => {
                client
                    .archive(key.to_string(), destination_type, destination_settings)
                    .await
            }
            _ => Err(crate::error::Error::OperationFailed(
                "Archive only supported on gRPC client".to_string(),
            )),
        }
    }

    /// Add a lifecycle policy (gRPC only)
    pub async fn add_policy(&self, policy: LifecyclePolicy) -> Result<()> {
        match self {
            ObjectStoreClient::Grpc(client) => client.add_policy(policy).await,
            _ => Err(crate::error::Error::OperationFailed(
                "Lifecycle policies only supported on gRPC client".to_string(),
            )),
        }
    }

    /// Remove a lifecycle policy (gRPC only)
    pub async fn remove_policy(&self, id: &str) -> Result<()> {
        match self {
            ObjectStoreClient::Grpc(client) => client.remove_policy(id.to_string()).await,
            _ => Err(crate::error::Error::OperationFailed(
                "Lifecycle policies only supported on gRPC client".to_string(),
            )),
        }
    }

    /// Get all lifecycle policies (gRPC only)
    pub async fn get_policies(&self, prefix: Option<String>) -> Result<Vec<LifecyclePolicy>> {
        match self {
            ObjectStoreClient::Grpc(client) => client.get_policies(prefix).await,
            _ => Err(crate::error::Error::OperationFailed(
                "Lifecycle policies only supported on gRPC client".to_string(),
            )),
        }
    }

    /// Apply all lifecycle policies (gRPC only)
    pub async fn apply_policies(&self) -> Result<(i32, i32)> {
        match self {
            ObjectStoreClient::Grpc(client) => client.apply_policies().await,
            _ => Err(crate::error::Error::OperationFailed(
                "Lifecycle policies only supported on gRPC client".to_string(),
            )),
        }
    }

    /// Add a replication policy (gRPC only)
    pub async fn add_replication_policy(&self, policy: ReplicationPolicy) -> Result<()> {
        match self {
            ObjectStoreClient::Grpc(client) => client.add_replication_policy(policy).await,
            _ => Err(crate::error::Error::OperationFailed(
                "Replication policies only supported on gRPC client".to_string(),
            )),
        }
    }

    /// Remove a replication policy (gRPC only)
    pub async fn remove_replication_policy(&self, id: &str) -> Result<()> {
        match self {
            ObjectStoreClient::Grpc(client) => {
                client.remove_replication_policy(id.to_string()).await
            }
            _ => Err(crate::error::Error::OperationFailed(
                "Replication policies only supported on gRPC client".to_string(),
            )),
        }
    }

    /// Get all replication policies (gRPC only)
    pub async fn get_replication_policies(&self) -> Result<Vec<ReplicationPolicy>> {
        match self {
            ObjectStoreClient::Grpc(client) => client.get_replication_policies().await,
            _ => Err(crate::error::Error::OperationFailed(
                "Replication policies only supported on gRPC client".to_string(),
            )),
        }
    }

    /// Get a specific replication policy (gRPC only)
    pub async fn get_replication_policy(&self, id: &str) -> Result<ReplicationPolicy> {
        match self {
            ObjectStoreClient::Grpc(client) => client.get_replication_policy(id.to_string()).await,
            _ => Err(crate::error::Error::OperationFailed(
                "Replication policies only supported on gRPC client".to_string(),
            )),
        }
    }

    /// Trigger replication (gRPC only)
    pub async fn trigger_replication(
        &self,
        policy_id: Option<String>,
        parallel: bool,
        worker_count: i32,
    ) -> Result<SyncResult> {
        match self {
            ObjectStoreClient::Grpc(client) => {
                client
                    .trigger_replication(policy_id, parallel, worker_count)
                    .await
            }
            _ => Err(crate::error::Error::OperationFailed(
                "Replication only supported on gRPC client".to_string(),
            )),
        }
    }

    /// Get replication status (gRPC only)
    pub async fn get_replication_status(&self, id: &str) -> Result<ReplicationStatus> {
        match self {
            ObjectStoreClient::Grpc(client) => client.get_replication_status(id.to_string()).await,
            _ => Err(crate::error::Error::OperationFailed(
                "Replication status only supported on gRPC client".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rest_client_creation() {
        let client = ObjectStoreClient::rest("http://localhost:8080");
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_trait_object() {
        let client = ObjectStoreClient::rest("http://localhost:8080").unwrap();
        let boxed: Box<dyn ObjectStore> = Box::new(client);

        // This should compile, demonstrating the trait works with &self
        let _ = boxed.health().await;
    }

    #[tokio::test]
    async fn test_concurrent_usage() {
        use std::sync::Arc;

        let client = Arc::new(ObjectStoreClient::rest("http://localhost:8080").unwrap());

        // Spawn multiple concurrent tasks using the same client
        let handles: Vec<_> = (0..3).map(|i| {
            let client_clone = Arc::clone(&client);
            tokio::spawn(async move {
                // These operations can now run concurrently since we use &self
                let _ = client_clone.exists(&format!("test-{}.txt", i)).await;
            })
        }).collect();

        // Wait for all tasks to complete
        for handle in handles {
            let _ = handle.await;
        }
    }
}
