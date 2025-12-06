use crate::error::{Error, Result};
use crate::types::*;
use bytes::Bytes;
use futures::StreamExt;
use std::collections::HashMap;
use tonic::transport::Channel;

// Include the generated protobuf code
pub mod pb {
    include!("proto/objstore.v1.rs");
}

use pb::object_store_client::ObjectStoreClient as GrpcObjectStoreClient;

/// gRPC client for go-objstore
#[derive(Clone)]
pub struct GrpcClient {
    client: GrpcObjectStoreClient<Channel>,
}

impl GrpcClient {
    /// Create a new gRPC client
    pub async fn new(endpoint: impl Into<String>) -> Result<Self> {
        let endpoint = endpoint.into();
        let channel = Channel::from_shared(endpoint)
            .map_err(|e| Error::Configuration(e.to_string()))?
            .connect()
            .await?;

        Ok(Self {
            client: GrpcObjectStoreClient::new(channel),
        })
    }

    /// Put an object into storage
    pub async fn put(&self, key: String, data: Bytes, metadata: Option<Metadata>) -> Result<PutResponse> {
        let mut client = self.client.clone();
        let metadata_pb = metadata.map(|m| pb::Metadata {
            content_type: m.content_type.unwrap_or_default(),
            content_encoding: m.content_encoding.unwrap_or_default(),
            size: m.size,
            last_modified: m.last_modified.map(|dt| prost_types::Timestamp {
                seconds: dt.timestamp(),
                nanos: dt.timestamp_subsec_nanos() as i32,
            }),
            etag: m.etag.unwrap_or_default(),
            custom: m.custom,
        });

        let request = tonic::Request::new(pb::PutRequest {
            key,
            data: data.to_vec(),
            metadata: metadata_pb,
        });

        let response = client.put(request).await?.into_inner();

        Ok(PutResponse {
            success: response.success,
            message: if response.message.is_empty() {
                None
            } else {
                Some(response.message)
            },
            etag: if response.etag.is_empty() {
                None
            } else {
                Some(response.etag)
            },
        })
    }

    /// Get an object from storage
    pub async fn get(&self, key: String) -> Result<(Bytes, Metadata)> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::GetRequest { key });

        let mut stream = client.get(request).await?.into_inner();

        let mut data = Vec::new();
        let mut metadata: Option<Metadata> = None;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            data.extend_from_slice(&chunk.data);

            if metadata.is_none() && chunk.metadata.is_some() {
                metadata = chunk.metadata.map(convert_pb_metadata);
            }
        }

        Ok((
            Bytes::from(data),
            metadata.unwrap_or_default(),
        ))
    }

    /// Delete an object from storage
    pub async fn delete(&self, key: String) -> Result<DeleteResponse> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::DeleteRequest { key });

        let response = client.delete(request).await?.into_inner();

        Ok(DeleteResponse {
            success: response.success,
            message: if response.message.is_empty() {
                None
            } else {
                Some(response.message)
            },
        })
    }

    /// List objects with optional prefix filtering
    pub async fn list(&self, list_req: ListRequest) -> Result<ListResponse> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::ListRequest {
            prefix: list_req.prefix.unwrap_or_default(),
            delimiter: list_req.delimiter.unwrap_or_default(),
            max_results: list_req.max_results.unwrap_or(100),
            continue_from: list_req.continue_from.unwrap_or_default(),
        });

        let response = client.list(request).await?.into_inner();

        Ok(ListResponse {
            objects: response
                .objects
                .into_iter()
                .map(|obj| ObjectInfo {
                    key: obj.key,
                    metadata: obj.metadata.map(convert_pb_metadata).unwrap_or_default(),
                })
                .collect(),
            common_prefixes: response.common_prefixes,
            next_token: if response.next_token.is_empty() {
                None
            } else {
                Some(response.next_token)
            },
            truncated: response.truncated,
        })
    }

    /// Check if an object exists
    pub async fn exists(&self, key: String) -> Result<bool> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::ExistsRequest { key });

        let response = client.exists(request).await?.into_inner();

        Ok(response.exists)
    }

    /// Get metadata for an object
    pub async fn get_metadata(&self, key: String) -> Result<Metadata> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::GetMetadataRequest { key });

        let response = client.get_metadata(request).await?.into_inner();

        if !response.success {
            return Err(Error::OperationFailed(
                if response.message.is_empty() {
                    "Failed to get metadata".to_string()
                } else {
                    response.message
                },
            ));
        }

        Ok(response.metadata.map(convert_pb_metadata).unwrap_or_default())
    }

    /// Update metadata for an object
    pub async fn update_metadata(&self, key: String, metadata: Metadata) -> Result<()> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::UpdateMetadataRequest {
            key,
            metadata: Some(pb::Metadata {
                content_type: metadata.content_type.unwrap_or_default(),
                content_encoding: metadata.content_encoding.unwrap_or_default(),
                size: metadata.size,
                last_modified: metadata.last_modified.map(|dt| prost_types::Timestamp {
                    seconds: dt.timestamp(),
                    nanos: dt.timestamp_subsec_nanos() as i32,
                }),
                etag: metadata.etag.unwrap_or_default(),
                custom: metadata.custom,
            }),
        });

        let response = client.update_metadata(request).await?.into_inner();

        if !response.success {
            return Err(Error::OperationFailed(
                if response.message.is_empty() {
                    "Failed to update metadata".to_string()
                } else {
                    response.message
                },
            ));
        }

        Ok(())
    }

    /// Health check
    pub async fn health(&self, service: Option<String>) -> Result<HealthResponse> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::HealthRequest {
            service: service.unwrap_or_default(),
        });

        let response = client.health(request).await?.into_inner();

        Ok(HealthResponse {
            status: match pb::health_response::Status::try_from(response.status) {
                Ok(pb::health_response::Status::Serving) => HealthStatus::Serving,
                Ok(pb::health_response::Status::NotServing) => HealthStatus::NotServing,
                _ => HealthStatus::Unknown,
            },
            message: if response.message.is_empty() {
                None
            } else {
                Some(response.message)
            },
        })
    }

    /// Archive an object
    pub async fn archive(
        &self,
        key: String,
        destination_type: String,
        destination_settings: HashMap<String, String>,
    ) -> Result<()> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::ArchiveRequest {
            key,
            destination_type,
            destination_settings,
        });

        let response = client.archive(request).await?.into_inner();

        if !response.success {
            return Err(Error::OperationFailed(
                if response.message.is_empty() {
                    "Failed to archive".to_string()
                } else {
                    response.message
                },
            ));
        }

        Ok(())
    }

    /// Add a lifecycle policy
    pub async fn add_policy(&self, policy: LifecyclePolicy) -> Result<()> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::AddPolicyRequest {
            policy: Some(pb::LifecyclePolicy {
                id: policy.id,
                prefix: policy.prefix,
                retention_seconds: policy.retention_seconds,
                action: policy.action,
                destination_type: policy.destination_type.unwrap_or_default(),
                destination_settings: policy.destination_settings,
            }),
        });

        let response = client.add_policy(request).await?.into_inner();

        if !response.success {
            return Err(Error::OperationFailed(
                if response.message.is_empty() {
                    "Failed to add policy".to_string()
                } else {
                    response.message
                },
            ));
        }

        Ok(())
    }

    /// Remove a lifecycle policy
    pub async fn remove_policy(&self, id: String) -> Result<()> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::RemovePolicyRequest { id });

        let response = client.remove_policy(request).await?.into_inner();

        if !response.success {
            return Err(Error::OperationFailed(
                if response.message.is_empty() {
                    "Failed to remove policy".to_string()
                } else {
                    response.message
                },
            ));
        }

        Ok(())
    }

    /// Get all lifecycle policies
    pub async fn get_policies(&self, prefix: Option<String>) -> Result<Vec<LifecyclePolicy>> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::GetPoliciesRequest {
            prefix: prefix.unwrap_or_default(),
        });

        let response = client.get_policies(request).await?.into_inner();

        Ok(response
            .policies
            .into_iter()
            .map(|p| LifecyclePolicy {
                id: p.id,
                prefix: p.prefix,
                retention_seconds: p.retention_seconds,
                action: p.action,
                destination_type: if p.destination_type.is_empty() {
                    None
                } else {
                    Some(p.destination_type)
                },
                destination_settings: p.destination_settings,
            })
            .collect())
    }

    /// Apply all lifecycle policies
    pub async fn apply_policies(&self) -> Result<(i32, i32)> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::ApplyPoliciesRequest {});

        let response = client.apply_policies(request).await?.into_inner();

        if !response.success {
            return Err(Error::OperationFailed(
                if response.message.is_empty() {
                    "Failed to apply policies".to_string()
                } else {
                    response.message
                },
            ));
        }

        Ok((response.policies_count, response.objects_processed))
    }

    /// Add a replication policy
    pub async fn add_replication_policy(&self, policy: ReplicationPolicy) -> Result<()> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::AddReplicationPolicyRequest {
            policy: Some(convert_to_pb_replication_policy(policy)),
        });

        let response = client.add_replication_policy(request).await?.into_inner();

        if !response.success {
            return Err(Error::OperationFailed(
                if response.message.is_empty() {
                    "Failed to add replication policy".to_string()
                } else {
                    response.message
                },
            ));
        }

        Ok(())
    }

    /// Remove a replication policy
    pub async fn remove_replication_policy(&self, id: String) -> Result<()> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::RemoveReplicationPolicyRequest { id });

        let response = client.remove_replication_policy(request).await?.into_inner();

        if !response.success {
            return Err(Error::OperationFailed(
                if response.message.is_empty() {
                    "Failed to remove replication policy".to_string()
                } else {
                    response.message
                },
            ));
        }

        Ok(())
    }

    /// Get all replication policies
    pub async fn get_replication_policies(&self) -> Result<Vec<ReplicationPolicy>> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::GetReplicationPoliciesRequest {});

        let response = client.get_replication_policies(request).await?.into_inner();

        Ok(response
            .policies
            .into_iter()
            .map(convert_from_pb_replication_policy)
            .collect())
    }

    /// Get a specific replication policy
    pub async fn get_replication_policy(&self, id: String) -> Result<ReplicationPolicy> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::GetReplicationPolicyRequest { id });

        let response = client.get_replication_policy(request).await?.into_inner();

        response
            .policy
            .map(convert_from_pb_replication_policy)
            .ok_or_else(|| Error::NotFound("Replication policy not found".to_string()))
    }

    /// Trigger replication
    pub async fn trigger_replication(
        &self,
        policy_id: Option<String>,
        parallel: bool,
        worker_count: i32,
    ) -> Result<SyncResult> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::TriggerReplicationRequest {
            policy_id: policy_id.unwrap_or_default(),
            parallel,
            worker_count,
        });

        let response = client.trigger_replication(request).await?.into_inner();

        if !response.success {
            return Err(Error::OperationFailed(
                if response.message.is_empty() {
                    "Failed to trigger replication".to_string()
                } else {
                    response.message
                },
            ));
        }

        response
            .result
            .map(|r| SyncResult {
                policy_id: r.policy_id,
                synced: r.synced,
                deleted: r.deleted,
                failed: r.failed,
                bytes_total: r.bytes_total,
                duration_ms: r.duration_ms,
                errors: r.errors,
            })
            .ok_or_else(|| Error::InvalidResponse("Missing sync result".to_string()))
    }

    /// Get replication status
    pub async fn get_replication_status(&self, id: String) -> Result<ReplicationStatus> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::GetReplicationStatusRequest { id });

        let response = client.get_replication_status(request).await?.into_inner();

        if !response.success {
            return Err(Error::OperationFailed(
                if response.message.is_empty() {
                    "Failed to get replication status".to_string()
                } else {
                    response.message
                },
            ));
        }

        response
            .status
            .map(|s| ReplicationStatus {
                policy_id: s.policy_id,
                source_backend: s.source_backend,
                destination_backend: s.destination_backend,
                enabled: s.enabled,
                total_objects_synced: s.total_objects_synced,
                total_objects_deleted: s.total_objects_deleted,
                total_bytes_synced: s.total_bytes_synced,
                total_errors: s.total_errors,
                last_sync_time: s.last_sync_time.map(|ts| {
                    chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                        .unwrap_or_default()
                }),
                average_sync_duration_ms: s.average_sync_duration_ms,
                sync_count: s.sync_count,
            })
            .ok_or_else(|| Error::InvalidResponse("Missing replication status".to_string()))
    }
}

// Helper functions for converting between protobuf and SDK types

fn convert_pb_metadata(m: pb::Metadata) -> Metadata {
    Metadata {
        content_type: if m.content_type.is_empty() {
            None
        } else {
            Some(m.content_type)
        },
        content_encoding: if m.content_encoding.is_empty() {
            None
        } else {
            Some(m.content_encoding)
        },
        size: m.size,
        last_modified: m.last_modified.map(|ts| {
            chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                .unwrap_or_default()
        }),
        etag: if m.etag.is_empty() { None } else { Some(m.etag) },
        custom: m.custom,
    }
}

fn convert_to_pb_replication_policy(p: ReplicationPolicy) -> pb::ReplicationPolicy {
    pb::ReplicationPolicy {
        id: p.id,
        source_backend: p.source_backend,
        source_settings: p.source_settings,
        source_prefix: p.source_prefix,
        destination_backend: p.destination_backend,
        destination_settings: p.destination_settings,
        check_interval_seconds: p.check_interval_seconds,
        last_sync_time: p.last_sync_time.map(|dt| prost_types::Timestamp {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos() as i32,
        }),
        enabled: p.enabled,
        encryption: p.encryption.map(|e| pb::EncryptionPolicy {
            backend: e.backend.map(|c| pb::EncryptionConfig {
                enabled: c.enabled,
                provider: c.provider,
                default_key: c.default_key,
            }),
            source: e.source.map(|c| pb::EncryptionConfig {
                enabled: c.enabled,
                provider: c.provider,
                default_key: c.default_key,
            }),
            destination: e.destination.map(|c| pb::EncryptionConfig {
                enabled: c.enabled,
                provider: c.provider,
                default_key: c.default_key,
            }),
        }),
        replication_mode: match p.replication_mode {
            ReplicationMode::Transparent => pb::ReplicationMode::Transparent as i32,
            ReplicationMode::Opaque => pb::ReplicationMode::Opaque as i32,
        },
    }
}

fn convert_from_pb_replication_policy(p: pb::ReplicationPolicy) -> ReplicationPolicy {
    ReplicationPolicy {
        id: p.id,
        source_backend: p.source_backend,
        source_settings: p.source_settings,
        source_prefix: p.source_prefix,
        destination_backend: p.destination_backend,
        destination_settings: p.destination_settings,
        check_interval_seconds: p.check_interval_seconds,
        last_sync_time: p.last_sync_time.map(|ts| {
            chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                .unwrap_or_default()
        }),
        enabled: p.enabled,
        encryption: p.encryption.map(|e| EncryptionPolicy {
            backend: e.backend.map(|c| EncryptionConfig {
                enabled: c.enabled,
                provider: c.provider,
                default_key: c.default_key,
            }),
            source: e.source.map(|c| EncryptionConfig {
                enabled: c.enabled,
                provider: c.provider,
                default_key: c.default_key,
            }),
            destination: e.destination.map(|c| EncryptionConfig {
                enabled: c.enabled,
                provider: c.provider,
                default_key: c.default_key,
            }),
        }),
        replication_mode: if p.replication_mode == pb::ReplicationMode::Opaque as i32 {
            ReplicationMode::Opaque
        } else {
            ReplicationMode::Transparent
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_pb_metadata() {
        let pb_metadata = pb::Metadata {
            content_type: "application/json".to_string(),
            content_encoding: "gzip".to_string(),
            size: 1024,
            last_modified: None,
            etag: "abc123".to_string(),
            custom: HashMap::new(),
        };

        let metadata = convert_pb_metadata(pb_metadata);
        assert_eq!(metadata.content_type, Some("application/json".to_string()));
        assert_eq!(metadata.size, 1024);
    }

    #[test]
    fn test_replication_mode_conversion() {
        let policy = ReplicationPolicy {
            id: "test".to_string(),
            source_backend: "local".to_string(),
            source_settings: HashMap::new(),
            source_prefix: "".to_string(),
            destination_backend: "s3".to_string(),
            destination_settings: HashMap::new(),
            check_interval_seconds: 60,
            last_sync_time: None,
            enabled: true,
            encryption: None,
            replication_mode: ReplicationMode::Transparent,
        };

        let pb_policy = convert_to_pb_replication_policy(policy.clone());
        let converted_policy = convert_from_pb_replication_policy(pb_policy);
        assert_eq!(converted_policy.replication_mode, ReplicationMode::Transparent);
    }
}
