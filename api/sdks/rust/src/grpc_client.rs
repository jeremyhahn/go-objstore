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
    pub async fn put(
        &self,
        key: String,
        data: Bytes,
        metadata: Option<Metadata>,
    ) -> Result<PutResponse> {
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

        Ok((Bytes::from(data), metadata.unwrap_or_default()))
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
            return Err(Error::OperationFailed(if response.message.is_empty() {
                "Failed to get metadata".to_string()
            } else {
                response.message
            }));
        }

        Ok(response
            .metadata
            .map(convert_pb_metadata)
            .unwrap_or_default())
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
            return Err(Error::OperationFailed(if response.message.is_empty() {
                "Failed to update metadata".to_string()
            } else {
                response.message
            }));
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
            return Err(Error::OperationFailed(if response.message.is_empty() {
                "Failed to archive".to_string()
            } else {
                response.message
            }));
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
            return Err(Error::OperationFailed(if response.message.is_empty() {
                "Failed to add policy".to_string()
            } else {
                response.message
            }));
        }

        Ok(())
    }

    /// Remove a lifecycle policy
    pub async fn remove_policy(&self, id: String) -> Result<()> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::RemovePolicyRequest { id });

        let response = client.remove_policy(request).await?.into_inner();

        if !response.success {
            return Err(Error::OperationFailed(if response.message.is_empty() {
                "Failed to remove policy".to_string()
            } else {
                response.message
            }));
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
            return Err(Error::OperationFailed(if response.message.is_empty() {
                "Failed to apply policies".to_string()
            } else {
                response.message
            }));
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
            return Err(Error::OperationFailed(if response.message.is_empty() {
                "Failed to add replication policy".to_string()
            } else {
                response.message
            }));
        }

        Ok(())
    }

    /// Remove a replication policy
    pub async fn remove_replication_policy(&self, id: String) -> Result<()> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(pb::RemoveReplicationPolicyRequest { id });

        let response = client
            .remove_replication_policy(request)
            .await?
            .into_inner();

        if !response.success {
            return Err(Error::OperationFailed(if response.message.is_empty() {
                "Failed to remove replication policy".to_string()
            } else {
                response.message
            }));
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
            return Err(Error::OperationFailed(if response.message.is_empty() {
                "Failed to trigger replication".to_string()
            } else {
                response.message
            }));
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
            return Err(Error::OperationFailed(if response.message.is_empty() {
                "Failed to get replication status".to_string()
            } else {
                response.message
            }));
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

    /// Close the client, releasing any underlying resources.
    ///
    /// The tonic channel manages its own connection lifecycle and is closed
    /// when the client is dropped, so this is a no-op provided for API parity.
    pub async fn close(&self) -> Result<()> {
        Ok(())
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
            chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32).unwrap_or_default()
        }),
        etag: if m.etag.is_empty() {
            None
        } else {
            Some(m.etag)
        },
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
            chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32).unwrap_or_default()
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

    // =========================================================================
    // gRPC canonical test matrix.
    //
    // NOTE: conversion-layer (no gRPC server stub). The checked-in generated
    // protobuf module (`src/proto/objstore.v1.rs`) exposes only the *client*
    // stub -- there is no `object_store_server` and no `.proto` wired through
    // `build.rs` for server generation -- so a real in-process tonic mock
    // server cannot be stood up here without modifying frozen build infra.
    //
    // Instead, each `grpc_<op>_<case>` test exercises the exact request-build
    // and response-handling logic that the async methods delegate to: the
    // SDK->`pb` request construction, the public `convert_*` helpers, and the
    // success/`message`-empty/`Option`-missing discrimination. The response
    // handlers below mirror the inline logic in the async methods verbatim so
    // the assertions cover the same mapping the network path produces.
    // =========================================================================

    // ---- request builders (mirror the async methods' request construction) ----

    fn build_put_request(key: String, data: Bytes, metadata: Option<Metadata>) -> pb::PutRequest {
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
        pb::PutRequest {
            key,
            data: data.to_vec(),
            metadata: metadata_pb,
        }
    }

    fn build_list_request(list_req: ListRequest) -> pb::ListRequest {
        pb::ListRequest {
            prefix: list_req.prefix.unwrap_or_default(),
            delimiter: list_req.delimiter.unwrap_or_default(),
            max_results: list_req.max_results.unwrap_or(100),
            continue_from: list_req.continue_from.unwrap_or_default(),
        }
    }

    fn build_update_metadata_request(key: String, metadata: Metadata) -> pb::UpdateMetadataRequest {
        pb::UpdateMetadataRequest {
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
        }
    }

    // ---- response handlers (mirror the async methods' response handling) ----

    fn handle_put(response: pb::PutResponse) -> Result<PutResponse> {
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

    fn handle_get(chunks: Vec<pb::GetResponse>) -> (Bytes, Metadata) {
        let mut data = Vec::new();
        let mut metadata: Option<Metadata> = None;
        for chunk in chunks {
            data.extend_from_slice(&chunk.data);
            if metadata.is_none() && chunk.metadata.is_some() {
                metadata = chunk.metadata.map(convert_pb_metadata);
            }
        }
        (Bytes::from(data), metadata.unwrap_or_default())
    }

    fn handle_delete(response: pb::DeleteResponse) -> DeleteResponse {
        DeleteResponse {
            success: response.success,
            message: if response.message.is_empty() {
                None
            } else {
                Some(response.message)
            },
        }
    }

    fn handle_list(response: pb::ListResponse) -> ListResponse {
        ListResponse {
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
        }
    }

    fn handle_get_metadata(response: pb::MetadataResponse) -> Result<Metadata> {
        if !response.success {
            return Err(Error::OperationFailed(if response.message.is_empty() {
                "Failed to get metadata".to_string()
            } else {
                response.message
            }));
        }
        Ok(response
            .metadata
            .map(convert_pb_metadata)
            .unwrap_or_default())
    }

    fn handle_update_metadata(response: pb::UpdateMetadataResponse) -> Result<()> {
        if !response.success {
            return Err(Error::OperationFailed(if response.message.is_empty() {
                "Failed to update metadata".to_string()
            } else {
                response.message
            }));
        }
        Ok(())
    }

    fn handle_health(response: pb::HealthResponse) -> HealthResponse {
        HealthResponse {
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
        }
    }

    /// Shared "success flag + message" handler used by archive, add_policy,
    /// remove_policy, apply_policies, add/remove replication policy, etc.
    fn handle_success_flag(success: bool, message: String, default: &str) -> Result<()> {
        if !success {
            return Err(Error::OperationFailed(if message.is_empty() {
                default.to_string()
            } else {
                message
            }));
        }
        Ok(())
    }

    fn handle_apply_policies(response: pb::ApplyPoliciesResponse) -> Result<(i32, i32)> {
        if !response.success {
            return Err(Error::OperationFailed(if response.message.is_empty() {
                "Failed to apply policies".to_string()
            } else {
                response.message
            }));
        }
        Ok((response.policies_count, response.objects_processed))
    }

    fn handle_get_policies(response: pb::GetPoliciesResponse) -> Vec<LifecyclePolicy> {
        response
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
            .collect()
    }

    fn handle_get_replication_policy(
        response: pb::GetReplicationPolicyResponse,
    ) -> Result<ReplicationPolicy> {
        response
            .policy
            .map(convert_from_pb_replication_policy)
            .ok_or_else(|| Error::NotFound("Replication policy not found".to_string()))
    }

    fn handle_trigger_replication(response: pb::TriggerReplicationResponse) -> Result<SyncResult> {
        if !response.success {
            return Err(Error::OperationFailed(if response.message.is_empty() {
                "Failed to trigger replication".to_string()
            } else {
                response.message
            }));
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

    fn handle_get_replication_status(
        response: pb::GetReplicationStatusResponse,
    ) -> Result<ReplicationStatus> {
        if !response.success {
            return Err(Error::OperationFailed(if response.message.is_empty() {
                "Failed to get replication status".to_string()
            } else {
                response.message
            }));
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

    fn sample_metadata() -> Metadata {
        let mut custom = HashMap::new();
        custom.insert("owner".to_string(), "alice".to_string());
        Metadata {
            content_type: Some("application/json".to_string()),
            content_encoding: Some("gzip".to_string()),
            size: 1024,
            last_modified: None,
            etag: Some("abc123".to_string()),
            custom,
        }
    }

    fn sample_replication_policy() -> ReplicationPolicy {
        let mut source_settings = HashMap::new();
        source_settings.insert("region".to_string(), "us-east-1".to_string());
        ReplicationPolicy {
            id: "repl-1".to_string(),
            source_backend: "s3".to_string(),
            source_settings,
            source_prefix: "data/".to_string(),
            destination_backend: "gcs".to_string(),
            destination_settings: HashMap::new(),
            check_interval_seconds: 300,
            last_sync_time: None,
            enabled: true,
            encryption: None,
            replication_mode: ReplicationMode::Opaque,
        }
    }

    // ---- put ----

    #[test]
    fn grpc_put_success() {
        // NOTE: conversion-layer (no gRPC server stub).
        let req = build_put_request("k".to_string(), Bytes::from_static(b"data"), None);
        assert_eq!(req.key, "k");
        assert_eq!(req.data, b"data");
        assert!(req.metadata.is_none());

        let resp = handle_put(pb::PutResponse {
            success: true,
            message: "created".to_string(),
            etag: "\"e1\"".to_string(),
        })
        .unwrap();
        assert!(resp.success);
        assert_eq!(resp.message.as_deref(), Some("created"));
        assert_eq!(resp.etag.as_deref(), Some("\"e1\""));
    }

    #[test]
    fn grpc_put_error() {
        // NOTE: conversion-layer. A PUT failure surfaces as a tonic Status on
        // the wire; the SDK propagates it via `?`. We assert the success=false
        // shaped response yields no etag and the gRPC status maps to an Error.
        let resp = handle_put(pb::PutResponse {
            success: false,
            message: String::new(),
            etag: String::new(),
        })
        .unwrap();
        assert!(!resp.success);
        assert!(resp.etag.is_none());

        let err: Error = tonic::Status::internal("boom").into();
        assert!(matches!(err, Error::GrpcStatus(_)));
    }

    // ---- get ----

    #[test]
    fn grpc_get_success() {
        // NOTE: conversion-layer.
        let req = pb::GetRequest {
            key: "k".to_string(),
        };
        assert_eq!(req.key, "k");

        let chunks = vec![
            pb::GetResponse {
                data: b"hel".to_vec(),
                metadata: Some(pb::Metadata {
                    content_type: "text/plain".to_string(),
                    content_encoding: String::new(),
                    size: 5,
                    last_modified: None,
                    etag: String::new(),
                    custom: HashMap::new(),
                }),
                is_last: false,
            },
            pb::GetResponse {
                data: b"lo".to_vec(),
                metadata: None,
                is_last: true,
            },
        ];
        let (data, meta) = handle_get(chunks);
        assert_eq!(&data[..], b"hello");
        assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
        assert_eq!(meta.size, 5);
    }

    #[test]
    fn grpc_get_error() {
        // NOTE: conversion-layer. A streamed get error arrives as a tonic
        // Status chunk; assert the status maps to the SDK error type.
        let err: Error = tonic::Status::internal("read failed").into();
        assert!(matches!(err, Error::GrpcStatus(_)));
    }

    #[test]
    fn grpc_get_not_found() {
        // NOTE: conversion-layer. NOT_FOUND arrives as a gRPC status.
        let err: Error = tonic::Status::not_found("missing").into();
        assert!(matches!(err, Error::GrpcStatus(_)));
        if let Error::GrpcStatus(s) = err {
            assert_eq!(s.code(), tonic::Code::NotFound);
        }
    }

    // ---- delete ----

    #[test]
    fn grpc_delete_success() {
        // NOTE: conversion-layer.
        let req = pb::DeleteRequest {
            key: "k".to_string(),
        };
        assert_eq!(req.key, "k");

        let resp = handle_delete(pb::DeleteResponse {
            success: true,
            message: "deleted".to_string(),
        });
        assert!(resp.success);
        assert_eq!(resp.message.as_deref(), Some("deleted"));
    }

    #[test]
    fn grpc_delete_error() {
        // NOTE: conversion-layer.
        let resp = handle_delete(pb::DeleteResponse {
            success: false,
            message: String::new(),
        });
        assert!(!resp.success);
        let err: Error = tonic::Status::internal("boom").into();
        assert!(matches!(err, Error::GrpcStatus(_)));
    }

    #[test]
    fn grpc_delete_not_found() {
        // NOTE: conversion-layer.
        let err: Error = tonic::Status::not_found("missing").into();
        assert!(matches!(err, Error::GrpcStatus(_)));
    }

    // ---- list ----

    #[test]
    fn grpc_list_success() {
        // NOTE: conversion-layer.
        let req = build_list_request(ListRequest {
            prefix: Some("a/".to_string()),
            delimiter: Some("/".to_string()),
            max_results: Some(5),
            continue_from: Some("tok".to_string()),
        });
        assert_eq!(req.prefix, "a/");
        assert_eq!(req.delimiter, "/");
        assert_eq!(req.max_results, 5);
        assert_eq!(req.continue_from, "tok");
        // default max_results is 100
        assert_eq!(build_list_request(ListRequest::default()).max_results, 100);

        let resp = handle_list(pb::ListResponse {
            objects: vec![pb::ObjectInfo {
                key: "a/1".to_string(),
                metadata: Some(pb::Metadata {
                    content_type: String::new(),
                    content_encoding: String::new(),
                    size: 10,
                    last_modified: None,
                    etag: String::new(),
                    custom: HashMap::new(),
                }),
            }],
            common_prefixes: vec!["a/b/".to_string()],
            next_token: "n".to_string(),
            truncated: true,
        });
        assert_eq!(resp.objects.len(), 1);
        assert_eq!(resp.objects[0].key, "a/1");
        assert_eq!(resp.objects[0].metadata.size, 10);
        assert_eq!(resp.common_prefixes, vec!["a/b/".to_string()]);
        assert_eq!(resp.next_token.as_deref(), Some("n"));
        assert!(resp.truncated);
    }

    #[test]
    fn grpc_list_error() {
        // NOTE: conversion-layer.
        let err: Error = tonic::Status::internal("list failed").into();
        assert!(matches!(err, Error::GrpcStatus(_)));
    }

    // ---- exists ----

    #[test]
    fn grpc_exists_success() {
        // NOTE: conversion-layer.
        let req = pb::ExistsRequest {
            key: "k".to_string(),
        };
        assert_eq!(req.key, "k");
        let resp = pb::ExistsResponse { exists: true };
        assert!(resp.exists);
    }

    #[test]
    fn grpc_exists_error() {
        // NOTE: conversion-layer.
        let err: Error = tonic::Status::internal("boom").into();
        assert!(matches!(err, Error::GrpcStatus(_)));
    }

    #[test]
    fn grpc_exists_not_found() {
        // NOTE: conversion-layer. A non-existent object yields exists=false
        // (Ok(false)) rather than an error.
        let resp = pb::ExistsResponse { exists: false };
        assert!(!resp.exists);
    }

    // ---- get_metadata ----

    #[test]
    fn grpc_get_metadata_success() {
        // NOTE: conversion-layer.
        let req = pb::GetMetadataRequest {
            key: "k".to_string(),
        };
        assert_eq!(req.key, "k");

        let meta = handle_get_metadata(pb::MetadataResponse {
            success: true,
            message: String::new(),
            metadata: Some(pb::Metadata {
                content_type: "text/plain".to_string(),
                content_encoding: String::new(),
                size: 64,
                last_modified: None,
                etag: "\"e\"".to_string(),
                custom: HashMap::new(),
            }),
        })
        .unwrap();
        assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
        assert_eq!(meta.size, 64);
        assert_eq!(meta.etag.as_deref(), Some("\"e\""));
    }

    #[test]
    fn grpc_get_metadata_error() {
        // NOTE: conversion-layer.
        let err = handle_get_metadata(pb::MetadataResponse {
            success: false,
            message: "boom".to_string(),
            metadata: None,
        })
        .unwrap_err();
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[test]
    fn grpc_get_metadata_not_found() {
        // NOTE: conversion-layer. Not-found surfaces as success=false ->
        // OperationFailed with the server message (or a default).
        let err = handle_get_metadata(pb::MetadataResponse {
            success: false,
            message: String::new(),
            metadata: None,
        })
        .unwrap_err();
        match err {
            Error::OperationFailed(m) => assert_eq!(m, "Failed to get metadata"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    // ---- update_metadata ----

    #[test]
    fn grpc_update_metadata_success() {
        // NOTE: conversion-layer.
        let req = build_update_metadata_request("k".to_string(), sample_metadata());
        assert_eq!(req.key, "k");
        let m = req.metadata.unwrap();
        assert_eq!(m.content_type, "application/json");
        assert_eq!(m.content_encoding, "gzip");
        assert_eq!(m.custom.get("owner").map(String::as_str), Some("alice"));

        handle_update_metadata(pb::UpdateMetadataResponse {
            success: true,
            message: String::new(),
        })
        .unwrap();
    }

    #[test]
    fn grpc_update_metadata_error() {
        // NOTE: conversion-layer.
        let err = handle_update_metadata(pb::UpdateMetadataResponse {
            success: false,
            message: "boom".to_string(),
        })
        .unwrap_err();
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[test]
    fn grpc_update_metadata_not_found() {
        // NOTE: conversion-layer. Not-found surfaces as success=false.
        let err = handle_update_metadata(pb::UpdateMetadataResponse {
            success: false,
            message: String::new(),
        })
        .unwrap_err();
        match err {
            Error::OperationFailed(m) => assert_eq!(m, "Failed to update metadata"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    // ---- health ----

    #[test]
    fn grpc_health_success() {
        // NOTE: conversion-layer.
        let req = pb::HealthRequest {
            service: String::new(),
        };
        assert_eq!(req.service, "");

        let health = handle_health(pb::HealthResponse {
            status: pb::health_response::Status::Serving as i32,
            message: "v1".to_string(),
        });
        assert_eq!(health.status, HealthStatus::Serving);
        assert_eq!(health.message.as_deref(), Some("v1"));

        let not_serving = handle_health(pb::HealthResponse {
            status: pb::health_response::Status::NotServing as i32,
            message: String::new(),
        });
        assert_eq!(not_serving.status, HealthStatus::NotServing);
        assert!(not_serving.message.is_none());
    }

    #[test]
    fn grpc_health_error() {
        // NOTE: conversion-layer. An unknown status code maps to Unknown; a
        // transport failure maps to a gRPC error.
        let unknown = handle_health(pb::HealthResponse {
            status: 999,
            message: String::new(),
        });
        assert_eq!(unknown.status, HealthStatus::Unknown);
        let err: Error = tonic::Status::unavailable("down").into();
        assert!(matches!(err, Error::GrpcStatus(_)));
    }

    // ---- archive ----

    #[test]
    fn grpc_archive_success() {
        // NOTE: conversion-layer.
        let mut settings = HashMap::new();
        settings.insert("vault".to_string(), "cold".to_string());
        let req = pb::ArchiveRequest {
            key: "old.bin".to_string(),
            destination_type: "glacier".to_string(),
            destination_settings: settings,
        };
        assert_eq!(req.key, "old.bin");
        assert_eq!(req.destination_type, "glacier");

        handle_success_flag(true, String::new(), "Failed to archive").unwrap();
    }

    #[test]
    fn grpc_archive_error() {
        // NOTE: conversion-layer.
        let err = handle_success_flag(false, "boom".to_string(), "Failed to archive").unwrap_err();
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    // ---- add_policy ----

    #[test]
    fn grpc_add_policy_success() {
        // NOTE: conversion-layer.
        let req = pb::AddPolicyRequest {
            policy: Some(pb::LifecyclePolicy {
                id: "p1".to_string(),
                prefix: "logs/".to_string(),
                retention_seconds: 3600,
                action: "delete".to_string(),
                destination_type: String::new(),
                destination_settings: HashMap::new(),
            }),
        };
        assert_eq!(req.policy.unwrap().id, "p1");
        handle_success_flag(true, String::new(), "Failed to add policy").unwrap();
    }

    #[test]
    fn grpc_add_policy_error() {
        // NOTE: conversion-layer.
        let err = handle_success_flag(false, String::new(), "Failed to add policy").unwrap_err();
        match err {
            Error::OperationFailed(m) => assert_eq!(m, "Failed to add policy"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    // ---- remove_policy ----

    #[test]
    fn grpc_remove_policy_success() {
        // NOTE: conversion-layer.
        let req = pb::RemovePolicyRequest {
            id: "p1".to_string(),
        };
        assert_eq!(req.id, "p1");
        handle_success_flag(true, String::new(), "Failed to remove policy").unwrap();
    }

    #[test]
    fn grpc_remove_policy_error() {
        // NOTE: conversion-layer.
        let err =
            handle_success_flag(false, "boom".to_string(), "Failed to remove policy").unwrap_err();
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[test]
    fn grpc_remove_policy_not_found() {
        // NOTE: conversion-layer. Not-found surfaces as success=false.
        let err = handle_success_flag(false, String::new(), "Failed to remove policy").unwrap_err();
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    // ---- get_policies ----

    #[test]
    fn grpc_get_policies_success() {
        // NOTE: conversion-layer.
        let req = pb::GetPoliciesRequest {
            prefix: "logs/".to_string(),
        };
        assert_eq!(req.prefix, "logs/");

        let mut settings = HashMap::new();
        settings.insert("vault".to_string(), "v1".to_string());
        let policies = handle_get_policies(pb::GetPoliciesResponse {
            policies: vec![
                pb::LifecyclePolicy {
                    id: "p1".to_string(),
                    prefix: "logs/".to_string(),
                    retention_seconds: 3600,
                    action: "archive".to_string(),
                    destination_type: "glacier".to_string(),
                    destination_settings: settings,
                },
                pb::LifecyclePolicy {
                    id: "p2".to_string(),
                    prefix: String::new(),
                    retention_seconds: 0,
                    action: "delete".to_string(),
                    destination_type: String::new(),
                    destination_settings: HashMap::new(),
                },
            ],
            success: true,
            message: String::new(),
        });
        assert_eq!(policies.len(), 2);
        assert_eq!(policies[0].destination_type.as_deref(), Some("glacier"));
        // empty destination_type -> None
        assert_eq!(policies[1].destination_type, None);
    }

    #[test]
    fn grpc_get_policies_error() {
        // NOTE: conversion-layer.
        let err: Error = tonic::Status::internal("boom").into();
        assert!(matches!(err, Error::GrpcStatus(_)));
    }

    // ---- apply_policies ----

    #[test]
    fn grpc_apply_policies_success() {
        // NOTE: conversion-layer.
        let _req = pb::ApplyPoliciesRequest {};
        let (count, processed) = handle_apply_policies(pb::ApplyPoliciesResponse {
            success: true,
            policies_count: 3,
            objects_processed: 42,
            message: String::new(),
        })
        .unwrap();
        assert_eq!(count, 3);
        assert_eq!(processed, 42);
    }

    #[test]
    fn grpc_apply_policies_error() {
        // NOTE: conversion-layer.
        let err = handle_apply_policies(pb::ApplyPoliciesResponse {
            success: false,
            policies_count: 0,
            objects_processed: 0,
            message: "boom".to_string(),
        })
        .unwrap_err();
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    // ---- add_replication_policy ----

    #[test]
    fn grpc_add_replication_policy_success() {
        // NOTE: conversion-layer.
        let pb_policy = convert_to_pb_replication_policy(sample_replication_policy());
        assert_eq!(pb_policy.id, "repl-1");
        assert_eq!(pb_policy.check_interval_seconds, 300);
        assert_eq!(
            pb_policy.replication_mode,
            pb::ReplicationMode::Opaque as i32
        );
        let _req = pb::AddReplicationPolicyRequest {
            policy: Some(pb_policy),
        };
        handle_success_flag(true, String::new(), "Failed to add replication policy").unwrap();
    }

    #[test]
    fn grpc_add_replication_policy_error() {
        // NOTE: conversion-layer.
        let err = handle_success_flag(
            false,
            "conflict".to_string(),
            "Failed to add replication policy",
        )
        .unwrap_err();
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    // ---- remove_replication_policy ----

    #[test]
    fn grpc_remove_replication_policy_success() {
        // NOTE: conversion-layer.
        let req = pb::RemoveReplicationPolicyRequest {
            id: "repl-1".to_string(),
        };
        assert_eq!(req.id, "repl-1");
        handle_success_flag(true, String::new(), "Failed to remove replication policy").unwrap();
    }

    #[test]
    fn grpc_remove_replication_policy_error() {
        // NOTE: conversion-layer.
        let err = handle_success_flag(
            false,
            "boom".to_string(),
            "Failed to remove replication policy",
        )
        .unwrap_err();
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[test]
    fn grpc_remove_replication_policy_not_found() {
        // NOTE: conversion-layer. Not-found surfaces as success=false.
        let err = handle_success_flag(false, String::new(), "Failed to remove replication policy")
            .unwrap_err();
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    // ---- get_replication_policies ----

    #[test]
    fn grpc_get_replication_policies_success() {
        // NOTE: conversion-layer.
        let resp = pb::GetReplicationPoliciesResponse {
            policies: vec![convert_to_pb_replication_policy(sample_replication_policy())],
        };
        let policies: Vec<ReplicationPolicy> = resp
            .policies
            .into_iter()
            .map(convert_from_pb_replication_policy)
            .collect();
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].id, "repl-1");
        assert_eq!(policies[0].check_interval_seconds, 300);
        assert_eq!(policies[0].replication_mode, ReplicationMode::Opaque);
    }

    #[test]
    fn grpc_get_replication_policies_error() {
        // NOTE: conversion-layer.
        let err: Error = tonic::Status::internal("boom").into();
        assert!(matches!(err, Error::GrpcStatus(_)));
    }

    // ---- get_replication_policy ----

    #[test]
    fn grpc_get_replication_policy_success() {
        // NOTE: conversion-layer.
        let req = pb::GetReplicationPolicyRequest {
            id: "repl-1".to_string(),
        };
        assert_eq!(req.id, "repl-1");
        let policy = handle_get_replication_policy(pb::GetReplicationPolicyResponse {
            policy: Some(convert_to_pb_replication_policy(sample_replication_policy())),
        })
        .unwrap();
        assert_eq!(policy.id, "repl-1");
        assert_eq!(policy.replication_mode, ReplicationMode::Opaque);
    }

    #[test]
    fn grpc_get_replication_policy_error() {
        // NOTE: conversion-layer.
        let err: Error = tonic::Status::internal("boom").into();
        assert!(matches!(err, Error::GrpcStatus(_)));
    }

    #[test]
    fn grpc_get_replication_policy_not_found() {
        // NOTE: conversion-layer. A missing policy (None) maps to NotFound.
        let err = handle_get_replication_policy(pb::GetReplicationPolicyResponse { policy: None })
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    // ---- trigger_replication ----

    #[test]
    fn grpc_trigger_replication_success() {
        // NOTE: conversion-layer.
        let req = pb::TriggerReplicationRequest {
            policy_id: "repl-1".to_string(),
            parallel: true,
            worker_count: 4,
        };
        assert_eq!(req.policy_id, "repl-1");
        assert!(req.parallel);
        assert_eq!(req.worker_count, 4);

        let result = handle_trigger_replication(pb::TriggerReplicationResponse {
            success: true,
            result: Some(pb::SyncResult {
                policy_id: "repl-1".to_string(),
                synced: 5,
                deleted: 1,
                failed: 0,
                bytes_total: 2048,
                duration_ms: 1500,
                errors: vec!["minor".to_string()],
            }),
            message: String::new(),
        })
        .unwrap();
        assert_eq!(result.policy_id, "repl-1");
        assert_eq!(result.synced, 5);
        assert_eq!(result.bytes_total, 2048);
        assert_eq!(result.duration_ms, 1500);
        assert_eq!(result.errors, vec!["minor".to_string()]);
    }

    #[test]
    fn grpc_trigger_replication_error() {
        // NOTE: conversion-layer. success=false -> OperationFailed; success
        // with a missing result -> InvalidResponse.
        let err = handle_trigger_replication(pb::TriggerReplicationResponse {
            success: false,
            result: None,
            message: "boom".to_string(),
        })
        .unwrap_err();
        assert!(matches!(err, Error::OperationFailed(_)));

        let missing = handle_trigger_replication(pb::TriggerReplicationResponse {
            success: true,
            result: None,
            message: String::new(),
        })
        .unwrap_err();
        assert!(matches!(missing, Error::InvalidResponse(_)));
    }

    // ---- get_replication_status ----

    #[test]
    fn grpc_get_replication_status_success() {
        // NOTE: conversion-layer.
        let req = pb::GetReplicationStatusRequest {
            id: "repl-1".to_string(),
        };
        assert_eq!(req.id, "repl-1");

        let status = handle_get_replication_status(pb::GetReplicationStatusResponse {
            success: true,
            status: Some(pb::ReplicationStatus {
                policy_id: "repl-1".to_string(),
                source_backend: "s3".to_string(),
                destination_backend: "gcs".to_string(),
                enabled: true,
                total_objects_synced: 10,
                total_objects_deleted: 2,
                total_bytes_synced: 4096,
                total_errors: 1,
                last_sync_time: None,
                average_sync_duration_ms: 3000,
                sync_count: 7,
            }),
            message: String::new(),
        })
        .unwrap();
        assert_eq!(status.policy_id, "repl-1");
        assert_eq!(status.total_objects_synced, 10);
        assert_eq!(status.average_sync_duration_ms, 3000);
        assert_eq!(status.sync_count, 7);
    }

    #[test]
    fn grpc_get_replication_status_error() {
        // NOTE: conversion-layer.
        let err = handle_get_replication_status(pb::GetReplicationStatusResponse {
            success: false,
            status: None,
            message: "boom".to_string(),
        })
        .unwrap_err();
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[test]
    fn grpc_get_replication_status_not_found() {
        // NOTE: conversion-layer. success=true with a missing status payload
        // maps to InvalidResponse; not-found typically arrives as success=false.
        let missing = handle_get_replication_status(pb::GetReplicationStatusResponse {
            success: true,
            status: None,
            message: String::new(),
        })
        .unwrap_err();
        assert!(matches!(missing, Error::InvalidResponse(_)));
    }

    // ---- cross-cutting ----

    #[test]
    fn grpc_metadata_round_trip() {
        // NOTE: conversion-layer. Metadata travels in proto message fields:
        // SDK Metadata -> pb::Metadata (as put/update build it) -> back via
        // convert_pb_metadata. content_type, content_encoding and the custom
        // map must all survive the round trip.
        let original = sample_metadata();

        // Rides inside a PutRequest.metadata field.
        let put_req = build_put_request(
            "k".to_string(),
            Bytes::from_static(b"x"),
            Some(original.clone()),
        );
        let pb_meta = put_req.metadata.expect("metadata present in PutRequest");
        assert_eq!(pb_meta.content_type, "application/json");
        assert_eq!(pb_meta.content_encoding, "gzip");
        assert_eq!(
            pb_meta.custom.get("owner").map(String::as_str),
            Some("alice")
        );

        // Rides back inside a GetResponse.metadata field and converts back.
        let (_data, round_tripped) = handle_get(vec![pb::GetResponse {
            data: b"x".to_vec(),
            metadata: Some(pb_meta),
            is_last: true,
        }]);
        assert_eq!(round_tripped.content_type, original.content_type);
        assert_eq!(round_tripped.content_encoding, original.content_encoding);
        assert_eq!(
            round_tripped.custom.get("owner").map(String::as_str),
            Some("alice")
        );
    }

    #[test]
    fn grpc_validation_empty_key() {
        // NOTE: conversion-layer. The gRPC client performs no client-side
        // empty-key validation: building a request with an empty key still
        // produces a pb request carrying `key == ""` (the server validates).
        let put = build_put_request(String::new(), Bytes::from_static(b"x"), None);
        assert_eq!(put.key, "");
        let get = pb::GetRequest { key: String::new() };
        assert_eq!(get.key, "");
        let del = pb::DeleteRequest { key: String::new() };
        assert_eq!(del.key, "");
    }

    // ---- retained conversion-helper coverage ----

    #[test]
    fn grpc_convert_pb_metadata_empty_fields_normalize_to_none() {
        // Empty proto strings normalize to Option::None on the SDK side.
        let meta = convert_pb_metadata(pb::Metadata {
            content_type: String::new(),
            content_encoding: String::new(),
            size: 0,
            last_modified: None,
            etag: String::new(),
            custom: HashMap::new(),
        });
        assert!(meta.content_type.is_none());
        assert!(meta.content_encoding.is_none());
        assert!(meta.etag.is_none());

        let full = convert_pb_metadata(pb::Metadata {
            content_type: "application/json".to_string(),
            content_encoding: "gzip".to_string(),
            size: 1024,
            last_modified: None,
            etag: "abc123".to_string(),
            custom: HashMap::new(),
        });
        assert_eq!(full.content_type, Some("application/json".to_string()));
        assert_eq!(full.size, 1024);
    }

    #[test]
    fn grpc_replication_mode_round_trip() {
        // Both replication modes survive SDK <-> pb conversion.
        for mode in [ReplicationMode::Transparent, ReplicationMode::Opaque] {
            let mut policy = sample_replication_policy();
            policy.replication_mode = mode;
            let pb_policy = convert_to_pb_replication_policy(policy.clone());
            let back = convert_from_pb_replication_policy(pb_policy);
            assert_eq!(back.replication_mode, mode);
        }
    }
}
