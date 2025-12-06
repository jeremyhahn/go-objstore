use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Metadata for an object in storage
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Metadata {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub size: i64,
    pub last_modified: Option<DateTime<Utc>>,
    pub etag: Option<String>,
    pub custom: HashMap<String, String>,
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            content_type: None,
            content_encoding: None,
            size: 0,
            last_modified: None,
            etag: None,
            custom: HashMap::new(),
        }
    }
}

/// Information about a stored object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectInfo {
    pub key: String,
    pub metadata: Metadata,
}

/// Response from a Put operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutResponse {
    pub success: bool,
    pub message: Option<String>,
    pub etag: Option<String>,
}

/// Response from a Delete operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub success: bool,
    pub message: Option<String>,
}

/// Request for listing objects
#[derive(Debug, Clone, Default)]
pub struct ListRequest {
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub max_results: Option<i32>,
    pub continue_from: Option<String>,
}

/// Response from a List operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListResponse {
    pub objects: Vec<ObjectInfo>,
    pub common_prefixes: Vec<String>,
    pub next_token: Option<String>,
    pub truncated: bool,
}

/// Health check status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    Unknown,
    Serving,
    NotServing,
}

/// Response from a Health check
#[derive(Debug, Clone)]
pub struct HealthResponse {
    pub status: HealthStatus,
    pub message: Option<String>,
}

/// Lifecycle policy for objects
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecyclePolicy {
    pub id: String,
    pub prefix: String,
    pub retention_seconds: i64,
    pub action: String,
    pub destination_type: Option<String>,
    pub destination_settings: HashMap<String, String>,
}

/// Encryption configuration for a layer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    pub enabled: bool,
    pub provider: String,
    pub default_key: String,
}

/// Encryption policy for all layers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionPolicy {
    pub backend: Option<EncryptionConfig>,
    pub source: Option<EncryptionConfig>,
    pub destination: Option<EncryptionConfig>,
}

/// Replication mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplicationMode {
    Transparent,
    Opaque,
}

/// Replication policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationPolicy {
    pub id: String,
    pub source_backend: String,
    pub source_settings: HashMap<String, String>,
    pub source_prefix: String,
    pub destination_backend: String,
    pub destination_settings: HashMap<String, String>,
    pub check_interval_seconds: i64,
    pub last_sync_time: Option<DateTime<Utc>>,
    pub enabled: bool,
    pub encryption: Option<EncryptionPolicy>,
    pub replication_mode: ReplicationMode,
}

/// Sync result for replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    pub policy_id: String,
    pub synced: i32,
    pub deleted: i32,
    pub failed: i32,
    pub bytes_total: i64,
    pub duration_ms: i64,
    pub errors: Vec<String>,
}

/// Replication status and metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationStatus {
    pub policy_id: String,
    pub source_backend: String,
    pub destination_backend: String,
    pub enabled: bool,
    pub total_objects_synced: i64,
    pub total_objects_deleted: i64,
    pub total_bytes_synced: i64,
    pub total_errors: i64,
    pub last_sync_time: Option<DateTime<Utc>>,
    pub average_sync_duration_ms: i64,
    pub sync_count: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_default() {
        let metadata = Metadata::default();
        assert_eq!(metadata.size, 0);
        assert!(metadata.custom.is_empty());
    }

    #[test]
    fn test_metadata_serialization() {
        let mut metadata = Metadata::default();
        metadata.content_type = Some("application/json".to_string());
        metadata.custom.insert("key".to_string(), "value".to_string());

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: Metadata = serde_json::from_str(&json).unwrap();
        assert_eq!(metadata, deserialized);
    }

    #[test]
    fn test_list_request_default() {
        let req = ListRequest::default();
        assert!(req.prefix.is_none());
        assert!(req.max_results.is_none());
    }

    #[test]
    fn test_health_status() {
        assert_eq!(HealthStatus::Serving, HealthStatus::Serving);
        assert_ne!(HealthStatus::Serving, HealthStatus::NotServing);
    }

    #[test]
    fn test_replication_mode() {
        let mode = ReplicationMode::Transparent;
        let json = serde_json::to_string(&mode).unwrap();
        let deserialized: ReplicationMode = serde_json::from_str(&json).unwrap();
        assert_eq!(mode, deserialized);
    }
}
