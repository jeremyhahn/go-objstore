//! Comprehensive unit tests for go-objstore Rust SDK
//!
//! These tests verify the core functionality of all clients and types
//! without requiring a running server.

use go_objstore::{
    DeleteResponse, EncryptionConfig, EncryptionPolicy, Error, HealthResponse, HealthStatus,
    LifecyclePolicy, ListRequest, ListResponse, Metadata, ObjectInfo, PutResponse, ReplicationMode,
    ReplicationPolicy, ReplicationStatus, SyncResult,
};
use std::collections::HashMap;

// =============================================================================
// Type Tests
// =============================================================================

mod metadata_tests {
    use super::*;

    #[test]
    fn test_metadata_default() {
        let metadata = Metadata::default();
        assert_eq!(metadata.content_type, None);
        assert_eq!(metadata.content_encoding, None);
        assert_eq!(metadata.size, 0);
        assert_eq!(metadata.last_modified, None);
        assert_eq!(metadata.etag, None);
        assert!(metadata.custom.is_empty());
    }

    #[test]
    fn test_metadata_with_values() {
        let mut custom = HashMap::new();
        custom.insert("key1".to_string(), "value1".to_string());
        custom.insert("key2".to_string(), "value2".to_string());

        let metadata = Metadata {
            content_type: Some("application/json".to_string()),
            content_encoding: Some("gzip".to_string()),
            size: 1024,
            last_modified: None,
            etag: Some("abc123".to_string()),
            custom,
        };

        assert_eq!(metadata.content_type, Some("application/json".to_string()));
        assert_eq!(metadata.content_encoding, Some("gzip".to_string()));
        assert_eq!(metadata.size, 1024);
        assert_eq!(metadata.etag, Some("abc123".to_string()));
        assert_eq!(metadata.custom.len(), 2);
        assert_eq!(metadata.custom.get("key1"), Some(&"value1".to_string()));
    }

    #[test]
    fn test_metadata_clone() {
        let metadata = Metadata {
            content_type: Some("text/plain".to_string()),
            content_encoding: None,
            size: 512,
            last_modified: None,
            etag: Some("def456".to_string()),
            custom: HashMap::new(),
        };

        let cloned = metadata.clone();
        assert_eq!(cloned.content_type, metadata.content_type);
        assert_eq!(cloned.size, metadata.size);
        assert_eq!(cloned.etag, metadata.etag);
    }
}

mod object_info_tests {
    use super::*;

    #[test]
    fn test_object_info_creation() {
        let metadata = Metadata {
            content_type: Some("text/plain".to_string()),
            size: 256,
            ..Default::default()
        };

        let info = ObjectInfo {
            key: "test/file.txt".to_string(),
            metadata,
        };

        assert_eq!(info.key, "test/file.txt");
        assert_eq!(info.metadata.size, 256);
    }

    #[test]
    fn test_object_info_construction() {
        let info = ObjectInfo {
            key: String::new(),
            metadata: Metadata::default(),
        };
        assert_eq!(info.key, "");
        assert_eq!(info.metadata.size, 0);
    }
}

mod list_request_tests {
    use super::*;

    #[test]
    fn test_list_request_default() {
        let req = ListRequest::default();
        assert_eq!(req.prefix, None);
        assert_eq!(req.delimiter, None);
        assert_eq!(req.max_results, None);
        assert_eq!(req.continue_from, None);
    }

    #[test]
    fn test_list_request_with_values() {
        let req = ListRequest {
            prefix: Some("test/".to_string()),
            delimiter: Some("/".to_string()),
            max_results: Some(100),
            continue_from: Some("token123".to_string()),
        };

        assert_eq!(req.prefix, Some("test/".to_string()));
        assert_eq!(req.delimiter, Some("/".to_string()));
        assert_eq!(req.max_results, Some(100));
        assert_eq!(req.continue_from, Some("token123".to_string()));
    }
}

mod list_response_tests {
    use super::*;

    #[test]
    fn test_list_response_empty() {
        let resp = ListResponse {
            objects: vec![],
            common_prefixes: vec![],
            next_token: None,
            truncated: false,
        };
        assert!(resp.objects.is_empty());
        assert!(resp.common_prefixes.is_empty());
        assert_eq!(resp.next_token, None);
        assert!(!resp.truncated);
    }

    #[test]
    fn test_list_response_with_objects() {
        let objects = vec![
            ObjectInfo {
                key: "file1.txt".to_string(),
                metadata: Metadata::default(),
            },
            ObjectInfo {
                key: "file2.txt".to_string(),
                metadata: Metadata::default(),
            },
        ];

        let resp = ListResponse {
            objects,
            common_prefixes: vec!["prefix1/".to_string(), "prefix2/".to_string()],
            next_token: Some("token456".to_string()),
            truncated: true,
        };

        assert_eq!(resp.objects.len(), 2);
        assert_eq!(resp.common_prefixes.len(), 2);
        assert_eq!(resp.next_token, Some("token456".to_string()));
        assert!(resp.truncated);
    }
}

mod put_response_tests {
    use super::*;

    #[test]
    fn test_put_response_success() {
        let resp = PutResponse {
            success: true,
            message: Some("Object created".to_string()),
            etag: Some("\"abc123\"".to_string()),
        };

        assert!(resp.success);
        assert_eq!(resp.message, Some("Object created".to_string()));
        assert_eq!(resp.etag, Some("\"abc123\"".to_string()));
    }

    #[test]
    fn test_put_response_failure() {
        let resp = PutResponse {
            success: false,
            message: Some("Storage full".to_string()),
            etag: None,
        };

        assert!(!resp.success);
        assert!(resp.etag.is_none());
    }
}

mod delete_response_tests {
    use super::*;

    #[test]
    fn test_delete_response_success() {
        let resp = DeleteResponse {
            success: true,
            message: None,
        };

        assert!(resp.success);
        assert!(resp.message.is_none());
    }

    #[test]
    fn test_delete_response_with_message() {
        let resp = DeleteResponse {
            success: true,
            message: Some("Object deleted".to_string()),
        };

        assert!(resp.success);
        assert_eq!(resp.message, Some("Object deleted".to_string()));
    }
}

mod health_response_tests {
    use super::*;

    #[test]
    fn test_health_status_serving() {
        let resp = HealthResponse {
            status: HealthStatus::Serving,
            message: Some("v1.0.0".to_string()),
        };

        assert_eq!(resp.status, HealthStatus::Serving);
    }

    #[test]
    fn test_health_status_not_serving() {
        let resp = HealthResponse {
            status: HealthStatus::NotServing,
            message: Some("Maintenance".to_string()),
        };

        assert_eq!(resp.status, HealthStatus::NotServing);
    }

    #[test]
    fn test_health_status_unknown() {
        let resp = HealthResponse {
            status: HealthStatus::Unknown,
            message: None,
        };

        assert_eq!(resp.status, HealthStatus::Unknown);
    }

    #[test]
    fn test_health_status_equality() {
        assert_eq!(HealthStatus::Serving, HealthStatus::Serving);
        assert_ne!(HealthStatus::Serving, HealthStatus::NotServing);
        assert_ne!(HealthStatus::NotServing, HealthStatus::Unknown);
    }
}

mod lifecycle_policy_tests {
    use super::*;

    #[test]
    fn test_lifecycle_policy_creation() {
        let mut settings = HashMap::new();
        settings.insert("bucket".to_string(), "archive-bucket".to_string());

        let policy = LifecyclePolicy {
            id: "policy-1".to_string(),
            prefix: "logs/".to_string(),
            retention_seconds: 86400,
            action: "delete".to_string(),
            destination_type: Some("glacier".to_string()),
            destination_settings: settings,
        };

        assert_eq!(policy.id, "policy-1");
        assert_eq!(policy.prefix, "logs/");
        assert_eq!(policy.retention_seconds, 86400);
        assert_eq!(policy.action, "delete");
        assert_eq!(policy.destination_type, Some("glacier".to_string()));
    }

    #[test]
    fn test_lifecycle_policy_without_destination() {
        let policy = LifecyclePolicy {
            id: "policy-2".to_string(),
            prefix: "temp/".to_string(),
            retention_seconds: 3600,
            action: "delete".to_string(),
            destination_type: None,
            destination_settings: HashMap::new(),
        };

        assert!(policy.destination_type.is_none());
        assert!(policy.destination_settings.is_empty());
    }
}

mod replication_policy_tests {
    use super::*;

    #[test]
    fn test_replication_policy_creation() {
        let policy = ReplicationPolicy {
            id: "repl-1".to_string(),
            source_backend: "s3".to_string(),
            source_settings: HashMap::new(),
            source_prefix: "data/".to_string(),
            destination_backend: "gcs".to_string(),
            destination_settings: HashMap::new(),
            check_interval_seconds: 300,
            last_sync_time: None,
            enabled: true,
            encryption: None,
            replication_mode: ReplicationMode::Transparent,
        };

        assert_eq!(policy.id, "repl-1");
        assert_eq!(policy.source_backend, "s3");
        assert_eq!(policy.destination_backend, "gcs");
        assert!(policy.enabled);
        assert_eq!(policy.replication_mode, ReplicationMode::Transparent);
    }

    #[test]
    fn test_replication_mode_values() {
        assert_eq!(ReplicationMode::Transparent, ReplicationMode::Transparent);
        assert_eq!(ReplicationMode::Opaque, ReplicationMode::Opaque);
        assert_ne!(ReplicationMode::Transparent, ReplicationMode::Opaque);
    }

    #[test]
    fn test_replication_policy_with_encryption() {
        let encryption = EncryptionPolicy {
            backend: Some(EncryptionConfig {
                enabled: true,
                provider: "aes-gcm".to_string(),
                default_key: "key-id-1".to_string(),
            }),
            source: None,
            destination: Some(EncryptionConfig {
                enabled: true,
                provider: "kms".to_string(),
                default_key: "dest-key".to_string(),
            }),
        };

        let policy = ReplicationPolicy {
            id: "repl-encrypted".to_string(),
            source_backend: "local".to_string(),
            source_settings: HashMap::new(),
            source_prefix: "".to_string(),
            destination_backend: "s3".to_string(),
            destination_settings: HashMap::new(),
            check_interval_seconds: 600,
            last_sync_time: None,
            enabled: true,
            encryption: Some(encryption),
            replication_mode: ReplicationMode::Opaque,
        };

        assert!(policy.encryption.is_some());
        let enc = policy.encryption.unwrap();
        assert!(enc.backend.is_some());
        assert!(enc.source.is_none());
        assert!(enc.destination.is_some());
    }
}

mod encryption_config_tests {
    use super::*;

    #[test]
    fn test_encryption_config() {
        let config = EncryptionConfig {
            enabled: true,
            provider: "aws-kms".to_string(),
            default_key: "arn:aws:kms:us-east-1:123456789:key/abc".to_string(),
        };

        assert!(config.enabled);
        assert_eq!(config.provider, "aws-kms");
        assert!(!config.default_key.is_empty());
    }

    #[test]
    fn test_encryption_policy() {
        let policy = EncryptionPolicy {
            backend: Some(EncryptionConfig {
                enabled: true,
                provider: "builtin".to_string(),
                default_key: "master-key".to_string(),
            }),
            source: None,
            destination: None,
        };

        assert!(policy.backend.is_some());
        assert!(policy.source.is_none());
        assert!(policy.destination.is_none());
    }
}

mod replication_status_tests {
    use super::*;

    #[test]
    fn test_replication_status() {
        let status = ReplicationStatus {
            policy_id: "repl-1".to_string(),
            source_backend: "s3".to_string(),
            destination_backend: "gcs".to_string(),
            enabled: true,
            total_objects_synced: 1000,
            total_objects_deleted: 50,
            total_bytes_synced: 1048576000,
            total_errors: 3,
            last_sync_time: None,
            average_sync_duration_ms: 5000,
            sync_count: 100,
        };

        assert_eq!(status.policy_id, "repl-1");
        assert_eq!(status.total_objects_synced, 1000);
        assert_eq!(status.total_objects_deleted, 50);
        assert_eq!(status.total_bytes_synced, 1048576000);
        assert_eq!(status.total_errors, 3);
        assert_eq!(status.sync_count, 100);
    }
}

mod sync_result_tests {
    use super::*;

    #[test]
    fn test_sync_result() {
        let result = SyncResult {
            policy_id: "repl-1".to_string(),
            synced: 50,
            deleted: 5,
            failed: 2,
            bytes_total: 1048576,
            duration_ms: 3000,
            errors: vec!["Error 1".to_string(), "Error 2".to_string()],
        };

        assert_eq!(result.synced, 50);
        assert_eq!(result.deleted, 5);
        assert_eq!(result.failed, 2);
        assert_eq!(result.bytes_total, 1048576);
        assert_eq!(result.duration_ms, 3000);
        assert_eq!(result.errors.len(), 2);
    }
}

mod error_tests {
    use super::*;

    #[test]
    fn test_not_found_error() {
        let err = Error::NotFound("missing-key".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("missing-key") || msg.contains("not found"));
    }

    #[test]
    fn test_operation_failed_error() {
        let err = Error::OperationFailed("Storage full".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("Storage full") || msg.contains("failed"));
    }

    #[test]
    fn test_configuration_error() {
        let err = Error::Configuration("Invalid endpoint".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("Invalid endpoint") || msg.contains("configuration"));
    }

    #[test]
    fn test_error_display() {
        let errors = vec![
            Error::NotFound("key".to_string()),
            Error::OperationFailed("msg".to_string()),
            Error::Configuration("config".to_string()),
            Error::InvalidResponse("response".to_string()),
        ];

        for err in errors {
            // Just verify Display trait works
            let _ = format!("{}", err);
        }
    }
}

// =============================================================================
// Client Constructor Tests (without network)
// =============================================================================

mod rest_client_tests {
    use go_objstore::RestClient;

    #[test]
    fn test_rest_client_new() {
        let result = RestClient::new("http://localhost:8080");
        assert!(result.is_ok());
    }

    #[test]
    fn test_rest_client_new_with_https() {
        let result = RestClient::new("https://localhost:8080");
        assert!(result.is_ok());
    }

    #[test]
    fn test_url_encoding() {
        let key = "path/to/file with spaces.txt";
        let encoded = urlencoding::encode(key);
        assert!(encoded.contains("%20"));
        assert!(encoded.contains("%2F"));
    }
}

// =============================================================================
// Serialization/Deserialization Tests
// =============================================================================

mod serialization_tests {
    use super::*;

    #[test]
    fn test_metadata_serialization() {
        let metadata = Metadata {
            content_type: Some("application/json".to_string()),
            content_encoding: None,
            size: 1024,
            last_modified: None,
            etag: Some("abc".to_string()),
            custom: HashMap::new(),
        };

        // Test that we can serialize to JSON
        let json = serde_json::to_string(&metadata).unwrap();
        assert!(json.contains("application/json"));
        assert!(json.contains("1024"));
    }

    #[test]
    fn test_metadata_deserialization() {
        let json = r#"{
            "content_type": "text/plain",
            "size": 512,
            "etag": "xyz",
            "custom": {}
        }"#;

        let metadata: Metadata = serde_json::from_str(json).unwrap();
        assert_eq!(metadata.content_type, Some("text/plain".to_string()));
        assert_eq!(metadata.size, 512);
    }

    #[test]
    fn test_list_response_serialization() {
        let resp = ListResponse {
            objects: vec![ObjectInfo {
                key: "test.txt".to_string(),
                metadata: Metadata::default(),
            }],
            common_prefixes: vec!["prefix/".to_string()],
            next_token: None,
            truncated: false,
        };

        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("test.txt"));
        assert!(json.contains("prefix/"));
    }

    #[test]
    fn test_replication_policy_serialization() {
        let policy = ReplicationPolicy {
            id: "pol-1".to_string(),
            source_backend: "s3".to_string(),
            source_settings: HashMap::new(),
            source_prefix: "".to_string(),
            destination_backend: "gcs".to_string(),
            destination_settings: HashMap::new(),
            check_interval_seconds: 300,
            last_sync_time: None,
            enabled: true,
            encryption: None,
            replication_mode: ReplicationMode::Transparent,
        };

        let json = serde_json::to_string(&policy).unwrap();
        assert!(json.contains("pol-1"));
        assert!(json.contains("s3"));
        assert!(json.contains("gcs"));
    }
}

// =============================================================================
// Clone and Default Trait Tests
// =============================================================================

mod trait_tests {
    use super::*;

    #[test]
    fn test_types_implement_clone() {
        let metadata = Metadata::default();
        let _ = metadata.clone();

        let info = ObjectInfo {
            key: "test".to_string(),
            metadata: Metadata::default(),
        };
        let _ = info.clone();

        let req = ListRequest::default();
        let _ = req.clone();

        let resp = ListResponse {
            objects: vec![],
            common_prefixes: vec![],
            next_token: None,
            truncated: false,
        };
        let _ = resp.clone();

        let put = PutResponse {
            success: true,
            message: None,
            etag: None,
        };
        let _ = put.clone();

        let del = DeleteResponse {
            success: true,
            message: None,
        };
        let _ = del.clone();
    }

    #[test]
    fn test_types_implement_debug() {
        let metadata = Metadata::default();
        let _ = format!("{:?}", metadata);

        let status = HealthStatus::Serving;
        let _ = format!("{:?}", status);

        let mode = ReplicationMode::Transparent;
        let _ = format!("{:?}", mode);
    }
}

// =============================================================================
// Edge Case Tests
// =============================================================================

mod edge_case_tests {
    use super::*;

    #[test]
    fn test_empty_key() {
        let info = ObjectInfo {
            key: "".to_string(),
            metadata: Metadata::default(),
        };
        assert!(info.key.is_empty());
    }

    #[test]
    fn test_special_characters_in_key() {
        let info = ObjectInfo {
            key: "path/to/file with spaces & special!@#$%^chars.txt".to_string(),
            metadata: Metadata::default(),
        };
        assert!(info.key.contains("&"));
        assert!(info.key.contains("@"));
    }

    #[test]
    fn test_unicode_in_key() {
        let info = ObjectInfo {
            key: "путь/к/файлу/文件.txt".to_string(),
            metadata: Metadata::default(),
        };
        assert!(info.key.contains("файлу"));
        assert!(info.key.contains("文件"));
    }

    #[test]
    fn test_large_metadata_custom_map() {
        let mut custom = HashMap::new();
        for i in 0..1000 {
            custom.insert(format!("key-{}", i), format!("value-{}", i));
        }

        let metadata = Metadata {
            custom,
            ..Default::default()
        };

        assert_eq!(metadata.custom.len(), 1000);
    }

    #[test]
    fn test_zero_size_object() {
        let metadata = Metadata {
            size: 0,
            ..Default::default()
        };
        assert_eq!(metadata.size, 0);
    }

    #[test]
    fn test_large_size_object() {
        let metadata = Metadata {
            size: i64::MAX,
            ..Default::default()
        };
        assert_eq!(metadata.size, i64::MAX);
    }

    #[test]
    fn test_empty_list_response() {
        let resp = ListResponse {
            objects: vec![],
            common_prefixes: vec![],
            next_token: None,
            truncated: false,
        };

        assert!(resp.objects.is_empty());
        assert!(!resp.truncated);
    }

    #[test]
    fn test_truncated_list_response() {
        let resp = ListResponse {
            objects: vec![ObjectInfo {
                key: "test".to_string(),
                metadata: Metadata::default(),
            }],
            common_prefixes: vec![],
            next_token: Some("next-page-token".to_string()),
            truncated: true,
        };

        assert!(resp.truncated);
        assert!(resp.next_token.is_some());
    }
}
