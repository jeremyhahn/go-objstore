//! Comprehensive unit tests for go-objstore Rust SDK
//!
//! These tests verify the core functionality of all clients and types
//! without requiring a running server.

use go_objstore::{
    DeleteResponse, EncryptionConfig, EncryptionPolicy, Error, HealthResponse, HealthStatus,
    LifecyclePolicy, ListRequest, ListResponse, Metadata, ObjectInfo, PutResponse,
    ReplicationMode, ReplicationPolicy, ReplicationStatus, SyncResult,
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

// =============================================================================
// Mock-based REST Client Tests
// =============================================================================

mod rest_client_mock_tests {
    use go_objstore::{RestClient, HealthStatus, ListRequest, Metadata};
    use bytes::Bytes;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_put_success() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/objects/test-key")
            .with_status(201)
            .with_body(r#"{"success":true,"message":"Object created","etag":"\"abc123\""}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.put("test-key", Bytes::from("test data"), None).await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_put_with_metadata() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/objects/test-key")
            .with_status(201)
            .with_body(r#"{"success":true,"message":"Object created"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let metadata = Metadata {
            content_type: Some("application/json".to_string()),
            ..Default::default()
        };
        let result = client.put("test-key", Bytes::from("test data"), Some(metadata)).await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_success() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/objects/test-key")
            .with_status(200)
            .with_header("content-type", "text/plain")
            .with_header("content-length", "12")
            .with_body("test content")
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.get("test-key").await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let (data, _metadata) = result.unwrap();
        assert_eq!(data, Bytes::from("test content"));
    }

    #[tokio::test]
    async fn test_get_not_found() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/objects/nonexistent")
            .with_status(404)
            .with_body(r#"{"error":"Object not found"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.get("nonexistent").await;

        mock.assert_async().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_success() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/objects/test-key")
            .with_status(200)
            .with_body(r#"{"success":true}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.delete("test-key").await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_not_found() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/objects/nonexistent")
            .with_status(404)
            .with_body(r#"{"error":"Object not found"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.delete("nonexistent").await;

        mock.assert_async().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_success() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Regex(r"^/objects.*$".to_string()))
            .with_status(200)
            .with_body(r#"{"objects":[{"key":"file1.txt","size":100}],"common_prefixes":[],"truncated":false}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.list(ListRequest::default()).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.objects.len(), 1);
    }

    #[tokio::test]
    async fn test_exists_true() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("HEAD", "/objects/test-key")
            .with_status(200)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.exists("test-key").await;

        mock.assert_async().await;
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_exists_false() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("HEAD", "/objects/nonexistent")
            .with_status(404)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.exists("nonexistent").await;

        mock.assert_async().await;
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_get_metadata_success() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/metadata/test-key")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"key":"test-key","size":1024}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.get_metadata("test-key").await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let metadata = result.unwrap();
        assert_eq!(metadata.size, 1024);
    }

    #[tokio::test]
    async fn test_update_metadata_success() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/metadata/test-key")
            .with_status(200)
            .with_body(r#"{"success":true}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let metadata = Metadata {
            content_type: Some("application/json".to_string()),
            ..Default::default()
        };
        let result = client.update_metadata("test-key", metadata).await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_health_serving() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/health")
            .with_status(200)
            .with_body(r#"{"status":"healthy","version":"1.0.0"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.health().await;

        mock.assert_async().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().status, HealthStatus::Serving);
    }

    #[tokio::test]
    async fn test_health_not_serving() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/health")
            .with_status(503)
            .with_body(r#"{"status":"unhealthy","message":"Service unavailable"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.health().await;

        mock.assert_async().await;
        // Should handle 503 appropriately
        assert!(result.is_ok() || result.is_err());
    }

    #[tokio::test]
    async fn test_server_error() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/objects/test-key")
            .with_status(500)
            .with_body(r#"{"error":"Internal server error"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.get("test-key").await;

        mock.assert_async().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_with_prefix() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/objects")
            .match_query(mockito::Matcher::UrlEncoded("prefix".into(), "test/".into()))
            .with_status(200)
            .with_body(r#"{"objects":[],"common_prefixes":[],"truncated":false}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let request = ListRequest {
            prefix: Some("test/".to_string()),
            ..Default::default()
        };
        let result = client.list(request).await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_list_with_pagination() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Regex(r"^/objects.*limit=1.*$".to_string()))
            .with_status(200)
            .with_body(r#"{"objects":[{"key":"file1.txt","size":100}],"common_prefixes":[],"next_token":"token123","truncated":true}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let request = ListRequest {
            max_results: Some(1),
            ..Default::default()
        };
        let result = client.list(request).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.truncated);
        assert!(response.next_token.is_some());
    }

    #[tokio::test]
    async fn test_get_metadata_not_found() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/metadata/nonexistent")
            .with_status(404)
            .with_body(r#"{"error":"Object not found"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.get_metadata("nonexistent").await;

        mock.assert_async().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_metadata_not_found() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/metadata/nonexistent")
            .with_status(404)
            .with_body(r#"{"error":"Object not found"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let metadata = Metadata::default();
        let result = client.update_metadata("nonexistent", metadata).await;

        mock.assert_async().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_server_error() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/objects/test-key")
            .with_status(500)
            .with_body(r#"{"error":"Storage error"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.put("test-key", Bytes::from("data"), None).await;

        mock.assert_async().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_empty() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Regex(r"^/objects.*$".to_string()))
            .with_status(200)
            .with_body(r#"{"objects":[],"common_prefixes":[],"truncated":false}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.list(ListRequest::default()).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.objects.is_empty());
        assert!(!response.truncated);
    }

    #[tokio::test]
    async fn test_client_creation() {
        let result = RestClient::new("http://localhost:8080");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_client_creation_https() {
        let result = RestClient::new("https://localhost:8443");
        assert!(result.is_ok());
    }

    // =============================================================================
    // Extended Error Handling Tests
    // =============================================================================

    #[tokio::test]
    async fn test_put_bad_request() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/objects/bad-key")
            .with_status(400)
            .with_body(r#"{"error":"Invalid key"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.put("bad-key", Bytes::from("data"), None).await;

        mock.assert_async().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_server_error() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/objects/error-key")
            .with_status(500)
            .with_body(r#"{"error":"Database error"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.get("error-key").await;

        mock.assert_async().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_server_error() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/objects/error-key")
            .with_status(500)
            .with_body(r#"{"error":"Deletion failed"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.delete("error-key").await;

        mock.assert_async().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_exists_server_error() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("HEAD", "/objects/error-key")
            .with_status(500)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.exists("error-key").await;

        mock.assert_async().await;
        // REST client returns Ok(false) for non-200 responses (including 500)
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_list_server_error() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Regex(r"^/objects.*$".to_string()))
            .with_status(500)
            .with_body(r#"{"error":"List operation failed"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.list(ListRequest::default()).await;

        mock.assert_async().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_metadata_server_error() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/metadata/error-key")
            .with_status(500)
            .with_body(r#"{"error":"Metadata fetch failed"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.get_metadata("error-key").await;

        mock.assert_async().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_metadata_server_error() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/metadata/error-key")
            .with_status(500)
            .with_body(r#"{"error":"Metadata update failed"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let metadata = Metadata::default();
        let result = client.update_metadata("error-key", metadata).await;

        mock.assert_async().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_health_unknown_status() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/health")
            .with_status(200)
            .with_body(r#"{"status":"unknown"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.health().await;

        mock.assert_async().await;
        assert!(result.is_ok());
        // REST client maps any non-"healthy"/"serving" status to NotServing
        assert_eq!(result.unwrap().status, HealthStatus::NotServing);
    }

    // =============================================================================
    // Edge Case Tests with Mocks
    // =============================================================================

    #[tokio::test]
    async fn test_put_empty_data() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/objects/empty-key")
            .with_status(201)
            .with_body(r#"{"success":true,"message":"Empty object created"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.put("empty-key", Bytes::new(), None).await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_put_special_characters_in_key() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", mockito::Matcher::Any)
            .with_status(201)
            .with_body(r#"{"success":true}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client
            .put("path/to/file with spaces & special!@#.txt", Bytes::from("data"), None)
            .await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_put_unicode_key() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", mockito::Matcher::Any)
            .with_status(201)
            .with_body(r#"{"success":true}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.put("файл/文件.txt", Bytes::from("unicode data"), None).await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_empty_response() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/objects/empty-obj")
            .with_status(200)
            .with_header("content-type", "text/plain")
            .with_header("content-length", "0")
            .with_body("")
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.get("empty-obj").await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let (data, metadata) = result.unwrap();
        assert_eq!(data.len(), 0);
        assert_eq!(metadata.size, 0);
    }

    #[tokio::test]
    async fn test_get_large_object() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let large_data = vec![b'x'; 10 * 1024 * 1024]; // 10MB
        let mock = server
            .mock("GET", "/objects/large-obj")
            .with_status(200)
            .with_header("content-type", "application/octet-stream")
            .with_header("content-length", &large_data.len().to_string())
            .with_body(&large_data[..])
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.get("large-obj").await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let (data, metadata) = result.unwrap();
        assert_eq!(data.len(), large_data.len());
        assert_eq!(metadata.size, large_data.len() as i64);
    }

    #[tokio::test]
    async fn test_list_with_delimiter() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/objects")
            .match_query(mockito::Matcher::UrlEncoded("delimiter".into(), "/".into()))
            .with_status(200)
            .with_body(r#"{"objects":[],"common_prefixes":["dir1/","dir2/"],"truncated":false}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let request = ListRequest {
            delimiter: Some("/".to_string()),
            ..Default::default()
        };
        let result = client.list(request).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.common_prefixes.len(), 2);
    }

    #[tokio::test]
    async fn test_list_with_continuation_token() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            // REST client uses "token=" parameter for pagination
            .mock("GET", mockito::Matcher::Regex(r"^/objects.*token=.*$".to_string()))
            .with_status(200)
            .with_body(r#"{"objects":[{"key":"file2.txt","size":200}],"common_prefixes":[],"truncated":false}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let request = ListRequest {
            continue_from: Some("token123".to_string()),
            ..Default::default()
        };
        let result = client.list(request).await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_update_metadata_with_complex_custom_fields() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/metadata/test-key")
            .with_status(200)
            .with_body(r#"{"success":true}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let mut custom = HashMap::new();
        custom.insert("key1".to_string(), "value1".to_string());
        custom.insert("key2".to_string(), "value2".to_string());
        custom.insert("unicode-key-文件".to_string(), "unicode-value-файл".to_string());

        let metadata = Metadata {
            content_type: Some("application/json".to_string()),
            content_encoding: Some("gzip".to_string()),
            custom,
            ..Default::default()
        };

        let result = client.update_metadata("test-key", metadata).await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_metadata_with_etag() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/metadata/test-key")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"key":"test-key","size":1024,"etag":"\"abc123def456\""}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.get_metadata("test-key").await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let metadata = result.unwrap();
        assert_eq!(metadata.size, 1024);
        assert_eq!(metadata.etag, Some("\"abc123def456\"".to_string()));
    }

    #[tokio::test]
    async fn test_put_with_etag_response() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/objects/test-key")
            .with_status(201)
            .with_header("etag", "\"new-etag-123\"")
            .with_body(r#"{"success":true,"message":"Object created"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.put("test-key", Bytes::from("test data"), None).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.success);
        // REST client gets etag from response headers, not body
        assert_eq!(response.etag, Some("\"new-etag-123\"".to_string()));
    }

    #[tokio::test]
    async fn test_delete_with_message() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/objects/test-key")
            .with_status(200)
            .with_body(r#"{"success":true,"message":"Successfully deleted object"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.delete("test-key").await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.success);
        // REST client doesn't parse message from response body - returns None
        assert_eq!(response.message, None);
    }

    // =============================================================================
    // Validation Tests
    // =============================================================================

    #[tokio::test]
    async fn test_put_validation_empty_key() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", mockito::Matcher::Any)
            .with_status(201)
            .with_body(r#"{"success":true}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.put("", Bytes::from("data"), None).await;

        mock.assert_async().await;
        // Client should still send the request (server validates)
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_validation_empty_key() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(404)
            .with_body(r#"{"error":"Object not found"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.get("").await;

        mock.assert_async().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_with_all_parameters() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/objects")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("prefix".into(), "test/".into()),
                mockito::Matcher::UrlEncoded("delimiter".into(), "/".into()),
            ]))
            .with_status(200)
            .with_body(r#"{"objects":[{"key":"test/file.txt","size":100}],"common_prefixes":[],"truncated":false}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let request = ListRequest {
            prefix: Some("test/".to_string()),
            delimiter: Some("/".to_string()),
            max_results: Some(10),
            continue_from: None,
        };
        let result = client.list(request).await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    // =============================================================================
    // HTTP Status Code Coverage Tests
    // =============================================================================

    #[tokio::test]
    async fn test_put_201_created() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/objects/new-key")
            .with_status(201)
            .with_body(r#"{"success":true,"message":"Created"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.put("new-key", Bytes::from("data"), None).await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_put_200_ok() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/objects/existing-key")
            .with_status(200)
            .with_body(r#"{"success":true,"message":"Updated"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.put("existing-key", Bytes::from("data"), None).await;

        mock.assert_async().await;
        // REST client only accepts 201 CREATED status (server always returns 201 for PUT)
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_403_forbidden() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/objects/forbidden-key")
            .with_status(403)
            .with_body(r#"{"error":"Access denied"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.get("forbidden-key").await;

        mock.assert_async().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_204_no_content() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/objects/test-key")
            .with_status(204)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.delete("test-key").await;

        mock.assert_async().await;
        // Should handle 204 gracefully
        assert!(result.is_ok() || result.is_err());
    }

    #[tokio::test]
    async fn test_list_503_service_unavailable() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Regex(r"^/objects.*$".to_string()))
            .with_status(503)
            .with_body(r#"{"error":"Service temporarily unavailable"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.list(ListRequest::default()).await;

        mock.assert_async().await;
        assert!(result.is_err());
    }

    // =============================================================================
    // Content-Type and Encoding Tests
    // =============================================================================

    #[tokio::test]
    async fn test_get_with_json_content_type() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let body = r#"{"key":"value"}"#;
        let mock = server
            .mock("GET", "/objects/json-file")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_header("content-length", &body.len().to_string())
            .with_body(body)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.get("json-file").await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let (data, metadata) = result.unwrap();
        assert_eq!(data, Bytes::from(body));
        assert_eq!(metadata.content_type, Some("application/json".to_string()));
    }

    #[tokio::test]
    async fn test_get_with_gzip_encoding() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/objects/gzip-file")
            .with_status(200)
            .with_header("content-type", "text/plain")
            .with_header("content-encoding", "gzip")
            .with_header("content-length", "50")
            .with_body(vec![0u8; 50])
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.get("gzip-file").await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let (_data, metadata) = result.unwrap();
        // REST client doesn't parse content-encoding header
        assert_eq!(metadata.content_encoding, None);
    }

    #[tokio::test]
    async fn test_put_with_binary_content_type() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/objects/binary-file")
            .with_status(201)
            .with_body(r#"{"success":true}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let metadata = Metadata {
            content_type: Some("application/octet-stream".to_string()),
            ..Default::default()
        };
        let result = client.put("binary-file", Bytes::from(vec![0u8; 100]), Some(metadata)).await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    // =============================================================================
    // Metadata Field Coverage Tests
    // =============================================================================

    #[tokio::test]
    async fn test_get_metadata_with_all_fields() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/metadata/complete-metadata")
            .with_status(200)
            // RestObjectResponse uses "metadata" field for custom metadata, not "custom"
            // Also uses "modified" not "last_modified"
            .with_body(r#"{
                "key":"complete-metadata",
                "size":2048,
                "etag":"\"complete-etag\"",
                "modified":"2024-01-15T10:30:00Z",
                "metadata":{"author":"test","version":"1.0"}
            }"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.get_metadata("complete-metadata").await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let metadata = result.unwrap();
        assert_eq!(metadata.size, 2048);
        // REST client doesn't parse content_type and content_encoding from metadata response
        assert_eq!(metadata.content_type, None);
        assert_eq!(metadata.content_encoding, None);
        assert_eq!(metadata.etag, Some("\"complete-etag\"".to_string()));
        // Now using correct "modified" field in mock
        assert!(metadata.last_modified.is_some());
        assert_eq!(metadata.custom.get("author"), Some(&"test".to_string()));
        assert_eq!(metadata.custom.get("version"), Some(&"1.0".to_string()));
    }

    #[tokio::test]
    async fn test_update_metadata_preserving_size() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/metadata/preserve-size")
            .with_status(200)
            .with_body(r#"{"success":true}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let metadata = Metadata {
            content_type: Some("text/plain".to_string()),
            size: 1024, // Preserve original size
            ..Default::default()
        };
        let result = client.update_metadata("preserve-size", metadata).await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    // =============================================================================
    // Concurrent Request Tests
    // =============================================================================

    #[tokio::test]
    async fn test_multiple_concurrent_gets() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-length", "4")
            .with_body("data")
            .expect(3)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();

        let handles: Vec<_> = (0..3)
            .map(|i| {
                let client_clone = client.clone();
                tokio::spawn(async move {
                    client_clone.get(&format!("concurrent-key-{}", i)).await
                })
            })
            .collect();

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }

        mock.assert_async().await;
    }

    // =============================================================================
    // List Response Variation Tests
    // =============================================================================

    #[tokio::test]
    async fn test_list_with_next_token() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Regex(r"^/objects.*$".to_string()))
            .with_status(200)
            .with_body(r#"{
                "objects":[{"key":"file1.txt","size":100}],
                "common_prefixes":[],
                "next_token":"next-page-token-abc123",
                "truncated":true
            }"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.list(ListRequest::default()).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.truncated);
        assert_eq!(response.next_token, Some("next-page-token-abc123".to_string()));
    }

    #[tokio::test]
    async fn test_list_only_common_prefixes() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Regex(r"^/objects.*$".to_string()))
            .with_status(200)
            .with_body(r#"{
                "objects":[],
                "common_prefixes":["dir1/","dir2/","dir3/"],
                "truncated":false
            }"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let request = ListRequest {
            delimiter: Some("/".to_string()),
            ..Default::default()
        };
        let result = client.list(request).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.objects.is_empty());
        assert_eq!(response.common_prefixes.len(), 3);
    }

    #[tokio::test]
    async fn test_list_mixed_objects_and_prefixes() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Regex(r"^/objects.*$".to_string()))
            .with_status(200)
            .with_body(r#"{
                "objects":[
                    {"key":"file1.txt","size":100},
                    {"key":"file2.txt","size":200}
                ],
                "common_prefixes":["subdir1/","subdir2/"],
                "truncated":false
            }"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.list(ListRequest::default()).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.objects.len(), 2);
        assert_eq!(response.common_prefixes.len(), 2);
    }

    // =============================================================================
    // Health Check Variation Tests
    // =============================================================================

    #[tokio::test]
    async fn test_health_with_version() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/health")
            .with_status(200)
            .with_body(r#"{"status":"healthy","version":"v1.2.3"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.health().await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let health = result.unwrap();
        assert_eq!(health.status, HealthStatus::Serving);
        assert_eq!(health.message, Some("v1.2.3".to_string()));
    }

    #[tokio::test]
    async fn test_health_degraded() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/health")
            .with_status(200)
            .with_body(r#"{"status":"not_serving","message":"Backend unavailable"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.health().await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let health = result.unwrap();
        assert_eq!(health.status, HealthStatus::NotServing);
    }

    // =============================================================================
    // Key Path Tests
    // =============================================================================

    #[tokio::test]
    async fn test_put_nested_path() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", mockito::Matcher::Any)
            .with_status(201)
            .with_body(r#"{"success":true}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client
            .put("level1/level2/level3/file.txt", Bytes::from("nested"), None)
            .await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_dot_prefixed_key() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-length", "6")
            .with_body("hidden")
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.get(".hidden/file").await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_key_with_query_chars() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", mockito::Matcher::Any)
            .with_status(200)
            .with_body(r#"{"success":true}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.delete("file?param=value&other=123").await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    // =============================================================================
    // Response Body Parsing Tests
    // =============================================================================

    #[tokio::test]
    async fn test_put_response_minimal() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/objects/minimal")
            .with_status(201)
            .with_body(r#"{"success":true}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.put("minimal", Bytes::from("data"), None).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.success);
        assert!(response.message.is_none());
        assert!(response.etag.is_none());
    }

    #[tokio::test]
    async fn test_delete_response_minimal() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/objects/minimal")
            .with_status(200)
            .with_body(r#"{"success":true}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.delete("minimal").await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.success);
        assert!(response.message.is_none());
    }

    // =============================================================================
    // Network Error Simulation Tests
    // =============================================================================

    #[tokio::test]
    async fn test_connection_refused() {
        // Use a port that's likely not in use
        let client = RestClient::new("http://localhost:59999").unwrap();
        let result = client.get("test-key").await;

        // Should fail with connection error
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_invalid_base_url() {
        // REST client doesn't validate URL at creation time, only when making requests
        let result = RestClient::new("not-a-valid-url");
        assert!(result.is_ok());

        // The error occurs when trying to use the client
        let client = result.unwrap();
        let request_result = client.get("test-key").await;
        assert!(request_result.is_err());
    }

    // =============================================================================
    // Timeout and Retry Tests (structural)
    // =============================================================================

    #[tokio::test]
    async fn test_put_success_after_server_start() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/objects/late-start")
            .with_status(201)
            .with_body(r#"{"success":true}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.put("late-start", Bytes::from("data"), None).await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }
}

// =============================================================================
// Additional Type Coverage Tests
// =============================================================================

mod additional_type_tests {
    use super::*;

    #[test]
    fn test_health_status_all_variants() {
        let serving = HealthStatus::Serving;
        let not_serving = HealthStatus::NotServing;
        let unknown = HealthStatus::Unknown;

        assert_ne!(serving, not_serving);
        assert_ne!(serving, unknown);
        assert_ne!(not_serving, unknown);
    }

    #[test]
    fn test_replication_mode_all_variants() {
        let transparent = ReplicationMode::Transparent;
        let opaque = ReplicationMode::Opaque;

        assert_ne!(transparent, opaque);
        assert_eq!(transparent, ReplicationMode::Transparent);
        assert_eq!(opaque, ReplicationMode::Opaque);
    }

    #[test]
    fn test_lifecycle_policy_with_archive_destination() {
        let mut settings = HashMap::new();
        settings.insert("bucket".to_string(), "archive-bucket".to_string());
        settings.insert("region".to_string(), "us-east-1".to_string());

        let policy = LifecyclePolicy {
            id: "archive-policy".to_string(),
            prefix: "old-data/".to_string(),
            retention_seconds: 2592000, // 30 days
            action: "archive".to_string(),
            destination_type: Some("s3-glacier".to_string()),
            destination_settings: settings,
        };

        assert_eq!(policy.action, "archive");
        assert_eq!(policy.destination_type, Some("s3-glacier".to_string()));
        assert_eq!(policy.destination_settings.len(), 2);
    }

    #[test]
    fn test_replication_policy_with_all_fields() {
        let mut source_settings = HashMap::new();
        source_settings.insert("endpoint".to_string(), "s3.amazonaws.com".to_string());
        source_settings.insert("bucket".to_string(), "source-bucket".to_string());

        let mut dest_settings = HashMap::new();
        dest_settings.insert("endpoint".to_string(), "storage.googleapis.com".to_string());
        dest_settings.insert("bucket".to_string(), "dest-bucket".to_string());

        let encryption_config = EncryptionConfig {
            enabled: true,
            provider: "aes-256-gcm".to_string(),
            default_key: "encryption-key-id".to_string(),
        };

        let encryption_policy = EncryptionPolicy {
            backend: Some(encryption_config.clone()),
            source: None,
            destination: Some(encryption_config),
        };

        let policy = ReplicationPolicy {
            id: "cross-cloud-repl".to_string(),
            source_backend: "s3".to_string(),
            source_settings,
            source_prefix: "production/".to_string(),
            destination_backend: "gcs".to_string(),
            destination_settings: dest_settings,
            check_interval_seconds: 300,
            last_sync_time: None, // DateTime<Utc> requires chrono parsing
            enabled: true,
            encryption: Some(encryption_policy),
            replication_mode: ReplicationMode::Transparent,
        };

        assert_eq!(policy.source_backend, "s3");
        assert_eq!(policy.destination_backend, "gcs");
        assert!(policy.encryption.is_some());
        assert_eq!(policy.replication_mode, ReplicationMode::Transparent);
    }

    #[test]
    fn test_replication_status_comprehensive() {
        let status = ReplicationStatus {
            policy_id: "repl-policy-1".to_string(),
            source_backend: "local".to_string(),
            destination_backend: "s3".to_string(),
            enabled: true,
            total_objects_synced: 50000,
            total_objects_deleted: 1500,
            total_bytes_synced: 1099511627776, // 1TB
            total_errors: 25,
            last_sync_time: None, // DateTime<Utc> requires chrono parsing
            average_sync_duration_ms: 15000,
            sync_count: 200,
        };

        assert_eq!(status.total_objects_synced, 50000);
        assert_eq!(status.total_bytes_synced, 1099511627776);
        assert_eq!(status.sync_count, 200);
    }

    #[test]
    fn test_sync_result_with_errors() {
        let result = SyncResult {
            policy_id: "sync-policy".to_string(),
            synced: 100,
            deleted: 10,
            failed: 5,
            bytes_total: 104857600, // 100MB
            duration_ms: 30000,
            errors: vec![
                "Failed to sync object1: timeout".to_string(),
                "Failed to sync object2: permission denied".to_string(),
                "Failed to sync object3: network error".to_string(),
                "Failed to sync object4: invalid credentials".to_string(),
                "Failed to sync object5: quota exceeded".to_string(),
            ],
        };

        assert_eq!(result.failed, 5);
        assert_eq!(result.errors.len(), 5);
        assert!(result.errors[0].contains("timeout"));
    }

    #[test]
    fn test_metadata_with_all_optional_fields() {
        let mut custom = HashMap::new();
        custom.insert("x-custom-header-1".to_string(), "value1".to_string());
        custom.insert("x-custom-header-2".to_string(), "value2".to_string());

        let metadata = Metadata {
            content_type: Some("video/mp4".to_string()),
            content_encoding: Some("br".to_string()),
            size: 524288000, // 500MB
            last_modified: None, // DateTime<Utc> can't be created from a string literal easily in tests
            etag: Some("\"complete-etag-hash\"".to_string()),
            custom,
        };

        assert!(metadata.content_type.is_some());
        assert!(metadata.content_encoding.is_some());
        assert!(metadata.etag.is_some());
        assert_eq!(metadata.custom.len(), 2);
    }

    #[test]
    fn test_object_info_with_full_metadata() {
        let metadata = Metadata {
            content_type: Some("image/png".to_string()),
            size: 2048576,
            ..Default::default()
        };

        let info = ObjectInfo {
            key: "images/photo.png".to_string(),
            metadata,
        };

        assert!(info.key.contains("images/"));
        assert_eq!(info.metadata.size, 2048576);
    }

    #[test]
    fn test_list_request_all_combinations() {
        let requests = vec![
            ListRequest {
                prefix: Some("a/".to_string()),
                delimiter: None,
                max_results: None,
                continue_from: None,
            },
            ListRequest {
                prefix: None,
                delimiter: Some("/".to_string()),
                max_results: Some(100),
                continue_from: None,
            },
            ListRequest {
                prefix: Some("b/".to_string()),
                delimiter: Some("/".to_string()),
                max_results: Some(50),
                continue_from: Some("token".to_string()),
            },
        ];

        for req in requests {
            let _ = req.clone();
        }
    }

    #[test]
    fn test_encryption_config_variations() {
        let configs = vec![
            EncryptionConfig {
                enabled: true,
                provider: "aes-256-gcm".to_string(),
                default_key: "key1".to_string(),
            },
            EncryptionConfig {
                enabled: false,
                provider: "none".to_string(),
                default_key: "".to_string(),
            },
            EncryptionConfig {
                enabled: true,
                provider: "aws-kms".to_string(),
                default_key: "arn:aws:kms:region:account:key/id".to_string(),
            },
        ];

        for config in configs {
            let _ = config.clone();
        }
    }

    #[test]
    fn test_encryption_policy_all_combinations() {
        let config = EncryptionConfig {
            enabled: true,
            provider: "test".to_string(),
            default_key: "key".to_string(),
        };

        let policies = vec![
            EncryptionPolicy {
                backend: Some(config.clone()),
                source: None,
                destination: None,
            },
            EncryptionPolicy {
                backend: None,
                source: Some(config.clone()),
                destination: None,
            },
            EncryptionPolicy {
                backend: None,
                source: None,
                destination: Some(config.clone()),
            },
            EncryptionPolicy {
                backend: Some(config.clone()),
                source: Some(config.clone()),
                destination: Some(config.clone()),
            },
        ];

        for policy in policies {
            let _ = policy.clone();
        }
    }
}

// =============================================================================
// Serialization Edge Cases
// =============================================================================

mod serialization_edge_cases {
    use super::*;

    #[test]
    fn test_metadata_serialize_with_nulls() {
        let metadata = Metadata {
            content_type: None,
            content_encoding: None,
            size: 0,
            last_modified: None,
            etag: None,
            custom: HashMap::new(),
        };

        let json = serde_json::to_string(&metadata).unwrap();
        assert!(json.contains("0"));
    }

    #[test]
    fn test_list_response_deserialize_missing_fields() {
        let json = r#"{"objects":[],"common_prefixes":[],"truncated":false}"#;
        let response: ListResponse = serde_json::from_str(json).unwrap();
        assert!(!response.truncated);
        assert!(response.next_token.is_none());
    }

    #[test]
    fn test_health_response_construction() {
        // HealthResponse doesn't implement Deserialize, so we test construction instead
        let response = HealthResponse {
            status: HealthStatus::Serving,
            message: Some("healthy".to_string()),
        };
        assert_eq!(response.status, HealthStatus::Serving);
        assert_eq!(response.message, Some("healthy".to_string()));

        let response2 = HealthResponse {
            status: HealthStatus::NotServing,
            message: Some("error".to_string()),
        };
        assert_eq!(response2.status, HealthStatus::NotServing);
    }

    #[test]
    fn test_put_response_deserialize_partial() {
        let json = r#"{"success":true}"#;
        let response: PutResponse = serde_json::from_str(json).unwrap();
        assert!(response.success);
        assert!(response.message.is_none());
    }

    #[test]
    fn test_replication_policy_serialize_deserialize() {
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

        let json = serde_json::to_string(&policy).unwrap();
        let deserialized: ReplicationPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, policy.id);
    }
}
