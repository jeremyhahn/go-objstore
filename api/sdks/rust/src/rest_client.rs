use crate::duration::parse_go_duration_ms;
use crate::error::{Error, Result};
use crate::types::*;
use bytes::Bytes;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// REST client for go-objstore
#[derive(Clone)]
pub struct RestClient {
    base_url: String,
    client: Client,
}

#[derive(Debug, Serialize, Deserialize)]
struct RestMetadata {
    content_type: Option<String>,
    content_encoding: Option<String>,
    size: i64,
    last_modified: Option<String>,
    etag: Option<String>,
    custom: Option<HashMap<String, String>>,
}

/// Wire response from `GET /objects/{key}` list items.
#[derive(Debug, Deserialize)]
struct RestObjectResponse {
    key: String,
    size: i64,
    modified: Option<String>,
    etag: Option<String>,
    metadata: Option<HashMap<String, String>>,
}

/// Wire response from `GET /metadata/{key}`.
///
/// The server's `RespondWithObject` sends an `ObjectResponse` JSON body whose
/// `size` is the stored object size and `metadata` is the custom key/value map.
/// The HTTP `Content-Length` header reflects the length of the JSON payload, not
/// the stored object, so we must parse from the body.
#[derive(Debug, Deserialize)]
struct MetadataObjectResponse {
    #[serde(default)]
    content_type: Option<String>,
    size: i64,
    modified: Option<String>,
    etag: Option<String>,
    /// Custom metadata is returned as the `metadata` JSON field (not `custom`).
    metadata: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
struct RestListResponse {
    objects: Vec<RestObjectResponse>,
    common_prefixes: Option<Vec<String>>,
    next_token: Option<String>,
    truncated: bool,
}

#[derive(Debug, Deserialize)]
struct RestSuccessResponse {
    #[allow(dead_code)]
    message: String,
}

#[derive(Debug, Deserialize)]
struct RestHealthResponse {
    status: String,
    version: Option<String>,
}

impl RestClient {
    /// Create a new REST client
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        let base_url = base_url.into();
        let client = Client::builder()
            .build()
            .map_err(|e| Error::Configuration(e.to_string()))?;

        Ok(Self { base_url, client })
    }

    /// Put an object into storage
    pub async fn put(
        &self,
        key: &str,
        data: Bytes,
        metadata: Option<Metadata>,
    ) -> Result<PutResponse> {
        let url = format!("{}/objects/{}", self.base_url, urlencoding::encode(key));

        let mut request = self.client.put(&url);

        // Apply the canonical X-Object-Metadata contract: Content-Type and
        // Content-Encoding travel as standard HTTP headers, while the custom
        // string->string map is JSON-encoded into X-Object-Metadata.
        if let Some(meta) = &metadata {
            for (name, value) in put_metadata_headers(meta) {
                request = request.header(name, value);
            }
        }

        request = request.body(data);

        let response = request.send().await?;

        if response.status() == StatusCode::CREATED {
            let etag = response
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(String::from);

            Ok(PutResponse {
                success: true,
                message: None,
                etag,
            })
        } else {
            Err(Error::OperationFailed(format!(
                "Failed to put object: {}",
                response.status()
            )))
        }
    }

    /// Get an object from storage
    pub async fn get(&self, key: &str) -> Result<(Bytes, Metadata)> {
        let url = format!("{}/objects/{}", self.base_url, urlencoding::encode(key));

        let response = self.client.get(&url).send().await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::NotFound(key.to_string()));
        }

        if !response.status().is_success() {
            return Err(Error::OperationFailed(format!(
                "Failed to get object: {}",
                response.status()
            )));
        }

        let metadata = metadata_from_headers(response.headers());

        let data = response.bytes().await?;

        Ok((data, metadata))
    }

    /// Delete an object from storage
    pub async fn delete(&self, key: &str) -> Result<DeleteResponse> {
        let url = format!("{}/objects/{}", self.base_url, urlencoding::encode(key));

        let response = self.client.delete(&url).send().await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::NotFound(key.to_string()));
        }

        if response.status().is_success() {
            Ok(DeleteResponse {
                success: true,
                message: None,
            })
        } else {
            Err(Error::OperationFailed(format!(
                "Failed to delete object: {}",
                response.status()
            )))
        }
    }

    /// List objects with optional prefix filtering
    pub async fn list(&self, list_req: ListRequest) -> Result<ListResponse> {
        let mut url = format!("{}/objects", self.base_url);
        let mut params = Vec::new();

        if let Some(prefix) = &list_req.prefix {
            params.push(format!("prefix={}", urlencoding::encode(prefix)));
        }

        if let Some(delimiter) = &list_req.delimiter {
            params.push(format!("delimiter={}", urlencoding::encode(delimiter)));
        }

        if let Some(max_results) = list_req.max_results {
            params.push(format!("limit={}", max_results));
        }

        if let Some(token) = &list_req.continue_from {
            params.push(format!("token={}", urlencoding::encode(token)));
        }

        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(Error::OperationFailed(format!(
                "Failed to list objects: {}",
                response.status()
            )));
        }

        let rest_response: RestListResponse = response.json().await?;

        Ok(ListResponse {
            objects: rest_response
                .objects
                .into_iter()
                .map(|obj| ObjectInfo {
                    key: obj.key,
                    metadata: Metadata {
                        content_type: None,
                        content_encoding: None,
                        size: obj.size,
                        last_modified: obj.modified.and_then(|s| {
                            chrono::DateTime::parse_from_rfc3339(&s)
                                .ok()
                                .map(|dt| dt.with_timezone(&chrono::Utc))
                        }),
                        etag: obj.etag,
                        custom: obj.metadata.unwrap_or_default(),
                    },
                })
                .collect(),
            common_prefixes: rest_response.common_prefixes.unwrap_or_default(),
            next_token: rest_response.next_token,
            truncated: rest_response.truncated,
        })
    }

    /// Check if an object exists
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let url = format!("{}/objects/{}", self.base_url, urlencoding::encode(key));

        let response = self.client.head(&url).send().await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(false);
        }

        if !response.status().is_success() {
            return Err(Error::OperationFailed(format!(
                "Failed to check object existence: {}",
                response.status()
            )));
        }

        Ok(true)
    }

    /// Get metadata for an object
    pub async fn get_metadata(&self, key: &str) -> Result<Metadata> {
        let url = format!("{}/metadata/{}", self.base_url, urlencoding::encode(key));

        let response = self.client.get(&url).send().await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::NotFound(key.to_string()));
        }

        if !response.status().is_success() {
            return Err(Error::OperationFailed(format!(
                "Failed to get metadata: {}",
                response.status()
            )));
        }

        // The metadata endpoint returns a JSON body (ObjectResponse) whose
        // `size` field carries the stored object's size.  Parsing
        // Content-Length from the HTTP response would give the size of the
        // JSON payload itself, not the stored object.
        let body: MetadataObjectResponse = response.json().await?;
        Ok(Metadata {
            content_type: body.content_type,
            content_encoding: None,
            size: body.size,
            last_modified: body.modified.and_then(|s| {
                chrono::DateTime::parse_from_rfc3339(&s)
                    .ok()
                    .map(|dt| dt.with_timezone(&chrono::Utc))
            }),
            etag: body.etag,
            custom: body.metadata.unwrap_or_default(),
        })
    }

    /// Update metadata for an object
    pub async fn update_metadata(&self, key: &str, metadata: Metadata) -> Result<()> {
        let url = format!("{}/metadata/{}", self.base_url, urlencoding::encode(key));

        let rest_metadata = RestMetadata {
            content_type: metadata.content_type,
            content_encoding: metadata.content_encoding,
            size: metadata.size,
            last_modified: metadata.last_modified.map(|dt| dt.to_rfc3339()),
            etag: metadata.etag,
            custom: if metadata.custom.is_empty() {
                None
            } else {
                Some(metadata.custom)
            },
        };

        let response = self.client.put(&url).json(&rest_metadata).send().await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::NotFound(key.to_string()));
        }

        if !response.status().is_success() {
            return Err(Error::OperationFailed(format!(
                "Failed to update metadata: {}",
                response.status()
            )));
        }

        Ok(())
    }

    /// Health check
    pub async fn health(&self) -> Result<HealthResponse> {
        let url = format!("{}/health", self.base_url);

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(Error::OperationFailed(format!(
                "Health check failed: {}",
                response.status()
            )));
        }

        let health: RestHealthResponse = response.json().await?;

        Ok(HealthResponse {
            status: match health.status.as_str() {
                "healthy" | "serving" => HealthStatus::Serving,
                _ => HealthStatus::NotServing,
            },
            message: health.version,
        })
    }

    /// Archive an object to a different storage backend
    pub async fn archive(
        &self,
        key: &str,
        destination_type: String,
        destination_settings: HashMap<String, String>,
    ) -> Result<()> {
        let url = format!("{}/archive", self.base_url);

        let body = serde_json::json!({
            "key": key,
            "destination_type": destination_type,
            "destination_settings": destination_settings,
        });

        let response = self.client.post(&url).json(&body).send().await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(Error::OperationFailed(format!(
                "Failed to archive object: {}",
                response.status()
            )))
        }
    }

    /// Add a lifecycle policy
    pub async fn add_policy(&self, policy: LifecyclePolicy) -> Result<()> {
        let url = format!("{}/policies", self.base_url);

        let mut body = serde_json::json!({
            "id": policy.id,
            "prefix": policy.prefix,
            "retention_seconds": policy.retention_seconds,
            "action": policy.action,
        });
        if let Some(dest_type) = &policy.destination_type {
            body["destination_type"] = serde_json::Value::String(dest_type.clone());
        }
        if !policy.destination_settings.is_empty() {
            body["destination_settings"] = serde_json::to_value(&policy.destination_settings)?;
        }

        let response = self.client.post(&url).json(&body).send().await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(Error::OperationFailed(format!(
                "Failed to add policy: {}",
                response.status()
            )))
        }
    }

    /// Remove a lifecycle policy
    pub async fn remove_policy(&self, id: &str) -> Result<()> {
        let url = format!("{}/policies/{}", self.base_url, urlencoding::encode(id));

        let response = self.client.delete(&url).send().await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(Error::OperationFailed(format!(
                "Failed to remove policy: {}",
                response.status()
            )))
        }
    }

    /// Get all lifecycle policies, optionally filtered by prefix
    pub async fn get_policies(&self, prefix: Option<String>) -> Result<Vec<LifecyclePolicy>> {
        let mut url = format!("{}/policies", self.base_url);
        if let Some(prefix) = &prefix {
            url.push_str(&format!("?prefix={}", urlencoding::encode(prefix)));
        }

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(Error::OperationFailed(format!(
                "Failed to get policies: {}",
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct RestPolicy {
            id: String,
            #[serde(default)]
            prefix: String,
            #[serde(default)]
            retention_seconds: i64,
            #[serde(default)]
            action: String,
            #[serde(default)]
            destination_type: Option<String>,
            #[serde(default)]
            destination_settings: Option<HashMap<String, String>>,
        }

        #[derive(Deserialize)]
        struct RestPoliciesResponse {
            #[serde(default)]
            policies: Vec<RestPolicy>,
        }

        let parsed: RestPoliciesResponse = response.json().await?;

        Ok(parsed
            .policies
            .into_iter()
            .map(|p| LifecyclePolicy {
                id: p.id,
                prefix: p.prefix,
                retention_seconds: p.retention_seconds,
                action: p.action,
                destination_type: p.destination_type.filter(|s| !s.is_empty()),
                destination_settings: p.destination_settings.unwrap_or_default(),
            })
            .collect())
    }

    /// Apply all lifecycle policies, returning (policies_count, objects_processed)
    pub async fn apply_policies(&self) -> Result<(i32, i32)> {
        let url = format!("{}/policies/apply", self.base_url);

        let response = self.client.post(&url).send().await?;

        if !response.status().is_success() {
            return Err(Error::OperationFailed(format!(
                "Failed to apply policies: {}",
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct ApplyResponse {
            #[serde(default)]
            policies_count: i32,
            #[serde(default)]
            objects_processed: i32,
        }

        let parsed: ApplyResponse = response.json().await?;
        Ok((parsed.policies_count, parsed.objects_processed))
    }

    /// Add a replication policy
    pub async fn add_replication_policy(&self, policy: ReplicationPolicy) -> Result<()> {
        let url = format!("{}/replication/policies", self.base_url);

        let body = replication_policy_to_rest_json(&policy, "check_interval_seconds");

        let response = self.client.post(&url).json(&body).send().await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(Error::OperationFailed(format!(
                "Failed to add replication policy: {}",
                response.status()
            )))
        }
    }

    /// Remove a replication policy
    pub async fn remove_replication_policy(&self, id: &str) -> Result<()> {
        let url = format!(
            "{}/replication/policies/{}",
            self.base_url,
            urlencoding::encode(id)
        );

        let response = self.client.delete(&url).send().await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(Error::OperationFailed(format!(
                "Failed to remove replication policy: {}",
                response.status()
            )))
        }
    }

    /// Get all replication policies
    pub async fn get_replication_policies(&self) -> Result<Vec<ReplicationPolicy>> {
        let url = format!("{}/replication/policies", self.base_url);

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(Error::OperationFailed(format!(
                "Failed to get replication policies: {}",
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct PoliciesResponse {
            #[serde(default)]
            policies: Vec<RestReplicationPolicy>,
        }

        let parsed: PoliciesResponse = response.json().await?;
        Ok(parsed
            .policies
            .into_iter()
            .map(rest_replication_policy_into)
            .collect())
    }

    /// Get a specific replication policy
    pub async fn get_replication_policy(&self, id: &str) -> Result<ReplicationPolicy> {
        let url = format!(
            "{}/replication/policies/{}",
            self.base_url,
            urlencoding::encode(id)
        );

        let response = self.client.get(&url).send().await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::NotFound(id.to_string()));
        }

        if !response.status().is_success() {
            return Err(Error::OperationFailed(format!(
                "Failed to get replication policy: {}",
                response.status()
            )));
        }

        let parsed: RestReplicationPolicy = response.json().await?;
        Ok(rest_replication_policy_into(parsed))
    }

    /// Trigger replication synchronization
    pub async fn trigger_replication(
        &self,
        policy_id: Option<String>,
        parallel: bool,
        worker_count: i32,
    ) -> Result<SyncResult> {
        let url = format!("{}/replication/trigger", self.base_url);

        let mut body = serde_json::json!({
            "parallel": parallel,
            "worker_count": worker_count,
        });
        if let Some(id) = &policy_id {
            body["policy_id"] = serde_json::Value::String(id.clone());
        }

        let response = self.client.post(&url).json(&body).send().await?;

        if !response.status().is_success() {
            return Err(Error::OperationFailed(format!(
                "Failed to trigger replication: {}",
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct TriggerResponse {
            #[serde(default)]
            result: Option<RestSyncResult>,
        }

        let parsed: TriggerResponse = response.json().await?;
        parsed
            .result
            .map(rest_sync_result_into)
            .ok_or_else(|| Error::InvalidResponse("Missing sync result".to_string()))
    }

    /// Get replication status for a policy
    pub async fn get_replication_status(&self, id: &str) -> Result<ReplicationStatus> {
        let url = format!(
            "{}/replication/status/{}",
            self.base_url,
            urlencoding::encode(id)
        );

        let response = self.client.get(&url).send().await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::NotFound(id.to_string()));
        }

        if !response.status().is_success() {
            return Err(Error::OperationFailed(format!(
                "Failed to get replication status: {}",
                response.status()
            )));
        }

        let parsed: RestReplicationStatus = response.json().await?;
        Ok(rest_replication_status_into(parsed))
    }

    /// Close the client, releasing any underlying resources.
    ///
    /// The REST client holds no persistent connections beyond reqwest's
    /// connection pool, so this is a no-op provided for API parity.
    pub async fn close(&self) -> Result<()> {
        Ok(())
    }
}

/// Wire representation of a replication policy returned by the REST server.
#[derive(Debug, Deserialize)]
struct RestReplicationPolicy {
    #[serde(default)]
    id: String,
    #[serde(default)]
    source_backend: String,
    #[serde(default)]
    source_settings: Option<HashMap<String, String>>,
    #[serde(default)]
    source_prefix: String,
    #[serde(default)]
    destination_backend: String,
    #[serde(default)]
    destination_settings: Option<HashMap<String, String>>,
    #[serde(default)]
    check_interval_seconds: i64,
    #[serde(default)]
    last_sync_time: Option<String>,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    replication_mode: Option<String>,
    #[serde(default)]
    encryption: Option<EncryptionPolicy>,
}

/// Wire representation of a sync result returned by the REST server.
#[derive(Debug, Deserialize)]
struct RestSyncResult {
    #[serde(default)]
    policy_id: String,
    #[serde(default)]
    synced: i32,
    #[serde(default)]
    deleted: i32,
    #[serde(default)]
    failed: i32,
    #[serde(default)]
    bytes_total: i64,
    #[serde(default)]
    duration: Option<String>,
    #[serde(default)]
    errors: Option<Vec<String>>,
}

/// Wire representation of replication status returned by the REST server.
#[derive(Debug, Deserialize)]
struct RestReplicationStatus {
    #[serde(default)]
    policy_id: String,
    #[serde(default)]
    source_backend: String,
    #[serde(default)]
    destination_backend: String,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    total_objects_synced: i64,
    #[serde(default)]
    total_objects_deleted: i64,
    #[serde(default)]
    total_bytes_synced: i64,
    #[serde(default)]
    total_errors: i64,
    #[serde(default)]
    last_sync_time: Option<String>,
    #[serde(default)]
    average_sync_duration: Option<String>,
    #[serde(default)]
    sync_count: i64,
}

fn rest_replication_policy_into(p: RestReplicationPolicy) -> ReplicationPolicy {
    ReplicationPolicy {
        id: p.id,
        source_backend: p.source_backend,
        source_settings: p.source_settings.unwrap_or_default(),
        source_prefix: p.source_prefix,
        destination_backend: p.destination_backend,
        destination_settings: p.destination_settings.unwrap_or_default(),
        check_interval_seconds: p.check_interval_seconds,
        last_sync_time: p.last_sync_time.and_then(parse_rfc3339),
        enabled: p.enabled,
        encryption: p.encryption,
        replication_mode: match p.replication_mode.as_deref() {
            Some("opaque") => ReplicationMode::Opaque,
            _ => ReplicationMode::Transparent,
        },
    }
}

fn rest_sync_result_into(r: RestSyncResult) -> SyncResult {
    SyncResult {
        policy_id: r.policy_id,
        synced: r.synced,
        deleted: r.deleted,
        failed: r.failed,
        bytes_total: r.bytes_total,
        duration_ms: parse_go_duration_ms(r.duration.as_deref().unwrap_or("")),
        errors: r.errors.unwrap_or_default(),
    }
}

fn rest_replication_status_into(s: RestReplicationStatus) -> ReplicationStatus {
    ReplicationStatus {
        policy_id: s.policy_id,
        source_backend: s.source_backend,
        destination_backend: s.destination_backend,
        enabled: s.enabled,
        total_objects_synced: s.total_objects_synced,
        total_objects_deleted: s.total_objects_deleted,
        total_bytes_synced: s.total_bytes_synced,
        total_errors: s.total_errors,
        last_sync_time: s.last_sync_time.and_then(parse_rfc3339),
        average_sync_duration_ms: parse_go_duration_ms(
            s.average_sync_duration.as_deref().unwrap_or(""),
        ),
        sync_count: s.sync_count,
    }
}

fn parse_rfc3339(s: String) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(&s)
        .ok()
        .map(|dt| dt.with_timezone(&chrono::Utc))
}

/// Serialize a [`ReplicationPolicy`] into the JSON body expected by the server.
///
/// `interval_field` is the JSON key used for the check interval: REST uses
/// `check_interval_seconds`, while QUIC uses `check_interval`.
pub(crate) fn replication_policy_to_rest_json(
    policy: &ReplicationPolicy,
    interval_field: &str,
) -> serde_json::Value {
    let mode = match policy.replication_mode {
        ReplicationMode::Transparent => "transparent",
        ReplicationMode::Opaque => "opaque",
    };

    let mut body = serde_json::json!({
        "id": policy.id,
        "source_backend": policy.source_backend,
        "destination_backend": policy.destination_backend,
        "enabled": policy.enabled,
        "replication_mode": mode,
    });
    body[interval_field] = serde_json::Value::from(policy.check_interval_seconds);

    if !policy.source_settings.is_empty() {
        body["source_settings"] =
            serde_json::to_value(&policy.source_settings).unwrap_or(serde_json::Value::Null);
    }
    if !policy.source_prefix.is_empty() {
        body["source_prefix"] = serde_json::Value::String(policy.source_prefix.clone());
    }
    if !policy.destination_settings.is_empty() {
        body["destination_settings"] =
            serde_json::to_value(&policy.destination_settings).unwrap_or(serde_json::Value::Null);
    }
    if let Some(encryption) = &policy.encryption {
        if let Ok(value) = serde_json::to_value(encryption) {
            body["encryption"] = value;
        }
    }

    body
}

/// Build the HTTP headers for a PUT request following the canonical
/// `X-Object-Metadata` contract.
///
/// `Content-Type` and `Content-Encoding` are emitted as standard headers (the
/// latter only when present). The custom string->string map is serialized to
/// JSON and placed in `X-Object-Metadata`; the header is omitted entirely when
/// there is no custom metadata. `content_type`/`content_encoding` are never
/// duplicated into the JSON body.
fn put_metadata_headers(meta: &Metadata) -> Vec<(&'static str, String)> {
    let mut headers = Vec::new();

    if let Some(content_type) = &meta.content_type {
        headers.push(("Content-Type", content_type.clone()));
    }
    if let Some(content_encoding) = &meta.content_encoding {
        headers.push(("Content-Encoding", content_encoding.clone()));
    }
    if !meta.custom.is_empty() {
        if let Ok(json) = serde_json::to_string(&meta.custom) {
            headers.push(("X-Object-Metadata", json));
        }
    }

    headers
}

/// Parse object [`Metadata`] from HTTP response headers following the canonical
/// `X-Object-Metadata` contract.
///
/// `Content-Type`, `Content-Encoding`, `Content-Length`, `ETag` and
/// `Last-Modified` are read from their standard headers. The custom
/// string->string map is parsed from the JSON-encoded `X-Object-Metadata`
/// header, if present.
fn metadata_from_headers(headers: &reqwest::header::HeaderMap) -> Metadata {
    let header_str = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(String::from)
    };

    let content_type = header_str("content-type");
    let content_encoding = header_str("content-encoding");

    let size = headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);

    let etag = header_str("etag");

    let last_modified = headers
        .get("last-modified")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| chrono::DateTime::parse_from_rfc2822(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc));

    let custom = headers
        .get("x-object-metadata")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| serde_json::from_str::<HashMap<String, String>>(s).ok())
        .unwrap_or_default();

    Metadata {
        content_type,
        content_encoding,
        size,
        last_modified,
        etag,
        custom,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::Server;

    // =========================================================================
    // REST canonical test matrix.
    //
    // Each op gets `rest_<op>_success` / `rest_<op>_error`; the nine read/mutate
    // ops additionally get `rest_<op>_not_found`. Plus `rest_metadata_round_trip`
    // and `rest_validation_empty_key`. The transport is mocked with `mockito`;
    // no live server is required.
    //
    // Documented impl behaviors:
    //  - PUT only treats HTTP 201 CREATED as success.
    //  - `exists` returns Ok(false) only for 404 and Ok(true) for a success
    //    status; any other status (e.g. 5xx) returns an Err -> see
    //    rest_exists_error / rest_exists_not_found.
    //  - The REST client performs NO client-side empty-key validation; an empty
    //    key is sent to the server -> see rest_validation_empty_key.
    // =========================================================================

    fn meta_with_custom(pairs: &[(&str, &str)]) -> Metadata {
        let mut meta = Metadata::default();
        for (k, v) in pairs {
            meta.custom.insert(k.to_string(), v.to_string());
        }
        meta
    }

    fn sample_replication_policy() -> ReplicationPolicy {
        let mut source_settings = HashMap::new();
        source_settings.insert("region".to_string(), "us-east-1".to_string());
        let mut destination_settings = HashMap::new();
        destination_settings.insert("bucket".to_string(), "backup".to_string());
        ReplicationPolicy {
            id: "repl-1".to_string(),
            source_backend: "s3".to_string(),
            source_settings,
            source_prefix: "data/".to_string(),
            destination_backend: "gcs".to_string(),
            destination_settings,
            check_interval_seconds: 300,
            last_sync_time: None,
            enabled: true,
            encryption: None,
            replication_mode: ReplicationMode::Opaque,
        }
    }

    // ---- constructor / helper unit tests (language-specific extras) ----

    #[test]
    fn rest_client_new_ok() {
        assert!(RestClient::new("http://localhost:8080").is_ok());
        assert!(RestClient::new("https://localhost:8443").is_ok());
    }

    #[test]
    fn rest_put_metadata_headers_scheme() {
        // Content-Type and Content-Encoding are standard headers; custom map is
        // JSON in X-Object-Metadata (custom keys only); header omitted when no
        // custom metadata.
        let mut meta = meta_with_custom(&[("owner", "alice")]);
        meta.content_type = Some("application/json".to_string());
        meta.content_encoding = Some("gzip".to_string());
        let headers = put_metadata_headers(&meta);
        assert!(headers.contains(&("Content-Type", "application/json".to_string())));
        assert!(headers.contains(&("Content-Encoding", "gzip".to_string())));
        let object_meta = headers
            .iter()
            .find(|(n, _)| *n == "X-Object-Metadata")
            .map(|(_, v)| v.as_str())
            .expect("X-Object-Metadata header present");
        let parsed: HashMap<String, String> = serde_json::from_str(object_meta).unwrap();
        assert_eq!(parsed.get("owner").map(String::as_str), Some("alice"));
        assert!(!parsed.contains_key("content_type"));
        assert!(!parsed.contains_key("content_encoding"));

        // No custom -> no X-Object-Metadata; no content-encoding when absent.
        let mut bare = Metadata::default();
        bare.content_type = Some("text/plain".to_string());
        let headers = put_metadata_headers(&bare);
        assert!(!headers.iter().any(|(n, _)| *n == "X-Object-Metadata"));
        assert!(!headers.iter().any(|(n, _)| *n == "Content-Encoding"));
    }

    #[test]
    fn rest_metadata_from_headers_parsing() {
        use reqwest::header::{HeaderMap, HeaderValue};
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        headers.insert("content-encoding", HeaderValue::from_static("gzip"));
        headers.insert("content-length", HeaderValue::from_static("42"));
        headers.insert("etag", HeaderValue::from_static("\"abc123\""));
        headers.insert(
            "x-object-metadata",
            HeaderValue::from_str(r#"{"owner":"alice"}"#).unwrap(),
        );
        let meta = metadata_from_headers(&headers);
        assert_eq!(meta.content_type.as_deref(), Some("application/json"));
        assert_eq!(meta.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(meta.size, 42);
        assert_eq!(meta.etag.as_deref(), Some("\"abc123\""));
        assert_eq!(meta.custom.get("owner").map(String::as_str), Some("alice"));

        // Malformed custom JSON is tolerated -> empty custom map.
        let mut bad = HeaderMap::new();
        bad.insert("x-object-metadata", HeaderValue::from_static("not-json"));
        assert!(metadata_from_headers(&bad).custom.is_empty());
    }

    // ---- put ----

    #[tokio::test]
    async fn rest_put_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/objects/k")
            .with_status(201)
            .with_header("etag", "\"e1\"")
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let resp = client
            .put("k", Bytes::from_static(b"data"), None)
            .await
            .unwrap();
        mock.assert_async().await;
        assert!(resp.success);
        assert_eq!(resp.etag.as_deref(), Some("\"e1\""));
    }

    #[tokio::test]
    async fn rest_put_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/objects/k")
            .with_status(500)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client
            .put("k", Bytes::from_static(b"d"), None)
            .await
            .unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    // ---- get ----

    #[tokio::test]
    async fn rest_get_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/objects/k")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_header("content-length", "5")
            .with_body("hello")
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let (data, meta) = client.get("k").await.unwrap();
        mock.assert_async().await;
        assert_eq!(&data[..], b"hello");
        assert_eq!(meta.content_type.as_deref(), Some("application/json"));
        assert_eq!(meta.size, 5);
    }

    #[tokio::test]
    async fn rest_get_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/objects/k")
            .with_status(500)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.get("k").await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[tokio::test]
    async fn rest_get_not_found() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/objects/k")
            .with_status(404)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.get("k").await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::NotFound(_)));
    }

    // ---- delete ----

    #[tokio::test]
    async fn rest_delete_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/objects/k")
            .with_status(204)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let resp = client.delete("k").await.unwrap();
        mock.assert_async().await;
        assert!(resp.success);
    }

    #[tokio::test]
    async fn rest_delete_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/objects/k")
            .with_status(500)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.delete("k").await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[tokio::test]
    async fn rest_delete_not_found() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/objects/k")
            .with_status(404)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.delete("k").await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::NotFound(_)));
    }

    // ---- list ----

    #[tokio::test]
    async fn rest_list_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/objects?prefix=a%2F&delimiter=%2F&limit=5&token=abc")
            .with_status(200)
            .with_body(
                r#"{"objects":[{"key":"a/1","size":10,"modified":"2024-01-01T00:00:00Z","etag":"e1","metadata":{"k":"v"}}],"common_prefixes":["a/b/"],"next_token":"next","truncated":true}"#,
            )
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let req = ListRequest {
            prefix: Some("a/".to_string()),
            delimiter: Some("/".to_string()),
            max_results: Some(5),
            continue_from: Some("abc".to_string()),
        };
        let resp = client.list(req).await.unwrap();
        mock.assert_async().await;
        assert_eq!(resp.objects.len(), 1);
        assert_eq!(resp.objects[0].key, "a/1");
        assert_eq!(resp.objects[0].metadata.size, 10);
        assert_eq!(resp.common_prefixes, vec!["a/b/".to_string()]);
        assert_eq!(resp.next_token.as_deref(), Some("next"));
        assert!(resp.truncated);
    }

    #[tokio::test]
    async fn rest_list_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/objects")
            .with_status(500)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.list(ListRequest::default()).await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    // ---- exists ----

    #[tokio::test]
    async fn rest_exists_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("HEAD", "/objects/k")
            .with_status(200)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        assert!(client.exists("k").await.unwrap());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn rest_exists_error() {
        // A 5xx must surface as an error, not be swallowed as Ok(false).
        let mut server = Server::new_async().await;
        let mock = server
            .mock("HEAD", "/objects/k")
            .with_status(500)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.exists("k").await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[tokio::test]
    async fn rest_exists_not_found() {
        // 404 -> Ok(false), no throw.
        let mut server = Server::new_async().await;
        let mock = server
            .mock("HEAD", "/objects/k")
            .with_status(404)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        assert!(!client.exists("k").await.unwrap());
        mock.assert_async().await;
    }

    // ---- get_metadata ----

    #[tokio::test]
    async fn rest_get_metadata_success() {
        // The metadata endpoint returns a JSON body (ObjectResponse), not
        // headers.  `size` comes from the JSON `size` field and `custom` comes
        // from the JSON `metadata` field.
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/metadata/k")
            .with_status(200)
            .with_body(
                r#"{"key":"k","size":42,"content_type":"text/plain","metadata":{"k":"v"}}"#,
            )
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let meta = client.get_metadata("k").await.unwrap();
        mock.assert_async().await;
        assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
        assert_eq!(meta.size, 42);
        assert_eq!(meta.custom.get("k").map(String::as_str), Some("v"));
    }

    #[tokio::test]
    async fn rest_get_metadata_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/metadata/k")
            .with_status(500)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.get_metadata("k").await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[tokio::test]
    async fn rest_get_metadata_not_found() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/metadata/k")
            .with_status(404)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.get_metadata("k").await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::NotFound(_)));
    }

    // ---- update_metadata ----

    #[tokio::test]
    async fn rest_update_metadata_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/metadata/k")
            .with_status(200)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let mut meta = Metadata::default();
        meta.content_type = Some("text/plain".to_string());
        meta.custom.insert("k".to_string(), "v".to_string());
        client.update_metadata("k", meta).await.unwrap();
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn rest_update_metadata_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/metadata/k")
            .with_status(500)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client
            .update_metadata("k", Metadata::default())
            .await
            .unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[tokio::test]
    async fn rest_update_metadata_not_found() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("PUT", "/metadata/k")
            .with_status(404)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client
            .update_metadata("k", Metadata::default())
            .await
            .unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::NotFound(_)));
    }

    // ---- health ----

    #[tokio::test]
    async fn rest_health_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/health")
            .with_status(200)
            .with_body(r#"{"status":"healthy","version":"1.2.3"}"#)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let health = client.health().await.unwrap();
        mock.assert_async().await;
        assert_eq!(health.status, HealthStatus::Serving);
        assert_eq!(health.message.as_deref(), Some("1.2.3"));
    }

    #[tokio::test]
    async fn rest_health_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/health")
            .with_status(500)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.health().await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    // ---- archive ----

    #[tokio::test]
    async fn rest_archive_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/archive")
            .match_body(mockito::Matcher::PartialJsonString(
                r#"{"key":"old.bin","destination_type":"glacier"}"#.to_string(),
            ))
            .with_status(200)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let mut settings = HashMap::new();
        settings.insert("vault".to_string(), "cold".to_string());
        client
            .archive("old.bin", "glacier".to_string(), settings)
            .await
            .unwrap();
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn rest_archive_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/archive")
            .with_status(500)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client
            .archive("old.bin", "glacier".to_string(), HashMap::new())
            .await
            .unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    // ---- add_policy ----

    #[tokio::test]
    async fn rest_add_policy_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/policies")
            .match_body(mockito::Matcher::PartialJsonString(
                r#"{"id":"p1","prefix":"logs/","action":"delete","destination_type":"glacier"}"#
                    .to_string(),
            ))
            .with_status(201)
            .create_async()
            .await;
        let mut destination_settings = HashMap::new();
        destination_settings.insert("vault".to_string(), "v1".to_string());
        let policy = LifecyclePolicy {
            id: "p1".to_string(),
            prefix: "logs/".to_string(),
            retention_seconds: 86400,
            action: "delete".to_string(),
            destination_type: Some("glacier".to_string()),
            destination_settings,
        };
        let client = RestClient::new(server.url()).unwrap();
        client.add_policy(policy).await.unwrap();
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn rest_add_policy_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/policies")
            .with_status(400)
            .create_async()
            .await;
        let policy = LifecyclePolicy {
            id: "p1".to_string(),
            prefix: String::new(),
            retention_seconds: 0,
            action: "delete".to_string(),
            destination_type: None,
            destination_settings: HashMap::new(),
        };
        let client = RestClient::new(server.url()).unwrap();
        let err = client.add_policy(policy).await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    // ---- remove_policy ----

    #[tokio::test]
    async fn rest_remove_policy_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/policies/p1")
            .with_status(200)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        client.remove_policy("p1").await.unwrap();
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn rest_remove_policy_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/policies/p1")
            .with_status(500)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.remove_policy("p1").await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[tokio::test]
    async fn rest_remove_policy_not_found() {
        // Impl maps 404 here to OperationFailed (no NotFound special-casing).
        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/policies/p1")
            .with_status(404)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.remove_policy("p1").await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    // ---- get_policies ----

    #[tokio::test]
    async fn rest_get_policies_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/policies?prefix=logs%2F")
            .with_status(200)
            .with_body(
                r#"{"policies":[{"id":"p1","prefix":"logs/","retention_seconds":3600,"action":"archive","destination_type":"glacier","destination_settings":{"vault":"v1"}},{"id":"p2","action":"delete"}]}"#,
            )
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let policies = client
            .get_policies(Some("logs/".to_string()))
            .await
            .unwrap();
        mock.assert_async().await;
        assert_eq!(policies.len(), 2);
        assert_eq!(policies[0].destination_type.as_deref(), Some("glacier"));
        assert_eq!(policies[1].destination_type, None);
    }

    #[tokio::test]
    async fn rest_get_policies_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/policies")
            .with_status(500)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.get_policies(None).await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    // ---- apply_policies ----

    #[tokio::test]
    async fn rest_apply_policies_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/policies/apply")
            .with_status(200)
            .with_body(r#"{"policies_count":3,"objects_processed":42}"#)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let (count, processed) = client.apply_policies().await.unwrap();
        mock.assert_async().await;
        assert_eq!(count, 3);
        assert_eq!(processed, 42);
    }

    #[tokio::test]
    async fn rest_apply_policies_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/policies/apply")
            .with_status(500)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.apply_policies().await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    // ---- add_replication_policy ----

    #[tokio::test]
    async fn rest_add_replication_policy_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/replication/policies")
            .match_body(mockito::Matcher::PartialJsonString(
                r#"{"id":"repl-1","source_backend":"s3","destination_backend":"gcs","check_interval_seconds":300,"replication_mode":"opaque"}"#.to_string(),
            ))
            .with_status(201)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        client
            .add_replication_policy(sample_replication_policy())
            .await
            .unwrap();
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn rest_add_replication_policy_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/replication/policies")
            .with_status(409)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client
            .add_replication_policy(sample_replication_policy())
            .await
            .unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    // ---- remove_replication_policy ----

    #[tokio::test]
    async fn rest_remove_replication_policy_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/replication/policies/repl-1")
            .with_status(200)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        client.remove_replication_policy("repl-1").await.unwrap();
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn rest_remove_replication_policy_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/replication/policies/repl-1")
            .with_status(500)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client
            .remove_replication_policy("repl-1")
            .await
            .unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[tokio::test]
    async fn rest_remove_replication_policy_not_found() {
        // Impl maps 404 here to OperationFailed (no NotFound special-casing).
        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/replication/policies/missing")
            .with_status(404)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client
            .remove_replication_policy("missing")
            .await
            .unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    // ---- get_replication_policies ----

    #[tokio::test]
    async fn rest_get_replication_policies_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/replication/policies")
            .with_status(200)
            .with_body(
                r#"{"policies":[{"id":"repl-1","source_backend":"s3","destination_backend":"gcs","check_interval_seconds":300,"enabled":true,"replication_mode":"opaque","last_sync_time":"2024-01-02T03:04:05Z"}]}"#,
            )
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let policies = client.get_replication_policies().await.unwrap();
        mock.assert_async().await;
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].id, "repl-1");
        assert_eq!(policies[0].check_interval_seconds, 300);
        assert_eq!(policies[0].replication_mode, ReplicationMode::Opaque);
        assert!(policies[0].last_sync_time.is_some());
    }

    #[tokio::test]
    async fn rest_get_replication_policies_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/replication/policies")
            .with_status(500)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.get_replication_policies().await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    // ---- get_replication_policy ----

    #[tokio::test]
    async fn rest_get_replication_policy_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/replication/policies/repl-1")
            .with_status(200)
            .with_body(
                r#"{"id":"repl-1","source_backend":"s3","destination_backend":"gcs","check_interval_seconds":120,"replication_mode":"transparent"}"#,
            )
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let policy = client.get_replication_policy("repl-1").await.unwrap();
        mock.assert_async().await;
        assert_eq!(policy.id, "repl-1");
        assert_eq!(policy.replication_mode, ReplicationMode::Transparent);
    }

    #[tokio::test]
    async fn rest_get_replication_policy_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/replication/policies/boom")
            .with_status(500)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.get_replication_policy("boom").await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[tokio::test]
    async fn rest_get_replication_policy_not_found() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/replication/policies/missing")
            .with_status(404)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.get_replication_policy("missing").await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::NotFound(_)));
    }

    // ---- trigger_replication ----

    #[tokio::test]
    async fn rest_trigger_replication_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/replication/trigger")
            .match_body(mockito::Matcher::PartialJsonString(
                r#"{"parallel":true,"worker_count":4,"policy_id":"repl-1"}"#.to_string(),
            ))
            .with_status(200)
            .with_body(
                r#"{"result":{"policy_id":"repl-1","synced":5,"deleted":1,"failed":0,"bytes_total":1024,"duration":"1.5s","errors":["minor"]}}"#,
            )
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let result = client
            .trigger_replication(Some("repl-1".to_string()), true, 4)
            .await
            .unwrap();
        mock.assert_async().await;
        assert_eq!(result.policy_id, "repl-1");
        assert_eq!(result.synced, 5);
        assert_eq!(result.bytes_total, 1024);
        assert_eq!(result.duration_ms, 1500);
        assert_eq!(result.errors, vec!["minor".to_string()]);
    }

    #[tokio::test]
    async fn rest_trigger_replication_error() {
        // Non-success -> OperationFailed; success but missing result -> InvalidResponse.
        let mut server = Server::new_async().await;
        let bad = server
            .mock("POST", "/replication/trigger")
            .with_status(500)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client
            .trigger_replication(None, false, 1)
            .await
            .unwrap_err();
        bad.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));

        let mut server = Server::new_async().await;
        let empty = server
            .mock("POST", "/replication/trigger")
            .with_status(200)
            .with_body(r#"{}"#)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client
            .trigger_replication(None, false, 1)
            .await
            .unwrap_err();
        empty.assert_async().await;
        assert!(matches!(err, Error::InvalidResponse(_)));
    }

    // ---- get_replication_status ----

    #[tokio::test]
    async fn rest_get_replication_status_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/replication/status/repl-1")
            .with_status(200)
            .with_body(
                r#"{"policy_id":"repl-1","source_backend":"s3","destination_backend":"gcs","enabled":true,"total_objects_synced":10,"total_objects_deleted":2,"total_bytes_synced":2048,"total_errors":1,"last_sync_time":"2024-05-06T07:08:09Z","average_sync_duration":"2s","sync_count":7}"#,
            )
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let status = client.get_replication_status("repl-1").await.unwrap();
        mock.assert_async().await;
        assert_eq!(status.policy_id, "repl-1");
        assert_eq!(status.total_objects_synced, 10);
        assert_eq!(status.average_sync_duration_ms, 2000);
        assert!(status.last_sync_time.is_some());
    }

    #[tokio::test]
    async fn rest_get_replication_status_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/replication/status/boom")
            .with_status(503)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.get_replication_status("boom").await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[tokio::test]
    async fn rest_get_replication_status_not_found() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/replication/status/missing")
            .with_status(404)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.get_replication_status("missing").await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::NotFound(_)));
    }

    // ---- cross-cutting ----

    #[tokio::test]
    async fn rest_metadata_round_trip() {
        // PUT request must carry Content-Type, Content-Encoding and
        // X-Object-Metadata = JSON(custom map only). GET + get_metadata then
        // return all three from the response headers.
        let mut server = Server::new_async().await;
        let put = server
            .mock("PUT", "/objects/obj")
            .match_header("content-type", "application/octet-stream")
            .match_header("content-encoding", "gzip")
            .match_header("x-object-metadata", r#"{"owner":"dave"}"#)
            .with_status(201)
            .create_async()
            .await;
        let get = server
            .mock("GET", "/objects/obj")
            .with_status(200)
            .with_header("content-type", "application/octet-stream")
            .with_header("content-encoding", "gzip")
            .with_header("content-length", "7")
            .with_header("x-object-metadata", r#"{"owner":"dave"}"#)
            .with_body("payload")
            .create_async()
            .await;
        let get_meta = server
            .mock("GET", "/metadata/obj")
            .with_status(200)
            .with_body(
                r#"{"key":"obj","size":7,"content_type":"application/octet-stream","metadata":{"owner":"dave"}}"#,
            )
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let mut meta = meta_with_custom(&[("owner", "dave")]);
        meta.content_type = Some("application/octet-stream".to_string());
        meta.content_encoding = Some("gzip".to_string());
        client
            .put("obj", Bytes::from_static(b"payload"), Some(meta))
            .await
            .unwrap();

        let (data, got) = client.get("obj").await.unwrap();
        assert_eq!(&data[..], b"payload");
        assert_eq!(
            got.content_type.as_deref(),
            Some("application/octet-stream")
        );
        assert_eq!(got.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(got.custom.get("owner").map(String::as_str), Some("dave"));

        let head = client.get_metadata("obj").await.unwrap();
        assert_eq!(
            head.content_type.as_deref(),
            Some("application/octet-stream")
        );
        assert_eq!(head.size, 7, "size must come from JSON body, not Content-Length");
        assert_eq!(head.custom.get("owner").map(String::as_str), Some("dave"));

        put.assert_async().await;
        get.assert_async().await;
        get_meta.assert_async().await;
    }

    #[tokio::test]
    async fn rest_validation_empty_key() {
        // The REST client performs NO client-side empty-key validation: the
        // request is sent with an empty key and the server decides. Here the
        // server returns 404, which surfaces as Error::NotFound.
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(404)
            .create_async()
            .await;
        let client = RestClient::new(server.url()).unwrap();
        let err = client.get("").await.unwrap_err();
        mock.assert_async().await;
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[tokio::test]
    async fn rest_close_is_noop() {
        let client = RestClient::new("http://localhost:9").unwrap();
        client.close().await.unwrap();
    }
}
