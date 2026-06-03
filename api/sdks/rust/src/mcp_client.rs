use crate::auth::{apply_auth, AuthConfig};
use crate::duration::parse_go_duration_ms;
use crate::error::{Error, Result};
use crate::jsonrpc::{string_map, JsonRpcRequest, JsonRpcResponse};
use crate::types::*;
use base64::Engine as _;
use bytes::Bytes;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// HTTP/JSON-RPC 2.0 client for the go-objstore MCP server.
///
/// The MCP server accepts `tools/call` JSON-RPC requests via HTTP POST.
/// Each operation is mapped to an `objstore_<op>` tool name; the result
/// is a JSON string embedded in `result.content[0].text`.
///
/// Binary object data (put/get) is base64-encoded in transit.
///
/// # Example
///
/// ```no_run
/// use go_objstore::McpClient;
/// use bytes::Bytes;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = McpClient::new("http://localhost:8081")?;
/// let resp = client.put("hello.txt", Bytes::from("world"), None).await?;
/// assert!(resp.success);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct McpClient {
    base_url: String,
    client: Client,
    auth: AuthConfig,
    next_id: std::sync::Arc<AtomicU64>,
}

// ── wire types ─────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct ToolCallParams<'a> {
    name: &'a str,
    arguments: Value,
}

#[derive(Debug, Deserialize)]
struct ToolCallResult {
    content: Vec<ContentItem>,
}

#[derive(Debug, Deserialize)]
struct ContentItem {
    #[serde(rename = "type")]
    content_type: String,
    text: Option<String>,
}

impl McpClient {
    /// Create a new MCP client.
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        Self::new_with_auth(base_url, AuthConfig::default())
    }

    /// Create a new MCP client with authentication configuration.
    pub fn new_with_auth(base_url: impl Into<String>, auth: AuthConfig) -> Result<Self> {
        let client = Client::builder()
            .build()
            .map_err(|e| Error::Configuration(e.to_string()))?;
        Ok(Self {
            base_url: base_url.into(),
            client,
            auth,
            next_id: std::sync::Arc::new(AtomicU64::new(1)),
        })
    }

    // ── low-level tool call ──────────────────────────────────────────────

    async fn call_tool(&self, tool_name: &str, arguments: Value) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);

        let request_body = JsonRpcRequest::new(
            "tools/call",
            ToolCallParams {
                name: tool_name,
                arguments,
            },
            id,
        );

        let url = self.base_url.trim_end_matches('/').to_string() + "/";
        let req = self.client.post(&url).json(&request_body);
        let req = apply_auth(req, &self.auth);

        let response = req.send().await?;

        if !response.status().is_success() {
            return Err(Error::OperationFailed(format!(
                "MCP server returned HTTP {}",
                response.status()
            )));
        }

        let rpc_resp: JsonRpcResponse<ToolCallResult> = response.json().await?;

        if let Some(err) = rpc_resp.error {
            return Err(err.into_error());
        }

        let result = rpc_resp
            .result
            .ok_or_else(|| Error::InvalidResponse("missing result".to_string()))?;

        let text = result
            .content
            .into_iter()
            .find(|c| c.content_type == "text")
            .and_then(|c| c.text)
            .ok_or_else(|| Error::InvalidResponse("missing text content".to_string()))?;

        let value: Value = serde_json::from_str(&text)?;
        Ok(value)
    }

    // ── public API ────────────────────────────────────────────────────────

    /// Put an object into storage.  Binary data is base64-encoded.
    pub async fn put(
        &self,
        key: &str,
        data: Bytes,
        metadata: Option<Metadata>,
    ) -> Result<PutResponse> {
        let encoded = base64::engine::general_purpose::STANDARD.encode(&data);
        let mut args = serde_json::json!({ "key": key, "data": encoded });

        if let Some(meta) = &metadata {
            let mut meta_obj = serde_json::Map::new();
            if let Some(ct) = &meta.content_type {
                meta_obj.insert("content_type".to_string(), Value::String(ct.clone()));
            }
            if let Some(ce) = &meta.content_encoding {
                meta_obj.insert("content_encoding".to_string(), Value::String(ce.clone()));
            }
            if !meta.custom.is_empty() {
                let custom: serde_json::Map<String, Value> = meta
                    .custom
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                    .collect();
                meta_obj.insert("custom".to_string(), Value::Object(custom));
            }
            args["metadata"] = Value::Object(meta_obj);
        }

        let _result = self.call_tool("objstore_put", args).await?;
        Ok(PutResponse {
            success: true,
            message: None,
            etag: None,
        })
    }

    /// Get an object from storage.
    pub async fn get(&self, key: &str) -> Result<(Bytes, Metadata)> {
        let args = serde_json::json!({ "key": key });
        let result = self.call_tool("objstore_get", args).await?;

        // The MCP server returns the object data base64-encoded in the "data"
        // field of the JSON tool result (matching how put encodes it).
        let raw_data = result["data"]
            .as_str()
            .ok_or_else(|| Error::InvalidResponse("missing data field".to_string()))?;

        let bytes = base64::engine::general_purpose::STANDARD
            .decode(raw_data)
            .map_err(|e| Error::InvalidResponse(format!("invalid base64 data: {e}")))?;

        let metadata = Metadata {
            size: bytes.len() as i64,
            content_type: result["content_type"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(String::from),
            content_encoding: result["content_encoding"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(String::from),
            ..Default::default()
        };

        Ok((Bytes::from(bytes), metadata))
    }

    /// Delete an object from storage.
    pub async fn delete(&self, key: &str) -> Result<DeleteResponse> {
        let args = serde_json::json!({ "key": key });
        self.call_tool("objstore_delete", args).await?;
        Ok(DeleteResponse {
            success: true,
            message: None,
        })
    }

    /// List objects with optional prefix filtering.
    pub async fn list(&self, list_req: ListRequest) -> Result<ListResponse> {
        let mut args = serde_json::json!({});
        if let Some(prefix) = &list_req.prefix {
            args["prefix"] = Value::String(prefix.clone());
        }
        if let Some(max) = list_req.max_results {
            args["max_results"] = Value::from(max);
        }
        if let Some(tok) = &list_req.continue_from {
            args["continue_from"] = Value::String(tok.clone());
        }

        let result = self.call_tool("objstore_list", args).await?;

        let keys: Vec<String> = result["keys"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let truncated = result["truncated"].as_bool().unwrap_or(false);
        let next_token = result["next_token"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(String::from);

        Ok(ListResponse {
            objects: keys
                .into_iter()
                .map(|k| ObjectInfo {
                    key: k,
                    metadata: Metadata::default(),
                })
                .collect(),
            common_prefixes: vec![],
            next_token,
            truncated,
        })
    }

    /// Check if an object exists.
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let args = serde_json::json!({ "key": key });
        let result = self.call_tool("objstore_exists", args).await?;
        Ok(result["exists"].as_bool().unwrap_or(false))
    }

    /// Get metadata for an object.
    pub async fn get_metadata(&self, key: &str) -> Result<Metadata> {
        let args = serde_json::json!({ "key": key });
        let result = self.call_tool("objstore_get_metadata", args).await?;

        let size = result["size"].as_i64().unwrap_or(0);
        let content_type = result["content_type"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(String::from);
        let content_encoding = result["content_encoding"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(String::from);
        let etag = result["etag"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(String::from);

        let custom = string_map(&result["custom"]);

        let last_modified = result["last_modified"]
            .as_str()
            .filter(|s| !s.is_empty())
            .and_then(|s| {
                chrono::DateTime::parse_from_rfc3339(s)
                    .ok()
                    .map(|dt| dt.with_timezone(&chrono::Utc))
            });

        Ok(Metadata {
            content_type,
            content_encoding,
            size,
            last_modified,
            etag,
            custom,
        })
    }

    /// Update metadata for an object.
    pub async fn update_metadata(&self, key: &str, metadata: Metadata) -> Result<()> {
        let mut meta_obj = serde_json::Map::new();
        if let Some(ct) = &metadata.content_type {
            meta_obj.insert("content_type".to_string(), Value::String(ct.clone()));
        }
        if let Some(ce) = &metadata.content_encoding {
            meta_obj.insert("content_encoding".to_string(), Value::String(ce.clone()));
        }
        if !metadata.custom.is_empty() {
            let custom: serde_json::Map<String, Value> = metadata
                .custom
                .into_iter()
                .map(|(k, v)| (k, Value::String(v)))
                .collect();
            meta_obj.insert("custom".to_string(), Value::Object(custom));
        }

        let args = serde_json::json!({
            "key": key,
            "metadata": Value::Object(meta_obj),
        });
        self.call_tool("objstore_update_metadata", args).await?;
        Ok(())
    }

    /// Health check.
    pub async fn health(&self) -> Result<HealthResponse> {
        let result = self
            .call_tool("objstore_health", serde_json::json!({}))
            .await?;
        let status_str = result["status"].as_str().unwrap_or("unknown");
        Ok(HealthResponse {
            status: match status_str {
                "healthy" | "serving" => HealthStatus::Serving,
                _ => HealthStatus::NotServing,
            },
            message: result["version"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(String::from),
        })
    }

    /// Archive an object to a different storage backend.
    pub async fn archive(
        &self,
        key: &str,
        destination_type: String,
        destination_settings: HashMap<String, String>,
    ) -> Result<()> {
        let args = serde_json::json!({
            "key": key,
            "destination_type": destination_type,
            "destination_settings": destination_settings,
        });
        self.call_tool("objstore_archive", args).await?;
        Ok(())
    }

    /// Add a lifecycle policy.
    pub async fn add_policy(&self, policy: LifecyclePolicy) -> Result<()> {
        let mut args = serde_json::json!({
            "id": policy.id,
            "prefix": policy.prefix,
            "retention_seconds": policy.retention_seconds,
            "action": policy.action,
        });
        if let Some(dt) = &policy.destination_type {
            args["destination_type"] = Value::String(dt.clone());
        }
        if !policy.destination_settings.is_empty() {
            args["destination_settings"] = serde_json::to_value(&policy.destination_settings)?;
        }
        self.call_tool("objstore_add_policy", args).await?;
        Ok(())
    }

    /// Remove a lifecycle policy.
    pub async fn remove_policy(&self, id: &str) -> Result<()> {
        let args = serde_json::json!({ "id": id });
        self.call_tool("objstore_remove_policy", args).await?;
        Ok(())
    }

    /// Get lifecycle policies, optionally filtered by prefix.
    pub async fn get_policies(&self, prefix: Option<String>) -> Result<Vec<LifecyclePolicy>> {
        let mut args = serde_json::json!({});
        if let Some(p) = prefix {
            args["prefix"] = Value::String(p);
        }
        let result = self.call_tool("objstore_get_policies", args).await?;
        let policies_val = if result.is_array() {
            result
        } else {
            result
                .get("policies")
                .cloned()
                .unwrap_or(Value::Array(vec![]))
        };
        let policies: Vec<Value> = serde_json::from_value(policies_val)?;
        Ok(policies
            .into_iter()
            .map(|v| LifecyclePolicy {
                id: v["id"].as_str().unwrap_or("").to_string(),
                prefix: v["prefix"].as_str().unwrap_or("").to_string(),
                retention_seconds: v["retention_seconds"].as_i64().unwrap_or(0),
                action: v["action"].as_str().unwrap_or("").to_string(),
                destination_type: v["destination_type"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .map(String::from),
                destination_settings: string_map(&v["destination_settings"]),
            })
            .collect())
    }

    /// Apply all lifecycle policies, returning (policies_count, objects_processed).
    pub async fn apply_policies(&self) -> Result<(i32, i32)> {
        let result = self
            .call_tool("objstore_apply_policies", serde_json::json!({}))
            .await?;
        let count = result["policies_count"].as_i64().unwrap_or(0) as i32;
        let processed = result["objects_processed"].as_i64().unwrap_or(0) as i32;
        Ok((count, processed))
    }

    /// Add a replication policy.
    pub async fn add_replication_policy(&self, policy: ReplicationPolicy) -> Result<()> {
        let mode = match policy.replication_mode {
            ReplicationMode::Transparent => "transparent",
            ReplicationMode::Opaque => "opaque",
        };
        let mut args = serde_json::json!({
            "id": policy.id,
            "source_backend": policy.source_backend,
            "destination_backend": policy.destination_backend,
            "check_interval": policy.check_interval_seconds,
            "enabled": policy.enabled,
            "replication_mode": mode,
        });
        if !policy.source_settings.is_empty() {
            args["source_settings"] = serde_json::to_value(&policy.source_settings)?;
        }
        if !policy.source_prefix.is_empty() {
            args["source_prefix"] = Value::String(policy.source_prefix);
        }
        if !policy.destination_settings.is_empty() {
            args["destination_settings"] = serde_json::to_value(&policy.destination_settings)?;
        }
        self.call_tool("objstore_add_replication_policy", args)
            .await?;
        Ok(())
    }

    /// Remove a replication policy.
    pub async fn remove_replication_policy(&self, id: &str) -> Result<()> {
        let args = serde_json::json!({ "id": id });
        self.call_tool("objstore_remove_replication_policy", args)
            .await?;
        Ok(())
    }

    /// Get all replication policies.
    pub async fn get_replication_policies(&self) -> Result<Vec<ReplicationPolicy>> {
        let result = self
            .call_tool("objstore_list_replication_policies", serde_json::json!({}))
            .await?;
        let policies_val = if result.is_array() {
            result
        } else {
            result
                .get("policies")
                .cloned()
                .unwrap_or(Value::Array(vec![]))
        };
        let policies: Vec<Value> = serde_json::from_value(policies_val)?;
        Ok(policies
            .into_iter()
            .map(mcp_replication_policy_from_value)
            .collect())
    }

    /// Get a specific replication policy.
    pub async fn get_replication_policy(&self, id: &str) -> Result<ReplicationPolicy> {
        let args = serde_json::json!({ "id": id });
        let result = self
            .call_tool("objstore_get_replication_policy", args)
            .await?;
        Ok(mcp_replication_policy_from_value(result))
    }

    /// Trigger replication synchronization.
    pub async fn trigger_replication(
        &self,
        policy_id: Option<String>,
        _parallel: bool,
        _worker_count: i32,
    ) -> Result<SyncResult> {
        let mut args = serde_json::json!({});
        if let Some(id) = &policy_id {
            args["policy_id"] = Value::String(id.clone());
        }
        let result = self.call_tool("objstore_trigger_replication", args).await?;

        let inner = result.get("result").unwrap_or(&result);
        Ok(SyncResult {
            policy_id: inner["policy_id"]
                .as_str()
                .or_else(|| policy_id.as_deref())
                .unwrap_or("")
                .to_string(),
            synced: inner["synced"].as_i64().unwrap_or(0) as i32,
            deleted: inner["deleted"].as_i64().unwrap_or(0) as i32,
            failed: inner["failed"].as_i64().unwrap_or(0) as i32,
            bytes_total: inner["bytes_total"].as_i64().unwrap_or(0),
            duration_ms: parse_go_duration_ms(inner["duration"].as_str().unwrap_or("")),
            errors: inner["errors"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
        })
    }

    /// Get replication status for a policy.
    pub async fn get_replication_status(&self, id: &str) -> Result<ReplicationStatus> {
        let args = serde_json::json!({ "policy_id": id });
        let result = self
            .call_tool("objstore_get_replication_status", args)
            .await?;

        Ok(ReplicationStatus {
            policy_id: result["policy_id"].as_str().unwrap_or(id).to_string(),
            source_backend: result["source_backend"].as_str().unwrap_or("").to_string(),
            destination_backend: result["destination_backend"]
                .as_str()
                .unwrap_or("")
                .to_string(),
            enabled: result["enabled"].as_bool().unwrap_or(false),
            total_objects_synced: result["total_objects_synced"].as_i64().unwrap_or(0),
            total_objects_deleted: result["total_objects_deleted"].as_i64().unwrap_or(0),
            total_bytes_synced: result["total_bytes_synced"].as_i64().unwrap_or(0),
            total_errors: result["total_errors"].as_i64().unwrap_or(0),
            last_sync_time: result["last_sync_time"]
                .as_str()
                .filter(|s| !s.is_empty())
                .and_then(|s| {
                    chrono::DateTime::parse_from_rfc3339(s)
                        .ok()
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                }),
            average_sync_duration_ms: parse_go_duration_ms(
                result["average_sync_duration"].as_str().unwrap_or(""),
            ),
            sync_count: result["sync_count"].as_i64().unwrap_or(0),
        })
    }

    /// Close the client.  The underlying connection pool is managed by reqwest
    /// and does not need explicit cleanup.
    pub async fn close(&self) -> Result<()> {
        Ok(())
    }
}

fn mcp_replication_policy_from_value(v: Value) -> ReplicationPolicy {
    let mode = match v["replication_mode"].as_str().unwrap_or("") {
        "opaque" => ReplicationMode::Opaque,
        _ => ReplicationMode::Transparent,
    };
    ReplicationPolicy {
        id: v["id"].as_str().unwrap_or("").to_string(),
        source_backend: v["source_backend"].as_str().unwrap_or("").to_string(),
        source_settings: string_map(&v["source_settings"]),
        source_prefix: v["source_prefix"].as_str().unwrap_or("").to_string(),
        destination_backend: v["destination_backend"].as_str().unwrap_or("").to_string(),
        destination_settings: string_map(&v["destination_settings"]),
        check_interval_seconds: v["check_interval"].as_i64().unwrap_or(0),
        last_sync_time: v["last_sync_time"]
            .as_str()
            .filter(|s| !s.is_empty())
            .and_then(|s| {
                chrono::DateTime::parse_from_rfc3339(s)
                    .ok()
                    .map(|dt| dt.with_timezone(&chrono::Utc))
            }),
        enabled: v["enabled"].as_bool().unwrap_or(false),
        encryption: None,
        replication_mode: mode,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::Server;

    // ── helpers ──────────────────────────────────────────────────────────────

    /// Wrap a JSON result value into an MCP-style tool response.
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

    /// Wrap an error into an MCP-style error response.
    fn mcp_err(code: i64, msg: &str) -> String {
        serde_json::json!({
            "jsonrpc": "2.0",
            "error": { "code": code, "message": msg },
            "id": 1
        })
        .to_string()
    }

    fn mcp_post(server: &mut Server, body: String) -> mockito::Mock {
        server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create()
    }

    fn client(url: String) -> McpClient {
        McpClient::new(url).unwrap()
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

    // ── constructor ───────────────────────────────────────────────────────────

    #[test]
    fn mcp_client_new_ok() {
        assert!(McpClient::new("http://localhost:8080").is_ok());
        assert!(McpClient::new("https://localhost:8443").is_ok());
    }

    #[test]
    fn mcp_client_new_with_auth() {
        let auth = AuthConfig {
            token: Some("tok".to_string()),
            tenant_id: Some("acme".to_string()),
            ..Default::default()
        };
        assert!(McpClient::new_with_auth("http://localhost:8080", auth).is_ok());
    }

    // ── health ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn mcp_health_success() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({ "status": "healthy", "version": "1.0.0" })),
        );
        let c = client(server.url());
        let h = c.health().await.unwrap();
        assert_eq!(h.status, HealthStatus::Serving);
        assert_eq!(h.message.as_deref(), Some("1.0.0"));
    }

    #[tokio::test]
    async fn mcp_health_not_serving() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({ "status": "degraded" })),
        );
        let c = client(server.url());
        let h = c.health().await.unwrap();
        assert_eq!(h.status, HealthStatus::NotServing);
    }

    // ── put ───────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn mcp_put_success() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({ "success": true, "key": "k", "size": 5 })),
        );
        let c = client(server.url());
        let r = c
            .put("k", Bytes::from_static(b"hello"), None)
            .await
            .unwrap();
        assert!(r.success);
    }

    #[tokio::test]
    async fn mcp_put_with_metadata() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(&mut server, mcp_ok(serde_json::json!({ "success": true })));
        let c = client(server.url());
        let mut meta = Metadata::default();
        meta.content_type = Some("text/plain".to_string());
        meta.custom.insert("owner".to_string(), "alice".to_string());
        c.put("k", Bytes::from_static(b"hi"), Some(meta))
            .await
            .unwrap();
    }

    // ── get ───────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn mcp_get_success() {
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"world");
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({
                "success": true,
                "data": encoded,
                "size": 5
            })),
        );
        let c = client(server.url());
        let (data, _meta) = c.get("k").await.unwrap();
        assert_eq!(&data[..], b"world");
    }

    #[tokio::test]
    async fn mcp_get_invalid_base64_errors() {
        // The server always base64-encodes object data; a non-base64 payload is
        // a protocol violation and must surface as an error, not be guessed at.
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({ "success": true, "data": "not*valid*base64" })),
        );
        let c = client(server.url());
        assert!(c.get("k").await.is_err());
    }

    // ── delete ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn mcp_delete_success() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({ "success": true, "deleted": true })),
        );
        let c = client(server.url());
        let r = c.delete("k").await.unwrap();
        assert!(r.success);
    }

    // ── list ──────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn mcp_list_success() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({
                "success": true,
                "keys": ["a/1", "a/2"],
                "count": 2,
                "truncated": false,
                "next_token": ""
            })),
        );
        let c = client(server.url());
        let r = c.list(ListRequest::default()).await.unwrap();
        assert_eq!(r.objects.len(), 2);
        assert_eq!(r.objects[0].key, "a/1");
        assert!(!r.truncated);
    }

    #[tokio::test]
    async fn mcp_list_truncated() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({
                "success": true,
                "keys": ["k1"],
                "truncated": true,
                "next_token": "tok"
            })),
        );
        let c = client(server.url());
        let r = c
            .list(ListRequest {
                prefix: Some("p/".to_string()),
                max_results: Some(1),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(r.truncated);
        assert_eq!(r.next_token.as_deref(), Some("tok"));
    }

    // ── exists ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn mcp_exists_true() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({ "success": true, "exists": true })),
        );
        let c = client(server.url());
        assert!(c.exists("k").await.unwrap());
    }

    #[tokio::test]
    async fn mcp_exists_false() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({ "success": true, "exists": false })),
        );
        let c = client(server.url());
        assert!(!c.exists("k").await.unwrap());
    }

    // ── get_metadata ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn mcp_get_metadata_success() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({
                "success": true,
                "key": "k",
                "size": 42,
                "content_type": "text/plain",
                "content_encoding": "",
                "etag": "\"abc\"",
                "custom": { "x": "y" }
            })),
        );
        let c = client(server.url());
        let meta = c.get_metadata("k").await.unwrap();
        assert_eq!(meta.size, 42);
        assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
        assert_eq!(meta.custom.get("x").map(String::as_str), Some("y"));
    }

    // ── update_metadata ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn mcp_update_metadata_success() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({ "success": true, "updated": true })),
        );
        let c = client(server.url());
        c.update_metadata("k", Metadata::default()).await.unwrap();
    }

    // ── archive ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn mcp_archive_success() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({ "success": true, "archived": true })),
        );
        let c = client(server.url());
        c.archive("k", "glacier".to_string(), HashMap::new())
            .await
            .unwrap();
    }

    // ── policy ops ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn mcp_add_policy_success() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({ "success": true, "added": true })),
        );
        let c = client(server.url());
        c.add_policy(LifecyclePolicy {
            id: "p1".to_string(),
            prefix: String::new(),
            retention_seconds: 86400,
            action: "delete".to_string(),
            destination_type: None,
            destination_settings: HashMap::new(),
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn mcp_remove_policy_success() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({ "success": true, "removed": true })),
        );
        let c = client(server.url());
        c.remove_policy("p1").await.unwrap();
    }

    #[tokio::test]
    async fn mcp_get_policies_success() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({
                "success": true,
                "policies": [
                    { "id": "p1", "prefix": "logs/", "retention_seconds": 3600, "action": "delete" }
                ]
            })),
        );
        let c = client(server.url());
        let ps = c.get_policies(None).await.unwrap();
        assert_eq!(ps.len(), 1);
        assert_eq!(ps[0].id, "p1");
    }

    #[tokio::test]
    async fn mcp_apply_policies_success() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({
                "success": true,
                "policies_count": 3,
                "objects_processed": 7
            })),
        );
        let c = client(server.url());
        let (count, processed) = c.apply_policies().await.unwrap();
        assert_eq!(count, 3);
        assert_eq!(processed, 7);
    }

    // ── replication ops ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn mcp_add_replication_policy_success() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(&mut server, mcp_ok(serde_json::json!({ "success": true })));
        let c = client(server.url());
        c.add_replication_policy(sample_replication_policy())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn mcp_remove_replication_policy_success() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(&mut server, mcp_ok(serde_json::json!({ "success": true })));
        let c = client(server.url());
        c.remove_replication_policy("r1").await.unwrap();
    }

    #[tokio::test]
    async fn mcp_get_replication_policies_success() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({
                "success": true,
                "policies": [
                    {
                        "id": "r1",
                        "source_backend": "s3",
                        "destination_backend": "gcs",
                        "check_interval": 300,
                        "enabled": true,
                        "replication_mode": "opaque"
                    }
                ]
            })),
        );
        let c = client(server.url());
        let ps = c.get_replication_policies().await.unwrap();
        assert_eq!(ps.len(), 1);
        assert_eq!(ps[0].id, "r1");
        assert_eq!(ps[0].replication_mode, ReplicationMode::Opaque);
    }

    #[tokio::test]
    async fn mcp_get_replication_policy_success() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({
                "id": "r1",
                "source_backend": "s3",
                "destination_backend": "gcs",
                "check_interval": 120,
                "enabled": true,
                "replication_mode": "transparent"
            })),
        );
        let c = client(server.url());
        let p = c.get_replication_policy("r1").await.unwrap();
        assert_eq!(p.id, "r1");
        assert_eq!(p.replication_mode, ReplicationMode::Transparent);
    }

    #[tokio::test]
    async fn mcp_trigger_replication_success() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({
                "success": true,
                "result": {
                    "policy_id": "r1",
                    "synced": 5,
                    "deleted": 1,
                    "failed": 0,
                    "bytes_total": 1024,
                    "duration": "1.5s",
                    "errors": []
                }
            })),
        );
        let c = client(server.url());
        let r = c
            .trigger_replication(Some("r1".to_string()), false, 1)
            .await
            .unwrap();
        assert_eq!(r.policy_id, "r1");
        assert_eq!(r.synced, 5);
        assert_eq!(r.bytes_total, 1024);
        assert_eq!(r.duration_ms, 1500);
    }

    #[tokio::test]
    async fn mcp_get_replication_status_success() {
        let mut server = Server::new_async().await;
        let _m = mcp_post(
            &mut server,
            mcp_ok(serde_json::json!({
                "policy_id": "r1",
                "source_backend": "s3",
                "destination_backend": "gcs",
                "enabled": true,
                "total_objects_synced": 20,
                "total_objects_deleted": 2,
                "total_bytes_synced": 4096,
                "total_errors": 0,
                "sync_count": 5,
                "average_sync_duration": "2s"
            })),
        );
        let c = client(server.url());
        let s = c.get_replication_status("r1").await.unwrap();
        assert_eq!(s.policy_id, "r1");
        assert_eq!(s.total_objects_synced, 20);
        assert_eq!(s.average_sync_duration_ms, 2000);
    }

    // ── error paths ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn mcp_rpc_error_surfaces_as_operation_failed() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mcp_err(-32000, "internal error"))
            .create();
        let c = client(server.url());
        let err = c.health().await.unwrap_err();
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[tokio::test]
    async fn mcp_not_found_error() {
        // -32004 is the server's not-found code; mapping is code-based.
        let mut server = Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mcp_err(-32004, "object not found: k"))
            .create();
        let c = client(server.url());
        let err = c.get("k").await.unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[tokio::test]
    async fn mcp_not_found_message_with_generic_code_is_operation_failed() {
        // Mapping is code-based, never message-based: a "not found" message
        // with a generic error code must not become Error::NotFound.
        let mut server = Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mcp_err(-32603, "not found: k"))
            .create();
        let c = client(server.url());
        let err = c.get("k").await.unwrap_err();
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[tokio::test]
    async fn mcp_forbidden_error() {
        // -32001 is the server's authorization-denied code and must not be
        // misreported as NotFound.
        let mut server = Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mcp_err(-32001, "forbidden"))
            .create();
        let c = client(server.url());
        let err = c.get("k").await.unwrap_err();
        assert!(matches!(err, Error::Forbidden(_)));
    }

    #[tokio::test]
    async fn mcp_unauthenticated_error() {
        // -32002 is the server's unauthenticated code.
        let mut server = Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mcp_err(-32002, "unauthenticated"))
            .create();
        let c = client(server.url());
        let err = c.get("k").await.unwrap_err();
        assert!(matches!(err, Error::Unauthenticated(_)));
    }

    #[tokio::test]
    async fn mcp_already_exists_error() {
        // -32005 is the server's already-exists code.
        let mut server = Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mcp_err(-32005, "object already exists: k"))
            .create();
        let c = client(server.url());
        let err = c.get("k").await.unwrap_err();
        assert!(matches!(err, Error::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn mcp_rate_limited_error() {
        // -32029 is the server's rate-limited code.
        let mut server = Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mcp_err(-32029, "rate limited"))
            .create();
        let c = client(server.url());
        let err = c.get("k").await.unwrap_err();
        assert!(matches!(err, Error::RateLimited(_)));
    }

    #[tokio::test]
    async fn mcp_invalid_params_error() {
        // -32602 is the JSON-RPC invalid-params code.
        let mut server = Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mcp_err(-32602, "invalid params"))
            .create();
        let c = client(server.url());
        let err = c.get("k").await.unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn mcp_http_error_surfaces_as_operation_failed() {
        let mut server = Server::new_async().await;
        let _m = server.mock("POST", "/").with_status(500).create();
        let c = client(server.url());
        let err = c.health().await.unwrap_err();
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[tokio::test]
    async fn mcp_close_is_noop() {
        let c = McpClient::new("http://localhost:9").unwrap();
        c.close().await.unwrap();
    }

    // ── auth header injection ─────────────────────────────────────────────────

    #[tokio::test]
    async fn mcp_auth_token_sent() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .match_header("authorization", "Bearer mytoken")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mcp_ok(serde_json::json!({ "status": "healthy" })))
            .create();
        let auth = AuthConfig {
            token: Some("mytoken".to_string()),
            ..Default::default()
        };
        let c = McpClient::new_with_auth(server.url(), auth).unwrap();
        c.health().await.unwrap();
    }

    #[tokio::test]
    async fn mcp_tenant_id_header_sent() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .match_header("x-tenant-id", "acme")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mcp_ok(serde_json::json!({ "status": "healthy" })))
            .create();
        let auth = AuthConfig {
            tenant_id: Some("acme".to_string()),
            ..Default::default()
        };
        let c = McpClient::new_with_auth(server.url(), auth).unwrap();
        c.health().await.unwrap();
    }
}
