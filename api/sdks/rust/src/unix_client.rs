use crate::error::{Error, Result};
use crate::jsonrpc::{string_map, JsonRpcRequest, JsonRpcResponse};
use crate::types::*;
use base64::Engine as _;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufStream};
use tokio::net::UnixStream;
use tokio::sync::Mutex;

/// Client for the go-objstore Unix-domain socket JSON-RPC 2.0 server.
///
/// The server speaks newline-delimited JSON-RPC 2.0 over a persistent Unix
/// socket connection.  The client holds one connection, connecting lazily on
/// the first call and reconnecting transparently when the server closes an
/// idle connection (the server's idle read deadline is ~30s).  Binary object
/// data is base64-encoded in transit.  Authentication is handled server-side
/// via peer credentials; the client sends no auth headers.
///
/// # Example
///
/// ```no_run
/// use go_objstore::UnixClient;
/// use bytes::Bytes;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = UnixClient::new("/var/run/objstore.sock")?;
/// let resp = client.put("hello.txt", Bytes::from("world"), None).await?;
/// assert!(resp.success);
/// # Ok(())
/// # }
/// ```
pub struct UnixClient {
    socket_path: PathBuf,
    next_id: AtomicU64,
    /// Persistent connection, established lazily and serialized via the mutex.
    conn: Mutex<Option<BufStream<UnixStream>>>,
}

// ── response shapes from the server (protocol.go) ─────────────────────────────

#[derive(Debug, Deserialize)]
struct GetResult {
    data: String, // base64
    metadata: Option<MetadataResult>,
}

#[derive(Debug, Deserialize)]
struct MetadataResult {
    content_type: Option<String>,
    content_encoding: Option<String>,
    #[serde(default)]
    custom: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
struct ExistsResult {
    exists: bool,
}

#[derive(Debug, Deserialize)]
struct ListResult {
    objects: Vec<ObjectInfoResult>,
    next_cursor: Option<String>,
    is_truncated: bool,
}

#[derive(Debug, Deserialize)]
struct ObjectInfoResult {
    key: String,
    size: i64,
    last_modified: Option<String>,
    etag: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ApplyPoliciesResult {
    policies_count: i32,
    objects_processed: i32,
}

#[derive(Debug, Deserialize)]
struct TriggerReplicationResult {
    objects_synced: i32,
    objects_failed: i32,
    bytes_transferred: i64,
    errors: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct ReplicationStatusResult {
    policy_id: String,
    #[serde(default)]
    #[allow(dead_code)]
    status: String,
    last_sync_time: Option<String>,
    objects_synced: i32,
    #[allow(dead_code)]
    objects_pending: i32,
    objects_failed: i32,
}

#[derive(Debug, Deserialize)]
struct HealthResult {
    status: String,
    version: Option<String>,
}

impl UnixClient {
    /// Create a new Unix-socket client pointing at `socket_path`.
    pub fn new(socket_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            socket_path: socket_path.as_ref().to_path_buf(),
            next_id: AtomicU64::new(1),
            conn: Mutex::new(None),
        })
    }

    // ── low-level RPC ──────────────────────────────────────────────────────

    /// Write one newline-delimited request and read one response line on the
    /// persistent connection.
    async fn round_trip(stream: &mut BufStream<UnixStream>, line: &str) -> Result<String> {
        stream.write_all(line.as_bytes()).await?;
        stream.flush().await?;

        let mut response_line = String::new();
        let n = stream.read_line(&mut response_line).await?;
        if n == 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed by server",
            )));
        }
        Ok(response_line)
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);

        let request = JsonRpcRequest::new(method, params, id);
        let mut line = serde_json::to_string(&request)?;
        line.push('\n');

        // Serialize request/response pairs over the single persistent
        // connection.  Connect lazily; the server closes idle connections
        // after ~30s, so a request that fails on a reused connection is
        // retried once on a fresh one.
        let mut guard = self.conn.lock().await;
        let reused = guard.is_some();
        if guard.is_none() {
            *guard = Some(BufStream::new(
                UnixStream::connect(&self.socket_path).await?,
            ));
        }

        let response_line =
            match Self::round_trip(guard.as_mut().expect("connection established above"), &line)
                .await
            {
                Ok(response_line) => response_line,
                Err(e) => {
                    // Drop the broken connection.  If it was a reused (possibly
                    // idle-closed) connection, reconnect and retry once.
                    *guard = None;
                    if !reused {
                        return Err(e);
                    }
                    *guard = Some(BufStream::new(
                        UnixStream::connect(&self.socket_path).await?,
                    ));
                    match Self::round_trip(
                        guard.as_mut().expect("connection established above"),
                        &line,
                    )
                    .await
                    {
                        Ok(response_line) => response_line,
                        Err(e) => {
                            *guard = None;
                            return Err(e);
                        }
                    }
                }
            };

        let response: JsonRpcResponse<Value> = match serde_json::from_str(response_line.trim()) {
            Ok(response) => response,
            Err(e) => {
                // Unparseable framing leaves the connection in an unknown
                // state; drop it so the next call reconnects.
                *guard = None;
                return Err(Error::Serialization(e));
            }
        };

        if response.id.as_u64() != Some(id) {
            // A mismatched id means request/response pairing is broken; drop
            // the connection so the next call starts clean.
            *guard = None;
            return Err(Error::InvalidResponse(format!(
                "response id {} does not match request id {}",
                response.id, id
            )));
        }
        drop(guard);

        if let Some(err) = response.error {
            return Err(err.into_error());
        }

        response
            .result
            .ok_or_else(|| Error::InvalidResponse("missing result".to_string()))
    }

    // ── public API (matching RestClient / GrpcClient surface) ─────────────

    /// Put an object into storage.  Binary data is base64-encoded.
    pub async fn put(
        &self,
        key: &str,
        data: Bytes,
        metadata: Option<Metadata>,
    ) -> Result<PutResponse> {
        let encoded = base64::engine::general_purpose::STANDARD.encode(&data);

        let meta_value = metadata.as_ref().map(|m| {
            let mut obj = serde_json::Map::new();
            if let Some(ct) = &m.content_type {
                obj.insert("content_type".to_string(), Value::String(ct.clone()));
            }
            if let Some(ce) = &m.content_encoding {
                obj.insert("content_encoding".to_string(), Value::String(ce.clone()));
            }
            if !m.custom.is_empty() {
                let custom: serde_json::Map<String, Value> = m
                    .custom
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                    .collect();
                obj.insert("custom".to_string(), Value::Object(custom));
            }
            Value::Object(obj)
        });

        let mut params = serde_json::json!({ "key": key, "data": encoded });
        if let Some(meta) = meta_value {
            params["metadata"] = meta;
        }

        let _result = self.call("put", params).await?;
        Ok(PutResponse {
            success: true,
            message: None,
            etag: None,
        })
    }

    /// Get an object from storage.  The returned `Bytes` is decoded from base64.
    pub async fn get(&self, key: &str) -> Result<(Bytes, Metadata)> {
        let params = serde_json::json!({ "key": key });
        let result = self.call("get", params).await?;

        let get: GetResult = serde_json::from_value(result)?;
        let data = base64::engine::general_purpose::STANDARD
            .decode(&get.data)
            .map_err(|e| Error::InvalidResponse(format!("base64 decode: {e}")))?;

        let metadata = get.metadata.map(metadata_from_result).unwrap_or_default();
        Ok((Bytes::from(data), metadata))
    }

    /// Delete an object from storage.
    pub async fn delete(&self, key: &str) -> Result<DeleteResponse> {
        let params = serde_json::json!({ "key": key });
        let result = self.call("delete", params).await;

        match result {
            Ok(_) => Ok(DeleteResponse {
                success: true,
                message: None,
            }),
            Err(e) => Err(e),
        }
    }

    /// List objects with optional prefix filtering.
    pub async fn list(&self, list_req: ListRequest) -> Result<ListResponse> {
        let mut params = serde_json::json!({});
        if let Some(prefix) = &list_req.prefix {
            params["prefix"] = Value::String(prefix.clone());
        }
        if let Some(delimiter) = &list_req.delimiter {
            params["delimiter"] = Value::String(delimiter.clone());
        }
        if let Some(max) = list_req.max_results {
            params["max_results"] = Value::from(max);
        }
        if let Some(tok) = &list_req.continue_from {
            params["continue_from"] = Value::String(tok.clone());
        }

        let result = self.call("list", params).await?;
        let list: ListResult = serde_json::from_value(result)?;

        Ok(ListResponse {
            objects: list
                .objects
                .into_iter()
                .map(|o| ObjectInfo {
                    key: o.key,
                    metadata: Metadata {
                        size: o.size,
                        etag: if o.etag.as_deref().unwrap_or("").is_empty() {
                            None
                        } else {
                            o.etag
                        },
                        last_modified: o.last_modified.and_then(|s| {
                            chrono::DateTime::parse_from_rfc3339(&s)
                                .ok()
                                .map(|dt| dt.with_timezone(&chrono::Utc))
                        }),
                        ..Default::default()
                    },
                })
                .collect(),
            common_prefixes: vec![],
            next_token: if list.next_cursor.as_deref().unwrap_or("").is_empty() {
                None
            } else {
                list.next_cursor
            },
            truncated: list.is_truncated,
        })
    }

    /// Check if an object exists.
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let params = serde_json::json!({ "key": key });
        let result = self.call("exists", params).await?;
        let er: ExistsResult = serde_json::from_value(result)?;
        Ok(er.exists)
    }

    /// Get metadata for an object.
    pub async fn get_metadata(&self, key: &str) -> Result<Metadata> {
        let params = serde_json::json!({ "key": key });
        let result = self.call("get_metadata", params).await?;
        let meta: MetadataResult = serde_json::from_value(result)?;
        Ok(metadata_from_result(meta))
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

        let params = serde_json::json!({
            "key": key,
            "metadata": Value::Object(meta_obj),
        });
        self.call("update_metadata", params).await?;
        Ok(())
    }

    /// Health check.
    pub async fn health(&self) -> Result<HealthResponse> {
        let result = self.call("health", serde_json::json!({})).await?;
        let h: HealthResult = serde_json::from_value(result)?;
        Ok(HealthResponse {
            status: match h.status.as_str() {
                "healthy" | "serving" => HealthStatus::Serving,
                _ => HealthStatus::NotServing,
            },
            message: h.version,
        })
    }

    /// Archive an object to a different storage backend.
    pub async fn archive(
        &self,
        key: &str,
        destination_type: String,
        destination_settings: HashMap<String, String>,
    ) -> Result<()> {
        let params = serde_json::json!({
            "key": key,
            "destination_type": destination_type,
            "destination_settings": destination_settings,
        });
        self.call("archive", params).await?;
        Ok(())
    }

    /// Add a lifecycle policy.
    ///
    /// Retention is sent as `retention_seconds`; the server gives it
    /// precedence over the legacy whole-day `after_days` field.
    pub async fn add_policy(&self, policy: LifecyclePolicy) -> Result<()> {
        let params = serde_json::json!({
            "id": policy.id,
            "prefix": policy.prefix,
            "action": policy.action,
            "retention_seconds": policy.retention_seconds,
        });
        self.call("add_policy", params).await?;
        Ok(())
    }

    /// Remove a lifecycle policy.
    pub async fn remove_policy(&self, id: &str) -> Result<()> {
        let params = serde_json::json!({ "id": id });
        self.call("remove_policy", params).await?;
        Ok(())
    }

    /// Get lifecycle policies.
    pub async fn get_policies(&self, prefix: Option<String>) -> Result<Vec<LifecyclePolicy>> {
        let mut params = serde_json::json!({});
        if let Some(p) = prefix {
            params["prefix"] = Value::String(p);
        }
        let result = self.call("get_policies", params).await?;
        // The server returns an array or an object with a "policies" key.
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
                // Prefer the exact retention_seconds field; fall back to the
                // legacy whole-day after_days field.
                retention_seconds: v["retention_seconds"]
                    .as_i64()
                    .unwrap_or_else(|| v["after_days"].as_i64().unwrap_or(0) * 86400),
                action: v["action"].as_str().unwrap_or("").to_string(),
                destination_type: None,
                destination_settings: HashMap::new(),
            })
            .collect())
    }

    /// Apply all lifecycle policies.
    pub async fn apply_policies(&self) -> Result<(i32, i32)> {
        let result = self.call("apply_policies", serde_json::json!({})).await?;
        let r: ApplyPoliciesResult = serde_json::from_value(result)?;
        Ok((r.policies_count, r.objects_processed))
    }

    /// Add a replication policy.
    pub async fn add_replication_policy(&self, policy: ReplicationPolicy) -> Result<()> {
        let mode = match policy.replication_mode {
            ReplicationMode::Opaque => "opaque",
            ReplicationMode::Transparent => "transparent",
        };
        let params = serde_json::json!({
            "id": policy.id,
            "source_prefix": policy.source_prefix,
            "destination_type": policy.destination_backend,
            "destination": policy.destination_settings,
            "enabled": policy.enabled,
            "replication_mode": mode,
        });
        self.call("add_replication_policy", params).await?;
        Ok(())
    }

    /// Remove a replication policy.
    pub async fn remove_replication_policy(&self, id: &str) -> Result<()> {
        let params = serde_json::json!({ "id": id });
        self.call("remove_replication_policy", params).await?;
        Ok(())
    }

    /// Get all replication policies.
    pub async fn get_replication_policies(&self) -> Result<Vec<ReplicationPolicy>> {
        let result = self
            .call("get_replication_policies", serde_json::json!({}))
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
            .map(replication_policy_from_value)
            .collect())
    }

    /// Get a specific replication policy.
    pub async fn get_replication_policy(&self, id: &str) -> Result<ReplicationPolicy> {
        let params = serde_json::json!({ "id": id });
        let result = self.call("get_replication_policy", params).await?;
        Ok(replication_policy_from_value(result))
    }

    /// Trigger replication.
    pub async fn trigger_replication(
        &self,
        policy_id: Option<String>,
        _parallel: bool,
        _worker_count: i32,
    ) -> Result<SyncResult> {
        let mut params = serde_json::json!({});
        if let Some(id) = &policy_id {
            params["id"] = Value::String(id.clone());
        }
        let result = self.call("trigger_replication", params).await?;
        let r: TriggerReplicationResult = serde_json::from_value(result)?;
        Ok(SyncResult {
            policy_id: policy_id.unwrap_or_default(),
            synced: r.objects_synced,
            deleted: 0,
            failed: r.objects_failed,
            bytes_total: r.bytes_transferred,
            duration_ms: 0,
            errors: r.errors.unwrap_or_default(),
        })
    }

    /// Get replication status.
    pub async fn get_replication_status(&self, id: &str) -> Result<ReplicationStatus> {
        let params = serde_json::json!({ "id": id });
        let result = self.call("get_replication_status", params).await?;
        let r: ReplicationStatusResult = serde_json::from_value(result)?;
        Ok(ReplicationStatus {
            policy_id: r.policy_id,
            source_backend: String::new(),
            destination_backend: String::new(),
            enabled: true,
            total_objects_synced: r.objects_synced as i64,
            total_objects_deleted: 0,
            total_bytes_synced: 0,
            total_errors: r.objects_failed as i64,
            last_sync_time: r.last_sync_time.and_then(|s| {
                chrono::DateTime::parse_from_rfc3339(&s)
                    .ok()
                    .map(|dt| dt.with_timezone(&chrono::Utc))
            }),
            average_sync_duration_ms: 0,
            sync_count: 0,
        })
    }

    /// Close the client, dropping the persistent connection if one is open.
    /// A subsequent call reconnects automatically.
    pub async fn close(&self) -> Result<()> {
        *self.conn.lock().await = None;
        Ok(())
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn metadata_from_result(m: MetadataResult) -> Metadata {
    Metadata {
        content_type: m.content_type.filter(|s| !s.is_empty()),
        content_encoding: m.content_encoding.filter(|s| !s.is_empty()),
        size: 0,
        last_modified: None,
        etag: None,
        custom: m.custom.unwrap_or_default(),
    }
}

fn replication_policy_from_value(v: Value) -> ReplicationPolicy {
    let mode = match v["replication_mode"].as_str().unwrap_or("") {
        "opaque" => ReplicationMode::Opaque,
        _ => ReplicationMode::Transparent,
    };
    let dest_settings = string_map(&v["destination"]);

    ReplicationPolicy {
        id: v["id"].as_str().unwrap_or("").to_string(),
        source_backend: String::new(),
        source_settings: HashMap::new(),
        source_prefix: v["source_prefix"].as_str().unwrap_or("").to_string(),
        destination_backend: v["destination_type"].as_str().unwrap_or("").to_string(),
        destination_settings: dest_settings,
        check_interval_seconds: 0,
        last_sync_time: None,
        enabled: v["enabled"].as_bool().unwrap_or(false),
        encryption: None,
        replication_mode: mode,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, Write};
    use std::os::unix::net::UnixListener;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Arc, Mutex as StdMutex};
    use std::thread;

    // ── constructor ──────────────────────────────────────────────────────────

    #[test]
    fn unix_client_new_ok() {
        // Construction succeeds even when no socket exists yet.
        assert!(UnixClient::new("/tmp/does-not-exist.sock").is_ok());
    }

    // ── helpers unit tests ───────────────────────────────────────────────────

    #[test]
    fn metadata_from_result_maps_fields() {
        let r = MetadataResult {
            content_type: Some("text/plain".to_string()),
            content_encoding: Some("gzip".to_string()),
            custom: Some({
                let mut m = HashMap::new();
                m.insert("k".to_string(), "v".to_string());
                m
            }),
        };
        let meta = metadata_from_result(r);
        assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
        assert_eq!(meta.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(meta.custom.get("k").map(String::as_str), Some("v"));
    }

    #[test]
    fn metadata_from_result_empty_strings_become_none() {
        let r = MetadataResult {
            content_type: Some(String::new()),
            content_encoding: Some(String::new()),
            custom: None,
        };
        let meta = metadata_from_result(r);
        assert!(meta.content_type.is_none());
        assert!(meta.content_encoding.is_none());
        assert!(meta.custom.is_empty());
    }

    #[test]
    fn replication_policy_from_value_transparent() {
        let v = serde_json::json!({
            "id": "p1",
            "source_prefix": "data/",
            "destination_type": "gcs",
            "destination": { "bucket": "bk" },
            "enabled": true,
            "replication_mode": "transparent",
        });
        let p = replication_policy_from_value(v);
        assert_eq!(p.id, "p1");
        assert_eq!(p.replication_mode, ReplicationMode::Transparent);
        assert!(p.enabled);
        assert_eq!(
            p.destination_settings.get("bucket").map(String::as_str),
            Some("bk")
        );
    }

    #[test]
    fn replication_policy_from_value_opaque() {
        let v = serde_json::json!({ "replication_mode": "opaque" });
        let p = replication_policy_from_value(v);
        assert_eq!(p.replication_mode, ReplicationMode::Opaque);
    }

    // ── mock-server RPC tests ────────────────────────────────────────────────

    /// Handle to an in-process Unix-socket JSON-RPC mock server.
    struct MockServer {
        path: PathBuf,
        /// Number of connections the server has accepted.
        connections: Arc<AtomicUsize>,
        /// Raw request objects received, in order.
        requests: Arc<StdMutex<Vec<Value>>>,
    }

    /// Spawn a mock server speaking persistent newline-delimited JSON-RPC.
    ///
    /// For every request line received the server replies with `template` (a
    /// JSON object containing either a "result" or an "error" key) after
    /// filling in the protocol version and echoing the request id (unless
    /// `id_override` forces a fixed id).  When `close_after_each` is true the
    /// connection is closed after every response, simulating the server's
    /// idle-timeout behavior.
    fn spawn_mock_server_full(
        template: Value,
        close_after_each: bool,
        id_override: Option<u64>,
    ) -> MockServer {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.sock");
        let listener = UnixListener::bind(&path).expect("bind");
        let connections = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(StdMutex::new(Vec::new()));

        let server = MockServer {
            path,
            connections: Arc::clone(&connections),
            requests: Arc::clone(&requests),
        };

        thread::spawn(move || {
            // Keep the tempdir (and socket path) alive for the test duration.
            let _dir = dir;
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                connections.fetch_add(1, Ordering::SeqCst);
                let Ok(read_half) = stream.try_clone() else {
                    break;
                };
                let reader = std::io::BufReader::new(read_half);
                let mut writer = stream;
                for line in reader.lines() {
                    let Ok(line) = line else { break };
                    let Ok(req) = serde_json::from_str::<Value>(&line) else {
                        break;
                    };
                    let mut resp = template.clone();
                    resp["jsonrpc"] = Value::from("2.0");
                    resp["id"] = match id_override {
                        Some(id) => Value::from(id),
                        None => req["id"].clone(),
                    };
                    requests.lock().unwrap().push(req);
                    let mut body = resp.to_string();
                    body.push('\n');
                    if writer.write_all(body.as_bytes()).is_err() {
                        break;
                    }
                    if close_after_each {
                        break;
                    }
                }
            }
        });
        server
    }

    fn spawn_mock_server(template: Value) -> MockServer {
        spawn_mock_server_full(template, false, None)
    }

    fn make_result(value: serde_json::Value) -> Value {
        serde_json::json!({ "result": value })
    }

    fn make_error(code: i64, message: &str) -> Value {
        serde_json::json!({ "error": { "code": code, "message": message } })
    }

    #[tokio::test]
    async fn unix_health_success() {
        let resp = make_result(serde_json::json!({
            "status": "healthy",
            "version": "1.2.3"
        }));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let health = client.health().await.unwrap();
        assert_eq!(health.status, HealthStatus::Serving);
        assert_eq!(health.message.as_deref(), Some("1.2.3"));
    }

    #[tokio::test]
    async fn unix_put_success() {
        let resp = make_result(serde_json::json!({ "success": true }));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let r = client
            .put("k", Bytes::from_static(b"hello"), None)
            .await
            .unwrap();
        assert!(r.success);
    }

    #[tokio::test]
    async fn unix_get_success() {
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"world");
        let resp = make_result(serde_json::json!({
            "data": encoded,
            "metadata": { "content_type": "text/plain", "custom": {} }
        }));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let (data, meta) = client.get("k").await.unwrap();
        assert_eq!(&data[..], b"world");
        assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
    }

    #[tokio::test]
    async fn unix_delete_success() {
        let resp = make_result(serde_json::json!({ "success": true }));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let r = client.delete("k").await.unwrap();
        assert!(r.success);
    }

    #[tokio::test]
    async fn unix_exists_true() {
        let resp = make_result(serde_json::json!({ "exists": true }));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        assert!(client.exists("k").await.unwrap());
    }

    #[tokio::test]
    async fn unix_exists_false() {
        let resp = make_result(serde_json::json!({ "exists": false }));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        assert!(!client.exists("k").await.unwrap());
    }

    #[tokio::test]
    async fn unix_list_success() {
        let resp = make_result(serde_json::json!({
            "objects": [{ "key": "a/1", "size": 10, "last_modified": null, "etag": "e1" }],
            "next_cursor": "",
            "is_truncated": false
        }));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let list = client.list(ListRequest::default()).await.unwrap();
        assert_eq!(list.objects.len(), 1);
        assert_eq!(list.objects[0].key, "a/1");
        assert!(!list.truncated);
    }

    #[tokio::test]
    async fn unix_get_metadata_success() {
        let resp = make_result(serde_json::json!({
            "content_type": "application/json",
            "content_encoding": null,
            "custom": { "owner": "alice" }
        }));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let meta = client.get_metadata("k").await.unwrap();
        assert_eq!(meta.content_type.as_deref(), Some("application/json"));
        assert_eq!(meta.custom.get("owner").map(String::as_str), Some("alice"));
    }

    #[tokio::test]
    async fn unix_update_metadata_success() {
        let resp = make_result(serde_json::json!({ "success": true }));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        client
            .update_metadata("k", Metadata::default())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn unix_archive_success() {
        let resp = make_result(serde_json::json!({ "success": true }));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        client
            .archive("k", "glacier".to_string(), HashMap::new())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn unix_add_policy_success() {
        let resp = make_result(serde_json::json!({ "success": true }));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        client
            .add_policy(LifecyclePolicy {
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
    async fn unix_remove_policy_success() {
        let resp = make_result(serde_json::json!({ "success": true }));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        client.remove_policy("p1").await.unwrap();
    }

    #[tokio::test]
    async fn unix_apply_policies_success() {
        let resp = make_result(serde_json::json!({
            "policies_count": 2,
            "objects_processed": 5
        }));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let (count, processed) = client.apply_policies().await.unwrap();
        assert_eq!(count, 2);
        assert_eq!(processed, 5);
    }

    #[tokio::test]
    async fn unix_add_replication_policy_success() {
        let resp = make_result(serde_json::json!({ "success": true }));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        client
            .add_replication_policy(ReplicationPolicy {
                id: "r1".to_string(),
                source_backend: "s3".to_string(),
                source_settings: HashMap::new(),
                source_prefix: "data/".to_string(),
                destination_backend: "gcs".to_string(),
                destination_settings: HashMap::new(),
                check_interval_seconds: 300,
                last_sync_time: None,
                enabled: true,
                encryption: None,
                replication_mode: ReplicationMode::Opaque,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn unix_remove_replication_policy_success() {
        let resp = make_result(serde_json::json!({ "success": true }));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        client.remove_replication_policy("r1").await.unwrap();
    }

    #[tokio::test]
    async fn unix_trigger_replication_success() {
        let resp = make_result(serde_json::json!({
            "objects_synced": 3,
            "objects_failed": 0,
            "bytes_transferred": 1024,
        }));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let r = client
            .trigger_replication(Some("r1".to_string()), false, 1)
            .await
            .unwrap();
        assert_eq!(r.synced, 3);
        assert_eq!(r.bytes_total, 1024);
    }

    #[tokio::test]
    async fn unix_get_replication_status_success() {
        let resp = make_result(serde_json::json!({
            "policy_id": "r1",
            "status": "active",
            "objects_synced": 10,
            "objects_pending": 1,
            "objects_failed": 0,
        }));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let s = client.get_replication_status("r1").await.unwrap();
        assert_eq!(s.policy_id, "r1");
        assert_eq!(s.total_objects_synced, 10);
    }

    #[tokio::test]
    async fn unix_error_surfaces_as_operation_failed() {
        let resp = make_error(-32000, "internal error");
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let err = client.health().await.unwrap_err();
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[tokio::test]
    async fn unix_not_found_error() {
        // -32004 is the server's not-found code; mapping is code-based.
        let resp = make_error(-32004, "object not found: k");
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let err = client.get("k").await.unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[tokio::test]
    async fn unix_not_found_message_with_generic_code_is_operation_failed() {
        // Mapping is code-based, never message-based: a "not found" message
        // with a generic error code must not become Error::NotFound.
        let resp = make_error(-32603, "object not found: k");
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let err = client.get("k").await.unwrap_err();
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[tokio::test]
    async fn unix_forbidden_error() {
        // -32001 is the server's authorization-denied code.
        let resp = make_error(-32001, "forbidden");
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let err = client.get("k").await.unwrap_err();
        assert!(matches!(err, Error::Forbidden(_)));
    }

    #[tokio::test]
    async fn unix_unauthenticated_error() {
        // -32002 is the server's unauthenticated code.
        let resp = make_error(-32002, "unauthenticated");
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let err = client.get("k").await.unwrap_err();
        assert!(matches!(err, Error::Unauthenticated(_)));
    }

    #[tokio::test]
    async fn unix_already_exists_error() {
        // -32005 is the server's already-exists code.
        let resp = make_error(-32005, "object already exists: k");
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let err = client.get("k").await.unwrap_err();
        assert!(matches!(err, Error::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn unix_rate_limited_error() {
        // -32029 is the server's rate-limited code.
        let resp = make_error(-32029, "rate limited");
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let err = client.get("k").await.unwrap_err();
        assert!(matches!(err, Error::RateLimited(_)));
    }

    #[tokio::test]
    async fn unix_invalid_params_error() {
        // -32602 is the JSON-RPC invalid-params code.
        let resp = make_error(-32602, "invalid params");
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let err = client.get("k").await.unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn unix_close_without_connection_ok() {
        let client = UnixClient::new("/tmp/noop.sock").unwrap();
        client.close().await.unwrap();
    }

    // ── persistent connection behavior ──────────────────────────────────────

    #[tokio::test]
    async fn unix_persistent_connection_reused_across_calls() {
        let server = spawn_mock_server(make_result(serde_json::json!({ "exists": true })));
        let client = UnixClient::new(&server.path).unwrap();
        for _ in 0..3 {
            assert!(client.exists("k").await.unwrap());
        }
        assert_eq!(
            server.connections.load(Ordering::SeqCst),
            1,
            "all calls must share one connection"
        );
    }

    #[tokio::test]
    async fn unix_reconnects_after_server_closes_connection() {
        // The server closes the connection after every response (as the real
        // server does after its 30s idle deadline); the client must
        // transparently reconnect on the next call.
        let server = spawn_mock_server_full(
            make_result(serde_json::json!({ "exists": true })),
            true,
            None,
        );
        let client = UnixClient::new(&server.path).unwrap();
        assert!(client.exists("k").await.unwrap());
        assert!(client.exists("k").await.unwrap());
        assert_eq!(server.connections.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn unix_close_drops_connection_then_reconnects() {
        let server = spawn_mock_server(make_result(serde_json::json!({ "exists": true })));
        let client = UnixClient::new(&server.path).unwrap();
        assert!(client.exists("k").await.unwrap());
        client.close().await.unwrap();
        assert!(client.exists("k").await.unwrap());
        assert_eq!(server.connections.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn unix_response_id_mismatch_is_invalid_response() {
        // A response whose id does not match the request id indicates broken
        // request/response pairing and must be rejected.
        let server = spawn_mock_server_full(
            make_result(serde_json::json!({ "exists": true })),
            false,
            Some(999),
        );
        let client = UnixClient::new(&server.path).unwrap();
        let err = client.exists("k").await.unwrap_err();
        assert!(matches!(err, Error::InvalidResponse(_)));
    }

    // ── retention round-trip ────────────────────────────────────────────────

    #[tokio::test]
    async fn unix_add_policy_sends_retention_seconds() {
        // Sub-day retention values are valid and sent verbatim as
        // retention_seconds (no whole-day restriction).
        let server = spawn_mock_server(make_result(serde_json::json!({ "success": true })));
        let client = UnixClient::new(&server.path).unwrap();
        client
            .add_policy(LifecyclePolicy {
                id: "p1".to_string(),
                prefix: "logs/".to_string(),
                retention_seconds: 90_000,
                action: "delete".to_string(),
                destination_type: None,
                destination_settings: HashMap::new(),
            })
            .await
            .unwrap();

        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["method"], "add_policy");
        assert_eq!(requests[0]["params"]["retention_seconds"], 90_000);
        assert!(requests[0]["params"].get("after_days").is_none());
    }

    #[tokio::test]
    async fn unix_get_policies_prefers_retention_seconds() {
        // When the server returns both fields, retention_seconds wins.
        let resp = make_result(serde_json::json!([
            {
                "id": "p1",
                "prefix": "logs/",
                "action": "delete",
                "after_days": 7,
                "retention_seconds": 90_000
            }
        ]));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let ps = client.get_policies(None).await.unwrap();
        assert_eq!(ps.len(), 1);
        assert_eq!(ps[0].retention_seconds, 90_000);
    }

    #[tokio::test]
    async fn unix_connect_fails_on_missing_socket() {
        let client = UnixClient::new("/tmp/definitely-does-not-exist-xyz.sock").unwrap();
        let err = client.health().await.unwrap_err();
        assert!(matches!(err, Error::Io(_)));
    }

    #[tokio::test]
    async fn unix_get_replication_policies_success() {
        let resp = make_result(serde_json::json!([
            {
                "id": "r1",
                "source_prefix": "data/",
                "destination_type": "gcs",
                "destination": { "bucket": "bk" },
                "enabled": true,
                "replication_mode": "opaque",
            }
        ]));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let policies = client.get_replication_policies().await.unwrap();
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].id, "r1");
    }

    #[tokio::test]
    async fn unix_get_replication_policy_success() {
        let resp = make_result(serde_json::json!({
            "id": "r1",
            "source_prefix": "",
            "destination_type": "s3",
            "destination": {},
            "enabled": false,
            "replication_mode": "transparent",
        }));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let p = client.get_replication_policy("r1").await.unwrap();
        assert_eq!(p.id, "r1");
        assert_eq!(p.replication_mode, ReplicationMode::Transparent);
    }

    #[tokio::test]
    async fn unix_get_policies_array_result() {
        let resp = make_result(serde_json::json!([
            { "id": "p1", "prefix": "logs/", "action": "delete", "after_days": 7 }
        ]));
        let server = spawn_mock_server(resp);
        let client = UnixClient::new(&server.path).unwrap();
        let ps = client.get_policies(None).await.unwrap();
        assert_eq!(ps.len(), 1);
        assert_eq!(ps[0].id, "p1");
        assert_eq!(ps[0].retention_seconds, 7 * 86400);
    }
}
