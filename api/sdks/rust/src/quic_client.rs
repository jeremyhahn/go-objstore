use crate::duration::parse_go_duration_ms;
use crate::error::{error_from_http_status, Error, Result};
use crate::rest_client::replication_policy_to_rest_json;
use crate::types::*;
use bytes::{Buf, Bytes};
use h3::client::SendRequest;
use http::{HeaderMap, Method, Request, StatusCode};
use quinn::{ClientConfig, Endpoint};
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

/// TLS verification mode for QUIC connections
#[derive(Debug, Clone, Copy)]
pub enum TlsVerification {
    /// Verify server certificates (production use)
    Enabled,
    /// Skip certificate verification (testing only - INSECURE)
    Disabled,
}

/// QUIC/HTTP3 client for go-objstore
pub struct QuicClient {
    endpoint: Endpoint,
    server_addr: SocketAddr,
    server_name: String,
}

impl QuicClient {
    /// Create a new QUIC/HTTP3 client with default TLS verification enabled
    pub async fn new(server_addr: SocketAddr, server_name: impl Into<String>) -> Result<Self> {
        Self::new_with_tls(server_addr, server_name, TlsVerification::Enabled).await
    }

    /// Create a new QUIC/HTTP3 client with custom TLS verification
    ///
    /// # Warning
    /// Using `TlsVerification::Disabled` bypasses certificate validation and should ONLY
    /// be used in testing environments. This creates a security vulnerability in production.
    pub async fn new_with_tls(
        server_addr: SocketAddr,
        server_name: impl Into<String>,
        tls_verification: TlsVerification,
    ) -> Result<Self> {
        let crypto = match tls_verification {
            TlsVerification::Enabled => {
                // Use system root certificates for proper verification
                let mut root_store = rustls::RootCertStore::empty();
                // Add system root certificates (rustls 0.23 API)
                let native_certs = rustls_native_certs::load_native_certs();
                for err in &native_certs.errors {
                    tracing::warn!("failed to load native cert: {}", err);
                }
                for cert in native_certs.certs {
                    root_store
                        .add(cert)
                        .map_err(|e| Error::Tls(e.to_string()))?;
                }
                rustls::ClientConfig::builder_with_provider(Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))
                .with_safe_default_protocol_versions()
                .map_err(|e| Error::Tls(e.to_string()))?
                .with_root_certificates(root_store)
                .with_no_client_auth()
            }
            TlsVerification::Disabled => {
                // INSECURE: Skip certificate verification (testing only)
                eprintln!("WARNING: TLS certificate verification is DISABLED. This is INSECURE and should only be used for testing!");
                rustls::ClientConfig::builder_with_provider(Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))
                .with_safe_default_protocol_versions()
                .map_err(|e| Error::Tls(e.to_string()))?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
                .with_no_client_auth()
            }
        };

        // Enable ALPN for HTTP/3
        let mut crypto = crypto;
        crypto.alpn_protocols = vec![b"h3".to_vec()];

        let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
            .map_err(|e| Error::Configuration(e.to_string()))?;
        let mut client_config = ClientConfig::new(Arc::new(quic_crypto));

        let mut transport_config = quinn::TransportConfig::default();
        transport_config.max_idle_timeout(Some(
            quinn::IdleTimeout::try_from(std::time::Duration::from_secs(30))
                .map_err(|e| Error::Configuration(e.to_string()))?,
        ));
        client_config.transport_config(Arc::new(transport_config));

        let bind_addr: SocketAddr = "[::]:0"
            .parse()
            .map_err(|e: std::net::AddrParseError| Error::Configuration(e.to_string()))?;
        let mut endpoint =
            Endpoint::client(bind_addr).map_err(|e| Error::Configuration(e.to_string()))?;
        endpoint.set_default_client_config(client_config);

        Ok(Self {
            endpoint,
            server_addr,
            server_name: server_name.into(),
        })
    }

    /// Establish a connection and return an HTTP3 client
    async fn connect(&self) -> Result<SendRequest<h3_quinn::OpenStreams, Bytes>> {
        let conn = self
            .endpoint
            .connect(self.server_addr, &self.server_name)
            .map_err(|e| Error::Configuration(e.to_string()))?
            .await?;

        let h3_conn = h3_quinn::Connection::new(conn);
        let (mut driver, send_request) = h3::client::new(h3_conn).await?;

        tokio::spawn(async move {
            let _ = futures::future::poll_fn(|cx| driver.poll_close(cx)).await;
        });

        Ok(send_request)
    }

    /// Put an object into storage
    pub async fn put(
        &self,
        key: &str,
        data: Bytes,
        metadata: Option<Metadata>,
    ) -> Result<PutResponse> {
        let mut client = self.connect().await?;

        let uri: http::Uri = format!(
            "https://{}/objects/{}",
            self.server_name,
            urlencoding::encode(key)
        )
        .parse()
        .map_err(|_: http::uri::InvalidUri| Error::InvalidUrl(url::ParseError::EmptyHost))?;

        // Resolve Content-Type and assemble per-key X-Meta-* headers from metadata.
        let content_type = metadata
            .as_ref()
            .and_then(|m| m.content_type.clone())
            .unwrap_or_else(|| "application/octet-stream".to_string());

        let mut builder = Request::builder()
            .method(Method::PUT)
            .uri(uri)
            .header("content-type", content_type);

        if let Some(meta) = &metadata {
            if let Some(content_encoding) = &meta.content_encoding {
                builder = builder.header("content-encoding", content_encoding);
            }
            for (k, v) in &meta.custom {
                builder = builder.header(format!("X-Meta-{}", k), v);
            }
        }

        let request = builder
            .body(())
            .map_err(|e| Error::Configuration(e.to_string()))?;

        let mut stream = client
            .send_request(request)
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        stream
            .send_data(data)
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        stream
            .finish()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        let response = stream
            .recv_response()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

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
            Err(error_from_http_status(
                response.status().as_u16(),
                Some(key),
                format!("Failed to put object: {}", response.status()),
            ))
        }
    }

    /// Get an object from storage
    pub async fn get(&self, key: &str) -> Result<(Bytes, Metadata)> {
        let mut client = self.connect().await?;

        let uri: http::Uri = format!(
            "https://{}/objects/{}",
            self.server_name,
            urlencoding::encode(key)
        )
        .parse()
        .map_err(|_: http::uri::InvalidUri| Error::InvalidUrl(url::ParseError::EmptyHost))?;

        let request = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(())
            .map_err(|e| Error::Configuration(e.to_string()))?;

        let mut stream = client
            .send_request(request)
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        stream
            .finish()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        let response = stream
            .recv_response()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        if !response.status().is_success() {
            return Err(error_from_http_status(
                response.status().as_u16(),
                Some(key),
                format!("Failed to get object: {}", response.status()),
            ));
        }

        let mut metadata = metadata_from_headers(response.headers());

        let mut data = Vec::new();
        while let Some(mut chunk) = stream
            .recv_data()
            .await
            .map_err(|e| Error::H3(e.to_string()))?
        {
            while chunk.has_remaining() {
                let bytes = chunk.chunk();
                data.extend_from_slice(bytes);
                chunk.advance(bytes.len());
            }
        }

        if metadata.size == 0 {
            metadata.size = data.len() as i64;
        }

        Ok((Bytes::from(data), metadata))
    }

    /// Delete an object from storage
    pub async fn delete(&self, key: &str) -> Result<DeleteResponse> {
        let mut client = self.connect().await?;

        let uri: http::Uri = format!(
            "https://{}/objects/{}",
            self.server_name,
            urlencoding::encode(key)
        )
        .parse()
        .map_err(|_: http::uri::InvalidUri| Error::InvalidUrl(url::ParseError::EmptyHost))?;

        let request = Request::builder()
            .method(Method::DELETE)
            .uri(uri)
            .body(())
            .map_err(|e| Error::Configuration(e.to_string()))?;

        let mut stream = client
            .send_request(request)
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        stream
            .finish()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        let response = stream
            .recv_response()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        if response.status().is_success() {
            Ok(DeleteResponse {
                success: true,
                message: None,
            })
        } else {
            Err(error_from_http_status(
                response.status().as_u16(),
                Some(key),
                format!("Failed to delete object: {}", response.status()),
            ))
        }
    }

    /// Check if an object exists
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let mut client = self.connect().await?;

        let uri: http::Uri = format!(
            "https://{}/objects/{}",
            self.server_name,
            urlencoding::encode(key)
        )
        .parse()
        .map_err(|_: http::uri::InvalidUri| Error::InvalidUrl(url::ParseError::EmptyHost))?;

        let request = Request::builder()
            .method(Method::HEAD)
            .uri(uri)
            .body(())
            .map_err(|e| Error::Configuration(e.to_string()))?;

        let mut stream = client
            .send_request(request)
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        stream
            .finish()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        let response = stream
            .recv_response()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(false);
        }

        if !response.status().is_success() {
            return Err(error_from_http_status(
                response.status().as_u16(),
                Some(key),
                format!("Failed to check object existence: {}", response.status()),
            ));
        }

        Ok(true)
    }

    /// List objects with optional prefix filtering
    pub async fn list(&self, list_req: ListRequest) -> Result<ListResponse> {
        let mut client = self.connect().await?;

        let mut url = format!("https://{}/objects", self.server_name);
        let mut params = Vec::new();

        if let Some(prefix) = &list_req.prefix {
            params.push(format!("prefix={}", urlencoding::encode(prefix)));
        }

        if let Some(delimiter) = &list_req.delimiter {
            params.push(format!("delimiter={}", urlencoding::encode(delimiter)));
        }

        if let Some(max_results) = list_req.max_results {
            params.push(format!("max={}", max_results));
        }

        if let Some(token) = &list_req.continue_from {
            params.push(format!("continue={}", urlencoding::encode(token)));
        }

        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }

        let uri: http::Uri = url
            .parse()
            .map_err(|_: http::uri::InvalidUri| Error::InvalidUrl(url::ParseError::EmptyHost))?;

        let request = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(())
            .map_err(|e| Error::Configuration(e.to_string()))?;

        let mut stream = client
            .send_request(request)
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        stream
            .finish()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        let response = stream
            .recv_response()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        if !response.status().is_success() {
            return Err(error_from_http_status(
                response.status().as_u16(),
                None,
                format!("Failed to list objects: {}", response.status()),
            ));
        }

        let mut data = Vec::new();
        while let Some(mut chunk) = stream
            .recv_data()
            .await
            .map_err(|e| Error::H3(e.to_string()))?
        {
            while chunk.has_remaining() {
                let bytes = chunk.chunk();
                data.extend_from_slice(bytes);
                chunk.advance(bytes.len());
            }
        }

        #[derive(serde::Deserialize)]
        struct RestListResponse {
            objects: Vec<RestObjectResponse>,
            common_prefixes: Option<Vec<String>>,
            next_token: Option<String>,
            truncated: bool,
        }

        #[derive(serde::Deserialize)]
        struct RestObjectResponse {
            #[allow(dead_code)]
            key: String,
            size: i64,
            modified: Option<String>,
            etag: Option<String>,
            metadata: Option<HashMap<String, String>>,
        }

        let list_response: RestListResponse = serde_json::from_slice(&data)?;

        Ok(ListResponse {
            objects: list_response
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
            common_prefixes: list_response.common_prefixes.unwrap_or_default(),
            next_token: list_response.next_token,
            truncated: list_response.truncated,
        })
    }

    /// Get metadata for an object via HEAD on `/objects/{key}`, reading headers.
    pub async fn get_metadata(&self, key: &str) -> Result<Metadata> {
        let mut client = self.connect().await?;

        let uri: http::Uri = format!(
            "https://{}/objects/{}",
            self.server_name,
            urlencoding::encode(key)
        )
        .parse()
        .map_err(|_: http::uri::InvalidUri| Error::InvalidUrl(url::ParseError::EmptyHost))?;

        let request = Request::builder()
            .method(Method::HEAD)
            .uri(uri)
            .body(())
            .map_err(|e| Error::Configuration(e.to_string()))?;

        let mut stream = client
            .send_request(request)
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        stream
            .finish()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        let response = stream
            .recv_response()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        if !response.status().is_success() {
            return Err(error_from_http_status(
                response.status().as_u16(),
                Some(key),
                format!("Failed to get metadata: {}", response.status()),
            ));
        }

        Ok(metadata_from_headers(response.headers()))
    }

    /// Update metadata for an object via PATCH on `/objects/{key}`.
    pub async fn update_metadata(&self, key: &str, metadata: Metadata) -> Result<()> {
        let mut client = self.connect().await?;

        let uri: http::Uri = format!(
            "https://{}/objects/{}",
            self.server_name,
            urlencoding::encode(key)
        )
        .parse()
        .map_err(|_: http::uri::InvalidUri| Error::InvalidUrl(url::ParseError::EmptyHost))?;

        let mut body = serde_json::Map::new();
        if let Some(content_type) = &metadata.content_type {
            body.insert(
                "content_type".to_string(),
                serde_json::Value::String(content_type.clone()),
            );
        }
        if let Some(content_encoding) = &metadata.content_encoding {
            body.insert(
                "content_encoding".to_string(),
                serde_json::Value::String(content_encoding.clone()),
            );
        }
        if !metadata.custom.is_empty() {
            body.insert(
                "custom".to_string(),
                serde_json::to_value(&metadata.custom)?,
            );
        }
        let body = serde_json::to_vec(&serde_json::Value::Object(body))?;

        let request = Request::builder()
            .method(Method::PATCH)
            .uri(uri)
            .header("content-type", "application/json")
            .body(())
            .map_err(|e| Error::Configuration(e.to_string()))?;

        let mut stream = client
            .send_request(request)
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        stream
            .send_data(Bytes::from(body))
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        stream
            .finish()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        let response = stream
            .recv_response()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        if !response.status().is_success() {
            return Err(error_from_http_status(
                response.status().as_u16(),
                Some(key),
                format!("Failed to update metadata: {}", response.status()),
            ));
        }

        Ok(())
    }

    /// Health check
    pub async fn health(&self) -> Result<HealthResponse> {
        let mut client = self.connect().await?;

        let uri: http::Uri = format!("https://{}/health", self.server_name)
            .parse()
            .map_err(|_: http::uri::InvalidUri| Error::InvalidUrl(url::ParseError::EmptyHost))?;

        let request = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(())
            .map_err(|e| Error::Configuration(e.to_string()))?;

        let mut stream = client
            .send_request(request)
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        stream
            .finish()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        let response = stream
            .recv_response()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        if response.status().is_success() {
            Ok(HealthResponse {
                status: HealthStatus::Serving,
                message: None,
            })
        } else {
            Ok(HealthResponse {
                status: HealthStatus::NotServing,
                message: Some(format!("Status: {}", response.status())),
            })
        }
    }

    /// Perform an HTTP/3 request with an optional JSON body, returning the
    /// response status together with the collected body bytes.
    async fn request_json(
        &self,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<(StatusCode, Vec<u8>)> {
        let mut client = self.connect().await?;

        let uri: http::Uri = format!("https://{}{}", self.server_name, path)
            .parse()
            .map_err(|_: http::uri::InvalidUri| Error::InvalidUrl(url::ParseError::EmptyHost))?;

        let mut builder = Request::builder().method(method).uri(uri);
        if body.is_some() {
            builder = builder.header("content-type", "application/json");
        }
        let request = builder
            .body(())
            .map_err(|e| Error::Configuration(e.to_string()))?;

        let mut stream = client
            .send_request(request)
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        if let Some(body) = body {
            stream
                .send_data(Bytes::from(body))
                .await
                .map_err(|e| Error::H3(e.to_string()))?;
        }

        stream
            .finish()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        let response = stream
            .recv_response()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        let status = response.status();

        let mut data = Vec::new();
        while let Some(mut chunk) = stream
            .recv_data()
            .await
            .map_err(|e| Error::H3(e.to_string()))?
        {
            while chunk.has_remaining() {
                let bytes = chunk.chunk();
                data.extend_from_slice(bytes);
                chunk.advance(bytes.len());
            }
        }

        Ok((status, data))
    }

    /// Archive an object to a different storage backend
    pub async fn archive(
        &self,
        key: &str,
        destination_type: String,
        destination_settings: HashMap<String, String>,
    ) -> Result<()> {
        let body = serde_json::json!({
            "key": key,
            "destination_type": destination_type,
            "destination_settings": destination_settings,
        });
        let (status, _) = self
            .request_json(Method::POST, "/archive", Some(serde_json::to_vec(&body)?))
            .await?;

        if status.is_success() {
            Ok(())
        } else {
            Err(error_from_http_status(
                status.as_u16(),
                Some(key),
                format!("Failed to archive object: {}", status),
            ))
        }
    }

    /// Add a lifecycle policy
    pub async fn add_policy(&self, policy: LifecyclePolicy) -> Result<()> {
        let mut body = serde_json::json!({
            "id": policy.id,
            "retention_seconds": policy.retention_seconds,
            "action": policy.action,
        });
        if !policy.prefix.is_empty() {
            body["prefix"] = serde_json::Value::String(policy.prefix.clone());
        }
        if let Some(dest_type) = &policy.destination_type {
            body["destination_type"] = serde_json::Value::String(dest_type.clone());
        }
        if !policy.destination_settings.is_empty() {
            body["destination_settings"] = serde_json::to_value(&policy.destination_settings)?;
        }

        let (status, _) = self
            .request_json(Method::POST, "/policies", Some(serde_json::to_vec(&body)?))
            .await?;

        if status.is_success() {
            Ok(())
        } else {
            Err(error_from_http_status(
                status.as_u16(),
                None,
                format!("Failed to add policy: {}", status),
            ))
        }
    }

    /// Remove a lifecycle policy
    pub async fn remove_policy(&self, id: &str) -> Result<()> {
        let path = format!("/policies/{}", urlencoding::encode(id));
        let (status, _) = self.request_json(Method::DELETE, &path, None).await?;

        if status.is_success() {
            Ok(())
        } else {
            Err(error_from_http_status(
                status.as_u16(),
                Some(id),
                format!("Failed to remove policy: {}", status),
            ))
        }
    }

    /// Get all lifecycle policies, optionally filtered by prefix
    pub async fn get_policies(&self, prefix: Option<String>) -> Result<Vec<LifecyclePolicy>> {
        let mut path = String::from("/policies");
        if let Some(prefix) = &prefix {
            path.push_str(&format!("?prefix={}", urlencoding::encode(prefix)));
        }

        let (status, data) = self.request_json(Method::GET, &path, None).await?;

        if !status.is_success() {
            return Err(error_from_http_status(
                status.as_u16(),
                None,
                format!("Failed to get policies: {}", status),
            ));
        }

        #[derive(Deserialize)]
        struct QuicPolicy {
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
        struct PoliciesResponse {
            #[serde(default)]
            policies: Vec<QuicPolicy>,
        }

        let parsed: PoliciesResponse = serde_json::from_slice(&data)?;
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
        let (status, data) = self
            .request_json(Method::POST, "/policies/apply", None)
            .await?;

        if !status.is_success() {
            return Err(error_from_http_status(
                status.as_u16(),
                None,
                format!("Failed to apply policies: {}", status),
            ));
        }

        #[derive(Deserialize)]
        struct ApplyResponse {
            #[serde(default)]
            policies_count: i32,
            #[serde(default)]
            objects_processed: i32,
        }

        let parsed: ApplyResponse = serde_json::from_slice(&data)?;
        Ok((parsed.policies_count, parsed.objects_processed))
    }

    /// Add a replication policy
    pub async fn add_replication_policy(&self, policy: ReplicationPolicy) -> Result<()> {
        // QUIC uses the `check_interval` field name (seconds), not check_interval_seconds.
        let body = replication_policy_to_rest_json(&policy, "check_interval");
        let (status, _) = self
            .request_json(
                Method::POST,
                "/replication/policies",
                Some(serde_json::to_vec(&body)?),
            )
            .await?;

        if status.is_success() {
            Ok(())
        } else {
            Err(error_from_http_status(
                status.as_u16(),
                Some(&policy.id),
                format!("Failed to add replication policy: {}", status),
            ))
        }
    }

    /// Remove a replication policy
    pub async fn remove_replication_policy(&self, id: &str) -> Result<()> {
        let path = format!("/replication/policies/{}", urlencoding::encode(id));
        let (status, _) = self.request_json(Method::DELETE, &path, None).await?;

        if status.is_success() {
            Ok(())
        } else {
            Err(error_from_http_status(
                status.as_u16(),
                Some(id),
                format!("Failed to remove replication policy: {}", status),
            ))
        }
    }

    /// Get all replication policies
    pub async fn get_replication_policies(&self) -> Result<Vec<ReplicationPolicy>> {
        let (status, data) = self
            .request_json(Method::GET, "/replication/policies", None)
            .await?;

        if !status.is_success() {
            return Err(error_from_http_status(
                status.as_u16(),
                None,
                format!("Failed to get replication policies: {}", status),
            ));
        }

        #[derive(Deserialize)]
        struct PoliciesResponse {
            #[serde(default)]
            policies: Vec<QuicReplicationPolicy>,
        }

        let parsed: PoliciesResponse = serde_json::from_slice(&data)?;
        Ok(parsed
            .policies
            .into_iter()
            .map(quic_replication_policy_into)
            .collect())
    }

    /// Get a specific replication policy
    pub async fn get_replication_policy(&self, id: &str) -> Result<ReplicationPolicy> {
        let path = format!("/replication/policies/{}", urlencoding::encode(id));
        let (status, data) = self.request_json(Method::GET, &path, None).await?;

        if !status.is_success() {
            return Err(error_from_http_status(
                status.as_u16(),
                Some(id),
                format!("Failed to get replication policy: {}", status),
            ));
        }

        let parsed: QuicReplicationPolicy = serde_json::from_slice(&data)?;
        Ok(quic_replication_policy_into(parsed))
    }

    /// Trigger replication synchronization. The policy id is supplied as a
    /// query parameter; an empty id syncs all policies.
    pub async fn trigger_replication(
        &self,
        policy_id: Option<String>,
        _parallel: bool,
        _worker_count: i32,
    ) -> Result<SyncResult> {
        let mut path = String::from("/replication/trigger");
        if let Some(id) = &policy_id {
            if !id.is_empty() {
                path.push_str(&format!("?policy_id={}", urlencoding::encode(id)));
            }
        }

        let (status, data) = self.request_json(Method::POST, &path, None).await?;

        if !status.is_success() {
            return Err(error_from_http_status(
                status.as_u16(),
                policy_id.as_deref(),
                format!("Failed to trigger replication: {}", status),
            ));
        }

        #[derive(Deserialize)]
        struct TriggerResponse {
            #[serde(default)]
            result: Option<QuicSyncResult>,
        }

        let parsed: TriggerResponse = serde_json::from_slice(&data)?;
        parsed
            .result
            .map(quic_sync_result_into)
            .ok_or_else(|| Error::InvalidResponse("Missing sync result".to_string()))
    }

    /// Get replication status for a policy
    pub async fn get_replication_status(&self, id: &str) -> Result<ReplicationStatus> {
        let path = format!("/replication/status/{}", urlencoding::encode(id));
        let (status, data) = self.request_json(Method::GET, &path, None).await?;

        if !status.is_success() {
            return Err(error_from_http_status(
                status.as_u16(),
                Some(id),
                format!("Failed to get replication status: {}", status),
            ));
        }

        let parsed: QuicReplicationStatus = serde_json::from_slice(&data)?;
        Ok(quic_replication_status_into(parsed))
    }

    /// Close the client, releasing the underlying QUIC endpoint.
    ///
    /// This signals all open connections to close gracefully and waits for the
    /// endpoint to become idle. After calling this the client should not be
    /// reused. [`Drop`] performs the same cleanup if `close` is not called.
    pub async fn close(&self) -> Result<()> {
        self.endpoint.close(0u32.into(), b"client closed");
        self.endpoint.wait_idle().await;
        Ok(())
    }
}

impl Drop for QuicClient {
    fn drop(&mut self) {
        // Best-effort: signal connections to close. The runtime drives the
        // actual shutdown; we do not block in Drop.
        self.endpoint.close(0u32.into(), b"client dropped");
    }
}

/// Build a [`Metadata`] from QUIC/HTTP response headers, reading
/// Content-Type, Content-Encoding, ETag, Content-Length, Last-Modified and
/// any `X-Meta-*` custom headers.
fn metadata_from_headers(headers: &HeaderMap) -> Metadata {
    let header_str = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(String::from)
    };

    let size = headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);

    let last_modified = header_str("last-modified").and_then(|s| {
        chrono::DateTime::parse_from_rfc2822(&s)
            .or_else(|_| chrono::DateTime::parse_from_rfc3339(&s))
            .ok()
            .map(|dt| dt.with_timezone(&chrono::Utc))
    });

    let mut custom = HashMap::new();
    for (name, value) in headers.iter() {
        let name_str = name.as_str();
        if let Some(key) = name_str.strip_prefix("x-meta-") {
            if let Ok(v) = value.to_str() {
                custom.insert(key.to_string(), v.to_string());
            }
        }
    }

    Metadata {
        content_type: header_str("content-type"),
        content_encoding: header_str("content-encoding"),
        size,
        last_modified,
        etag: header_str("etag"),
        custom,
    }
}

/// Wire representation of a replication policy returned by the QUIC server
/// (flat fields, `check_interval` in seconds).
#[derive(Debug, Deserialize)]
struct QuicReplicationPolicy {
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
    check_interval: i64,
    #[serde(default)]
    last_sync_time: Option<String>,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    replication_mode: Option<String>,
    #[serde(default)]
    encryption: Option<EncryptionPolicy>,
}

#[derive(Debug, Deserialize)]
struct QuicSyncResult {
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

#[derive(Debug, Deserialize)]
struct QuicReplicationStatus {
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

fn quic_replication_policy_into(p: QuicReplicationPolicy) -> ReplicationPolicy {
    ReplicationPolicy {
        id: p.id,
        source_backend: p.source_backend,
        source_settings: p.source_settings.unwrap_or_default(),
        source_prefix: p.source_prefix,
        destination_backend: p.destination_backend,
        destination_settings: p.destination_settings.unwrap_or_default(),
        check_interval_seconds: p.check_interval,
        last_sync_time: p.last_sync_time.and_then(parse_rfc3339),
        enabled: p.enabled,
        encryption: p.encryption,
        replication_mode: match p.replication_mode.as_deref() {
            Some("opaque") => ReplicationMode::Opaque,
            _ => ReplicationMode::Transparent,
        },
    }
}

fn quic_sync_result_into(r: QuicSyncResult) -> SyncResult {
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

fn quic_replication_status_into(s: QuicReplicationStatus) -> ReplicationStatus {
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

// Custom certificate verifier that skips verification (for testing only)
// WARNING: This is INSECURE and should only be used for testing
#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Response;
    use std::sync::Arc;
    use tokio::sync::oneshot;

    #[test]
    fn test_skip_verification() {
        let _verifier = SkipServerVerification;
        // Basic sanity check - verifier should be constructable
    }

    #[test]
    fn test_metadata_from_headers_quic() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("content-encoding", "gzip".parse().unwrap());
        headers.insert("content-length", "7".parse().unwrap());
        headers.insert("etag", "\"abc\"".parse().unwrap());
        headers.insert("x-meta-owner", "alice".parse().unwrap());
        headers.insert(
            "last-modified",
            "Mon, 06 May 2024 07:08:09 GMT".parse().unwrap(),
        );

        let meta = metadata_from_headers(&headers);
        assert_eq!(meta.content_type.as_deref(), Some("application/json"));
        assert_eq!(meta.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(meta.size, 7);
        assert_eq!(meta.etag.as_deref(), Some("\"abc\""));
        assert_eq!(meta.custom.get("owner").map(String::as_str), Some("alice"));
        assert!(meta.last_modified.is_some());
    }

    /// A canned HTTP/3 response for a route.
    #[derive(Clone)]
    struct MockResponse {
        status: StatusCode,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
    }

    impl MockResponse {
        fn new(status: u16) -> Self {
            Self {
                status: StatusCode::from_u16(status).unwrap(),
                body: Vec::new(),
                headers: Vec::new(),
            }
        }

        fn body(mut self, body: &str) -> Self {
            self.body = body.as_bytes().to_vec();
            self
        }

        fn header(mut self, name: &str, value: &str) -> Self {
            self.headers.push((name.to_string(), value.to_string()));
            self
        }
    }

    /// In-process HTTP/3 mock server. Routes are matched on
    /// `"METHOD path-and-query"`, falling back to `"METHOD path"`.
    struct MockH3Server {
        addr: SocketAddr,
        shutdown: Option<oneshot::Sender<()>>,
        handle: Option<tokio::task::JoinHandle<()>>,
    }

    impl MockH3Server {
        async fn start(routes: HashMap<String, MockResponse>) -> Self {
            let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
            let cert_der = cert.cert.der().to_vec();
            let key_der = cert.key_pair.serialize_der();

            let cert_der = rustls_pki_types::CertificateDer::from(cert_der);
            let key_der = rustls_pki_types::PrivateKeyDer::try_from(key_der).unwrap();
            let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der)
            .unwrap();
            tls.alpn_protocols = vec![b"h3".to_vec()];

            let quic_server_crypto =
                quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
            let server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_server_crypto));
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let endpoint = Endpoint::server(server_config, addr).unwrap();
            let local_addr = endpoint.local_addr().unwrap();

            let (tx, mut rx) = oneshot::channel::<()>();
            let routes = Arc::new(routes);

            let handle = tokio::spawn(async move {
                loop {
                    let incoming = tokio::select! {
                        _ = &mut rx => break,
                        conn = endpoint.accept() => conn,
                    };
                    let connecting = match incoming {
                        Some(c) => c,
                        None => break,
                    };
                    let routes = routes.clone();
                    tokio::spawn(async move {
                        let conn = match connecting.await {
                            Ok(c) => c,
                            Err(_) => return,
                        };
                        let mut h3_conn: h3::server::Connection<_, Bytes> =
                            match h3::server::Connection::new(h3_quinn::Connection::new(conn)).await
                            {
                                Ok(c) => c,
                                Err(_) => return,
                            };

                        loop {
                            let resolver = match h3_conn.accept().await {
                                Ok(Some(r)) => r,
                                Ok(None) | Err(_) => break,
                            };
                            let (req, mut stream) = match resolver.resolve_request().await {
                                Ok(pair) => pair,
                                Err(_) => continue,
                            };
                            // Drain any request body.
                            while let Ok(Some(mut chunk)) = stream.recv_data().await {
                                while chunk.has_remaining() {
                                    let n = chunk.chunk().len();
                                    chunk.advance(n);
                                }
                            }

                            let method = req.method().clone();
                            let path = req
                                .uri()
                                .path_and_query()
                                .map(|pq| pq.as_str().to_string())
                                .unwrap_or_else(|| req.uri().path().to_string());
                            let route = routes
                                .get(&format!("{} {}", method, path))
                                .or_else(|| routes.get(&format!("{} {}", method, req.uri().path())))
                                .cloned()
                                .unwrap_or_else(|| MockResponse::new(404));

                            let mut builder = Response::builder().status(route.status);
                            for (name, value) in &route.headers {
                                builder = builder.header(name, value);
                            }
                            let response = builder.body(()).unwrap();
                            if stream.send_response(response).await.is_err() {
                                continue;
                            }
                            if !route.body.is_empty() {
                                let _ = stream.send_data(Bytes::from(route.body.clone())).await;
                            }
                            let _ = stream.finish().await;
                        }
                    });
                }
                endpoint.wait_idle().await;
            });

            Self {
                addr: local_addr,
                shutdown: Some(tx),
                handle: Some(handle),
            }
        }

        async fn client(&self) -> QuicClient {
            QuicClient::new_with_tls(self.addr, "localhost", TlsVerification::Disabled)
                .await
                .unwrap()
        }
    }

    impl Drop for MockH3Server {
        fn drop(&mut self) {
            if let Some(tx) = self.shutdown.take() {
                let _ = tx.send(());
            }
            if let Some(handle) = self.handle.take() {
                handle.abort();
            }
        }
    }

    fn routes(entries: Vec<(&str, MockResponse)>) -> HashMap<String, MockResponse> {
        entries
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
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

    // =====================================================================
    // QUIC canonical test matrix.
    //
    // Each op gets `quic_<op>_success` / `quic_<op>_error`; the nine
    // read/mutate ops additionally get `quic_<op>_not_found`. Plus
    // `quic_metadata_round_trip` and `quic_validation_empty_key`. Transport is
    // the in-process `MockH3Server` (quinn + h3 over the TlsVerification::
    // Disabled seam); no live server is required.
    //
    // Documented impl behaviors:
    //  - PUT treats only HTTP 201 CREATED as success.
    //  - `exists` returns Ok(true) only for 200; any other status (404 or 5xx)
    //    returns Ok(false) and never errors.
    //  - The QUIC client performs NO client-side empty-key validation.
    // =====================================================================

    async fn one(method_path: &str, resp: MockResponse) -> MockH3Server {
        MockH3Server::start(routes(vec![(method_path, resp)])).await
    }

    // ---- put ----

    #[tokio::test]
    async fn quic_put_success() {
        let server = one(
            "PUT /objects/up.bin",
            MockResponse::new(201).header("etag", "\"q1\""),
        )
        .await;
        let client = server.client().await;
        let mut meta = Metadata::default();
        meta.content_type = Some("application/octet-stream".to_string());
        let put = client
            .put("up.bin", Bytes::from_static(b"payload"), Some(meta))
            .await
            .unwrap();
        assert!(put.success);
        assert_eq!(put.etag.as_deref(), Some("\"q1\""));
    }

    #[tokio::test]
    async fn quic_put_error() {
        let server = one("PUT /objects/up.bin", MockResponse::new(500)).await;
        let client = server.client().await;
        let err = client
            .put("up.bin", Bytes::from_static(b"d"), None)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    // ---- get ----

    #[tokio::test]
    async fn quic_get_success() {
        let server = one(
            "GET /objects/up.bin",
            MockResponse::new(200)
                .header("content-type", "application/octet-stream")
                .header("x-meta-owner", "dave")
                .body("payload"),
        )
        .await;
        let client = server.client().await;
        let (data, meta) = client.get("up.bin").await.unwrap();
        assert_eq!(&data[..], b"payload");
        assert_eq!(
            meta.content_type.as_deref(),
            Some("application/octet-stream")
        );
        assert_eq!(meta.custom.get("owner").map(String::as_str), Some("dave"));
        assert_eq!(meta.size, 7);
    }

    #[tokio::test]
    async fn quic_get_error() {
        let server = one("GET /objects/boom", MockResponse::new(500)).await;
        let client = server.client().await;
        assert!(matches!(
            client.get("boom").await.unwrap_err(),
            Error::OperationFailed(_)
        ));
    }

    #[tokio::test]
    async fn quic_get_not_found() {
        let server = one("GET /objects/missing", MockResponse::new(404)).await;
        let client = server.client().await;
        assert!(matches!(
            client.get("missing").await.unwrap_err(),
            Error::NotFound(_)
        ));
    }

    // ---- delete ----

    #[tokio::test]
    async fn quic_delete_success() {
        let server = one("DELETE /objects/k", MockResponse::new(204)).await;
        let client = server.client().await;
        assert!(client.delete("k").await.unwrap().success);
    }

    #[tokio::test]
    async fn quic_delete_error() {
        let server = one("DELETE /objects/err", MockResponse::new(500)).await;
        let client = server.client().await;
        assert!(matches!(
            client.delete("err").await.unwrap_err(),
            Error::OperationFailed(_)
        ));
    }

    #[tokio::test]
    async fn quic_delete_not_found() {
        let server = one("DELETE /objects/gone", MockResponse::new(404)).await;
        let client = server.client().await;
        assert!(matches!(
            client.delete("gone").await.unwrap_err(),
            Error::NotFound(_)
        ));
    }

    // ---- list ----

    #[tokio::test]
    async fn quic_list_success() {
        let server = one(
            "GET /objects?prefix=a%2F&delimiter=%2F&max=5&continue=tok",
            MockResponse::new(200).body(
                r#"{"objects":[{"key":"a/1","size":12,"modified":"2024-01-01T00:00:00Z","etag":"e1","metadata":{"k":"v"}}],"common_prefixes":["a/b/"],"next_token":"n","truncated":true}"#,
            ),
        )
        .await;
        let client = server.client().await;
        let req = ListRequest {
            prefix: Some("a/".to_string()),
            delimiter: Some("/".to_string()),
            max_results: Some(5),
            continue_from: Some("tok".to_string()),
        };
        let resp = client.list(req).await.unwrap();
        assert_eq!(resp.objects.len(), 1);
        assert_eq!(resp.objects[0].key, "a/1");
        assert_eq!(resp.objects[0].metadata.size, 12);
        assert_eq!(resp.common_prefixes, vec!["a/b/".to_string()]);
        assert!(resp.truncated);
    }

    #[tokio::test]
    async fn quic_list_error() {
        let server = one("GET /objects", MockResponse::new(500)).await;
        let client = server.client().await;
        assert!(matches!(
            client.list(ListRequest::default()).await.unwrap_err(),
            Error::OperationFailed(_)
        ));
    }

    // ---- exists ----

    #[tokio::test]
    async fn quic_exists_success() {
        let server = one("HEAD /objects/k", MockResponse::new(200)).await;
        let client = server.client().await;
        assert!(client.exists("k").await.unwrap());
    }

    #[tokio::test]
    async fn quic_exists_error() {
        // A 5xx must surface as an error, not be swallowed as Ok(false).
        let server = one("HEAD /objects/boom", MockResponse::new(500)).await;
        let client = server.client().await;
        assert!(matches!(
            client.exists("boom").await.unwrap_err(),
            Error::OperationFailed(_)
        ));
    }

    #[tokio::test]
    async fn quic_exists_not_found() {
        let server = one("HEAD /objects/nope", MockResponse::new(404)).await;
        let client = server.client().await;
        assert!(!client.exists("nope").await.unwrap());
    }

    // ---- get_metadata ----

    #[tokio::test]
    async fn quic_get_metadata_success() {
        let server = one(
            "HEAD /objects/m.txt",
            MockResponse::new(200)
                .header("content-type", "text/plain")
                .header("x-meta-k", "v"),
        )
        .await;
        let client = server.client().await;
        let meta = client.get_metadata("m.txt").await.unwrap();
        assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
        assert_eq!(meta.custom.get("k").map(String::as_str), Some("v"));
    }

    #[tokio::test]
    async fn quic_get_metadata_error() {
        let server = one("HEAD /objects/boom", MockResponse::new(500)).await;
        let client = server.client().await;
        assert!(matches!(
            client.get_metadata("boom").await.unwrap_err(),
            Error::OperationFailed(_)
        ));
    }

    #[tokio::test]
    async fn quic_get_metadata_not_found() {
        let server = one("HEAD /objects/missing", MockResponse::new(404)).await;
        let client = server.client().await;
        assert!(matches!(
            client.get_metadata("missing").await.unwrap_err(),
            Error::NotFound(_)
        ));
    }

    // ---- update_metadata ----

    #[tokio::test]
    async fn quic_update_metadata_success() {
        let server = one("PATCH /objects/m.txt", MockResponse::new(200)).await;
        let client = server.client().await;
        let mut update = Metadata::default();
        update.content_type = Some("text/plain".to_string());
        update.content_encoding = Some("gzip".to_string());
        update.custom.insert("k".to_string(), "v".to_string());
        client.update_metadata("m.txt", update).await.unwrap();
    }

    #[tokio::test]
    async fn quic_update_metadata_error() {
        let server = one("PATCH /objects/boom", MockResponse::new(500)).await;
        let client = server.client().await;
        assert!(matches!(
            client
                .update_metadata("boom", Metadata::default())
                .await
                .unwrap_err(),
            Error::OperationFailed(_)
        ));
    }

    #[tokio::test]
    async fn quic_update_metadata_not_found() {
        let server = one("PATCH /objects/missing", MockResponse::new(404)).await;
        let client = server.client().await;
        assert!(matches!(
            client
                .update_metadata("missing", Metadata::default())
                .await
                .unwrap_err(),
            Error::NotFound(_)
        ));
    }

    // ---- health ----

    #[tokio::test]
    async fn quic_health_success() {
        let server = one("GET /health", MockResponse::new(200)).await;
        let client = server.client().await;
        assert_eq!(client.health().await.unwrap().status, HealthStatus::Serving);
    }

    #[tokio::test]
    async fn quic_health_error() {
        // Impl maps a non-success status to a NotServing HealthResponse rather
        // than an Err -- assert that documented behavior.
        let server = one("GET /health", MockResponse::new(503)).await;
        let client = server.client().await;
        let health = client.health().await.unwrap();
        assert_eq!(health.status, HealthStatus::NotServing);
        assert!(health.message.is_some());
    }

    // ---- archive ----

    #[tokio::test]
    async fn quic_archive_success() {
        let server = one("POST /archive", MockResponse::new(200)).await;
        let client = server.client().await;
        let mut settings = HashMap::new();
        settings.insert("vault".to_string(), "cold".to_string());
        client
            .archive("old.bin", "glacier".to_string(), settings)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn quic_archive_error() {
        let server = one("POST /archive", MockResponse::new(500)).await;
        let client = server.client().await;
        assert!(matches!(
            client
                .archive("old.bin", "glacier".to_string(), HashMap::new())
                .await
                .unwrap_err(),
            Error::OperationFailed(_)
        ));
    }

    // ---- add_policy ----

    #[tokio::test]
    async fn quic_add_policy_success() {
        let server = one("POST /policies", MockResponse::new(201)).await;
        let client = server.client().await;
        let mut destination_settings = HashMap::new();
        destination_settings.insert("vault".to_string(), "v1".to_string());
        let policy = LifecyclePolicy {
            id: "p1".to_string(),
            prefix: "logs/".to_string(),
            retention_seconds: 3600,
            action: "archive".to_string(),
            destination_type: Some("glacier".to_string()),
            destination_settings,
        };
        client.add_policy(policy).await.unwrap();
    }

    #[tokio::test]
    async fn quic_add_policy_error() {
        let server = one("POST /policies", MockResponse::new(400)).await;
        let client = server.client().await;
        let policy = LifecyclePolicy {
            id: "p2".to_string(),
            prefix: String::new(),
            retention_seconds: 0,
            action: "delete".to_string(),
            destination_type: None,
            destination_settings: HashMap::new(),
        };
        // 400 maps to InvalidArgument per the canonical table.
        assert!(matches!(
            client.add_policy(policy).await.unwrap_err(),
            Error::InvalidArgument(_)
        ));
    }

    // ---- remove_policy ----

    #[tokio::test]
    async fn quic_remove_policy_success() {
        let server = one("DELETE /policies/p1", MockResponse::new(200)).await;
        let client = server.client().await;
        client.remove_policy("p1").await.unwrap();
    }

    #[tokio::test]
    async fn quic_remove_policy_error() {
        let server = one("DELETE /policies/p1", MockResponse::new(500)).await;
        let client = server.client().await;
        assert!(matches!(
            client.remove_policy("p1").await.unwrap_err(),
            Error::OperationFailed(_)
        ));
    }

    #[tokio::test]
    async fn quic_remove_policy_not_found() {
        // 404 maps to NotFound per the canonical table.
        let server = one("DELETE /policies/p2", MockResponse::new(404)).await;
        let client = server.client().await;
        assert!(matches!(
            client.remove_policy("p2").await.unwrap_err(),
            Error::NotFound(_)
        ));
    }

    // ---- get_policies ----

    #[tokio::test]
    async fn quic_get_policies_success() {
        let server = one(
            "GET /policies?prefix=logs%2F",
            MockResponse::new(200).body(
                r#"{"policies":[{"id":"p1","prefix":"logs/","retention_seconds":3600,"action":"archive","destination_type":"glacier","destination_settings":{"vault":"v1"}},{"id":"p2","action":"delete"}]}"#,
            ),
        )
        .await;
        let client = server.client().await;
        let policies = client
            .get_policies(Some("logs/".to_string()))
            .await
            .unwrap();
        assert_eq!(policies.len(), 2);
        assert_eq!(policies[0].destination_type.as_deref(), Some("glacier"));
        assert_eq!(policies[1].destination_type, None);
    }

    #[tokio::test]
    async fn quic_get_policies_error() {
        let server = one("GET /policies", MockResponse::new(500)).await;
        let client = server.client().await;
        assert!(matches!(
            client.get_policies(None).await.unwrap_err(),
            Error::OperationFailed(_)
        ));
    }

    // ---- apply_policies ----

    #[tokio::test]
    async fn quic_apply_policies_success() {
        let server = one(
            "POST /policies/apply",
            MockResponse::new(200).body(r#"{"policies_count":2,"objects_processed":17}"#),
        )
        .await;
        let client = server.client().await;
        let (count, processed) = client.apply_policies().await.unwrap();
        assert_eq!(count, 2);
        assert_eq!(processed, 17);
    }

    #[tokio::test]
    async fn quic_apply_policies_error() {
        let server = one("POST /policies/apply", MockResponse::new(500)).await;
        let client = server.client().await;
        assert!(matches!(
            client.apply_policies().await.unwrap_err(),
            Error::OperationFailed(_)
        ));
    }

    // ---- add_replication_policy ----

    #[tokio::test]
    async fn quic_add_replication_policy_success() {
        let server = one("POST /replication/policies", MockResponse::new(201)).await;
        let client = server.client().await;
        client
            .add_replication_policy(sample_replication_policy())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn quic_add_replication_policy_error() {
        let server = one("POST /replication/policies", MockResponse::new(409)).await;
        let client = server.client().await;
        // 409 maps to AlreadyExists per the canonical table.
        assert!(matches!(
            client
                .add_replication_policy(sample_replication_policy())
                .await
                .unwrap_err(),
            Error::AlreadyExists(_)
        ));
    }

    // ---- remove_replication_policy ----

    #[tokio::test]
    async fn quic_remove_replication_policy_success() {
        let server = one(
            "DELETE /replication/policies/repl-1",
            MockResponse::new(200),
        )
        .await;
        let client = server.client().await;
        client.remove_replication_policy("repl-1").await.unwrap();
    }

    #[tokio::test]
    async fn quic_remove_replication_policy_error() {
        let server = one(
            "DELETE /replication/policies/repl-1",
            MockResponse::new(500),
        )
        .await;
        let client = server.client().await;
        assert!(matches!(
            client
                .remove_replication_policy("repl-1")
                .await
                .unwrap_err(),
            Error::OperationFailed(_)
        ));
    }

    #[tokio::test]
    async fn quic_remove_replication_policy_not_found() {
        // 404 maps to NotFound per the canonical table.
        let server = one(
            "DELETE /replication/policies/missing",
            MockResponse::new(404),
        )
        .await;
        let client = server.client().await;
        assert!(matches!(
            client
                .remove_replication_policy("missing")
                .await
                .unwrap_err(),
            Error::NotFound(_)
        ));
    }

    // ---- get_replication_policies ----

    #[tokio::test]
    async fn quic_get_replication_policies_success() {
        let server = one(
            "GET /replication/policies",
            MockResponse::new(200).body(
                r#"{"policies":[{"id":"repl-1","source_backend":"s3","destination_backend":"gcs","check_interval":300,"enabled":true,"replication_mode":"opaque","last_sync_time":"2024-01-02T03:04:05Z"}]}"#,
            ),
        )
        .await;
        let client = server.client().await;
        let policies = client.get_replication_policies().await.unwrap();
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].check_interval_seconds, 300);
        assert_eq!(policies[0].replication_mode, ReplicationMode::Opaque);
        assert!(policies[0].last_sync_time.is_some());
    }

    #[tokio::test]
    async fn quic_get_replication_policies_error() {
        let server = one("GET /replication/policies", MockResponse::new(500)).await;
        let client = server.client().await;
        assert!(matches!(
            client.get_replication_policies().await.unwrap_err(),
            Error::OperationFailed(_)
        ));
    }

    // ---- get_replication_policy ----

    #[tokio::test]
    async fn quic_get_replication_policy_success() {
        let server = one(
            "GET /replication/policies/repl-1",
            MockResponse::new(200).body(
                r#"{"id":"repl-1","source_backend":"s3","destination_backend":"gcs","check_interval":120,"replication_mode":"transparent"}"#,
            ),
        )
        .await;
        let client = server.client().await;
        let policy = client.get_replication_policy("repl-1").await.unwrap();
        assert_eq!(policy.id, "repl-1");
        assert_eq!(policy.check_interval_seconds, 120);
        assert_eq!(policy.replication_mode, ReplicationMode::Transparent);
    }

    #[tokio::test]
    async fn quic_get_replication_policy_error() {
        let server = one("GET /replication/policies/boom", MockResponse::new(500)).await;
        let client = server.client().await;
        assert!(matches!(
            client.get_replication_policy("boom").await.unwrap_err(),
            Error::OperationFailed(_)
        ));
    }

    #[tokio::test]
    async fn quic_get_replication_policy_not_found() {
        let server = one("GET /replication/policies/missing", MockResponse::new(404)).await;
        let client = server.client().await;
        assert!(matches!(
            client.get_replication_policy("missing").await.unwrap_err(),
            Error::NotFound(_)
        ));
    }

    // ---- trigger_replication ----

    #[tokio::test]
    async fn quic_trigger_replication_success() {
        let server = one(
            "POST /replication/trigger?policy_id=repl-1",
            MockResponse::new(200).body(
                r#"{"result":{"policy_id":"repl-1","synced":5,"deleted":1,"failed":0,"bytes_total":2048,"duration":"2s","errors":["minor"]}}"#,
            ),
        )
        .await;
        let client = server.client().await;
        let result = client
            .trigger_replication(Some("repl-1".to_string()), true, 4)
            .await
            .unwrap();
        assert_eq!(result.policy_id, "repl-1");
        assert_eq!(result.synced, 5);
        assert_eq!(result.bytes_total, 2048);
        assert_eq!(result.duration_ms, 2000);
        assert_eq!(result.errors, vec!["minor".to_string()]);
    }

    #[tokio::test]
    async fn quic_trigger_replication_error() {
        // Non-success -> OperationFailed; success but missing result ->
        // InvalidResponse (empty policy id -> no query string).
        let server = one("POST /replication/trigger", MockResponse::new(500)).await;
        let client = server.client().await;
        assert!(matches!(
            client
                .trigger_replication(None, false, 1)
                .await
                .unwrap_err(),
            Error::OperationFailed(_)
        ));

        let server = one(
            "POST /replication/trigger",
            MockResponse::new(200).body(r#"{}"#),
        )
        .await;
        let client = server.client().await;
        assert!(matches!(
            client
                .trigger_replication(Some(String::new()), false, 1)
                .await
                .unwrap_err(),
            Error::InvalidResponse(_)
        ));
    }

    // ---- get_replication_status ----

    #[tokio::test]
    async fn quic_get_replication_status_success() {
        let server = one(
            "GET /replication/status/repl-1",
            MockResponse::new(200).body(
                r#"{"policy_id":"repl-1","source_backend":"s3","destination_backend":"gcs","enabled":true,"total_objects_synced":10,"total_objects_deleted":2,"total_bytes_synced":4096,"total_errors":1,"last_sync_time":"2024-05-06T07:08:09Z","average_sync_duration":"3s","sync_count":7}"#,
            ),
        )
        .await;
        let client = server.client().await;
        let status = client.get_replication_status("repl-1").await.unwrap();
        assert_eq!(status.policy_id, "repl-1");
        assert_eq!(status.total_objects_synced, 10);
        assert_eq!(status.average_sync_duration_ms, 3000);
        assert!(status.last_sync_time.is_some());
    }

    #[tokio::test]
    async fn quic_get_replication_status_error() {
        let server = one("GET /replication/status/boom", MockResponse::new(500)).await;
        let client = server.client().await;
        assert!(matches!(
            client.get_replication_status("boom").await.unwrap_err(),
            Error::OperationFailed(_)
        ));
    }

    #[tokio::test]
    async fn quic_get_replication_status_not_found() {
        let server = one("GET /replication/status/missing", MockResponse::new(404)).await;
        let client = server.client().await;
        assert!(matches!(
            client.get_replication_status("missing").await.unwrap_err(),
            Error::NotFound(_)
        ));
    }

    // ---- cross-cutting ----

    #[tokio::test]
    async fn quic_metadata_round_trip() {
        // QUIC wire scheme: PUT sets Content-Type, Content-Encoding and one
        // X-Meta-<key> header per custom entry; metadata is read back from the
        // response headers on GET / get_metadata (HEAD). Here the mock echoes
        // those headers so the inbound parse round-trips all three.
        let server = MockH3Server::start(routes(vec![
            ("PUT /objects/obj", MockResponse::new(201)),
            (
                "GET /objects/obj",
                MockResponse::new(200)
                    .header("content-type", "application/octet-stream")
                    .header("content-encoding", "gzip")
                    .header("x-meta-owner", "dave")
                    .body("payload"),
            ),
            (
                "HEAD /objects/obj",
                MockResponse::new(200)
                    .header("content-type", "application/octet-stream")
                    .header("content-encoding", "gzip")
                    .header("x-meta-owner", "dave"),
            ),
        ]))
        .await;
        let client = server.client().await;

        let mut meta = Metadata::default();
        meta.content_type = Some("application/octet-stream".to_string());
        meta.content_encoding = Some("gzip".to_string());
        meta.custom.insert("owner".to_string(), "dave".to_string());
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
        assert_eq!(head.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(head.custom.get("owner").map(String::as_str), Some("dave"));
    }

    #[tokio::test]
    async fn quic_validation_empty_key() {
        // The QUIC client performs no client-side empty-key validation: the
        // request goes to /objects/ and the server decides. Here a 404 surfaces
        // as Error::NotFound.
        let server = one("GET /objects/", MockResponse::new(404)).await;
        let client = server.client().await;
        assert!(matches!(
            client.get("").await.unwrap_err(),
            Error::NotFound(_)
        ));
    }

    #[tokio::test]
    async fn quic_http_status_canonical_mapping() {
        // Every row of the canonical HTTP status table, asserted over the
        // mocked transport: 400 InvalidArgument, 401 Unauthenticated,
        // 403 Forbidden, 404 NotFound, 409 AlreadyExists, 429 RateLimited,
        // 5xx OperationFailed.
        let cases: [(u16, fn(&Error) -> bool); 7] = [
            (400, |e| matches!(e, Error::InvalidArgument(_))),
            (401, |e| matches!(e, Error::Unauthenticated(_))),
            (403, |e| matches!(e, Error::Forbidden(_))),
            (404, |e| matches!(e, Error::NotFound(_))),
            (409, |e| matches!(e, Error::AlreadyExists(_))),
            (429, |e| matches!(e, Error::RateLimited(_))),
            (500, |e| matches!(e, Error::OperationFailed(_))),
        ];
        for (status, check) in cases {
            let server = one("GET /objects/k", MockResponse::new(status)).await;
            let client = server.client().await;
            let err = client.get("k").await.unwrap_err();
            assert!(check(&err), "HTTP {} mapped to {:?}", status, err);
        }
    }

    #[tokio::test]
    async fn quic_close() {
        let server = one("GET /health", MockResponse::new(200)).await;
        let client = server.client().await;
        client.health().await.unwrap();
        client.close().await.unwrap();
    }
}
