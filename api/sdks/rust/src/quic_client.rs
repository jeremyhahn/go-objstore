use crate::error::{Error, Result};
use crate::types::*;
use bytes::{Buf, Bytes};
use h3::client::SendRequest;
use http::{Method, Request, StatusCode};
use quinn::{ClientConfig, Endpoint};
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
    pub async fn new(
        server_addr: SocketAddr,
        server_name: impl Into<String>,
    ) -> Result<Self> {
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
                // Add system root certificates (rustls 0.21 API)
                for cert in rustls_native_certs::load_native_certs()
                    .map_err(|e| Error::Tls(e.to_string()))?
                {
                    root_store.add(&rustls::Certificate(cert.0))
                        .map_err(|e| Error::Tls(e.to_string()))?;
                }
                rustls::ClientConfig::builder()
                    .with_safe_defaults()
                    .with_root_certificates(root_store)
                    .with_no_client_auth()
            }
            TlsVerification::Disabled => {
                // INSECURE: Skip certificate verification (testing only)
                eprintln!("WARNING: TLS certificate verification is DISABLED. This is INSECURE and should only be used for testing!");
                rustls::ClientConfig::builder()
                    .with_safe_defaults()
                    .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
                    .with_no_client_auth()
            }
        };

        // Enable ALPN for HTTP/3
        let mut crypto = crypto;
        crypto.alpn_protocols = vec![b"h3".to_vec()];

        let mut client_config = ClientConfig::new(Arc::new(crypto));

        let mut transport_config = quinn::TransportConfig::default();
        transport_config.max_idle_timeout(Some(
            quinn::IdleTimeout::try_from(std::time::Duration::from_secs(30))
                .map_err(|e| Error::Configuration(e.to_string()))?,
        ));
        client_config.transport_config(Arc::new(transport_config));

        let bind_addr: SocketAddr = "[::]:0"
            .parse()
            .map_err(|e: std::net::AddrParseError| Error::Configuration(e.to_string()))?;
        let mut endpoint = Endpoint::client(bind_addr)
            .map_err(|e| Error::Configuration(e.to_string()))?;
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
    pub async fn put(&self, key: &str, data: Bytes, _metadata: Option<Metadata>) -> Result<PutResponse> {
        let mut client = self.connect().await?;

        let uri: http::Uri = format!("https://{}/objects/{}", self.server_name, urlencoding::encode(key))
            .parse()
            .map_err(|_: http::uri::InvalidUri| Error::InvalidUrl(url::ParseError::EmptyHost))?;

        let request = Request::builder()
            .method(Method::PUT)
            .uri(uri)
            .header("content-type", "application/octet-stream")
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

        stream.finish().await.map_err(|e| Error::H3(e.to_string()))?;

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
            Err(Error::OperationFailed(format!(
                "Failed to put object: {}",
                response.status()
            )))
        }
    }

    /// Get an object from storage
    pub async fn get(&self, key: &str) -> Result<(Bytes, Metadata)> {
        let mut client = self.connect().await?;

        let uri: http::Uri = format!("https://{}/objects/{}", self.server_name, urlencoding::encode(key))
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

        stream.finish().await.map_err(|e| Error::H3(e.to_string()))?;

        let response = stream
            .recv_response()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::NotFound(key.to_string()));
        }

        if !response.status().is_success() {
            return Err(Error::OperationFailed(format!(
                "Failed to get object: {}",
                response.status()
            )));
        }

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let etag = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let mut data = Vec::new();
        while let Some(mut chunk) = stream.recv_data().await.map_err(|e| Error::H3(e.to_string()))? {
            while chunk.has_remaining() {
                let bytes = chunk.chunk();
                data.extend_from_slice(bytes);
                chunk.advance(bytes.len());
            }
        }

        let metadata = Metadata {
            content_type,
            content_encoding: None,
            size: data.len() as i64,
            last_modified: None,
            etag,
            custom: HashMap::new(),
        };

        Ok((Bytes::from(data), metadata))
    }

    /// Delete an object from storage
    pub async fn delete(&self, key: &str) -> Result<DeleteResponse> {
        let mut client = self.connect().await?;

        let uri: http::Uri = format!("https://{}/objects/{}", self.server_name, urlencoding::encode(key))
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

        stream.finish().await.map_err(|e| Error::H3(e.to_string()))?;

        let response = stream
            .recv_response()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

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

    /// Check if an object exists
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let mut client = self.connect().await?;

        let uri: http::Uri = format!("https://{}/objects/{}", self.server_name, urlencoding::encode(key))
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

        stream.finish().await.map_err(|e| Error::H3(e.to_string()))?;

        let response = stream
            .recv_response()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        Ok(response.status() == StatusCode::OK)
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
            params.push(format!("limit={}", max_results));
        }

        if let Some(token) = &list_req.continue_from {
            params.push(format!("token={}", urlencoding::encode(token)));
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

        stream.finish().await.map_err(|e| Error::H3(e.to_string()))?;

        let response = stream
            .recv_response()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        if !response.status().is_success() {
            return Err(Error::OperationFailed(format!(
                "Failed to list objects: {}",
                response.status()
            )));
        }

        let mut data = Vec::new();
        while let Some(mut chunk) = stream.recv_data().await.map_err(|e| Error::H3(e.to_string()))? {
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

    /// Get metadata for an object
    pub async fn get_metadata(&self, key: &str) -> Result<Metadata> {
        let mut client = self.connect().await?;

        let uri: http::Uri = format!(
            "https://{}/objects/{}/metadata",
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

        stream.finish().await.map_err(|e| Error::H3(e.to_string()))?;

        let response = stream
            .recv_response()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::NotFound(key.to_string()));
        }

        if !response.status().is_success() {
            return Err(Error::OperationFailed(format!(
                "Failed to get metadata: {}",
                response.status()
            )));
        }

        let mut data = Vec::new();
        while let Some(mut chunk) = stream.recv_data().await.map_err(|e| Error::H3(e.to_string()))? {
            while chunk.has_remaining() {
                let bytes = chunk.chunk();
                data.extend_from_slice(bytes);
                chunk.advance(bytes.len());
            }
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

        let obj: RestObjectResponse = serde_json::from_slice(&data)?;

        Ok(Metadata {
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
        })
    }

    /// Update metadata for an object
    pub async fn update_metadata(&self, key: &str, metadata: Metadata) -> Result<()> {
        let mut client = self.connect().await?;

        let uri: http::Uri = format!(
            "https://{}/objects/{}/metadata",
            self.server_name,
            urlencoding::encode(key)
        )
        .parse()
        .map_err(|_: http::uri::InvalidUri| Error::InvalidUrl(url::ParseError::EmptyHost))?;

        #[derive(serde::Serialize)]
        struct RestMetadata {
            content_type: Option<String>,
            content_encoding: Option<String>,
            size: i64,
            last_modified: Option<String>,
            etag: Option<String>,
            custom: Option<HashMap<String, String>>,
        }

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

        let body = serde_json::to_vec(&rest_metadata)?;

        let request = Request::builder()
            .method(Method::PUT)
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

        stream.finish().await.map_err(|e| Error::H3(e.to_string()))?;

        let response = stream
            .recv_response()
            .await
            .map_err(|e| Error::H3(e.to_string()))?;

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

        stream.finish().await.map_err(|e| Error::H3(e.to_string()))?;

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
}

// Custom certificate verifier that skips verification (for testing only)
// WARNING: This is INSECURE and should only be used for testing
#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> std::result::Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skip_verification() {
        let _verifier = SkipServerVerification;
        // Basic sanity check - verifier should be constructable
    }
}
