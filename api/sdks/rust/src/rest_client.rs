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

#[derive(Debug, Deserialize)]
struct RestObjectResponse {
    key: String,
    size: i64,
    modified: Option<String>,
    etag: Option<String>,
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
    pub async fn put(&self, key: &str, data: Bytes, metadata: Option<Metadata>) -> Result<PutResponse> {
        let url = format!("{}/objects/{}", self.base_url, urlencoding::encode(key));

        let mut request = self.client.put(&url);

        // Send metadata as JSON in X-Metadata header
        if let Some(meta) = &metadata {
            let rest_metadata = RestMetadata {
                content_type: meta.content_type.clone(),
                content_encoding: meta.content_encoding.clone(),
                size: meta.size,
                last_modified: meta.last_modified.map(|dt| dt.to_rfc3339()),
                etag: meta.etag.clone(),
                custom: if meta.custom.is_empty() {
                    None
                } else {
                    Some(meta.custom.clone())
                },
            };
            if let Ok(json) = serde_json::to_string(&rest_metadata) {
                request = request.header("X-Metadata", json);
            }
            // Also set Content-Type header for the body
            if let Some(content_type) = &meta.content_type {
                request = request.header("Content-Type", content_type);
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

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let content_length = response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);

        let etag = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let last_modified = response
            .headers()
            .get("last-modified")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| chrono::DateTime::parse_from_rfc2822(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc));

        let data = response.bytes().await?;

        let metadata = Metadata {
            content_type,
            content_encoding: None,
            size: content_length,
            last_modified,
            etag,
            custom: HashMap::new(),
        };

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

        Ok(response.status() == StatusCode::OK)
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

        let obj: RestObjectResponse = response.json().await?;

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rest_client_new() {
        let client = RestClient::new("http://localhost:8080");
        assert!(client.is_ok());
    }

    #[test]
    fn test_url_encoding() {
        let key = "path/to/file with spaces.txt";
        let encoded = urlencoding::encode(key);
        assert!(encoded.contains("%20"));
    }

    #[tokio::test]
    async fn test_exists_with_mock() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("HEAD", "/objects/test.txt")
            .with_status(200)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let exists = client.exists("test.txt").await.unwrap();

        mock.assert_async().await;
        assert!(exists);
    }

    #[tokio::test]
    async fn test_health_with_mock() {
        use mockito::Server;

        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/health")
            .with_status(200)
            .with_body(r#"{"status":"healthy","version":"1.0.0"}"#)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let health = client.health().await.unwrap();

        mock.assert_async().await;
        assert_eq!(health.status, HealthStatus::Serving);
    }
}
