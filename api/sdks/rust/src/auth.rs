use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::collections::HashMap;
use std::str::FromStr;

/// Optional authentication/tenant configuration added to every client.
///
/// Supply a `token` to have `Authorization: Bearer <token>` added to
/// REST, QUIC, and MCP HTTP requests (or gRPC metadata).  Supply
/// `extra_headers` to inject arbitrary request headers.  Supply
/// `tenant_id` to add an `X-Tenant-ID` header (or gRPC metadata key).
///
/// Unix-socket connections perform no header injection (auth is handled
/// by the server via peer credentials).
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    /// Bearer token inserted as `Authorization: Bearer <token>`.
    pub token: Option<String>,

    /// Arbitrary extra headers forwarded on every request.
    pub extra_headers: HashMap<String, String>,

    /// Tenant identifier forwarded as `X-Tenant-ID`.
    pub tenant_id: Option<String>,
}

impl AuthConfig {
    /// Return true when no auth configuration has been provided.
    pub fn is_empty(&self) -> bool {
        self.token.is_none() && self.extra_headers.is_empty() && self.tenant_id.is_none()
    }

    /// Build a [`HeaderMap`] from this configuration.
    ///
    /// Invalid header names or values are silently skipped so that a
    /// misconfigured extra header does not prevent all requests from
    /// completing.
    pub fn to_header_map(&self) -> HeaderMap {
        let mut map = HeaderMap::new();

        if let Some(token) = &self.token {
            let value = format!("Bearer {token}");
            if let Ok(v) = HeaderValue::from_str(&value) {
                map.insert(reqwest::header::AUTHORIZATION, v);
            }
        }

        if let Some(tid) = &self.tenant_id {
            if let (Ok(name), Ok(value)) = (
                HeaderName::from_str("x-tenant-id"),
                HeaderValue::from_str(tid),
            ) {
                map.insert(name, value);
            }
        }

        for (k, v) in &self.extra_headers {
            if let (Ok(name), Ok(value)) = (HeaderName::from_str(k), HeaderValue::from_str(v)) {
                map.insert(name, value);
            }
        }

        map
    }

    /// Build `tonic` request metadata entries.
    ///
    /// Returns a `Vec<(key, value)>` that the caller inserts into a
    /// `tonic::Request` before sending.
    pub fn to_grpc_metadata(
        &self,
    ) -> Vec<(
        tonic::metadata::MetadataKey<tonic::metadata::Ascii>,
        tonic::metadata::MetadataValue<tonic::metadata::Ascii>,
    )> {
        let mut entries = Vec::new();

        if let Some(token) = &self.token {
            let value = format!("Bearer {token}");
            if let (Ok(k), Ok(v)) = (
                tonic::metadata::MetadataKey::<tonic::metadata::Ascii>::from_bytes(
                    b"authorization",
                ),
                tonic::metadata::MetadataValue::<tonic::metadata::Ascii>::try_from(value.as_str()),
            ) {
                entries.push((k, v));
            }
        }

        if let Some(tid) = &self.tenant_id {
            if let (Ok(k), Ok(v)) = (
                tonic::metadata::MetadataKey::<tonic::metadata::Ascii>::from_bytes(b"x-tenant-id"),
                tonic::metadata::MetadataValue::<tonic::metadata::Ascii>::try_from(tid.as_str()),
            ) {
                entries.push((k, v));
            }
        }

        for (key, val) in &self.extra_headers {
            if let (Ok(k), Ok(v)) = (
                tonic::metadata::MetadataKey::<tonic::metadata::Ascii>::from_bytes(key.as_bytes()),
                tonic::metadata::MetadataValue::<tonic::metadata::Ascii>::try_from(val.as_str()),
            ) {
                entries.push((k, v));
            }
        }

        entries
    }
}

/// Apply [`AuthConfig`] headers to a [`reqwest::RequestBuilder`].
pub fn apply_auth(builder: reqwest::RequestBuilder, auth: &AuthConfig) -> reqwest::RequestBuilder {
    if auth.is_empty() {
        return builder;
    }
    builder.headers(auth.to_header_map())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_config_default_is_empty() {
        assert!(AuthConfig::default().is_empty());
    }

    #[test]
    fn auth_config_token_sets_authorization_header() {
        let auth = AuthConfig {
            token: Some("secret".to_string()),
            ..Default::default()
        };
        assert!(!auth.is_empty());
        let map = auth.to_header_map();
        let val = map
            .get(reqwest::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert_eq!(val, "Bearer secret");
    }

    #[test]
    fn auth_config_tenant_id_header() {
        let auth = AuthConfig {
            tenant_id: Some("acme".to_string()),
            ..Default::default()
        };
        let map = auth.to_header_map();
        let val = map
            .get("x-tenant-id")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert_eq!(val, "acme");
    }

    #[test]
    fn auth_config_extra_headers() {
        let mut extra = HashMap::new();
        extra.insert("x-custom".to_string(), "value".to_string());
        let auth = AuthConfig {
            extra_headers: extra,
            ..Default::default()
        };
        let map = auth.to_header_map();
        let val = map
            .get("x-custom")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert_eq!(val, "value");
    }

    #[test]
    fn auth_config_all_fields() {
        let mut extra = HashMap::new();
        extra.insert("x-req-id".to_string(), "123".to_string());
        let auth = AuthConfig {
            token: Some("tok".to_string()),
            tenant_id: Some("tenant1".to_string()),
            extra_headers: extra,
        };
        assert!(!auth.is_empty());
        let map = auth.to_header_map();
        assert!(map.contains_key(reqwest::header::AUTHORIZATION));
        assert!(map.contains_key("x-tenant-id"));
        assert!(map.contains_key("x-req-id"));
    }
}
