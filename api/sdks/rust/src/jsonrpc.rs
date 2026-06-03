//! Crate-private JSON-RPC 2.0 envelope types and helpers shared by the
//! Unix-socket and MCP clients.

use crate::error::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// JSON-RPC error code for authorization-denied responses.
pub(crate) const CODE_FORBIDDEN: i64 = -32001;
/// JSON-RPC error code for unauthenticated responses.
pub(crate) const CODE_UNAUTHENTICATED: i64 = -32002;
/// JSON-RPC error code for object/resource-not-found responses.
pub(crate) const CODE_NOT_FOUND: i64 = -32004;
/// JSON-RPC error code for already-exists conflicts.
pub(crate) const CODE_ALREADY_EXISTS: i64 = -32005;
/// JSON-RPC error code for rate-limited responses.
pub(crate) const CODE_RATE_LIMITED: i64 = -32029;
/// JSON-RPC error code for invalid request parameters.
pub(crate) const CODE_INVALID_PARAMS: i64 = -32602;

/// JSON-RPC 2.0 request envelope, generic over the params payload.
#[derive(Debug, Serialize)]
pub(crate) struct JsonRpcRequest<'a, P> {
    pub jsonrpc: &'static str,
    pub method: &'a str,
    pub params: P,
    pub id: u64,
}

impl<'a, P> JsonRpcRequest<'a, P> {
    /// Build a request envelope with the protocol version pre-filled.
    pub fn new(method: &'a str, params: P, id: u64) -> Self {
        Self {
            jsonrpc: "2.0",
            method,
            params,
            id,
        }
    }
}

/// JSON-RPC 2.0 response envelope, generic over the result payload.
#[derive(Debug, Deserialize)]
pub(crate) struct JsonRpcResponse<R> {
    pub result: Option<R>,
    pub error: Option<RpcError>,
    #[serde(default)]
    pub id: Value,
}

/// JSON-RPC 2.0 error object.
#[derive(Debug, Deserialize)]
pub(crate) struct RpcError {
    pub code: i64,
    pub message: String,
}

impl RpcError {
    /// Convert this RPC error into the SDK [`Error`] based on its code.
    pub fn into_error(self) -> Error {
        error_from_code(self.code, self.message)
    }
}

/// Map a JSON-RPC error code and message to the SDK [`Error`] type.
///
/// Canonical table: -32602 -> [`Error::InvalidArgument`], -32002 ->
/// [`Error::Unauthenticated`], -32001 -> [`Error::Forbidden`], -32004 ->
/// [`Error::NotFound`], -32005 -> [`Error::AlreadyExists`], -32029 ->
/// [`Error::RateLimited`]. Codes without a dedicated SDK variant surface as
/// [`Error::OperationFailed`] with the server message preserved.
pub(crate) fn error_from_code(code: i64, message: String) -> Error {
    match code {
        CODE_NOT_FOUND => Error::NotFound(message),
        CODE_FORBIDDEN => Error::Forbidden(message),
        CODE_UNAUTHENTICATED => Error::Unauthenticated(message),
        CODE_ALREADY_EXISTS => Error::AlreadyExists(message),
        CODE_RATE_LIMITED => Error::RateLimited(message),
        CODE_INVALID_PARAMS => Error::InvalidArgument(message),
        _ => Error::OperationFailed(message),
    }
}

/// Extract a string-to-string map from a JSON object value.
///
/// Non-object values yield an empty map; non-string entry values are skipped.
pub(crate) fn string_map(v: &Value) -> HashMap<String, String> {
    v.as_object()
        .map(|o| {
            o.iter()
                .filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_from_code_not_found() {
        let err = error_from_code(CODE_NOT_FOUND, "object not found: k".to_string());
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[test]
    fn error_from_code_canonical_table() {
        assert!(matches!(
            error_from_code(CODE_FORBIDDEN, "forbidden".to_string()),
            Error::Forbidden(_)
        ));
        assert!(matches!(
            error_from_code(CODE_UNAUTHENTICATED, "unauthenticated".to_string()),
            Error::Unauthenticated(_)
        ));
        assert!(matches!(
            error_from_code(CODE_ALREADY_EXISTS, "exists".to_string()),
            Error::AlreadyExists(_)
        ));
        assert!(matches!(
            error_from_code(CODE_RATE_LIMITED, "throttled".to_string()),
            Error::RateLimited(_)
        ));
        assert!(matches!(
            error_from_code(CODE_INVALID_PARAMS, "bad params".to_string()),
            Error::InvalidArgument(_)
        ));
    }

    #[test]
    fn error_from_code_unmapped_codes_are_operation_failed() {
        for code in [-32603, -32000] {
            assert!(matches!(
                error_from_code(code, "boom".to_string()),
                Error::OperationFailed(_)
            ));
        }
    }

    #[test]
    fn error_mapping_ignores_message_text() {
        // Mapping is code-based: a "not found" message with a generic code
        // must NOT become Error::NotFound.
        let err = error_from_code(-32603, "object not found: k".to_string());
        assert!(matches!(err, Error::OperationFailed(_)));
    }

    #[test]
    fn rpc_error_into_error_uses_code() {
        let err = RpcError {
            code: CODE_NOT_FOUND,
            message: "missing".to_string(),
        };
        assert!(matches!(err.into_error(), Error::NotFound(_)));
    }

    #[test]
    fn string_map_extracts_string_values() {
        let v = serde_json::json!({ "a": "1", "b": 2, "c": "3" });
        let m = string_map(&v);
        assert_eq!(m.get("a").map(String::as_str), Some("1"));
        assert_eq!(m.get("c").map(String::as_str), Some("3"));
        assert!(!m.contains_key("b"));
    }

    #[test]
    fn string_map_non_object_is_empty() {
        assert!(string_map(&serde_json::json!(null)).is_empty());
        assert!(string_map(&serde_json::json!([1, 2])).is_empty());
    }

    #[test]
    fn request_envelope_serializes() {
        let req = JsonRpcRequest::new("get", serde_json::json!({ "key": "k" }), 7);
        let s = serde_json::to_string(&req).unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["method"], "get");
        assert_eq!(v["id"], 7);
        assert_eq!(v["params"]["key"], "k");
    }

    #[test]
    fn response_envelope_deserializes_error() {
        let resp: JsonRpcResponse<Value> = serde_json::from_str(
            r#"{"jsonrpc":"2.0","error":{"code":-32004,"message":"nope"},"id":1}"#,
        )
        .unwrap();
        assert!(resp.result.is_none());
        let err = resp.error.unwrap();
        assert_eq!(err.code, CODE_NOT_FOUND);
        assert_eq!(resp.id, Value::from(1));
    }
}
