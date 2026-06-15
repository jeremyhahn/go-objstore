use thiserror::Error;

/// Result type alias for the go-objstore SDK
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for the go-objstore SDK
#[derive(Error, Debug)]
pub enum Error {
    /// gRPC transport error
    #[error("gRPC error: {0}")]
    GrpcTransport(#[from] tonic::transport::Error),

    /// gRPC status error without a dedicated SDK variant
    #[error("gRPC status error: {0}")]
    GrpcStatus(tonic::Status),

    /// HTTP request error
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// QUIC/HTTP3 connection error
    #[error("QUIC connection error: {0}")]
    QuicConnection(#[from] quinn::ConnectionError),

    /// QUIC/HTTP3 read error
    #[error("QUIC read error: {0}")]
    QuicRead(#[from] quinn::ReadError),

    /// QUIC/HTTP3 write error
    #[error("QUIC write error: {0}")]
    QuicWrite(#[from] quinn::WriteError),

    /// H3 error
    #[error("H3 error: {0}")]
    H3(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Invalid URL
    #[error("Invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),

    /// Object not found (HTTP 404, JSON-RPC -32004, gRPC `NotFound`)
    #[error("Object not found: {0}")]
    NotFound(String),

    /// Authorization denied (HTTP 403, JSON-RPC -32001, gRPC `PermissionDenied`)
    #[error("Forbidden: {0}")]
    Forbidden(String),

    /// Authentication required or invalid (HTTP 401, JSON-RPC -32002, gRPC `Unauthenticated`)
    #[error("Unauthenticated: {0}")]
    Unauthenticated(String),

    /// Object or resource already exists (HTTP 409, JSON-RPC -32005, gRPC `AlreadyExists`)
    #[error("Already exists: {0}")]
    AlreadyExists(String),

    /// Request was rate limited (HTTP 429, JSON-RPC -32029, gRPC `ResourceExhausted`)
    #[error("Rate limited: {0}")]
    RateLimited(String),

    /// Invalid request argument (HTTP 400, JSON-RPC -32602, gRPC `InvalidArgument`)
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// Operation failed
    #[error("Operation failed: {0}")]
    OperationFailed(String),

    /// Invalid response
    #[error("Invalid response: {0}")]
    InvalidResponse(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// TLS error
    #[error("TLS error: {0}")]
    Tls(String),

    /// Generic error
    #[error("{0}")]
    Generic(String),
}

/// Map an HTTP status code to the canonical SDK [`Error`].
///
/// Canonical table: 400 -> [`Error::InvalidArgument`], 401 ->
/// [`Error::Unauthenticated`], 403 -> [`Error::Forbidden`], 404 ->
/// [`Error::NotFound`], 409 -> [`Error::AlreadyExists`], 429 ->
/// [`Error::RateLimited`]; any other failure status ->
/// [`Error::OperationFailed`].
///
/// `resource` names the object key or policy id involved and is carried by
/// the not-found / already-exists payloads; `message` describes the failed
/// operation and is used everywhere else (and as the fallback when no
/// resource applies).
pub(crate) fn error_from_http_status(
    status: u16,
    resource: Option<&str>,
    message: String,
) -> Error {
    match status {
        400 => Error::InvalidArgument(message),
        401 => Error::Unauthenticated(message),
        403 => Error::Forbidden(message),
        404 => Error::NotFound(resource.map_or(message, str::to_string)),
        409 => Error::AlreadyExists(resource.map_or(message, str::to_string)),
        429 => Error::RateLimited(message),
        _ => Error::OperationFailed(message),
    }
}

impl From<tonic::Status> for Error {
    /// Map a gRPC status to the canonical SDK [`Error`].
    ///
    /// Canonical table: `NotFound` -> [`Error::NotFound`], `PermissionDenied`
    /// -> [`Error::Forbidden`], `Unauthenticated` -> [`Error::Unauthenticated`],
    /// `AlreadyExists` -> [`Error::AlreadyExists`], `ResourceExhausted` ->
    /// [`Error::RateLimited`], `InvalidArgument` -> [`Error::InvalidArgument`];
    /// any other code is surfaced as [`Error::GrpcStatus`].
    fn from(status: tonic::Status) -> Self {
        let message = status.message().to_string();
        match status.code() {
            tonic::Code::NotFound => Error::NotFound(message),
            tonic::Code::PermissionDenied => Error::Forbidden(message),
            tonic::Code::Unauthenticated => Error::Unauthenticated(message),
            tonic::Code::AlreadyExists => Error::AlreadyExists(message),
            tonic::Code::ResourceExhausted => Error::RateLimited(message),
            tonic::Code::InvalidArgument => Error::InvalidArgument(message),
            _ => Error::GrpcStatus(status),
        }
    }
}

impl From<h3::error::ConnectionError> for Error {
    fn from(err: h3::error::ConnectionError) -> Self {
        Error::H3(err.to_string())
    }
}

impl From<h3::error::StreamError> for Error {
    fn from(err: h3::error::StreamError) -> Self {
        Error::H3(err.to_string())
    }
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Generic(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::Generic(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::NotFound("test.txt".to_string());
        assert_eq!(err.to_string(), "Object not found: test.txt");
    }

    #[test]
    fn test_error_from_string() {
        let err: Error = "test error".into();
        assert_eq!(err.to_string(), "test error");
    }

    #[test]
    fn test_operation_failed() {
        let err = Error::OperationFailed("delete failed".to_string());
        assert!(err.to_string().contains("Operation failed"));
    }

    #[test]
    fn test_configuration_error() {
        let err = Error::Configuration("invalid config".to_string());
        assert!(err.to_string().contains("Configuration error"));
    }

    #[test]
    fn test_canonical_variant_display() {
        assert_eq!(
            Error::Unauthenticated("no token".to_string()).to_string(),
            "Unauthenticated: no token"
        );
        assert_eq!(
            Error::Forbidden("denied".to_string()).to_string(),
            "Forbidden: denied"
        );
        assert_eq!(
            Error::AlreadyExists("k".to_string()).to_string(),
            "Already exists: k"
        );
        assert_eq!(
            Error::RateLimited("slow down".to_string()).to_string(),
            "Rate limited: slow down"
        );
        assert_eq!(
            Error::InvalidArgument("bad key".to_string()).to_string(),
            "Invalid argument: bad key"
        );
    }

    #[test]
    fn test_error_from_http_status_canonical_table() {
        let msg = || "operation failed: status".to_string();
        assert!(matches!(
            error_from_http_status(400, Some("k"), msg()),
            Error::InvalidArgument(_)
        ));
        assert!(matches!(
            error_from_http_status(401, Some("k"), msg()),
            Error::Unauthenticated(_)
        ));
        assert!(matches!(
            error_from_http_status(403, Some("k"), msg()),
            Error::Forbidden(_)
        ));
        assert!(matches!(
            error_from_http_status(404, Some("k"), msg()),
            Error::NotFound(resource) if resource == "k"
        ));
        assert!(matches!(
            error_from_http_status(409, Some("k"), msg()),
            Error::AlreadyExists(resource) if resource == "k"
        ));
        assert!(matches!(
            error_from_http_status(429, Some("k"), msg()),
            Error::RateLimited(_)
        ));
        assert!(matches!(
            error_from_http_status(500, Some("k"), msg()),
            Error::OperationFailed(_)
        ));
    }

    #[test]
    fn test_error_from_http_status_falls_back_to_message() {
        // Without a resource the not-found / already-exists payloads carry
        // the operation message instead.
        assert!(matches!(
            error_from_http_status(404, None, "Failed to list objects: 404".to_string()),
            Error::NotFound(m) if m.contains("Failed to list objects")
        ));
        assert!(matches!(
            error_from_http_status(409, None, "Failed to add policy: 409".to_string()),
            Error::AlreadyExists(m) if m.contains("Failed to add policy")
        ));
    }

    #[test]
    fn test_error_from_tonic_status_canonical_table() {
        assert!(matches!(
            Error::from(tonic::Status::not_found("missing")),
            Error::NotFound(_)
        ));
        assert!(matches!(
            Error::from(tonic::Status::permission_denied("denied")),
            Error::Forbidden(_)
        ));
        assert!(matches!(
            Error::from(tonic::Status::unauthenticated("no token")),
            Error::Unauthenticated(_)
        ));
        assert!(matches!(
            Error::from(tonic::Status::already_exists("dup")),
            Error::AlreadyExists(_)
        ));
        assert!(matches!(
            Error::from(tonic::Status::resource_exhausted("throttled")),
            Error::RateLimited(_)
        ));
        assert!(matches!(
            Error::from(tonic::Status::invalid_argument("bad key")),
            Error::InvalidArgument(_)
        ));
        // Codes without a dedicated variant stay as GrpcStatus.
        assert!(matches!(
            Error::from(tonic::Status::internal("boom")),
            Error::GrpcStatus(_)
        ));
    }
}
