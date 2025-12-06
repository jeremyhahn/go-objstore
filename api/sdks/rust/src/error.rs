use thiserror::Error;

/// Result type alias for the go-objstore SDK
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for the go-objstore SDK
#[derive(Error, Debug)]
pub enum Error {
    /// gRPC transport error
    #[error("gRPC error: {0}")]
    GrpcTransport(#[from] tonic::transport::Error),

    /// gRPC status error
    #[error("gRPC status error: {0}")]
    GrpcStatus(#[from] tonic::Status),

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

    /// Object not found
    #[error("Object not found: {0}")]
    NotFound(String),

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

impl From<h3::Error> for Error {
    fn from(err: h3::Error) -> Self {
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
}
