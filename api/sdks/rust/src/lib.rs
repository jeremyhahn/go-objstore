//! # go-objstore Rust SDK
//!
//! A comprehensive Rust SDK for the go-objstore library, providing unified access
//! to object storage operations via multiple protocols: REST, gRPC, QUIC/HTTP3,
//! MCP (HTTP JSON-RPC 2.0), and Unix-domain sockets (JSON-RPC 2.0).
//!
//! ## Features
//!
//! - **Multi-protocol support**: REST, gRPC, QUIC/HTTP3, MCP, and Unix socket
//! - **Async/await**: Built on Tokio for efficient async operations
//! - **Type-safe**: Strong typing with comprehensive error handling
//! - **Unified interface**: Common trait for all protocols
//! - **App-layer auth**: Optional `Authorization: Bearer`, `X-Tenant-ID`, and
//!   arbitrary extra headers injected by [`AuthConfig`]
//! - **Streaming**: `get_stream` / `put_stream` on REST, gRPC, and QUIC clients
//! - **Advanced features**: Lifecycle policies, replication, archiving
//!
//! ## Quick Start
//!
//! ```no_run
//! use go_objstore::{ObjectStoreClient, ObjectStore};
//! use bytes::Bytes;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create a REST client - clients use &self for concurrent access
//!     let client = ObjectStoreClient::rest("http://localhost:8080")?;
//!
//!     // Put an object
//!     let data = Bytes::from("Hello, World!");
//!     client.put("test.txt", data, None).await?;
//!
//!     // Get the object
//!     let (data, metadata) = client.get("test.txt").await?;
//!     println!("Retrieved {} bytes", metadata.size);
//!
//!     // Check if exists
//!     if client.exists("test.txt").await? {
//!         println!("Object exists!");
//!     }
//!
//!     // Delete the object
//!     client.delete("test.txt").await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Protocol-Specific Clients
//!
//! ### REST Client
//!
//! ```no_run
//! use go_objstore::ObjectStoreClient;
//!
//! let client = ObjectStoreClient::rest("http://localhost:8080")?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! ### gRPC Client
//!
//! ```no_run
//! use go_objstore::ObjectStoreClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = ObjectStoreClient::grpc("http://localhost:50051").await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### QUIC/HTTP3 Client
//!
//! ```no_run
//! use go_objstore::{ObjectStoreClient, QuicClient, TlsVerification};
//! use std::net::SocketAddr;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Production: Use TLS verification (default)
//! let addr: SocketAddr = "127.0.0.1:4433".parse()?;
//! let client = ObjectStoreClient::quic(addr, "localhost").await?;
//!
//! // Testing only: Disable TLS verification (INSECURE)
//! let test_client = QuicClient::new_with_tls(
//!     addr,
//!     "localhost",
//!     TlsVerification::Disabled
//! ).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### MCP Client (HTTP JSON-RPC 2.0)
//!
//! ```no_run
//! use go_objstore::{McpClient, AuthConfig};
//! use bytes::Bytes;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = McpClient::new("http://localhost:8081")?;
//! // With auth:
//! let auth_client = McpClient::new_with_auth(
//!     "http://localhost:8081",
//!     AuthConfig { token: Some("mytoken".to_string()), ..Default::default() },
//! )?;
//! let resp = client.put("k", Bytes::from("v"), None).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Unix-Socket Client (JSON-RPC 2.0)
//!
//! ```no_run
//! use go_objstore::UnixClient;
//! use bytes::Bytes;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = UnixClient::new("/var/run/objstore.sock")?;
//! let resp = client.put("k", Bytes::from("v"), None).await?;
//! # Ok(())
//! # }
//! ```

pub mod auth;
pub mod client;
pub mod duration;
pub mod error;
pub mod grpc_client;
pub(crate) mod jsonrpc;
pub mod mcp_client;
pub mod quic_client;
pub mod rest_client;
pub mod streaming;
pub mod types;
pub mod unix_client;

// Re-export main types for convenience
pub use auth::AuthConfig;
pub use client::{ObjectStore, ObjectStoreClient};
pub use error::{Error, Result};
pub use types::*;

// Re-export individual clients
pub use grpc_client::GrpcClient;
pub use mcp_client::McpClient;
pub use quic_client::{QuicClient, TlsVerification};
pub use rest_client::RestClient;
pub use unix_client::UnixClient;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_exports() {
        // Verify all main types are accessible
        let _: Result<()> = Ok(());
    }
}
