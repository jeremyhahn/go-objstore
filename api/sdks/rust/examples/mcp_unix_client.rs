//! Example: MCP and Unix-socket clients with optional auth.
//!
//! Demonstrates all five transports via the unified [`ObjectStoreClient`]
//! enum.  The MCP and Unix transports are highlighted; the rest are shown
//! for completeness.
//!
//! # Running
//!
//! ```sh
//! # MCP server (HTTP JSON-RPC 2.0):
//! cargo run --example mcp_unix_client -- mcp http://localhost:8081
//!
//! # Unix-socket JSON-RPC 2.0 server:
//! cargo run --example mcp_unix_client -- unix /var/run/objstore.sock
//! ```

use bytes::Bytes;
use go_objstore::{AuthConfig, McpClient, ObjectStore, ObjectStoreClient, UnixClient};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();
    let (transport, addr) = match args.as_slice() {
        [_, t, a] => (t.as_str(), a.as_str()),
        _ => ("mcp", "http://localhost:8081"),
    };

    println!("go-objstore Rust SDK — {transport} transport ({addr})");

    // ── build unified client ─────────────────────────────────────────────────

    let client: Box<dyn ObjectStore> = match transport {
        "mcp" => {
            // Optional auth: set GO_OBJSTORE_TOKEN / GO_OBJSTORE_TENANT env vars.
            let auth = AuthConfig {
                token: env::var("GO_OBJSTORE_TOKEN").ok(),
                tenant_id: env::var("GO_OBJSTORE_TENANT").ok(),
                ..Default::default()
            };
            let unified = if auth.is_empty() {
                ObjectStoreClient::mcp(addr)?
            } else {
                ObjectStoreClient::mcp_with_auth(addr, auth)?
            };
            Box::new(unified)
        }
        "unix" => Box::new(ObjectStoreClient::unix(addr)?),
        "rest" => Box::new(ObjectStoreClient::rest(addr)?),
        other => {
            eprintln!("Unknown transport '{other}'. Use: mcp | unix | rest");
            std::process::exit(1);
        }
    };

    // ── health check ─────────────────────────────────────────────────────────

    println!("\n--- health ---");
    match client.health().await {
        Ok(h) => println!(
            "status={:?}  version={}",
            h.status,
            h.message.as_deref().unwrap_or("(none)")
        ),
        Err(e) => println!("health check failed (server not running?): {e}"),
    }

    // ── put / get / exists / delete ──────────────────────────────────────────

    let key = "sdk-example/hello.txt";
    let payload = Bytes::from("Hello from the Rust SDK!");

    println!("\n--- put ---");
    match client.put(key, payload, None).await {
        Ok(r) => println!(
            "put OK  success={}  etag={}",
            r.success,
            r.etag.as_deref().unwrap_or("(none)")
        ),
        Err(e) => println!("put failed: {e}"),
    }

    println!("\n--- exists ---");
    match client.exists(key).await {
        Ok(true) => println!("{key} exists"),
        Ok(false) => println!("{key} does NOT exist"),
        Err(e) => println!("exists failed: {e}"),
    }

    println!("\n--- get ---");
    match client.get(key).await {
        Ok((data, meta)) => {
            println!(
                "got {} bytes  content_type={}",
                data.len(),
                meta.content_type.as_deref().unwrap_or("(none)")
            );
            println!("content: {}", String::from_utf8_lossy(&data));
        }
        Err(e) => println!("get failed: {e}"),
    }

    println!("\n--- delete ---");
    match client.delete(key).await {
        Ok(r) => println!("delete OK  success={}", r.success),
        Err(e) => println!("delete failed: {e}"),
    }

    // ── direct McpClient / UnixClient API ────────────────────────────────────

    println!("\n--- direct MCP client demo ---");
    let mcp = McpClient::new("http://localhost:8081");
    if let Ok(c) = mcp {
        match c.health().await {
            Ok(h) => println!("MCP direct health: {:?}", h.status),
            Err(e) => println!("MCP direct health (expected, no server): {e}"),
        }
    }

    println!("\n--- direct Unix client demo ---");
    let unix = UnixClient::new("/tmp/objstore.sock");
    if let Ok(c) = unix {
        match c.health().await {
            Ok(h) => println!("Unix direct health: {:?}", h.status),
            Err(e) => println!("Unix direct health (expected, no socket): {e}"),
        }
    }

    println!("\nDone.");
    Ok(())
}
