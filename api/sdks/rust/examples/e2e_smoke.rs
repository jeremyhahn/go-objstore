//! Live end-to-end smoke test for the MCP and Unix transports.
//! Run against a server started with -mcp and -unix enabled:
//!   cargo run --example e2e_smoke
use bytes::Bytes;
use go_objstore::{ListRequest, ObjectStore, ObjectStoreClient};

async fn run(name: &str, client: ObjectStoreClient) -> Result<(), Box<dyn std::error::Error>> {
    let key = format!("smoke/rs-{name}/o.txt");
    let payload = format!("hello rust {name}");

    client
        .put(&key, Bytes::from(payload.clone().into_bytes()), None)
        .await?;
    if !client.exists(&key).await? {
        return Err(format!("{name}: exists false after put").into());
    }
    let (data, _) = client.get(&key).await?;
    if data.as_ref() != payload.as_bytes() {
        return Err(format!("{name}: round-trip mismatch: {:?}", data).into());
    }
    let resp = client
        .list(ListRequest {
            prefix: Some(format!("smoke/rs-{name}")),
            delimiter: None,
            max_results: None,
            continue_from: None,
        })
        .await?;
    if resp.objects.is_empty() {
        return Err(format!("{name}: list empty").into());
    }
    client.delete(&key).await?;
    if client.exists(&key).await? {
        return Err(format!("{name}: still exists after delete").into());
    }
    println!("  {name} transport round-trip OK");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run("mcp", ObjectStoreClient::mcp("http://127.0.0.1:18081")?).await?;
    run("unix", ObjectStoreClient::unix("/tmp/objstore-test.sock")?).await?;
    println!("RUST E2E PASS");
    Ok(())
}
