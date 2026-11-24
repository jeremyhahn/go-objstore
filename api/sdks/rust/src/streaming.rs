// ! `get_stream` and `put_stream` extensions; REST streams chunked, gRPC and QUIC buffer, Unix and MCP have none.

use crate::error::{error_from_http_status, Error, Result};
use crate::grpc_client::GrpcClient;
use crate::quic_client::QuicClient;
use crate::rest_client::RestClient;
use crate::types::Metadata;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;

/// Collect an async byte stream into a single [`Bytes`] allocation. Shared by
/// the buffered `put_stream` implementations of the gRPC and QUIC transports.
async fn collect_stream(
    stream: impl Stream<Item = Result<Bytes>> + Send + 'static,
) -> Result<Bytes> {
    let mut chunks = Vec::new();
    futures::pin_mut!(stream);
    while let Some(chunk) = stream.next().await {
        chunks.extend_from_slice(&chunk?);
    }
    Ok(Bytes::from(chunks))
}

// ── REST streaming ────────────────────────────────────────────────────────────

impl RestClient {
    /// Stream an object's bytes from the REST server without buffering.
    pub async fn get_stream(
        &self,
        key: &str,
    ) -> Result<(impl Stream<Item = Result<Bytes>>, Metadata)> {
        let url = format!(
            "{}/objects/{}",
            self.base_url_ref(),
            urlencoding::encode(key)
        );

        let resp = self.http_client_ref().get(&url).send().await?;

        if !resp.status().is_success() {
            return Err(error_from_http_status(
                resp.status().as_u16(),
                Some(key),
                format!("Failed to get object stream: {}", resp.status()),
            ));
        }

        let metadata = crate::rest_client::metadata_from_headers_pub(resp.headers());
        let stream = resp.bytes_stream().map(|r| r.map_err(Error::Http));

        Ok((stream, metadata))
    }

    /// Upload a byte stream under `key` as a chunked request body.
    pub async fn put_stream(
        &self,
        key: &str,
        stream: impl Stream<Item = Result<Bytes>> + Send + 'static,
        metadata: Option<Metadata>,
    ) -> Result<crate::types::PutResponse> {
        self.put_body(key, reqwest::Body::wrap_stream(stream), metadata)
            .await
    }
}

// ── gRPC streaming ────────────────────────────────────────────────────────────

impl GrpcClient {
    /// Collect the server-streaming gRPC `Get` into a one-item stream; call `get` for progressive reads.
    pub async fn get_stream(&self, key: String) -> Result<impl Stream<Item = Result<Bytes>>> {
        let (data, _meta) = self.get(key).await?;
        Ok(futures::stream::once(async move { Ok(data) }))
    }

    /// Upload via the unary gRPC `Put`, buffering the stream first.
    pub async fn put_stream(
        &self,
        key: String,
        stream: impl Stream<Item = Result<Bytes>> + Send + 'static,
        metadata: Option<Metadata>,
    ) -> Result<crate::types::PutResponse> {
        self.put(key, collect_stream(stream).await?, metadata).await
    }
}

// ── QUIC streaming ────────────────────────────────────────────────────────────

impl QuicClient {
    /// Read an object over QUIC/HTTP3 as one buffered chunk.
    pub async fn get_stream(&self, key: &str) -> Result<impl Stream<Item = Result<Bytes>>> {
        let (data, _meta) = self.get(key).await?;
        let stream = futures::stream::once(async move { Ok(data) });
        Ok(stream)
    }

    /// Upload via QUIC/HTTP3, buffering the stream first.
    pub async fn put_stream(
        &self,
        key: &str,
        stream: impl Stream<Item = Result<Bytes>> + Send + 'static,
        metadata: Option<Metadata>,
    ) -> Result<crate::types::PutResponse> {
        self.put(key, collect_stream(stream).await?, metadata).await
    }
}

// Crate-private accessors the streaming methods need.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rest_client::RestClient;
    use bytes::Bytes;
    use futures::StreamExt;
    use mockito::Server;

    #[tokio::test]
    async fn rest_put_stream_roundtrip() {
        let mut server = Server::new_async().await;
        let put_mock = server
            .mock("PUT", "/objects/k")
            .match_body("streaming payload")
            .with_status(201)
            .with_header("etag", "\"e1\"")
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let data = b"streaming payload";
        let stream = futures::stream::once(async { Ok::<Bytes, Error>(Bytes::from_static(data)) });
        let resp = client.put_stream("k", stream, None).await.unwrap();
        put_mock.assert_async().await;
        assert!(resp.success);
    }

    #[tokio::test]
    async fn rest_put_stream_multi_chunk() {
        // Multiple chunks are sent with chunked transfer encoding; the server
        // must receive the concatenated payload.
        let mut server = Server::new_async().await;
        let put_mock = server
            .mock("PUT", "/objects/k")
            .match_body("hello world")
            .with_status(201)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let stream = futures::stream::iter(vec![
            Ok::<Bytes, Error>(Bytes::from_static(b"hello")),
            Ok(Bytes::from_static(b" world")),
        ]);
        let resp = client.put_stream("k", stream, None).await.unwrap();
        put_mock.assert_async().await;
        assert!(resp.success);
    }

    #[tokio::test]
    async fn rest_put_stream_propagates_stream_error() {
        // An error yielded mid-stream must surface to the caller instead of
        // silently truncating the upload.
        let mut server = Server::new_async().await;
        let _mock = server
            .mock("PUT", "/objects/k")
            .with_status(201)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let stream = futures::stream::iter(vec![
            Ok::<Bytes, Error>(Bytes::from_static(b"hello")),
            Err(Error::Generic("disk read failed".to_string())),
        ]);
        assert!(client.put_stream("k", stream, None).await.is_err());
    }

    #[tokio::test]
    async fn rest_get_stream_success() {
        let mut server = Server::new_async().await;
        let get_mock = server
            .mock("GET", "/objects/k")
            .with_status(200)
            .with_header("content-length", "5")
            .with_body("hello")
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let (stream, meta) = client.get_stream("k").await.unwrap();
        get_mock.assert_async().await;
        assert_eq!(meta.size, 5);

        let chunks: Vec<_> = stream.collect::<Vec<_>>().await;
        let all: Bytes = chunks
            .into_iter()
            .filter_map(|r| r.ok())
            .fold(Vec::new(), |mut acc, b| {
                acc.extend_from_slice(&b);
                acc
            })
            .into();
        assert_eq!(&all[..], b"hello");
    }

    #[tokio::test]
    async fn rest_get_stream_not_found() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/objects/missing")
            .with_status(404)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.get_stream("missing").await;
        mock.assert_async().await;
        let err = result.err().expect("expected error");
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[tokio::test]
    async fn rest_get_stream_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/objects/k")
            .with_status(500)
            .create_async()
            .await;

        let client = RestClient::new(server.url()).unwrap();
        let result = client.get_stream("k").await;
        mock.assert_async().await;
        let err = result.err().expect("expected error");
        assert!(matches!(err, Error::OperationFailed(_)));
    }
}
