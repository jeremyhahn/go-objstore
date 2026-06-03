//! Streaming helpers for `get_stream` and `put_stream` operations.
//!
//! Each protocol client gains two extension methods:
//!
//! - `get_stream` – returns the raw bytes of an object as an async byte
//!   stream ([`bytes::Bytes`] chunks from reqwest / gRPC / QUIC).
//! - `put_stream` – accepts a [`futures::Stream`] of [`bytes::Bytes`]
//!   chunks and stores the concatenated payload under `key`.  The REST
//!   client streams the chunks as a chunked-transfer-encoded request body;
//!   gRPC and QUIC buffer the stream before sending.
//!
//! Unix and MCP clients do not have streaming variants; they buffer data
//! in memory via their existing `put`/`get` methods.

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
    /// Stream the bytes of an object from the REST server.
    ///
    /// Returns an async stream of [`Bytes`] chunks.  The HTTP response body
    /// is streamed rather than buffered into a single allocation.
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

    /// Upload a stream of bytes to the REST server under `key`.
    ///
    /// The stream is sent as a chunked-transfer-encoded request body, so the
    /// payload is never buffered into a single contiguous allocation.
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
    /// Stream the bytes of an object via the gRPC server-streaming `Get` RPC.
    ///
    /// The gRPC `Get` method is already defined as server-streaming in the
    /// protobuf schema.  This wrapper collects the stream of chunks into a
    /// single Bytes value so the caller receives a one-item stream.  True
    /// progressive streaming is available by calling `get` directly and
    /// reading the underlying tonic stream.
    pub async fn get_stream(&self, key: String) -> Result<impl Stream<Item = Result<Bytes>>> {
        let (data, _meta) = self.get(key).await?;
        Ok(futures::stream::once(async move { Ok(data) }))
    }

    /// Upload a stream of bytes via the gRPC `Put` RPC.
    ///
    /// The stream is fully buffered before sending because the generated
    /// protobuf client uses a unary RPC for put.
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
    /// Stream the bytes of an object from the QUIC/HTTP3 server.
    ///
    /// The response body is yielded as a single [`Bytes`] chunk after the
    /// QUIC stream has been fully read.  True chunk-by-chunk streaming from
    /// QUIC would require lower-level HTTP/3 framing; the buffered approach
    /// keeps the interface consistent across transports.
    pub async fn get_stream(&self, key: &str) -> Result<impl Stream<Item = Result<Bytes>>> {
        let (data, _meta) = self.get(key).await?;
        let stream = futures::stream::once(async move { Ok(data) });
        Ok(stream)
    }

    /// Upload a stream of bytes via QUIC/HTTP3.
    ///
    /// The stream is buffered before sending; see [`RestClient::put_stream`]
    /// for the rationale.
    pub async fn put_stream(
        &self,
        key: &str,
        stream: impl Stream<Item = Result<Bytes>> + Send + 'static,
        metadata: Option<Metadata>,
    ) -> Result<crate::types::PutResponse> {
        self.put(key, collect_stream(stream).await?, metadata).await
    }
}

// ── accessor shims ────────────────────────────────────────────────────────────
//
// The streaming methods need access to the private fields of `RestClient`
// and `GrpcClient`.  We expose minimal accessor methods defined in the same
// crate (the `pub(crate)` pattern), keeping the public API clean.

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
