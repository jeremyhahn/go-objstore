using System.Buffers.Binary;
using System.Net;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Moq;
using Moq.Protected;

namespace ObjStore.SDK.Tests.Unit;

/// <summary>
/// gRPC test harness. The production GrpcClient builds its own ObjectStore.ObjectStoreClient from
/// a GrpcChannel, so the test seam is the channel's transport: a real GrpcChannel is created over a
/// mocked HttpMessageHandler that returns properly framed gRPC-over-HTTP/2 responses. This exercises
/// the client's real call/parse paths without a live server and without touching frozen client code.
/// </summary>
internal static class GrpcTestChannel
{
    /// <summary>Builds a GrpcChannel whose transport returns the given gRPC response for any call.</summary>
    public static GrpcChannel ForResponse(Func<HttpResponseMessage> responseFactory)
    {
        var handler = new Mock<HttpMessageHandler>();
        handler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>())
            .Returns<HttpRequestMessage, CancellationToken>((_, _) => Task.FromResult(responseFactory()));

        return GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions
        {
            HttpHandler = handler.Object,
            DisposeHttpClient = true
        });
    }

    /// <summary>A successful unary response carrying a single protobuf message.</summary>
    public static GrpcChannel Unary<TResponse>(TResponse message) where TResponse : IMessage<TResponse> =>
        ForResponse(() => GrpcResponse(new[] { (IMessage)message }, StatusCode.OK));

    /// <summary>A successful server-streaming response carrying zero or more protobuf messages.</summary>
    public static GrpcChannel Streaming<TResponse>(IEnumerable<TResponse> messages) where TResponse : IMessage<TResponse> =>
        ForResponse(() => GrpcResponse(messages.Cast<IMessage>().ToArray(), StatusCode.OK));

    /// <summary>A trailers-only error response (no body) carrying the given gRPC status.</summary>
    public static GrpcChannel Error(StatusCode status, string detail = "rpc error") =>
        ForResponse(() => GrpcResponse(Array.Empty<IMessage>(), status, detail));

    /// <summary>Builds a gRPC-over-HTTP/2 response: length-prefixed messages plus grpc-status trailers.</summary>
    private static HttpResponseMessage GrpcResponse(IReadOnlyList<IMessage> messages, StatusCode status, string detail = "")
    {
        var body = new MemoryStream();
        foreach (var msg in messages)
        {
            var payload = msg.ToByteArray();
            var prefix = new byte[5];
            prefix[0] = 0; // uncompressed
            BinaryPrimitives.WriteUInt32BigEndian(prefix.AsSpan(1), (uint)payload.Length);
            body.Write(prefix, 0, prefix.Length);
            body.Write(payload, 0, payload.Length);
        }
        body.Position = 0;

        var content = new StreamContent(body);
        content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/grpc");

        var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Version = new Version(2, 0),
            Content = content
        };

        // Grpc.Net reads the call status from the HTTP/2 trailing headers. The TrailingHeaders
        // getter is reachable at runtime but its accessor is not exposed by the compile-time
        // reference assembly here, so populate the real (mutable) collection via reflection.
        var trailers = (System.Net.Http.Headers.HttpResponseHeaders)typeof(HttpResponseMessage)
            .GetProperty("TrailingHeaders")!
            .GetValue(response)!;
        trailers.Add("grpc-status", ((int)status).ToString());
        if (!string.IsNullOrEmpty(detail))
        {
            trailers.Add("grpc-message", detail);
        }
        return response;
    }
}
