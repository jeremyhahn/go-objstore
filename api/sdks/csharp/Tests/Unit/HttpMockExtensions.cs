using System.Net;
using System.Text;
using Moq;
using Moq.Protected;

namespace ObjStore.SDK.Tests.Unit;

/// <summary>
/// Shared helpers for setting up a mocked HttpMessageHandler used by the REST and QUIC
/// client tests. Both clients are HTTP based; these helpers keep the canonical tests terse.
/// </summary>
internal static class HttpMockExtensions
{
    /// <summary>Returns a JSON response (default 200 OK) for any request matching the predicate.</summary>
    public static void SetupJson(
        this Mock<HttpMessageHandler> handler,
        Func<HttpRequestMessage, bool> match,
        string json,
        HttpStatusCode status = HttpStatusCode.OK)
    {
        handler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req => match(req)),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(() => new HttpResponseMessage
            {
                StatusCode = status,
                Content = new StringContent(json, Encoding.UTF8, "application/json")
            });
    }

    /// <summary>Returns a bare status-code response (no body) for matching requests.</summary>
    public static void SetupStatus(
        this Mock<HttpMessageHandler> handler,
        Func<HttpRequestMessage, bool> match,
        HttpStatusCode status)
    {
        handler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req => match(req)),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(() => new HttpResponseMessage { StatusCode = status });
    }

    /// <summary>Returns a pre-built response (allows custom headers/content) for matching requests.</summary>
    public static void SetupResponse(
        this Mock<HttpMessageHandler> handler,
        Func<HttpRequestMessage, bool> match,
        Func<HttpResponseMessage> factory)
    {
        handler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req => match(req)),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(factory);
    }

    /// <summary>Captures the outgoing request and returns the supplied response.</summary>
    public static void SetupCapture(
        this Mock<HttpMessageHandler> handler,
        Func<HttpRequestMessage, bool> match,
        Action<HttpRequestMessage> capture,
        Func<HttpResponseMessage> factory)
    {
        handler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req => match(req)),
                ItExpr.IsAny<CancellationToken>())
            .Returns<HttpRequestMessage, CancellationToken>((req, _) =>
            {
                capture(req);
                return Task.FromResult(factory());
            });
    }

    /// <summary>Any request triggers an HttpRequestException (transport failure).</summary>
    public static void SetupThrow(this Mock<HttpMessageHandler> handler)
    {
        handler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>())
            .ThrowsAsync(new HttpRequestException("transport failure"));
    }
}
