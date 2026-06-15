namespace ObjStore.SDK.Internal;

/// <summary>
/// Wraps a response body stream and the HttpResponseMessage that owns it so
/// disposing the stream also disposes the response. Required when a stream is
/// returned to the caller from a request sent with ResponseHeadersRead: the
/// response must stay alive while the caller reads, and must be released when
/// the caller is done.
/// </summary>
internal sealed class ResponseOwningStream : Stream
{
    private readonly Stream _inner;
    private readonly HttpResponseMessage _response;

    public ResponseOwningStream(Stream inner, HttpResponseMessage response)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _response = response ?? throw new ArgumentNullException(nameof(response));
    }

    public override bool CanRead => _inner.CanRead;
    public override bool CanSeek => _inner.CanSeek;
    public override bool CanWrite => _inner.CanWrite;
    public override long Length => _inner.Length;

    public override long Position
    {
        get => _inner.Position;
        set => _inner.Position = value;
    }

    public override void Flush() => _inner.Flush();
    public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);
    public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
    public override int Read(Span<byte> buffer) => _inner.Read(buffer);
    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => _inner.ReadAsync(buffer, offset, count, cancellationToken);
    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        => _inner.ReadAsync(buffer, cancellationToken);
    public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
    public override void SetLength(long value) => _inner.SetLength(value);
    public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _inner.Dispose();
            _response.Dispose();
        }
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        await _inner.DisposeAsync().ConfigureAwait(false);
        _response.Dispose();
        await base.DisposeAsync().ConfigureAwait(false);
    }
}
