namespace ObjStore.SDK.Internal;

/// <summary>
/// Wraps a caller-provided stream so that StreamContent (which disposes its
/// stream when the request is disposed) cannot close the caller's stream.
/// Ownership of the underlying stream stays with the caller.
/// </summary>
internal sealed class NonDisposingStream : Stream
{
    private readonly Stream _inner;

    public NonDisposingStream(Stream inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
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
        // Intentionally does not dispose _inner: the caller owns it.
        base.Dispose(disposing);
    }

    public override ValueTask DisposeAsync()
    {
        // Intentionally does not dispose _inner: the caller owns it.
        return base.DisposeAsync();
    }
}
