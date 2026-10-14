using System.Data;
using System.Threading.Channels;
using redb.Export.Providers;

namespace redb.Export.Services;

/// <summary>
/// Writes batches of rows over several connections at once.
///
/// The reader hands each full batch over and goes on reading; a fixed number of workers, one
/// connection each, send the batches to the database. The queue between them is bounded, so a
/// reader that outruns the database waits instead of filling memory with rows.
/// </summary>
internal sealed class ParallelImportWriter : IAsyncDisposable
{
    private readonly Channel<(string Table, DataTable Rows)> _queue;
    private readonly List<IDataProvider> _connections = new();
    private readonly List<Task> _workers = new();
    private readonly CancellationTokenSource _stop;

    private ParallelImportWriter(int degree, CancellationToken ct)
    {
        _stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _queue = Channel.CreateBounded<(string, DataTable)>(new BoundedChannelOptions(degree * 2)
        {
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait
        });
    }

    /// <summary>Connections the workers hold; reused afterwards to build indexes.</summary>
    public IReadOnlyList<IDataProvider> Connections => _connections;

    public static async Task<ParallelImportWriter> StartAsync(
        IParallelImportProvider provider, int degree, CancellationToken ct)
    {
        var writer = new ParallelImportWriter(degree, ct);
        try
        {
            for (var i = 0; i < degree; i++)
                writer._connections.Add(await provider.OpenWorkerAsync(ct));
        }
        catch
        {
            await writer.DisposeAsync();
            throw;
        }

        foreach (var connection in writer._connections)
            writer._workers.Add(Task.Run(() => writer.RunAsync(connection)));

        return writer;
    }

    /// <summary>
    /// Queues a batch. Waits while the queue is full; fails at once if a worker has already failed,
    /// so the reader does not go on parsing a file whose rows nobody will write.
    /// </summary>
    public async ValueTask EnqueueAsync(string table, DataTable rows)
    {
        try
        {
            await _queue.Writer.WriteAsync((table, rows), _stop.Token);
        }
        catch (OperationCanceledException) when (FirstFailure() is { } failure)
        {
            throw new InvalidOperationException("A writer connection failed: " + failure.Message, failure);
        }
    }

    /// <summary>Waits until every queued batch is written. Rethrows the first worker failure.</summary>
    public async Task CompleteAsync()
    {
        _queue.Writer.TryComplete();
        await Task.WhenAll(_workers);
    }

    private async Task RunAsync(IDataProvider connection)
    {
        try
        {
            await foreach (var (table, rows) in _queue.Reader.ReadAllAsync(_stop.Token))
            {
                await connection.BulkInsertAsync(table, rows, _stop.Token);
                rows.Dispose();
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested)
        {
            // Another worker failed or the caller cancelled; that one carries the reason.
        }
        catch
        {
            await _stop.CancelAsync();
            throw;
        }
    }

    private Exception? FirstFailure()
        => _workers.Where(w => w.IsFaulted).Select(w => w.Exception?.GetBaseException()).FirstOrDefault(e => e != null);

    public async ValueTask DisposeAsync()
    {
        _queue.Writer.TryComplete();
        await _stop.CancelAsync();
        try
        {
            await Task.WhenAll(_workers);
        }
        catch
        {
            // Already reported through CompleteAsync or EnqueueAsync.
        }

        foreach (var connection in _connections)
            await connection.DisposeAsync();
        _stop.Dispose();
    }
}

/// <summary>
/// Read-only stream wrapper that counts the bytes read, so progress through a compressed file can
/// be told from the uncompressed length even though the underlying stream cannot report a position.
/// </summary>
internal sealed class CountingStream(Stream inner) : Stream
{
    private long _read;

    public long BytesRead => Interlocked.Read(ref _read);

    public override int Read(byte[] buffer, int offset, int count)
    {
        var n = inner.Read(buffer, offset, count);
        Interlocked.Add(ref _read, n);
        return n;
    }

    public override int Read(Span<byte> buffer)
    {
        var n = inner.Read(buffer);
        Interlocked.Add(ref _read, n);
        return n;
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var n = await inner.ReadAsync(buffer, cancellationToken);
        Interlocked.Add(ref _read, n);
        return n;
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        var n = await inner.ReadAsync(buffer.AsMemory(offset, count), cancellationToken);
        Interlocked.Add(ref _read, n);
        return n;
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => BytesRead; set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing) inner.Dispose();
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        await inner.DisposeAsync();
        await base.DisposeAsync();
    }
}
