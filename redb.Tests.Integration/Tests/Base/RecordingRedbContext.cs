using System.Collections.Concurrent;
using System.Data;
using Microsoft.Extensions.DependencyInjection;
using redb.Core.Data;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// One command seen by <see cref="RecordingRedbContext"/>: its SQL text, the thread it was issued on and the context
/// instance (one scope, one connection) it ran on.
/// </summary>
public sealed record RecordedCommand(string Sql, int ThreadId, IRedbContext Context);

/// <summary>
/// Commands that went through a <see cref="RecordingRedbContext"/> between <see cref="Start"/> and <see cref="Stop"/>,
/// and how many contexts - scopes with a connection of their own - were created meanwhile.
/// </summary>
public sealed class SqlRecorder
{
    private readonly ConcurrentQueue<RecordedCommand> _commands = new();
    private volatile bool _armed;
    private int _contextsCreated;

    /// <summary>Contexts created since <see cref="Start"/>.</summary>
    public int ContextsCreated => Volatile.Read(ref _contextsCreated);

    public void Start()
    {
        _commands.Clear();
        Interlocked.Exchange(ref _contextsCreated, 0);
        _armed = true;
    }

    public IReadOnlyList<RecordedCommand> Stop()
    {
        _armed = false;
        return _commands.ToArray();
    }

    /// <summary>The commands recorded so far; recording goes on.</summary>
    public IReadOnlyList<RecordedCommand> Peek() => _commands.ToArray();

    internal void Note(IRedbContext context, string sql)
    {
        if (_armed) _commands.Enqueue(new RecordedCommand(sql, Environment.CurrentManagedThreadId, context));
    }

    internal void NoteCreated()
    {
        if (_armed) Interlocked.Increment(ref _contextsCreated);
    }
}

/// <summary>
/// Test decorator over the provider's <see cref="IRedbContext"/>: records the SQL text of every command that goes
/// through the context, then forwards it. It tells WHICH road a load took - the Free in-database JSON builder or the Pro
/// C# materializer - and ON WHICH context, without depending on the database having that function: on SQLite the Free
/// native extension is process-wide static state that another suite may or may not have loaded.
/// <para>
/// Every SQL-bearing member is implemented here, the synchronous ones included: their interface defaults forward to
/// <see cref="IRedbContext.Db"/> and would bypass the record. A new SQL member of <see cref="IRedbContext"/> must be
/// added here too.
/// </para>
/// </summary>
public sealed class RecordingRedbContext : IRedbContext
{
    private readonly IRedbContext _inner;
    private readonly SqlRecorder _recorder;

    public RecordingRedbContext(IRedbContext inner, SqlRecorder recorder)
    {
        _inner = inner;
        _recorder = recorder;
    }

    /// <summary>Wraps the last <see cref="IRedbContext"/> registration - the one every service resolves.</summary>
    public static void Decorate(IServiceCollection services, SqlRecorder recorder)
    {
        var descriptor = services.LastOrDefault(d => d.ServiceType == typeof(IRedbContext))
            ?? throw new InvalidOperationException("No IRedbContext registration to decorate");
        var factory = descriptor.ImplementationFactory
            ?? throw new InvalidOperationException($"IRedbContext is expected to be registered by a factory: {descriptor}");
        services.Remove(descriptor);
        services.Add(new ServiceDescriptor(typeof(IRedbContext), sp =>
        {
            recorder.NoteCreated();
            return new RecordingRedbContext((IRedbContext)factory(sp), recorder);
        }, descriptor.Lifetime));
    }

    private string Note(string sql)
    {
        _recorder.Note(this, sql);
        return sql;
    }

    public bool IsDisposed => _inner.IsDisposed;
    public IRedbConnection Db => _inner.Db;
    public IKeyGenerator Keys => _inner.Keys;
    public IBulkOperations Bulk => _inner.Bulk;

    public Task<List<T>> QueryAsync<T>(string sql, params object[] parameters) where T : new()
        => _inner.QueryAsync<T>(Note(sql), parameters);

    public Task<List<T>> QueryAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken) where T : new()
        => _inner.QueryAsync<T>(Note(sql), parameters, cancellationToken);

    public Task<T?> QueryFirstOrDefaultAsync<T>(string sql, params object[] parameters) where T : class, new()
        => _inner.QueryFirstOrDefaultAsync<T>(Note(sql), parameters);

    public Task<T?> QueryFirstOrDefaultAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken) where T : class, new()
        => _inner.QueryFirstOrDefaultAsync<T>(Note(sql), parameters, cancellationToken);

    public Task<T?> ExecuteScalarAsync<T>(string sql, params object[] parameters)
        => _inner.ExecuteScalarAsync<T>(Note(sql), parameters);

    public Task<T?> ExecuteScalarAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken)
        => _inner.ExecuteScalarAsync<T>(Note(sql), parameters, cancellationToken);

    public T? QueryFirstOrDefault<T>(string sql, params object[] parameters) where T : class, new()
        => _inner.QueryFirstOrDefault<T>(Note(sql), parameters);

    public T? ExecuteScalar<T>(string sql, params object[] parameters)
        => _inner.ExecuteScalar<T>(Note(sql), parameters);

    public string? ExecuteJson(string sql, params object[] parameters)
        => _inner.ExecuteJson(Note(sql), parameters);

    public List<T> Query<T>(string sql, params object[] parameters) where T : new()
        => _inner.Query<T>(Note(sql), parameters);

    public int Execute(string sql, params object[] parameters)
        => _inner.Execute(Note(sql), parameters);

    public Task<List<T>> QueryScalarListAsync<T>(string sql, params object[] parameters)
        => _inner.QueryScalarListAsync<T>(Note(sql), parameters);

    public Task<List<T>> QueryScalarListAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken)
        => _inner.QueryScalarListAsync<T>(Note(sql), parameters, cancellationToken);

    public Task<int> ExecuteAsync(string sql, params object[] parameters)
        => _inner.ExecuteAsync(Note(sql), parameters);

    public Task<int> ExecuteAsync(string sql, object[] parameters, CancellationToken cancellationToken)
        => _inner.ExecuteAsync(Note(sql), parameters, cancellationToken);

    public Task<string?> ExecuteJsonAsync(string sql, params object[] parameters)
        => _inner.ExecuteJsonAsync(Note(sql), parameters);

    public Task<string?> ExecuteJsonAsync(string sql, object[] parameters, CancellationToken cancellationToken)
        => _inner.ExecuteJsonAsync(Note(sql), parameters, cancellationToken);

    public Task<List<string>> ExecuteJsonListAsync(string sql, params object[] parameters)
        => _inner.ExecuteJsonListAsync(Note(sql), parameters);

    public Task<List<string>> ExecuteJsonListAsync(string sql, object[] parameters, CancellationToken cancellationToken)
        => _inner.ExecuteJsonListAsync(Note(sql), parameters, cancellationToken);

    public IRedbTransaction? CurrentTransaction => _inner.CurrentTransaction;
    public bool IsInTransaction => _inner.IsInTransaction;

    public Task<IRedbTransaction> BeginTransactionAsync(IsolationLevel? isolationLevel = null, CancellationToken cancellationToken = default)
        => _inner.BeginTransactionAsync(isolationLevel, cancellationToken);

    public Task ExecuteAtomicAsync(Func<Task> operations, CancellationToken cancellationToken = default)
        => _inner.ExecuteAtomicAsync(operations, cancellationToken);

    public Task ExecuteAtomicAsync(IsolationLevel isolationLevel, Func<Task> operations, CancellationToken cancellationToken = default)
        => _inner.ExecuteAtomicAsync(isolationLevel, operations, cancellationToken);

    public Task<T> ExecuteAtomicAsync<T>(Func<Task<T>> operations, CancellationToken cancellationToken = default)
        => _inner.ExecuteAtomicAsync(operations, cancellationToken);

    public Task<T> ExecuteAtomicAsync<T>(IsolationLevel isolationLevel, Func<Task<T>> operations, CancellationToken cancellationToken = default)
        => _inner.ExecuteAtomicAsync(isolationLevel, operations, cancellationToken);

    public Task<long> NextObjectIdAsync(CancellationToken cancellationToken = default) => _inner.NextObjectIdAsync(cancellationToken);
    public Task<long> NextValueIdAsync(CancellationToken cancellationToken = default) => _inner.NextValueIdAsync(cancellationToken);
    public Task<long[]> NextObjectIdBatchAsync(int count, CancellationToken cancellationToken = default) => _inner.NextObjectIdBatchAsync(count, cancellationToken);
    public Task<long[]> NextValueIdBatchAsync(int count, CancellationToken cancellationToken = default) => _inner.NextValueIdBatchAsync(count, cancellationToken);

    public ValueTask DisposeAsync() => _inner.DisposeAsync();
    public void Dispose() => _inner.Dispose();
}
