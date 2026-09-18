using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace redb.Core.Data
{
    /// <summary>
    /// Main database context interface for REDB.
    /// Facade over connection, key generator, and bulk operations.
    /// Replaces EF Core DbContext.
    /// </summary>
    public interface IRedbContext : IAsyncDisposable, IDisposable
    {
        /// <summary>True once the context has been disposed - the scope that owned it has ended (V4, review).</summary>
        bool IsDisposed { get; }

        // === COMPONENTS ===
        
        /// <summary>
        /// Database connection for queries and commands.
        /// </summary>
        IRedbConnection Db { get; }
        
        /// <summary>
        /// Key generator with caching.
        /// </summary>
        IKeyGenerator Keys { get; }
        
        /// <summary>
        /// Bulk operations (COPY protocol).
        /// </summary>
        IBulkOperations Bulk { get; }
        
        // === CONNECTION SHORTCUTS ===
        
        /// <summary>
        /// Execute SQL query and return list of mapped objects.
        /// </summary>
        Task<List<T>> QueryAsync<T>(string sql, params object[] parameters) where T : new();

        /// <summary>Cancellable form: parameters as an explicit array (params cannot precede ct).</summary>
        Task<List<T>> QueryAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken) where T : new();
        
        /// <summary>
        /// Execute SQL query and return first result or null.
        /// </summary>
        Task<T?> QueryFirstOrDefaultAsync<T>(string sql, params object[] parameters) where T : class, new();

        /// <summary>Cancellable form.</summary>
        Task<T?> QueryFirstOrDefaultAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken) where T : class, new();
        
        /// <summary>
        /// Execute SQL query and return scalar value.
        /// </summary>
        Task<T?> ExecuteScalarAsync<T>(string sql, params object[] parameters);

        /// <summary>Cancellable form.</summary>
        Task<T?> ExecuteScalarAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken);

        // === SYNCHRONOUS COUNTERPARTS (thread-pool-free lazy path) ===
        // See IRedbConnection: these forward to the connection, which runs the call on the
        // calling thread down to ADO.NET (no thread-pool continuations). Used by the sync
        // getter of RedbListItem.Object.

        /// <summary>Synchronous <see cref="QueryFirstOrDefaultAsync{T}(string, object[])"/> on the calling thread.</summary>
        T? QueryFirstOrDefault<T>(string sql, params object[] parameters) where T : class, new()
            => Db.QueryFirstOrDefault<T>(sql, parameters);

        /// <summary>Synchronous <see cref="ExecuteScalarAsync{T}(string, object[])"/> on the calling thread.</summary>
        T? ExecuteScalar<T>(string sql, params object[] parameters)
            => Db.ExecuteScalar<T>(sql, parameters);

        /// <summary>Synchronous <see cref="ExecuteJsonAsync(string, object[])"/> on the calling thread.</summary>
        string? ExecuteJson(string sql, params object[] parameters)
            => Db.ExecuteJson(sql, parameters);

        /// <summary>Synchronous <see cref="QueryAsync{T}(string, object[])"/> on the calling thread.</summary>
        List<T> Query<T>(string sql, params object[] parameters) where T : new()
            => Db.Query<T>(sql, parameters);

        /// <summary>Synchronous <see cref="ExecuteAsync(string, object[])"/> on the calling thread.</summary>
        int Execute(string sql, params object[] parameters)
            => Db.Execute(sql, parameters);

        /// <summary>
        /// Execute SQL query and return list of scalar values (first column only).
        /// </summary>
        Task<List<T>> QueryScalarListAsync<T>(string sql, params object[] parameters);

        /// <summary>Cancellable form.</summary>
        Task<List<T>> QueryScalarListAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken);
        
        /// <summary>
        /// Execute SQL command (INSERT, UPDATE, DELETE).
        /// </summary>
        Task<int> ExecuteAsync(string sql, params object[] parameters);

        /// <summary>Cancellable form.</summary>
        Task<int> ExecuteAsync(string sql, object[] parameters, CancellationToken cancellationToken);
        
        /// <summary>
        /// Execute SQL returning JSON.
        /// </summary>
        Task<string?> ExecuteJsonAsync(string sql, params object[] parameters);

        /// <summary>Cancellable form.</summary>
        Task<string?> ExecuteJsonAsync(string sql, object[] parameters, CancellationToken cancellationToken);
        
        /// <summary>
        /// Execute SQL returning multiple JSON rows.
        /// </summary>
        Task<List<string>> ExecuteJsonListAsync(string sql, params object[] parameters);

        /// <summary>Cancellable form.</summary>
        Task<List<string>> ExecuteJsonListAsync(string sql, object[] parameters, CancellationToken cancellationToken);
        
        // === TRANSACTION SHORTCUTS ===
        
        /// <summary>
        /// Current active transaction (null if none).
        /// </summary>
        IRedbTransaction? CurrentTransaction { get; }
        
        /// <summary>
        /// Whether any transaction is active — explicit (CurrentTransaction) or ambient (TransactionScope).
        /// EF Core pattern: checks both CurrentTransaction and Transaction.Current.
        /// </summary>
        bool IsInTransaction { get; }
        
        /// <summary>
        /// Begin new transaction.
        /// </summary>
        /// <param name="isolationLevel">
        /// BR-1 (owner decision 2026-09-02): optional isolation level for THIS transaction. Null keeps
        /// each provider's default, exactly as before. PostgreSQL/MSSQL apply it to the opened
        /// transaction; SQLite accepts it for portability and stays a single serial writer. Under
        /// elevated levels the database may abort the loser (PG 40001, MSSQL 3960): retry the WHOLE
        /// transaction - see DbErrorClassifier.IsSerializationFailure.
        /// </param>
        Task<IRedbTransaction> BeginTransactionAsync(System.Data.IsolationLevel? isolationLevel = null, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Execute operations atomically (all-or-nothing).
        /// Like EF SaveChanges() but explicit.
        /// </summary>
        Task ExecuteAtomicAsync(Func<Task> operations, CancellationToken cancellationToken = default);


        /// <summary>
        /// Atomic execution under an explicit isolation level (BR-1). An active or ambient transaction
        /// wins: operations join it and its level is NOT changed.
        /// </summary>
        Task ExecuteAtomicAsync(System.Data.IsolationLevel isolationLevel, Func<Task> operations, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Execute operations atomically and return result.
        /// </summary>
        Task<T> ExecuteAtomicAsync<T>(Func<Task<T>> operations, CancellationToken cancellationToken = default);


        /// <summary>Result-returning form of the isolation-level overload (BR-1).</summary>
        Task<T> ExecuteAtomicAsync<T>(System.Data.IsolationLevel isolationLevel, Func<Task<T>> operations, CancellationToken cancellationToken = default);
        
        // === KEY GENERATION SHORTCUTS ===
        
        /// <summary>
        /// Get next object ID.
        /// </summary>
        Task<long> NextObjectIdAsync(CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Get next value ID.
        /// </summary>
        Task<long> NextValueIdAsync(CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Get batch of object IDs.
        /// </summary>
        Task<long[]> NextObjectIdBatchAsync(int count, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Get batch of value IDs.
        /// </summary>
        Task<long[]> NextValueIdBatchAsync(int count, CancellationToken cancellationToken = default);
    }
}

