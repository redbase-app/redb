using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace redb.Core.Data
{
    /// <summary>
    /// Database connection abstraction for REDB.
    /// Replaces DbContext with pure ADO.NET approach.
    /// All methods automatically use CurrentTransaction if active.
    /// </summary>
    public interface IRedbConnection : IAsyncDisposable, IDisposable
    {
        /// <summary>True once the connection has been disposed - its scope has ended (V4, review).</summary>
        bool IsDisposed { get; }

        /// <summary>
        /// Connection string used for this connection.
        /// </summary>
        string ConnectionString { get; }
        
        /// <summary>
        /// Currently active transaction (null if no transaction).
        /// All operations automatically use this transaction.
        /// </summary>
        IRedbTransaction? CurrentTransaction { get; }
        
        /// <summary>
        /// Whether any transaction is active — explicit (CurrentTransaction) or ambient (TransactionScope).
        /// Use this instead of checking CurrentTransaction directly.
        /// EF Core pattern: checks both CurrentTransaction and Transaction.Current.
        /// </summary>
        bool IsInTransaction { get; }
        
        /// <summary>
        /// Get underlying DbConnection for advanced operations (e.g., COPY protocol).
        /// This ensures all operations use the same connection and transaction.
        /// </summary>
        Task<DbConnection> GetUnderlyingConnectionAsync(CancellationToken cancellationToken = default);
        
        // === QUERY METHODS ===
        
        /// <summary>
        /// Execute SQL query and return list of mapped objects.
        /// </summary>
        /// <typeparam name="T">Result type (must have parameterless constructor).</typeparam>
        /// <param name="sql">SQL query with parameters ($1, $2, etc.).</param>
        /// <param name="parameters">Query parameters.</param>
        /// <returns>List of mapped objects.</returns>
        Task<List<T>> QueryAsync<T>(string sql, params object[] parameters) where T : new();

        /// <summary>Cancellable form: parameters as an explicit array (params cannot precede ct).</summary>
        Task<List<T>> QueryAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken) where T : new();
        
        /// <summary>
        /// Execute SQL query and return first result or null.
        /// </summary>
        /// <typeparam name="T">Result type.</typeparam>
        /// <param name="sql">SQL query with parameters.</param>
        /// <param name="parameters">Query parameters.</param>
        /// <returns>First result or null.</returns>
        Task<T?> QueryFirstOrDefaultAsync<T>(string sql, params object[] parameters) where T : class, new();

        /// <summary>Cancellable form.</summary>
        Task<T?> QueryFirstOrDefaultAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken) where T : class, new();
        
        /// <summary>
        /// Execute SQL query and return scalar value.
        /// </summary>
        /// <typeparam name="T">Scalar type.</typeparam>
        /// <param name="sql">SQL query with parameters.</param>
        /// <param name="parameters">Query parameters.</param>
        /// <returns>Scalar value or default.</returns>
        Task<T?> ExecuteScalarAsync<T>(string sql, params object[] parameters);

        /// <summary>Cancellable form.</summary>
        Task<T?> ExecuteScalarAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken);

        // === SYNCHRONOUS COUNTERPARTS (thread-pool-free lazy path) ===
        // The sync getter of RedbListItem.Object must reach the database without a single
        // thread-pool continuation: on a saturated pool a blocked thread waiting for pool-scheduled
        // continuations degrades (or, pre-2026-09 fixes, deadlocks). These run the whole call on
        // the calling thread down to ADO.NET. The defaults fall back to blocking on the async
        // form, so third-party implementations keep compiling - but the fallback is pool-coupled;
        // the in-tree providers override with true sync ADO calls.

        /// <summary>Synchronous <see cref="QueryFirstOrDefaultAsync{T}(string, object[])"/>: runs on the calling thread down to ADO.NET.</summary>
        T? QueryFirstOrDefault<T>(string sql, params object[] parameters) where T : class, new()
            => QueryFirstOrDefaultAsync<T>(sql, parameters).ConfigureAwait(false).GetAwaiter().GetResult();

        /// <summary>Synchronous <see cref="ExecuteScalarAsync{T}(string, object[])"/>: runs on the calling thread down to ADO.NET.</summary>
        T? ExecuteScalar<T>(string sql, params object[] parameters)
            => ExecuteScalarAsync<T>(sql, parameters).ConfigureAwait(false).GetAwaiter().GetResult();

        /// <summary>Synchronous <see cref="ExecuteJsonAsync(string, object[])"/>: runs on the calling thread down to ADO.NET.</summary>
        string? ExecuteJson(string sql, params object[] parameters)
            => ExecuteJsonAsync(sql, parameters).ConfigureAwait(false).GetAwaiter().GetResult();

        /// <summary>Synchronous <see cref="QueryAsync{T}(string, object[])"/>: runs on the calling thread down to ADO.NET.</summary>
        List<T> Query<T>(string sql, params object[] parameters) where T : new()
            => QueryAsync<T>(sql, parameters).ConfigureAwait(false).GetAwaiter().GetResult();

        /// <summary>Synchronous <see cref="ExecuteAsync(string, object[])"/>: runs on the calling thread down to ADO.NET.</summary>
        int Execute(string sql, params object[] parameters)
            => ExecuteAsync(sql, parameters).ConfigureAwait(false).GetAwaiter().GetResult();

        /// <summary>
        /// Execute SQL query and return list of scalar values (first column only).
        /// Use for simple queries like SELECT _id FROM ... that return single column.
        /// </summary>
        /// <typeparam name="T">Scalar type (long, int, string, etc.).</typeparam>
        /// <param name="sql">SQL query with parameters.</param>
        /// <param name="parameters">Query parameters.</param>
        /// <returns>List of scalar values.</returns>
        Task<List<T>> QueryScalarListAsync<T>(string sql, params object[] parameters);

        /// <summary>Cancellable form.</summary>
        Task<List<T>> QueryScalarListAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken);
        
        /// <summary>
        /// Execute SQL command (INSERT, UPDATE, DELETE) and return affected rows count.
        /// </summary>
        /// <param name="sql">SQL command with parameters.</param>
        /// <param name="parameters">Command parameters.</param>
        /// <returns>Number of affected rows.</returns>
        Task<int> ExecuteAsync(string sql, params object[] parameters);

        /// <summary>Cancellable form.</summary>
        Task<int> ExecuteAsync(string sql, object[] parameters, CancellationToken cancellationToken);
        
        // === TRANSACTION METHODS ===
        
        /// <summary>
        /// Begin new transaction.
        /// Sets CurrentTransaction property.
        /// All subsequent operations will use this transaction.
        /// </summary>
        /// <returns>Transaction object.</returns>
        /// <param name="isolationLevel">
        /// BR-1 (owner decision 2026-09-02): optional isolation level for THIS transaction. Null keeps
        /// each provider's default, exactly as before. PostgreSQL/MSSQL apply it to the opened
        /// transaction; SQLite accepts it for portability and stays a single serial writer. Under
        /// elevated levels the database may abort the loser (PG 40001, MSSQL 3960): retry the WHOLE
        /// transaction - see DbErrorClassifier.IsSerializationFailure.
        /// </param>
        Task<IRedbTransaction> BeginTransactionAsync(System.Data.IsolationLevel? isolationLevel = null, CancellationToken cancellationToken = default);
        
        // === ATOMIC OPERATIONS (SaveChanges replacement) ===
        
        /// <summary>
        /// Execute multiple operations atomically (all-or-nothing).
        /// If no transaction is active, creates one automatically.
        /// Ensures atomicity like EF SaveChanges().
        /// </summary>
        /// <param name="operations">Operations to execute atomically.</param>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task ExecuteAtomicAsync(Func<Task> operations, CancellationToken cancellationToken = default);


        /// <summary>
        /// Atomic execution under an explicit isolation level (BR-1). An active or ambient transaction
        /// wins: operations join it and its level is NOT changed.
        /// </summary>
        Task ExecuteAtomicAsync(System.Data.IsolationLevel isolationLevel, Func<Task> operations, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Execute multiple operations atomically and return result.
        /// If no transaction is active, creates one automatically.
        /// </summary>
        /// <typeparam name="T">Result type.</typeparam>
        /// <param name="operations">Operations to execute atomically.</param>
        /// <returns>Operation result.</returns>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task<T> ExecuteAtomicAsync<T>(Func<Task<T>> operations, CancellationToken cancellationToken = default);


        /// <summary>Result-returning form of the isolation-level overload (BR-1).</summary>
        Task<T> ExecuteAtomicAsync<T>(System.Data.IsolationLevel isolationLevel, Func<Task<T>> operations, CancellationToken cancellationToken = default);
        
        // === RAW SQL ===
        
        /// <summary>
        /// Execute raw SQL function returning JSON.
        /// Commonly used for PostgreSQL functions like get_object_json().
        /// </summary>
        /// <param name="sql">SQL query returning JSON.</param>
        /// <param name="parameters">Query parameters.</param>
        /// <returns>JSON string or null.</returns>
        Task<string?> ExecuteJsonAsync(string sql, params object[] parameters);

        /// <summary>Cancellable form.</summary>
        Task<string?> ExecuteJsonAsync(string sql, object[] parameters, CancellationToken cancellationToken);
        
        /// <summary>
        /// Execute raw SQL function returning multiple JSON rows.
        /// </summary>
        /// <param name="sql">SQL query returning JSON rows.</param>
        /// <param name="parameters">Query parameters.</param>
        /// <returns>List of JSON strings.</returns>
        Task<List<string>> ExecuteJsonListAsync(string sql, params object[] parameters);

        /// <summary>Cancellable form.</summary>
        Task<List<string>> ExecuteJsonListAsync(string sql, object[] parameters, CancellationToken cancellationToken);
    }
}

