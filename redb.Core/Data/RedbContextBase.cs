using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace redb.Core.Data
{
    /// <summary>
    /// Base class for REDB context.
    /// Provides common logic and shortcuts.
    /// DB-specific implementations inherit from this.
    /// </summary>
    public abstract class RedbContextBase : IRedbContext
    {
        // === ABSTRACT COMPONENTS (set by derived class) ===
        
        /// <summary>
        /// Database connection.
        /// </summary>
        public abstract IRedbConnection Db { get; }

        /// <inheritdoc />
        public bool IsDisposed => Db.IsDisposed;
        
        /// <summary>
        /// Key generator.
        /// </summary>
        public abstract IKeyGenerator Keys { get; }
        
        /// <summary>
        /// Bulk operations.
        /// </summary>
        // Reached to write (COPY protocol): the transaction has written.
        public IBulkOperations Bulk { get { TransactionWrites.Mark(this); return BulkOperations; } }

        /// <summary>The provider's bulk operations.</summary>
        protected abstract IBulkOperations BulkOperations { get; }

        // === CONNECTION SHORTCUTS ===
        
        /// <summary>
        /// Execute SQL query and return list of mapped objects.
        /// </summary>
        public Task<List<T>> QueryAsync<T>(string sql, params object[] parameters) where T : new()
            => Db.QueryAsync<T>(sql, parameters);

        /// <inheritdoc />
        public Task<List<T>> QueryAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken) where T : new()
            => Db.QueryAsync<T>(sql, parameters, cancellationToken);
        
        /// <summary>
        /// Execute SQL query and return first result or null.
        /// </summary>
        public Task<T?> QueryFirstOrDefaultAsync<T>(string sql, params object[] parameters) where T : class, new()
            => Db.QueryFirstOrDefaultAsync<T>(sql, parameters);

        /// <inheritdoc />
        public Task<T?> QueryFirstOrDefaultAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken) where T : class, new()
            => Db.QueryFirstOrDefaultAsync<T>(sql, parameters, cancellationToken);
        
        /// <summary>
        /// Execute SQL query and return scalar value.
        /// </summary>
        public Task<T?> ExecuteScalarAsync<T>(string sql, params object[] parameters)
            => Db.ExecuteScalarAsync<T>(sql, parameters);

        /// <inheritdoc />
        public Task<T?> ExecuteScalarAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken)
            => Db.ExecuteScalarAsync<T>(sql, parameters, cancellationToken);
        
        /// <summary>
        /// Execute SQL query and return list of scalar values (first column only).
        /// </summary>
        public Task<List<T>> QueryScalarListAsync<T>(string sql, params object[] parameters)
            => Db.QueryScalarListAsync<T>(sql, parameters);

        /// <inheritdoc />
        public Task<List<T>> QueryScalarListAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken)
            => Db.QueryScalarListAsync<T>(sql, parameters, cancellationToken);
        
        /// <summary>
        /// Execute SQL command (INSERT, UPDATE, DELETE).
        /// </summary>
        public Task<int> ExecuteAsync(string sql, params object[] parameters)
            { TransactionWrites.Mark(this); return Db.ExecuteAsync(sql, parameters); }

        /// <inheritdoc />
        public Task<int> ExecuteAsync(string sql, object[] parameters, CancellationToken cancellationToken)
            { TransactionWrites.Mark(this); return Db.ExecuteAsync(sql, parameters, cancellationToken); }
        
        /// <summary>
        /// Execute SQL returning JSON.
        /// </summary>
        public Task<string?> ExecuteJsonAsync(string sql, params object[] parameters)
            => Db.ExecuteJsonAsync(sql, parameters);

        /// <inheritdoc />
        public Task<string?> ExecuteJsonAsync(string sql, object[] parameters, CancellationToken cancellationToken)
            => Db.ExecuteJsonAsync(sql, parameters, cancellationToken);
        
        /// <summary>
        /// Execute SQL returning multiple JSON rows.
        /// </summary>
        public Task<List<string>> ExecuteJsonListAsync(string sql, params object[] parameters)
            => Db.ExecuteJsonListAsync(sql, parameters);

        /// <inheritdoc />
        public Task<List<string>> ExecuteJsonListAsync(string sql, object[] parameters, CancellationToken cancellationToken)
            => Db.ExecuteJsonListAsync(sql, parameters, cancellationToken);

        // === TRANSACTION SHORTCUTS ===
        
        /// <summary>
        /// Current active transaction.
        /// </summary>
        public IRedbTransaction? CurrentTransaction => Db.CurrentTransaction;
        
        /// <summary>
        /// Whether any transaction is active — explicit or ambient TransactionScope.
        /// </summary>
        public bool IsInTransaction => Db.IsInTransaction;
        
        /// <summary>
        /// Begin new transaction.
        /// </summary>
        public Task<IRedbTransaction> BeginTransactionAsync(System.Data.IsolationLevel? isolationLevel = null, CancellationToken cancellationToken = default)
            => Db.BeginTransactionAsync(isolationLevel, cancellationToken);
        
        /// <summary>
        /// Execute operations atomically.
        /// </summary>
        public Task ExecuteAtomicAsync(Func<Task> operations, CancellationToken cancellationToken = default)
            => Db.ExecuteAtomicAsync(operations, cancellationToken);
        
        /// <summary>
        /// Execute operations atomically and return result.
        /// </summary>
        public Task<T> ExecuteAtomicAsync<T>(Func<Task<T>> operations, CancellationToken cancellationToken = default)
            => Db.ExecuteAtomicAsync(operations, cancellationToken);

        /// <inheritdoc />
        public Task ExecuteAtomicAsync(System.Data.IsolationLevel isolationLevel, Func<Task> operations, CancellationToken cancellationToken = default)
            => Db.ExecuteAtomicAsync(isolationLevel, operations, cancellationToken);

        /// <inheritdoc />
        public Task<T> ExecuteAtomicAsync<T>(System.Data.IsolationLevel isolationLevel, Func<Task<T>> operations, CancellationToken cancellationToken = default)
            => Db.ExecuteAtomicAsync(isolationLevel, operations, cancellationToken);

        // === KEY GENERATION SHORTCUTS ===
        
        /// <summary>
        /// Get next object ID.
        /// </summary>
        public Task<long> NextObjectIdAsync(CancellationToken cancellationToken = default)
            => Keys.NextObjectIdAsync(cancellationToken);
        
        /// <summary>
        /// Get next value ID.
        /// </summary>
        public Task<long> NextValueIdAsync(CancellationToken cancellationToken = default)
            => Keys.NextValueIdAsync(cancellationToken);
        
        /// <summary>
        /// Get batch of object IDs.
        /// </summary>
        public Task<long[]> NextObjectIdBatchAsync(int count, CancellationToken cancellationToken = default)
            => Keys.NextObjectIdBatchAsync(count, cancellationToken);
        
        /// <summary>
        /// Get batch of value IDs.
        /// </summary>
        public Task<long[]> NextValueIdBatchAsync(int count, CancellationToken cancellationToken = default)
            => Keys.NextValueIdBatchAsync(count, cancellationToken);

        // === DISPOSE ===
        
        /// <summary>
        /// Dispose context and all components asynchronously.
        /// </summary>
        public virtual async ValueTask DisposeAsync()
        {
            await Db.DisposeAsync();
        }
        
        /// <summary>
        /// Dispose context and all components synchronously.
        /// Required for DI container compatibility.
        /// </summary>
        public virtual void Dispose()
        {
            // Synchronous dispose - call async version and wait
            Db.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }
}

