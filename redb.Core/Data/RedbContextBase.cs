using System;
using System.Collections.Generic;
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
        
        /// <summary>
        /// Key generator.
        /// </summary>
        public abstract IKeyGenerator Keys { get; }
        
        /// <summary>
        /// Bulk operations.
        /// </summary>
        public abstract IBulkOperations Bulk { get; }

        // === CONNECTION SHORTCUTS ===
        
        /// <summary>
        /// Execute SQL query and return list of mapped objects.
        /// </summary>
        public Task<List<T>> QueryAsync<T>(string sql, params object[] parameters) where T : new()
            => Db.QueryAsync<T>(sql, parameters);
        
        /// <summary>
        /// Execute SQL query and return first result or null.
        /// </summary>
        public Task<T?> QueryFirstOrDefaultAsync<T>(string sql, params object[] parameters) where T : class, new()
            => Db.QueryFirstOrDefaultAsync<T>(sql, parameters);
        
        /// <summary>
        /// Execute SQL query and return scalar value.
        /// </summary>
        public Task<T?> ExecuteScalarAsync<T>(string sql, params object[] parameters)
            => Db.ExecuteScalarAsync<T>(sql, parameters);
        
        /// <summary>
        /// Execute SQL query and return list of scalar values (first column only).
        /// </summary>
        public Task<List<T>> QueryScalarListAsync<T>(string sql, params object[] parameters)
            => Db.QueryScalarListAsync<T>(sql, parameters);
        
        /// <summary>
        /// Execute SQL command (INSERT, UPDATE, DELETE).
        /// </summary>
        public Task<int> ExecuteAsync(string sql, params object[] parameters)
            => Db.ExecuteAsync(sql, parameters);
        
        /// <summary>
        /// Execute SQL returning JSON.
        /// </summary>
        public Task<string?> ExecuteJsonAsync(string sql, params object[] parameters)
            => Db.ExecuteJsonAsync(sql, parameters);
        
        /// <summary>
        /// Execute SQL returning multiple JSON rows.
        /// </summary>
        public Task<List<string>> ExecuteJsonListAsync(string sql, params object[] parameters)
            => Db.ExecuteJsonListAsync(sql, parameters);

        // === TRANSACTION SHORTCUTS ===
        
        /// <summary>
        /// Current active transaction.
        /// </summary>
        public IRedbTransaction? CurrentTransaction => Db.CurrentTransaction;
        
        /// <summary>
        /// Begin new transaction.
        /// </summary>
        public Task<IRedbTransaction> BeginTransactionAsync()
            => Db.BeginTransactionAsync();
        
        /// <summary>
        /// Execute operations atomically.
        /// </summary>
        public Task ExecuteAtomicAsync(Func<Task> operations)
            => Db.ExecuteAtomicAsync(operations);
        
        /// <summary>
        /// Execute operations atomically and return result.
        /// </summary>
        public Task<T> ExecuteAtomicAsync<T>(Func<Task<T>> operations)
            => Db.ExecuteAtomicAsync(operations);

        // === KEY GENERATION SHORTCUTS ===
        
        /// <summary>
        /// Get next object ID.
        /// </summary>
        public Task<long> NextObjectIdAsync()
            => Keys.NextObjectIdAsync();
        
        /// <summary>
        /// Get next value ID.
        /// </summary>
        public Task<long> NextValueIdAsync()
            => Keys.NextValueIdAsync();
        
        /// <summary>
        /// Get batch of object IDs.
        /// </summary>
        public Task<long[]> NextObjectIdBatchAsync(int count)
            => Keys.NextObjectIdBatchAsync(count);
        
        /// <summary>
        /// Get batch of value IDs.
        /// </summary>
        public Task<long[]> NextValueIdBatchAsync(int count)
            => Keys.NextValueIdBatchAsync(count);

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

