using System;
using System.Collections.Generic;
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
        
        /// <summary>
        /// Execute SQL query and return first result or null.
        /// </summary>
        Task<T?> QueryFirstOrDefaultAsync<T>(string sql, params object[] parameters) where T : class, new();
        
        /// <summary>
        /// Execute SQL query and return scalar value.
        /// </summary>
        Task<T?> ExecuteScalarAsync<T>(string sql, params object[] parameters);
        
        /// <summary>
        /// Execute SQL query and return list of scalar values (first column only).
        /// </summary>
        Task<List<T>> QueryScalarListAsync<T>(string sql, params object[] parameters);
        
        /// <summary>
        /// Execute SQL command (INSERT, UPDATE, DELETE).
        /// </summary>
        Task<int> ExecuteAsync(string sql, params object[] parameters);
        
        /// <summary>
        /// Execute SQL returning JSON.
        /// </summary>
        Task<string?> ExecuteJsonAsync(string sql, params object[] parameters);
        
        /// <summary>
        /// Execute SQL returning multiple JSON rows.
        /// </summary>
        Task<List<string>> ExecuteJsonListAsync(string sql, params object[] parameters);
        
        // === TRANSACTION SHORTCUTS ===
        
        /// <summary>
        /// Current active transaction (null if none).
        /// </summary>
        IRedbTransaction? CurrentTransaction { get; }
        
        /// <summary>
        /// Begin new transaction.
        /// </summary>
        Task<IRedbTransaction> BeginTransactionAsync();
        
        /// <summary>
        /// Execute operations atomically (all-or-nothing).
        /// Like EF SaveChanges() but explicit.
        /// </summary>
        Task ExecuteAtomicAsync(Func<Task> operations);
        
        /// <summary>
        /// Execute operations atomically and return result.
        /// </summary>
        Task<T> ExecuteAtomicAsync<T>(Func<Task<T>> operations);
        
        // === KEY GENERATION SHORTCUTS ===
        
        /// <summary>
        /// Get next object ID.
        /// </summary>
        Task<long> NextObjectIdAsync();
        
        /// <summary>
        /// Get next value ID.
        /// </summary>
        Task<long> NextValueIdAsync();
        
        /// <summary>
        /// Get batch of object IDs.
        /// </summary>
        Task<long[]> NextObjectIdBatchAsync(int count);
        
        /// <summary>
        /// Get batch of value IDs.
        /// </summary>
        Task<long[]> NextValueIdBatchAsync(int count);
    }
}

