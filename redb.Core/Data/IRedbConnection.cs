using System;
using System.Collections.Generic;
using System.Data.Common;
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
        /// Get underlying DbConnection for advanced operations (e.g., COPY protocol).
        /// This ensures all operations use the same connection and transaction.
        /// </summary>
        Task<DbConnection> GetUnderlyingConnectionAsync();
        
        // === QUERY METHODS ===
        
        /// <summary>
        /// Execute SQL query and return list of mapped objects.
        /// </summary>
        /// <typeparam name="T">Result type (must have parameterless constructor).</typeparam>
        /// <param name="sql">SQL query with parameters ($1, $2, etc.).</param>
        /// <param name="parameters">Query parameters.</param>
        /// <returns>List of mapped objects.</returns>
        Task<List<T>> QueryAsync<T>(string sql, params object[] parameters) where T : new();
        
        /// <summary>
        /// Execute SQL query and return first result or null.
        /// </summary>
        /// <typeparam name="T">Result type.</typeparam>
        /// <param name="sql">SQL query with parameters.</param>
        /// <param name="parameters">Query parameters.</param>
        /// <returns>First result or null.</returns>
        Task<T?> QueryFirstOrDefaultAsync<T>(string sql, params object[] parameters) where T : class, new();
        
        /// <summary>
        /// Execute SQL query and return scalar value.
        /// </summary>
        /// <typeparam name="T">Scalar type.</typeparam>
        /// <param name="sql">SQL query with parameters.</param>
        /// <param name="parameters">Query parameters.</param>
        /// <returns>Scalar value or default.</returns>
        Task<T?> ExecuteScalarAsync<T>(string sql, params object[] parameters);
        
        /// <summary>
        /// Execute SQL query and return list of scalar values (first column only).
        /// Use for simple queries like SELECT _id FROM ... that return single column.
        /// </summary>
        /// <typeparam name="T">Scalar type (long, int, string, etc.).</typeparam>
        /// <param name="sql">SQL query with parameters.</param>
        /// <param name="parameters">Query parameters.</param>
        /// <returns>List of scalar values.</returns>
        Task<List<T>> QueryScalarListAsync<T>(string sql, params object[] parameters);
        
        /// <summary>
        /// Execute SQL command (INSERT, UPDATE, DELETE) and return affected rows count.
        /// </summary>
        /// <param name="sql">SQL command with parameters.</param>
        /// <param name="parameters">Command parameters.</param>
        /// <returns>Number of affected rows.</returns>
        Task<int> ExecuteAsync(string sql, params object[] parameters);
        
        // === TRANSACTION METHODS ===
        
        /// <summary>
        /// Begin new transaction.
        /// Sets CurrentTransaction property.
        /// All subsequent operations will use this transaction.
        /// </summary>
        /// <returns>Transaction object.</returns>
        Task<IRedbTransaction> BeginTransactionAsync();
        
        // === ATOMIC OPERATIONS (SaveChanges replacement) ===
        
        /// <summary>
        /// Execute multiple operations atomically (all-or-nothing).
        /// If no transaction is active, creates one automatically.
        /// Ensures atomicity like EF SaveChanges().
        /// </summary>
        /// <param name="operations">Operations to execute atomically.</param>
        Task ExecuteAtomicAsync(Func<Task> operations);
        
        /// <summary>
        /// Execute multiple operations atomically and return result.
        /// If no transaction is active, creates one automatically.
        /// </summary>
        /// <typeparam name="T">Result type.</typeparam>
        /// <param name="operations">Operations to execute atomically.</param>
        /// <returns>Operation result.</returns>
        Task<T> ExecuteAtomicAsync<T>(Func<Task<T>> operations);
        
        // === RAW SQL ===
        
        /// <summary>
        /// Execute raw SQL function returning JSON.
        /// Commonly used for PostgreSQL functions like get_object_json().
        /// </summary>
        /// <param name="sql">SQL query returning JSON.</param>
        /// <param name="parameters">Query parameters.</param>
        /// <returns>JSON string or null.</returns>
        Task<string?> ExecuteJsonAsync(string sql, params object[] parameters);
        
        /// <summary>
        /// Execute raw SQL function returning multiple JSON rows.
        /// </summary>
        /// <param name="sql">SQL query returning JSON rows.</param>
        /// <param name="parameters">Query parameters.</param>
        /// <returns>List of JSON strings.</returns>
        Task<List<string>> ExecuteJsonListAsync(string sql, params object[] parameters);
    }
}

