using Npgsql;
using redb.Core.Data;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace redb.Postgres.Data
{
    /// <summary>
    /// PostgreSQL implementation of IRedbConnection using Npgsql.
    /// Provides pure ADO.NET database access with automatic transaction management.
    /// </summary>
    public class NpgsqlRedbConnection : IRedbConnection
    {
        private readonly NpgsqlDataSource _dataSource;
        private NpgsqlConnection? _connection;
        private NpgsqlRedbTransaction? _currentTransaction;
        private bool _disposed = false;
        
        /// <summary>
        /// Connection string.
        /// </summary>
        public string ConnectionString => _dataSource.ConnectionString;
        
        /// <summary>
        /// Current active transaction.
        /// </summary>
        public IRedbTransaction? CurrentTransaction => _currentTransaction;

        /// <summary>
        /// Create connection from data source.
        /// </summary>
        /// <param name="dataSource">Npgsql data source (pooled).</param>
        public NpgsqlRedbConnection(NpgsqlDataSource dataSource)
        {
            _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        }
        
        /// <summary>
        /// Create connection from connection string.
        /// </summary>
        /// <param name="connectionString">PostgreSQL connection string.</param>
        public NpgsqlRedbConnection(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException(nameof(connectionString));
            
            _dataSource = NpgsqlDataSource.Create(connectionString);
        }

        // === CONNECTION MANAGEMENT ===
        
        /// <summary>
        /// Get underlying connection (for bulk operations).
        /// This ensures all operations use the same connection and transaction.
        /// </summary>
        public async Task<System.Data.Common.DbConnection> GetUnderlyingConnectionAsync()
        {
            return await GetOpenConnectionAsync();
        }
        
        private async Task<NpgsqlConnection> GetOpenConnectionAsync()
        {
            if (_connection == null)
            {
                _connection = await _dataSource.OpenConnectionAsync();
            }
            else if (_connection.State != System.Data.ConnectionState.Open)
            {
                await _connection.OpenAsync();
            }
            return _connection;
        }
        
        private NpgsqlCommand CreateCommand(NpgsqlConnection connection, string sql, object[] parameters)
        {
            // Convert @p0, @p1 format to PostgreSQL $1, $2 format for cross-platform compatibility
            var convertedSql = ConvertParameters(sql, parameters.Length);
            var cmd = new NpgsqlCommand(convertedSql, connection);
            
            // Set transaction if active
            if (_currentTransaction != null)
            {
                cmd.Transaction = _currentTransaction.NpgsqlTransaction;
            }
            
            // Add positional parameters ($1, $2, etc.)
            foreach (var param in parameters)
            {
                NpgsqlParameter npgsqlParam;
                
                if (param == null)
                {
                    npgsqlParam = new NpgsqlParameter { Value = DBNull.Value };
                }
                else if (param is DateTimeOffset dto)
                {
                    // Both DateTimeOffset and DateTimeOffset? (when HasValue) match here
                    npgsqlParam = new NpgsqlParameter { Value = dto.ToUniversalTime() };
                }
                else if (param is long[] longArray)
                {
                    // Explicitly set array type for PostgreSQL bigint[]
                    npgsqlParam = new NpgsqlParameter 
                    { 
                        Value = longArray, 
                        NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Bigint 
                    };
                }
                else if (param is int[] intArray)
                {
                    // Explicitly set array type for PostgreSQL integer[]
                    npgsqlParam = new NpgsqlParameter 
                    { 
                        Value = intArray, 
                        NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer 
                    };
                }
                else if (param is string[] stringArray)
                {
                    // Explicitly set array type for PostgreSQL text[]
                    npgsqlParam = new NpgsqlParameter 
                    { 
                        Value = stringArray, 
                        NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text 
                    };
                }
                else
                {
                    npgsqlParam = new NpgsqlParameter { Value = param };
                }
                
                cmd.Parameters.Add(npgsqlParam);
            }
            
            return cmd;
        }

        // === QUERY METHODS ===
        
        /// <summary>
        /// Execute SQL query and map results to list of objects.
        /// Uses JsonPropertyName attribute for snake_case to PascalCase mapping.
        /// </summary>
        public async Task<List<T>> QueryAsync<T>(string sql, params object[] parameters) where T : new()
        {
            var conn = await GetOpenConnectionAsync();
            await using var cmd = CreateCommand(conn, sql, parameters);
            await using var reader = await cmd.ExecuteReaderAsync();
            
            var results = new List<T>();
            var mapper = new RedbRowMapper<T>();
            
            while (await reader.ReadAsync())
            {
                results.Add(mapper.MapRow(reader));
            }
            
            return results;
        }
        
        /// <summary>
        /// Execute SQL query and return first result.
        /// </summary>
        public async Task<T?> QueryFirstOrDefaultAsync<T>(string sql, params object[] parameters) where T : class, new()
        {
            var conn = await GetOpenConnectionAsync();
            await using var cmd = CreateCommand(conn, sql, parameters);
            await using var reader = await cmd.ExecuteReaderAsync();
            
            if (await reader.ReadAsync())
            {
                var mapper = new RedbRowMapper<T>();
                return mapper.MapRow(reader);
            }
            
            return null;
        }
        
        /// <summary>
        /// Execute SQL query and return scalar value.
        /// </summary>
        public async Task<T?> ExecuteScalarAsync<T>(string sql, params object[] parameters)
        {
            var conn = await GetOpenConnectionAsync();
            await using var cmd = CreateCommand(conn, sql, parameters);
            var result = await cmd.ExecuteScalarAsync();
            
            if (result == null || result == DBNull.Value)
                return default;
            
            // Handle nullable types
            var targetType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
            
            // Direct cast if types match
            if (result.GetType() == targetType || targetType.IsAssignableFrom(result.GetType()))
            {
                return (T)result;
            }
            
            // Convert for numeric types etc.
            return (T)Convert.ChangeType(result, targetType);
        }
        
        /// <summary>
        /// Execute SQL command (INSERT, UPDATE, DELETE).
        /// </summary>
        public async Task<int> ExecuteAsync(string sql, params object[] parameters)
        {
            var conn = await GetOpenConnectionAsync();
            await using var cmd = CreateCommand(conn, sql, parameters);
            return await cmd.ExecuteNonQueryAsync();
        }
        
        /// <summary>
        /// Execute SQL query and return list of scalar values (first column only).
        /// Use for simple queries like SELECT _id FROM ... that return single column.
        /// </summary>
        public async Task<List<T>> QueryScalarListAsync<T>(string sql, params object[] parameters)
        {
            var conn = await GetOpenConnectionAsync();
            await using var cmd = CreateCommand(conn, sql, parameters);
            await using var reader = await cmd.ExecuteReaderAsync();
            
            var results = new List<T>();
            var targetType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
            
            while (await reader.ReadAsync())
            {
                if (reader.IsDBNull(0))
                {
                    results.Add(default!);
                }
                else
                {
                    var value = reader.GetValue(0);
                    results.Add((T)Convert.ChangeType(value, targetType));
                }
            }
            
            return results;
        }

        // === TRANSACTION METHODS ===
        
        /// <summary>
        /// Begin new transaction.
        /// </summary>
        public async Task<IRedbTransaction> BeginTransactionAsync()
        {
            if (_currentTransaction != null && _currentTransaction.IsActive)
                throw new InvalidOperationException("Transaction already active. Commit or rollback first.");
            
            var conn = await GetOpenConnectionAsync();
            var npgsqlTx = await conn.BeginTransactionAsync();
            _currentTransaction = new NpgsqlRedbTransaction(npgsqlTx, () => _currentTransaction = null);
            return _currentTransaction;
        }

        // === ATOMIC OPERATIONS ===
        
        /// <summary>
        /// Execute operations atomically (SaveChanges replacement).
        /// </summary>
        public async Task ExecuteAtomicAsync(Func<Task> operations)
        {
            // If transaction already active - just execute
            if (_currentTransaction != null && _currentTransaction.IsActive)
            {
                await operations();
                return;
            }
            
            // Otherwise create auto-transaction
            await using var tx = await BeginTransactionAsync();
            try
            {
                await operations();
                await tx.CommitAsync();
            }
            catch
            {
                await tx.RollbackAsync();
                throw;
            }
        }
        
        /// <summary>
        /// Execute operations atomically and return result.
        /// </summary>
        public async Task<T> ExecuteAtomicAsync<T>(Func<Task<T>> operations)
        {
            if (_currentTransaction != null && _currentTransaction.IsActive)
            {
                return await operations();
            }
            
            await using var tx = await BeginTransactionAsync();
            try
            {
                var result = await operations();
                await tx.CommitAsync();
                return result;
            }
            catch
            {
                await tx.RollbackAsync();
                throw;
            }
        }

        // === JSON METHODS ===
        
        /// <summary>
        /// Execute SQL returning JSON (for PostgreSQL functions).
        /// </summary>
        public async Task<string?> ExecuteJsonAsync(string sql, params object[] parameters)
        {
            var conn = await GetOpenConnectionAsync();
            await using var cmd = CreateCommand(conn, sql, parameters);
            var result = await cmd.ExecuteScalarAsync();
            
            if (result == null || result == DBNull.Value)
                return null;
            
            return result.ToString();
        }
        
        /// <summary>
        /// Execute SQL returning multiple JSON rows.
        /// </summary>
        public async Task<List<string>> ExecuteJsonListAsync(string sql, params object[] parameters)
        {
            var conn = await GetOpenConnectionAsync();
            await using var cmd = CreateCommand(conn, sql, parameters);
            await using var reader = await cmd.ExecuteReaderAsync();
            
            var results = new List<string>();
            while (await reader.ReadAsync())
            {
                // Skip NULL values (e.g. get_object_json returns NULL for deleted objects)
                if (reader.IsDBNull(0))
                    continue;
                    
                var json = reader.GetString(0);
                if (!string.IsNullOrEmpty(json))
                    results.Add(json);
            }
            
            return results;
        }

        // === DISPOSE ===
        
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;
            
            _disposed = true;
            
            if (_currentTransaction != null)
            {
                await _currentTransaction.DisposeAsync();
                _currentTransaction = null;
            }
            
            if (_connection != null)
            {
                await _connection.DisposeAsync();
                _connection = null;
            }
        }
        
        /// <summary>
        /// Synchronous dispose for DI container compatibility.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;
            
            _disposed = true;
            
            _currentTransaction?.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _currentTransaction = null;
            
            _connection?.Dispose();
            _connection = null;
        }
        
        /// <summary>
        /// Convert @p0, @p1 parameter format to PostgreSQL $1, $2 format.
        /// Enables cross-platform SQL compatibility.
        /// </summary>
        private static string ConvertParameters(string sql, int paramCount)
        {
            var result = sql;
            
            // Replace @p0, @p1 with $1, $2 (in reverse order to avoid index shifting)
            for (int i = paramCount - 1; i >= 0; i--)
            {
                result = result.Replace($"@p{i}", $"${i + 1}");
            }
            
            return result;
        }
    }
    
    /// <summary>
    /// Row mapper for converting NpgsqlDataReader to objects.
    /// Supports multiple column name formats:
    /// - PostgreSQL table columns: _id, _name, _id_scheme
    /// - JSON output: id, name, id_scheme  
    /// - PascalCase: Id, Name, IdScheme
    /// </summary>
    /// <typeparam name="T">Target type.</typeparam>
    internal class RedbRowMapper<T> where T : new()
    {
        private readonly Dictionary<string, PropertyInfo> _propertyMap;
        
        public RedbRowMapper()
        {
            _propertyMap = new Dictionary<string, PropertyInfo>(StringComparer.OrdinalIgnoreCase);
            
            foreach (var prop in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (!prop.CanWrite) continue;
                
                // Check for JsonPropertyName attribute
                var jsonAttr = prop.GetCustomAttribute<JsonPropertyNameAttribute>();
                var jsonName = jsonAttr?.Name ?? ToSnakeCase(prop.Name);
                
                // Map all possible column name formats:
                // 1. JsonPropertyName value (e.g., "id", "id_scheme")
                _propertyMap[jsonName] = prop;
                
                // 2. PostgreSQL table column format (e.g., "_id", "_id_scheme")
                _propertyMap["_" + jsonName] = prop;
                
                // 3. PascalCase property name (e.g., "Id", "IdScheme")
                _propertyMap[prop.Name] = prop;
                
                // 4. Lowercase property name (PostgreSQL returns lowercase aliases!)
                _propertyMap[prop.Name.ToLowerInvariant()] = prop;

                // 5. Underscore + lowercase property name without underscores (e.g., "_datetimeoffset")
                _propertyMap["_" + prop.Name.ToLowerInvariant()] = prop;
            }
        }
        
        /// <summary>
        /// Convert PascalCase to snake_case.
        /// </summary>
        private static string ToSnakeCase(string pascalCase)
        {
            if (string.IsNullOrEmpty(pascalCase)) return pascalCase;
            
            var result = new System.Text.StringBuilder();
            for (int i = 0; i < pascalCase.Length; i++)
            {
                var c = pascalCase[i];
                if (char.IsUpper(c) && i > 0)
                {
                    result.Append('_');
                }
                result.Append(char.ToLowerInvariant(c));
            }
            return result.ToString();
        }
        
        /// <summary>
        /// Map current row to object instance.
        /// </summary>
        public T MapRow(NpgsqlDataReader reader)
        {
            var obj = new T();
            
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var columnName = reader.GetName(i);
                
                if (!_propertyMap.TryGetValue(columnName, out var property))
                    continue;
                
                var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                
                if (value != null)
                {
                    try
                    {
                        var targetType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
                        
                        // Special handling for DateTimeOffset (Npgsql returns DateTime for timestamptz)
                        if (targetType == typeof(DateTimeOffset))
                        {
                            if (value is DateTime dt)
                            {
                                property.SetValue(obj, new DateTimeOffset(dt, TimeSpan.Zero));
                            }
                            else if (value is DateTimeOffset dto)
                            {
                                property.SetValue(obj, dto);
                            }
                            continue;
                        }
                        
                        var convertedValue = Convert.ChangeType(value, targetType);
                        property.SetValue(obj, convertedValue);
                    }
                    catch
                    {
                        // Try direct assignment
                        try
                        {
                            property.SetValue(obj, value);
                        }
                        catch
                        {
                            // Skip if cannot convert
                        }
                    }
                }
            }
            
            return obj;
        }
    }
}

