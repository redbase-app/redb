using Microsoft.Data.SqlClient;
using redb.Core.Data;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Text;
using System.Text.Json.Serialization;

namespace redb.MSSql.Data;

/// <summary>
/// MS SQL Server implementation of IRedbConnection using Microsoft.Data.SqlClient.
/// Provides pure ADO.NET database access with automatic transaction management.
/// </summary>
public class SqlRedbConnection : IRedbConnection
{
    private readonly string _connectionString;
    private SqlConnection? _connection;
    private SqlRedbTransaction? _currentTransaction;
    private bool _disposed;
    
    /// <summary>
    /// Connection string.
    /// </summary>
    public string ConnectionString => _connectionString;
    
    /// <summary>
    /// Current active transaction.
    /// </summary>
    public IRedbTransaction? CurrentTransaction => _currentTransaction;

    /// <summary>
    /// Create connection from connection string.
    /// </summary>
    /// <param name="connectionString">MS SQL Server connection string.</param>
    public SqlRedbConnection(string connectionString)
    {
        if (string.IsNullOrEmpty(connectionString))
            throw new ArgumentNullException(nameof(connectionString));
        
        _connectionString = connectionString;
    }

    // === CONNECTION MANAGEMENT ===
    
    /// <summary>
    /// Get underlying connection (for bulk operations).
    /// This ensures all operations use the same connection and transaction.
    /// </summary>
    public async Task<DbConnection> GetUnderlyingConnectionAsync()
    {
        return await GetOpenConnectionAsync();
    }
    
    private async Task<SqlConnection> GetOpenConnectionAsync()
    {
        if (_connection == null)
        {
            _connection = new SqlConnection(_connectionString);
            await _connection.OpenAsync();
        }
        else if (_connection.State != ConnectionState.Open)
        {
            await _connection.OpenAsync();
        }
        return _connection;
    }
    
    /// <summary>
    /// Create SqlCommand with parameter conversion from PostgreSQL $1,$2 to @p0,@p1 format.
    /// </summary>
    private SqlCommand CreateCommand(SqlConnection connection, string sql, object[] parameters)
    {
        // Convert PostgreSQL positional parameters ($1, $2) to MSSQL named parameters (@p0, @p1)
        var convertedSql = ConvertParameters(sql, parameters.Length);
        
        var cmd = new SqlCommand(convertedSql, connection);
        
        // Set transaction if active
        if (_currentTransaction != null)
        {
            cmd.Transaction = _currentTransaction.SqlTransaction;
        }
        
        // Add named parameters
        for (int i = 0; i < parameters.Length; i++)
        {
            var param = parameters[i];
            var sqlParam = new SqlParameter($"@p{i}", param ?? DBNull.Value);
            
            if (param == null)
            {
                sqlParam.Value = DBNull.Value;
            }
            else if (param is DateTimeOffset dto)
            {
                sqlParam.Value = dto;
                sqlParam.SqlDbType = SqlDbType.DateTimeOffset;
            }
            else if (param is byte[] bytes)
            {
                sqlParam.Value = bytes;
                sqlParam.SqlDbType = SqlDbType.VarBinary;
            }
            else if (param is long[] longArray)
            {
                // MSSQL doesn't support array parameters natively
                // Use comma-separated string for use with STRING_SPLIT
                sqlParam.Value = string.Join(",", longArray);
                sqlParam.SqlDbType = SqlDbType.NVarChar;
            }
            else if (param is int[] intArray)
            {
                sqlParam.Value = string.Join(",", intArray);
                sqlParam.SqlDbType = SqlDbType.NVarChar;
            }
            else if (param is string[] stringArray)
            {
                sqlParam.Value = string.Join(",", stringArray);
                sqlParam.SqlDbType = SqlDbType.NVarChar;
            }
            
            cmd.Parameters.Add(sqlParam);
        }
        
        return cmd;
    }
    
    /// <summary>
    /// Convert PostgreSQL $1, $2 parameters to MSSQL @p0, @p1 format.
    /// </summary>
    private static string ConvertParameters(string sql, int paramCount)
    {
        var result = sql;
        
        // Replace in reverse order to avoid index shifting ($10 before $1)
        for (int i = paramCount; i >= 1; i--)
        {
            result = result.Replace($"${i}", $"@p{i - 1}");
        }
        
        return result;
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
        var mapper = new SqlRowMapper<T>();
        
        while (await reader.ReadAsync())
        {
            results.Add(mapper.MapRow(reader));
        }
        
        return results;
    }
    
    /// <summary>
    /// Execute SQL query and return first result.
    /// MSSQL FOR JSON may split large results into multiple rows (~2033 chars each).
    /// This method concatenates all rows for 'result' column before mapping.
    /// </summary>
    public async Task<T?> QueryFirstOrDefaultAsync<T>(string sql, params object[] parameters) where T : class, new()
    {
        var conn = await GetOpenConnectionAsync();
        await using var cmd = CreateCommand(conn, sql, parameters);
        await using var reader = await cmd.ExecuteReaderAsync();
        
        // Check if this is a single 'result' column (FOR JSON output OR scalar aggregate)
        if (reader.FieldCount == 1)
        {
            var columnName = reader.GetName(0);
            
            // FOR JSON typically returns unnamed column or starts with JSON_
            if (columnName.StartsWith("JSON_") || string.IsNullOrEmpty(columnName))
            {
                // Concatenate all rows for JSON result
                var jsonBuilder = new StringBuilder();
                while (await reader.ReadAsync())
                {
                    if (!reader.IsDBNull(0))
                    {
                        jsonBuilder.Append(reader.GetString(0));
                    }
                }
                
                if (jsonBuilder.Length == 0)
                {
                    return null;
                }
                
                // Create result object with 'result' property
                var resultType = typeof(T);
                var resultProp = resultType.GetProperty("result", BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                
                if (resultProp != null && resultProp.PropertyType == typeof(string))
                {
                    var obj = new T();
                    resultProp.SetValue(obj, jsonBuilder.ToString());
                    return obj;
                }
            }
            // For scalar aggregation results (column named 'result' with numeric type)
            else if (columnName == "result")
            {
                if (await reader.ReadAsync())
                {
                    if (reader.IsDBNull(0))
                        return null;
                    
                    var resultType = typeof(T);
                    var resultProp = resultType.GetProperty("result", BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                    
                    if (resultProp != null)
                    {
                        var obj = new T();
                        var value = reader.GetValue(0);
                        var targetType = Nullable.GetUnderlyingType(resultProp.PropertyType) ?? resultProp.PropertyType;
                        var convertedValue = Convert.ChangeType(value, targetType);
                        resultProp.SetValue(obj, convertedValue);
                        return obj;
                    }
                }
                return null;
            }
        }
        
        // Standard row mapping for non-JSON results
        if (await reader.ReadAsync())
        {
            var mapper = new SqlRowMapper<T>();
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
        try
        {
            return await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            throw;
        }
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
        var sqlTx = (SqlTransaction)await conn.BeginTransactionAsync();
        _currentTransaction = new SqlRedbTransaction(sqlTx, () => _currentTransaction = null);
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
    /// Execute SQL returning JSON (for MSSQL functions returning JSON).
    /// </summary>
    public async Task<string?> ExecuteJsonAsync(string sql, params object[] parameters)
    {
        var conn = await GetOpenConnectionAsync();
        await using var cmd = CreateCommand(conn, sql, parameters);
        await using var reader = await cmd.ExecuteReaderAsync();
        
        // MSSQL FOR JSON may split result across multiple rows
        var sb = new StringBuilder();
        while (await reader.ReadAsync())
        {
            if (!reader.IsDBNull(0))
            {
                sb.Append(reader.GetString(0));
            }
        }
        
        var result = sb.ToString();
        return string.IsNullOrEmpty(result) ? null : result;
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
}

/// <summary>
/// Row mapper for converting SqlDataReader to objects.
/// Supports multiple column name formats:
/// - MSSQL table columns: _id, _name, _id_scheme
/// - JSON output: id, name, id_scheme  
/// - PascalCase: Id, Name, IdScheme
/// </summary>
/// <typeparam name="T">Target type.</typeparam>
internal class SqlRowMapper<T> where T : new()
{
    private readonly Dictionary<string, PropertyInfo> _propertyMap;
    
    public SqlRowMapper()
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
            
            // 2. MSSQL table column format (e.g., "_id", "_id_scheme")
            _propertyMap["_" + jsonName] = prop;
            
            // 3. PascalCase property name (e.g., "Id", "IdScheme")
            _propertyMap[prop.Name] = prop;
            
            // 4. Lowercase property name
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
        
        var result = new StringBuilder();
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
    public T MapRow(SqlDataReader reader)
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
                    
                    // Special handling for DateTimeOffset
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
                    
                    // Special handling for Guid
                    if (targetType == typeof(Guid) && value is Guid guid)
                    {
                        property.SetValue(obj, guid);
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

