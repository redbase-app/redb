using Microsoft.Data.SqlClient;
using redb.Core.Data;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading;
using System.Transactions;

namespace redb.MSSql.Data;

/// <summary>
/// MS SQL Server implementation of IRedbConnection using Microsoft.Data.SqlClient.
/// Provides pure ADO.NET database access with automatic transaction management.
/// </summary>
public class SqlRedbConnection : IRedbConnection
{
    private readonly string _connectionString;
    // V4 (LAZY Л2): session flag for the JSON builders, applied on every open of this context's connection.
    private readonly bool _lazyReferences;
    private SqlConnection? _connection;
    private SqlRedbTransaction? _currentTransaction;
    private bool _disposed;
    public bool IsDisposed => _disposed;

    // Commands and teardown share ONE exclusion (CommandGate, tsum garage report 2026-09-11):
    // this connection holds ONE persistent SqlConnection reused for all commands and is NOT
    // thread-safe. Two concurrent commands fail fast; a command after teardown began is
    // refused with ObjectDisposedException (the lazy loader falls back to a detached scope);
    // DisposeAsync WAITS for the in-flight command instead of closing under it.
    private readonly CommandGate _gate = new(nameof(SqlRedbConnection));
    private CommandGate.Releaser EnterCommand() => _gate.Enter();

    /// <summary>
    /// Connection string.
    /// </summary>
    public string ConnectionString => _connectionString;
    
    /// <summary>
    /// Current active transaction.
    /// </summary>
    public IRedbTransaction? CurrentTransaction => _currentTransaction;
    
    /// <summary>
    /// Whether any transaction is active — explicit or ambient TransactionScope.
    /// EF Core pattern: checks both CurrentTransaction and Transaction.Current.
    /// </summary>
    public bool IsInTransaction =>
        (_currentTransaction != null && _currentTransaction.IsActive) ||
        System.Transactions.Transaction.Current != null;

    /// <summary>
    /// Create connection from connection string.
    /// </summary>
    /// <param name="connectionString">MS SQL Server connection string.</param>
    public SqlRedbConnection(string connectionString, bool lazyReferences = false)
    {
        if (string.IsNullOrEmpty(connectionString))
            throw new ArgumentNullException(nameof(connectionString));
        
        _connectionString = connectionString;
        _lazyReferences = lazyReferences;
    }

    // === CONNECTION MANAGEMENT ===
    
    /// <summary>
    /// Get underlying connection (for bulk operations).
    /// This ensures all operations use the same connection and transaction.
    /// </summary>
    public async Task<DbConnection> GetUnderlyingConnectionAsync(CancellationToken cancellationToken = default)
    {
        return await GetOpenConnectionAsync(cancellationToken);
    }
    
    private async Task<SqlConnection> GetOpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        // Dispose nulls _connection; without this check a call after Dispose would open a NEW
        // connection on a wrapper whose second Dispose is a no-op, and it would never be closed.
        if (_disposed)
            throw new ObjectDisposedException(nameof(SqlRedbConnection),
                "The scope that owned this connection has ended. Resolve a fresh scoped IRedbService " +
                "instead of reusing one from a finished scope or exchange.");
        var wasJustOpened = false;
        if (_connection == null)
        {
            _connection = new SqlConnection(_connectionString);
            await _connection.OpenAsync();
            wasJustOpened = true;
        }
        else if (_connection.State != ConnectionState.Open)
        {
            await _connection.OpenAsync();
            wasJustOpened = true;
        }
        if (wasJustOpened)
            await EnsureCleanTransactionStateAsync(_connection);

        if (wasJustOpened && _lazyReferences)
        {
            // V4 (LAZY L2): the session flag dbo.build_field_json reads via SESSION_CONTEXT.
            using var lazyCmd = _connection.CreateCommand();
            lazyCmd.CommandText = "EXEC sp_set_session_context @key = N'redb.lazy_refs', @value = 1";
            await lazyCmd.ExecuteNonQueryAsync();
        }
        return _connection;
    }

    /// <summary>
    /// Defensive pool-poisoning guard, mirror of <c>SqliteDataSource.EnsureCleanTransactionState</c>.
    /// Microsoft.Data.SqlClient's connection pool returns <see cref="SqlConnection"/> wrappers
    /// without inspecting the underlying internal connection's transaction state — a prior
    /// caller that failed to COMMIT or ROLLBACK (e.g. swallowed rollback in a dispose path)
    /// hands us a connection with <c>_currentTransaction</c> still set. The next
    /// <see cref="SqlConnection.BeginTransaction()"/> then fails with
    /// <c>InvalidOperationException: SqlConnection does not support parallel transactions</c>.
    /// A speculative <c>ROLLBACK</c> clears any leaked tx; "no transaction is active" is the
    /// normal/clean case and is silently caught (SQL Server error 3903 / 0x0F4F).
    /// </summary>
    private static async Task EnsureCleanTransactionStateAsync(SqlConnection conn)
    {
        try
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "ROLLBACK";
            // Rollback of a leaked pooled tx must always complete - never under a caller token (§3.4).
            await cmd.ExecuteNonQueryAsync(CancellationToken.None);
            // If we reach here, the pooled SqlConnection HAD a leaked tx — log so the
            // source of the leak is observable.
            // Console.WriteLine("[Diag-TX-LIFECYCLE-MSSQL] POOL-CLEANUP: rolled back leaked tx on pooled SqlConnection acquire.");
        }
        catch (Microsoft.Data.SqlClient.SqlException ex) when (ex.Number == 3903)
        {
            // SQL Server error 3903: "The ROLLBACK TRANSACTION request has no corresponding
            // BEGIN TRANSACTION." — clean handle, expected case.
        }
        catch
        {
            // Any other failure is non-fatal here — we don't want to break connection
            // acquisition for a defensive measure. The next BeginTransaction will surface
            // the real error if the state is still bad.
        }
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
    // ===== cancellation normalization (s3.1) =====

    /// <summary>
    /// SqlClient does not reliably surface an attention-based cancel as
    /// OperationCanceledException: a token tearing down WAITFOR or a long command often
    /// comes back as SqlException ("A severe error occurred..." / "Operation cancelled by
    /// user"). The contract says cancellation has exactly one shape - normalize here.
    /// </summary>
    private static async Task<T> NormalizeCancelAsync<T>(Func<Task<T>> run, CancellationToken cancellationToken)
    {
        try { return await run(); }
        catch (Microsoft.Data.SqlClient.SqlException ex) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException("The command was canceled.", ex, cancellationToken);
        }
    }

    public Task<List<T>> QueryAsync<T>(string sql, params object[] parameters) where T : new()
        => QueryAsync<T>(sql, parameters, CancellationToken.None);

    /// <inheritdoc />
    public async Task<List<T>> QueryAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken) where T : new()
    {
        using var _guard = EnterCommand();
        var conn = await GetOpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, sql, parameters);
        await using var reader = await NormalizeCancelAsync(() => cmd.ExecuteReaderAsync(cancellationToken), cancellationToken);
        
        var results = new List<T>();
        var mapper = new SqlRowMapper<T>();
        
        while (await reader.ReadAsync(cancellationToken))
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
    public Task<T?> QueryFirstOrDefaultAsync<T>(string sql, params object[] parameters) where T : class, new()
        => QueryFirstOrDefaultAsync<T>(sql, parameters, CancellationToken.None);

    /// <inheritdoc />
    public async Task<T?> QueryFirstOrDefaultAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken) where T : class, new()
    {
        using var _guard = EnterCommand();
        var conn = await GetOpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, sql, parameters);
        await using var reader = await NormalizeCancelAsync(() => cmd.ExecuteReaderAsync(cancellationToken), cancellationToken);
        
        // Check if this is a single 'result' column (FOR JSON output OR scalar aggregate)
        if (reader.FieldCount == 1)
        {
            var columnName = reader.GetName(0);
            
            // FOR JSON typically returns unnamed column or starts with JSON_
            if (columnName.StartsWith("JSON_") || string.IsNullOrEmpty(columnName))
            {
                // Concatenate all rows for JSON result
                var jsonBuilder = new StringBuilder();
                while (await reader.ReadAsync(cancellationToken))
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
                if (await reader.ReadAsync(cancellationToken))
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
        if (await reader.ReadAsync(cancellationToken))
        {
            var mapper = new SqlRowMapper<T>();
            return mapper.MapRow(reader);
        }
        
        return null;
    }
    
    /// <summary>
    /// Execute SQL query and return scalar value.
    /// </summary>
    public Task<T?> ExecuteScalarAsync<T>(string sql, params object[] parameters)
        => ExecuteScalarAsync<T>(sql, parameters, CancellationToken.None);

    /// <inheritdoc />
    public async Task<T?> ExecuteScalarAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken)
    {
        using var _guard = EnterCommand();
        var conn = await GetOpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, sql, parameters);
        var result = await NormalizeCancelAsync(() => cmd.ExecuteScalarAsync(cancellationToken), cancellationToken);
        return CoerceScalar<T>(result);
    }

    private static T? CoerceScalar<T>(object? result)
    {
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

    // === SYNCHRONOUS COUNTERPARTS (thread-pool-free lazy path) ===
    // The sync getter of RedbListItem.Object runs the whole load on the calling thread; these are
    // true sync ADO calls - no thread-pool continuation anywhere, so a saturated pool cannot slow
    // or deadlock them. Same command shape, session flag and pool-poisoning guard as the async
    // twins above.

    private SqlConnection GetOpenConnection()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(SqlRedbConnection),
                "The scope that owned this connection has ended. Resolve a fresh scoped IRedbService " +
                "instead of reusing one from a finished scope or exchange.");
        var wasJustOpened = false;
        if (_connection == null)
        {
            _connection = new SqlConnection(_connectionString);
            _connection.Open();
            wasJustOpened = true;
        }
        else if (_connection.State != ConnectionState.Open)
        {
            _connection.Open();
            wasJustOpened = true;
        }
        if (wasJustOpened)
        {
            EnsureCleanTransactionState(_connection);
            if (_lazyReferences)
            {
                using var lazyCmd = _connection.CreateCommand();
                lazyCmd.CommandText = "EXEC sp_set_session_context @key = N'redb.lazy_refs', @value = 1";
                lazyCmd.ExecuteNonQuery();
            }
        }
        return _connection;
    }

    private static void EnsureCleanTransactionState(SqlConnection conn)
    {
        try
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "ROLLBACK";
            cmd.ExecuteNonQuery();
        }
        catch (Microsoft.Data.SqlClient.SqlException ex) when (ex.Number == 3903)
        {
            // SQL Server error 3903: no corresponding BEGIN TRANSACTION - clean handle, expected.
        }
    }

    /// <inheritdoc />
    public T? QueryFirstOrDefault<T>(string sql, params object[] parameters) where T : class, new()
    {
        using var _guard = EnterCommand();
        var conn = GetOpenConnection();
        using var cmd = CreateCommand(conn, sql, parameters);
        using var reader = cmd.ExecuteReader();
        return reader.Read() ? new SqlRowMapper<T>().MapRow(reader) : null;
    }

    /// <inheritdoc />
    public T? ExecuteScalar<T>(string sql, params object[] parameters)
    {
        using var _guard = EnterCommand();
        var conn = GetOpenConnection();
        using var cmd = CreateCommand(conn, sql, parameters);
        return CoerceScalar<T>(cmd.ExecuteScalar());
    }

    /// <inheritdoc />
    public string? ExecuteJson(string sql, params object[] parameters)
    {
        using var _guard = EnterCommand();
        var conn = GetOpenConnection();
        using var cmd = CreateCommand(conn, sql, parameters);
        var result = cmd.ExecuteScalar();
        return result == null || result == DBNull.Value ? null : result.ToString();
    }
    
    /// <summary>
    /// Execute SQL command (INSERT, UPDATE, DELETE).
    /// </summary>
    public Task<int> ExecuteAsync(string sql, params object[] parameters)
        => ExecuteAsync(sql, parameters, CancellationToken.None);

    /// <inheritdoc />
    public async Task<int> ExecuteAsync(string sql, object[] parameters, CancellationToken cancellationToken)
    {
        using var _guard = EnterCommand();
        var conn = await GetOpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, sql, parameters);
        return await NormalizeCancelAsync(() => cmd.ExecuteNonQueryAsync(cancellationToken), cancellationToken);
    }
    
    /// <summary>
    /// Execute SQL query and return list of scalar values (first column only).
    /// Use for simple queries like SELECT _id FROM ... that return single column.
    /// </summary>
    public Task<List<T>> QueryScalarListAsync<T>(string sql, params object[] parameters)
        => QueryScalarListAsync<T>(sql, parameters, CancellationToken.None);

    /// <inheritdoc />
    public async Task<List<T>> QueryScalarListAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken)
    {
        using var _guard = EnterCommand();
        var conn = await GetOpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, sql, parameters);
        await using var reader = await NormalizeCancelAsync(() => cmd.ExecuteReaderAsync(cancellationToken), cancellationToken);
        
        var results = new List<T>();
        var targetType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
        
        while (await reader.ReadAsync(cancellationToken))
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
    public async Task<IRedbTransaction> BeginTransactionAsync(System.Data.IsolationLevel? isolationLevel = null, CancellationToken cancellationToken = default)
    {
        if (_currentTransaction != null && _currentTransaction.IsActive)
            throw new InvalidOperationException("Transaction already active. Commit or rollback first.");
        
        if (System.Transactions.Transaction.Current != null)
            throw new InvalidOperationException(
                "Ambient TransactionScope detected. Cannot create explicit transaction inside TransactionScope. " +
                "Use ExecuteAtomicAsync() which respects ambient transactions.");
        
        using var _guard = EnterCommand();
        var conn = await GetOpenConnectionAsync(cancellationToken);
        var sqlTx = (SqlTransaction)(isolationLevel.HasValue
            ? await conn.BeginTransactionAsync(isolationLevel.Value)   // BR-1: the requested level
            : await conn.BeginTransactionAsync());                     // the provider default, as always
        _currentTransaction = new SqlRedbTransaction(sqlTx, () => _currentTransaction = null);
        return _currentTransaction;
    }

    // === ATOMIC OPERATIONS ===
    
    /// <summary>
    /// Atomic execution under an explicit isolation level (BR-1). An active or ambient transaction
    /// wins: operations join it and its level is NOT changed.
    /// </summary>
    public async Task ExecuteAtomicAsync(System.Data.IsolationLevel isolationLevel, Func<Task> operations, CancellationToken cancellationToken = default)
    {

    
        if (IsInTransaction)

    
        {

    
            await operations();

    
            return;

    
        }


    
        await using var tx = await BeginTransactionAsync(isolationLevel, cancellationToken);

    
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


    
    /// <summary>Result-returning form of the isolation-level overload (BR-1).</summary>
    public async Task<T> ExecuteAtomicAsync<T>(System.Data.IsolationLevel isolationLevel, Func<Task<T>> operations, CancellationToken cancellationToken = default)

    
    {

    
        if (IsInTransaction)

    
            return await operations();


    
        await using var tx = await BeginTransactionAsync(isolationLevel, cancellationToken);

    
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


    
    /// <summary>
    /// Execute operations atomically (SaveChanges replacement).
    /// </summary>
    public async Task ExecuteAtomicAsync(Func<Task> operations, CancellationToken cancellationToken = default)
    {
        // EF pattern: if any transaction active (explicit or ambient TransactionScope) — just execute
        if (IsInTransaction)
        {
            await operations();
            return;
        }
        
        // Otherwise create auto-transaction
        await using var tx = await BeginTransactionAsync(cancellationToken: cancellationToken);
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
    public async Task<T> ExecuteAtomicAsync<T>(Func<Task<T>> operations, CancellationToken cancellationToken = default)
    {
        // EF pattern: if any transaction active (explicit or ambient TransactionScope) — just execute
        if (IsInTransaction)
        {
            return await operations();
        }
        
        await using var tx = await BeginTransactionAsync(cancellationToken: cancellationToken);
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
    public Task<string?> ExecuteJsonAsync(string sql, params object[] parameters)
        => ExecuteJsonAsync(sql, parameters, CancellationToken.None);

    /// <inheritdoc />
    public async Task<string?> ExecuteJsonAsync(string sql, object[] parameters, CancellationToken cancellationToken)
    {
        using var _guard = EnterCommand();
        var conn = await GetOpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, sql, parameters);
        await using var reader = await NormalizeCancelAsync(() => cmd.ExecuteReaderAsync(cancellationToken), cancellationToken);
        
        // MSSQL FOR JSON may split result across multiple rows
        var sb = new StringBuilder();
        while (await reader.ReadAsync(cancellationToken))
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
    public Task<List<string>> ExecuteJsonListAsync(string sql, params object[] parameters)
        => ExecuteJsonListAsync(sql, parameters, CancellationToken.None);

    /// <inheritdoc />
    public async Task<List<string>> ExecuteJsonListAsync(string sql, object[] parameters, CancellationToken cancellationToken)
    {
        using var _guard = EnterCommand();
        var conn = await GetOpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, sql, parameters);
        await using var reader = await NormalizeCancelAsync(() => cmd.ExecuteReaderAsync(cancellationToken), cancellationToken);
        
        var results = new List<string>();
        while (await reader.ReadAsync(cancellationToken))
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

        // Teardown takes the SAME exclusion as commands: wait for the in-flight one (new
        // entrants bounce with ObjectDisposedException the moment the gate flag is set). The
        // budget follows the connection's own Command Timeout. Unlike Npgsql/Sqlite,
        // SqlConnection exposes no command-timeout property on the live object - the
        // connection-string keyword (applied by the driver to every command) is the source.
        int commandTimeout;
        try { commandTimeout = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(_connectionString).CommandTimeout; }
        catch { commandTimeout = 0; }
        await _gate.DisposeAndWaitAsync(CommandGate.BudgetFrom(commandTimeout));
        
        // The physical connection MUST return to the pool even if disposing a broken transaction
        // throws (e.g. after a mid-query failure): finally guarantees the return, and the exception
        // is NOT swallowed — it propagates so the fault stays observable. The route's ReleaseScopes
        // loop is throw-resilient, so a propagated dispose fault won't strand sibling scopes.
        try
        {
            if (_currentTransaction != null)
                await _currentTransaction.DisposeAsync();
        }
        finally
        {
            _currentTransaction = null;
            var conn = _connection;
            _connection = null;
            if (conn != null)
                await conn.DisposeAsync();
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

        try
        {
            _currentTransaction?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
        finally
        {
            _currentTransaction = null;
            var conn = _connection;
            _connection = null;
            conn?.Dispose();
        }
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

