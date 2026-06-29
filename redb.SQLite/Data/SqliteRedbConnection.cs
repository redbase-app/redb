using Microsoft.Data.Sqlite;
using redb.Core.Data;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace redb.SQLite.Data
{
    /// <summary>
    /// SQLite implementation of IRedbConnection using Sqlite.
    /// Provides pure ADO.NET database access with automatic transaction management.
    /// </summary>
    public class SqliteRedbConnection : IRedbConnection
    {
        private readonly SqliteDataSource _dataSource;
        private SqliteConnection? _connection;
        private SqliteRedbTransaction? _currentTransaction;
        private bool _disposed = false;

        // ── [Diag-TXLOCK] process-wide BeginTransaction tracker ────────────
        //
        // Every BeginTransactionAsync call registers an entry keyed by this
        // SqliteRedbConnection's HashCode and dumps the current registry
        // contents before attempting BEGIN IMMEDIATE. A SqliteException
        // (BUSY / locked) on the BEGIN call triggers a second dump so the
        // operator can see WHO else is holding a write tx when the failure
        // hit. Active entries are removed on Commit / Rollback / Dispose.
        // Set DiagEnabled=true to enable [Diag-TXLOCK] BEGIN tracking dump
        // (single-writer concurrency triage). Off by default — the instrumentation
        // stays in code so it can be flipped back on without a recompile when the
        // next live writer-conflict scenario needs to be investigated.
        private sealed record BeginEntry(
            int ConnHash, int ThreadId, DateTime BeginUtc,
            string AmbientTxId, string CallerStack);

        private static readonly ConcurrentDictionary<int, BeginEntry> _activeBegins = new();
        public static bool DiagEnabled { get; set; } = false;
        
        /// <summary>
        /// Connection string.
        /// </summary>
        public string ConnectionString => _dataSource.ConnectionString;
        
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
            Transaction.Current != null;

        /// <summary>
        /// Create connection from data source.
        /// </summary>
        /// <param name="dataSource">Sqlite data source (pooled).</param>
        public SqliteRedbConnection(SqliteDataSource dataSource)
        {
            _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        }
        
        /// <summary>
        /// Create connection from connection string.
        /// </summary>
        /// <param name="connectionString">SQLite connection string.</param>
        public SqliteRedbConnection(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException(nameof(connectionString));
            
            _dataSource = SqliteDataSource.Create(connectionString);
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
        
        private async Task<SqliteConnection> GetOpenConnectionAsync()
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
        
        private SqliteCommand CreateCommand(SqliteConnection connection, string sql, object[] parameters)
        {
            // Convert @p0, @p1 format to SQLite $1, $2 format for cross-platform compatibility
            var convertedSql = ConvertParameters(sql, parameters.Length);
            var cmd = new SqliteCommand(convertedSql, connection);
            
            // Set transaction if active. The `_currentTransaction` field is
            // cleared on Dispose, NOT on Commit/Rollback — so after the caller
            // commits but before they dispose, this would otherwise try to
            // bind the command to an already-closed SQLite transaction object
            // and throw "transaction is not associated with the same connection".
            // Gate on IsActive so queries that fire between commit-and-dispose
            // (e.g. visibility probes in transaction-integrity tests) bind to
            // no transaction and run against the autocommit connection.
            if (_currentTransaction != null && _currentTransaction.IsActive)
            {
                cmd.Transaction = _currentTransaction.SqliteTransaction;
            }
            
            // Add parameters NAMED to match the converted $1,$2,... placeholders
            // (Microsoft.Data.Sqlite binds by name, not by position).
            for (int i = 0; i < parameters.Length; i++)
            {
                var param = parameters[i];
                var sqliteParam = new SqliteParameter { ParameterName = "$" + (i + 1) };

                switch (param)
                {
                    case null:
                        sqliteParam.Value = DBNull.Value;
                        break;
                    case DateTimeOffset dto:
                        // Datetimes are stored as REAL Julian day (UTC) — the native SQLite
                        // datetime format → numeric, correct, index-friendly comparisons and
                        // datetime()/strftime()/julianday() work directly. Covers writes
                        // (base _date_* + props _DateTimeOffset) AND parameterized query values.
                        sqliteParam.Value = SqliteJulian.ToJulian(dto);
                        break;
                    case DateTime dt2:
                        // e.g. a LINQ comparison value (DateTime) — same UTC Julian encoding.
                        sqliteParam.Value = SqliteJulian.ToJulian(dt2);
                        break;
                    case decimal dec:
                        // Microsoft.Data.Sqlite binds decimal as TEXT, but _Numeric/_value_numeric
                        // are REAL — and in SQLite type-ordering a REAL is always < any TEXT, so
                        // `Salary(REAL) > '80000'(TEXT)` is always false. Bind decimals as double.
                        sqliteParam.Value = (double)dec;
                        break;
                    case long[]:
                    case int[]:
                    case string[]:
                        // SQLite has no array parameters. Bind arrays as a JSON array
                        // (TEXT); SQL that needs the elements uses
                        // `IN (SELECT value FROM json_each($n))`.
                        sqliteParam.Value = JsonSerializer.Serialize(param);
                        break;
                    default:
                        sqliteParam.Value = param;
                        break;
                }

                cmd.Parameters.Add(sqliteParam);
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

            return ConvertScalar<T>(result);
        }

        /// <summary>
        /// Convert a raw SQLite value to T, handling TEXT-stored uuid->Guid,
        /// TEXT/ DateTime -> DateTimeOffset, and BLOB -> byte[] (which
        /// Convert.ChangeType cannot do), then falling back to ChangeType.
        /// </summary>
        private static T ConvertScalar<T>(object value)
        {
            var targetType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);

            if (value.GetType() == targetType || targetType.IsAssignableFrom(value.GetType()))
                return (T)value;

            if (targetType == typeof(Guid))
                return (T)(object)(value is Guid g ? g : Guid.Parse(value.ToString()!));

            // Datetimes are stored as REAL Julian day (UTC) → value is a double.
            if (targetType == typeof(DateTimeOffset))
                return (T)(object)(value is double jdo ? SqliteJulian.FromJulian(jdo)
                    : value is DateTimeOffset d ? d
                    : value is DateTime dt ? new DateTimeOffset(dt, TimeSpan.Zero)
                    : DateTimeOffset.Parse(value.ToString()!, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind));

            if (targetType == typeof(DateTime))
                return (T)(object)(value is double jdt ? SqliteJulian.FromJulian(jdt).UtcDateTime
                    : value is DateTime dt3 ? dt3
                    : value is DateTimeOffset dto3 ? dto3.UtcDateTime
                    : DateTime.Parse(value.ToString()!, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind));

            if (targetType == typeof(DateOnly))
                return (T)(object)(value is double jod ? DateOnly.FromDateTime(SqliteJulian.FromJulian(jod).UtcDateTime)
                    : value is DateTime dt4 ? DateOnly.FromDateTime(dt4)
                    : DateOnly.Parse(value.ToString()!, System.Globalization.CultureInfo.InvariantCulture));

            if (targetType == typeof(byte[]))
                return (T)(object)(byte[])value;

            return (T)Convert.ChangeType(value, targetType);
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

            while (await reader.ReadAsync())
            {
                if (reader.IsDBNull(0))
                {
                    results.Add(default!);
                }
                else
                {
                    results.Add(ConvertScalar<T>(reader.GetValue(0)));
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

            if (Transaction.Current != null)
                throw new InvalidOperationException(
                    "Ambient TransactionScope detected. Cannot create explicit transaction inside TransactionScope. " +
                    "Use ExecuteAtomicAsync() which respects ambient transactions.");

            var conn = await GetOpenConnectionAsync();

            // [Diag-TXLOCK] — capture WHO is about to BEGIN IMMEDIATE and WHAT
            // else is currently holding write tx; on BUSY we dump again so the
            // operator can see who was racing.
            var connHash = GetHashCode();
            var ambient = Transaction.Current?.TransactionInformation.LocalIdentifier ?? "<none>";
            var caller = DiagEnabled ? CaptureCallerStack(skipFrames: 1) : "<diag-off>";
            if (DiagEnabled)
            {
                Console.WriteLine(
                    $"[Diag-TXLOCK] BEGIN ENTRY conn=#{connHash} thread={Environment.CurrentManagedThreadId} " +
                    $"ambientTx={ambient} activeCount={_activeBegins.Count}");
                DumpActiveBegins("BEGIN ENTRY (pre-BeginImmediate)");
                // Console.WriteLine($"[Diag-TXLOCK] BEGIN CALLER conn=#{connHash}\n{caller}");
            }

            SqliteTransaction sqliteTx;
            try
            {
                // BEGIN IMMEDIATE (deferred:false) acquires SQLite's single write lock at
                // transaction start — the SQLite equivalent of pessimistic locking / the PG
                // `FOR UPDATE` intent. Atomicity here is critical and reused widely.
                sqliteTx = conn.BeginTransaction(deferred: false);
            }
            catch (SqliteException ex)
            {
                if (DiagEnabled)
                {
                    Console.WriteLine(
                        $"[Diag-TXLOCK] BEGIN FAILED conn=#{connHash} thread={Environment.CurrentManagedThreadId} " +
                        $"errno={ex.SqliteErrorCode} msg='{ex.Message}'");
                    DumpActiveBegins("BEGIN FAILED — current holders");
                }
                throw;
            }

            _currentTransaction = new SqliteRedbTransaction(sqliteTx, () =>
            {
                _currentTransaction = null;
                if (DiagEnabled)
                {
                    _activeBegins.TryRemove(connHash, out _);
                    Console.WriteLine(
                        $"[Diag-TXLOCK] BEGIN RELEASED conn=#{connHash} thread={Environment.CurrentManagedThreadId} " +
                        $"activeCount={_activeBegins.Count}");
                }
            });

            if (DiagEnabled)
            {
                _activeBegins[connHash] = new BeginEntry(
                    connHash, Environment.CurrentManagedThreadId, DateTime.UtcNow, ambient, caller);
                Console.WriteLine(
                    $"[Diag-TXLOCK] BEGIN OK    conn=#{connHash} thread={Environment.CurrentManagedThreadId} " +
                    $"activeCount={_activeBegins.Count}");
            }
            return _currentTransaction;
        }

        // ── [Diag-TXLOCK] helpers ──────────────────────────────────────────

        private static string CaptureCallerStack(int skipFrames)
        {
            try
            {
                var st = new StackTrace(skipFrames + 1, fNeedFileInfo: false);
                var frames = st.GetFrames();
                if (frames is null) return "<no frames>";
                var top = frames.Take(8).Select(f =>
                {
                    var m = f.GetMethod();
                    return m is null ? "<null>" : $"  at {m.DeclaringType?.FullName}.{m.Name}";
                });
                return string.Join("\n", top);
            }
            catch { return "<stack capture failed>"; }
        }

        private static void DumpActiveBegins(string label)
        {
            if (_activeBegins.IsEmpty)
            {
                // Console.WriteLine($"[Diag-TXLOCK] {label}: <none active>");
                return;
            }
            // Console.WriteLine($"[Diag-TXLOCK] {label}: count={_activeBegins.Count}");
            foreach (var (key, e) in _activeBegins)
            {
                var heldFor = DateTime.UtcNow - e.BeginUtc;
                Console.WriteLine(
                    $"  conn=#{key} thread={e.ThreadId} heldFor={heldFor.TotalMilliseconds:N0}ms " +
                    $"ambientAtBegin={e.AmbientTxId}");
                foreach (var line in e.CallerStack.Split('\n').Take(5))
                    Console.WriteLine($"    {line}");
            }
        }

        // === ATOMIC OPERATIONS ===
        
        /// <summary>
        /// Execute operations atomically (SaveChanges replacement).
        /// </summary>
        public async Task ExecuteAtomicAsync(Func<Task> operations)
        {
            // EF pattern: if any transaction active (explicit or ambient TransactionScope) — just execute
            if (IsInTransaction)
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
            // EF pattern: if any transaction active (explicit or ambient TransactionScope) — just execute
            if (IsInTransaction)
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
        /// Execute SQL returning JSON (for SQLite functions).
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

        /// <summary>
        /// Execute SQL and serialize the result rows to a JSON array string in C#
        /// (column names from the reader). SQLite has no row_to_json/json_agg, so the
        /// Pro analytics providers package dynamic-shaped results here instead of in SQL.
        /// Returns "[]" when there are no rows.
        /// </summary>
        public async Task<string> QueryRowsAsJsonAsync(string sql, params object[] parameters)
        {
            var conn = await GetOpenConnectionAsync();
            await using var cmd = CreateCommand(conn, sql, parameters);
            await using var reader = await cmd.ExecuteReaderAsync();

            var rows = new List<Dictionary<string, object?>>();
            while (await reader.ReadAsync())
            {
                var row = new Dictionary<string, object?>(reader.FieldCount);
                for (int i = 0; i < reader.FieldCount; i++)
                    row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                rows.Add(row);
            }
            return JsonSerializer.Serialize(rows);
        }

        /// <summary>
        /// Execute SQL and serialize the FIRST result row to a JSON object string in C#,
        /// or null if there are no rows. SQLite analog of "SELECT row_to_json(t) FROM (...) t".
        /// </summary>
        public async Task<string?> QueryFirstRowAsJsonAsync(string sql, params object[] parameters)
        {
            var conn = await GetOpenConnectionAsync();
            await using var cmd = CreateCommand(conn, sql, parameters);
            await using var reader = await cmd.ExecuteReaderAsync();

            if (!await reader.ReadAsync())
                return null;

            var row = new Dictionary<string, object?>(reader.FieldCount);
            for (int i = 0; i < reader.FieldCount; i++)
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            return JsonSerializer.Serialize(row);
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
        /// Convert @p0, @p1 parameter format to SQLite $1, $2 format.
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
    /// Row mapper for converting SqliteDataReader to objects.
    /// Supports multiple column name formats:
    /// - SQLite table columns: _id, _name, _id_scheme
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
                
                // 2. SQLite table column format (e.g., "_id", "_id_scheme")
                _propertyMap["_" + jsonName] = prop;
                
                // 3. PascalCase property name (e.g., "Id", "IdScheme")
                _propertyMap[prop.Name] = prop;
                
                // 4. Lowercase property name (SQLite returns lowercase aliases!)
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
        public T MapRow(SqliteDataReader reader)
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
                        
                        // Datetimes are stored as REAL Julian day (UTC) — value is a double.
                        if (targetType == typeof(DateTimeOffset))
                        {
                            if (value is double jd)
                                property.SetValue(obj, SqliteJulian.FromJulian(jd));
                            else if (value is DateTime dt)
                                property.SetValue(obj, new DateTimeOffset(dt, TimeSpan.Zero));
                            else if (value is DateTimeOffset dto)
                                property.SetValue(obj, dto);
                            else if (value is string sdt)
                                property.SetValue(obj, DateTimeOffset.Parse(sdt, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind));
                            continue;
                        }

                        if (targetType == typeof(DateTime))
                        {
                            if (value is double jd2) property.SetValue(obj, SqliteJulian.FromJulian(jd2).UtcDateTime);
                            else if (value is DateTime dt2) property.SetValue(obj, dt2);
                            else if (value is DateTimeOffset dto2) property.SetValue(obj, dto2.UtcDateTime);
                            else if (value is string sdt2) property.SetValue(obj, DateTime.Parse(sdt2, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind));
                            continue;
                        }

                        if (targetType == typeof(DateOnly))
                        {
                            if (value is double jd3) property.SetValue(obj, DateOnly.FromDateTime(SqliteJulian.FromJulian(jd3).UtcDateTime));
                            else if (value is DateTime dt3) property.SetValue(obj, DateOnly.FromDateTime(dt3));
                            else if (value is string sdt3) property.SetValue(obj, DateOnly.Parse(sdt3, System.Globalization.CultureInfo.InvariantCulture));
                            continue;
                        }

                        // SQLite stores uuid as TEXT — parse back to Guid.
                        if (targetType == typeof(Guid))
                        {
                            property.SetValue(obj, value is Guid g ? g : Guid.Parse(value.ToString()!));
                            continue;
                        }

                        // BLOB -> byte[] (Convert.ChangeType can't handle byte[]).
                        if (targetType == typeof(byte[]))
                        {
                            if (value is byte[] bytes) property.SetValue(obj, bytes);
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

