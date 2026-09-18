using Npgsql;
using redb.Core.Data;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

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
        // The current command's hold on the ambient transaction's connection; released with the command.
        private AmbientLease? _ambientLease;
        // Identity of the database in AmbientConnectionRegistry (computed on first use).
        private string? _databaseKey;
        private bool _disposed = false;
        public bool IsDisposed => _disposed;

        // Commands and teardown share ONE exclusion (CommandGate, tsum garage report
        // 2026-09-11): a single NpgsqlRedbConnection wraps ONE physical connection and is NOT
        // thread-safe. Two concurrent commands fail fast with a clear message; a command after
        // teardown began is refused with ObjectDisposedException (the lazy loader falls back
        // to a detached scope on it); and DisposeAsync WAITS for the in-flight command instead
        // of racing it with Close/Reset.
        private readonly CommandGate _gate = new(nameof(NpgsqlRedbConnection));

        private CommandScope EnterCommand() => new(this, _gate.Enter());

        /// <summary>
        /// One command: this wrapper's gate and, inside an ambient transaction, the lease on the
        /// transaction's connection taken by <see cref="GetOpenConnectionAsync"/> - both released when the
        /// command ends.
        /// </summary>
        private readonly struct CommandScope : IDisposable
        {
            private readonly NpgsqlRedbConnection _owner;
            private readonly CommandGate.Releaser _releaser;

            public CommandScope(NpgsqlRedbConnection owner, CommandGate.Releaser releaser)
            {
                _owner = owner;
                _releaser = releaser;
            }

            public void Dispose()
            {
                var lease = _owner._ambientLease;
                _owner._ambientLease = null;
                try
                {
                    lease?.Dispose();
                }
                finally
                {
                    _releaser.Dispose();
                }
            }
        }

        /// <summary>Host, port, database and login: two connection strings reaching the same data share one key.</summary>
        private string DatabaseKey => _databaseKey ??= DatabaseKeyOf(_dataSource.ConnectionString);

        private string? _sessionSignature;
        // This configuration in AmbientConnectionRegistry: the cache domain of the connection string plus the session
        // settings. A connection the transaction already holds for this database is shared only when they match.
        private string SessionSignature => _sessionSignature ??=
            redb.Core.Models.Configuration.RedbServiceConfiguration.ComputeCacheDomain(_dataSource.ConnectionString)
            + "|" + NpgsqlDataSourceFactory.SessionSettingsText(_dataSource);

        private static string DatabaseKeyOf(string connectionString)
        {
            var builder = new NpgsqlConnectionStringBuilder(connectionString);
            return $"pg|{builder.Host}|{builder.Port}|{builder.Database}|{builder.Username}";
        }
        
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

        // Dispose nulls _connection. Without this check a call after Dispose would see the null,
        // open a NEW physical connection on a wrapper whose second Dispose is a no-op, and that
        // connection would sit in the server's session list until the process died (prod, 2026-09).
        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(NpgsqlRedbConnection),
                    "The scope that owned this connection has ended. Resolve a fresh scoped IRedbService " +
                    "instead of reusing one from a finished scope or exchange.");
        }

        /// <summary>
        /// Get underlying connection (for bulk operations).
        /// This ensures all operations use the same connection and transaction.
        /// </summary>
        public async Task<System.Data.Common.DbConnection> GetUnderlyingConnectionAsync(CancellationToken cancellationToken = default)
        {
            // COPY runs between commands: inside an ambient transaction it takes the transaction's
            // connection without holding the gate a command holds.
            if (_currentTransaction is not { IsActive: true } && Transaction.Current is { } ambient)
            {
                ThrowIfDisposed();
                var entry = await AmbientConnectionRegistry.GetOrOpenAsync(ambient, DatabaseKey, SessionSignature, OpenAmbientAsync, null, cancellationToken);
                return entry.Connection;
            }
            return await GetOpenConnectionAsync(cancellationToken);
        }

        private async Task<NpgsqlConnection> GetOpenConnectionAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();

            // Inside an ambient transaction the TRANSACTION holds the connection (AmbientConnectionRegistry):
            // every scope of this database works through it, and a second open connection would need a
            // prepared (two-phase) transaction. An explicit transaction of this wrapper keeps its own connection.
            if (_currentTransaction is not { IsActive: true } && Transaction.Current is { } ambient)
            {
                _ambientLease ??= await AmbientConnectionRegistry.AcquireAsync(ambient, DatabaseKey, SessionSignature, OpenAmbientAsync, null, cancellationToken);
                return (NpgsqlConnection)_ambientLease.Entry.Connection;
            }

            if (_connection == null)
            {
                _connection = await _dataSource.OpenConnectionAsync(cancellationToken);
                // Session settings (collation, lazy refs) do not survive the pool: DISCARD ALL on
                // return resets them. Re-armed on every hand-out, like MSSQL and SQLite do.
                await NpgsqlDataSourceFactory.ApplySessionSettingsAsync(_dataSource, _connection);
            }
            else if (_connection.State != System.Data.ConnectionState.Open)
            {
                // Closed OR Broken (a cancellation tearing down a COPY breaks the connector).
                // A broken NpgsqlConnection cannot be re-opened in place - replace it with a
                // fresh one so the scope heals instead of failing every call from here on.
                try { await _connection.DisposeAsync(); }
                finally { _connection = null; }
                _connection = await _dataSource.OpenConnectionAsync(cancellationToken);
                await NpgsqlDataSourceFactory.ApplySessionSettingsAsync(_dataSource, _connection);
            }
            return _connection;
        }

        /// <summary>Opens the connection an ambient transaction holds; opened inside the scope, Npgsql enlists it.</summary>
        private async Task<System.Data.Common.DbConnection> OpenAmbientAsync(CancellationToken cancellationToken)
        {
            var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
            try
            {
                await NpgsqlDataSourceFactory.ApplySessionSettingsAsync(_dataSource, connection);
                return connection;
            }
            catch
            {
                await connection.DisposeAsync();
                throw;
            }
        }

        /// <summary>Synchronous <see cref="OpenAmbientAsync"/>.</summary>
        private System.Data.Common.DbConnection OpenAmbient()
        {
            var connection = _dataSource.OpenConnection();
            try
            {
                NpgsqlDataSourceFactory.ApplySessionSettings(_dataSource, connection);
                return connection;
            }
            catch
            {
                connection.Dispose();
                throw;
            }
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
        public Task<List<T>> QueryAsync<T>(string sql, params object[] parameters) where T : new()
            => QueryAsync<T>(sql, parameters, CancellationToken.None);

        /// <inheritdoc />
        public async Task<List<T>> QueryAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken) where T : new()
        {
            using var _guard = EnterCommand();
            var conn = await GetOpenConnectionAsync(cancellationToken);
            try
            {
                return await QueryCoreAsync<T>(conn, sql, parameters, cancellationToken);
            }
            catch (Exception ex)
            {
                await RollbackOrphanTransactionAsync(conn, ex);
                throw;
            }
        }

        private async Task<List<T>> QueryCoreAsync<T>(NpgsqlConnection conn, string sql, object[] parameters, CancellationToken cancellationToken) where T : new()
        {
            await using var cmd = CreateCommand(conn, sql, parameters);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var results = new List<T>();
            var mapper = new RedbRowMapper<T>();

            while (await reader.ReadAsync(cancellationToken))
            {
                results.Add(mapper.MapRow(reader));
            }

            return results;
        }
        
        /// <summary>
        /// Execute SQL query and return first result.
        /// </summary>
        public Task<T?> QueryFirstOrDefaultAsync<T>(string sql, params object[] parameters) where T : class, new()
            => QueryFirstOrDefaultAsync<T>(sql, parameters, CancellationToken.None);

        /// <inheritdoc />
        public async Task<T?> QueryFirstOrDefaultAsync<T>(string sql, object[] parameters, CancellationToken cancellationToken) where T : class, new()
        {
            using var _guard = EnterCommand();
            var conn = await GetOpenConnectionAsync(cancellationToken);
            try
            {
                return await QueryFirstOrDefaultCoreAsync<T>(conn, sql, parameters, cancellationToken);
            }
            catch (Exception ex)
            {
                await RollbackOrphanTransactionAsync(conn, ex);
                throw;
            }
        }

        private async Task<T?> QueryFirstOrDefaultCoreAsync<T>(NpgsqlConnection conn, string sql, object[] parameters, CancellationToken cancellationToken) where T : class, new()
        {
            await using var cmd = CreateCommand(conn, sql, parameters);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            if (await reader.ReadAsync(cancellationToken))
            {
                var mapper = new RedbRowMapper<T>();
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
            try
            {
                return await ExecuteScalarCoreAsync<T>(conn, sql, parameters, cancellationToken);
            }
            catch (Exception ex)
            {
                await RollbackOrphanTransactionAsync(conn, ex);
                throw;
            }
        }

        private async Task<T?> ExecuteScalarCoreAsync<T>(NpgsqlConnection conn, string sql, object[] parameters, CancellationToken cancellationToken)
        {
            await using var cmd = CreateCommand(conn, sql, parameters);
            var result = await cmd.ExecuteScalarAsync(cancellationToken);
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

        // === ORPHAN TRANSACTION HYGIENE ===

        private const string OrphanRollbackFailureKey = "redb.OrphanTransactionRollbackFailure";

        /// <summary>
        /// A command that failed can leave the session inside a transaction its own SQL text opened
        /// (a BEGIN in the text, then an error): an aborted transaction block this wrapper does not
        /// own, in which the scope's every later command and transaction is refused. It is rolled
        /// back here before the failure leaves, unless a transaction of this wrapper or an ambient
        /// one owns the session (its owner ends it). ROLLBACK outside a transaction is only a warning
        /// in PostgreSQL. The original exception always propagates; a rollback that fails as well is
        /// attached to it instead of replacing it.
        /// </summary>
        private async Task RollbackOrphanTransactionAsync(NpgsqlConnection conn, Exception failure)
        {
            if (_currentTransaction is { IsActive: true } || Transaction.Current != null
                || conn.State != System.Data.ConnectionState.Open)
                return;
            try
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "ROLLBACK";
                // The failure may be the caller's own cancellation - the rollback must land regardless.
                await cmd.ExecuteNonQueryAsync(CancellationToken.None);
            }
            catch (Exception rollbackFailure)
            {
                failure.Data[OrphanRollbackFailureKey] = rollbackFailure.Message;
            }
        }

        /// <summary>Synchronous twin of <see cref="RollbackOrphanTransactionAsync"/>.</summary>
        private void RollbackOrphanTransaction(NpgsqlConnection conn, Exception failure)
        {
            if (_currentTransaction is { IsActive: true } || Transaction.Current != null
                || conn.State != System.Data.ConnectionState.Open)
                return;
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "ROLLBACK";
                cmd.ExecuteNonQuery();
            }
            catch (Exception rollbackFailure)
            {
                failure.Data[OrphanRollbackFailureKey] = rollbackFailure.Message;
            }
        }

        // === SYNCHRONOUS COUNTERPARTS (thread-pool-free lazy path) ===
        // The sync getter of RedbListItem.Object runs the whole load on the calling thread; these
        // are true sync ADO calls - no thread-pool continuation anywhere, so a saturated pool
        // cannot slow or deadlock them. Same command shape, same session settings as the async
        // twins above.

        private NpgsqlConnection GetOpenConnection()
        {
            ThrowIfDisposed();

            // Same ambient-transaction rule as the async twin.
            if (_currentTransaction is not { IsActive: true } && Transaction.Current is { } ambient)
            {
                _ambientLease ??= AmbientConnectionRegistry.Acquire(ambient, DatabaseKey, SessionSignature, OpenAmbient, null);
                return (NpgsqlConnection)_ambientLease.Entry.Connection;
            }

            if (_connection == null)
            {
                _connection = _dataSource.OpenConnection();
                NpgsqlDataSourceFactory.ApplySessionSettings(_dataSource, _connection);
            }
            else if (_connection.State != System.Data.ConnectionState.Open)
            {
                // Same healing as the async twin: a broken connector is replaced, not re-opened.
                try { _connection.Dispose(); }
                finally { _connection = null; }
                _connection = _dataSource.OpenConnection();
                NpgsqlDataSourceFactory.ApplySessionSettings(_dataSource, _connection);
            }
            return _connection;
        }

        /// <inheritdoc />
        public T? QueryFirstOrDefault<T>(string sql, params object[] parameters) where T : class, new()
        {
            using var _guard = EnterCommand();
            var conn = GetOpenConnection();
            try
            {
                return QueryFirstOrDefaultCore<T>(conn, sql, parameters);
            }
            catch (Exception ex)
            {
                RollbackOrphanTransaction(conn, ex);
                throw;
            }
        }

        private T? QueryFirstOrDefaultCore<T>(NpgsqlConnection conn, string sql, object[] parameters) where T : class, new()
        {
            using var cmd = CreateCommand(conn, sql, parameters);
            using var reader = cmd.ExecuteReader();
            return reader.Read() ? new RedbRowMapper<T>().MapRow(reader) : null;
        }

        /// <inheritdoc />
        public T? ExecuteScalar<T>(string sql, params object[] parameters)
        {
            using var _guard = EnterCommand();
            var conn = GetOpenConnection();
            try
            {
                return ExecuteScalarCore<T>(conn, sql, parameters);
            }
            catch (Exception ex)
            {
                RollbackOrphanTransaction(conn, ex);
                throw;
            }
        }

        private T? ExecuteScalarCore<T>(NpgsqlConnection conn, string sql, object[] parameters)
        {
            using var cmd = CreateCommand(conn, sql, parameters);
            return CoerceScalar<T>(cmd.ExecuteScalar());
        }

        /// <inheritdoc />
        public string? ExecuteJson(string sql, params object[] parameters)
        {
            using var _guard = EnterCommand();
            var conn = GetOpenConnection();
            try
            {
                return ExecuteJsonCore(conn, sql, parameters);
            }
            catch (Exception ex)
            {
                RollbackOrphanTransaction(conn, ex);
                throw;
            }
        }

        private string? ExecuteJsonCore(NpgsqlConnection conn, string sql, object[] parameters)
        {
            using var cmd = CreateCommand(conn, sql, parameters);
            var result = cmd.ExecuteScalar();
            return result == null || result == DBNull.Value ? null : result.ToString();
        }

        /// <inheritdoc />
        public List<T> Query<T>(string sql, params object[] parameters) where T : new()
        {
            using var _guard = EnterCommand();
            var conn = GetOpenConnection();
            try
            {
                return QueryCore<T>(conn, sql, parameters);
            }
            catch (Exception ex)
            {
                RollbackOrphanTransaction(conn, ex);
                throw;
            }
        }

        private List<T> QueryCore<T>(NpgsqlConnection conn, string sql, object[] parameters) where T : new()
        {
            using var cmd = CreateCommand(conn, sql, parameters);
            using var reader = cmd.ExecuteReader();

            var results = new List<T>();
            var mapper = new RedbRowMapper<T>();
            while (reader.Read())
                results.Add(mapper.MapRow(reader));
            return results;
        }

        /// <inheritdoc />
        public int Execute(string sql, params object[] parameters)
        {
            using var _guard = EnterCommand();
            var conn = GetOpenConnection();
            try
            {
                using var cmd = CreateCommand(conn, sql, parameters);
                return cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                RollbackOrphanTransaction(conn, ex);
                throw;
            }
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
            try
            {
                return await ExecuteCoreAsync(conn, sql, parameters, cancellationToken);
            }
            catch (Exception ex)
            {
                await RollbackOrphanTransactionAsync(conn, ex);
                throw;
            }
        }

        private async Task<int> ExecuteCoreAsync(NpgsqlConnection conn, string sql, object[] parameters, CancellationToken cancellationToken)
        {
            await using var cmd = CreateCommand(conn, sql, parameters);
            return await cmd.ExecuteNonQueryAsync(cancellationToken);
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
            try
            {
                return await QueryScalarListCoreAsync<T>(conn, sql, parameters, cancellationToken);
            }
            catch (Exception ex)
            {
                await RollbackOrphanTransactionAsync(conn, ex);
                throw;
            }
        }

        private async Task<List<T>> QueryScalarListCoreAsync<T>(NpgsqlConnection conn, string sql, object[] parameters, CancellationToken cancellationToken)
        {
            await using var cmd = CreateCommand(conn, sql, parameters);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

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
            using var _guard = EnterCommand();
            if (_currentTransaction != null && _currentTransaction.IsActive)
                throw new InvalidOperationException("Transaction already active. Commit or rollback first.");
            
            if (Transaction.Current != null)
                throw new InvalidOperationException(
                    "Ambient TransactionScope detected. Cannot create explicit transaction inside TransactionScope. " +
                    "Use ExecuteAtomicAsync() which respects ambient transactions.");
            
            var conn = await GetOpenConnectionAsync(cancellationToken);
            var npgsqlTx = isolationLevel.HasValue
                ? await conn.BeginTransactionAsync(isolationLevel.Value, cancellationToken)   // BR-1: the requested level
                : await conn.BeginTransactionAsync(cancellationToken);                        // the provider default, as always
            _currentTransaction = new NpgsqlRedbTransaction(npgsqlTx, () => _currentTransaction = null);
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
        /// Execute SQL returning JSON (for PostgreSQL functions).
        /// </summary>
        public Task<string?> ExecuteJsonAsync(string sql, params object[] parameters)
            => ExecuteJsonAsync(sql, parameters, CancellationToken.None);

        /// <inheritdoc />
        public async Task<string?> ExecuteJsonAsync(string sql, object[] parameters, CancellationToken cancellationToken)
        {
            using var _guard = EnterCommand();
            var conn = await GetOpenConnectionAsync(cancellationToken);
            try
            {
                return await ExecuteJsonCoreAsync(conn, sql, parameters, cancellationToken);
            }
            catch (Exception ex)
            {
                await RollbackOrphanTransactionAsync(conn, ex);
                throw;
            }
        }

        private async Task<string?> ExecuteJsonCoreAsync(NpgsqlConnection conn, string sql, object[] parameters, CancellationToken cancellationToken)
        {
            await using var cmd = CreateCommand(conn, sql, parameters);
            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            
            if (result == null || result == DBNull.Value)
                return null;
            
            return result.ToString();
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
            try
            {
                return await ExecuteJsonListCoreAsync(conn, sql, parameters, cancellationToken);
            }
            catch (Exception ex)
            {
                await RollbackOrphanTransactionAsync(conn, ex);
                throw;
            }
        }

        private async Task<List<string>> ExecuteJsonListCoreAsync(NpgsqlConnection conn, string sql, object[] parameters, CancellationToken cancellationToken)
        {
            await using var cmd = CreateCommand(conn, sql, parameters);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

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
            // entrants bounce with ObjectDisposedException the moment the gate flag is set),
            // so Close/Reset never runs under a running command. The budget follows the
            // connection's own Command Timeout - a command may legally run that long. The LIVE
            // object is the source of truth (it can be changed programmatically); the string
            // is the fallback for a connection that was never opened.
            int commandTimeout;
            try
            {
                commandTimeout = _connection?.CommandTimeout
                    ?? new NpgsqlConnectionStringBuilder(_dataSource.ConnectionString).CommandTimeout;
            }
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

