using System.Data.Common;
using Npgsql;

namespace redb.Export.Providers;

/// <summary>
/// <see cref="IDataProvider"/> implementation for PostgreSQL.
/// <para>
/// Uses <c>COPY FROM STDIN</c> (TEXT format) for bulk inserts and
/// <c>session_replication_role = 'replica'</c> to bypass foreign-key triggers
/// during import.
/// </para>
/// </summary>
public sealed class PostgresProvider : IDataProvider, IParallelImportProvider
{
    private NpgsqlConnection? _connection;
    private string? _connectionString;
    private NpgsqlTransaction? _transaction;

    /// <inheritdoc />
    public async Task BeginImportAsync(CancellationToken ct = default)
    {
        if (_connection is null) throw new InvalidOperationException("Connection not opened. Call OpenAsync first.");
        if (_transaction is not null) throw new InvalidOperationException("An import transaction is already open.");
        // DDL, TRUNCATE and COPY are all transactional on PostgreSQL, and the constraint mode is SET LOCAL:
        // a rollback leaves nothing behind, not even the relaxed constraints.
        _transaction = await _connection.BeginTransactionAsync(ct);
    }

    /// <inheritdoc />
    public async Task CommitImportAsync(CancellationToken ct = default)
    {
        if (_transaction is null) throw new InvalidOperationException("No import transaction is open.");
        await _transaction.CommitAsync(ct);
        await _transaction.DisposeAsync();
        _transaction = null;
    }

    /// <inheritdoc />
    public async Task AbortImportAsync()
    {
        if (_transaction is null) return;
        await _transaction.RollbackAsync();
        await _transaction.DisposeAsync();
        _transaction = null;
    }

    /// <inheritdoc />
    public string Name => "postgres";

    /// <inheritdoc />
    public DbConnection Connection => _connection
        ?? throw new InvalidOperationException("Connection not opened. Call OpenAsync first.");

    /// <inheritdoc />
    public async Task OpenAsync(string connectionString, CancellationToken ct = default)
    {
        _connectionString = connectionString;
        _connection = new NpgsqlConnection(connectionString);
        await _connection.OpenAsync(ct);
    }

    /// <inheritdoc />
    public async Task CleanDatabaseAsync(CancellationToken ct = default)
    {
        if (_connection is null) return;

        // Order matters: dependents before parents.
        var tables = new[]
        {
            "_values",
            "_list_items",
            "_objects",
            "_permissions",
            "_functions",
            "_dependencies",
            "_structures",
            "_schemes",
            "_users_roles",
            "_users",
            "_roles",
            "_lists",
            "_links",
            "_types"
        };

        foreach (var table in tables)
        {
            await using var cmd = new NpgsqlCommand($"TRUNCATE TABLE {table} CASCADE", _connection, _transaction);
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    /// <inheritdoc />
    public async Task<long> GetSequenceValueAsync(CancellationToken ct = default)
    {
        if (_connection is null) return 0;

        await using var cmd = new NpgsqlCommand("SELECT last_value FROM global_identity", _connection, _transaction);
        var result = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt64(result);
    }

    /// <inheritdoc />
    public async Task SetSequenceValueAsync(long value, CancellationToken ct = default)
    {
        if (_connection is null) return;

        await using var cmd = new NpgsqlCommand($"SELECT setval('global_identity', {value})", _connection, _transaction);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    public async Task DisableConstraintsAsync(CancellationToken ct = default)
    {
        if (_connection is null) return;

        await using var cmd = new NpgsqlCommand(
            "SET LOCAL session_replication_role = 'replica'", _connection, _transaction);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    public async Task EnableConstraintsAsync(CancellationToken ct = default)
    {
        if (_connection is null) return;

        await using var cmd = new NpgsqlCommand(
            "SET LOCAL session_replication_role = 'origin'", _connection, _transaction);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    public async Task BulkInsertAsync(string tableName, System.Data.DataTable data, CancellationToken ct = default)
    {
        if (_connection is null || data.Rows.Count == 0) return;

        var columns = string.Join(", ", data.Columns.Cast<System.Data.DataColumn>().Select(c => c.ColumnName));
        var copyCommand = $"COPY {tableName} ({columns}) FROM STDIN (FORMAT TEXT, NULL '\\N')";

        await using var writer = await _connection.BeginTextImportAsync(copyCommand, ct);

        foreach (System.Data.DataRow row in data.Rows)
        {
            var values = new List<string>();
            for (int i = 0; i < data.Columns.Count; i++)
            {
                values.Add(FormatValueForCopy(row[i]));
            }
            await writer.WriteLineAsync(string.Join("\t", values));
        }
    }

    /// <inheritdoc />
    public async Task ExecuteAsync(string sql, CancellationToken ct = default)
    {
        if (_connection is null) throw new InvalidOperationException("Connection not opened. Call OpenAsync first.");

        await using var cmd = new NpgsqlCommand(sql, _connection, _transaction);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    // ===== Parallel import =====

    /// <summary>Tables large enough for their secondary indexes to dominate the load.</summary>
    private static readonly string[] LoadTables = ["_values", "_objects"];

    /// <summary>Where the definitions of the dropped indexes live until every index is back.</summary>
    private const string DroppedIndexesTable = "_import_dropped_indexes";

    /// <inheritdoc />
    public async Task<IDataProvider> OpenWorkerAsync(CancellationToken ct = default)
    {
        if (_connectionString is null) throw new InvalidOperationException("Connection not opened. Call OpenAsync first.");

        var worker = new PostgresProvider();
        await worker.OpenAsync(_connectionString, ct);
        // Session-wide, not SET LOCAL: a worker has no transaction of its own to scope it to.
        await worker.ExecuteAsync("SET session_replication_role = 'replica'", ct);
        return worker;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<DroppedIndex>> DropLoadIndexesAsync(CancellationToken ct = default)
    {
        if (_connection is null) throw new InvalidOperationException("Connection not opened. Call OpenAsync first.");

        // One transaction: the definitions are written and the indexes dropped together, so there is
        // no moment at which an index is gone and its definition is nowhere.
        await using var transaction = await _connection.BeginTransactionAsync(ct);

        await using (var create = new NpgsqlCommand(
            $"CREATE TABLE IF NOT EXISTS {DroppedIndexesTable} (name text PRIMARY KEY, table_name text NOT NULL, definition text NOT NULL)",
            _connection, transaction))
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        // Every index of the load tables that no constraint stands on. A unique index without a
        // constraint is dropped too: building it again checks the uniqueness of everything loaded.
        await using (var remember = new NpgsqlCommand($"""
            INSERT INTO {DroppedIndexesTable} (name, table_name, definition)
            SELECT i.relname, t.relname, pg_get_indexdef(x.indexrelid)
            FROM pg_index x
            JOIN pg_class i ON i.oid = x.indexrelid
            JOIN pg_class t ON t.oid = x.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = current_schema()
              AND t.relname = ANY(@tables)
              AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = x.indexrelid)
            ON CONFLICT (name) DO NOTHING
            """, _connection, transaction))
        {
            remember.Parameters.AddWithValue("tables", LoadTables);
            await remember.ExecuteNonQueryAsync(ct);
        }

        var dropped = new List<DroppedIndex>();
        await using (var read = new NpgsqlCommand(
            $"SELECT name, table_name, definition FROM {DroppedIndexesTable} ORDER BY table_name, name", _connection, transaction))
        await using (var reader = await read.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
                dropped.Add(new DroppedIndex(reader.GetString(0), reader.GetString(1), reader.GetString(2)));
        }

        foreach (var index in dropped)
        {
            await using var drop = new NpgsqlCommand(
                $"DROP INDEX IF EXISTS {QuoteIdentifier(index.Name)}", _connection, transaction);
            await drop.ExecuteNonQueryAsync(ct);
        }

        await transaction.CommitAsync(ct);
        return dropped;
    }

    /// <inheritdoc />
    public async Task CreateIndexAsync(DroppedIndex index, CancellationToken ct = default)
    {
        if (_connection is null) throw new InvalidOperationException("Connection not opened. Call OpenAsync first.");

        // pg_get_indexdef returns a plain CREATE INDEX. IF NOT EXISTS makes a repeated run harmless.
        var statement = index.Definition.StartsWith("CREATE UNIQUE INDEX ", StringComparison.Ordinal)
            ? "CREATE UNIQUE INDEX IF NOT EXISTS " + index.Definition["CREATE UNIQUE INDEX ".Length..]
            : "CREATE INDEX IF NOT EXISTS " + index.Definition["CREATE INDEX ".Length..];

        // An index build over tens of millions of rows outlives any ordinary command timeout.
        await using var cmd = new NpgsqlCommand(statement, _connection) { CommandTimeout = 0 };
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    public async Task ForgetDroppedIndexesAsync(CancellationToken ct = default)
    {
        if (_connection is null) return;
        await using var cmd = new NpgsqlCommand($"DROP TABLE IF EXISTS {DroppedIndexesTable}", _connection);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    // Each worker relaxes its own session in OpenWorkerAsync; nothing is switched off for the database.
    public Task RelaxConstraintsForLoadAsync(CancellationToken ct = default) => Task.CompletedTask;

    /// <inheritdoc />
    public Task RestoreConstraintsAfterLoadAsync(CancellationToken ct = default) => Task.CompletedTask;

    /// <inheritdoc />
    // CREATE INDEX takes a lock that lets other index builds on the same table run.
    public bool BuildsIndexesOfOneTableConcurrently => true;

    private static string QuoteIdentifier(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    /// <summary>
    /// Formats a CLR value for the PostgreSQL TEXT <c>COPY</c> protocol.
    /// </summary>
    private static string FormatValueForCopy(object? value)
    {
        if (value is null or DBNull)
            return "\\N";

        return value switch
        {
            bool b => b ? "t" : "f",
            DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff"),
            DateTimeOffset dto => dto.ToString("yyyy-MM-dd HH:mm:ss.ffffffzzz"),
            Guid g => g.ToString(),
            byte[] bytes => "\\\\x" + Convert.ToHexString(bytes),
            string s => EscapeCopyString(s),
            decimal d => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
            double d => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
            float f => f.ToString(System.Globalization.CultureInfo.InvariantCulture),
            _ => value.ToString() ?? "\\N"
        };
    }

    /// <summary>
    /// Escapes special characters for the PostgreSQL TEXT <c>COPY</c> format.
    /// </summary>
    private static string EscapeCopyString(string s)
    {
        return s
            .Replace("\\", "\\\\")
            .Replace("\t", "\\t")
            .Replace("\n", "\\n")
            .Replace("\r", "\\r");
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
        }
    }

    /// <inheritdoc />
    public DateTimeOffset ReadDateTimeOffset(DbDataReader reader, int ordinal) =>
        reader.GetFieldValue<DateTimeOffset>(ordinal);

    /// <inheritdoc />
    public Guid GuidFromDb(object raw) => (Guid)raw;

    /// <inheritdoc />
    public object GuidToDb(Guid value) => value;
}
