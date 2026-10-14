using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace redb.Export.Providers;

/// <summary>
/// <see cref="IDataProvider"/> implementation for Microsoft SQL Server.
/// <para>
/// Uses <see cref="SqlBulkCopy"/> for high-throughput inserts and
/// <c>sp_MSforeachtable</c> to toggle constraints and triggers during import.
/// </para>
/// </summary>
public sealed class MssqlProvider : IDataProvider, IParallelImportProvider
{
    private SqlConnection? _connection;
    private string? _connectionString;
    private SqlTransaction? _transaction;

    /// <inheritdoc />
    public async Task BeginImportAsync(CancellationToken ct = default)
    {
        if (_connection is null) throw new InvalidOperationException("Connection not opened. Call OpenAsync first.");
        if (_transaction is not null) throw new InvalidOperationException("An import transaction is already open.");
        // NOCHECK / DISABLE TRIGGER, TRUNCATE, SqlBulkCopy and ALTER SEQUENCE all take part in a SQL Server
        // transaction: a rollback also re-enables what the import disabled.
        _transaction = (SqlTransaction)await _connection.BeginTransactionAsync(ct);
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
        // A severe server error may already have rolled the transaction back; its connection then reports none.
        if (_transaction.Connection is not null)
            await _transaction.RollbackAsync();
        await _transaction.DisposeAsync();
        _transaction = null;
    }

    /// <inheritdoc />
    public string Name => "mssql";

    /// <inheritdoc />
    public DbConnection Connection => _connection
        ?? throw new InvalidOperationException("Connection not opened. Call OpenAsync first.");

    /// <inheritdoc />
    public async Task OpenAsync(string connectionString, CancellationToken ct = default)
    {
        _connectionString = connectionString;
        _connection = new SqlConnection(connectionString);
        await _connection.OpenAsync(ct);
    }

    /// <inheritdoc />
    public async Task CleanDatabaseAsync(CancellationToken ct = default)
    {
        if (_connection is null) return;

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

        // Disable triggers and FK checks before truncation.
        await using (var cmd = new SqlCommand(
            "EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'", _connection, _transaction))
        {
            await cmd.ExecuteNonQueryAsync(ct);
        }

        await using (var cmd = new SqlCommand(
            "EXEC sp_MSforeachtable 'ALTER TABLE ? DISABLE TRIGGER ALL'", _connection, _transaction))
        {
            await cmd.ExecuteNonQueryAsync(ct);
        }

        foreach (var table in tables)
        {
            // TRUNCATE is refused for a table any foreign key points at, disabled or not, so those are emptied
            // with DELETE - decided from the catalog, not by trying and swallowing the error: the old empty
            // catch also swallowed a lock timeout or a missing permission, the table stayed full, and the import
            // went on to mix old and new rows (review 2026-09-24). Any failure now fails the import, and the
            // import transaction undoes the cleaning.
            await using var referenced = new SqlCommand(
                "SELECT CASE WHEN EXISTS (SELECT 1 FROM sys.foreign_keys WHERE referenced_object_id = OBJECT_ID(@table)) THEN 1 ELSE 0 END",
                _connection, _transaction);
            referenced.Parameters.AddWithValue("@table", "dbo." + table);
            var isReferenced = Convert.ToInt32(await referenced.ExecuteScalarAsync(ct)) == 1;

            await using var cmd = new SqlCommand(
                isReferenced ? $"DELETE FROM {table}" : $"TRUNCATE TABLE {table}", _connection, _transaction);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        // Re-enable triggers and FK checks.
        await using (var cmd = new SqlCommand(
            "EXEC sp_MSforeachtable 'ALTER TABLE ? ENABLE TRIGGER ALL'", _connection, _transaction))
        {
            await cmd.ExecuteNonQueryAsync(ct);
        }

        await using (var cmd = new SqlCommand(
            "EXEC sp_MSforeachtable 'ALTER TABLE ? CHECK CONSTRAINT ALL'", _connection, _transaction))
        {
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    /// <inheritdoc />
    public async Task<long> GetSequenceValueAsync(CancellationToken ct = default)
    {
        if (_connection is null) return 0;

        const string sql = "SELECT CAST(current_value AS BIGINT) FROM sys.sequences WHERE name = 'global_identity'";
        await using var cmd = new SqlCommand(sql, _connection, _transaction);
        var result = await cmd.ExecuteScalarAsync(ct);
        return result is null ? 0 : Convert.ToInt64(result);
    }

    /// <inheritdoc />
    public async Task SetSequenceValueAsync(long value, CancellationToken ct = default)
    {
        if (_connection is null) return;

        await using var cmd = new SqlCommand($"ALTER SEQUENCE global_identity RESTART WITH {value + 1}", _connection, _transaction);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    public async Task DisableConstraintsAsync(CancellationToken ct = default)
    {
        if (_connection is null) return;

        await using var cmd1 = new SqlCommand(
            "EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'", _connection, _transaction) { CommandTimeout = 0 };
        await cmd1.ExecuteNonQueryAsync(ct);

        await using var cmd2 = new SqlCommand(
            "EXEC sp_MSforeachtable 'ALTER TABLE ? DISABLE TRIGGER ALL'", _connection, _transaction) { CommandTimeout = 0 };
        await cmd2.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    public async Task EnableConstraintsAsync(CancellationToken ct = default)
    {
        if (_connection is null) return;

        await using var cmd1 = new SqlCommand(
            "EXEC sp_MSforeachtable 'ALTER TABLE ? ENABLE TRIGGER ALL'", _connection, _transaction) { CommandTimeout = 0 };
        await cmd1.ExecuteNonQueryAsync(ct);

        await using var cmd2 = new SqlCommand(
            "EXEC sp_MSforeachtable 'ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL'", _connection, _transaction) { CommandTimeout = 0 };
        await cmd2.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    public async Task BulkInsertAsync(string tableName, System.Data.DataTable data, CancellationToken ct = default)
    {
        if (_connection is null || data.Rows.Count == 0) return;

        using var bulkCopy = new SqlBulkCopy(_connection, SqlBulkCopyOptions.Default, _transaction)
        {
            DestinationTableName = tableName,
            BatchSize = 5000,
            BulkCopyTimeout = 600
        };

        foreach (System.Data.DataColumn col in data.Columns)
        {
            bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
        }

        await bulkCopy.WriteToServerAsync(data, ct);
    }

    /// <inheritdoc />
    public async Task ExecuteAsync(string sql, CancellationToken ct = default)
    {
        if (_connection is null) throw new InvalidOperationException("Connection not opened. Call OpenAsync first.");

        await using var cmd = new SqlCommand(sql, _connection, _transaction);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    // ===== Parallel import =====

    /// <summary>Tables large enough for their secondary indexes to dominate the load.</summary>
    private static readonly string[] LoadTables = ["_values", "_objects"];

    /// <inheritdoc />
    public async Task<IDataProvider> OpenWorkerAsync(CancellationToken ct = default)
    {
        if (_connectionString is null) throw new InvalidOperationException("Connection not opened. Call OpenAsync first.");

        // Nothing to set on the session: SqlBulkCopy neither checks foreign keys nor fires triggers
        // unless asked to, and what it skipped is checked once at the end by
        // RestoreConstraintsAfterLoadAsync.
        var worker = new MssqlProvider();
        await worker.OpenAsync(_connectionString, ct);
        return worker;
    }

    /// <summary>
    /// Disables the secondary indexes of the large tables and returns the statements that rebuild them.
    /// A disabled index keeps its definition in the catalog, so nothing has to be stored elsewhere and
    /// a run that died half-way loses nothing: the next call finds the indexes still disabled and
    /// returns them with the rest. Left alone: the clustered index (disabling it takes the table
    /// offline), indexes that back a PRIMARY KEY or UNIQUE constraint, and any index a foreign key
    /// resolves through (disabling it would disable that foreign key).
    /// </summary>
    public async Task<IReadOnlyList<DroppedIndex>> DropLoadIndexesAsync(CancellationToken ct = default)
    {
        if (_connection is null) throw new InvalidOperationException("Connection not opened. Call OpenAsync first.");

        var indexes = new List<(string Name, string Table, bool Disabled)>();
        await using (var read = new SqlCommand($"""
            SELECT i.name, t.name, i.is_disabled
            FROM sys.indexes i
            JOIN sys.tables t ON t.object_id = i.object_id
            WHERE SCHEMA_NAME(t.schema_id) = 'dbo'
              AND t.name IN ({string.Join(", ", LoadTables.Select(n => "'" + n + "'"))})
              AND i.type = 2
              AND i.is_primary_key = 0
              AND i.is_unique_constraint = 0
              AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys f
                              WHERE f.referenced_object_id = i.object_id AND f.key_index_id = i.index_id)
            ORDER BY t.name, i.name
            """, _connection))
        await using (var reader = await read.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
                indexes.Add((reader.GetString(0), reader.GetString(1), reader.GetBoolean(2)));
        }

        var disabled = new List<DroppedIndex>();
        foreach (var (name, table, alreadyDisabled) in indexes)
        {
            var target = $"{QuoteIdentifier(name)} ON [dbo].{QuoteIdentifier(table)}";
            if (!alreadyDisabled)
            {
                await using var disable = new SqlCommand($"ALTER INDEX {target} DISABLE", _connection) { CommandTimeout = 0 };
                await disable.ExecuteNonQueryAsync(ct);
            }
            disabled.Add(new DroppedIndex(name, table, $"ALTER INDEX {target} REBUILD"));
        }

        return disabled;
    }

    /// <inheritdoc />
    public async Task CreateIndexAsync(DroppedIndex index, CancellationToken ct = default)
    {
        if (_connection is null) throw new InvalidOperationException("Connection not opened. Call OpenAsync first.");

        // A rebuild over tens of millions of rows outlives any ordinary command timeout.
        await using var cmd = new SqlCommand(index.Definition, _connection) { CommandTimeout = 0 };
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    // The catalog itself says which indexes are still disabled; there is nothing kept to forget.
    public Task ForgetDroppedIndexesAsync(CancellationToken ct = default) => Task.CompletedTask;

    /// <inheritdoc />
    public Task RelaxConstraintsForLoadAsync(CancellationToken ct = default) => DisableConstraintsAsync(ct);

    /// <inheritdoc />
    // WITH CHECK CHECK CONSTRAINT reads every loaded row against its foreign keys; on a large
    // database that is minutes, and it is what makes the constraints trusted again.
    public Task RestoreConstraintsAfterLoadAsync(CancellationToken ct = default) => EnableConstraintsAsync(ct);

    /// <inheritdoc />
    // An offline index rebuild ends by taking a schema-modification lock on the table; two of them
    // on one table wait for each other.
    public bool BuildsIndexesOfOneTableConcurrently => false;

    private static string QuoteIdentifier(string identifier) => "[" + identifier.Replace("]", "]]") + "]";

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
